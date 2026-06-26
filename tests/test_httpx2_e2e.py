"""End-to-end tests over a real socket: a real ``httpx2`` client drives requests
through ``CircuitBreakerTransport`` into an actual HTTP server running in a
background thread.

Unlike ``test_httpx2.py`` (which stubs the inner transport), these exercise the
full flow on the wire: real connections, real ``ConnectError`` on a refused
port, and the breaker short-circuiting *before* a socket is ever opened.

The upstream's health is a mutable switch the test flips to model "fails for a
period, then recovers". The breaker's *own* clock is a ``FakeClock``, so the
``OPEN -> HALF_OPEN`` wait is crossed deterministically with ``advance`` instead
of sleeping — real HTTP, deterministic time.
"""

from collections.abc import Callable

import pytest
from tests.conftest import FakeClock, Upstream, closed_port

import httpx2
from interlock import CircuitOpenError, Config
from interlock.httpx2 import AsyncCircuitBreakerTransport, CircuitBreakerTransport

# Trip after two failures so scenarios stay short; the default 10-call minimum
# would only add noise to an end-to-end flow test. One half-open probe decides
# recovery, so a single post-wait request closes or reopens the breaker.
_TRIP_FAST = Config(
    minimum_number_of_calls=2,
    failure_rate_threshold=0.5,
    permitted_calls_in_half_open=1,
)

# Past the default 60s wait_duration_in_open: advancing the breaker's clock by
# this admits the first half-open probe.
_PAST_OPEN_WAIT = 61.0


def _sync_client(fake_clock: FakeClock) -> httpx2.Client:
    transport = CircuitBreakerTransport(httpx2.HTTPTransport(), config=_TRIP_FAST, clock=fake_clock)
    return httpx2.Client(transport=transport)


def _async_client(fake_clock: FakeClock) -> httpx2.AsyncClient:
    transport = AsyncCircuitBreakerTransport(
        httpx2.AsyncHTTPTransport(), config=_TRIP_FAST, clock=fake_clock
    )
    return httpx2.AsyncClient(transport=transport)


def test__e2e_sync__healthy_upstream__passes_response_through(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    backend = Upstream(status=200, body=b'pong')
    url = serve(backend)

    with _sync_client(fake_clock) as client:
        response = client.get(url)

    assert response.status_code == 200
    assert response.text == 'pong'
    assert backend.received == 1


def test__e2e_sync__upstream_5xx_for_a_period__opens_then_recovers(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    backend = Upstream(status=503)
    url = serve(backend)

    with _sync_client(fake_clock) as client:
        assert client.get(url).status_code == 503
        assert client.get(url).status_code == 503
        assert backend.received == 2

        # Breaker is now OPEN: the next call is rejected without a socket.
        with pytest.raises(CircuitOpenError) as rejected:
            client.get(url)
        assert backend.received == 2
        assert rejected.value.breaker_name == '127.0.0.1'
        # The clock has not moved since the breaker opened, so the full default
        # wait remains until the next probe.
        assert rejected.value.retry_after == 60.0

        # Upstream recovers, but the breaker still rejects until its wait elapses.
        backend.status = 200
        with pytest.raises(CircuitOpenError):
            client.get(url)
        assert backend.received == 2

        # Cross the open-wait: the first call after is a probe that reaches the
        # now-healthy server and closes the breaker.
        fake_clock.advance(_PAST_OPEN_WAIT)
        assert client.get(url).status_code == 200
        assert backend.received == 3

        # Closed again: traffic flows normally.
        assert client.get(url).status_code == 200
        assert backend.received == 4


def test__e2e_sync__probe_still_failing__reopens(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    backend = Upstream(status=503)
    url = serve(backend)

    with _sync_client(fake_clock) as client:
        client.get(url)
        client.get(url)
        with pytest.raises(CircuitOpenError):
            client.get(url)

        # Probe is admitted after the wait, reaches the still-sick server, and
        # the breaker reopens — so the following call is rejected again.
        fake_clock.advance(_PAST_OPEN_WAIT)
        assert client.get(url).status_code == 503
        assert backend.received == 3
        with pytest.raises(CircuitOpenError):
            client.get(url)
        assert backend.received == 3


def test__e2e_sync__connection_refused__opens_breaker(fake_clock: FakeClock) -> None:
    url = f'http://127.0.0.1:{closed_port()}/'

    with _sync_client(fake_clock) as client:
        for _ in range(2):
            with pytest.raises(httpx2.ConnectError):
                client.get(url)

        # Real connect failures counted as failures trip the breaker, which now
        # short-circuits with its own error instead of a third connect attempt.
        with pytest.raises(CircuitOpenError) as rejected:
            client.get(url)
        # The breaker surfaces the real exception that tripped it.
        assert isinstance(rejected.value.last_failure, httpx2.ConnectError)


def test__e2e_sync__breakers_isolated_per_host(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    bad = Upstream(status=503)
    good = Upstream(status=200)
    bad_url = serve(bad, url_host='127.0.0.1')
    good_url = serve(good, url_host='localhost')

    with _sync_client(fake_clock) as client:
        client.get(bad_url)
        client.get(bad_url)
        with pytest.raises(CircuitOpenError):
            client.get(bad_url)

        # A different host has its own breaker, untouched by the bad one.
        assert client.get(good_url).status_code == 200


@pytest.mark.asyncio
async def test__e2e_async__healthy_upstream__passes_response_through(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    backend = Upstream(status=200, body=b'pong')
    url = serve(backend)

    async with _async_client(fake_clock) as client:
        response = await client.get(url)

    assert response.status_code == 200
    assert response.text == 'pong'
    assert backend.received == 1


@pytest.mark.asyncio
async def test__e2e_async__upstream_5xx_for_a_period__opens_then_recovers(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    backend = Upstream(status=503)
    url = serve(backend)

    async with _async_client(fake_clock) as client:
        assert (await client.get(url)).status_code == 503
        assert (await client.get(url)).status_code == 503
        assert backend.received == 2

        with pytest.raises(CircuitOpenError):
            await client.get(url)
        assert backend.received == 2

        backend.status = 200
        fake_clock.advance(_PAST_OPEN_WAIT)
        assert (await client.get(url)).status_code == 200
        assert backend.received == 3


@pytest.mark.asyncio
async def test__e2e_async__connection_refused__opens_breaker(fake_clock: FakeClock) -> None:
    url = f'http://127.0.0.1:{closed_port()}/'

    async with _async_client(fake_clock) as client:
        for _ in range(2):
            with pytest.raises(httpx2.ConnectError):
                await client.get(url)

        with pytest.raises(CircuitOpenError):
            await client.get(url)
