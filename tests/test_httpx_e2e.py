"""Loopback tests for the httpx transport integration."""

from collections.abc import Callable

import httpx
import pytest
from tests.conftest import FakeClock, Upstream, closed_port

from interlock import CircuitOpenError, Config
from interlock.integrations.httpx import (
    AsyncCircuitBreakerTransport,
    CircuitBreakerTransport,
    CircuitOpenTransportError,
)

_TRIP_FAST = Config(
    minimum_number_of_calls=2,
    failure_rate_threshold=0.5,
    permitted_calls_in_half_open=1,
)
_PAST_OPEN_WAIT = 61.0


def _sync_client(fake_clock: FakeClock) -> httpx.Client:
    transport = CircuitBreakerTransport(httpx.HTTPTransport(), config=_TRIP_FAST, clock=fake_clock)
    return httpx.Client(transport=transport)


def _async_client(fake_clock: FakeClock) -> httpx.AsyncClient:
    transport = AsyncCircuitBreakerTransport(
        httpx.AsyncHTTPTransport(), config=_TRIP_FAST, clock=fake_clock
    )
    return httpx.AsyncClient(transport=transport)


def test__httpx_e2e_sync__healthy_upstream__passes_response_through(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    backend = Upstream(status=200, body=b'pong')

    with _sync_client(fake_clock) as client:
        response = client.get(serve(backend))

    assert response.status_code == 200
    assert response.text == 'pong'
    assert backend.received == 1


def test__httpx_e2e_sync__upstream_5xx_for_a_period__opens_then_recovers(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    backend = Upstream(status=503)
    url = serve(backend)

    with _sync_client(fake_clock) as client:
        assert client.get(url).status_code == 503
        assert client.get(url).status_code == 503

        with pytest.raises(CircuitOpenError) as rejected:
            client.get(url)
        assert backend.received == 2
        assert rejected.value.breaker_name == '127.0.0.1'
        assert rejected.value.retry_after == 60.0

        backend.status = 200
        fake_clock.advance(_PAST_OPEN_WAIT)
        assert client.get(url).status_code == 200
        assert client.get(url).status_code == 200

    assert backend.received == 4


def test__httpx_e2e_sync__probe_still_failing__reopens(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    backend = Upstream(status=503)
    url = serve(backend)

    with _sync_client(fake_clock) as client:
        client.get(url)
        client.get(url)
        fake_clock.advance(_PAST_OPEN_WAIT)
        assert client.get(url).status_code == 503

        with pytest.raises(CircuitOpenError):
            client.get(url)

    assert backend.received == 3


def test__httpx_e2e_sync__connection_refused__opens_breaker(fake_clock: FakeClock) -> None:
    url = f'http://127.0.0.1:{closed_port()}/'

    with _sync_client(fake_clock) as client:
        for _ in range(2):
            with pytest.raises(httpx.ConnectError):
                client.get(url)

        with pytest.raises(CircuitOpenError) as rejected:
            client.get(url)
        assert isinstance(rejected.value.last_failure, httpx.ConnectError)


def test__httpx_e2e_sync__breakers_isolated_per_host(
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

        assert client.get(good_url).status_code == 200


@pytest.mark.asyncio
async def test__httpx_e2e_async__healthy_upstream__passes_response_through(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    backend = Upstream(status=200, body=b'pong')

    async with _async_client(fake_clock) as client:
        response = await client.get(serve(backend))

    assert response.status_code == 200
    assert response.text == 'pong'
    assert backend.received == 1


@pytest.mark.asyncio
async def test__httpx_e2e_async__upstream_5xx_for_a_period__opens_then_recovers(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    backend = Upstream(status=503)
    url = serve(backend)

    async with _async_client(fake_clock) as client:
        assert (await client.get(url)).status_code == 503
        assert (await client.get(url)).status_code == 503

        with pytest.raises(CircuitOpenError):
            await client.get(url)
        assert backend.received == 2

        backend.status = 200
        fake_clock.advance(_PAST_OPEN_WAIT)
        assert (await client.get(url)).status_code == 200

    assert backend.received == 3


@pytest.mark.asyncio
async def test__httpx_e2e_async__connection_refused__opens_breaker(fake_clock: FakeClock) -> None:
    url = f'http://127.0.0.1:{closed_port()}/'

    async with _async_client(fake_clock) as client:
        for _ in range(2):
            with pytest.raises(httpx.ConnectError):
                await client.get(url)

        with pytest.raises(CircuitOpenError):
            await client.get(url)


def test__httpx_e2e_sync__open_circuit__rejects_as_transport_error(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    """A real client must deliver the rejection through httpx's own hierarchy."""
    backend = Upstream(status=503)
    url = serve(backend)

    with _sync_client(fake_clock) as client:
        assert client.get(url).status_code == 503
        assert client.get(url).status_code == 503

        with pytest.raises(httpx.TransportError) as rejected:
            client.get(url)

    assert isinstance(rejected.value, CircuitOpenTransportError)
    assert rejected.value.request.url == httpx.URL(url)
    assert not isinstance(rejected.value, httpx.ConnectError)
