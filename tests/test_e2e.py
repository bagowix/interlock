"""End-to-end tests of the public API over a real socket, with no integration
extras: ``CircuitBreaker`` / ``Registry`` guard a stdlib ``urllib`` client
hitting a real ``http.server``.

These prove the *engine itself* drives the full flow on the wire — independent
of any third-party client — across all three public surfaces (``call``,
decorator, context manager) and the registry. ``test_httpx2_e2e.py`` covers the
same flow through the flagship transport; this covers it through the bare API.

``urlopen`` raises ``HTTPError`` for status >= 400 and ``URLError`` when the
connection is refused, so the breaker's default classifier (any raised
exception is a failure) trips on an unhealthy upstream without a custom policy.
The breaker's clock is a ``FakeClock``, so the ``OPEN -> HALF_OPEN`` wait is
crossed deterministically with ``advance`` — real HTTP, deterministic time.
"""

import urllib.error
import urllib.request
from collections.abc import Callable

import pytest
from tests.conftest import FakeClock, Upstream, closed_port

from interlock import CircuitBreaker, CircuitOpenError, Config, Registry, State

# Trip after two failures and let a single half-open probe decide recovery, so
# the full flow stays short and deterministic.
_TRIP_FAST = Config(
    minimum_number_of_calls=2,
    failure_rate_threshold=0.5,
    permitted_calls_in_half_open=1,
)

# Past the default 60s wait_duration_in_open: advancing the clock by this admits
# the first half-open probe.
_PAST_OPEN_WAIT = 61.0


def _fetch(url: str) -> int:
    """GET ``url`` over a real socket and return its status code.

    Raises ``HTTPError`` on an unhealthy status and ``URLError`` on a refused
    connection — both of which the breaker counts as failures.
    """
    try:
        with urllib.request.urlopen(url, timeout=5) as response:  # noqa: S310 (loopback URL)
            return response.status
    except urllib.error.HTTPError as error:
        # HTTPError holds the response socket; the breaker keeps it as
        # last_failure context, so release it now to avoid a leaked socket.
        error.close()
        raise


def _breaker(fake_clock: FakeClock, *, name: str = 'upstream') -> CircuitBreaker:
    return CircuitBreaker(name=name, config=_TRIP_FAST, clock=fake_clock)


def test__e2e_call__healthy_upstream__returns_status(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    upstream = Upstream(status=200)
    url = serve(upstream)
    breaker = _breaker(fake_clock)

    assert breaker.call(_fetch, url) == 200
    assert upstream.received == 1


def test__e2e_call__upstream_5xx_for_a_period__opens_then_recovers(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    upstream = Upstream(status=503)
    url = serve(upstream)
    breaker = _breaker(fake_clock)

    # Two failing fetches (urlopen raises HTTPError on 503) trip the breaker.
    for _ in range(2):
        with pytest.raises(urllib.error.HTTPError):
            breaker.call(_fetch, url)
    assert upstream.received == 2
    assert breaker.state is State.OPEN

    # OPEN: rejected before a socket, carrying the real context.
    with pytest.raises(CircuitOpenError) as rejected:
        breaker.call(_fetch, url)
    assert upstream.received == 2
    assert rejected.value.retry_after == 60.0
    assert isinstance(rejected.value.last_failure, urllib.error.HTTPError)

    # Upstream recovers, but the breaker stays shut until its wait elapses.
    upstream.status = 200
    with pytest.raises(CircuitOpenError):
        breaker.call(_fetch, url)
    assert upstream.received == 2

    # Cross the wait: the probe reaches the now-healthy server and closes.
    fake_clock.advance(_PAST_OPEN_WAIT)
    assert breaker.call(_fetch, url) == 200
    assert upstream.received == 3
    assert breaker.state is State.CLOSED

    # Closed again: traffic flows normally.
    assert breaker.call(_fetch, url) == 200
    assert upstream.received == 4


def test__e2e_call__probe_still_failing__reopens(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    upstream = Upstream(status=503)
    url = serve(upstream)
    breaker = _breaker(fake_clock)

    for _ in range(2):
        with pytest.raises(urllib.error.HTTPError):
            breaker.call(_fetch, url)
    with pytest.raises(CircuitOpenError):
        breaker.call(_fetch, url)

    # The probe reaches the still-sick server, so the breaker reopens and the
    # following call is rejected again.
    fake_clock.advance(_PAST_OPEN_WAIT)
    with pytest.raises(urllib.error.HTTPError):
        breaker.call(_fetch, url)
    assert upstream.received == 3
    with pytest.raises(CircuitOpenError):
        breaker.call(_fetch, url)
    assert upstream.received == 3


def test__e2e_call__connection_refused__opens_with_real_exception(fake_clock: FakeClock) -> None:
    url = f'http://127.0.0.1:{closed_port()}/'
    breaker = _breaker(fake_clock, name='down')

    for _ in range(2):
        with pytest.raises(urllib.error.URLError):
            breaker.call(_fetch, url)

    with pytest.raises(CircuitOpenError) as rejected:
        breaker.call(_fetch, url)
    assert isinstance(rejected.value.last_failure, urllib.error.URLError)


def test__e2e_decorator__guards_a_real_call(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    upstream = Upstream(status=200)
    url = serve(upstream)
    breaker = _breaker(fake_clock)

    @breaker
    def get_status() -> int:
        return _fetch(url)

    assert get_status() == 200
    assert upstream.received == 1


def test__e2e_context_manager__trips_on_real_errors(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    upstream = Upstream(status=503)
    url = serve(upstream)
    breaker = _breaker(fake_clock)

    # The context manager sees only the block's exception (no result-based
    # classification), which is enough for HTTPError to trip the breaker.
    for _ in range(2):
        with pytest.raises(urllib.error.HTTPError), breaker:
            _fetch(url)

    with pytest.raises(CircuitOpenError), breaker:
        _fetch(url)
    assert upstream.received == 2


def test__e2e_registry__isolates_breakers_per_host(
    serve: Callable[..., str], fake_clock: FakeClock
) -> None:
    bad = Upstream(status=503)
    good = Upstream(status=200)
    bad_url = serve(bad, url_host='127.0.0.1')
    good_url = serve(good, url_host='localhost')
    registry = Registry(config=_TRIP_FAST, clock=fake_clock)

    for _ in range(2):
        with pytest.raises(urllib.error.HTTPError):
            registry.get('127.0.0.1').call(_fetch, bad_url)
    with pytest.raises(CircuitOpenError):
        registry.get('127.0.0.1').call(_fetch, bad_url)

    # A different host gets its own breaker, untouched by the bad one.
    assert registry.get('localhost').call(_fetch, good_url) == 200
