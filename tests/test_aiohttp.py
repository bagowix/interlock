"""Tests for the aiohttp integration (``interlock.integrations.aiohttp``)."""

from collections.abc import Awaitable
from typing import cast

import pytest
from aiohttp import (
    ClientConnectionError,
    ClientError,
    ClientHandlerType,
    ClientOSError,
    ClientRequest,
    ClientResponse,
    ClientResponseError,
    ClientSession,
    ServerTimeoutError,
    web,
)
from aiohttp.test_utils import TestServer
from pytest_mock import MockerFixture
from tests.conftest import FakeClock
from yarl import URL

from interlock import CircuitBreaker, CircuitOpenError, Config, Registry, State
from interlock.integrations.aiohttp import (
    CircuitBreakerMiddleware,
    CircuitOpenClientError,
    HttpStatusClassifier,
)

_TRIP_FAST = Config(minimum_number_of_calls=2, failure_rate_threshold=0.5)


class _StubResponse:
    def __init__(self, status: int) -> None:
        self.status = status


class _StubRequest:
    def __init__(self, url: str) -> None:
        self.url = URL(url)


def _request(url: str) -> ClientRequest:
    return cast('ClientRequest', _StubRequest(url))


class _StubHandler:
    """Async handler double: pops the next scripted response or exception."""

    def __init__(self, script: list[int | BaseException]) -> None:
        self._script = script
        self.calls = 0

    async def __call__(self, request: ClientRequest) -> ClientResponse:
        self.calls += 1
        step = self._script.pop(0)
        if isinstance(step, BaseException):
            raise step
        return cast('ClientResponse', _StubResponse(step))


def _handler(script: list[int | BaseException]) -> _StubHandler:
    return _StubHandler(script)


# --- HttpStatusClassifier -----------------------------------------------------


@pytest.mark.parametrize('status', [429, 500, 502, 503, 504])
def test__http_status_classifier__retryable_status__is_failure(status: int) -> None:
    classifier = HttpStatusClassifier()

    assert classifier.is_failure(result=_StubResponse(status), exception=None) is True


@pytest.mark.parametrize('status', [200, 201, 301, 400, 404])
def test__http_status_classifier__healthy_or_client_error__is_success(status: int) -> None:
    classifier = HttpStatusClassifier()

    assert classifier.is_failure(result=_StubResponse(status), exception=None) is False


def test__http_status_classifier__exception__is_failure() -> None:
    classifier = HttpStatusClassifier()

    assert classifier.is_failure(result=None, exception=ConnectionError('boom')) is True


def test__http_status_classifier__excluded_exception__is_success() -> None:
    class _MissingCredentials(Exception):
        pass

    classifier = HttpStatusClassifier(excluded_exceptions=(_MissingCredentials,))

    assert classifier.is_failure(result=None, exception=_MissingCredentials()) is False
    assert classifier.is_failure(result=None, exception=ConnectionError('boom')) is True


@pytest.mark.parametrize('entry', ['ConnectionError', str, KeyboardInterrupt])
def test__http_status_classifier__non_exception_exclusion__raises(entry: object) -> None:
    with pytest.raises(TypeError, match='Exception subclasses'):
        HttpStatusClassifier(excluded_exceptions=[cast('type[Exception]', entry)])


# --- CircuitBreakerMiddleware (unit, stubbed handler) -------------------------


@pytest.mark.asyncio
async def test__middleware__failure_statuses__trip_breaker_and_reject(
    fake_clock: FakeClock,
) -> None:
    middleware = CircuitBreakerMiddleware(config=_TRIP_FAST, clock=fake_clock)
    handler = _handler([503, 503])

    await middleware(_request('https://api.a/x'), cast('ClientHandlerType', handler))
    await middleware(_request('https://api.a/x'), cast('ClientHandlerType', handler))

    with pytest.raises(CircuitOpenError):
        await middleware(_request('https://api.a/x'), cast('ClientHandlerType', handler))
    assert handler.calls == 2


@pytest.mark.asyncio
async def test__middleware__cached_breaker__is_not_redecorated_per_request(
    fake_clock: FakeClock, mocker: MockerFixture
) -> None:
    """The per-request path costs no decoration: no ``functools.wraps``, no detection."""
    middleware = CircuitBreakerMiddleware(config=_TRIP_FAST, clock=fake_clock)
    handler = _handler([200, 200])
    await middleware(_request('https://api.a/x'), cast('ClientHandlerType', handler))
    mocker.patch.object(CircuitBreaker, '__call__', side_effect=AssertionError('decorated'))

    response = await middleware(_request('https://api.a/x'), cast('ClientHandlerType', handler))

    assert response.status == 200


@pytest.mark.asyncio
async def test__middleware__handler_is_not_a_coroutine_function__still_awaited(
    fake_clock: FakeClock,
) -> None:
    """Middleware chains may hand over plain callables that return an awaitable."""
    middleware = CircuitBreakerMiddleware(config=_TRIP_FAST, clock=fake_clock)
    inner = _handler([200])

    def handler(request: ClientRequest) -> Awaitable[ClientResponse]:
        return inner(request)

    response = await middleware(_request('https://api.a/x'), cast('ClientHandlerType', handler))

    assert response.status == 200


@pytest.mark.asyncio
async def test__middleware__open_host__other_host_unaffected(fake_clock: FakeClock) -> None:
    middleware = CircuitBreakerMiddleware(config=_TRIP_FAST, clock=fake_clock)
    handler = _handler([503, 503, 200])

    await middleware(_request('https://api.a/x'), cast('ClientHandlerType', handler))
    await middleware(_request('https://api.a/x'), cast('ClientHandlerType', handler))
    with pytest.raises(CircuitOpenError):
        await middleware(_request('https://api.a/x'), cast('ClientHandlerType', handler))

    response = await middleware(_request('https://api.b/x'), cast('ClientHandlerType', handler))

    assert response.status == 200
    assert handler.calls == 3


@pytest.mark.asyncio
async def test__middleware__name_resolver__collapses_hosts_onto_one_breaker(
    fake_clock: FakeClock,
) -> None:
    middleware = CircuitBreakerMiddleware(
        config=_TRIP_FAST,
        clock=fake_clock,
        name_resolver=lambda request: request.url.host.removesuffix('.query.consul'),
    )
    handler = _handler([503, 503])

    await middleware(
        _request('https://orders.query.consul/v1'),
        cast('ClientHandlerType', handler),
    )
    await middleware(_request('https://orders/v2'), cast('ClientHandlerType', handler))

    with pytest.raises(CircuitOpenError):
        await middleware(
            _request('https://orders.query.consul/v3'),
            cast('ClientHandlerType', handler),
        )
    breaker = middleware.registry.get_existing('orders')
    assert breaker is not None
    assert breaker.snapshot().failed_calls == 2


@pytest.mark.asyncio
async def test__middleware__empty_resolved_name__raises_before_handler(
    fake_clock: FakeClock,
) -> None:
    middleware = CircuitBreakerMiddleware(
        clock=fake_clock,
        name_resolver=lambda _request: '   ',
    )
    handler = _handler([200])
    request = _request('https://api.example.com/v1')

    with pytest.raises(ValueError, match='empty breaker name') as raised:
        await middleware(request, cast('ClientHandlerType', handler))

    assert str(request.url) in str(raised.value)
    assert handler.calls == 0


@pytest.mark.parametrize('resolved_name', [None, b'orders'])
@pytest.mark.asyncio
async def test__middleware__non_string_resolved_name__raises_before_handler(
    fake_clock: FakeClock,
    resolved_name: object,
) -> None:
    middleware = CircuitBreakerMiddleware(
        clock=fake_clock,
        name_resolver=lambda _request: cast('str', resolved_name),
    )
    handler = _handler([200])
    request = _request('https://api.example.com/v1')

    with pytest.raises(ValueError, match='non-string breaker name') as raised:
        await middleware(request, cast('ClientHandlerType', handler))

    assert str(request.url) in str(raised.value)
    assert handler.calls == 0


@pytest.mark.asyncio
async def test__middleware__client_errors__do_not_trip(fake_clock: FakeClock) -> None:
    middleware = CircuitBreakerMiddleware(config=_TRIP_FAST, clock=fake_clock)
    handler = _handler([404] * 5)

    for _ in range(5):
        response = await middleware(_request('https://api.a/x'), cast('ClientHandlerType', handler))
        assert response.status == 404

    assert handler.calls == 5


@pytest.mark.asyncio
async def test__middleware__metrics_only_failures__record_without_rejecting(
    fake_clock: FakeClock,
) -> None:
    middleware = CircuitBreakerMiddleware(
        initial_state=State.METRICS_ONLY,
        config=_TRIP_FAST,
        clock=fake_clock,
    )
    handler = _handler([503] * 5)

    for _ in range(5):
        response = await middleware(
            _request('https://api.a/x'),
            cast('ClientHandlerType', handler),
        )
        assert response.status == 503

    breaker = middleware.registry.get_existing('api.a')
    assert breaker is not None
    assert breaker.state is State.METRICS_ONLY
    assert breaker.snapshot().failed_calls == 5


@pytest.mark.asyncio
async def test__middleware__aclose__releases_registry(mocker: MockerFixture) -> None:
    middleware = CircuitBreakerMiddleware()
    aclose_all = mocker.spy(middleware.registry, 'aclose_all')

    await middleware.aclose()

    aclose_all.assert_awaited_once_with()


@pytest.mark.asyncio
async def test__middlewares__shared_registry__merge_window(
    fake_clock: FakeClock,
) -> None:
    registry = Registry(
        config=_TRIP_FAST,
        clock=fake_clock,
        classifier=HttpStatusClassifier(),
    )
    first = CircuitBreakerMiddleware(registry=registry)
    second = CircuitBreakerMiddleware(registry=registry)
    handler = _handler([503, 503])

    await first(_request('https://api.a/x'), cast('ClientHandlerType', handler))
    await second(_request('https://api.a/x'), cast('ClientHandlerType', handler))
    breaker = registry.get_existing('api.a')

    assert first.registry is registry
    assert second.registry is registry
    assert breaker is not None
    assert breaker.snapshot().failed_calls == 2
    with pytest.raises(CircuitOpenError):
        await second(_request('https://api.a/x'), cast('ClientHandlerType', handler))


@pytest.mark.asyncio
async def test__middleware__injected_registry__aclose_preserves_registry(
    mocker: MockerFixture,
) -> None:
    registry = Registry()
    middleware = CircuitBreakerMiddleware(registry=registry)
    aclose_all = mocker.spy(registry, 'aclose_all')

    await middleware.aclose()

    aclose_all.assert_not_awaited()


def test__middleware__registry_with_config__raises_conflict() -> None:
    with pytest.raises(ValueError, match='config'):
        CircuitBreakerMiddleware(registry=Registry(), config=_TRIP_FAST)


@pytest.mark.asyncio
async def test__middleware__handler_exception__counts_as_failure(fake_clock: FakeClock) -> None:
    middleware = CircuitBreakerMiddleware(config=_TRIP_FAST, clock=fake_clock)
    handler = _handler([ConnectionError('down'), ConnectionError('down')])

    for _ in range(2):
        with pytest.raises(ConnectionError):
            await middleware(_request('https://api.a/x'), cast('ClientHandlerType', handler))

    with pytest.raises(CircuitOpenError):
        await middleware(_request('https://api.a/x'), cast('ClientHandlerType', handler))
    assert handler.calls == 2


@pytest.mark.asyncio
async def test__middleware__url_without_host__raises_value_error(fake_clock: FakeClock) -> None:
    middleware = CircuitBreakerMiddleware(config=_TRIP_FAST, clock=fake_clock)
    handler = _handler([200])

    with pytest.raises(ValueError, match='no host'):
        await middleware(_request('/relative/path'), cast('ClientHandlerType', handler))
    assert handler.calls == 0


# --- E2E against a real aiohttp server ----------------------------------------


@pytest.mark.asyncio
async def test__middleware__e2e_real_session__rejects_after_server_failures(
    fake_clock: FakeClock,
) -> None:
    async def unhealthy(_request: web.Request) -> web.Response:
        return web.Response(status=503)

    app = web.Application()
    app.router.add_get('/', unhealthy)
    server = TestServer(app)
    await server.start_server()

    middleware = CircuitBreakerMiddleware(config=_TRIP_FAST, clock=fake_clock)
    try:
        async with ClientSession(middlewares=(middleware,)) as session:
            for _ in range(2):
                async with session.get(server.make_url('/')) as response:
                    assert response.status == 503

            with pytest.raises(CircuitOpenError):
                await session.get(server.make_url('/'))
    finally:
        await server.close()


def test__http_status_classifier__custom_statuses__override_default_set() -> None:
    classifier = HttpStatusClassifier(failure_statuses={404, 408})

    assert classifier.is_failure(result=_StubResponse(404), exception=None) is True
    assert classifier.is_failure(result=_StubResponse(408), exception=None) is True
    assert classifier.is_failure(result=_StubResponse(500), exception=None) is False


# --- dialect errors -----------------------------------------------------------


async def _tripped_middleware(fake_clock: FakeClock) -> CircuitBreakerMiddleware:
    """A middleware whose ``api.a`` breaker has just been tripped open."""
    middleware = CircuitBreakerMiddleware(config=_TRIP_FAST, clock=fake_clock)
    handler = _handler([ClientConnectionError('down'), ClientConnectionError('down')])
    for _ in range(2):
        with pytest.raises(ClientConnectionError):
            await middleware(_request('https://api.a/x'), cast('ClientHandlerType', handler))

    return middleware


@pytest.mark.asyncio
async def test__middleware__open_circuit__rejects_inside_aiohttp_hierarchy(
    fake_clock: FakeClock,
) -> None:
    middleware = await _tripped_middleware(fake_clock)
    handler = _handler([])

    with pytest.raises(CircuitOpenClientError) as excinfo:
        await middleware(_request('https://api.a/x'), cast('ClientHandlerType', handler))

    assert isinstance(excinfo.value, ClientConnectionError)
    assert isinstance(excinfo.value, ClientError)
    assert isinstance(excinfo.value, CircuitOpenError)


@pytest.mark.asyncio
async def test__middleware__open_circuit__stays_outside_retryable_leaf_types(
    fake_clock: FakeClock,
) -> None:
    middleware = await _tripped_middleware(fake_clock)
    handler = _handler([])

    with pytest.raises(CircuitOpenClientError) as excinfo:
        await middleware(_request('https://api.a/x'), cast('ClientHandlerType', handler))

    assert not isinstance(excinfo.value, ClientResponseError)
    assert not isinstance(excinfo.value, ClientOSError)
    assert not isinstance(excinfo.value, ServerTimeoutError)


@pytest.mark.asyncio
async def test__middleware__open_circuit__carries_breaker_context(
    fake_clock: FakeClock,
) -> None:
    middleware = await _tripped_middleware(fake_clock)
    handler = _handler([])

    with pytest.raises(CircuitOpenClientError) as excinfo:
        await middleware(_request('https://api.a/x'), cast('ClientHandlerType', handler))

    error = excinfo.value
    assert error.breaker_name == 'api.a'
    assert error.retry_after == _TRIP_FAST.wait_duration_in_open
    assert isinstance(error.last_failure, ClientConnectionError)
    assert str(error) == f"Circuit 'api.a' is open; retry in ~{error.retry_after:.3f}s"
    assert type(error.__cause__) is CircuitOpenError


@pytest.mark.asyncio
async def test__middleware__inner_dialect_error__propagates_unchanged(
    fake_clock: FakeClock,
) -> None:
    rejection = CircuitOpenClientError('inner', retry_after=1.0)
    middleware = CircuitBreakerMiddleware(config=_TRIP_FAST, clock=fake_clock)
    handler = _handler([rejection])

    with pytest.raises(CircuitOpenClientError) as excinfo:
        await middleware(_request('https://api.a/x'), cast('ClientHandlerType', handler))

    assert excinfo.value is rejection
    assert excinfo.value.__cause__ is None


@pytest.mark.asyncio
async def test__middleware__e2e_real_session__rejection_is_caught_as_client_error(
    fake_clock: FakeClock,
) -> None:
    """A real session must not mangle the rejection on its way out of the chain."""

    async def unhealthy(_request: web.Request) -> web.Response:
        return web.Response(status=503)

    app = web.Application()
    app.router.add_get('/', unhealthy)
    server = TestServer(app)
    await server.start_server()

    middleware = CircuitBreakerMiddleware(config=_TRIP_FAST, clock=fake_clock)
    try:
        async with ClientSession(middlewares=(middleware,)) as session:
            for _ in range(2):
                async with session.get(server.make_url('/')) as response:
                    assert response.status == 503

            with pytest.raises(ClientConnectionError) as excinfo:
                await session.get(server.make_url('/'))
    finally:
        await server.close()

    assert isinstance(excinfo.value, CircuitOpenClientError)
