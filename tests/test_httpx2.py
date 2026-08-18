from collections.abc import Callable
from types import TracebackType
from typing import Self, cast

import httpx2
import pytest
from httpx2 import AsyncBaseTransport, BaseTransport, Request, Response
from pytest_mock import MockerFixture
from tests.conftest import FakeClock, RecordingListener

from interlock import (
    BulkheadFullError,
    CallTimeoutError,
    CircuitBreaker,
    CircuitOpenError,
    Config,
    Outcome,
    Registry,
    State,
)
from interlock.integrations.httpx2 import (
    AsyncCircuitBreakerTransport,
    BulkheadFullTransportError,
    CallTimeoutTransportError,
    CircuitBreakerTransport,
    CircuitOpenTransportError,
    HttpStatusClassifier,
)

_TRIP_FAST = Config(minimum_number_of_calls=2, failure_rate_threshold=0.5)


class _SyncStub(BaseTransport):
    def __init__(self, handler: Callable[[Request], Response]) -> None:
        self._handler = handler
        self.calls = 0
        self.closed = False

    def handle_request(self, request: Request) -> Response:
        self.calls += 1
        return self._handler(request)

    def close(self) -> None:
        self.closed = True


class _AsyncStub(AsyncBaseTransport):
    def __init__(self, handler: Callable[[Request], Response]) -> None:
        self._handler = handler
        self.calls = 0
        self.closed = False

    async def handle_async_request(self, request: Request) -> Response:
        self.calls += 1
        return self._handler(request)

    async def aclose(self) -> None:
        self.closed = True


class _SyncLifecycleStub(_SyncStub):
    def __init__(self) -> None:
        super().__init__(self._handle)
        self.entered = False
        self.exited = False

    def __enter__(self) -> Self:
        self.entered = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        self.exited = True
        super().__exit__(exc_type, exc_value, traceback)

    def _handle(self, _request: Request) -> Response:
        if not self.entered:
            raise RuntimeError('transport was not entered')
        return Response(200)


class _AsyncLifecycleStub(_AsyncStub):
    def __init__(self) -> None:
        super().__init__(self._handle)
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> Self:
        self.entered = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        self.exited = True
        await super().__aexit__(exc_type, exc_value, traceback)

    def _handle(self, _request: Request) -> Response:
        if not self.entered:
            raise RuntimeError('transport was not entered')
        return Response(200)


def _request(url: str = 'https://api.example.com/v1') -> Request:
    return Request('GET', url)


def test__http_status_classifier__exception__is_failure() -> None:
    classifier = HttpStatusClassifier()

    assert classifier.is_failure(result=None, exception=httpx2.ConnectError('boom'))


@pytest.mark.parametrize('status', [429, 500, 502, 503, 504])
def test__http_status_classifier__retryable_status__is_failure(status: int) -> None:
    classifier = HttpStatusClassifier()

    assert classifier.is_failure(result=Response(status), exception=None)


@pytest.mark.parametrize('status', [200, 301, 400, 404, 418, 501, 505])
def test__http_status_classifier__healthy_or_permanent_status__is_success(status: int) -> None:
    classifier = HttpStatusClassifier()

    assert not classifier.is_failure(result=Response(status), exception=None)


@pytest.mark.parametrize(
    'exception',
    [httpx2.UnsupportedProtocol('no scheme'), httpx2.LocalProtocolError('bad request')],
)
def test__http_status_classifier__caller_side_exception__is_success(
    exception: httpx2.TransportError,
) -> None:
    classifier = HttpStatusClassifier()

    assert not classifier.is_failure(result=None, exception=exception)


def test__http_status_classifier__pool_timeout__is_failure() -> None:
    classifier = HttpStatusClassifier()

    assert classifier.is_failure(result=None, exception=httpx2.PoolTimeout('exhausted'))


def test__http_status_classifier__custom_exclusions__override_default_set() -> None:
    classifier = HttpStatusClassifier(excluded_exceptions=(httpx2.PoolTimeout,))

    assert not classifier.is_failure(result=None, exception=httpx2.PoolTimeout('exhausted'))
    assert classifier.is_failure(result=None, exception=httpx2.UnsupportedProtocol('no scheme'))


def test__http_status_classifier__empty_exclusions__counts_every_exception() -> None:
    classifier = HttpStatusClassifier(excluded_exceptions=())

    assert classifier.is_failure(result=None, exception=httpx2.UnsupportedProtocol('no scheme'))


@pytest.mark.parametrize('entry', ['httpx2.PoolTimeout', str, KeyboardInterrupt])
def test__http_status_classifier__non_exception_exclusion__raises(entry: object) -> None:
    with pytest.raises(TypeError, match='Exception subclasses'):
        HttpStatusClassifier(excluded_exceptions=[cast('type[Exception]', entry)])


def test__sync_transport__caller_side_exceptions__keep_circuit_closed(
    fake_clock: FakeClock,
) -> None:
    def unsupported(_request: Request) -> Response:
        raise httpx2.UnsupportedProtocol('no scheme')

    inner = _SyncStub(unsupported)
    transport = CircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    for _ in range(4):
        with pytest.raises(httpx2.UnsupportedProtocol):
            transport.handle_request(_request())

    assert transport.registry.get('api.example.com').state is State.CLOSED


def test__sync_transport__success_response__passes_through(fake_clock: FakeClock) -> None:
    inner = _SyncStub(lambda _request: Response(200))
    transport = CircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    response = transport.handle_request(_request())

    assert response.status_code == 200


def test__sync_transport__cached_breaker__is_not_redecorated_per_request(
    fake_clock: FakeClock, mocker: MockerFixture
) -> None:
    """The per-request path costs no decoration: no ``functools.wraps``, no detection."""
    inner = _SyncStub(lambda _request: Response(200))
    transport = CircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)
    transport.handle_request(_request())
    mocker.patch.object(CircuitBreaker, '__call__', side_effect=AssertionError('decorated'))

    assert transport.handle_request(_request()).status_code == 200


def test__sync_transport__context_manager__delegates_wrapped_lifecycle() -> None:
    inner = _SyncLifecycleStub()
    transport = CircuitBreakerTransport(inner)

    with transport as entered:
        response = entered.handle_request(_request())

    assert entered is transport
    assert response.status_code == 200
    assert inner.exited
    assert inner.closed


def test__sync_transport__wrapped__exposes_inner_transport() -> None:
    inner = _SyncStub(lambda _request: Response(200))
    transport = CircuitBreakerTransport(inner)

    assert transport.wrapped is inner


def test__sync_transport__server_errors__open_breaker_for_host(fake_clock: FakeClock) -> None:
    inner = _SyncStub(lambda _request: Response(503))
    transport = CircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    transport.handle_request(_request())
    transport.handle_request(_request())

    with pytest.raises(CircuitOpenError):
        transport.handle_request(_request())
    assert inner.calls == 2  # the rejected request never reached the wrapped transport


def test__sync_transport__transport_exception__opens_breaker(fake_clock: FakeClock) -> None:
    def boom(_request: Request) -> Response:
        raise httpx2.ConnectError('down')

    inner = _SyncStub(boom)
    transport = CircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    for _ in range(2):
        with pytest.raises(httpx2.ConnectError):
            transport.handle_request(_request())

    with pytest.raises(CircuitOpenError):
        transport.handle_request(_request())


def test__sync_transport__client_errors__never_open(fake_clock: FakeClock) -> None:
    inner = _SyncStub(lambda _request: Response(404))
    transport = CircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    for _ in range(10):
        assert transport.handle_request(_request()).status_code == 404


def test__sync_transport__breakers_isolated_per_host(fake_clock: FakeClock) -> None:
    def handler(request: Request) -> Response:
        return Response(503) if request.url.host == 'bad.example.com' else Response(200)

    inner = _SyncStub(handler)
    transport = CircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    transport.handle_request(_request('https://bad.example.com/'))
    transport.handle_request(_request('https://bad.example.com/'))

    with pytest.raises(CircuitOpenError):
        transport.handle_request(_request('https://bad.example.com/'))
    assert transport.handle_request(_request('https://good.example.com/')).status_code == 200


def test__sync_transport__name_resolver__uses_custom_breaker_key(fake_clock: FakeClock) -> None:
    inner = _SyncStub(lambda _request: Response(200))
    transport = CircuitBreakerTransport(
        inner,
        clock=fake_clock,
        name_resolver=lambda request: request.url.host.removesuffix('.query.consul'),
    )

    transport.handle_request(_request('https://orders.query.consul/v1'))

    assert transport.registry.get_existing('orders') is not None
    assert transport.registry.get_existing('orders.query.consul') is None


def test__sync_transport__empty_resolved_name__raises_before_io(fake_clock: FakeClock) -> None:
    inner = _SyncStub(lambda _request: Response(200))
    transport = CircuitBreakerTransport(
        inner,
        clock=fake_clock,
        name_resolver=lambda _request: ' ',
    )
    request = _request('https://api.example.com/v1')

    with pytest.raises(ValueError, match='empty breaker name') as raised:
        transport.handle_request(request)

    assert str(request.url) in str(raised.value)
    assert inner.calls == 0


@pytest.mark.parametrize('resolved_name', [None, b'orders'])
def test__sync_transport__non_string_resolved_name__raises_before_io(
    fake_clock: FakeClock,
    resolved_name: object,
) -> None:
    inner = _SyncStub(lambda _request: Response(200))
    transport = CircuitBreakerTransport(
        inner,
        clock=fake_clock,
        name_resolver=lambda _request: cast('str', resolved_name),
    )
    request = _request('https://api.example.com/v1')

    with pytest.raises(ValueError, match='non-string breaker name') as raised:
        transport.handle_request(request)

    assert str(request.url) in str(raised.value)
    assert inner.calls == 0


def test__sync_transport__close__releases_wrapped_transport_and_registry(
    mocker: MockerFixture,
) -> None:
    inner = _SyncStub(lambda _request: Response(200))
    transport = CircuitBreakerTransport(inner)
    close_all = mocker.spy(transport.registry, 'close_all')

    transport.close()

    assert inner.closed
    close_all.assert_called_once_with()


def test__sync_transport__injected_registry__closes_only_wrapped_transport(
    mocker: MockerFixture,
) -> None:
    registry = Registry(classifier=HttpStatusClassifier())
    inner = _SyncStub(lambda _request: Response(200))
    transport = CircuitBreakerTransport(inner, registry=registry)
    close_all = mocker.spy(registry, 'close_all')

    transport.close()

    assert transport.registry is registry
    assert inner.closed
    close_all.assert_not_called()


def test__sync_transport__wrapped_close_raises__still_releases_registry(
    mocker: MockerFixture,
) -> None:
    inner = _SyncStub(lambda _request: Response(200))
    transport = CircuitBreakerTransport(inner)
    mocker.patch.object(inner, 'close', side_effect=RuntimeError('close failed'))
    close_all = mocker.spy(transport.registry, 'close_all')

    with pytest.raises(RuntimeError, match='close failed'):
        transport.close()

    close_all.assert_called_once_with()


@pytest.mark.asyncio
async def test__async_transport__success_response__passes_through(fake_clock: FakeClock) -> None:
    inner = _AsyncStub(lambda _request: Response(200))
    transport = AsyncCircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    response = await transport.handle_async_request(_request())

    assert response.status_code == 200


@pytest.mark.asyncio
async def test__async_transport__name_resolver__uses_custom_breaker_key(
    fake_clock: FakeClock,
) -> None:
    inner = _AsyncStub(lambda _request: Response(200))
    transport = AsyncCircuitBreakerTransport(
        inner,
        clock=fake_clock,
        name_resolver=lambda request: request.url.path.split('/')[1],
    )

    await transport.handle_async_request(_request('https://gateway.example.com/orders/1'))

    assert transport.registry.get_existing('orders') is not None
    assert transport.registry.get_existing('gateway.example.com') is None


@pytest.mark.asyncio
async def test__async_transport__cached_breaker__is_not_redecorated_per_request(
    fake_clock: FakeClock, mocker: MockerFixture
) -> None:
    inner = _AsyncStub(lambda _request: Response(200))
    transport = AsyncCircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)
    await transport.handle_async_request(_request())
    mocker.patch.object(CircuitBreaker, '__call__', side_effect=AssertionError('decorated'))

    response = await transport.handle_async_request(_request())

    assert response.status_code == 200


@pytest.mark.asyncio
async def test__async_transport__context_manager__delegates_wrapped_lifecycle() -> None:
    inner = _AsyncLifecycleStub()
    transport = AsyncCircuitBreakerTransport(inner)

    async with transport as entered:
        response = await entered.handle_async_request(_request())

    assert entered is transport
    assert response.status_code == 200
    assert inner.exited
    assert inner.closed


def test__async_transport__wrapped__exposes_inner_transport() -> None:
    inner = _AsyncStub(lambda _request: Response(200))
    transport = AsyncCircuitBreakerTransport(inner)

    assert transport.wrapped is inner


@pytest.mark.asyncio
async def test__async_transport__server_errors__open_breaker_for_host(
    fake_clock: FakeClock,
) -> None:
    inner = _AsyncStub(lambda _request: Response(500))
    transport = AsyncCircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    await transport.handle_async_request(_request())
    await transport.handle_async_request(_request())

    with pytest.raises(CircuitOpenError):
        await transport.handle_async_request(_request())
    assert inner.calls == 2


@pytest.mark.asyncio
async def test__async_transport__aclose__releases_wrapped_transport_and_registry(
    mocker: MockerFixture,
) -> None:
    inner = _AsyncStub(lambda _request: Response(200))
    transport = AsyncCircuitBreakerTransport(inner)
    aclose_all = mocker.spy(transport.registry, 'aclose_all')

    await transport.aclose()

    assert inner.closed
    aclose_all.assert_awaited_once_with()


@pytest.mark.asyncio
async def test__async_transport__injected_registry__closes_only_wrapped_transport(
    mocker: MockerFixture,
) -> None:
    registry = Registry(classifier=HttpStatusClassifier())
    inner = _AsyncStub(lambda _request: Response(200))
    transport = AsyncCircuitBreakerTransport(inner, registry=registry)
    aclose_all = mocker.spy(registry, 'aclose_all')

    await transport.aclose()

    assert transport.registry is registry
    assert inner.closed
    aclose_all.assert_not_awaited()


@pytest.mark.asyncio
async def test__async_transport__wrapped_aclose_raises__still_releases_registry(
    mocker: MockerFixture,
) -> None:
    inner = _AsyncStub(lambda _request: Response(200))
    transport = AsyncCircuitBreakerTransport(inner)
    mocker.patch.object(inner, 'aclose', side_effect=RuntimeError('close failed'))
    aclose_all = mocker.spy(transport.registry, 'aclose_all')

    with pytest.raises(RuntimeError, match='close failed'):
        await transport.aclose()

    aclose_all.assert_awaited_once_with()


@pytest.mark.asyncio
async def test__async_transport__metrics_only_server_errors__records_without_rejecting(
    fake_clock: FakeClock,
    listener: RecordingListener,
) -> None:
    inner = _AsyncStub(lambda _request: Response(503))
    transport = AsyncCircuitBreakerTransport(
        inner,
        initial_state=State.METRICS_ONLY,
        config=_TRIP_FAST,
        clock=fake_clock,
        listener=listener,
    )

    for _ in range(5):
        response = await transport.handle_async_request(_request())
        assert response.status_code == 503

    breaker = transport.registry.get_existing('api.example.com')
    assert breaker is not None
    assert breaker.state is State.METRICS_ONLY
    assert breaker.snapshot().failed_calls == 5
    assert [outcome for outcome, _duration in listener.calls] == [Outcome.FAILURE] * 5


@pytest.mark.asyncio
async def test__async_transport__metrics_only_transport_exception__propagates_and_records(
    fake_clock: FakeClock,
) -> None:
    def boom(_request: Request) -> Response:
        raise httpx2.ConnectError('down')

    transport = AsyncCircuitBreakerTransport(
        _AsyncStub(boom),
        initial_state=State.METRICS_ONLY,
        config=_TRIP_FAST,
        clock=fake_clock,
    )

    with pytest.raises(httpx2.ConnectError, match='down'):
        await transport.handle_async_request(_request())

    breaker = transport.registry.get_existing('api.example.com')
    assert breaker is not None
    assert breaker.snapshot().failed_calls == 1


@pytest.mark.asyncio
async def test__async_transport__metrics_only_new_hosts__all_start_in_shadow_mode(
    fake_clock: FakeClock,
) -> None:
    inner = _AsyncStub(lambda _request: Response(503))
    transport = AsyncCircuitBreakerTransport(
        inner,
        initial_state=State.METRICS_ONLY,
        config=_TRIP_FAST,
        clock=fake_clock,
    )

    for host in ('api-a.example.com', 'api-b.example.com'):
        for _ in range(3):
            response = await transport.handle_async_request(_request(f'https://{host}/'))
            assert response.status_code == 503

        breaker = transport.registry.get_existing(host)
        assert breaker is not None
        assert breaker.state is State.METRICS_ONLY


def test__http_status_classifier__custom_statuses__override_default_set() -> None:
    classifier = HttpStatusClassifier(failure_statuses={404, 408})

    assert classifier.is_failure(result=Response(404), exception=None)
    assert classifier.is_failure(result=Response(408), exception=None)
    assert not classifier.is_failure(result=Response(500), exception=None)


def test__sync_transport__url_without_host__raises_value_error(fake_clock: FakeClock) -> None:
    stub = _SyncStub(lambda _request: Response(200))
    transport = CircuitBreakerTransport(stub, clock=fake_clock)

    with pytest.raises(ValueError, match='no host'):
        transport.handle_request(_request('/relative/path'))

    assert stub.calls == 0


@pytest.mark.asyncio
async def test__async_transport__url_without_host__raises_value_error(
    fake_clock: FakeClock,
) -> None:
    stub = _AsyncStub(lambda _request: Response(200))
    transport = AsyncCircuitBreakerTransport(stub, clock=fake_clock)

    with pytest.raises(ValueError, match='no host'):
        await transport.handle_async_request(_request('/relative/path'))

    assert stub.calls == 0


# --- dialect errors -----------------------------------------------------------


def _tripped_sync_transport(fake_clock: FakeClock) -> CircuitBreakerTransport:
    """A transport whose ``api.example.com`` breaker has just been tripped open."""

    def boom(request: Request) -> Response:
        raise httpx2.ReadError('down', request=request)

    transport = CircuitBreakerTransport(_SyncStub(boom), config=_TRIP_FAST, clock=fake_clock)
    for _ in range(2):
        with pytest.raises(httpx2.ReadError):
            transport.handle_request(_request())

    return transport


def test__sync_transport__open_circuit__rejects_inside_httpx2_hierarchy(
    fake_clock: FakeClock,
) -> None:
    transport = _tripped_sync_transport(fake_clock)

    with pytest.raises(CircuitOpenTransportError) as excinfo:
        transport.handle_request(_request())

    assert isinstance(excinfo.value, httpx2.TransportError)
    assert isinstance(excinfo.value, CircuitOpenError)


def test__sync_transport__open_circuit__stays_outside_retryable_leaf_types(
    fake_clock: FakeClock,
) -> None:
    transport = _tripped_sync_transport(fake_clock)

    with pytest.raises(CircuitOpenTransportError) as excinfo:
        transport.handle_request(_request())

    assert not isinstance(excinfo.value, httpx2.ConnectError)
    assert not isinstance(excinfo.value, httpx2.TimeoutException)


def test__sync_transport__open_circuit__carries_breaker_and_request_context(
    fake_clock: FakeClock,
) -> None:
    transport = _tripped_sync_transport(fake_clock)
    request = _request()

    with pytest.raises(CircuitOpenTransportError) as excinfo:
        transport.handle_request(request)

    error = excinfo.value
    assert error.breaker_name == 'api.example.com'
    assert error.retry_after == _TRIP_FAST.wait_duration_in_open
    assert isinstance(error.last_failure, httpx2.ReadError)
    assert error.request is request
    assert str(error) == f"Circuit 'api.example.com' is open; retry in ~{error.retry_after:.3f}s"
    assert type(error.__cause__) is CircuitOpenError


@pytest.mark.asyncio
async def test__async_transport__open_circuit__rejects_inside_httpx2_hierarchy(
    fake_clock: FakeClock,
) -> None:
    def boom(request: Request) -> Response:
        raise httpx2.ReadError('down', request=request)

    transport = AsyncCircuitBreakerTransport(_AsyncStub(boom), config=_TRIP_FAST, clock=fake_clock)
    for _ in range(2):
        with pytest.raises(httpx2.ReadError):
            await transport.handle_async_request(_request())

    request = _request()
    with pytest.raises(CircuitOpenTransportError) as excinfo:
        await transport.handle_async_request(request)

    error = excinfo.value
    assert isinstance(error, httpx2.TransportError)
    assert isinstance(error, CircuitOpenError)
    assert error.breaker_name == 'api.example.com'
    assert error.request is request


def test__sync_transport__inner_call_timeout__retyped_as_httpx2_timeout(
    fake_clock: FakeClock,
) -> None:
    def boom(_request: Request) -> Response:
        raise CallTimeoutError(1.5)

    transport = CircuitBreakerTransport(_SyncStub(boom), config=_TRIP_FAST, clock=fake_clock)
    request = _request()

    with pytest.raises(CallTimeoutTransportError) as excinfo:
        transport.handle_request(request)

    error = excinfo.value
    assert isinstance(error, httpx2.TimeoutException)
    assert isinstance(error, CallTimeoutError)
    assert error.timeout == 1.5
    assert error.request is request
    assert str(error) == 'Operation exceeded its 1.500s timeout'


def test__sync_transport__inner_bulkhead_rejection__retyped_as_httpx2_pool_timeout(
    fake_clock: FakeClock,
) -> None:
    def boom(_request: Request) -> Response:
        raise BulkheadFullError(4, max_wait=0.25)

    transport = CircuitBreakerTransport(_SyncStub(boom), config=_TRIP_FAST, clock=fake_clock)
    request = _request()

    with pytest.raises(BulkheadFullTransportError) as excinfo:
        transport.handle_request(request)

    error = excinfo.value
    assert isinstance(error, httpx2.PoolTimeout)
    assert isinstance(error, BulkheadFullError)
    assert error.max_concurrent == 4
    assert error.max_wait == 0.25
    assert error.request is request


def test__sync_transport__inner_dialect_error__propagates_unchanged(
    fake_clock: FakeClock,
) -> None:
    rejection = CircuitOpenTransportError('inner', retry_after=1.0)

    def boom(_request: Request) -> Response:
        raise rejection

    transport = CircuitBreakerTransport(_SyncStub(boom), config=_TRIP_FAST, clock=fake_clock)

    with pytest.raises(CircuitOpenTransportError) as excinfo:
        transport.handle_request(_request())

    assert excinfo.value is rejection
    assert excinfo.value.__cause__ is None


@pytest.mark.asyncio
async def test__async_transport__inner_dialect_error__propagates_unchanged(
    fake_clock: FakeClock,
) -> None:
    rejection = CircuitOpenTransportError('inner', retry_after=1.0)

    def boom(_request: Request) -> Response:
        raise rejection

    transport = AsyncCircuitBreakerTransport(_AsyncStub(boom), config=_TRIP_FAST, clock=fake_clock)

    with pytest.raises(CircuitOpenTransportError) as excinfo:
        await transport.handle_async_request(_request())

    assert excinfo.value is rejection


@pytest.mark.parametrize(
    'error',
    [
        CircuitOpenTransportError('api'),
        CallTimeoutTransportError(1.0),
        BulkheadFullTransportError(2),
    ],
)
def test__dialect_errors__request_not_set__keep_httpx2_contract(error: httpx2.HTTPError) -> None:
    with pytest.raises(RuntimeError, match='has not been set'):
        _ = error.request
