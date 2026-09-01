from collections.abc import AsyncIterator, Callable, Iterator
from types import TracebackType
from typing import Self, cast

import httpx
import pytest
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
from interlock.integrations.httpx import (
    AsyncCircuitBreakerTransport,
    BulkheadFullTransportError,
    CallTimeoutTransportError,
    CircuitBreakerTransport,
    CircuitOpenTransportError,
    HttpStatusClassifier,
)

_TRIP_FAST = Config(minimum_number_of_calls=2, failure_rate_threshold=0.5)


class _SyncStream(httpx.SyncByteStream):
    def __iter__(self) -> Iterator[bytes]:
        yield b'streamed'


class _AsyncStream(httpx.AsyncByteStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b'streamed'


class _SyncStub(httpx.BaseTransport):
    def __init__(self, handler: Callable[[httpx.Request], httpx.Response]) -> None:
        self._handler = handler
        self.calls = 0
        self.closed = False

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        return self._handler(request)

    def close(self) -> None:
        self.closed = True


class _AsyncStub(httpx.AsyncBaseTransport):
    def __init__(self, handler: Callable[[httpx.Request], httpx.Response]) -> None:
        self._handler = handler
        self.calls = 0
        self.closed = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
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

    def _handle(self, _request: httpx.Request) -> httpx.Response:
        if not self.entered:
            raise RuntimeError('transport was not entered')
        return httpx.Response(200)


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

    def _handle(self, _request: httpx.Request) -> httpx.Response:
        if not self.entered:
            raise RuntimeError('transport was not entered')
        return httpx.Response(200)


def _request(url: str = 'https://api.example.com/v1') -> httpx.Request:
    return httpx.Request('GET', url)


def test__http_status_classifier__exception__is_failure() -> None:
    classifier = HttpStatusClassifier()

    assert classifier.is_failure(result=None, exception=httpx.ConnectError('boom'))


@pytest.mark.parametrize('status', [429, 500, 502, 503, 504])
def test__http_status_classifier__retryable_status__is_failure(status: int) -> None:
    classifier = HttpStatusClassifier()

    assert classifier.is_failure(result=httpx.Response(status), exception=None)


@pytest.mark.parametrize('status', [200, 301, 400, 404, 418, 501, 505])
def test__http_status_classifier__healthy_or_permanent_status__is_success(status: int) -> None:
    classifier = HttpStatusClassifier()

    assert not classifier.is_failure(result=httpx.Response(status), exception=None)


def test__http_status_classifier__custom_statuses__override_default_set() -> None:
    classifier = HttpStatusClassifier(failure_statuses={404, 408})

    assert classifier.is_failure(result=httpx.Response(404), exception=None)
    assert classifier.is_failure(result=httpx.Response(408), exception=None)
    assert not classifier.is_failure(result=httpx.Response(500), exception=None)


@pytest.mark.parametrize(
    'exception',
    [httpx.UnsupportedProtocol('no scheme'), httpx.LocalProtocolError('bad request')],
)
def test__http_status_classifier__caller_side_exception__is_success(
    exception: httpx.TransportError,
) -> None:
    classifier = HttpStatusClassifier()

    assert not classifier.is_failure(result=None, exception=exception)


def test__http_status_classifier__pool_timeout__is_failure() -> None:
    classifier = HttpStatusClassifier()

    assert classifier.is_failure(result=None, exception=httpx.PoolTimeout('exhausted'))


def test__http_status_classifier__custom_exclusions__override_default_set() -> None:
    classifier = HttpStatusClassifier(excluded_exceptions=(httpx.PoolTimeout,))

    assert not classifier.is_failure(result=None, exception=httpx.PoolTimeout('exhausted'))
    assert classifier.is_failure(result=None, exception=httpx.UnsupportedProtocol('no scheme'))


def test__http_status_classifier__empty_exclusions__counts_every_exception() -> None:
    classifier = HttpStatusClassifier(excluded_exceptions=())

    assert classifier.is_failure(result=None, exception=httpx.UnsupportedProtocol('no scheme'))


@pytest.mark.parametrize('entry', ['httpx.PoolTimeout', str, KeyboardInterrupt])
def test__http_status_classifier__non_exception_exclusion__raises(entry: object) -> None:
    with pytest.raises(TypeError, match='Exception subclasses'):
        HttpStatusClassifier(excluded_exceptions=[cast('type[Exception]', entry)])


def test__sync_transport__caller_side_exceptions__keep_circuit_closed(
    fake_clock: FakeClock,
) -> None:
    def unsupported(_request: httpx.Request) -> httpx.Response:
        raise httpx.UnsupportedProtocol('no scheme')

    inner = _SyncStub(unsupported)
    transport = CircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    for _ in range(4):
        with pytest.raises(httpx.UnsupportedProtocol):
            transport.handle_request(_request())

    assert transport.registry.get('api.example.com').state is State.CLOSED


def test__sync_transport__success_response__passes_through(fake_clock: FakeClock) -> None:
    inner = _SyncStub(lambda _request: httpx.Response(200))
    transport = CircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    response = transport.handle_request(_request())

    assert response.status_code == 200


def test__sync_transport__cached_breaker__is_not_redecorated_per_request(
    fake_clock: FakeClock, mocker: MockerFixture
) -> None:
    """The per-request path costs no decoration: no ``functools.wraps``, no detection."""
    inner = _SyncStub(lambda _request: httpx.Response(200))
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
    inner = _SyncStub(lambda _request: httpx.Response(200))
    transport = CircuitBreakerTransport(inner)

    assert transport.wrapped is inner


def test__sync_transport__streaming_response__preserves_stream(fake_clock: FakeClock) -> None:
    response = httpx.Response(200, stream=_SyncStream())
    inner = _SyncStub(lambda _request: response)
    transport = CircuitBreakerTransport(inner, clock=fake_clock)

    returned = transport.handle_request(_request())

    assert returned is response
    assert b''.join(returned.iter_raw()) == b'streamed'


def test__sync_transport__server_errors__open_breaker_for_host(fake_clock: FakeClock) -> None:
    inner = _SyncStub(lambda _request: httpx.Response(503))
    transport = CircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    transport.handle_request(_request())
    transport.handle_request(_request())

    with pytest.raises(CircuitOpenError):
        transport.handle_request(_request())
    assert inner.calls == 2


def test__sync_transport__transport_exception__opens_breaker(fake_clock: FakeClock) -> None:
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError('down', request=request)

    inner = _SyncStub(boom)
    transport = CircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    for _ in range(2):
        with pytest.raises(httpx.ConnectError):
            transport.handle_request(_request())

    with pytest.raises(CircuitOpenError):
        transport.handle_request(_request())


def test__sync_transport__client_errors__never_open(fake_clock: FakeClock) -> None:
    inner = _SyncStub(lambda _request: httpx.Response(404))
    transport = CircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    for _ in range(10):
        assert transport.handle_request(_request()).status_code == 404


def test__sync_transport__custom_classifier__uses_configured_statuses(
    fake_clock: FakeClock,
) -> None:
    inner = _SyncStub(lambda _request: httpx.Response(404))
    classifier = HttpStatusClassifier(failure_statuses={404})
    transport = CircuitBreakerTransport(
        inner,
        config=_TRIP_FAST,
        clock=fake_clock,
        classifier=classifier,
    )

    transport.handle_request(_request())
    transport.handle_request(_request())

    with pytest.raises(CircuitOpenError):
        transport.handle_request(_request())


def test__sync_transport__breakers_isolated_per_host(fake_clock: FakeClock) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503) if request.url.host == 'bad.example.com' else httpx.Response(200)

    inner = _SyncStub(handler)
    transport = CircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    transport.handle_request(_request('https://bad.example.com/'))
    transport.handle_request(_request('https://bad.example.com/'))

    with pytest.raises(CircuitOpenError):
        transport.handle_request(_request('https://bad.example.com/'))
    assert transport.handle_request(_request('https://good.example.com/')).status_code == 200


def test__sync_transport__name_resolver__collapses_hosts_onto_one_breaker(
    fake_clock: FakeClock,
) -> None:
    inner = _SyncStub(lambda _request: httpx.Response(503))
    transport = CircuitBreakerTransport(
        inner,
        config=_TRIP_FAST,
        clock=fake_clock,
        name_resolver=lambda request: request.url.host.removesuffix('.query.consul'),
    )

    transport.handle_request(_request('https://orders.query.consul/v1'))
    transport.handle_request(_request('https://orders/v2'))

    with pytest.raises(CircuitOpenError):
        transport.handle_request(_request('https://orders.query.consul/v3'))
    breaker = transport.registry.get_existing('orders')
    assert breaker is not None
    assert breaker.snapshot().failed_calls == 2
    assert transport.registry.get_existing('orders.query.consul') is None


def test__sync_transport__name_resolver__aligns_listener_name(
    fake_clock: FakeClock,
    listener: RecordingListener,
) -> None:
    transport = CircuitBreakerTransport(
        _SyncStub(lambda _request: httpx.Response(200)),
        clock=fake_clock,
        listener=listener,
        name_resolver=lambda _request: 'orders',
    )

    transport.handle_request(_request())

    assert listener.names == ['orders']


@pytest.mark.parametrize('resolved_name', ['', '   '])
def test__sync_transport__empty_resolved_name__raises_before_io(
    fake_clock: FakeClock,
    mocker: MockerFixture,
    resolved_name: str,
) -> None:
    inner = _SyncStub(lambda _request: httpx.Response(200))
    transport = CircuitBreakerTransport(
        inner,
        clock=fake_clock,
        name_resolver=lambda _request: resolved_name,
    )
    registry_get = mocker.spy(transport.registry, 'get')
    request = _request('https://api.example.com/v1')

    with pytest.raises(ValueError, match='empty breaker name') as raised:
        transport.handle_request(request)

    assert str(request.url) in str(raised.value)
    registry_get.assert_not_called()
    assert inner.calls == 0


@pytest.mark.parametrize('resolved_name', [None, b'orders'])
def test__sync_transport__non_string_resolved_name__raises_before_io(
    fake_clock: FakeClock,
    mocker: MockerFixture,
    resolved_name: object,
) -> None:
    inner = _SyncStub(lambda _request: httpx.Response(200))
    transport = CircuitBreakerTransport(
        inner,
        clock=fake_clock,
        name_resolver=lambda _request: cast('str', resolved_name),
    )
    registry_get = mocker.spy(transport.registry, 'get')
    request = _request('https://api.example.com/v1')

    with pytest.raises(ValueError, match='non-string breaker name') as raised:
        transport.handle_request(request)

    assert str(request.url) in str(raised.value)
    registry_get.assert_not_called()
    assert inner.calls == 0


def test__sync_transports__shared_registry__merge_window(
    fake_clock: FakeClock,
) -> None:
    registry = Registry(
        config=_TRIP_FAST,
        clock=fake_clock,
        classifier=HttpStatusClassifier(),
    )
    first_inner = _SyncStub(lambda _request: httpx.Response(503))
    second_inner = _SyncStub(lambda _request: httpx.Response(503))
    first = CircuitBreakerTransport(first_inner, registry=registry)
    second = CircuitBreakerTransport(second_inner, registry=registry)

    first.handle_request(_request())
    first_breaker = first.registry.get_existing('api.example.com')
    second.handle_request(_request())
    second_breaker = second.registry.get_existing('api.example.com')

    assert first.registry is registry
    assert second.registry is registry
    assert first_breaker is not None
    assert first_breaker is second_breaker
    assert first_breaker.snapshot().failed_calls == 2
    with pytest.raises(CircuitOpenError):
        first.handle_request(_request())


def test__sync_transport__injected_registry__closes_only_wrapped_transport(
    mocker: MockerFixture,
) -> None:
    registry = Registry(classifier=HttpStatusClassifier())
    inner = _SyncStub(lambda _request: httpx.Response(200))
    transport = CircuitBreakerTransport(inner, registry=registry)
    close_all = mocker.spy(registry, 'close_all')

    transport.close()

    assert inner.closed
    close_all.assert_not_called()


def test__sync_transport__registry_with_builder_options__raises_all_conflicts(
    fake_clock: FakeClock,
    listener: RecordingListener,
) -> None:
    registry = Registry()

    with pytest.raises(ValueError, match='registry') as raised:
        CircuitBreakerTransport(
            _SyncStub(lambda _request: httpx.Response(200)),
            registry=registry,
            config=_TRIP_FAST,
            clock=fake_clock,
            initial_state=State.CLOSED,
            classifier=HttpStatusClassifier(),
            listener=listener,
        )

    message = str(raised.value)
    for name in ('config', 'clock', 'initial_state', 'classifier', 'listener'):
        assert name in message


def test__sync_transport__url_without_host__raises_value_error(fake_clock: FakeClock) -> None:
    inner = _SyncStub(lambda _request: httpx.Response(200))
    transport = CircuitBreakerTransport(inner, clock=fake_clock)

    with pytest.raises(ValueError, match='no host'):
        transport.handle_request(_request('/relative/path'))

    assert inner.calls == 0


def test__sync_transport__close__releases_wrapped_transport_and_registry(
    mocker: MockerFixture,
) -> None:
    inner = _SyncStub(lambda _request: httpx.Response(200))
    transport = CircuitBreakerTransport(inner)
    close_all = mocker.spy(transport.registry, 'close_all')

    transport.close()

    assert inner.closed
    close_all.assert_called_once_with()


def test__sync_transport__wrapped_close_raises__still_releases_registry(
    mocker: MockerFixture,
) -> None:
    inner = _SyncStub(lambda _request: httpx.Response(200))
    transport = CircuitBreakerTransport(inner)
    mocker.patch.object(inner, 'close', side_effect=RuntimeError('close failed'))
    close_all = mocker.spy(transport.registry, 'close_all')

    with pytest.raises(RuntimeError, match='close failed'):
        transport.close()

    close_all.assert_called_once_with()


def test__sync_transport__metrics_only_server_errors__records_without_rejecting(
    fake_clock: FakeClock,
    listener: RecordingListener,
) -> None:
    inner = _SyncStub(lambda _request: httpx.Response(503))
    transport = CircuitBreakerTransport(
        inner,
        initial_state=State.METRICS_ONLY,
        config=_TRIP_FAST,
        clock=fake_clock,
        listener=listener,
    )

    for _ in range(5):
        response = transport.handle_request(_request())
        assert response.status_code == 503

    breaker = transport.registry.get_existing('api.example.com')
    assert breaker is not None
    assert breaker.state is State.METRICS_ONLY
    assert breaker.snapshot().failed_calls == 5
    assert [outcome for outcome, _duration in listener.calls] == [Outcome.FAILURE] * 5


@pytest.mark.asyncio
async def test__async_transport__success_response__passes_through(fake_clock: FakeClock) -> None:
    inner = _AsyncStub(lambda _request: httpx.Response(200))
    transport = AsyncCircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    response = await transport.handle_async_request(_request())

    assert response.status_code == 200


@pytest.mark.asyncio
async def test__async_transport__cached_breaker__is_not_redecorated_per_request(
    fake_clock: FakeClock, mocker: MockerFixture
) -> None:
    inner = _AsyncStub(lambda _request: httpx.Response(200))
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
    inner = _AsyncStub(lambda _request: httpx.Response(200))
    transport = AsyncCircuitBreakerTransport(inner)

    assert transport.wrapped is inner


@pytest.mark.asyncio
async def test__async_transport__streaming_response__preserves_stream(
    fake_clock: FakeClock,
) -> None:
    response = httpx.Response(200, stream=_AsyncStream())
    inner = _AsyncStub(lambda _request: response)
    transport = AsyncCircuitBreakerTransport(inner, clock=fake_clock)

    returned = await transport.handle_async_request(_request())

    assert returned is response
    assert b''.join([chunk async for chunk in returned.aiter_raw()]) == b'streamed'


@pytest.mark.asyncio
async def test__async_transport__server_errors__open_breaker_for_host(
    fake_clock: FakeClock,
) -> None:
    inner = _AsyncStub(lambda _request: httpx.Response(500))
    transport = AsyncCircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    await transport.handle_async_request(_request())
    await transport.handle_async_request(_request())

    with pytest.raises(CircuitOpenError):
        await transport.handle_async_request(_request())
    assert inner.calls == 2


@pytest.mark.asyncio
async def test__async_transport__transport_exception__opens_breaker(
    fake_clock: FakeClock,
) -> None:
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError('down', request=request)

    inner = _AsyncStub(boom)
    transport = AsyncCircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    for _ in range(2):
        with pytest.raises(httpx.ConnectError):
            await transport.handle_async_request(_request())

    with pytest.raises(CircuitOpenError):
        await transport.handle_async_request(_request())


@pytest.mark.asyncio
async def test__async_transport__custom_classifier__uses_configured_statuses(
    fake_clock: FakeClock,
) -> None:
    inner = _AsyncStub(lambda _request: httpx.Response(404))
    classifier = HttpStatusClassifier(failure_statuses={404})
    transport = AsyncCircuitBreakerTransport(
        inner,
        config=_TRIP_FAST,
        clock=fake_clock,
        classifier=classifier,
    )

    await transport.handle_async_request(_request())
    await transport.handle_async_request(_request())

    with pytest.raises(CircuitOpenError):
        await transport.handle_async_request(_request())


@pytest.mark.asyncio
async def test__async_transport__breakers_isolated_per_host(fake_clock: FakeClock) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503) if request.url.host == 'bad.example.com' else httpx.Response(200)

    inner = _AsyncStub(handler)
    transport = AsyncCircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    await transport.handle_async_request(_request('https://bad.example.com/'))
    await transport.handle_async_request(_request('https://bad.example.com/'))

    with pytest.raises(CircuitOpenError):
        await transport.handle_async_request(_request('https://bad.example.com/'))
    response = await transport.handle_async_request(_request('https://good.example.com/'))
    assert response.status_code == 200


@pytest.mark.asyncio
async def test__async_transport__name_resolver__splits_host_by_path(
    fake_clock: FakeClock,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return (
            httpx.Response(503) if request.url.path.startswith('/orders') else httpx.Response(200)
        )

    inner = _AsyncStub(handler)
    transport = AsyncCircuitBreakerTransport(
        inner,
        config=_TRIP_FAST,
        clock=fake_clock,
        name_resolver=lambda request: request.url.path.split('/')[1],
    )

    await transport.handle_async_request(_request('https://gateway.example.com/orders/1'))
    await transport.handle_async_request(_request('https://gateway.example.com/orders/2'))

    with pytest.raises(CircuitOpenError):
        await transport.handle_async_request(_request('https://gateway.example.com/orders/3'))
    response = await transport.handle_async_request(
        _request('https://gateway.example.com/payments/1')
    )
    orders = transport.registry.get_existing('orders')
    payments = transport.registry.get_existing('payments')
    assert response.status_code == 200
    assert orders is not None
    assert payments is not None
    assert orders is not payments


@pytest.mark.asyncio
async def test__async_transport__empty_resolved_name__raises_before_io(
    fake_clock: FakeClock,
) -> None:
    inner = _AsyncStub(lambda _request: httpx.Response(200))
    transport = AsyncCircuitBreakerTransport(
        inner,
        clock=fake_clock,
        name_resolver=lambda _request: '',
    )
    request = _request('https://api.example.com/v1')

    with pytest.raises(ValueError, match='empty breaker name') as raised:
        await transport.handle_async_request(request)

    assert str(request.url) in str(raised.value)
    assert inner.calls == 0


@pytest.mark.asyncio
async def test__async_transports__shared_registry__survives_one_transport_closing(
    fake_clock: FakeClock,
    mocker: MockerFixture,
) -> None:
    registry = Registry(clock=fake_clock, classifier=HttpStatusClassifier())
    first_inner = _AsyncStub(lambda _request: httpx.Response(200))
    second_inner = _AsyncStub(lambda _request: httpx.Response(200))
    first = AsyncCircuitBreakerTransport(first_inner, registry=registry)
    second = AsyncCircuitBreakerTransport(second_inner, registry=registry)
    aclose_all = mocker.spy(registry, 'aclose_all')

    await first.handle_async_request(_request())
    await first.aclose()
    response = await second.handle_async_request(_request())

    assert first_inner.closed
    assert response.status_code == 200
    assert second.registry.get_existing('api.example.com') is registry.get_existing(
        'api.example.com'
    )
    aclose_all.assert_not_awaited()


@pytest.mark.asyncio
async def test__async_transport__url_without_host__raises_value_error(
    fake_clock: FakeClock,
) -> None:
    inner = _AsyncStub(lambda _request: httpx.Response(200))
    transport = AsyncCircuitBreakerTransport(inner, clock=fake_clock)

    with pytest.raises(ValueError, match='no host'):
        await transport.handle_async_request(_request('/relative/path'))

    assert inner.calls == 0


@pytest.mark.asyncio
async def test__async_transport__aclose__releases_wrapped_transport_and_registry(
    mocker: MockerFixture,
) -> None:
    inner = _AsyncStub(lambda _request: httpx.Response(200))
    transport = AsyncCircuitBreakerTransport(inner)
    aclose_all = mocker.spy(transport.registry, 'aclose_all')

    await transport.aclose()

    assert inner.closed
    aclose_all.assert_awaited_once_with()


@pytest.mark.asyncio
async def test__async_transport__wrapped_aclose_raises__still_releases_registry(
    mocker: MockerFixture,
) -> None:
    inner = _AsyncStub(lambda _request: httpx.Response(200))
    transport = AsyncCircuitBreakerTransport(inner)
    mocker.patch.object(inner, 'aclose', side_effect=RuntimeError('close failed'))
    aclose_all = mocker.spy(transport.registry, 'aclose_all')

    with pytest.raises(RuntimeError, match='close failed'):
        await transport.aclose()

    aclose_all.assert_awaited_once_with()


@pytest.mark.asyncio
async def test__async_transport__metrics_only_transport_exception__propagates_and_records(
    fake_clock: FakeClock,
) -> None:
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError('down', request=request)

    transport = AsyncCircuitBreakerTransport(
        _AsyncStub(boom),
        initial_state=State.METRICS_ONLY,
        config=_TRIP_FAST,
        clock=fake_clock,
    )

    with pytest.raises(httpx.ConnectError, match='down'):
        await transport.handle_async_request(_request())

    breaker = transport.registry.get_existing('api.example.com')
    assert breaker is not None
    assert breaker.state is State.METRICS_ONLY
    assert breaker.snapshot().failed_calls == 1


@pytest.mark.asyncio
async def test__async_transport__metrics_only_new_hosts__all_start_in_shadow_mode(
    fake_clock: FakeClock,
) -> None:
    inner = _AsyncStub(lambda _request: httpx.Response(503))
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


# --- dialect errors -----------------------------------------------------------


def _tripped_sync_transport(fake_clock: FakeClock) -> CircuitBreakerTransport:
    """A transport whose ``api.example.com`` breaker has just been tripped open."""

    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadError('down', request=request)

    transport = CircuitBreakerTransport(_SyncStub(boom), config=_TRIP_FAST, clock=fake_clock)
    for _ in range(2):
        with pytest.raises(httpx.ReadError):
            transport.handle_request(_request())

    return transport


def _failing_async_transport(fake_clock: FakeClock) -> AsyncCircuitBreakerTransport:
    """An async transport whose inner stub fails every request; the caller trips it."""

    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadError('down', request=request)

    return AsyncCircuitBreakerTransport(_AsyncStub(boom), config=_TRIP_FAST, clock=fake_clock)


def test__sync_transport__open_circuit__rejects_inside_httpx_hierarchy(
    fake_clock: FakeClock,
) -> None:
    transport = _tripped_sync_transport(fake_clock)

    with pytest.raises(CircuitOpenTransportError) as excinfo:
        transport.handle_request(_request())

    assert isinstance(excinfo.value, httpx.TransportError)
    assert isinstance(excinfo.value, CircuitOpenError)


def test__sync_transport__open_circuit__stays_outside_retryable_leaf_types(
    fake_clock: FakeClock,
) -> None:
    transport = _tripped_sync_transport(fake_clock)

    with pytest.raises(CircuitOpenTransportError) as excinfo:
        transport.handle_request(_request())

    assert not isinstance(excinfo.value, httpx.ConnectError)
    assert not isinstance(excinfo.value, httpx.TimeoutException)


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
    assert isinstance(error.last_failure, httpx.ReadError)
    assert error.request is request
    assert str(error) == f"Circuit 'api.example.com' is open; retry in ~{error.retry_after:.3f}s"
    assert type(error.__cause__) is CircuitOpenError


@pytest.mark.asyncio
async def test__async_transport__open_circuit__rejects_inside_httpx_hierarchy(
    fake_clock: FakeClock,
) -> None:
    transport = _failing_async_transport(fake_clock)
    for _ in range(2):
        with pytest.raises(httpx.ReadError):
            await transport.handle_async_request(_request())

    request = _request()
    with pytest.raises(CircuitOpenTransportError) as excinfo:
        await transport.handle_async_request(request)

    error = excinfo.value
    assert isinstance(error, httpx.TransportError)
    assert isinstance(error, CircuitOpenError)
    assert error.breaker_name == 'api.example.com'
    assert error.request is request


def test__sync_transport__inner_call_timeout__retyped_as_httpx_timeout(
    fake_clock: FakeClock,
) -> None:
    def boom(_request: httpx.Request) -> httpx.Response:
        raise CallTimeoutError(1.5)

    transport = CircuitBreakerTransport(_SyncStub(boom), config=_TRIP_FAST, clock=fake_clock)
    request = _request()

    with pytest.raises(CallTimeoutTransportError) as excinfo:
        transport.handle_request(request)

    error = excinfo.value
    assert isinstance(error, httpx.TimeoutException)
    assert isinstance(error, CallTimeoutError)
    assert error.timeout == 1.5
    assert error.request is request
    assert str(error) == 'Operation exceeded its 1.500s timeout'


def test__sync_transport__inner_bulkhead_rejection__retyped_as_httpx_pool_timeout(
    fake_clock: FakeClock,
) -> None:
    def boom(_request: httpx.Request) -> httpx.Response:
        raise BulkheadFullError(4, max_wait=0.25)

    transport = CircuitBreakerTransport(_SyncStub(boom), config=_TRIP_FAST, clock=fake_clock)
    request = _request()

    with pytest.raises(BulkheadFullTransportError) as excinfo:
        transport.handle_request(request)

    error = excinfo.value
    assert isinstance(error, httpx.PoolTimeout)
    assert isinstance(error, BulkheadFullError)
    assert error.max_concurrent == 4
    assert error.max_wait == 0.25
    assert error.request is request


def test__sync_transport__inner_dialect_error__propagates_unchanged(
    fake_clock: FakeClock,
) -> None:
    rejection = CircuitOpenTransportError('inner', retry_after=1.0)

    def boom(_request: httpx.Request) -> httpx.Response:
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

    def boom(_request: httpx.Request) -> httpx.Response:
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
def test__dialect_errors__request_not_set__keep_httpx_contract(error: httpx.HTTPError) -> None:
    with pytest.raises(RuntimeError, match='has not been set'):
        _ = error.request


def test__half_open__pool_timeout_probe__does_not_reopen_the_breaker() -> None:
    """A probe that never got a connection carries no verdict about the host."""
    clock = FakeClock()
    config = Config(
        minimum_number_of_calls=2,
        window_size=10,
        permitted_calls_in_half_open=2,
        max_concurrent_probes=1,
        wait_duration_in_open=5.0,
    )
    responses: list[object] = [
        httpx.Response(500),
        httpx.Response(500),
        httpx.PoolTimeout('no connection available'),
    ]

    def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
        outcome = responses.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    transport = CircuitBreakerTransport(httpx.MockTransport(handler), config=config, clock=clock)
    with httpx.Client(transport=transport) as client:
        for _ in range(2):
            client.get('https://service.test/path')
        clock.advance(5.0)
        with pytest.raises(httpx.PoolTimeout):
            client.get('https://service.test/path')

    assert transport.registry.get('service.test').state is State.HALF_OPEN
