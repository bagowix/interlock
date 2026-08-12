from collections.abc import AsyncIterator, Callable, Iterator
from types import TracebackType
from typing import Self, cast

import httpx
import pytest
from pytest_mock import MockerFixture
from tests.conftest import FakeClock, RecordingListener

from interlock import CircuitOpenError, Config, Outcome, Registry, State
from interlock.integrations.httpx import (
    AsyncCircuitBreakerTransport,
    CircuitBreakerTransport,
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


def test__sync_transport__context_manager__delegates_wrapped_lifecycle() -> None:
    inner = _SyncLifecycleStub()
    transport = CircuitBreakerTransport(inner)

    with transport as entered:
        response = entered.handle_request(_request())

    assert entered is transport
    assert response.status_code == 200
    assert inner.exited
    assert inner.closed


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
async def test__async_transport__context_manager__delegates_wrapped_lifecycle() -> None:
    inner = _AsyncLifecycleStub()
    transport = AsyncCircuitBreakerTransport(inner)

    async with transport as entered:
        response = await entered.handle_async_request(_request())

    assert entered is transport
    assert response.status_code == 200
    assert inner.exited
    assert inner.closed


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
