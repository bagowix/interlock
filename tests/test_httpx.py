from collections.abc import AsyncIterator, Callable, Iterator

import httpx
import pytest
from tests.conftest import FakeClock

from interlock import CircuitOpenError, Config
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


def test__sync_transport__success_response__passes_through(fake_clock: FakeClock) -> None:
    inner = _SyncStub(lambda _request: httpx.Response(200))
    transport = CircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    response = transport.handle_request(_request())

    assert response.status_code == 200


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


def test__sync_transport__url_without_host__raises_value_error(fake_clock: FakeClock) -> None:
    inner = _SyncStub(lambda _request: httpx.Response(200))
    transport = CircuitBreakerTransport(inner, clock=fake_clock)

    with pytest.raises(ValueError, match='no host'):
        transport.handle_request(_request('/relative/path'))

    assert inner.calls == 0


def test__sync_transport__close__delegates_to_wrapped() -> None:
    inner = _SyncStub(lambda _request: httpx.Response(200))
    transport = CircuitBreakerTransport(inner)

    transport.close()

    assert inner.closed


@pytest.mark.asyncio
async def test__async_transport__success_response__passes_through(fake_clock: FakeClock) -> None:
    inner = _AsyncStub(lambda _request: httpx.Response(200))
    transport = AsyncCircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    response = await transport.handle_async_request(_request())

    assert response.status_code == 200


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
async def test__async_transport__url_without_host__raises_value_error(
    fake_clock: FakeClock,
) -> None:
    inner = _AsyncStub(lambda _request: httpx.Response(200))
    transport = AsyncCircuitBreakerTransport(inner, clock=fake_clock)

    with pytest.raises(ValueError, match='no host'):
        await transport.handle_async_request(_request('/relative/path'))

    assert inner.calls == 0


@pytest.mark.asyncio
async def test__async_transport__aclose__delegates_to_wrapped() -> None:
    inner = _AsyncStub(lambda _request: httpx.Response(200))
    transport = AsyncCircuitBreakerTransport(inner)

    await transport.aclose()

    assert inner.closed
