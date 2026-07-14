from collections.abc import Callable

import httpx2
import pytest
from httpx2 import AsyncBaseTransport, BaseTransport, Request, Response
from tests.conftest import FakeClock

from interlock import CircuitOpenError, Config
from interlock.integrations.httpx2 import (
    AsyncCircuitBreakerTransport,
    CircuitBreakerTransport,
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


def test__sync_transport__success_response__passes_through(fake_clock: FakeClock) -> None:
    inner = _SyncStub(lambda _request: Response(200))
    transport = CircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    response = transport.handle_request(_request())

    assert response.status_code == 200


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


def test__sync_transport__close__delegates_to_wrapped() -> None:
    inner = _SyncStub(lambda _request: Response(200))
    transport = CircuitBreakerTransport(inner)

    transport.close()

    assert inner.closed


@pytest.mark.asyncio
async def test__async_transport__success_response__passes_through(fake_clock: FakeClock) -> None:
    inner = _AsyncStub(lambda _request: Response(200))
    transport = AsyncCircuitBreakerTransport(inner, config=_TRIP_FAST, clock=fake_clock)

    response = await transport.handle_async_request(_request())

    assert response.status_code == 200


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
async def test__async_transport__aclose__delegates_to_wrapped() -> None:
    inner = _AsyncStub(lambda _request: Response(200))
    transport = AsyncCircuitBreakerTransport(inner)

    await transport.aclose()

    assert inner.closed


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
