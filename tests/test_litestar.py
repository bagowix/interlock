"""Tests for the Litestar integration (``interlock.integrations.litestar``)."""

from litestar import get
from litestar.di import NamedDependency, Provide
from litestar.testing import RequestFactory, TestClient, create_test_client

from interlock import CircuitBreaker, CircuitOpenError, Config, Registry
from interlock.integrations.litestar import breaker_dependency, circuit_open_handler


def test__circuit_open_handler__with_retry_after__returns_503_and_header() -> None:
    exc = CircuitOpenError('payments', retry_after=2.2)

    response = circuit_open_handler(RequestFactory().get('/'), exc)

    assert response.status_code == 503
    assert response.headers['Retry-After'] == '3'  # ceil(2.2)
    assert response.content == {'detail': "Circuit 'payments' is open"}


def test__circuit_open_handler__without_retry_after__omits_header() -> None:
    exc = CircuitOpenError('payments', retry_after=None)

    response = circuit_open_handler(RequestFactory().get('/'), exc)

    assert response.status_code == 503
    assert 'Retry-After' not in response.headers


def test__breaker_dependency__returns_a_provide_with_the_shared_breaker() -> None:
    registry = Registry()
    provided = breaker_dependency('orders', registry=registry)

    assert isinstance(provided, Provide)
    first = provided.dependency()
    second = provided.dependency()
    assert isinstance(first, CircuitBreaker)
    assert first is second  # the registry caches one breaker per name


def _client() -> TestClient:
    registry = Registry(
        config=Config(minimum_number_of_calls=1, window_size=10, wait_duration_in_open=30.0)
    )

    @get('/ok', sync_to_thread=False)
    def ok(breaker: NamedDependency[CircuitBreaker]) -> dict[str, str]:
        return {'result': breaker.call(lambda: 'fine')}

    @get('/fail', sync_to_thread=False)
    def fail(breaker: NamedDependency[CircuitBreaker]) -> None:
        def boom() -> None:
            raise RuntimeError('downstream down')

        breaker.call(boom)

    return create_test_client(
        route_handlers=[ok, fail],
        dependencies={'breaker': breaker_dependency('downstream', registry=registry)},
        exception_handlers={CircuitOpenError: circuit_open_handler},
        raise_server_exceptions=False,
    )


def test__e2e__healthy_route__passes_through() -> None:
    with _client() as client:
        response = client.get('/ok')

    assert response.status_code == 200
    assert response.json() == {'result': 'fine'}


def test__e2e__tripped_breaker__responds_503_with_retry_after() -> None:
    with _client() as client:
        assert client.get('/fail').status_code == 500  # the failure itself, recorded
        rejected = client.get('/fail')  # now the circuit is open

        assert rejected.status_code == 503
        assert rejected.headers['Retry-After'] == '30'
        assert rejected.json() == {'detail': "Circuit 'downstream' is open"}


def test__e2e__open_circuit__rejects_every_route_sharing_the_breaker() -> None:
    with _client() as client:
        client.get('/fail')  # trip the shared 'downstream' breaker

        response = client.get('/ok')  # same breaker name -> also rejected

        assert response.status_code == 503
        assert response.json() == {'detail': "Circuit 'downstream' is open"}
