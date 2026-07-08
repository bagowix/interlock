"""Tests for the FastAPI integration (``interlock.integrations.fastapi``)."""

import json
from typing import Annotated

import httpx2
import pytest
from fastapi import Depends, FastAPI, Request

from interlock import CircuitBreaker, CircuitOpenError, Config, Registry
from interlock.integrations.fastapi import (
    breaker_dependency,
    circuit_open_handler,
    install_exception_handler,
)


def _request() -> Request:
    return Request({'type': 'http', 'method': 'GET', 'path': '/', 'headers': []})


def test__circuit_open_handler__with_retry_after__returns_503_and_header() -> None:
    exc = CircuitOpenError('payments', retry_after=2.2)

    response = circuit_open_handler(_request(), exc)

    assert response.status_code == 503
    assert response.headers['retry-after'] == '3'  # ceil(2.2)
    assert json.loads(response.body)['detail'] == "Circuit 'payments' is open"


def test__circuit_open_handler__without_retry_after__omits_header() -> None:
    exc = CircuitOpenError('payments', retry_after=None)

    response = circuit_open_handler(_request(), exc)

    assert response.status_code == 503
    assert 'retry-after' not in response.headers


def test__breaker_dependency__returns_shared_breaker_from_registry() -> None:
    registry = Registry()
    dependency = breaker_dependency('orders', registry=registry)

    first = dependency()
    second = dependency()

    assert isinstance(first, CircuitBreaker)
    assert first is second  # the registry caches one breaker per name


def _build_app() -> FastAPI:
    registry = Registry(
        config=Config(minimum_number_of_calls=1, window_size=10, wait_duration_in_open=30.0)
    )
    app = FastAPI()
    install_exception_handler(app)
    guarded = breaker_dependency('downstream', registry=registry)

    @app.get('/ok')
    async def ok(breaker: Annotated[CircuitBreaker, Depends(guarded)]) -> dict[str, str]:
        async def healthy() -> str:
            return 'pong'

        return {'result': await breaker.call(healthy)}

    @app.get('/down')
    async def down(breaker: Annotated[CircuitBreaker, Depends(guarded)]) -> dict[str, str]:
        async def failing() -> str:
            raise RuntimeError('downstream is down')

        return {'result': await breaker.call(failing)}

    return app


@pytest.mark.asyncio
async def test__route__healthy_dependency__returns_200() -> None:
    transport = httpx2.ASGITransport(app=_build_app())
    async with httpx2.AsyncClient(transport=transport, base_url='http://test') as client:
        response = await client.get('/ok')

    assert response.status_code == 200
    assert response.json() == {'result': 'pong'}


@pytest.mark.asyncio
async def test__route__open_circuit__returns_503_with_retry_after() -> None:
    transport = httpx2.ASGITransport(app=_build_app(), raise_app_exceptions=False)
    async with httpx2.AsyncClient(transport=transport, base_url='http://test') as client:
        first = await client.get('/down')  # executes, fails, trips the breaker
        assert first.status_code == 500

        second = await client.get('/down')  # breaker now open → short-circuits

    assert second.status_code == 503
    assert int(second.headers['retry-after']) >= 1
    assert second.json()['detail'] == "Circuit 'downstream' is open"
