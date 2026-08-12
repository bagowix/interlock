"""Benchmarks for the transport-integration hot path.

Every outgoing request of a wrapped client pays the integration's own overhead:
resolving the breaker name, looking the breaker up in the registry and running
the request under it. The wrapped transport here answers from memory, so what is
measured is that per-request wrapper rather than a socket.
"""

import asyncio
from collections.abc import Iterator

import httpx
import pytest
from pytest_codspeed import BenchmarkFixture

from interlock import Config
from interlock.integrations.httpx import AsyncCircuitBreakerTransport, CircuitBreakerTransport

_CONFIG = Config(window_size=100, minimum_number_of_calls=10)
_REQUEST = httpx.Request('GET', 'https://api.example.com/orders')
_OK = 200


class _StubTransport(httpx.BaseTransport):
    """The wrapped dependency: cheap enough to expose the wrapper's own cost."""

    def handle_request(self, _request: httpx.Request) -> httpx.Response:
        """Answer from memory, the way a healthy dependency would."""
        return httpx.Response(_OK)


class _AsyncStubTransport(httpx.AsyncBaseTransport):
    """The async twin of :class:`_StubTransport`."""

    async def handle_async_request(self, _request: httpx.Request) -> httpx.Response:
        """Answer from memory, the way a healthy dependency would."""
        return httpx.Response(_OK)


@pytest.fixture
def runner() -> Iterator[asyncio.Runner]:
    """An event-loop runner reused across iterations, so loop setup is excluded."""
    with asyncio.Runner() as loop_runner:
        yield loop_runner


def test_baseline_unwrapped_transport(benchmark: BenchmarkFixture) -> None:
    """The bare transport: the denominator for the wrapper's overhead ratio."""
    transport = _StubTransport()

    benchmark(lambda: transport.handle_request(_REQUEST))


def test_transport_request(benchmark: BenchmarkFixture) -> None:
    """A guarded request on a closed breaker: name, registry lookup, protected call."""
    transport = CircuitBreakerTransport(_StubTransport(), config=_CONFIG)

    response = benchmark(lambda: transport.handle_request(_REQUEST))

    assert response.status_code == _OK


def test_baseline_unwrapped_transport_async(
    benchmark: BenchmarkFixture, runner: asyncio.Runner
) -> None:
    """The bare async transport, behind the same coroutine frame the guarded path pays."""
    transport = _AsyncStubTransport()

    async def bare() -> httpx.Response:
        return await transport.handle_async_request(_REQUEST)

    benchmark(lambda: runner.run(bare()))


def test_transport_request_async(benchmark: BenchmarkFixture, runner: asyncio.Runner) -> None:
    """The async twin of :func:`test_transport_request`."""
    transport = AsyncCircuitBreakerTransport(_AsyncStubTransport(), config=_CONFIG)

    async def guarded() -> httpx.Response:
        return await transport.handle_async_request(_REQUEST)

    response = benchmark(lambda: runner.run(guarded()))

    assert response.status_code == _OK
