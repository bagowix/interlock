"""Benchmarks for the resilience pipeline.

A pipeline stacks strategies around one call, so its overhead is what a caller
pays on every request that goes through the composed policy. Both the happy path
and the substitution path (fallback over a rejecting breaker) are measured, sync
and async.
"""

import asyncio
from collections.abc import Iterator

import pytest
from pytest_codspeed import BenchmarkFixture

from interlock import (
    BulkheadStrategy,
    CircuitBreaker,
    CircuitBreakerStrategy,
    CircuitOpenError,
    Config,
    FallbackStrategy,
    Pipeline,
    PipelineBuilder,
    TimeoutStrategy,
)

_CONFIG = Config(window_size=100, minimum_number_of_calls=10)
_FALLBACK_RESULT = -1


def _work() -> int:
    """The protected payload."""
    return 42


async def _async_work() -> int:
    """The async twin of :func:`_work`."""
    return 42


@pytest.fixture
def runner() -> Iterator[asyncio.Runner]:
    """An event-loop runner reused across iterations, so loop setup is excluded."""
    with asyncio.Runner() as loop_runner:
        yield loop_runner


def _build_sync_pipeline(breaker: CircuitBreaker) -> Pipeline:
    """Fallback over a bulkhead-limited breaker: the common sync composition."""
    return (
        PipelineBuilder()
        .fallback(lambda _exc: _FALLBACK_RESULT, on=(CircuitOpenError,))
        .bulkhead(10)
        .circuit_breaker(breaker)
        .build()
    )


def test_pipeline_breaker_only(benchmark: BenchmarkFixture) -> None:
    """A single-strategy pipeline: the layering overhead over a bare breaker."""
    breaker = CircuitBreaker(name='bench-pipeline-breaker', config=_CONFIG)
    pipeline = Pipeline(CircuitBreakerStrategy(breaker))

    benchmark(lambda: pipeline.call(_work))


def test_pipeline_sync_success(benchmark: BenchmarkFixture) -> None:
    """Fallback + bulkhead + breaker on the happy path."""
    breaker = CircuitBreaker(name='bench-pipeline-sync', config=_CONFIG)
    pipeline = _build_sync_pipeline(breaker)

    benchmark(lambda: pipeline.call(_work))


def test_pipeline_sync_fallback(benchmark: BenchmarkFixture) -> None:
    """The substitution path: an open breaker rejects, the fallback answers."""
    breaker = CircuitBreaker(name='bench-pipeline-fallback', config=_CONFIG)
    breaker.force_open()
    pipeline = _build_sync_pipeline(breaker)

    assert pipeline.call(_work) == _FALLBACK_RESULT
    benchmark(lambda: pipeline.call(_work))


def test_pipeline_decorator(benchmark: BenchmarkFixture) -> None:
    """The decorated pipeline: one wrapper resolved per call."""
    breaker = CircuitBreaker(name='bench-pipeline-decorated', config=_CONFIG)
    guarded = _build_sync_pipeline(breaker)(_work)

    benchmark(guarded)


def test_pipeline_async_success(benchmark: BenchmarkFixture, runner: asyncio.Runner) -> None:
    """Timeout + bulkhead + breaker on the async happy path."""
    breaker = CircuitBreaker(name='bench-pipeline-async', config=_CONFIG)
    pipeline = Pipeline(
        FallbackStrategy(lambda _exc: _FALLBACK_RESULT, on=(CircuitOpenError,)),
        TimeoutStrategy(5.0),
        BulkheadStrategy(10),
        CircuitBreakerStrategy(breaker),
    )

    async def run() -> int:
        return await pipeline.call(_async_work)

    benchmark(lambda: runner.run(run()))
