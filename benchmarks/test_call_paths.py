"""Benchmarks for the protected-call hot paths of ``CircuitBreaker``.

Every protected call pays the breaker's overhead twice: once on admission and
once on settling the outcome into the sliding window. These benchmarks isolate
that overhead by protecting a trivial callable, so what is measured is the
breaker itself rather than the work it guards.
"""

import asyncio
from collections.abc import Iterator

import pytest
from pytest_codspeed import BenchmarkFixture

from interlock import CircuitBreaker, CircuitOpenError, Config, WindowType

_CLOSED_CONFIG = Config(window_size=100, minimum_number_of_calls=10)
_TIME_BASED_CONFIG = Config(window_type=WindowType.TIME_BASED, window_size=60)


def _work(left: int, right: int) -> int:
    """The protected payload: cheap enough to expose the breaker's own cost."""
    return left + right


async def _async_work(left: int, right: int) -> int:
    """The async twin of :func:`_work`."""
    return left + right


def _boom() -> None:
    """Fail the way a broken dependency would."""
    raise RuntimeError('dependency down')


@pytest.fixture
def runner() -> Iterator[asyncio.Runner]:
    """An event-loop runner reused across iterations, so loop setup is excluded."""
    with asyncio.Runner() as loop_runner:
        yield loop_runner


def test_baseline_direct_call(benchmark: BenchmarkFixture) -> None:
    """The unwrapped sync callable: the denominator for every overhead ratio."""
    benchmark(lambda: _work(1, 2))


def test_baseline_direct_call_async(benchmark: BenchmarkFixture, runner: asyncio.Runner) -> None:
    """The unwrapped coroutine, behind the same wrapper frame the guarded paths pay.

    Real callers await the protected work from inside their own coroutine either
    way, so the baseline wraps ``_async_work`` exactly like ``test_call_async``
    wraps ``breaker.call`` — the ratio then isolates the breaker, not a frame.
    """

    async def bare() -> int:
        return await _async_work(1, 2)

    benchmark(lambda: runner.run(bare()))


def test_call_sync(benchmark: BenchmarkFixture) -> None:
    """``call`` on a closed breaker: detect, admit, run, classify, record."""
    breaker = CircuitBreaker(name='bench-call-sync', config=_CLOSED_CONFIG)
    benchmark(lambda: breaker.call(_work, 1, 2))


def test_call_sync_time_based_window(benchmark: BenchmarkFixture) -> None:
    """The same path against a time-based window (bucketed, clock-driven)."""
    breaker = CircuitBreaker(name='bench-call-time', config=_TIME_BASED_CONFIG)
    benchmark(lambda: breaker.call(_work, 1, 2))


def test_decorated_sync(benchmark: BenchmarkFixture) -> None:
    """The decorator path: what most callers actually use in production."""
    breaker = CircuitBreaker(name='bench-decorated-sync', config=_CLOSED_CONFIG)
    guarded = breaker(_work)
    benchmark(lambda: guarded(1, 2))


def test_context_manager_sync(benchmark: BenchmarkFixture) -> None:
    """The guarded-block path, including the ContextVar bookkeeping."""
    breaker = CircuitBreaker(name='bench-block-sync', config=_CLOSED_CONFIG)

    def guarded_block() -> int:
        with breaker:
            return _work(1, 2)

    benchmark(guarded_block)


def test_call_failure_recorded(benchmark: BenchmarkFixture) -> None:
    """The failure path: classification plus a failure recorded in the window.

    Shadow mode keeps the breaker from tripping, so every iteration measures the
    same work instead of degrading into the rejection path.
    """
    breaker = CircuitBreaker(name='bench-failure', config=_CLOSED_CONFIG)
    breaker.metrics_only()

    def failing_call() -> None:
        try:
            breaker.call(_boom)
        except RuntimeError:
            pass

    benchmark(failing_call)


def test_call_rejected(benchmark: BenchmarkFixture) -> None:
    """The rejection fast path: an open breaker short-circuiting the call."""
    breaker = CircuitBreaker(name='bench-rejected', config=_CLOSED_CONFIG)
    breaker.force_open()

    def rejected_call() -> None:
        try:
            breaker.call(_work, 1, 2)
        except CircuitOpenError:
            pass

    benchmark(rejected_call)


def test_call_async(benchmark: BenchmarkFixture, runner: asyncio.Runner) -> None:
    """``call`` on a coroutine function, driven on a pre-built event loop."""
    breaker = CircuitBreaker(name='bench-call-async', config=_CLOSED_CONFIG)

    async def guarded() -> int:
        return await breaker.call(_async_work, 1, 2)

    benchmark(lambda: runner.run(guarded()))


def test_context_manager_async(benchmark: BenchmarkFixture, runner: asyncio.Runner) -> None:
    """The async guarded-block path (``async with breaker``)."""
    breaker = CircuitBreaker(name='bench-block-async', config=_CLOSED_CONFIG)

    async def guarded_block() -> int:
        async with breaker:
            return await _async_work(1, 2)

    benchmark(lambda: runner.run(guarded_block()))


def test_snapshot(benchmark: BenchmarkFixture) -> None:
    """Reading window aggregates: the observability path, expected to be O(1)."""
    breaker = CircuitBreaker(name='bench-snapshot', config=_CLOSED_CONFIG)
    for _ in range(100):
        breaker.call(_work, 1, 2)

    benchmark(breaker.snapshot)
