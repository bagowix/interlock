"""Pipeline core: the Strategy protocol, the executor and the v1 adapters (D1-D3)."""

import asyncio
import threading
from collections.abc import Awaitable, Callable
from typing import TypeVar

import pytest

from conftest import FakeClock
from interlock import CallTimeoutError, CircuitBreaker, CircuitOpenError, Config
from interlock._detect import is_async_callable
from interlock.pipeline import CircuitBreakerStrategy, Pipeline, Strategy, TimeoutStrategy

R = TypeVar('R')

TRIP_FAST = Config(failure_rate_threshold=0.5, minimum_number_of_calls=2, window_size=2)


class Recorder:
    """A strategy that logs when each layer enters and leaves."""

    def __init__(self, tag: str, log: list[str]) -> None:
        self._tag = tag
        self._log = log

    def execute(self, call: Callable[[], R]) -> R:
        self._log.append(f'{self._tag}:enter')
        result = call()
        self._log.append(f'{self._tag}:exit')
        return result

    async def execute_async(self, call: Callable[[], Awaitable[R]]) -> R:
        self._log.append(f'{self._tag}:enter')
        result = await call()
        self._log.append(f'{self._tag}:exit')
        return result


def test__pipeline__no_strategies__runs_the_callable() -> None:
    assert Pipeline().call(lambda: 42) == 42


@pytest.mark.asyncio
async def test__pipeline__no_strategies_async__runs_the_callable() -> None:
    async def answer() -> int:
        return 42

    assert await Pipeline().call(answer) == 42


def test__pipeline__sync__args_and_kwargs_reach_the_callable() -> None:
    def combine(a: int, *, b: str) -> str:
        return f'{a}-{b}'

    assert Pipeline().call(combine, 1, b='x') == '1-x'


@pytest.mark.asyncio
async def test__pipeline__async__args_and_kwargs_reach_the_callable() -> None:
    async def combine(a: int, *, b: str) -> str:
        return f'{a}-{b}'

    assert await Pipeline().call(combine, 1, b='x') == '1-x'


def test__pipeline__sync__strategies_apply_outermost_first() -> None:
    log: list[str] = []
    pipeline = Pipeline(Recorder('outer', log), Recorder('inner', log))

    result = pipeline.call(lambda: 'ok')

    assert result == 'ok'
    assert log == ['outer:enter', 'inner:enter', 'inner:exit', 'outer:exit']


@pytest.mark.asyncio
async def test__pipeline__async__strategies_apply_outermost_first() -> None:
    log: list[str] = []
    pipeline = Pipeline(Recorder('outer', log), Recorder('inner', log))

    async def work() -> str:
        return 'ok'

    result = await pipeline.call(work)

    assert result == 'ok'
    assert log == ['outer:enter', 'inner:enter', 'inner:exit', 'outer:exit']


def test__pipeline__sync_exception__propagates_unchanged() -> None:
    log: list[str] = []
    pipeline = Pipeline(Recorder('outer', log))

    def boom() -> None:
        raise RuntimeError('boom')

    with pytest.raises(RuntimeError, match='boom'):
        pipeline.call(boom)
    assert log == ['outer:enter']


@pytest.mark.asyncio
async def test__pipeline__async_exception__propagates_unchanged() -> None:
    async def boom() -> None:
        raise RuntimeError('boom')

    with pytest.raises(RuntimeError, match='boom'):
        await Pipeline().call(boom)


@pytest.mark.asyncio
async def test__pipeline__async__thunk_is_a_real_coroutine_function() -> None:
    """Strategies must be able to detect-dispatch on the next layer."""
    seen: list[bool] = []

    class Probe:
        def execute(self, call: Callable[[], R]) -> R:
            return call()

        async def execute_async(self, call: Callable[[], Awaitable[R]]) -> R:
            seen.append(is_async_callable(call))
            return await call()

    async def work() -> int:
        return 1

    await Pipeline(Probe(), Probe()).call(work)

    assert seen == [True, True]


def test__strategy__runtime_checkable__adapters_conform() -> None:
    breaker = CircuitBreaker(name='p')

    assert isinstance(CircuitBreakerStrategy(breaker), Strategy)
    assert isinstance(TimeoutStrategy(1.0), Strategy)
    assert not isinstance(object(), Strategy)


def test__circuit_breaker_strategy__sync_failures__trip_and_reject(fake_clock: FakeClock) -> None:
    breaker = CircuitBreaker(name='dep', config=TRIP_FAST, clock=fake_clock)
    pipeline = Pipeline(CircuitBreakerStrategy(breaker))
    reached = 0

    def flaky() -> None:
        nonlocal reached
        reached += 1
        raise ValueError('down')

    for _ in range(2):
        with pytest.raises(ValueError, match='down'):
            pipeline.call(flaky)
    with pytest.raises(CircuitOpenError):
        pipeline.call(flaky)

    assert reached == 2  # the third call never reached the dependency


@pytest.mark.asyncio
async def test__circuit_breaker_strategy__async_failures__trip_and_reject(
    fake_clock: FakeClock,
) -> None:
    breaker = CircuitBreaker(name='dep', config=TRIP_FAST, clock=fake_clock)
    pipeline = Pipeline(CircuitBreakerStrategy(breaker))
    reached = 0

    async def flaky() -> None:
        nonlocal reached
        reached += 1
        raise ValueError('down')

    for _ in range(2):
        with pytest.raises(ValueError, match='down'):
            await pipeline.call(flaky)
    with pytest.raises(CircuitOpenError):
        await pipeline.call(flaky)

    assert reached == 2


def test__circuit_breaker_strategy__success__returns_result_and_records(
    fake_clock: FakeClock,
) -> None:
    breaker = CircuitBreaker(name='dep', config=TRIP_FAST, clock=fake_clock)

    assert Pipeline(CircuitBreakerStrategy(breaker)).call(lambda: 'ok') == 'ok'
    assert breaker.snapshot().total_calls == 1


def test__circuit_breaker_strategy__standalone_use__keeps_working(fake_clock: FakeClock) -> None:
    """The same instance keeps working directly — the standalone invariant (§2.0)."""
    breaker = CircuitBreaker(name='dep', config=TRIP_FAST, clock=fake_clock)
    pipeline = Pipeline(CircuitBreakerStrategy(breaker))

    assert pipeline.call(lambda: 'via pipeline') == 'via pipeline'
    assert breaker.call(lambda: 'direct') == 'direct'
    assert breaker.snapshot().total_calls == 2


@pytest.mark.asyncio
async def test__circuit_breaker_strategy__cancellation__not_recorded(
    fake_clock: FakeClock,
) -> None:
    breaker = CircuitBreaker(name='dep', config=TRIP_FAST, clock=fake_clock)
    pipeline = Pipeline(CircuitBreakerStrategy(breaker))
    started = asyncio.Event()

    async def hang() -> None:
        started.set()
        await asyncio.sleep(5)

    task = asyncio.ensure_future(pipeline.call(hang))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert breaker.snapshot().total_calls == 0


def test__timeout_strategy__non_positive_seconds__raises_value_error() -> None:
    with pytest.raises(ValueError, match='seconds'):
        TimeoutStrategy(0.0)


def test__timeout_strategy__fast_call__returns_result() -> None:
    assert Pipeline(TimeoutStrategy(1.0)).call(lambda: 'ok') == 'ok'


@pytest.mark.asyncio
async def test__timeout_strategy__fast_async_call__returns_result() -> None:
    async def quick() -> str:
        return 'ok'

    assert await Pipeline(TimeoutStrategy(1.0)).call(quick) == 'ok'


def test__timeout_strategy__sync_overrun__raises_call_timeout_error() -> None:
    release = threading.Event()
    pipeline = Pipeline(TimeoutStrategy(0.01))

    def stuck() -> None:
        release.wait(5)

    try:
        with pytest.raises(CallTimeoutError):
            pipeline.call(stuck)
    finally:
        release.set()  # let the worker thread finish promptly

    assert release.is_set()


@pytest.mark.asyncio
async def test__timeout_strategy__async_overrun__raises_call_timeout_error() -> None:
    async def stuck() -> None:
        await asyncio.sleep(5)

    with pytest.raises(CallTimeoutError):
        await Pipeline(TimeoutStrategy(0.01)).call(stuck)


@pytest.mark.asyncio
async def test__composition__breaker_outside_timeout__timeouts_trip_the_circuit(
    fake_clock: FakeClock,
) -> None:
    """The v1.3 manual recipe in miniature: CB -> Timeout, hangs become failures."""
    breaker = CircuitBreaker(name='slow-dep', config=TRIP_FAST, clock=fake_clock)
    pipeline = Pipeline(CircuitBreakerStrategy(breaker), TimeoutStrategy(0.01))
    reached = 0

    async def hang() -> None:
        nonlocal reached
        reached += 1
        await asyncio.sleep(5)

    for _ in range(2):
        with pytest.raises(CallTimeoutError):
            await pipeline.call(hang)
    with pytest.raises(CircuitOpenError):
        await pipeline.call(hang)

    assert reached == 2


def test__composition__sync__order_holds_around_the_breaker(fake_clock: FakeClock) -> None:
    log: list[str] = []
    breaker = CircuitBreaker(name='dep', config=TRIP_FAST, clock=fake_clock)
    pipeline = Pipeline(Recorder('outer', log), CircuitBreakerStrategy(breaker))

    assert pipeline.call(lambda: 'ok') == 'ok'
    assert log == ['outer:enter', 'outer:exit']
    assert breaker.snapshot().total_calls == 1
