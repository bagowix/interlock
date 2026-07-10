"""Pipeline core: the Strategy protocol, the executor and the v1 adapters (D1-D3)."""

import asyncio
import inspect
import threading
from collections.abc import Awaitable, Callable
from typing import TypeVar

import pytest

import interlock
from conftest import FakeClock
from interlock import BulkheadFullError, CallTimeoutError, CircuitBreaker, CircuitOpenError, Config
from interlock._detect import is_async_callable
from interlock.pipeline import (
    BulkheadStrategy,
    CircuitBreakerStrategy,
    FallbackStrategy,
    Pipeline,
    Strategy,
    TimeoutStrategy,
)

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
    assert isinstance(BulkheadStrategy(1), Strategy)
    assert isinstance(FallbackStrategy(lambda _exc: None), Strategy)
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


def test__bulkhead_strategy__non_positive_limit__raises_value_error() -> None:
    with pytest.raises(ValueError, match='max_concurrent'):
        BulkheadStrategy(0)


def test__bulkhead_strategy__negative_max_wait__raises_value_error() -> None:
    with pytest.raises(ValueError, match='max_wait'):
        BulkheadStrategy(1, max_wait=-0.1)


def test__bulkhead_strategy__under_the_limit__runs_and_releases() -> None:
    pipeline = Pipeline(BulkheadStrategy(1))

    assert pipeline.call(lambda: 'first') == 'first'
    assert pipeline.call(lambda: 'second') == 'second'  # the slot was released


def test__bulkhead_strategy__exception__releases_the_slot() -> None:
    pipeline = Pipeline(BulkheadStrategy(1))

    def boom() -> None:
        raise ValueError('boom')

    with pytest.raises(ValueError, match='boom'):
        pipeline.call(boom)
    assert pipeline.call(lambda: 'ok') == 'ok'


def test__bulkhead_strategy__saturated__rejects_immediately_by_default() -> None:
    pipeline = Pipeline(BulkheadStrategy(1))
    inside = threading.Event()
    release = threading.Event()

    def hold() -> str:
        inside.set()
        release.wait(5)
        return 'held'

    worker = threading.Thread(target=pipeline.call, args=(hold,))
    worker.start()
    try:
        assert inside.wait(5)
        with pytest.raises(BulkheadFullError):
            pipeline.call(lambda: 'rejected')
    finally:
        release.set()
        worker.join(timeout=5)


def test__bulkhead_strategy__slot_freed_within_max_wait__proceeds() -> None:
    pipeline = Pipeline(BulkheadStrategy(1, max_wait=5.0))
    inside = threading.Event()
    release = threading.Event()

    def hold() -> str:
        inside.set()
        release.wait(5)
        return 'held'

    worker = threading.Thread(target=pipeline.call, args=(hold,))
    worker.start()
    try:
        assert inside.wait(5)
        releaser = threading.Timer(0.05, release.set)
        releaser.start()
        assert pipeline.call(lambda: 'ok') == 'ok'  # blocks until the holder frees the slot
    finally:
        release.set()
        worker.join(timeout=5)


@pytest.mark.asyncio
async def test__bulkhead_strategy__async_saturated__rejects_immediately() -> None:
    pipeline = Pipeline(BulkheadStrategy(1))
    inside = asyncio.Event()
    release = asyncio.Event()

    async def hold() -> str:
        inside.set()
        await release.wait()
        return 'held'

    async def rejected() -> str:
        return 'never'

    task = asyncio.ensure_future(pipeline.call(hold))
    await inside.wait()
    with pytest.raises(BulkheadFullError):
        await pipeline.call(rejected)
    release.set()
    assert await task == 'held'


@pytest.mark.asyncio
async def test__bulkhead_strategy__async_slot_freed_within_max_wait__proceeds() -> None:
    pipeline = Pipeline(BulkheadStrategy(1, max_wait=5.0))
    inside = asyncio.Event()

    async def hold() -> str:
        inside.set()
        await asyncio.sleep(0.01)
        return 'held'

    async def follow_up() -> str:
        return 'ok'

    task = asyncio.ensure_future(pipeline.call(hold))
    await inside.wait()
    assert await pipeline.call(follow_up) == 'ok'  # waited for the slot
    assert await task == 'held'


@pytest.mark.asyncio
async def test__bulkhead_strategy__async_exception__releases_the_slot() -> None:
    pipeline = Pipeline(BulkheadStrategy(1))

    async def boom() -> None:
        raise ValueError('boom')

    async def follow_up() -> str:
        return 'ok'

    with pytest.raises(ValueError, match='boom'):
        await pipeline.call(boom)
    assert await pipeline.call(follow_up) == 'ok'


def test__fallback_strategy__empty_on__raises_value_error() -> None:
    with pytest.raises(ValueError, match='on'):
        FallbackStrategy(lambda _exc: 'substitute', on=())


def test__fallback_strategy__non_exception_entry__raises_type_error() -> None:
    with pytest.raises(TypeError, match='Exception subclasses'):
        FallbackStrategy(lambda _exc: 'substitute', on=(KeyboardInterrupt,))  # type: ignore[arg-type]


def test__fallback_strategy__success__returns_the_original_result() -> None:
    invoked: list[BaseException] = []
    pipeline = Pipeline(FallbackStrategy(invoked.append))

    assert pipeline.call(lambda: 'real') == 'real'
    assert invoked == []


def test__fallback_strategy__matching_failure__returns_the_substitute() -> None:
    seen: list[BaseException] = []

    def substitute(exc: BaseException) -> str:
        seen.append(exc)
        return 'cached'

    pipeline = Pipeline(FallbackStrategy(substitute, on=(ValueError,)))
    boom = ValueError('down')

    def failing() -> str:
        raise boom

    assert pipeline.call(failing) == 'cached'
    assert seen == [boom]  # the fallback sees the exact exception


def test__fallback_strategy__non_matching_failure__propagates() -> None:
    pipeline = Pipeline(FallbackStrategy(lambda _exc: 'cached', on=(ValueError,)))

    def failing() -> str:
        raise KeyError('other')

    with pytest.raises(KeyError, match='other'):
        pipeline.call(failing)


@pytest.mark.asyncio
async def test__fallback_strategy__async_matching_failure__returns_the_substitute() -> None:
    pipeline = Pipeline(FallbackStrategy(lambda _exc: 'cached', on=(ValueError,)))

    async def failing() -> str:
        raise ValueError('down')

    assert await pipeline.call(failing) == 'cached'


@pytest.mark.asyncio
async def test__fallback_strategy__async_success__returns_the_original_result() -> None:
    pipeline = Pipeline(FallbackStrategy(lambda _exc: 'cached'))

    async def work() -> str:
        return 'real'

    assert await pipeline.call(work) == 'real'


@pytest.mark.asyncio
async def test__fallback_strategy__cancellation__passes_through_uncaught() -> None:
    invoked: list[BaseException] = []
    pipeline = Pipeline(FallbackStrategy(invoked.append))
    started = asyncio.Event()

    async def hang() -> None:
        started.set()
        await asyncio.sleep(5)

    task = asyncio.ensure_future(pipeline.call(hang))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert invoked == []  # the default on=(Exception,) never catches cancellation


def test__fallback_strategy__outside_the_breaker__substitutes_rejections(
    fake_clock: FakeClock,
) -> None:
    breaker = CircuitBreaker(name='dep', config=TRIP_FAST, clock=fake_clock)
    pipeline = Pipeline(
        FallbackStrategy(lambda _exc: 'cached', on=(CircuitOpenError,)),
        CircuitBreakerStrategy(breaker),
    )

    def failing() -> str:
        raise ValueError('down')

    for _ in range(2):
        with pytest.raises(ValueError, match='down'):  # not in `on` — propagates
            pipeline.call(failing)
    assert pipeline.call(failing) == 'cached'  # open circuit -> substitute


def test__fallback_strategy__metrics_only_breaker__shadow_stats_not_masked(
    fake_clock: FakeClock,
) -> None:
    """The known-problems check: a fallback must not hide shadow-mode statistics."""
    breaker = CircuitBreaker(name='dep', clock=fake_clock)  # default window fits all 3 calls
    breaker.metrics_only()
    pipeline = Pipeline(FallbackStrategy(lambda _exc: 'cached'), CircuitBreakerStrategy(breaker))

    def failing() -> str:
        raise ValueError('down')

    for _ in range(3):
        assert pipeline.call(failing) == 'cached'

    snapshot = breaker.snapshot()
    assert snapshot.total_calls == 3
    assert snapshot.failed_calls == 3  # every masked failure is still recorded


def test__pipeline_decorator__sync__preserves_name_and_applies_strategies() -> None:
    log: list[str] = []
    pipeline = Pipeline(Recorder('outer', log))

    @pipeline
    def combine(a: int, *, b: str) -> str:
        return f'{a}-{b}'

    assert combine(1, b='x') == '1-x'
    assert combine.__name__ == 'combine'
    assert log == ['outer:enter', 'outer:exit']


@pytest.mark.asyncio
async def test__pipeline_decorator__async__preserves_nature_and_applies_strategies() -> None:
    log: list[str] = []
    pipeline = Pipeline(Recorder('outer', log))

    @pipeline
    async def double(value: int) -> int:
        return value * 2

    assert inspect.iscoroutinefunction(double)
    assert await double(21) == 42
    assert log == ['outer:enter', 'outer:exit']


def test__pipeline_decorator__breaker_inside__trips(fake_clock: FakeClock) -> None:
    breaker = CircuitBreaker(name='dep', config=TRIP_FAST, clock=fake_clock)

    @Pipeline(CircuitBreakerStrategy(breaker))
    def failing() -> None:
        raise ValueError('down')

    for _ in range(2):
        with pytest.raises(ValueError, match='down'):
            failing()
    with pytest.raises(CircuitOpenError):
        failing()


def test__pipeline_builder__empty__builds_a_passthrough() -> None:
    assert Pipeline.builder().build().call(lambda: 42) == 42


def test__pipeline_builder__declaration_order__is_application_order() -> None:
    log: list[str] = []
    pipeline = Pipeline.builder().add(Recorder('outer', log)).add(Recorder('inner', log)).build()

    assert pipeline.call(lambda: 'ok') == 'ok'
    assert log == ['outer:enter', 'inner:enter', 'inner:exit', 'outer:exit']


def test__pipeline_builder__fallback_over_breaker__substitutes_rejections(
    fake_clock: FakeClock,
) -> None:
    breaker = CircuitBreaker(name='dep', config=TRIP_FAST, clock=fake_clock)
    pipeline = (
        Pipeline.builder()
        .fallback(lambda _exc: 'cached', on=(CircuitOpenError,))
        .circuit_breaker(breaker)
        .build()
    )

    def failing() -> str:
        raise ValueError('down')

    for _ in range(2):
        with pytest.raises(ValueError, match='down'):
            pipeline.call(failing)
    assert pipeline.call(failing) == 'cached'


@pytest.mark.asyncio
async def test__pipeline_builder__timeout_step__bounds_the_call() -> None:
    pipeline = Pipeline.builder().timeout(0.01).build()

    async def stuck() -> None:
        await asyncio.sleep(5)

    with pytest.raises(CallTimeoutError):
        await pipeline.call(stuck)


@pytest.mark.asyncio
async def test__pipeline_builder__bulkhead_step__caps_concurrency() -> None:
    pipeline = Pipeline.builder().bulkhead(1).build()
    inside = asyncio.Event()
    release = asyncio.Event()

    async def hold() -> str:
        inside.set()
        await release.wait()
        return 'held'

    async def rejected() -> str:
        return 'never'

    task = asyncio.ensure_future(pipeline.call(hold))
    await inside.wait()
    with pytest.raises(BulkheadFullError):
        await pipeline.call(rejected)
    release.set()
    assert await task == 'held'


def test__public_surface__pipeline_names_are_exported_from_interlock() -> None:
    for name in (
        'BulkheadStrategy',
        'CircuitBreakerStrategy',
        'FallbackStrategy',
        'Pipeline',
        'PipelineBuilder',
        'Strategy',
        'TimeoutStrategy',
    ):
        assert hasattr(interlock, name)
        assert name in interlock.__all__


class RecordingPipelineListener:
    """Records only the v2.0 pipeline hooks (retry / bulkhead / fallback)."""

    def __init__(self) -> None:
        self.retries: list[tuple[str, int, float]] = []
        self.bulkhead_rejections: list[str] = []
        self.fallbacks: list[tuple[str, BaseException]] = []

    def on_retry(self, *, name: str, attempt: int, delay: float) -> None:
        self.retries.append((name, attempt, delay))

    def on_bulkhead_rejected(self, *, name: str) -> None:
        self.bulkhead_rejections.append(name)

    def on_fallback(self, *, name: str, error: BaseException) -> None:
        self.fallbacks.append((name, error))


def test__bulkhead_strategy__rejection__notifies_the_listener() -> None:
    events = RecordingPipelineListener()
    pipeline = Pipeline(BulkheadStrategy(1, name='db-pool', listener=events))
    inside = threading.Event()
    release = threading.Event()

    def hold() -> str:
        inside.set()
        release.wait(5)
        return 'held'

    worker = threading.Thread(target=pipeline.call, args=(hold,))
    worker.start()
    try:
        assert inside.wait(5)
        with pytest.raises(BulkheadFullError):
            pipeline.call(lambda: 'rejected')
    finally:
        release.set()
        worker.join(timeout=5)

    assert events.bulkhead_rejections == ['db-pool']


@pytest.mark.asyncio
async def test__bulkhead_strategy__async_rejection__notifies_the_listener() -> None:
    events = RecordingPipelineListener()
    pipeline = Pipeline(BulkheadStrategy(1, name='db-pool', listener=events))
    inside = asyncio.Event()
    release = asyncio.Event()

    async def hold() -> str:
        inside.set()
        await release.wait()
        return 'held'

    async def rejected() -> str:
        return 'never'

    task = asyncio.ensure_future(pipeline.call(hold))
    await inside.wait()
    with pytest.raises(BulkheadFullError):
        await pipeline.call(rejected)
    release.set()
    await task

    assert events.bulkhead_rejections == ['db-pool']


def test__bulkhead_strategy__admitted_call__emits_no_event() -> None:
    events = RecordingPipelineListener()
    pipeline = Pipeline(BulkheadStrategy(1, listener=events))

    assert pipeline.call(lambda: 'ok') == 'ok'
    assert events.bulkhead_rejections == []


def test__fallback_strategy__substitution__notifies_the_listener() -> None:
    events = RecordingPipelineListener()
    pipeline = Pipeline(
        FallbackStrategy(lambda _exc: 'cached', on=(ValueError,), name='recs', listener=events)
    )
    boom = ValueError('down')

    def failing() -> str:
        raise boom

    assert pipeline.call(failing) == 'cached'
    assert events.fallbacks == [('recs', boom)]


def test__fallback_strategy__non_matching_failure__emits_no_event() -> None:
    events = RecordingPipelineListener()
    pipeline = Pipeline(FallbackStrategy(lambda _exc: 'cached', on=(ValueError,), listener=events))

    def failing() -> str:
        raise KeyError('other')

    with pytest.raises(KeyError):
        pipeline.call(failing)
    assert events.fallbacks == []


def test__pipeline_observability__pre_v2_listener__keeps_working() -> None:
    """A listener written before the pipeline hooks existed must not break strategies."""

    class PreV2Listener:
        def on_state_change(self, *, name: str, old: object, new: object) -> None: ...
        def on_call(self, *, name: str, outcome: object, duration: float) -> None: ...
        def on_rejected(self, *, name: str) -> None: ...
        def on_reset(self, *, name: str) -> None: ...

    listener = PreV2Listener()
    pipeline = Pipeline(
        FallbackStrategy(lambda _exc: 'cached', on=(BulkheadFullError,), listener=listener),
        BulkheadStrategy(1, listener=listener),
    )
    inside = threading.Event()
    release = threading.Event()

    def hold() -> str:
        inside.set()
        release.wait(5)
        return 'held'

    worker = threading.Thread(target=pipeline.call, args=(hold,))
    worker.start()
    try:
        assert inside.wait(5)
        assert pipeline.call(lambda: 'rejected') == 'cached'  # no AttributeError anywhere
    finally:
        release.set()
        worker.join(timeout=5)


def test__pipeline_builder__steps_pass_name_and_listener_through() -> None:
    events = RecordingPipelineListener()
    pipeline = (
        Pipeline.builder()
        .fallback(lambda _exc: 'cached', on=(ValueError,), name='recs', listener=events)
        .build()
    )

    def failing() -> str:
        raise ValueError('down')

    assert pipeline.call(failing) == 'cached'
    assert [name for name, _ in events.fallbacks] == ['recs']
