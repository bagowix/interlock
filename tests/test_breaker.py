import asyncio
import inspect
import threading
from collections.abc import Awaitable

import pytest

from conftest import FakeClock, RecordingListener
from interlock import CircuitBreaker, CircuitOpenError, Config, State
from interlock.outcome import Outcome
from interlock.window import WindowSnapshot


class _PausingWindow:
    def __init__(self) -> None:
        self.update_started = threading.Event()
        self.allow_update = threading.Event()
        self._total = 0
        self._failed = 0
        self._slow = 0

    def record(self, outcome: Outcome) -> None:
        self._total += 1
        self.update_started.set()
        if not self.allow_update.wait(5.0):
            raise AssertionError('window update was not released')
        self._failed += outcome.is_failure
        self._slow += outcome.is_slow

    def snapshot(self) -> WindowSnapshot:
        return WindowSnapshot(
            total_calls=self._total,
            failed_calls=self._failed,
            slow_calls=self._slow,
        )


class _ObservedLock:
    def __init__(self) -> None:
        self.contention_seen = threading.Event()
        self._lock = threading.Lock()

    def __enter__(self) -> None:
        if self._lock.acquire(blocking=False):
            return
        self.contention_seen.set()
        self._lock.acquire()

    def __exit__(self, *_args: object) -> None:
        self._lock.release()


@pytest.fixture
def config() -> Config:
    return Config(
        minimum_number_of_calls=2,
        window_size=10,
        slow_call_duration_threshold=1.0,
        permitted_calls_in_half_open=2,
        max_concurrent_probes=2,
        wait_duration_in_open=5.0,
    )


@pytest.fixture
def breaker(config: Config, fake_clock: FakeClock) -> CircuitBreaker:
    return CircuitBreaker(name='svc', config=config, clock=fake_clock)


def _fail(breaker: CircuitBreaker) -> None:
    def boom() -> None:
        raise ValueError('boom')

    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            breaker.call(boom)


def test__init__defaults__usable_without_config_or_clock() -> None:
    breaker = CircuitBreaker(name='default')

    assert breaker.call(lambda: 1) == 1
    assert breaker.state is State.CLOSED


@pytest.mark.parametrize(
    'initial_state',
    [State.CLOSED, State.FORCED_OPEN, State.DISABLED, State.METRICS_ONLY],
)
def test__init__supported_initial_state__starts_in_requested_state(initial_state: State) -> None:
    breaker = CircuitBreaker(name='rollout', initial_state=initial_state)

    assert breaker.state is initial_state


@pytest.mark.parametrize('initial_state', [State.OPEN, State.HALF_OPEN])
def test__init__transitional_initial_state__raises_value_error(initial_state: State) -> None:
    with pytest.raises(ValueError, match='initial_state'):
        CircuitBreaker(name='invalid', initial_state=initial_state)


def test__init__metrics_only__records_without_tripping_or_synthetic_transition(
    config: Config,
    fake_clock: FakeClock,
    listener: RecordingListener,
) -> None:
    breaker = CircuitBreaker(
        name='shadow',
        initial_state=State.METRICS_ONLY,
        config=config,
        clock=fake_clock,
        listener=listener,
    )

    _fail(breaker)
    _fail(breaker)

    assert breaker.state is State.METRICS_ONLY
    assert breaker.snapshot() == WindowSnapshot(total_calls=4, failed_calls=4, slow_calls=0)
    assert listener.state_changes == []


def test__name__exposes_breaker_name(breaker: CircuitBreaker) -> None:
    assert breaker.name == 'svc'


def test__call__success__returns_result(breaker: CircuitBreaker) -> None:
    assert breaker.call(lambda: 42) == 42


def test__call__failures_reach_threshold__opens(breaker: CircuitBreaker) -> None:
    _fail(breaker)

    assert breaker.state is State.OPEN


def test__call__open_circuit__raises_circuit_open_error(breaker: CircuitBreaker) -> None:
    _fail(breaker)

    with pytest.raises(CircuitOpenError):
        breaker.call(lambda: 1)


@pytest.mark.asyncio
async def test__call__async_callable__awaited(breaker: CircuitBreaker) -> None:
    async def ok() -> str:
        return 'a'

    assert await breaker.call(ok) == 'a'


def test__call_sync__success__returns_result(breaker: CircuitBreaker) -> None:
    assert breaker.call_sync(lambda a, b: a + b, 2, 3) == 5


def test__call_sync__failures_reach_threshold__opens(breaker: CircuitBreaker) -> None:
    def boom() -> None:
        raise ValueError('boom')

    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            breaker.call_sync(boom)

    assert breaker.state is State.OPEN


def test__call_sync__open_circuit__raises_circuit_open_error(breaker: CircuitBreaker) -> None:
    _fail(breaker)

    with pytest.raises(CircuitOpenError):
        breaker.call_sync(lambda: 1)


def test__call_sync__async_callable__is_never_detected(breaker: CircuitBreaker) -> None:
    """The caller states the nature; a coroutine function is run, not awaited."""

    async def never_awaited() -> str:
        return 'a'

    coroutine = breaker.call_sync(never_awaited)

    assert breaker.snapshot() == WindowSnapshot(total_calls=1, failed_calls=0, slow_calls=0)
    coroutine.close()


@pytest.mark.asyncio
async def test__call_async__success__returns_result(breaker: CircuitBreaker) -> None:
    async def add(a: int, b: int) -> int:
        return a + b

    assert await breaker.call_async(add, 2, 3) == 5


@pytest.mark.asyncio
async def test__call_async__failures_reach_threshold__opens(breaker: CircuitBreaker) -> None:
    async def boom() -> None:
        raise ValueError('boom')

    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            await breaker.call_async(boom)

    assert breaker.state is State.OPEN


@pytest.mark.asyncio
async def test__call_async__open_circuit__raises_circuit_open_error(
    breaker: CircuitBreaker,
) -> None:
    async def boom() -> None:
        raise ValueError('boom')

    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            await breaker.call_async(boom)

    with pytest.raises(CircuitOpenError):
        await breaker.call_async(boom)


@pytest.mark.asyncio
async def test__call_async__plain_callable_returning_awaitable__awaited(
    breaker: CircuitBreaker,
) -> None:
    """Middleware chains hand over callables that are not coroutine functions."""

    async def inner() -> str:
        return 'a'

    def handler() -> Awaitable[str]:
        return inner()

    assert await breaker.call_async(handler) == 'a'


def test__decorator__sync__preserves_name_and_runs(breaker: CircuitBreaker) -> None:
    @breaker
    def add(a: int, b: int) -> int:
        return a + b

    assert add(2, 3) == 5
    assert add.__name__ == 'add'
    assert not inspect.iscoroutinefunction(add)


def test__decorator__sync__opens_after_failures(breaker: CircuitBreaker) -> None:
    @breaker
    def boom() -> None:
        raise ValueError('boom')

    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            boom()

    with pytest.raises(CircuitOpenError):
        boom()


@pytest.mark.asyncio
async def test__decorator__async__preserves_nature_and_runs(breaker: CircuitBreaker) -> None:
    @breaker
    async def fetch(x: int) -> int:
        return x

    assert inspect.iscoroutinefunction(fetch)
    assert await fetch(7) == 7
    assert fetch.__name__ == 'fetch'


def test__context_manager__success__records_call(breaker: CircuitBreaker) -> None:
    with breaker:
        pass

    snapshot = breaker.snapshot()
    assert snapshot.total_calls == 1
    assert snapshot.failed_calls == 0


def test__snapshot__concurrent_settlement__waits_for_complete_window_update(
    breaker: CircuitBreaker, fake_clock: FakeClock
) -> None:
    window = _PausingWindow()
    lock = _ObservedLock()
    breaker._engine._machine._window = window
    breaker._engine._lock = lock

    def slow_call() -> None:
        fake_clock.advance(2.0)

    call_thread = threading.Thread(target=breaker.call, args=(slow_call,))
    call_thread.start()
    assert window.update_started.wait(5.0)

    snapshots: list[WindowSnapshot] = []

    def take_snapshot() -> None:
        snapshots.append(breaker.snapshot())

    snapshot_thread = threading.Thread(target=take_snapshot)
    snapshot_thread.start()
    try:
        assert lock.contention_seen.wait(5.0)
        assert snapshots == []
    finally:
        window.allow_update.set()
        call_thread.join(5.0)
        snapshot_thread.join(5.0)

    assert not call_thread.is_alive()
    assert not snapshot_thread.is_alive()
    assert snapshots == [WindowSnapshot(total_calls=1, failed_calls=0, slow_calls=1)]


def test__context_manager__failure__records_and_propagates(breaker: CircuitBreaker) -> None:
    with pytest.raises(ValueError, match='boom'), breaker:
        raise ValueError('boom')

    assert breaker.snapshot().failed_calls == 1


def test__context_manager__slow_block__records_slow(
    breaker: CircuitBreaker, fake_clock: FakeClock
) -> None:
    with breaker:
        fake_clock.advance(2.0)

    assert breaker.snapshot().slow_calls == 1


def test__context_manager__open_circuit__raises_on_enter(breaker: CircuitBreaker) -> None:
    _fail(breaker)
    entered = False

    with pytest.raises(CircuitOpenError):  # noqa: SIM117
        with breaker:
            entered = True

    assert entered is False


def test__context_manager__base_exception__not_recorded(breaker: CircuitBreaker) -> None:
    class Cancelled(BaseException):
        pass

    with pytest.raises(Cancelled), breaker:
        raise Cancelled

    assert breaker.snapshot().total_calls == 0


@pytest.mark.asyncio
async def test__async_context_manager__success__records_call(breaker: CircuitBreaker) -> None:
    async with breaker:
        pass

    assert breaker.snapshot().total_calls == 1


@pytest.mark.asyncio
async def test__async_context_manager__failure__records_and_propagates(
    breaker: CircuitBreaker,
) -> None:
    with pytest.raises(ValueError, match='boom'):
        async with breaker:
            raise ValueError('boom')

    assert breaker.snapshot().failed_calls == 1


@pytest.mark.asyncio
async def test__async_context_manager__open_circuit__raises_on_enter(
    breaker: CircuitBreaker,
) -> None:
    _fail(breaker)

    with pytest.raises(CircuitOpenError):
        async with breaker:
            pass


@pytest.mark.asyncio
async def test__async_context_manager__interleaved_tasks__durations_not_swapped(
    config: Config, fake_clock: FakeClock
) -> None:
    listener = RecordingListener()
    breaker = CircuitBreaker(name='svc', config=config, clock=fake_clock, listener=listener)

    a_in, a_go, b_in, b_go = (asyncio.Event() for _ in range(4))

    async def task_a() -> None:
        async with breaker:  # enters at t=0.0
            a_in.set()
            await a_go.wait()  # exits at t=0.7

    async def task_b() -> None:
        async with breaker:  # enters at t=0.5
            b_in.set()
            await b_go.wait()  # exits at t=1.0

    ta = asyncio.create_task(task_a())
    await a_in.wait()
    fake_clock.advance(0.5)
    tb = asyncio.create_task(task_b())
    await b_in.wait()
    fake_clock.advance(0.2)
    a_go.set()
    await ta  # A exits first, while B is still inside
    fake_clock.advance(0.3)
    b_go.set()
    await tb

    durations = [duration for _, duration in listener.calls]
    assert durations == pytest.approx([0.7, 0.5])


def test__context_manager__overlapping_threads__durations_not_swapped(
    config: Config, fake_clock: FakeClock
) -> None:
    listener = RecordingListener()
    breaker = CircuitBreaker(name='svc', config=config, clock=fake_clock, listener=listener)

    a_in, a_go, b_in, b_go = (threading.Event() for _ in range(4))

    def worker_a() -> None:
        with breaker:  # enters at t=0.0
            a_in.set()
            a_go.wait()  # exits at t=0.7

    def worker_b() -> None:
        with breaker:  # enters at t=0.5
            b_in.set()
            b_go.wait()  # exits at t=1.0

    ta = threading.Thread(target=worker_a)
    ta.start()
    assert a_in.wait(5.0)
    fake_clock.advance(0.5)
    tb = threading.Thread(target=worker_b)
    tb.start()
    assert b_in.wait(5.0)
    fake_clock.advance(0.2)
    a_go.set()
    ta.join(5.0)  # A exits first, while B is still inside
    fake_clock.advance(0.3)
    b_go.set()
    tb.join(5.0)

    durations = [duration for _, duration in listener.calls]
    assert durations == pytest.approx([0.7, 0.5])


def test__context_manager__nested_blocks__inner_settles_before_outer(
    config: Config, fake_clock: FakeClock
) -> None:
    listener = RecordingListener()
    breaker = CircuitBreaker(name='svc', config=config, clock=fake_clock, listener=listener)

    with breaker:  # outer enters at t=0.0
        fake_clock.advance(0.3)
        with breaker:  # inner enters at t=0.3
            fake_clock.advance(0.1)
        # inner exits at t=0.4 -> 0.1
        fake_clock.advance(0.6)
    # outer exits at t=1.0 -> 1.0

    durations = [duration for _, duration in listener.calls]
    assert durations == pytest.approx([0.1, 1.0])
