import asyncio
import inspect
import threading

import pytest

from conftest import FakeClock, RecordingListener
from interlock import CircuitBreaker, CircuitOpenError, Config, State


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
