import asyncio

import pytest

from conftest import FakeClock
from interlock import CircuitOpenError, Config, State
from interlock._classify import DefaultFailureClassifier
from interlock._engine import Engine


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
def engine(config: Config, fake_clock: FakeClock) -> Engine:
    return Engine(
        name='test',
        config=config,
        clock=fake_clock,
        classifier=DefaultFailureClassifier(),
    )


def _trip_to_open(engine: Engine) -> None:
    def boom() -> None:
        raise ValueError('boom')

    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            engine.call_sync(boom)


def test__call_sync__success__returns_result_and_records(engine: Engine) -> None:
    assert engine.call_sync(lambda: 42) == 42

    snapshot = engine.snapshot()
    assert snapshot.total_calls == 1
    assert snapshot.failed_calls == 0


def test__call_sync__raising__records_failure_and_reraises(engine: Engine) -> None:
    def boom() -> None:
        raise ValueError('downstream')

    with pytest.raises(ValueError, match='downstream'):
        engine.call_sync(boom)

    assert engine.snapshot().failed_calls == 1


def test__call_sync__slow_success__records_slow(engine: Engine, fake_clock: FakeClock) -> None:
    def slow() -> str:
        fake_clock.advance(2.0)  # exceeds slow_call_duration_threshold (1.0)
        return 'ok'

    engine.call_sync(slow)

    snapshot = engine.snapshot()
    assert snapshot.slow_calls == 1
    assert snapshot.failed_calls == 0


def test__call_sync__slow_raise__records_slow_failure(
    engine: Engine, fake_clock: FakeClock
) -> None:
    def slow_boom() -> None:
        fake_clock.advance(2.0)
        raise ValueError('boom')

    with pytest.raises(ValueError, match='boom'):
        engine.call_sync(slow_boom)

    snapshot = engine.snapshot()
    assert snapshot.failed_calls == 1
    assert snapshot.slow_calls == 1


def test__call_sync__failures_reach_threshold__opens(engine: Engine) -> None:
    _trip_to_open(engine)

    assert engine.state is State.OPEN


def test__call_sync__open_circuit__rejects_with_circuit_open_error(engine: Engine) -> None:
    _trip_to_open(engine)

    with pytest.raises(CircuitOpenError):
        engine.call_sync(lambda: 1)


def test__call_sync__open_circuit__does_not_execute_callable(engine: Engine) -> None:
    _trip_to_open(engine)
    executed: list[int] = []

    with pytest.raises(CircuitOpenError):
        engine.call_sync(lambda: executed.append(1))

    assert executed == []


def test__call_sync__open_circuit__error_names_breaker(engine: Engine) -> None:
    _trip_to_open(engine)

    with pytest.raises(CircuitOpenError) as exc_info:
        engine.call_sync(lambda: 1)

    assert exc_info.value.breaker_name == 'test'


def test__call_sync__open_circuit__error_carries_retry_after(
    engine: Engine, fake_clock: FakeClock
) -> None:
    _trip_to_open(engine)  # opens at t=0, wait_duration_in_open is 5.0
    fake_clock.advance(2.0)

    with pytest.raises(CircuitOpenError) as exc_info:
        engine.call_sync(lambda: 1)

    assert exc_info.value.retry_after == 3.0


def test__call_sync__open_circuit__error_carries_last_failure(engine: Engine) -> None:
    _trip_to_open(engine)  # the tripping failures raise ValueError('boom')

    with pytest.raises(CircuitOpenError) as exc_info:
        engine.call_sync(lambda: 1)

    assert isinstance(exc_info.value.last_failure, ValueError)
    assert str(exc_info.value.last_failure) == 'boom'


def test__call_sync__forced_open__error_has_no_retry_after(engine: Engine) -> None:
    engine.force_open()  # operator override has no time-based estimate

    with pytest.raises(CircuitOpenError) as exc_info:
        engine.call_sync(lambda: 1)

    assert exc_info.value.retry_after is None


def test__call_sync__nested_call__does_not_deadlock(engine: Engine) -> None:
    # The lock must be released around the protected call; otherwise a re-entrant
    # call would deadlock on the non-reentrant threading.Lock.
    def outer() -> str:
        return engine.call_sync(lambda: 'inner')

    assert engine.call_sync(outer) == 'inner'
    assert engine.snapshot().total_calls == 2


def test__call_sync__result_classified_as_failure__records_failure(
    config: Config, fake_clock: FakeClock
) -> None:
    class NoneIsFailure:
        def is_failure(self, *, result: object, exception: BaseException | None) -> bool:
            return exception is not None or result is None

    engine = Engine(name='t', config=config, clock=fake_clock, classifier=NoneIsFailure())
    engine.call_sync(lambda: None)

    assert engine.snapshot().failed_calls == 1


def test__call__sync_callable__returns_value_directly(engine: Engine) -> None:
    assert engine.call(lambda: 5) == 5


@pytest.mark.asyncio
async def test__call_async__success__returns_result_and_records(engine: Engine) -> None:
    async def ok() -> int:
        return 7

    assert await engine.call_async(ok) == 7

    snapshot = engine.snapshot()
    assert snapshot.total_calls == 1
    assert snapshot.failed_calls == 0


@pytest.mark.asyncio
async def test__call_async__raising__records_failure_and_reraises(engine: Engine) -> None:
    async def boom() -> None:
        raise ValueError('boom')

    with pytest.raises(ValueError, match='boom'):
        await engine.call_async(boom)

    assert engine.snapshot().failed_calls == 1


@pytest.mark.asyncio
async def test__call__async_callable__dispatched_and_awaited(engine: Engine) -> None:
    async def ok() -> str:
        return 'a'

    assert await engine.call(ok) == 'a'


@pytest.mark.asyncio
async def test__call_async__open_circuit__rejects_without_executing(engine: Engine) -> None:
    _trip_to_open(engine)
    executed: list[int] = []

    async def probe() -> None:
        executed.append(1)

    with pytest.raises(CircuitOpenError):
        await engine.call_async(probe)

    assert executed == []


def test__stale_block__settling_in_half_open__not_counted_as_probe(
    engine: Engine, fake_clock: FakeClock
) -> None:
    start, admission = engine.enter_block()  # admitted while CLOSED
    _trip_to_open(engine)  # breaker trips while the block is still running
    fake_clock.advance(5.0)
    assert engine.call_sync(lambda: 'probe') == 'probe'  # -> HALF_OPEN, first probe succeeds

    engine.exit_block(start=start, admission=admission, exception=None)

    # permitted_calls_in_half_open=2: had the stale block counted as the second
    # probe, the round would have finished and the breaker would have closed.
    assert engine.state is State.HALF_OPEN


def test__reset__clears_last_failure(engine: Engine) -> None:
    _trip_to_open(engine)  # last_failure is now ValueError('boom')

    engine.reset()
    engine.force_open()
    with pytest.raises(CircuitOpenError) as exc_info:
        engine.call_sync(lambda: 1)

    assert exc_info.value.last_failure is None


def test__call_sync__base_exception_in_half_open__releases_the_probe_slot(
    engine: Engine, fake_clock: FakeClock
) -> None:
    _trip_to_open(engine)
    fake_clock.advance(5.0)

    def interrupted() -> None:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        engine.call_sync(interrupted)

    # Both probe slots must be available again and the interrupted call must
    # not count toward the round: two clean probes close the breaker.
    assert engine.call_sync(lambda: 'ok') == 'ok'
    assert engine.call_sync(lambda: 'ok') == 'ok'
    assert engine.state is State.CLOSED


@pytest.mark.asyncio
async def test__call_async__cancelled_probe__releases_the_probe_slot(
    engine: Engine, fake_clock: FakeClock
) -> None:
    _trip_to_open(engine)
    fake_clock.advance(5.0)

    probe_running = asyncio.Event()

    async def hanging_probe() -> None:
        probe_running.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(engine.call_async(hanging_probe))
    await probe_running.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    async def ok() -> str:
        return 'ok'

    assert await engine.call_async(ok) == 'ok'
    assert await engine.call_async(ok) == 'ok'
    assert engine.state is State.CLOSED


def test__exit_block__base_exception_in_half_open__releases_the_probe_slot(
    engine: Engine, fake_clock: FakeClock
) -> None:
    _trip_to_open(engine)
    fake_clock.advance(5.0)
    start, admission = engine.enter_block()  # admitted as a probe

    engine.exit_block(start=start, admission=admission, exception=KeyboardInterrupt())

    assert engine.call_sync(lambda: 'ok') == 'ok'
    assert engine.call_sync(lambda: 'ok') == 'ok'
    assert engine.state is State.CLOSED


def test__call_sync__base_exception_in_closed__records_nothing(engine: Engine) -> None:
    def interrupted() -> None:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        engine.call_sync(interrupted)

    assert engine.snapshot().total_calls == 0
