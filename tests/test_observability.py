import pytest

from conftest import FakeClock, RecordingListener
from interlock import CircuitBreaker, Config, Outcome, State
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
def breaker(config: Config, fake_clock: FakeClock, listener: RecordingListener) -> CircuitBreaker:
    return CircuitBreaker(name='svc', config=config, clock=fake_clock, listener=listener)


def _trip(breaker: CircuitBreaker) -> None:
    def boom() -> None:
        raise ValueError('boom')

    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            breaker.call(boom)


def test__on_call__success__reports_outcome_and_duration(
    breaker: CircuitBreaker, fake_clock: FakeClock, listener: RecordingListener
) -> None:
    def work() -> int:
        fake_clock.advance(0.25)
        return 1

    breaker.call(work)

    assert listener.calls == [(Outcome.SUCCESS, 0.25)]


def test__on_call__failure__reports_failure_outcome(
    breaker: CircuitBreaker, listener: RecordingListener
) -> None:
    def boom() -> None:
        raise ValueError('boom')

    with pytest.raises(ValueError, match='boom'):
        breaker.call(boom)

    assert listener.calls == [(Outcome.FAILURE, 0.0)]


def test__on_state_change__trip_to_open__reports_transition(
    breaker: CircuitBreaker, listener: RecordingListener
) -> None:
    _trip(breaker)

    assert listener.state_changes == [(State.CLOSED, State.OPEN)]


def test__on_rejected__open_circuit__reports_each_rejection(
    breaker: CircuitBreaker, listener: RecordingListener
) -> None:
    _trip(breaker)

    with pytest.raises(Exception, match='open'):
        breaker.call(lambda: 1)

    assert listener.rejected == 1


def test__on_state_change__open_to_half_open__reported_on_lazy_probe(
    breaker: CircuitBreaker, fake_clock: FakeClock, listener: RecordingListener
) -> None:
    _trip(breaker)
    fake_clock.advance(5.0)

    breaker.call(lambda: 1)

    assert (State.OPEN, State.HALF_OPEN) in listener.state_changes


def test__context_manager__success__reports_on_call(
    breaker: CircuitBreaker, listener: RecordingListener
) -> None:
    with breaker:
        pass

    assert listener.calls == [(Outcome.SUCCESS, 0.0)]


def test__reset__reports_reset_and_returns_to_closed(
    breaker: CircuitBreaker, listener: RecordingListener
) -> None:
    _trip(breaker)
    listener.state_changes.clear()

    breaker.reset()

    assert breaker.state is State.CLOSED
    assert listener.resets == 1
    assert listener.state_changes == [(State.OPEN, State.CLOSED)]


def test__reset__already_closed__reports_reset_without_state_change(
    breaker: CircuitBreaker, listener: RecordingListener
) -> None:
    breaker.reset()

    assert listener.resets == 1
    assert listener.state_changes == []


def test__force_open__rejects_and_reports_transition(
    breaker: CircuitBreaker, listener: RecordingListener
) -> None:
    breaker.force_open()

    assert breaker.state is State.FORCED_OPEN
    assert listener.state_changes == [(State.CLOSED, State.FORCED_OPEN)]


def test__disable__admits_all_without_recording(breaker: CircuitBreaker) -> None:
    breaker.disable()

    def boom() -> None:
        raise ValueError('boom')

    for _ in range(5):
        with pytest.raises(ValueError, match='boom'):
            breaker.call(boom)

    assert breaker.state is State.DISABLED
    assert breaker.snapshot().total_calls == 0


def test__metrics_only__records_but_never_trips(breaker: CircuitBreaker) -> None:
    breaker.metrics_only()

    def boom() -> None:
        raise ValueError('boom')

    for _ in range(5):
        with pytest.raises(ValueError, match='boom'):
            breaker.call(boom)

    assert breaker.state is State.METRICS_ONLY
    assert breaker.snapshot().total_calls == 5


def test__no_listener__operations_do_not_raise() -> None:
    breaker = CircuitBreaker(name='quiet')

    assert breaker.call(lambda: 7) == 7
    breaker.force_open()
    breaker.reset()


@pytest.mark.asyncio
async def test__on_call__async__reported(
    breaker: CircuitBreaker, listener: RecordingListener
) -> None:
    async def ok() -> int:
        return 3

    await breaker.call(ok)

    assert listener.calls == [(Outcome.SUCCESS, 0.0)]


def test__engine__listener__threads_through_call(
    config: Config, fake_clock: FakeClock, listener: RecordingListener
) -> None:
    engine = Engine(name='e', config=config, clock=fake_clock, listener=listener)

    engine.call_sync(lambda: 1)

    assert listener.calls == [(Outcome.SUCCESS, 0.0)]
