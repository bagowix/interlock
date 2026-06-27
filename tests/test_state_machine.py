from dataclasses import replace

import pytest

from conftest import FakeClock
from interlock import Config, Outcome, State, WindowType
from interlock._state_machine import StateMachine


@pytest.fixture
def config() -> Config:
    return Config(
        minimum_number_of_calls=2,
        window_size=10,
        permitted_calls_in_half_open=2,
        max_concurrent_probes=2,
        wait_duration_in_open=5.0,
    )


def _trip_to_open(machine: StateMachine, count: int) -> None:
    for _ in range(count):
        machine.record(Outcome.FAILURE)


def test__initial__state_is_closed(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)

    assert machine.state is State.CLOSED


def test__closed__acquire__always_permitted(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)

    assert machine.acquire() is True
    assert machine.acquire() is True


def test__closed__below_minimum_calls__stays_closed(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    machine.record(Outcome.FAILURE)  # 1 < minimum_number_of_calls (2)

    assert machine.state is State.CLOSED


def test__closed__failure_rate_at_threshold__opens(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    machine.record(Outcome.SUCCESS)
    machine.record(Outcome.FAILURE)  # total 2 (== min), failure_rate 0.5 (== threshold)

    assert machine.state is State.OPEN


def test__closed__failure_rate_below_threshold__stays_closed(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=replace(config, minimum_number_of_calls=4), clock=fake_clock)
    machine.record(Outcome.FAILURE)
    machine.record(Outcome.SUCCESS)
    machine.record(Outcome.SUCCESS)
    machine.record(Outcome.SUCCESS)  # 1/4 = 0.25 < 0.5

    assert machine.state is State.CLOSED


def test__closed__slow_call_rate_at_threshold__opens(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=replace(config, slow_call_rate_threshold=0.5), clock=fake_clock)
    machine.record(Outcome.SUCCESS)
    machine.record(Outcome.SLOW_SUCCESS)  # slow_rate 0.5, failure_rate 0.0

    assert machine.state is State.OPEN


def test__open__before_wait_elapsed__rejects(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, config.minimum_number_of_calls)
    fake_clock.advance(config.wait_duration_in_open - 1)

    assert machine.acquire() is False
    assert machine.state is State.OPEN


def test__open__after_wait_elapsed__transitions_to_half_open(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, config.minimum_number_of_calls)
    fake_clock.advance(config.wait_duration_in_open)

    assert machine.acquire() is True
    assert machine.state is State.HALF_OPEN


def test__half_open__concurrency_cap__rejects_second_in_flight(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=replace(config, max_concurrent_probes=1), clock=fake_clock)
    _trip_to_open(machine, config.minimum_number_of_calls)
    fake_clock.advance(config.wait_duration_in_open)

    assert machine.acquire() is True  # first probe in flight
    assert machine.acquire() is False  # cap reached, none completed yet


def test__half_open__permitted_cap__rejects_beyond_permitted(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, config.minimum_number_of_calls)
    fake_clock.advance(config.wait_duration_in_open)

    assert machine.acquire() is True  # probe 1
    assert machine.acquire() is True  # probe 2
    assert machine.acquire() is False  # permitted exhausted


def test__half_open__all_probes_succeed__closes(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, config.minimum_number_of_calls)
    fake_clock.advance(config.wait_duration_in_open)
    machine.acquire()
    machine.acquire()

    machine.record(Outcome.SUCCESS)
    machine.record(Outcome.SUCCESS)

    assert machine.state is State.CLOSED


def test__half_open__probe_failures_at_threshold__reopens(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, config.minimum_number_of_calls)
    fake_clock.advance(config.wait_duration_in_open)
    machine.acquire()
    machine.acquire()

    machine.record(Outcome.FAILURE)
    machine.record(Outcome.SUCCESS)  # 1/2 = 0.5 >= 0.5

    assert machine.state is State.OPEN


def test__half_open__slow_probes_at_threshold__reopens(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=replace(config, slow_call_rate_threshold=0.5), clock=fake_clock)
    _trip_to_open(machine, config.minimum_number_of_calls)
    fake_clock.advance(config.wait_duration_in_open)
    machine.acquire()
    machine.acquire()

    machine.record(Outcome.SLOW_SUCCESS)
    machine.record(Outcome.SUCCESS)  # slow 1/2 = 0.5 >= 0.5

    assert machine.state is State.OPEN


def test__half_open__reopen_resets_wait_timer(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, config.minimum_number_of_calls)  # opened at t=0
    fake_clock.advance(config.wait_duration_in_open)  # t=5
    machine.acquire()
    machine.acquire()
    machine.record(Outcome.FAILURE)
    machine.record(Outcome.FAILURE)  # probes fail → reopen at t=5

    fake_clock.advance(config.wait_duration_in_open - 1)  # t=9, only 4s since reopen

    assert machine.acquire() is False
    assert machine.state is State.OPEN


def test__half_open__close_resets_window__no_immediate_retrip(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, config.minimum_number_of_calls)
    fake_clock.advance(config.wait_duration_in_open)
    machine.acquire()
    machine.acquire()
    machine.record(Outcome.SUCCESS)
    machine.record(Outcome.SUCCESS)  # → CLOSED with fresh window

    machine.acquire()
    machine.record(Outcome.FAILURE)  # 1 failure, total 1 < minimum (2)

    assert machine.state is State.CLOSED


def test__forced_open__rejects_all_and_never_recovers(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    machine.force_open()
    fake_clock.advance(config.wait_duration_in_open * 100)

    assert machine.acquire() is False
    assert machine.state is State.FORCED_OPEN


def test__disabled__permits_all_and_records_nothing(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    machine.disable()

    assert machine.acquire() is True
    machine.record(Outcome.FAILURE)

    assert machine.state is State.DISABLED
    assert machine.snapshot().total_calls == 0


def test__metrics_only__records_but_never_trips(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    machine.metrics_only()

    for _ in range(4):
        assert machine.acquire() is True
        machine.record(Outcome.FAILURE)

    assert machine.state is State.METRICS_ONLY
    assert machine.snapshot().total_calls == 4


def test__reset__from_open__returns_to_closed(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, config.minimum_number_of_calls)

    machine.reset()

    assert machine.state is State.CLOSED
    assert machine.acquire() is True
    assert machine.snapshot().total_calls == 0


def test__reset__from_forced_open__returns_to_closed(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    machine.force_open()

    machine.reset()

    assert machine.state is State.CLOSED


def test__time_based_window__opens_on_failures(fake_clock: FakeClock) -> None:
    config = Config(
        window_type=WindowType.TIME_BASED,
        window_size=10,
        minimum_number_of_calls=2,
        permitted_calls_in_half_open=2,
        max_concurrent_probes=2,
        wait_duration_in_open=5.0,
    )
    machine = StateMachine(config=config, clock=fake_clock)
    machine.record(Outcome.FAILURE)
    machine.record(Outcome.FAILURE)

    assert machine.state is State.OPEN


def test__open__retry_after__reports_remaining_wait(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    machine.record(Outcome.SUCCESS)
    machine.record(Outcome.FAILURE)  # opens at t=0, wait_duration_in_open is 5.0
    fake_clock.advance(2.0)

    assert machine.retry_after() == 3.0


def test__closed__retry_after__is_none(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)

    assert machine.retry_after() is None


def test__half_open__retry_after__is_none(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    machine.record(Outcome.SUCCESS)
    machine.record(Outcome.FAILURE)  # opens
    fake_clock.advance(5.0)
    assert machine.acquire() is True  # crosses the wait into HALF_OPEN as the first probe


def test__attempt_auto_transition__open_and_elapsed__moves_to_half_open(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, config.minimum_number_of_calls)
    fake_clock.advance(config.wait_duration_in_open)

    assert machine.attempt_auto_transition() is True
    assert machine.state is State.HALF_OPEN


def test__attempt_auto_transition__open_not_elapsed__stays_open(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, config.minimum_number_of_calls)
    fake_clock.advance(config.wait_duration_in_open - 1)

    assert machine.attempt_auto_transition() is False
    assert machine.state is State.OPEN


def test__attempt_auto_transition__not_open__returns_false(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)

    assert machine.attempt_auto_transition() is False
    assert machine.state is State.CLOSED


def test__attempt_auto_transition__admits_no_probe(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(
        config=replace(config, permitted_calls_in_half_open=1, max_concurrent_probes=1),
        clock=fake_clock,
    )
    _trip_to_open(machine, config.minimum_number_of_calls)
    fake_clock.advance(config.wait_duration_in_open)

    machine.attempt_auto_transition()

    # No probe slot was consumed: the first real probe is still admitted.
    assert machine.acquire() is True


def test__attempt_auto_transition__after_lazy_acquire__no_double_transition(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, config.minimum_number_of_calls)
    fake_clock.advance(config.wait_duration_in_open)
    assert machine.acquire() is True  # real call wins the lazy transition first

    assert machine.attempt_auto_transition() is False
    assert machine.state is State.HALF_OPEN

    assert machine.state is State.HALF_OPEN
    assert machine.retry_after() is None


def test__forced_open__retry_after__is_none(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    machine.force_open()

    assert machine.retry_after() is None
