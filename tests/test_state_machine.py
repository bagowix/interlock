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


def test__record__stale_generation__not_counted_as_probe(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    stale_generation = machine.generation  # calls admitted while CLOSED carry this era
    _trip_to_open(machine, 2)
    fake_clock.advance(5.0)
    assert machine.acquire() is True  # OPEN -> HALF_OPEN, first probe admitted

    machine.record(Outcome.SUCCESS, generation=stale_generation)
    machine.record(Outcome.SUCCESS, generation=stale_generation)

    # Two stale CLOSED-era successes must not finish the probe round
    # (permitted_calls_in_half_open=2 would otherwise close the breaker here).
    assert machine.state is State.HALF_OPEN


def test__record__current_generation__is_recorded(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, 2)
    fake_clock.advance(5.0)
    assert machine.acquire() is True
    assert machine.acquire() is True
    generation = machine.generation

    machine.record(Outcome.SUCCESS, generation=generation)
    machine.record(Outcome.SUCCESS, generation=generation)

    assert machine.state is State.CLOSED


def test__record__probe_settling_after_reset__does_not_pollute_fresh_window(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, 2)
    fake_clock.advance(5.0)
    assert machine.acquire() is True  # probe in flight
    generation = machine.generation
    machine.reset()  # operator resets while the probe still runs

    machine.record(Outcome.FAILURE, generation=generation)

    assert machine.snapshot().total_calls == 0


def test__generation__bumped_by_overrides(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    stale_generation = machine.generation

    machine.metrics_only()
    machine.record(Outcome.FAILURE, generation=stale_generation)

    assert machine.snapshot().total_calls == 0  # pre-override call not recorded into shadow window


def test__release_probe__frees_the_slot_for_another_probe(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, 2)
    fake_clock.advance(5.0)
    assert machine.acquire() is True
    assert machine.acquire() is True
    assert machine.acquire() is False  # both slots in flight
    generation = machine.generation

    machine.release_probe(generation=generation)

    assert machine.acquire() is True  # the freed slot is admitted again


def test__release_probe__does_not_count_toward_the_probe_round(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, 2)
    fake_clock.advance(5.0)
    assert machine.acquire() is True
    generation = machine.generation
    machine.release_probe(generation=generation)

    # permitted_calls_in_half_open=2: two real probes must still be required.
    assert machine.acquire() is True
    machine.record(Outcome.SUCCESS, generation=generation)
    assert machine.state is State.HALF_OPEN  # round not finished by the released slot

    assert machine.acquire() is True
    machine.record(Outcome.SUCCESS, generation=generation)
    assert machine.state is State.CLOSED


def test__release_probe__stale_generation__ignored(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, 2)
    fake_clock.advance(5.0)
    assert machine.acquire() is True
    stale_generation = machine.generation
    machine.reset()  # transition bumps the generation and resets probe accounting

    machine.release_probe(generation=stale_generation)

    assert machine.state is State.CLOSED
    assert machine.acquire() is True


def test__release_probe__outside_half_open__ignored(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(config=config, clock=fake_clock)

    machine.release_probe(generation=machine.generation)

    assert machine.state is State.CLOSED
    assert machine.acquire() is True


def test__release_probe__stale_generation_in_a_later_round__ignored(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, 2)
    fake_clock.advance(5.0)
    assert machine.acquire() is True
    first_round = machine.generation
    assert machine.acquire() is True
    machine.record(Outcome.FAILURE, generation=first_round)
    machine.record(Outcome.FAILURE, generation=first_round)  # round one fails -> OPEN
    fake_clock.advance(5.0)
    assert machine.acquire() is True  # round two, probe 1 of 2

    machine.release_probe(generation=first_round)  # a probe of round one, interrupted late

    # Being in HALF_OPEN is not enough: the slot belonged to a round that is
    # already over, and returning it would hand round two a third probe.
    assert machine.acquire() is True
    assert machine.acquire() is False


def test__release_probe__frees_exactly_one_concurrency_slot(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(
        config=replace(config, permitted_calls_in_half_open=4, max_concurrent_probes=1),
        clock=fake_clock,
    )
    _trip_to_open(machine, 2)
    fake_clock.advance(5.0)
    assert machine.acquire() is True
    machine.release_probe(generation=machine.generation)

    assert machine.acquire() is True  # the interrupted probe gave its slot back
    assert machine.acquire() is False  # and gave back exactly one


def test__release_probe__returns_exactly_one_call_to_the_probe_budget(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, 2)
    fake_clock.advance(5.0)
    assert machine.acquire() is True
    machine.release_probe(generation=machine.generation)

    # permitted_calls_in_half_open=2, and the released call spent none of it:
    # exactly two probes may still run, not three.
    assert machine.acquire() is True
    machine.record(Outcome.SUCCESS)
    assert machine.acquire() is True
    assert machine.acquire() is False


def test__half_open__permitted_cap__counts_settled_probes_too(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, 2)
    fake_clock.advance(5.0)
    assert machine.acquire() is True
    machine.record(Outcome.SUCCESS)  # settles, so the concurrency slot is free again

    assert machine.acquire() is True  # the second and last permitted probe
    assert machine.acquire() is False  # budget spent even though a slot is free


def test__half_open__settled_probe__frees_exactly_one_concurrency_slot(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(
        config=replace(config, permitted_calls_in_half_open=4, max_concurrent_probes=1),
        clock=fake_clock,
    )
    _trip_to_open(machine, 2)
    fake_clock.advance(5.0)
    assert machine.acquire() is True
    machine.record(Outcome.SUCCESS)

    assert machine.acquire() is True
    assert machine.acquire() is False


def test__half_open__one_bad_probe_in_three__closes(config: Config, fake_clock: FakeClock) -> None:
    machine = StateMachine(
        config=replace(
            config,
            permitted_calls_in_half_open=3,
            max_concurrent_probes=3,
            slow_call_rate_threshold=0.5,
        ),
        clock=fake_clock,
    )
    _trip_to_open(machine, 2)
    fake_clock.advance(5.0)
    for _ in range(3):
        assert machine.acquire() is True

    machine.record(Outcome.SLOW_FAILURE)
    machine.record(Outcome.SUCCESS)
    machine.record(Outcome.SUCCESS)

    # One of three probes failed and one was slow: both 1/3, under the 0.5
    # thresholds. The round is judged on rates, not on counts.
    assert machine.state is State.CLOSED


def test__generation__never_repeats_across_transitions(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    seen = {machine.generation}

    def assert_new_era() -> None:
        # A reused generation would let an outcome admitted in an older era
        # settle into the current one — the exact confusion the counter exists
        # to prevent. Any transition must therefore mint a value never used yet.
        assert machine.generation not in seen
        seen.add(machine.generation)

    _trip_to_open(machine, 2)
    assert_new_era()
    fake_clock.advance(5.0)
    assert machine.acquire() is True
    assert_new_era()
    assert machine.acquire() is True
    machine.record(Outcome.FAILURE)
    machine.record(Outcome.FAILURE)
    assert_new_era()
    machine.force_open()
    assert_new_era()
    machine.disable()
    assert_new_era()
    machine.metrics_only()
    assert_new_era()
    machine.reset()
    assert_new_era()


def test__open__retry_after__measured_from_the_moment_it_opened(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    fake_clock.advance(10.0)  # the breaker does not open at t=0
    _trip_to_open(machine, 2)
    fake_clock.advance(2.0)

    assert machine.retry_after() == 3.0


def test__open__retry_after__is_zero_once_the_wait_elapsed(
    config: Config, fake_clock: FakeClock
) -> None:
    machine = StateMachine(config=config, clock=fake_clock)
    _trip_to_open(machine, 2)
    fake_clock.advance(config.wait_duration_in_open * 2)

    # A caller that sleeps for retry_after must not be sent away again: the
    # wait is over and the next call is the probe.
    assert machine.retry_after() == 0.0


def test__close__time_based_window__is_rebuilt_with_the_injected_clock(
    fake_clock: FakeClock,
) -> None:
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

    machine.reset()  # the fresh window still needs a clock to bucket by
    machine.record(Outcome.SUCCESS)

    assert machine.snapshot().total_calls == 1
