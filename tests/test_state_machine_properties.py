"""Property-based invariants for the state machine (hypothesis).

The example-based suite in ``test_state_machine.py`` pins specific transitions;
these properties assert the invariants that must hold across *all* valid configs
and outcome sequences — the guarantees a circuit breaker lives or dies by:

- the ``minimum_number_of_calls`` gate never trips on too little data;
- a saturated failure window always opens once the gate is met;
- ``OPEN`` rejects every call until ``wait_duration_in_open`` elapses;
- ``HALF_OPEN`` never admits more probes than its two caps allow.

All time is driven through ``FakeClock`` so the runs stay deterministic.
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from conftest import FakeClock, configs
from interlock import Config, Outcome, State
from interlock._state_machine import StateMachine


def _open(config: Config, clock: FakeClock) -> StateMachine:
    """Trip a fresh machine to ``OPEN`` with an all-failure window."""
    machine = StateMachine(config=config, clock=clock)
    for _ in range(config.minimum_number_of_calls):
        machine.record(Outcome.FAILURE)
    return machine


@given(config=configs(), extra=st.integers(min_value=0, max_value=50))
def test__closed__saturated_failure_window__always_opens(config: Config, extra: int) -> None:
    machine = StateMachine(config=config, clock=FakeClock())

    for _ in range(config.minimum_number_of_calls + extra):
        machine.record(Outcome.FAILURE)

    # failure_rate is 1.0, which meets any threshold in (0, 1] once the gate is met.
    assert machine.state is State.OPEN


@given(config=configs(), outcomes=st.lists(st.sampled_from(list(Outcome)), max_size=60))
def test__closed__below_minimum_calls__never_opens(config: Config, outcomes: list[Outcome]) -> None:
    machine = StateMachine(config=config, clock=FakeClock())

    for outcome in outcomes[: config.minimum_number_of_calls - 1]:
        machine.record(outcome)

    assert machine.state is State.CLOSED


@given(config=configs(), fraction=st.floats(min_value=0.0, max_value=0.999))
def test__open__before_wait_elapsed__always_rejects(config: Config, fraction: float) -> None:
    clock = FakeClock()
    machine = _open(config, clock)
    # Stay strictly inside the wait so the boundary case (== wait, which admits) is excluded.
    clock.advance(config.wait_duration_in_open * fraction)

    assert machine.acquire() is False
    assert machine.state is State.OPEN


@settings(max_examples=200)
@given(config=configs(), attempts=st.integers(min_value=0, max_value=40))
def test__half_open__probe_admission__never_exceeds_caps(config: Config, attempts: int) -> None:
    clock = FakeClock()
    machine = _open(config, clock)
    clock.advance(config.wait_duration_in_open)

    admitted = sum(machine.acquire() for _ in range(attempts))

    # No probe completes here, so in-flight only grows: admission is bounded by
    # whichever cap is tighter — concurrency or the total permitted.
    assert admitted <= min(config.max_concurrent_probes, config.permitted_calls_in_half_open)
