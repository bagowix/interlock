"""Model-based test of the state machine (hypothesis ``RuleBasedStateMachine``).

``test_state_machine_properties.py`` generates a config and then drives a
*fixed*, hand-written sequence of calls, which finds arithmetic errors at the
boundaries it is pointed at. This model generates the sequence itself —
arbitrary interleavings of outcomes, clock advances, admissions and operator
overrides — and shrinks any failure to a minimal reproducer. Order-dependent
corruption (an override during a probe round, a probe settling after the state
already moved) lives in exactly those interleavings.

The model carries its own prediction of the state, the generation, the window
aggregates and the probe budget, derived from the documented contract rather
than read back from the machine, and asserts it after every step. Admissions
flow through a bundle: an admitted call yields a ticket (the generation the
call layer captures), and settling one consumes it — so probes can settle out
of order, or long after their era ended.

All time comes from ``FakeClock``; nothing here reads real time.
"""

from dataclasses import replace

from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    Bundle,
    MultipleResults,
    RuleBasedStateMachine,
    consumes,
    initialize,
    invariant,
    multiple,
    rule,
)

from conftest import FakeClock, configs
from interlock import Config, Outcome, State
from interlock._state_machine import StateMachine

_PERMIT_ALL = (State.CLOSED, State.DISABLED, State.METRICS_ONLY)

# Every recorded outcome costs two steps (admit, then settle), so a large
# `minimum_number_of_calls` would spend a whole run inside CLOSED and never
# reach the probe round. Narrow that one field; the rest is `configs()`.
_MODEL_CONFIGS = st.builds(
    lambda config, minimum: replace(config, minimum_number_of_calls=minimum),
    config=configs(),
    minimum=st.integers(min_value=1, max_value=4),
)


class StateMachineModel(RuleBasedStateMachine):
    """Drives a ``StateMachine`` against an independently predicted model of it."""

    tickets = Bundle('tickets')

    @initialize(config=_MODEL_CONFIGS)
    def build(self, config: Config) -> None:
        self.config = config
        self.clock = FakeClock()
        self.machine = StateMachine(config=config, clock=self.clock)
        self.expected = State.CLOSED
        self.override: State | None = None
        self.era = self.machine.generation
        self.opened_at = 0.0
        self.total = 0
        self.failed = 0
        self.slow = 0
        self._forget_probes()

    @rule(fraction=st.floats(min_value=0.0, max_value=2.0))
    def advance(self, fraction: float) -> None:
        # Fractions of the open wait, so 1.0 lands exactly on the boundary.
        self.clock.advance(self.config.wait_duration_in_open * fraction)

    @rule(target=tickets)
    def admit(self) -> int | MultipleResults[int]:
        admits = self._admits()
        begins_probing = self.expected is State.OPEN and admits

        assert self.machine.acquire() is admits

        if begins_probing:
            self._to_half_open()
        if not admits:
            return multiple()
        if self.expected is State.HALF_OPEN:
            self.probes_in_flight += 1
            self.probes_admitted += 1

        return self.machine.generation

    @rule(ticket=consumes(tickets), outcome=st.sampled_from(list(Outcome)))
    def settle(self, ticket: int, outcome: Outcome) -> None:
        self._predict_settle(ticket, outcome)
        self.machine.record(outcome, generation=ticket)

    @rule(ticket=consumes(tickets))
    def release(self, ticket: int) -> None:
        # What the call layer does when a BaseException interrupts a call: the
        # outcome must not count, but the probe slot has to come back.
        if ticket == self.era and self.expected is State.HALF_OPEN:
            self.probes_in_flight -= 1
            self.probes_admitted -= 1

        self.machine.release_probe(generation=ticket)

    @rule()
    def auto_transition(self) -> None:
        elapsed = self.expected is State.OPEN and self._wait_elapsed()

        assert self.machine.attempt_auto_transition() is elapsed

        if elapsed:
            self._to_half_open()

    @rule()
    def force_open(self) -> None:
        self.machine.force_open()
        self._to_override(State.FORCED_OPEN)

    @rule()
    def disable(self) -> None:
        self.machine.disable()
        self._to_override(State.DISABLED)

    @rule()
    def metrics_only(self) -> None:
        self.machine.metrics_only()
        self._to_override(State.METRICS_ONLY)

    @rule()
    def reset(self) -> None:
        self.machine.reset()
        self.override = None
        self._to_closed()

    @invariant()
    def state_is_predicted(self) -> None:
        assert self.machine.state is self.expected

    @invariant()
    def generation_is_predicted(self) -> None:
        # Every transition bumps it exactly once: the fence that stops an
        # outcome from a previous era settling into the current one.
        assert self.machine.generation == self.era

    @invariant()
    def override_is_authoritative(self) -> None:
        # #79: only another override or a reset may clear an operator override.
        if self.override is not None:
            assert self.machine.state is self.override

    @invariant()
    def probe_budget_holds(self) -> None:
        # Cross-checking the machine's own accounting surfaces a leaked or
        # double-returned slot on the step it happens, rather than later as a
        # rejected admission.
        assert self.machine._probes_in_flight == self.probes_in_flight
        assert self.machine._probes_admitted == self.probes_admitted
        assert self.probes_in_flight <= self.config.max_concurrent_probes
        assert self.probes_admitted <= self.config.permitted_calls_in_half_open

    @invariant()
    def window_matches_history(self) -> None:
        snapshot = self.machine.snapshot()
        assert snapshot.total_calls == self.total
        assert snapshot.failed_calls == self.failed
        assert snapshot.slow_calls == self.slow

    @invariant()
    def retry_after_is_the_remaining_wait(self) -> None:
        if self.expected is not State.OPEN:
            assert self.machine.retry_after() is None
        else:
            elapsed = self.clock.monotonic() - self.opened_at
            assert self.machine.retry_after() == max(
                0.0, self.config.wait_duration_in_open - elapsed
            )

    def _admits(self) -> bool:
        if self.expected in _PERMIT_ALL:
            return True
        if self.expected is State.OPEN:
            # The transition admits the triggering call as the first probe,
            # and a fresh probe round always has room for one.
            return self._wait_elapsed()
        if self.expected is State.HALF_OPEN:
            return (
                self.probes_in_flight < self.config.max_concurrent_probes
                and self.probes_admitted < self.config.permitted_calls_in_half_open
            )
        return False  # FORCED_OPEN

    def _predict_settle(self, ticket: int, outcome: Outcome) -> None:
        if ticket != self.era:
            return  # A previous era's outcome is dropped: nothing moves.

        if self.expected is State.CLOSED:
            self._record_in_window(outcome)
            if self.total >= self.config.minimum_number_of_calls and self._exceeds(
                failure_rate=self.failed / self.total,
                slow_call_rate=self.slow / self.total,
            ):
                self._to_open()
        elif self.expected is State.METRICS_ONLY:
            self._record_in_window(outcome)
        elif self.expected is State.HALF_OPEN:
            self._settle_probe(outcome)

    def _settle_probe(self, outcome: Outcome) -> None:
        self.probes_in_flight -= 1
        self.probes_completed += 1
        self.probe_failures += outcome.is_failure
        self.probe_slows += outcome.is_slow

        if self.probes_completed < self.config.permitted_calls_in_half_open:
            return

        if self._exceeds(
            failure_rate=self.probe_failures / self.probes_completed,
            slow_call_rate=self.probe_slows / self.probes_completed,
        ):
            self._to_open()
        else:
            self._to_closed()

    def _record_in_window(self, outcome: Outcome) -> None:
        self.total += 1
        self.failed += outcome.is_failure
        self.slow += outcome.is_slow

    def _exceeds(self, *, failure_rate: float, slow_call_rate: float) -> bool:
        return (
            failure_rate >= self.config.failure_rate_threshold
            or slow_call_rate >= self.config.slow_call_rate_threshold
        )

    def _wait_elapsed(self) -> bool:
        elapsed = self.clock.monotonic() - self.opened_at
        return elapsed >= self.config.wait_duration_in_open

    def _to_open(self) -> None:
        self.expected = State.OPEN
        self.opened_at = self.clock.monotonic()
        self._transition()

    def _to_half_open(self) -> None:
        self.expected = State.HALF_OPEN
        self._transition()

    def _to_closed(self) -> None:
        self.expected = State.CLOSED
        self.total = self.failed = self.slow = 0  # Closing rebuilds the window.
        self._transition()

    def _to_override(self, state: State) -> None:
        self.expected = state
        self.override = state
        self._transition()

    def _transition(self) -> None:
        self.era += 1
        self._forget_probes()

    def _forget_probes(self) -> None:
        self.probes_admitted = 0
        self.probes_in_flight = 0
        self.probes_completed = 0
        self.probe_failures = 0
        self.probe_slows = 0


StateMachineModel.TestCase.settings = settings(
    max_examples=200,
    stateful_step_count=50,
    deadline=None,
)
TestStateMachineModel = StateMachineModel.TestCase
