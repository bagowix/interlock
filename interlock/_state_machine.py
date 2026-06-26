"""The I/O-free circuit breaker state machine.

This is the core: it owns the current ``State``, the sliding window of recent
outcomes, and every transition between them. It performs no I/O and knows
nothing about sync vs async — the call layer wraps it in a lock and feeds it
admission requests (``acquire``) and results (``record``). All time comes from
the injected ``Clock``, so transitions are fully deterministic under test.

Three core states cycle on downstream health:

- ``CLOSED`` admits everything and trips to ``OPEN`` once the window holds at
  least ``minimum_number_of_calls`` and the failure *or* slow-call rate reaches
  its threshold.
- ``OPEN`` rejects everything until ``wait_duration_in_open`` has elapsed, then
  lazily (on the next ``acquire``) moves to ``HALF_OPEN``.
- ``HALF_OPEN`` admits a bounded number of probes — at most
  ``permitted_calls_in_half_open`` total and ``max_concurrent_probes`` at once —
  then closes or reopens based on the probes' rates.

Three special states are operator overrides: ``FORCED_OPEN`` (reject all),
``DISABLED`` (admit all, no metrics), ``METRICS_ONLY`` (admit all, record
metrics, never trip).
"""

from interlock._windows import build_window
from interlock.config import Config
from interlock.outcome import Outcome
from interlock.protocols import Clock
from interlock.state import State
from interlock.window import WindowSnapshot

__all__ = ('StateMachine',)

_PERMIT_ALL = frozenset({State.CLOSED, State.DISABLED, State.METRICS_ONLY})


class StateMachine:
    """Owns breaker state and drives transitions from recorded outcomes.

    Not thread-safe on its own: the call layer serialises ``acquire`` and
    ``record`` under a single lock. Time is read only through the injected
    ``Clock``.
    """

    def __init__(self, *, config: Config, clock: Clock) -> None:
        self._config = config
        self._clock = clock
        self._window = build_window(config=config, clock=clock)
        self._state = State.CLOSED
        self._opened_at = 0.0
        self._reset_probes()

    @property
    def state(self) -> State:
        """The current lifecycle state."""
        return self._state

    def snapshot(self) -> WindowSnapshot:
        """An immutable view of the current window aggregates."""
        return self._window.snapshot()

    def retry_after(self) -> float | None:
        """Seconds until the next probe is admitted, when the breaker can estimate it.

        Estimable only in ``OPEN`` — the remainder of ``wait_duration_in_open``.
        ``HALF_OPEN`` rejections come from probe caps rather than time, and
        ``FORCED_OPEN`` waits for an operator; neither has a time estimate, so
        both (like the admitting states) return ``None``.
        """
        if self._state is not State.OPEN:
            return None

        elapsed = self._clock.monotonic() - self._opened_at
        return max(0.0, self._config.wait_duration_in_open - elapsed)

    def acquire(self) -> bool:
        """Decide whether one call may proceed, mutating state lazily.

        ``OPEN`` becomes ``HALF_OPEN`` here once its wait has elapsed, and the
        triggering call is admitted as the first probe. The caller raises
        ``CircuitOpenError`` on a ``False`` result; this method never raises.
        """
        if self._state in _PERMIT_ALL:
            return True

        if self._state is State.OPEN:
            return self._begin_probing_if_elapsed()

        if self._state is State.HALF_OPEN:
            return self._admit_probe()

        return False  # FORCED_OPEN

    def record(self, outcome: Outcome) -> None:
        """Record one completed call's outcome and evaluate any transition."""
        if self._state is State.CLOSED:
            self._window.record(outcome)
            self._evaluate_closed()
        elif self._state is State.HALF_OPEN:
            self._record_probe(outcome)
        elif self._state is State.METRICS_ONLY:
            self._window.record(outcome)

    def force_open(self) -> None:
        """Override to ``FORCED_OPEN``: reject all traffic until reset."""
        self._state = State.FORCED_OPEN
        self._reset_probes()

    def disable(self) -> None:
        """Override to ``DISABLED``: admit all traffic, record nothing."""
        self._state = State.DISABLED
        self._reset_probes()

    def metrics_only(self) -> None:
        """Override to ``METRICS_ONLY``: admit all traffic, record but never trip."""
        self._state = State.METRICS_ONLY
        self._reset_probes()

    def reset(self) -> None:
        """Return to ``CLOSED`` with a fresh window, discarding past metrics."""
        self._close()

    def _begin_probing_if_elapsed(self) -> bool:
        if self._clock.monotonic() - self._opened_at < self._config.wait_duration_in_open:
            return False

        self._to_half_open()
        return self._admit_probe()

    def _admit_probe(self) -> bool:
        # Cap total probes (don't hammer a barely-recovered dependency) and how
        # many run at once (else the whole parallel load floods in as probes).
        if (
            self._probes_in_flight >= self._config.max_concurrent_probes
            or self._probes_admitted >= self._config.permitted_calls_in_half_open
        ):
            return False

        self._probes_admitted += 1
        self._probes_in_flight += 1
        return True

    def _evaluate_closed(self) -> None:
        snapshot = self._window.snapshot()
        if snapshot.total_calls < self._config.minimum_number_of_calls:
            return

        if self._exceeds_threshold(
            failure_rate=snapshot.failure_rate,
            slow_call_rate=snapshot.slow_call_rate,
        ):
            self._open()

    def _record_probe(self, outcome: Outcome) -> None:
        self._probes_in_flight -= 1
        self._probes_completed += 1
        self._probe_failures += outcome.is_failure
        self._probe_slows += outcome.is_slow

        if self._probes_completed >= self._config.permitted_calls_in_half_open:
            self._evaluate_probes()

    def _evaluate_probes(self) -> None:
        completed = self._probes_completed
        if self._exceeds_threshold(
            failure_rate=self._probe_failures / completed,
            slow_call_rate=self._probe_slows / completed,
        ):
            self._open()
        else:
            self._close()

    def _exceeds_threshold(self, *, failure_rate: float, slow_call_rate: float) -> bool:
        return (
            failure_rate >= self._config.failure_rate_threshold
            or slow_call_rate >= self._config.slow_call_rate_threshold
        )

    def _open(self) -> None:
        self._state = State.OPEN
        self._opened_at = self._clock.monotonic()

    def _to_half_open(self) -> None:
        self._state = State.HALF_OPEN
        self._reset_probes()

    def _close(self) -> None:
        self._state = State.CLOSED
        self._window = build_window(config=self._config, clock=self._clock)
        self._reset_probes()

    def _reset_probes(self) -> None:
        self._probes_admitted = 0
        self._probes_in_flight = 0
        self._probes_completed = 0
        self._probe_failures = 0
        self._probe_slows = 0
