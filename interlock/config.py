"""Immutable circuit breaker configuration with eager validation."""

import math
from dataclasses import dataclass
from typing import Final

from interlock.window import WindowType

__all__ = ('Config',)

# The exponent stops here. Sixty-four rounds of any sane multiplier already dwarf
# every ceiling a caller would set, while an unbounded count walks a long-broken
# dependency into float's range: the wait becomes infinite, never elapses, and the
# breaker can never leave OPEN again. With wait_duration_in_open_max set the wait
# itself stops growing but the count does not, so days of failure get there.
MAX_BACKOFF_ROUNDS: Final[int] = 64


@dataclass(frozen=True, kw_only=True, slots=True)
class Config:
    """Thresholds, window and timing for a circuit breaker.

    Reusable across breakers: the registry shares one config and overrides per
    name. Failure classification (which exceptions/results count) is a separate
    concern, handled by the ``FailureClassifier``, not here.

    Defaults follow resilience4j: trip at 50% failures over at least 10 calls,
    treat calls slower than 60s as slow (but never trip on slowness alone until
    tuned), stay open 60s before a single probe is allowed.

    ``wait_duration_backoff_multiplier`` lengthens the open wait after each probe
    round that fails back-to-back, capped by ``wait_duration_in_open_max``. It
    defaults to ``1.0`` — a constant wait, the historical behaviour. Raise it when
    a breaker can fail its probes for a reason waiting alone will not fix: a
    constant wait then retries forever at full rate, and the growing interval is
    itself the signal that the dependency is not merely slow to recover.

    The backoff is a local decision: with a shared ``Storage`` the coordinated
    lane asks the backend to reopen after ``wait_duration_in_open``, and the
    failed-round count lives in no shared state, so a coordinated breaker retries
    on the base wait however many rounds have failed.

    ``auto_transition`` opts into a timer that proactively moves a breaker from
    ``OPEN`` to ``HALF_OPEN`` once ``wait_duration_in_open`` elapses, emitting the
    state change without waiting for the next call. It defaults to ``False``,
    preserving the lazy transition (which stays authoritative either way).

    Raises:
        ValueError: If any value is out of range or inconsistent.
    """

    failure_rate_threshold: float = 0.5
    minimum_number_of_calls: int = 10
    slow_call_duration_threshold: float = 60.0
    slow_call_rate_threshold: float = 1.0
    permitted_calls_in_half_open: int = 10
    max_concurrent_probes: int = 1
    wait_duration_in_open: float = 60.0
    wait_duration_backoff_multiplier: float = 1.0
    wait_duration_in_open_max: float | None = None
    auto_transition: bool = False
    window_type: WindowType = WindowType.COUNT_BASED
    window_size: int = 100

    def __post_init__(self) -> None:  # noqa: C901 - a flat list of guards, not branching logic
        if not 0.0 < self.failure_rate_threshold <= 1.0:
            raise ValueError(
                f'failure_rate_threshold must be in (0, 1], got {self.failure_rate_threshold!r}'
            )
        if not 0.0 < self.slow_call_rate_threshold <= 1.0:
            raise ValueError(
                f'slow_call_rate_threshold must be in (0, 1], got {self.slow_call_rate_threshold!r}'
            )
        if self.minimum_number_of_calls < 1:
            raise ValueError(
                f'minimum_number_of_calls must be >= 1, got {self.minimum_number_of_calls!r}'
            )
        if self.slow_call_duration_threshold <= 0.0:
            raise ValueError(
                f'slow_call_duration_threshold must be > 0, '
                f'got {self.slow_call_duration_threshold!r}'
            )
        if self.wait_duration_in_open <= 0.0:
            raise ValueError(
                f'wait_duration_in_open must be > 0, got {self.wait_duration_in_open!r}'
            )
        # Both multiplier guards sit behind one comparison: at the default of 1.0
        # there is no backoff to check, and the exponentiation would be a no-op
        # computed on every construction.
        if self.wait_duration_backoff_multiplier != 1.0:
            if self.wait_duration_backoff_multiplier < 1.0:
                raise ValueError(
                    f'wait_duration_backoff_multiplier must be >= 1, '
                    f'got {self.wait_duration_backoff_multiplier!r}'
                )
            if not math.isfinite(self._peak_wait_duration()):
                raise ValueError(
                    f'wait_duration_backoff_multiplier is too large for '
                    f'wait_duration_in_open={self.wait_duration_in_open!r}: after '
                    f'{MAX_BACKOFF_ROUNDS} rounds the wait stops being a finite number, '
                    f'got {self.wait_duration_backoff_multiplier!r}'
                )
        if (
            self.wait_duration_in_open_max is not None
            and self.wait_duration_in_open_max < self.wait_duration_in_open
        ):
            raise ValueError(
                f'wait_duration_in_open_max must be >= wait_duration_in_open '
                f'({self.wait_duration_in_open!r}), got {self.wait_duration_in_open_max!r}'
            )
        if self.permitted_calls_in_half_open < 1:
            raise ValueError(
                f'permitted_calls_in_half_open must be >= 1, '
                f'got {self.permitted_calls_in_half_open!r}'
            )
        if not 1 <= self.max_concurrent_probes <= self.permitted_calls_in_half_open:
            raise ValueError(
                f'max_concurrent_probes must be in [1, {self.permitted_calls_in_half_open}], '
                f'got {self.max_concurrent_probes!r}'
            )
        if self.window_size < 1:
            raise ValueError(f'window_size must be >= 1, got {self.window_size!r}')

    def _peak_wait_duration(self) -> float:
        """The longest wait the backoff can produce, or infinity if it overruns.

        A wait that is not a finite number never elapses, so the breaker it
        governs could never leave ``OPEN``. Catching that here keeps the failure
        at construction, where the offending value is in front of the caller.
        """
        try:
            return self.wait_duration_in_open * (
                self.wait_duration_backoff_multiplier**MAX_BACKOFF_ROUNDS
            )
        except OverflowError:
            return math.inf
