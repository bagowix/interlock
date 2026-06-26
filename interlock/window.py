"""Sliding-window aggregates over recent call outcomes."""

from dataclasses import dataclass
from enum import StrEnum, auto

__all__ = ('WindowSnapshot', 'WindowType')


class WindowType(StrEnum):
    """Which sliding-window implementation a breaker uses.

    ``COUNT_BASED`` keeps the last N calls; ``TIME_BASED`` keeps calls from the
    last N seconds. ``window_size`` in the config is read accordingly.
    """

    COUNT_BASED = auto()
    TIME_BASED = auto()


@dataclass(frozen=True, slots=True)
class WindowSnapshot:
    """Immutable aggregate view of a sliding window at one instant.

    Rates are ``0.0`` for an empty window; the state machine gates on
    ``minimum_number_of_calls`` before trusting a rate, so an empty window
    never looks like a healthy one.
    """

    total_calls: int
    failed_calls: int
    slow_calls: int

    @property
    def failure_rate(self) -> float:
        """Fraction of recorded calls that failed (0.0 when empty)."""
        if self.total_calls == 0:
            return 0.0

        return self.failed_calls / self.total_calls

    @property
    def slow_call_rate(self) -> float:
        """Fraction of recorded calls that were slow (0.0 when empty)."""
        if self.total_calls == 0:
            return 0.0

        return self.slow_calls / self.total_calls
