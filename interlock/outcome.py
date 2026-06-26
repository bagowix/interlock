"""Classification of a single protected call's result."""

from enum import StrEnum, auto

__all__ = ('Outcome',)


class Outcome(StrEnum):
    """The outcome of one protected call, crossing success/failure with latency.

    Slow-call detection is orthogonal to success: a call can succeed yet be
    slow. The sliding window counts ``failure_rate`` and ``slow_call_rate``
    independently, so both dimensions are encoded in a single value rather than
    tracked as two separate flags.
    """

    SUCCESS = auto()
    FAILURE = auto()
    SLOW_SUCCESS = auto()
    SLOW_FAILURE = auto()

    @property
    def is_failure(self) -> bool:
        """Whether this outcome counts toward the failure rate."""
        return self in (Outcome.FAILURE, Outcome.SLOW_FAILURE)

    @property
    def is_slow(self) -> bool:
        """Whether this outcome counts toward the slow-call rate."""
        return self in (Outcome.SLOW_SUCCESS, Outcome.SLOW_FAILURE)
