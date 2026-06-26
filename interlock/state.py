"""Circuit breaker lifecycle states."""

from enum import StrEnum, auto

__all__ = ('State',)


class State(StrEnum):
    """The lifecycle state of a circuit breaker.

    Three core states model the breaker's reaction to downstream health
    (``CLOSED`` → ``OPEN`` → ``HALF_OPEN``); three special states are operator
    overrides for safe rollout and manual control:

    - ``FORCED_OPEN``: rejects all traffic regardless of metrics.
    - ``DISABLED``: passes all traffic, no metrics, breaker is a no-op.
    - ``METRICS_ONLY``: shadow/observe mode — passes all traffic and records
      metrics, but never trips. The key to tuning thresholds before enforcing.

    Values are stable lowercase identifiers used in logs and metrics.
    """

    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()
    FORCED_OPEN = auto()
    DISABLED = auto()
    METRICS_ONLY = auto()
