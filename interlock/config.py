"""Immutable circuit breaker configuration with eager validation."""

from dataclasses import dataclass

from interlock.window import WindowType

__all__ = ('Config',)


@dataclass(frozen=True, kw_only=True, slots=True)
class Config:
    """Thresholds, window and timing for a circuit breaker.

    Reusable across breakers: the registry shares one config and overrides per
    name. Failure classification (which exceptions/results count) is a separate
    concern, handled by the ``FailureClassifier``, not here.

    Defaults follow resilience4j: trip at 50% failures over at least 10 calls,
    treat calls slower than 60s as slow (but never trip on slowness alone until
    tuned), stay open 60s before a single probe is allowed.

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
    auto_transition: bool = False
    window_type: WindowType = WindowType.COUNT_BASED
    window_size: int = 100

    def __post_init__(self) -> None:
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
