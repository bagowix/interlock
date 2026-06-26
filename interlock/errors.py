"""Exception and warning hierarchy for interlock."""

__all__ = (
    'CallTimeoutError',
    'CircuitOpenError',
    'InterlockDeprecationWarning',
    'InterlockError',
)


class InterlockError(Exception):
    """Base class for all errors raised by interlock."""


class CircuitOpenError(InterlockError):
    """Raised when a call is rejected because the circuit is not closed.

    Carries enough context to act on the rejection without inspecting the
    breaker: which breaker rejected the call, roughly how long until the next
    probe is allowed, and the most recent recorded failure (if any).

    Args:
        breaker_name: Name of the breaker that rejected the call.
        retry_after: Seconds until the next probe is allowed, or ``None`` when
            the breaker cannot estimate it (e.g. ``FORCED_OPEN``).
        last_failure: The most recent recorded failure, if any.
    """

    def __init__(
        self,
        breaker_name: str,
        *,
        retry_after: float | None = None,
        last_failure: BaseException | None = None,
    ) -> None:
        self.breaker_name = breaker_name
        self.retry_after = retry_after
        self.last_failure = last_failure

        super().__init__(self._build_message())

    def _build_message(self) -> str:
        message = f'Circuit {self.breaker_name!r} is open'
        if self.retry_after is not None:
            message = f'{message}; retry in ~{self.retry_after:.3f}s'

        return message


class CallTimeoutError(InterlockError):
    """Raised when a guarded operation exceeds its timeout deadline.

    A call that hangs forever would never be counted as slow or failed — it just
    holds a resource; the timeout converts it into a failure a surrounding
    breaker can observe.

    Args:
        timeout: The deadline, in seconds, that was exceeded.
    """

    def __init__(self, timeout: float) -> None:
        self.timeout = timeout
        message = f'Operation exceeded its {timeout:.3f}s timeout'
        super().__init__(message)


class InterlockDeprecationWarning(UserWarning):
    """Deprecation warning that is visible by default.

    Subclasses ``UserWarning`` rather than ``DeprecationWarning`` so it is
    shown to end users without enabling the deprecation filter.
    """
