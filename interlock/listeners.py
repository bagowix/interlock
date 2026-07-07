"""Built-in ``EventListener`` implementations (zero-dependency)."""

import logging

from interlock.outcome import Outcome
from interlock.state import State

__all__ = ('LoggingEventListener',)


class LoggingEventListener:
    """An ``EventListener`` that logs every breaker event via stdlib logging.

    State changes and rejections log at ``WARNING`` (operationally significant),
    resets at ``INFO``, and individual calls at ``DEBUG`` (high volume).

    Args:
        logger: Logger to write to. Defaults to ``logging.getLogger('interlock')``.
    """

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._log = logger if logger is not None else logging.getLogger('interlock')

    def on_state_change(self, *, name: str, old: State, new: State) -> None:
        """Log a state transition at WARNING."""
        self._log.warning('circuit %r: state %s -> %s', name, old, new)

    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None:
        """Log a completed call at DEBUG."""
        self._log.debug('circuit %r: call %s in %.3fs', name, outcome, duration)

    def on_rejected(self, *, name: str) -> None:
        """Log a rejected call at WARNING."""
        self._log.warning('circuit %r: call rejected (open)', name)

    def on_reset(self, *, name: str) -> None:
        """Log a manual reset at INFO."""
        self._log.info('circuit %r: reset', name)

    def on_storage_degraded(self, *, name: str, error: BaseException) -> None:
        """Log a storage degradation at WARNING."""
        self._log.warning('circuit %r: shared storage degraded, running local: %s', name, error)

    def on_storage_recovered(self, *, name: str) -> None:
        """Log a storage recovery at INFO."""
        self._log.info('circuit %r: shared storage recovered', name)
