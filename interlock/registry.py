"""A registry of named circuit breakers.

Breakers are created lazily on first ``get`` and cached by name, so the same
name always returns the same instance. All breakers share the registry's
default ``Config`` unless a per-name override is supplied at creation.
"""

import threading

from interlock._clock import SystemClock
from interlock.breaker import CircuitBreaker
from interlock.config import Config
from interlock.protocols import Clock, EventListener, FailureClassifier

__all__ = ('Registry',)


class Registry:
    """Creates and caches named circuit breakers.

    Args:
        config: Default config shared by breakers without an override.
            Defaults to ``Config()``.
        clock: Time source shared by all breakers. Defaults to ``SystemClock``.
        classifier: Failure policy shared by every breaker. Defaults to the
            breaker's own default (any raised exception is a failure).
        listener: Observability hooks shared by every breaker. Defaults to
            no observation.
    """

    def __init__(
        self,
        *,
        config: Config | None = None,
        clock: Clock | None = None,
        classifier: FailureClassifier | None = None,
        listener: EventListener | None = None,
    ) -> None:
        self._config = config if config is not None else Config()
        self._clock = clock if clock is not None else SystemClock()
        self._classifier = classifier
        self._listener = listener
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()

    def get(self, name: str, *, config: Config | None = None) -> CircuitBreaker:
        """Return the breaker named ``name``, creating it on first request.

        Args:
            name: The breaker's name.
            config: Config for this breaker if it does not exist yet; otherwise
                the registry default is used. Ignored once the breaker exists.

        Returns:
            The cached or newly created breaker.
        """
        with self._lock:
            breaker = self._breakers.get(name)
            if breaker is None:
                breaker = CircuitBreaker(
                    name=name,
                    config=config if config is not None else self._config,
                    clock=self._clock,
                    classifier=self._classifier,
                    listener=self._listener,
                )
                self._breakers[name] = breaker

            return breaker
