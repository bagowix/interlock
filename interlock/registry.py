"""A registry of named circuit breakers.

Breakers are created lazily on first ``get`` and cached by name, so the same
name always returns the same instance. All breakers share the registry's
default ``Config`` unless a per-name override is supplied at creation.
"""

import threading
from contextlib import AsyncExitStack, ExitStack

from interlock._clock import SystemClock
from interlock._initial_state import validate_initial_state
from interlock.breaker import CircuitBreaker
from interlock.config import Config
from interlock.protocols import (
    AsyncStorage,
    Clock,
    CoreEventListener,
    FailureClassifier,
    Storage,
    StorageEventListener,
)
from interlock.state import State

__all__ = ('Registry',)


class Registry:
    """Creates and caches named circuit breakers.

    Args:
        config: Default config shared by breakers without an override.
            Defaults to ``Config()``.
        clock: Time source shared by all breakers. Defaults to ``SystemClock``.
        initial_state: Stable state assigned to every breaker before it is
            published from the registry. Use ``State.METRICS_ONLY`` for a
            shadow rollout. Defaults to ``State.CLOSED``.
        classifier: Failure policy shared by every breaker. Defaults to the
            breaker's own default (any raised exception is a failure).
        listener: Observability hooks shared by every breaker. Defaults to
            no observation.
        storage: Shared backend for coordinated state, handed to every breaker
            (each coordinates under its own name). Defaults to local state.

    Raises:
        ValueError: If ``initial_state`` is not a supported stable state.
    """

    def __init__(
        self,
        *,
        config: Config | None = None,
        clock: Clock | None = None,
        initial_state: State = State.CLOSED,
        classifier: FailureClassifier | None = None,
        listener: CoreEventListener | StorageEventListener | None = None,
        storage: Storage | AsyncStorage | None = None,
    ) -> None:
        validate_initial_state(initial_state)
        self._config = config if config is not None else Config()
        self._clock = clock if clock is not None else SystemClock()
        self._initial_state = initial_state
        self._classifier = classifier
        self._listener = listener
        self._storage = storage
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
        # A hit reads the dict without the lock: this is the per-request path of
        # every transport integration, and a breaker is never replaced or
        # removed once cached, so the worst a concurrent creation can do is send
        # this reader down the locked path below.
        cached = self._breakers.get(name)
        if cached is not None:
            return cached

        with self._lock:
            breaker = self._breakers.get(name)
            if breaker is None:
                breaker = CircuitBreaker(
                    name=name,
                    config=config if config is not None else self._config,
                    clock=self._clock,
                    initial_state=self._initial_state,
                    classifier=self._classifier,
                    listener=self._listener,
                    storage=self._storage,
                )
                self._breakers[name] = breaker

            return breaker

    def get_existing(self, name: str) -> CircuitBreaker | None:
        """Return a cached breaker without creating one for a missing name."""
        with self._lock:
            return self._breakers.get(name)

    def close_all(self) -> None:
        """Release the background resources of every breaker created so far.

        Breakers stay cached — ``get`` keeps returning the same, now torn-down
        instances rather than silently resurrecting a lane after shutdown.

        Raises:
            InterlockError: If the registry's storage is asynchronous; use
                ``aclose_all`` there.
        """
        with ExitStack() as stack:
            for breaker in self._snapshot():
                stack.callback(breaker.close)

    async def aclose_all(self) -> None:
        """Release every breaker's background resources; the async mirror.

        Raises:
            InterlockError: If the registry's storage is synchronous; use
                ``close_all`` there.
        """
        async with AsyncExitStack() as stack:
            for breaker in self._snapshot():
                stack.push_async_callback(breaker.aclose)

    def _snapshot(self) -> tuple[CircuitBreaker, ...]:
        """The breakers created so far; taken under the lock, closed outside it."""
        with self._lock:
            return tuple(self._breakers.values())
