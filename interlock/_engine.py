"""The ``call()`` primitive: detect, dispatch, time, classify, record.

This is the I/O-aware layer wrapping the I/O-free ``StateMachine``. It owns a
single ``threading.Lock`` and holds it only around the two await-free critical
sections — admitting a call (``acquire``) and recording its outcome
(``record``). The protected callable runs *outside* the lock, so a slow
downstream never serialises throughput and a re-entrant call cannot deadlock.

A single instance serves both sync and async callers: ``call`` detects the
callable's nature via ``is_async_callable`` and dispatches to ``call_sync`` or
``call_async``. The lock is a ``threading.Lock`` because the critical sections
never ``await``; it is correct for threads and for a single event loop alike.
"""

import threading
from collections.abc import Awaitable, Callable
from typing import cast

from interlock._classify import DefaultFailureClassifier
from interlock._detect import is_async_callable
from interlock._state_machine import StateMachine
from interlock._typing import AsyncCallable, P, R, SyncCallable
from interlock.config import Config
from interlock.errors import CircuitOpenError
from interlock.outcome import Outcome
from interlock.protocols import Clock, EventListener, FailureClassifier
from interlock.state import State
from interlock.window import WindowSnapshot

__all__ = ('Engine',)

_OUTCOME_BY_FLAGS = {
    (False, False): Outcome.SUCCESS,
    (True, False): Outcome.FAILURE,
    (False, True): Outcome.SLOW_SUCCESS,
    (True, True): Outcome.SLOW_FAILURE,
}


class _NoopListener:
    """Null EventListener used when none is configured.

    Lets the engine always call ``self._listener.<hook>(...)`` without a None
    check; every hook is a no-op.
    """

    def on_state_change(self, *, name: str, old: State, new: State) -> None: ...
    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None: ...
    def on_rejected(self, *, name: str) -> None: ...
    def on_reset(self, *, name: str) -> None: ...


_NOOP_LISTENER: EventListener = _NoopListener()


class Engine:
    """Runs callables under one breaker, mediating the state machine.

    Args:
        name: Breaker name, surfaced on ``CircuitOpenError``.
        config: Thresholds, window and timing.
        clock: Time source; injected for deterministic tests.
        classifier: Decides which outcomes count as failures. Defaults to
            ``DefaultFailureClassifier`` (any raised exception is a failure).
        listener: Observability hooks. Defaults to a no-op listener.
    """

    def __init__(
        self,
        *,
        name: str,
        config: Config,
        clock: Clock,
        classifier: FailureClassifier | None = None,
        listener: EventListener | None = None,
    ) -> None:
        self._name = name
        self._config = config
        self._clock = clock
        self._classifier = classifier if classifier is not None else DefaultFailureClassifier()
        self._listener = listener if listener is not None else _NOOP_LISTENER
        self._machine = StateMachine(config=config, clock=clock)
        self._lock = threading.Lock()
        self._last_failure: BaseException | None = None
        self._timer: threading.Timer | None = None
        self._timer_lock = threading.Lock()

    @property
    def state(self) -> State:
        """The breaker's current lifecycle state."""
        return self._machine.state

    def snapshot(self) -> WindowSnapshot:
        """An immutable view of the current window aggregates."""
        return self._machine.snapshot()

    def call(
        self,
        fn: AsyncCallable[P, R] | SyncCallable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Awaitable[R] | R:
        """Run ``fn`` under protection, dispatching on its sync/async nature.

        Returns an awaitable for a coroutine function and the plain result for a
        synchronous one; the precise sync/async-preserving typing is supplied by
        the public ``CircuitBreaker`` surface (M5) over this primitive.
        """
        if is_async_callable(fn):
            return self.call_async(cast('AsyncCallable[P, R]', fn), *args, **kwargs)

        return self.call_sync(cast('SyncCallable[P, R]', fn), *args, **kwargs)

    def call_sync(self, fn: SyncCallable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        """Run a synchronous ``fn`` under protection.

        Raises:
            CircuitOpenError: If the breaker rejects the call.
        """
        self._admit()
        start = self._clock.monotonic()

        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            self._settle(result=None, exception=exc, start=start)
            raise
        else:
            self._settle(result=result, exception=None, start=start)
            return result

    async def call_async(self, fn: AsyncCallable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        """Run an asynchronous ``fn`` under protection.

        Raises:
            CircuitOpenError: If the breaker rejects the call.
        """
        self._admit()
        start = self._clock.monotonic()

        try:
            result = await fn(*args, **kwargs)
        except Exception as exc:
            self._settle(result=None, exception=exc, start=start)
            raise
        else:
            self._settle(result=result, exception=None, start=start)
            return result

    def enter_block(self) -> float:
        """Admit a guarded block and return its start time.

        Backs the context-manager surface, where there is no callable to run —
        only a block whose exception and duration are observed.

        Raises:
            CircuitOpenError: If the breaker rejects the block.
        """
        self._admit()
        return self._clock.monotonic()

    def exit_block(self, *, start: float, exception: BaseException | None) -> None:
        """Record a guarded block's outcome from its exception and duration."""
        if exception is not None and not isinstance(exception, Exception):
            return  # mirror call(): cancellation/shutdown are not downstream failures
        self._settle(result=None, exception=exception, start=start)

    def reset(self) -> None:
        """Return to ``CLOSED`` with a fresh window, discarding past metrics."""
        with self._lock:
            before = self._machine.state
            self._machine.reset()
            after = self._machine.state

        self._on_transition(before, after)
        self._listener.on_reset(name=self._name)

    def force_open(self) -> None:
        """Override to ``FORCED_OPEN``: reject all traffic until reset."""
        self._override(self._machine.force_open)

    def disable(self) -> None:
        """Override to ``DISABLED``: admit all traffic, record nothing."""
        self._override(self._machine.disable)

    def metrics_only(self) -> None:
        """Override to ``METRICS_ONLY``: admit all traffic, record but never trip."""
        self._override(self._machine.metrics_only)

    def _override(self, mutate: Callable[[], None]) -> None:
        with self._lock:
            before = self._machine.state
            mutate()
            after = self._machine.state

        self._on_transition(before, after)

    def _admit(self) -> None:
        with self._lock:
            before = self._machine.state
            admitted = self._machine.acquire()
            after = self._machine.state
            retry_after = None if admitted else self._machine.retry_after()
            last_failure = self._last_failure

        self._on_transition(before, after)
        if not admitted:
            self._listener.on_rejected(name=self._name)
            raise CircuitOpenError(
                self._name,
                retry_after=retry_after,
                last_failure=last_failure,
            )

    def _settle(self, *, result: object, exception: Exception | None, start: float) -> None:
        duration = self._clock.monotonic() - start
        failure = self._classifier.is_failure(result=result, exception=exception)
        slow = duration >= self._config.slow_call_duration_threshold
        outcome = _OUTCOME_BY_FLAGS[failure, slow]

        with self._lock:
            before = self._machine.state
            self._machine.record(outcome)
            after = self._machine.state
            if failure and exception is not None:
                self._last_failure = exception

        self._listener.on_call(name=self._name, outcome=outcome, duration=duration)
        self._on_transition(before, after)

    def _on_transition(self, before: State, after: State) -> None:
        if after == before:
            return

        self._listener.on_state_change(name=self._name, old=before, new=after)
        if after is State.OPEN:
            self._schedule_auto_transition()
        elif before is State.OPEN:
            self._cancel_auto_transition()

    def _schedule_auto_transition(self) -> None:
        """Arm a timer to fire the proactive OPEN → HALF_OPEN once the wait ends.

        A no-op unless ``auto_transition`` is enabled. The timer is a daemon so a
        pending one never blocks interpreter shutdown, and it is cancelled the
        moment the breaker leaves ``OPEN`` by any path (a real probe, ``reset``,
        ``force_open`` or the timer itself).
        """
        if not self._config.auto_transition:
            return

        timer = threading.Timer(self._config.wait_duration_in_open, self._fire_auto_transition)
        timer.daemon = True
        with self._timer_lock:
            self._cancel_timer_locked()
            self._timer = timer

        timer.start()

    def _cancel_auto_transition(self) -> None:
        with self._timer_lock:
            self._cancel_timer_locked()

    def _cancel_timer_locked(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _fire_auto_transition(self) -> None:
        """Timer callback: flip OPEN → HALF_OPEN under the lock, then emit.

        Races with a real call are settled by the lock: whichever acquires it
        first performs the single transition; the loser sees a non-``OPEN`` state,
        so ``attempt_auto_transition`` is a no-op and ``_on_transition`` (which
        ignores an unchanged state) emits nothing. The state changes exactly once
        and the event fires exactly once.
        """
        with self._lock:
            before = self._machine.state
            self._machine.attempt_auto_transition()
            after = self._machine.state

        self._on_transition(before, after)
