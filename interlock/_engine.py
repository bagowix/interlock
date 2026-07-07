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
from dataclasses import dataclass
from inspect import iscoroutinefunction
from typing import cast

from interlock._classify import DefaultFailureClassifier
from interlock._coordination import AsyncCoordinator, SyncCoordinator
from interlock._detect import is_async_callable
from interlock._state_machine import StateMachine
from interlock._typing import AsyncCallable, P, R, SyncCallable
from interlock.config import Config
from interlock.errors import CircuitOpenError, InterlockError
from interlock.outcome import Outcome
from interlock.protocols import AsyncStorage, Clock, EventListener, FailureClassifier, Storage
from interlock.shared import SharedState
from interlock.state import State
from interlock.window import WindowSnapshot

__all__ = ('Engine',)

_OUTCOME_BY_FLAGS = {
    (False, False): Outcome.SUCCESS,
    (True, False): Outcome.FAILURE,
    (False, True): Outcome.SLOW_SUCCESS,
    (True, True): Outcome.SLOW_FAILURE,
}

# Shared states that override local admission (a shared CLOSED defers to local).
_SHARED_AUTHORITATIVE = frozenset({State.OPEN, State.HALF_OPEN})


class _NoopListener:
    """Null EventListener used when none is configured.

    Lets the engine always call ``self._listener.<hook>(...)`` without a None
    check; every hook is a no-op.
    """

    def on_state_change(self, *, name: str, old: State, new: State) -> None: ...
    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None: ...
    def on_rejected(self, *, name: str) -> None: ...
    def on_reset(self, *, name: str) -> None: ...
    def on_storage_degraded(self, *, name: str, error: BaseException) -> None: ...
    def on_storage_recovered(self, *, name: str) -> None: ...


_NOOP_LISTENER: EventListener = _NoopListener()


def _notify_safely(listener: EventListener, hook: str, **kwargs: object) -> None:
    """Dispatch a storage hook via ``getattr`` so pre-1.2 listeners keep working."""
    method = getattr(listener, hook, None)
    if callable(method):
        method(**kwargs)


@dataclass(frozen=True, slots=True)
class Admission:
    """What ``_admit`` granted: the era it happened in, and probe provenance."""

    generation: int
    probe: bool = False


class Engine:
    """Runs callables under one breaker, mediating the state machine.

    Args:
        name: Breaker name, surfaced on ``CircuitOpenError``.
        config: Thresholds, window and timing.
        clock: Time source; injected for deterministic tests.
        classifier: Decides which outcomes count as failures. Defaults to
            ``DefaultFailureClassifier`` (any raised exception is a failure).
        listener: Observability hooks. Defaults to a no-op listener.
        storage: Optional shared backend for coordinated, distributed state.
            ``None`` (the default) keeps the breaker purely local. A coordinated
            breaker matches its storage's runtime: a sync ``Storage`` serves
            only the sync API, an ``AsyncStorage`` only the async one.
    """

    def __init__(
        self,
        *,
        name: str,
        config: Config,
        clock: Clock,
        classifier: FailureClassifier | None = None,
        listener: EventListener | None = None,
        storage: Storage | AsyncStorage | None = None,
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
        self._shared_view: SharedState | None = None
        self._storage_degraded = False
        self._sync_coordinator: SyncCoordinator | None = None
        self._async_coordinator: AsyncCoordinator | None = None
        if storage is not None:
            if iscoroutinefunction(storage.read):
                self._async_coordinator = AsyncCoordinator(
                    name=name,
                    config=config,
                    clock=clock,
                    storage=cast('AsyncStorage', storage),
                    on_view=self._on_shared_view,
                    on_degraded=self._on_storage_degraded,
                    on_recovered=self._on_storage_recovered,
                )
            else:
                self._sync_coordinator = SyncCoordinator(
                    name=name,
                    config=config,
                    clock=clock,
                    storage=cast('Storage', storage),
                    on_view=self._on_shared_view,
                    on_degraded=self._on_storage_degraded,
                    on_recovered=self._on_storage_recovered,
                )

    @property
    def state(self) -> State:
        """The breaker's effective lifecycle state.

        In coordinated mode a shared OPEN/HALF_OPEN overrides the local state
        (it governs admission); otherwise — including while the storage is
        degraded — the local state machine's state is reported.
        """
        with self._lock:
            return self._effective_state_locked()

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
        admission = self._admit()
        start = self._clock.monotonic()

        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            self._settle(result=None, exception=exc, start=start, admission=admission)
            raise
        else:
            self._settle(result=result, exception=None, start=start, admission=admission)
            return result

    async def call_async(self, fn: AsyncCallable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        """Run an asynchronous ``fn`` under protection.

        Raises:
            CircuitOpenError: If the breaker rejects the call.
            InterlockError: If the breaker is coordinated through a sync storage.
        """
        admission = await self._admit_async()
        start = self._clock.monotonic()

        try:
            result = await fn(*args, **kwargs)
        except Exception as exc:
            self._settle(result=None, exception=exc, start=start, admission=admission)
            raise
        else:
            self._settle(result=result, exception=None, start=start, admission=admission)
            return result

    def enter_block(self) -> tuple[float, Admission]:
        """Admit a guarded block; return its start time and admission.

        Backs the context-manager surface, where there is no callable to run —
        only a block whose exception and duration are observed. The admission
        is handed back to ``exit_block`` so a block that outlives a state
        transition is not recorded into the wrong era.

        Raises:
            CircuitOpenError: If the breaker rejects the block.
        """
        admission = self._admit()
        return self._clock.monotonic(), admission

    async def enter_block_async(self) -> tuple[float, Admission]:
        """Admit a guarded ``async with`` block; the async mirror of ``enter_block``.

        Raises:
            CircuitOpenError: If the breaker rejects the block.
            InterlockError: If the breaker is coordinated through a sync storage.
        """
        admission = await self._admit_async()
        return self._clock.monotonic(), admission

    def exit_block(
        self, *, start: float, admission: Admission, exception: BaseException | None
    ) -> None:
        """Record a guarded block's outcome from its exception and duration."""
        if exception is not None and not isinstance(exception, Exception):
            return  # mirror call(): cancellation/shutdown are not downstream failures
        self._settle(result=None, exception=exception, start=start, admission=admission)

    def reset(self) -> None:
        """Return to ``CLOSED`` with a fresh window, discarding past metrics."""
        with self._lock:
            effective_before = self._effective_state_locked()
            before = self._machine.state
            self._machine.reset()
            after = self._machine.state
            effective_after = self._effective_state_locked()
            self._last_failure = None

        self._emit_transitions(before, after, effective_before, effective_after)
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
            effective_before = self._effective_state_locked()
            before = self._machine.state
            mutate()
            after = self._machine.state
            effective_after = self._effective_state_locked()

        self._emit_transitions(before, after, effective_before, effective_after)

    def _admit(self) -> Admission:
        """Admit one synchronous call.

        Raises:
            CircuitOpenError: If the breaker rejects the call.
            InterlockError: If the breaker is coordinated through an async storage.
        """
        if self._async_coordinator is not None:
            msg = (
                f'circuit {self._name!r} is coordinated through an async storage; '
                f'only its async API may be used'
            )
            raise InterlockError(msg)

        coordinator = self._sync_coordinator
        if coordinator is not None:
            coordinator.ensure_lane()
            if self._shared_gate() is State.HALF_OPEN:
                granted = coordinator.try_lease()
                if granted is not None:
                    return self._finish_lease(granted=granted)
                # storage degraded mid-lease: fall through to local admission

        return self._admit_local()

    async def _admit_async(self) -> Admission:
        """Admit one asynchronous call; the async mirror of ``_admit``."""
        if self._sync_coordinator is not None:
            msg = (
                f'circuit {self._name!r} is coordinated through a sync storage; '
                f'only its sync API may be used'
            )
            raise InterlockError(msg)

        coordinator = self._async_coordinator
        if coordinator is not None:
            coordinator.ensure_lane()
            if self._shared_gate() is State.HALF_OPEN:
                granted = await coordinator.try_lease()
                if granted is not None:
                    return self._finish_lease(granted=granted)

        return self._admit_local()

    def _shared_gate(self) -> State | None:
        """The shared state governing admission, or ``None`` when local rules.

        Raises ``CircuitOpenError`` directly for a shared OPEN — no probe can be
        admitted there, and local state must not overrule a coordinated trip.
        """
        with self._lock:
            view = self._shared_view
            if view is None or self._storage_degraded:
                return None
            shared = view.state
            last_failure = self._last_failure

        if shared is State.OPEN:
            self._listener.on_rejected(name=self._name)
            raise CircuitOpenError(self._name, retry_after=None, last_failure=last_failure)

        return shared if shared is State.HALF_OPEN else None

    def _finish_lease(self, *, granted: bool) -> Admission:
        """Turn a shared probe-lease outcome into an admission or a rejection."""
        with self._lock:
            generation = self._machine.generation
            last_failure = self._last_failure

        if not granted:
            self._listener.on_rejected(name=self._name)
            raise CircuitOpenError(self._name, retry_after=None, last_failure=last_failure)

        return Admission(generation=generation, probe=True)

    def _admit_local(self) -> Admission:
        """Admit one call by local state alone (the only path when not coordinated)."""
        with self._lock:
            effective_before = self._effective_state_locked()
            before = self._machine.state
            admitted = self._machine.acquire()
            after = self._machine.state
            effective_after = self._effective_state_locked()
            generation = self._machine.generation
            retry_after = None if admitted else self._machine.retry_after()
            last_failure = self._last_failure

        self._emit_transitions(before, after, effective_before, effective_after)
        if not admitted:
            self._listener.on_rejected(name=self._name)
            raise CircuitOpenError(
                self._name,
                retry_after=retry_after,
                last_failure=last_failure,
            )

        return Admission(generation=generation)

    def _settle(
        self, *, result: object, exception: Exception | None, start: float, admission: Admission
    ) -> None:
        duration = self._clock.monotonic() - start
        failure = self._classifier.is_failure(result=result, exception=exception)
        slow = duration >= self._config.slow_call_duration_threshold
        outcome = _OUTCOME_BY_FLAGS[failure, slow]

        with self._lock:
            effective_before = self._effective_state_locked()
            before = self._machine.state
            self._machine.record(outcome, generation=admission.generation)
            after = self._machine.state
            effective_after = self._effective_state_locked()
            if failure and exception is not None:
                self._last_failure = exception

        self._listener.on_call(name=self._name, outcome=outcome, duration=duration)
        self._emit_transitions(before, after, effective_before, effective_after)

        coordinator = self._sync_coordinator or self._async_coordinator
        if coordinator is not None:
            if admission.probe:
                coordinator.notify_probe_outcome(outcome)
            if before is State.CLOSED and after is State.OPEN:
                coordinator.notify_local_trip()

    def _effective_state_locked(self) -> State:
        """The state that governs admission; caller must hold ``self._lock``."""
        view = self._shared_view
        if view is not None and not self._storage_degraded and view.state in _SHARED_AUTHORITATIVE:
            return view.state
        return self._machine.state

    def _emit_transitions(
        self,
        machine_before: State,
        machine_after: State,
        effective_before: State,
        effective_after: State,
    ) -> None:
        """Emit the observable transition and manage the auto-transition timer.

        Events reflect the *effective* state (what callers experience); the
        timer tracks the *local machine* (only it performs the lazy
        OPEN → HALF_OPEN). In local mode the two are identical.
        """
        if effective_after != effective_before:
            self._listener.on_state_change(
                name=self._name, old=effective_before, new=effective_after
            )

        if machine_after == machine_before:
            return
        if machine_after is State.OPEN:
            self._schedule_auto_transition()
        elif machine_before is State.OPEN:
            self._cancel_auto_transition()

    def _on_shared_view(self, view: SharedState) -> None:
        """Coordinator callback: adopt a fresh shared view (any thread).

        A shared transition to CLOSED with a newer version means some
        instance's probes passed globally — a locally open machine adopts the
        recovery (fresh window) instead of waiting out its own OPEN.
        """
        with self._lock:
            previous = self._shared_view
            effective_before = self._effective_state_locked()
            before = self._machine.state
            self._shared_view = view
            if (
                view.state is State.CLOSED
                and previous is not None
                and previous.state in _SHARED_AUTHORITATIVE
                and view.version > previous.version
                and self._machine.state in _SHARED_AUTHORITATIVE
            ):
                self._machine.reset()
            after = self._machine.state
            effective_after = self._effective_state_locked()

        self._emit_transitions(before, after, effective_before, effective_after)

    def _on_storage_degraded(self, error: BaseException) -> None:
        """Coordinator callback: storage failed; local state takes over."""
        with self._lock:
            effective_before = self._effective_state_locked()
            machine_state = self._machine.state
            self._storage_degraded = True
            effective_after = self._effective_state_locked()

        _notify_safely(self._listener, 'on_storage_degraded', name=self._name, error=error)
        self._emit_transitions(machine_state, machine_state, effective_before, effective_after)

    def _on_storage_recovered(self) -> None:
        """Coordinator callback: storage is back; the shared view is authoritative."""
        with self._lock:
            effective_before = self._effective_state_locked()
            machine_state = self._machine.state
            self._storage_degraded = False
            effective_after = self._effective_state_locked()

        _notify_safely(self._listener, 'on_storage_recovered', name=self._name)
        self._emit_transitions(machine_state, machine_state, effective_before, effective_after)

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
            effective_before = self._effective_state_locked()
            before = self._machine.state
            self._machine.attempt_auto_transition()
            after = self._machine.state
            effective_after = self._effective_state_locked()

        self._emit_transitions(before, after, effective_before, effective_after)
