"""Extension-point contracts for the circuit breaker core.

The core depends only on these protocols, never on concrete implementations.
This keeps the state machine I/O-free and lets storage, windows, clocks,
classification and observability be swapped without touching it.
"""

# EventListener hooks are deliberately concrete no-ops; their parameters form the public contract.
# ruff: noqa: ARG002, RET501

from typing import Protocol, runtime_checkable

from interlock.outcome import Outcome
from interlock.shared import ProbeLease, SharedState
from interlock.state import State
from interlock.window import WindowSnapshot

__all__ = (
    'AsyncStorage',
    'Clock',
    'CoreEventListener',
    'EventListener',
    'FailureClassifier',
    'PipelineEventListener',
    'SlidingWindow',
    'Storage',
    'StorageEventListener',
)


@runtime_checkable
class Clock(Protocol):
    """Source of time for the core. Injected so tests stay deterministic."""

    def monotonic(self) -> float:
        """Return a monotonically increasing, non-negative time in seconds.

        The reference point is arbitrary — only differences are ever used — but
        the values must not be negative: ``TimeBasedSlidingWindow`` buckets by
        whole seconds and reserves a negative second as its "never written"
        sentinel, so a clock that went below zero would report a permanently
        empty window and the breaker would never trip.
        """
        ...


@runtime_checkable
class SlidingWindow(Protocol):
    """Aggregates recent outcomes into failure and slow-call rates."""

    def record(self, outcome: Outcome) -> None:
        """Record one call outcome into the window."""
        ...

    def snapshot(self) -> WindowSnapshot:
        """Return an immutable view of the current aggregates."""
        ...


@runtime_checkable
class Storage(Protocol):
    """Coordinates breaker state across instances through a shared backend.

    Every operation is an atomic *intent*, not a raw read-modify-write: the
    backend owns the atomicity (e.g. a Lua script or a locked section), so racing
    instances never desync. The methods carry mechanism only — threshold policy
    stays in the core state machine, which calls ``trip_open`` / ``close`` once
    it has decided. ``ttl`` refreshes the key's expiry so abandoned instances let
    state self-expire.

    This is the synchronous contract; ``AsyncStorage`` mirrors it for async
    breakers. No method may raise into the protected path — the engine degrades
    to local state on backend failure.

    A custom implementation must uphold the fencing, probe-lease TTL and
    teardown guarantees described in the "Coordinated mode contract" section
    of ``docs/integrations/redis.md``, including its implementation checklist.
    """

    def read(self, name: str) -> SharedState | None:
        """Return the current shared view, or ``None`` if no key exists."""
        ...

    def trip_open(
        self, *, name: str, ttl: float, expected_version: int | None = None
    ) -> SharedState:
        """Transition to ``OPEN``, stamping the backend's time as ``opened_at``.

        Idempotent while already ``OPEN`` (the first opener's time stands);
        from ``HALF_OPEN`` it reopens with a fresh ``opened_at`` and clears
        probe accounting. With ``expected_version`` set, the transition is
        fenced: it applies only while the backend still holds that version, so
        a decision computed from a stale view cannot clobber a newer state.
        A fenced-out call is a no-op returning the current view.
        """
        ...

    def begin_half_open_if_elapsed(
        self, *, name: str, wait_duration: float, permitted: int, ttl: float
    ) -> SharedState:
        """Move ``OPEN`` → ``HALF_OPEN`` once ``wait_duration`` has elapsed.

        Atomically seeds the global probe budget (``permitted``) on the first
        successful transition; later calls observe ``HALF_OPEN`` and do not
        reseed. A no-op (returns the current view) until the wait elapses.
        """
        ...

    def lease_probe(self, *, name: str, ttl: float) -> ProbeLease:
        """Claim one global probe slot, decrementing the shared budget.

        ``granted`` is true only in ``HALF_OPEN`` with budget remaining; this
        bounds total probes across all instances against a recovering downstream.
        """
        ...

    def record_probe(self, *, name: str, outcome: Outcome, ttl: float) -> SharedState:
        """Tally one completed probe's outcome into the shared accounting.

        Tallies only while ``HALF_OPEN`` — a probe outcome that arrives after
        the state has already moved on (another instance tripped or closed) is
        dropped, returning the current view untouched. The caller treats
        ``probes_completed >= probes_permitted`` as "round finished, decide now".
        """
        ...

    def close(self, *, name: str, ttl: float, expected_version: int | None = None) -> SharedState:
        """Transition to ``CLOSED`` and clear probe accounting.

        With ``expected_version`` set, fenced exactly like ``trip_open``: a
        delayed "probes passed" decision cannot close a breaker that has since
        re-opened.
        """
        ...


@runtime_checkable
class AsyncStorage(Protocol):
    """Async mirror of ``Storage`` for async breakers.

    See ``Storage`` for the full contract, including the coordinated-mode
    contract a custom implementation must uphold; every method here is the
    awaitable counterpart.
    """

    async def read(self, name: str) -> SharedState | None:
        """Return the current shared view, or ``None`` if no key exists."""
        ...

    async def trip_open(
        self, *, name: str, ttl: float, expected_version: int | None = None
    ) -> SharedState:
        """Transition to ``OPEN``, stamping the backend's time as ``opened_at``."""
        ...

    async def begin_half_open_if_elapsed(
        self, *, name: str, wait_duration: float, permitted: int, ttl: float
    ) -> SharedState:
        """Move ``OPEN`` → ``HALF_OPEN`` once ``wait_duration`` has elapsed."""
        ...

    async def lease_probe(self, *, name: str, ttl: float) -> ProbeLease:
        """Claim one global probe slot, decrementing the shared budget."""
        ...

    async def record_probe(self, *, name: str, outcome: Outcome, ttl: float) -> SharedState:
        """Tally one completed probe's outcome (only while ``HALF_OPEN``)."""
        ...

    async def close(
        self, *, name: str, ttl: float, expected_version: int | None = None
    ) -> SharedState:
        """Transition to ``CLOSED`` and clear probe accounting."""
        ...


@runtime_checkable
class FailureClassifier(Protocol):
    """Decides what counts as a failure, by exception and by result."""

    def is_failure(self, *, result: object, exception: Exception | None) -> bool:
        """Return whether a completed call counts as a failure.

        Exactly one dimension is meaningful per call: when ``exception`` is not
        ``None`` the call raised; otherwise ``result`` is its return value.

        Only an ``Exception`` is ever passed: a ``BaseException`` — cancellation,
        shutdown — says nothing about the dependency, so the engine releases the
        call without classifying it. A classifier that handles cancellation here
        is writing code that can never run.
        """
        ...


@runtime_checkable
class CoreEventListener(Protocol):
    """Observes circuit-breaker calls, rejections, resets and transitions.

    A hook that raises an ``Exception`` is logged to the ``interlock`` logger
    and ignored: the protected call keeps its result, a failing call keeps its
    own exception, and transition bookkeeping completes. ``BaseException`` —
    cancellation, shutdown — still propagates.

    Every hook is dispatched by name and only if defined, so a listener may
    implement just the ones it needs and one written before a hook existed
    keeps working unchanged. Inherit this protocol to make a partial listener
    type-check: its default hook implementations are no-ops. See the partial
    listener pattern in ``docs/guides/observability.md``.
    """

    def on_state_change(self, *, name: str, old: State, new: State) -> None:
        """Called after breaker ``name`` transitions between states."""
        return None

    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None:
        """Called after a call protected by breaker ``name`` completes."""
        return None

    def on_rejected(self, *, name: str) -> None:
        """Called when breaker ``name`` rejects a call because its circuit is open."""
        return None

    def on_reset(self, *, name: str) -> None:
        """Called when breaker ``name`` is manually reset."""
        return None


@runtime_checkable
class StorageEventListener(Protocol):
    """Observes the shared-storage path of a coordinated circuit breaker.

    ``name`` always identifies the breaker whose storage path emitted the
    event. Hooks inherit the same failure isolation and optional-dispatch
    policy as ``CoreEventListener``.
    """

    def on_storage_degraded(self, *, name: str, error: BaseException) -> None:
        """Called when breaker ``name`` loses its shared storage backend.

        The breaker keeps working on local state; this event makes the
        degradation observable instead of silent. It reports storage failures
        only — a listener of your own that raises is logged, never reported
        here.
        """
        return None

    def on_storage_recovered(self, *, name: str) -> None:
        """Called when breaker ``name`` reaches its shared storage backend again."""
        return None

    def on_storage_write_dropped(self, *, name: str) -> None:
        """Called when breaker ``name`` drops a write from its full storage queue.

        The queue only fills when the background lane stops draining it, so
        this is the signal that shared writes are being lost while the breaker
        keeps protecting the process locally. The shared state is reconciled
        by the next successful poll and by ``state_ttl``.
        """
        return None


@runtime_checkable
class PipelineEventListener(Protocol):
    """Observes retry, bulkhead and fallback pipeline strategies.

    ``name`` always identifies the strategy that emitted the event. Hooks
    inherit the same failure isolation and optional-dispatch policy as
    ``CoreEventListener``.
    """

    def on_retry(self, *, name: str, attempt: int, delay: float) -> None:
        """Called before retry strategy ``name`` sleeps between attempts.

        ``attempt`` is the number of the attempt that just failed; ``delay``
        is the upcoming backoff in seconds.
        """
        return None

    def on_bulkhead_rejected(self, *, name: str) -> None:
        """Called when bulkhead strategy ``name`` rejects a call (no free slot)."""
        return None

    def on_fallback(self, *, name: str, error: BaseException) -> None:
        """Called when fallback strategy ``name`` substitutes a value for ``error``."""
        return None


@runtime_checkable
class EventListener(
    CoreEventListener,
    StorageEventListener,
    PipelineEventListener,
    Protocol,
):
    """Complete listener contract combining core, storage and pipeline hooks."""
