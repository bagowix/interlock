"""Extension-point contracts for the circuit breaker core.

The core depends only on these protocols, never on concrete implementations.
This keeps the state machine I/O-free and lets storage, windows, clocks,
classification and observability be swapped without touching it.
"""

from typing import Protocol, runtime_checkable

from interlock.outcome import Outcome
from interlock.shared import ProbeLease, SharedState
from interlock.state import State
from interlock.window import WindowSnapshot

__all__ = (
    'AsyncStorage',
    'Clock',
    'EventListener',
    'FailureClassifier',
    'SlidingWindow',
    'Storage',
)


@runtime_checkable
class Clock(Protocol):
    """Source of time for the core. Injected so tests stay deterministic."""

    def monotonic(self) -> float:
        """Return a monotonically increasing time in fractional seconds."""
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

    See ``Storage`` for the full contract; every method is the awaitable
    counterpart.
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

    def is_failure(self, *, result: object, exception: BaseException | None) -> bool:
        """Return whether a completed call counts as a failure.

        Exactly one dimension is meaningful per call: when ``exception`` is not
        ``None`` the call raised; otherwise ``result`` is its return value.
        """
        ...


@runtime_checkable
class EventListener(Protocol):
    """Hooks for observability. Implementations must not raise into the core."""

    def on_state_change(self, *, name: str, old: State, new: State) -> None:
        """Called after the breaker transitions between states."""
        ...

    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None:
        """Called after a protected call completes, success or failure."""
        ...

    def on_rejected(self, *, name: str) -> None:
        """Called when a call is rejected because the circuit is open."""
        ...

    def on_reset(self, *, name: str) -> None:
        """Called when the breaker is manually reset."""
        ...

    def on_storage_degraded(self, *, name: str, error: BaseException) -> None:
        """Called when the shared storage backend becomes unavailable.

        The breaker keeps working on local state; this event makes the
        degradation observable instead of silent. The engine invokes the two
        storage hooks via safe ``getattr``, so listeners written before they
        existed keep working unchanged.
        """
        ...

    def on_storage_recovered(self, *, name: str) -> None:
        """Called when the shared storage backend becomes reachable again."""
        ...
