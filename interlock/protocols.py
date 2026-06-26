"""Extension-point contracts for the circuit breaker core.

The core depends only on these protocols, never on concrete implementations.
This keeps the state machine I/O-free and lets storage, windows, clocks,
classification and observability be swapped without touching it.
"""

from typing import Protocol, runtime_checkable

from interlock.outcome import Outcome
from interlock.state import State
from interlock.window import WindowSnapshot

__all__ = (
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
    """Where breaker state lives. Default in-memory; shared backends later."""

    def load(self, name: str) -> State:
        """Load the persisted state for the named breaker."""
        ...

    def save(self, *, name: str, state: State) -> None:
        """Persist the state for the named breaker."""
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
