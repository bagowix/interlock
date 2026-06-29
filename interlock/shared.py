"""Cross-instance breaker state shared through a ``Storage`` backend.

``SharedState`` is the data contract a distributed backend persists: the
coordinated ``State`` plus the metadata needed to coordinate transitions across
instances without a global sliding window — a versioned snapshot (for fencing),
the timestamp the breaker opened (in the *backend's* time, since instance clocks
are not comparable), and the bounded HALF_OPEN probe accounting.

Threshold policy stays in the core state machine (Python). This type carries
only what crosses the wire: mechanism, not policy.
"""

from dataclasses import dataclass
from typing import Self

from interlock.state import State

__all__ = ('ProbeLease', 'SharedState')


@dataclass(frozen=True, slots=True)
class SharedState:
    """Immutable snapshot of one breaker's coordinated state.

    Probe fields are meaningful only in ``HALF_OPEN``; elsewhere they are zero.
    The caller treats ``probes_completed >= probes_permitted`` as the signal that
    the bounded probe round is finished and a CLOSED/OPEN decision is due.
    """

    state: State
    opened_at: float
    version: int
    probes_permitted: int
    probes_remaining: int
    probes_completed: int
    probe_failures: int
    probe_slows: int

    @classmethod
    def closed(cls) -> Self:
        """A fresh ``CLOSED`` baseline — the implied state of an absent key."""
        return cls(
            state=State.CLOSED,
            opened_at=0.0,
            version=0,
            probes_permitted=0,
            probes_remaining=0,
            probes_completed=0,
            probe_failures=0,
            probe_slows=0,
        )


@dataclass(frozen=True, slots=True)
class ProbeLease:
    """Result of attempting to claim one global HALF_OPEN probe slot.

    ``granted`` says whether this instance may run a probe; ``state`` is the
    backend view after the attempt, so the caller can refresh its cache either
    way.
    """

    granted: bool
    state: SharedState
