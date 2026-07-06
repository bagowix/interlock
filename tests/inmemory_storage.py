"""In-memory reference ``Storage`` / ``AsyncStorage`` for tests.

Not shipped: a deterministic double that mirrors the atomic semantics the Redis
backend will provide via Lua, so the contract suite (and later engine-coordination
tests) run without a live server. Time comes from an injected ``Clock``; ``ttl``
is accepted but inert (key expiry is a Redis concern, covered by T2.4).
"""

import dataclasses
import threading

from interlock.outcome import Outcome
from interlock.protocols import Clock
from interlock.shared import ProbeLease, SharedState
from interlock.state import State


class _Core:
    """Lock-guarded atomic logic shared by the sync and async facades."""

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._store: dict[str, SharedState] = {}
        self._lock = threading.Lock()

    def read(self, name: str) -> SharedState | None:
        with self._lock:
            return self._store.get(name)

    def trip_open(self, *, name: str, expected_version: int | None) -> SharedState:
        with self._lock:
            current = self._store.get(name) or SharedState.closed()
            if expected_version is not None and current.version != expected_version:
                return current
            if current.state is State.OPEN:
                return current

            new = dataclasses.replace(
                SharedState.closed(),
                state=State.OPEN,
                opened_at=self._clock.monotonic(),
                version=current.version + 1,
            )
            self._store[name] = new

            return new

    def begin_half_open_if_elapsed(
        self, *, name: str, wait_duration: float, permitted: int
    ) -> SharedState:
        with self._lock:
            current = self._store.get(name) or SharedState.closed()
            if current.state is not State.OPEN:
                return current
            if self._clock.monotonic() - current.opened_at < wait_duration:
                return current

            new = dataclasses.replace(
                current,
                state=State.HALF_OPEN,
                version=current.version + 1,
                probes_permitted=permitted,
                probes_remaining=permitted,
                probes_completed=0,
                probe_failures=0,
                probe_slows=0,
            )
            self._store[name] = new

            return new

    def lease_probe(self, *, name: str) -> ProbeLease:
        with self._lock:
            current = self._store.get(name) or SharedState.closed()
            if current.state is not State.HALF_OPEN or current.probes_remaining <= 0:
                return ProbeLease(granted=False, state=current)

            new = dataclasses.replace(
                current,
                version=current.version + 1,
                probes_remaining=current.probes_remaining - 1,
            )
            self._store[name] = new

            return ProbeLease(granted=True, state=new)

    def record_probe(self, *, name: str, outcome: Outcome) -> SharedState:
        with self._lock:
            current = self._store.get(name) or SharedState.closed()
            if current.state is not State.HALF_OPEN:
                return current  # stale probe outcome: the state has moved on

            new = dataclasses.replace(
                current,
                version=current.version + 1,
                probes_completed=current.probes_completed + 1,
                probe_failures=current.probe_failures + outcome.is_failure,
                probe_slows=current.probe_slows + outcome.is_slow,
            )
            self._store[name] = new

            return new

    def close(self, *, name: str, expected_version: int | None) -> SharedState:
        with self._lock:
            current = self._store.get(name) or SharedState.closed()
            if expected_version is not None and current.version != expected_version:
                return current

            new = dataclasses.replace(SharedState.closed(), version=current.version + 1)
            self._store[name] = new
            return new


class InMemoryStorage:
    """Synchronous in-memory ``Storage`` reference."""

    def __init__(self, *, clock: Clock) -> None:
        self._core = _Core(clock)

    def read(self, name: str) -> SharedState | None:
        return self._core.read(name)

    def trip_open(
        self, *, name: str, ttl: float, expected_version: int | None = None
    ) -> SharedState:
        return self._core.trip_open(name=name, expected_version=expected_version)

    def begin_half_open_if_elapsed(
        self, *, name: str, wait_duration: float, permitted: int, ttl: float
    ) -> SharedState:
        return self._core.begin_half_open_if_elapsed(
            name=name, wait_duration=wait_duration, permitted=permitted
        )

    def lease_probe(self, *, name: str, ttl: float) -> ProbeLease:
        return self._core.lease_probe(name=name)

    def record_probe(self, *, name: str, outcome: Outcome, ttl: float) -> SharedState:
        return self._core.record_probe(name=name, outcome=outcome)

    def close(self, *, name: str, ttl: float, expected_version: int | None = None) -> SharedState:
        return self._core.close(name=name, expected_version=expected_version)


class AsyncInMemoryStorage:
    """Asynchronous in-memory ``AsyncStorage`` reference."""

    def __init__(self, *, clock: Clock) -> None:
        self._core = _Core(clock)

    async def read(self, name: str) -> SharedState | None:
        return self._core.read(name)

    async def trip_open(
        self, *, name: str, ttl: float, expected_version: int | None = None
    ) -> SharedState:
        return self._core.trip_open(name=name, expected_version=expected_version)

    async def begin_half_open_if_elapsed(
        self, *, name: str, wait_duration: float, permitted: int, ttl: float
    ) -> SharedState:
        return self._core.begin_half_open_if_elapsed(
            name=name, wait_duration=wait_duration, permitted=permitted
        )

    async def lease_probe(self, *, name: str, ttl: float) -> ProbeLease:
        return self._core.lease_probe(name=name)

    async def record_probe(self, *, name: str, outcome: Outcome, ttl: float) -> SharedState:
        return self._core.record_probe(name=name, outcome=outcome)

    async def close(
        self, *, name: str, ttl: float, expected_version: int | None = None
    ) -> SharedState:
        return self._core.close(name=name, expected_version=expected_version)
