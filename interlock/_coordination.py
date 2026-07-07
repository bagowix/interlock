"""Coordination between a local breaker and a shared ``Storage`` backend.

Active only when an ``Engine`` is constructed with a ``storage``. The local
state machine keeps owning the CLOSED window and trip detection; the backend
owns the shared OPEN/HALF_OPEN state and the global probe budget. This module
is the plumbing between the two, built so the protected path stays fast:

- The engine admits calls against a locally cached view of the shared state —
  zero inline I/O in CLOSED and OPEN. The single inline network operation is
  ``lease_probe`` in the short HALF_OPEN window.
- All writes (trip on local threshold, probe outcomes, the final close/reopen
  decision) are fire-and-forget: they run on a background *lane* (one daemon
  thread for a sync storage, one asyncio task for an async one) that doubles as
  the poller refreshing the cached view every ``poll_interval``.
- Storage failures never reach the protected path (T3.1): any storage error
  flips the coordinator into degraded mode — the breaker runs on local state,
  writes are dropped, and the lane keeps retrying after ``retry_backoff``
  seconds. Degradation and recovery surface through the engine's listener
  callbacks (T3.2); on recovery the shared view becomes authoritative again
  (T3.3).

Tuning knobs are read from optional attributes on the storage object
(``state_ttl``, ``poll_interval``, ``retry_backoff``) with conservative
defaults, so the ``Storage`` protocol itself stays minimal and the core
``Config`` stays storage-agnostic.

The probe-round decision is the one piece of threshold policy applied here:
after the last probe the lane computes the same rate checks as the local state
machine (single source of policy: ``Config``) and sends ``close`` /
``trip_open`` fenced with ``expected_version``, so a stale decision can never
clobber a newer shared state.
"""

import asyncio
import queue
import threading
import weakref
from collections.abc import Awaitable, Callable

from interlock.config import Config
from interlock.outcome import Outcome
from interlock.protocols import AsyncStorage, Clock, Storage
from interlock.shared import SharedState
from interlock.state import State

__all__ = ('AsyncCoordinator', 'SyncCoordinator')

_DEFAULT_STATE_TTL = 300.0
_DEFAULT_POLL_INTERVAL = 1.0
_DEFAULT_RETRY_BACKOFF = 5.0

_SyncOp = Callable[[], None]
_AsyncOp = Callable[[], Awaitable[None]]


class _CoordinatorBase:
    """State and policy shared by the sync and async coordinators.

    Everything here is I/O-free; subclasses supply the storage calls and the
    background lane. ``on_view`` / ``on_degraded`` / ``on_recovered`` are engine
    callbacks — they must be fast and must not raise.
    """

    def __init__(
        self,
        *,
        name: str,
        config: Config,
        clock: Clock,
        storage: object,
        on_view: Callable[[SharedState], None],
        on_degraded: Callable[[BaseException], None],
        on_recovered: Callable[[], None],
    ) -> None:
        self._name = name
        self._config = config
        self._clock = clock
        self._on_view = on_view
        self._on_degraded = on_degraded
        self._on_recovered = on_recovered
        self._ttl = float(getattr(storage, 'state_ttl', _DEFAULT_STATE_TTL))
        self._interval = float(getattr(storage, 'poll_interval', _DEFAULT_POLL_INTERVAL))
        self._backoff = float(getattr(storage, 'retry_backoff', _DEFAULT_RETRY_BACKOFF))
        self._lock = threading.Lock()
        self._degraded = False
        self._retry_at = 0.0
        self._last_view: SharedState | None = None
        self._lane_started = False

    def _gate_open(self) -> bool:
        """Whether the storage may be touched now (not degraded, or retry due)."""
        with self._lock:
            return not self._degraded or self._clock.monotonic() >= self._retry_at

    def _accept(self, view: SharedState) -> None:
        with self._lock:
            changed = view != self._last_view
            self._last_view = view

        if changed:
            self._on_view(view)

    def _degrade(self, error: BaseException) -> None:
        with self._lock:
            first = not self._degraded
            self._degraded = True
            self._retry_at = self._clock.monotonic() + self._backoff

        if first:
            self._on_degraded(error)

    def _mark_available(self) -> None:
        with self._lock:
            was_degraded = self._degraded
            self._degraded = False

        if was_degraded:
            self._on_recovered()

    def _round_finished(self, view: SharedState) -> bool:
        return (
            view.state is State.HALF_OPEN
            and view.probes_permitted > 0
            and view.probes_completed >= view.probes_permitted
        )

    def _round_failed(self, view: SharedState) -> bool:
        # Same threshold policy as StateMachine._evaluate_probes — Config is the
        # single source; only the mechanism differs (shared counters, not local).
        completed = view.probes_completed
        return (
            view.probe_failures / completed >= self._config.failure_rate_threshold
            or view.probe_slows / completed >= self._config.slow_call_rate_threshold
        )


class SyncCoordinator(_CoordinatorBase):
    """Coordinates a sync ``Storage``; the lane is a daemon thread."""

    def __init__(
        self,
        *,
        name: str,
        config: Config,
        clock: Clock,
        storage: Storage,
        on_view: Callable[[SharedState], None],
        on_degraded: Callable[[BaseException], None],
        on_recovered: Callable[[], None],
    ) -> None:
        super().__init__(
            name=name,
            config=config,
            clock=clock,
            storage=storage,
            on_view=on_view,
            on_degraded=on_degraded,
            on_recovered=on_recovered,
        )
        self._storage = storage
        self._work: queue.Queue[_SyncOp] = queue.Queue()

    def ensure_lane(self) -> None:
        """Start the background lane once; safe to call on every admission."""
        with self._lock:
            if self._lane_started:
                return
            self._lane_started = True

        thread = threading.Thread(
            target=_sync_lane,
            args=(weakref.ref(self), self._work, self._interval),
            name=f'interlock-coordinator-{self._name}',
            daemon=True,
        )
        thread.start()

    def try_lease(self) -> bool | None:
        """Claim one shared probe slot inline; ``None`` means storage degraded."""
        if not self._gate_open():
            return None

        try:
            lease = self._storage.lease_probe(name=self._name, ttl=self._ttl)
        except Exception as error:
            self._degrade(error)
            return None

        self._mark_available()
        self._accept(lease.state)
        return lease.granted

    def notify_local_trip(self) -> None:
        """Propagate a local threshold trip to the shared state (fire-and-forget)."""

        def op() -> None:
            self._accept(self._storage.trip_open(name=self._name, ttl=self._ttl))

        self._enqueue(op)

    def notify_probe_outcome(self, outcome: Outcome) -> None:
        """Tally a probe outcome; decide close/reopen after the final probe."""

        def op() -> None:
            view = self._storage.record_probe(name=self._name, outcome=outcome, ttl=self._ttl)
            self._accept(view)
            if not self._round_finished(view):
                return

            if self._round_failed(view):
                final = self._storage.trip_open(
                    name=self._name, ttl=self._ttl, expected_version=view.version
                )
            else:
                final = self._storage.close(
                    name=self._name, ttl=self._ttl, expected_version=view.version
                )
            self._accept(final)

        self._enqueue(op)

    def poll_once(self) -> None:
        """One poll tick: refresh the cached view, driving OPEN → HALF_OPEN.

        While the shared state is OPEN the poll doubles as the transition
        attempt — ``begin_half_open_if_elapsed`` is a server-side no-op until
        the wait elapses and returns the current view either way.
        """
        if not self._gate_open():
            return

        with self._lock:
            last_state = self._last_view.state if self._last_view is not None else None

        try:
            if last_state is State.OPEN:
                view = self._storage.begin_half_open_if_elapsed(
                    name=self._name,
                    wait_duration=self._config.wait_duration_in_open,
                    permitted=self._config.permitted_calls_in_half_open,
                    ttl=self._ttl,
                )
            else:
                view = self._storage.read(self._name) or SharedState.closed()
        except Exception as error:
            self._degrade(error)
            return

        self._mark_available()
        self._accept(view)

    def wait_idle(self) -> None:
        """Block until every queued write has been processed (test helper)."""
        self._work.join()

    def _enqueue(self, op: _SyncOp) -> None:
        self._work.put(op)
        self.ensure_lane()

    def execute_op(self, op: _SyncOp) -> None:
        """Run one queued write under degradation protection (lane-internal)."""
        if not self._gate_open():
            return  # degraded: drop the write, run local until the backend is back

        try:
            op()
        except Exception as error:
            self._degrade(error)
        else:
            self._mark_available()


def _sync_lane_tick(
    ref: 'weakref.ref[SyncCoordinator]',
    work: 'queue.Queue[_SyncOp]',
    interval: float,
) -> bool:
    """One lane iteration: run a queued write, or poll on timeout.

    Returns whether the lane should keep running. The lane holds only a weak
    reference between iterations so an abandoned breaker can be collected and
    its lane exits on the next tick.
    """
    try:
        op = work.get(timeout=interval)
    except queue.Empty:
        op = None

    coordinator = ref()
    if coordinator is None:
        return False

    if op is None:
        coordinator.poll_once()
    else:
        try:
            coordinator.execute_op(op)
        finally:
            work.task_done()

    return True


def _sync_lane(
    ref: 'weakref.ref[SyncCoordinator]',
    work: 'queue.Queue[_SyncOp]',
    interval: float,
) -> None:
    while _sync_lane_tick(ref, work, interval):
        pass


class AsyncCoordinator(_CoordinatorBase):
    """Coordinates an ``AsyncStorage``; the lane is an asyncio task."""

    def __init__(
        self,
        *,
        name: str,
        config: Config,
        clock: Clock,
        storage: AsyncStorage,
        on_view: Callable[[SharedState], None],
        on_degraded: Callable[[BaseException], None],
        on_recovered: Callable[[], None],
    ) -> None:
        super().__init__(
            name=name,
            config=config,
            clock=clock,
            storage=storage,
            on_view=on_view,
            on_degraded=on_degraded,
            on_recovered=on_recovered,
        )
        self._storage = storage
        self._work: asyncio.Queue[_AsyncOp] = asyncio.Queue()
        self._lane_task: asyncio.Task[None] | None = None

    def ensure_lane(self) -> None:
        """Start the lane task once; must be called with a running event loop."""
        with self._lock:
            if self._lane_started:
                return
            self._lane_started = True

        self._lane_task = asyncio.get_running_loop().create_task(
            _async_lane(weakref.ref(self), self._work, self._interval),
            name=f'interlock-coordinator-{self._name}',
        )

    async def try_lease(self) -> bool | None:
        """Claim one shared probe slot inline; ``None`` means storage degraded."""
        if not self._gate_open():
            return None

        try:
            lease = await self._storage.lease_probe(name=self._name, ttl=self._ttl)
        except Exception as error:
            self._degrade(error)
            return None

        self._mark_available()
        self._accept(lease.state)
        return lease.granted

    def notify_local_trip(self) -> None:
        """Propagate a local threshold trip to the shared state (fire-and-forget)."""

        async def op() -> None:
            self._accept(await self._storage.trip_open(name=self._name, ttl=self._ttl))

        self._enqueue(op)

    def notify_probe_outcome(self, outcome: Outcome) -> None:
        """Tally a probe outcome; decide close/reopen after the final probe."""

        async def op() -> None:
            view = await self._storage.record_probe(name=self._name, outcome=outcome, ttl=self._ttl)
            self._accept(view)
            if not self._round_finished(view):
                return

            if self._round_failed(view):
                final = await self._storage.trip_open(
                    name=self._name, ttl=self._ttl, expected_version=view.version
                )
            else:
                final = await self._storage.close(
                    name=self._name, ttl=self._ttl, expected_version=view.version
                )
            self._accept(final)

        self._enqueue(op)

    async def poll_once(self) -> None:
        """One poll tick: refresh the cached view, driving OPEN → HALF_OPEN."""
        if not self._gate_open():
            return

        with self._lock:
            last_state = self._last_view.state if self._last_view is not None else None

        try:
            if last_state is State.OPEN:
                view = await self._storage.begin_half_open_if_elapsed(
                    name=self._name,
                    wait_duration=self._config.wait_duration_in_open,
                    permitted=self._config.permitted_calls_in_half_open,
                    ttl=self._ttl,
                )
            else:
                view = await self._storage.read(self._name) or SharedState.closed()
        except Exception as error:
            self._degrade(error)
            return

        self._mark_available()
        self._accept(view)

    async def wait_idle(self) -> None:
        """Wait until every queued write has been processed (test helper)."""
        await self._work.join()

    def _enqueue(self, op: _AsyncOp) -> None:
        self._work.put_nowait(op)
        self.ensure_lane()

    async def execute_op(self, op: _AsyncOp) -> None:
        """Run one queued write under degradation protection (lane-internal)."""
        if not self._gate_open():
            return  # degraded: drop the write, run local until the backend is back

        try:
            await op()
        except Exception as error:
            self._degrade(error)
        else:
            self._mark_available()


async def _async_lane_tick(
    ref: 'weakref.ref[AsyncCoordinator]',
    work: 'asyncio.Queue[_AsyncOp]',
    interval: float,
) -> bool:
    """One lane iteration: run a queued write, or poll on timeout."""
    try:
        op = await asyncio.wait_for(work.get(), timeout=interval)
    except TimeoutError:
        op = None

    coordinator = ref()
    if coordinator is None:
        return False

    if op is None:
        await coordinator.poll_once()
    else:
        try:
            await coordinator.execute_op(op)
        finally:
            work.task_done()

    return True


async def _async_lane(
    ref: 'weakref.ref[AsyncCoordinator]',
    work: 'asyncio.Queue[_AsyncOp]',
    interval: float,
) -> None:
    while await _async_lane_tick(ref, work, interval):
        pass
