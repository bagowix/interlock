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
- The queue feeding that lane is bounded (``write_queue_size``, at least 1).
  Enqueues are O(transitions), not O(traffic), so the bound is only ever
  reached by a lane that stopped draining; the newest write is then dropped
  and reported through ``on_storage_write_dropped`` rather than blocking or
  raising into the protected path. Coordinated writes are best-effort either
  way — a dropped one is reconciled by the next poll and by ``state_ttl``.
- Storage failures never reach the protected path: any storage error
  flips the coordinator into degraded mode — the breaker runs on local state,
  writes are dropped, and the lane keeps retrying after a backoff delay that
  grows with consecutive failures (``retry_backoff`` *
  ``retry_backoff_multiplier`` ** attempts, capped at ``retry_backoff_max``,
  plus proportional ``retry_jitter``) so many instances recovering from the
  same outage do not retry in lockstep. Degradation and recovery surface
  through the engine's listener callbacks; on recovery the shared view
  becomes authoritative again and the attempt counter resets.

Tuning knobs are read from optional attributes on the storage object
(``state_ttl``, ``poll_interval``, ``retry_backoff``,
``retry_backoff_multiplier``, ``retry_backoff_max``, ``retry_jitter``,
``write_queue_size``) with conservative defaults, so the ``Storage`` protocol
itself stays minimal and the core ``Config`` stays storage-agnostic.

The probe-round decision is the one piece of threshold policy applied here:
after the last probe the lane computes the same rate checks as the local state
machine (single source of policy: ``Config``) and sends ``close`` /
``trip_open`` fenced with ``expected_version``, so a stale decision can never
clobber a newer shared state.
"""

import asyncio
import queue
import random
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
_DEFAULT_RETRY_BACKOFF_MULTIPLIER = 1.0
_DEFAULT_RETRY_BACKOFF_MAX: float | None = None
_DEFAULT_RETRY_JITTER = 0.0
_DEFAULT_WRITE_QUEUE_SIZE = 128

_SyncOp = Callable[[], None]
_AsyncOp = Callable[[], Awaitable[None]]


def _sync_stop() -> None:
    """Queue sentinel: identity-compared by the lane, never invoked."""


async def _async_stop() -> None:
    """Queue sentinel: identity-compared by the lane, never invoked."""


class _CoordinatorBase:
    """State and policy shared by the sync and async coordinators.

    Everything here is I/O-free; subclasses supply the storage calls and the
    background lane. ``on_view`` / ``on_degraded`` / ``on_recovered`` /
    ``on_write_dropped`` are engine callbacks — they must be fast and must not
    raise. They notify the user's listener, which is why every hook goes through
    ``_notify.notify``: a raising listener would otherwise kill the lane
    (``poll_once`` calls back outside its ``try``) or be misreported as a
    storage failure (inside ``execute_op``).

    ``shutdown`` is terminal: the lane never restarts, and later writes are
    dropped exactly as they are while the storage is degraded. Everything that
    touches ``_stopped``, the lane handle and ``_work.put`` happens under
    ``_lock``, so a write can never be enqueued onto a lane that will not run
    to consume it — which would leave ``wait_idle`` unable to return.

    The work queue holds ``write_queue_size`` writes plus one slot reserved for
    the shutdown sentinel: ``_enqueue`` refuses to fill the last slot, so
    ``shutdown`` can always wake a lane whose queue is full and never blocks
    under ``_lock``.
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
        on_write_dropped: Callable[[], None],
    ) -> None:
        self._name = name
        self._config = config
        self._clock = clock
        self._on_view = on_view
        self._on_degraded = on_degraded
        self._on_recovered = on_recovered
        self._on_write_dropped = on_write_dropped
        self._ttl = float(getattr(storage, 'state_ttl', _DEFAULT_STATE_TTL))
        self._interval = float(getattr(storage, 'poll_interval', _DEFAULT_POLL_INTERVAL))
        self._backoff = float(getattr(storage, 'retry_backoff', _DEFAULT_RETRY_BACKOFF))
        self._backoff_multiplier = float(
            getattr(storage, 'retry_backoff_multiplier', _DEFAULT_RETRY_BACKOFF_MULTIPLIER)
        )
        backoff_max = getattr(storage, 'retry_backoff_max', _DEFAULT_RETRY_BACKOFF_MAX)
        self._backoff_max = float(backoff_max) if backoff_max is not None else None
        self._jitter = float(getattr(storage, 'retry_jitter', _DEFAULT_RETRY_JITTER))
        self._queue_size = int(getattr(storage, 'write_queue_size', _DEFAULT_WRITE_QUEUE_SIZE))
        self._lock = threading.Lock()
        self._degraded = False
        self._retry_at = 0.0
        self._failures = 0
        self._last_view: SharedState | None = None
        self._stopped = False

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
            self._retry_at = self._clock.monotonic() + self._next_delay(self._failures)
            self._failures += 1

        if first:
            self._on_degraded(error)

    def _mark_available(self) -> None:
        with self._lock:
            was_degraded = self._degraded
            self._degraded = False
            self._failures = 0

        if was_degraded:
            self._on_recovered()

    def _next_delay(self, attempt: int) -> float:
        """Backoff for the ``attempt``-th consecutive failure (0-indexed).

        Grows geometrically from ``retry_backoff``, capped at
        ``retry_backoff_max``, then widened by up to ``retry_jitter`` (a
        fraction of the capped delay) so that many instances degrading from
        the same outage do not all retry at the same instant. The jitter draw
        is seeded from the clock reading, the attempt number and the breaker
        name rather than process-global randomness, so it stays reproducible
        under an injected (fake) clock in tests.
        """
        delay = self._backoff * (self._backoff_multiplier**attempt)
        if self._backoff_max is not None:
            delay = min(delay, self._backoff_max)
        if self._jitter <= 0.0:
            return delay

        seed = f'{self._name}:{self._clock.monotonic()}:{attempt}'.encode()
        spread = random.Random(seed).random()  # noqa: S311 - jitter, not crypto
        return delay + delay * self._jitter * spread

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
        on_write_dropped: Callable[[], None],
    ) -> None:
        super().__init__(
            name=name,
            config=config,
            clock=clock,
            storage=storage,
            on_view=on_view,
            on_degraded=on_degraded,
            on_recovered=on_recovered,
            on_write_dropped=on_write_dropped,
        )
        self._storage = storage
        self._work: queue.Queue[_SyncOp] = queue.Queue(maxsize=self._queue_size + 1)
        self._thread: threading.Thread | None = None

    def ensure_lane(self) -> None:
        """Start the background lane once; safe to call on every admission."""
        with self._lock:
            self._start_lane_locked()

    def shutdown(self) -> None:
        """Drain the queued writes, stop the lane and join it. Idempotent.

        The sentinel wakes a lane parked in ``get`` immediately, so shutdown
        never waits out ``poll_interval``. Writes already queued run first
        (FIFO); ones the degraded gate would drop are still dropped. The
        sentinel goes into the slot ``_enqueue`` keeps reserved, so a full
        queue can never make this block while holding ``_lock``.
        """
        with self._lock:
            first = not self._stopped
            self._stopped = True
            thread = self._thread
            if first and thread is not None:
                self._work.put_nowait(_sync_stop)

        if thread is not None:
            thread.join()  # outside the lock: the lane takes it in poll_once

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
        with self._lock:
            if self._stopped:
                return  # shut down: drop the write, as the degraded gate does
            # Drop the newest, not the oldest: the queued writes are already
            # accounted for by ``wait_idle``, so evicting one would need a
            # matching ``task_done``. Dropping the arriving one keeps FIFO.
            dropped = self._work.qsize() >= self._queue_size
            if not dropped:
                self._work.put_nowait(op)
            self._start_lane_locked()

        if dropped:
            self._on_write_dropped()  # outside the lock: it calls the listener

    def _start_lane_locked(self) -> None:
        if self._thread is not None or self._stopped:
            return

        self._thread = threading.Thread(
            target=_sync_lane,
            args=(weakref.ref(self), self._work, self._interval),
            name=f'interlock-coordinator-{self._name}',
            daemon=True,
        )
        self._thread.start()

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

    Returns whether the lane should keep running. It stops on the shutdown
    sentinel, and on a dead weak reference — the lane holds only a weak
    reference between iterations, so an abandoned breaker can be collected and
    its lane exits on the next tick. Either way a dequeued op is marked done,
    or ``unfinished_tasks`` would stay positive and ``wait_idle`` could never
    return.
    """
    try:
        op = work.get(timeout=interval)
    except queue.Empty:
        op = None

    coordinator = ref()
    if coordinator is None or op is _sync_stop:
        if op is not None:
            work.task_done()
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
        on_write_dropped: Callable[[], None],
    ) -> None:
        super().__init__(
            name=name,
            config=config,
            clock=clock,
            storage=storage,
            on_view=on_view,
            on_degraded=on_degraded,
            on_recovered=on_recovered,
            on_write_dropped=on_write_dropped,
        )
        self._storage = storage
        self._work: asyncio.Queue[_AsyncOp] = asyncio.Queue(maxsize=self._queue_size + 1)
        self._lane_task: asyncio.Task[None] | None = None

    def ensure_lane(self) -> None:
        """Start the lane task once; must be called with a running event loop."""
        with self._lock:
            self._start_lane_locked()

    async def shutdown(self) -> None:
        """Drain the queued writes, stop the lane and await it. Idempotent.

        The async mirror of ``SyncCoordinator.shutdown``; awaiting the task is
        what keeps a pending lane from outliving ``asyncio.run``.
        """
        with self._lock:
            first = not self._stopped
            self._stopped = True
            task = self._lane_task
            if first and task is not None:
                self._work.put_nowait(_async_stop)

        if task is not None:
            await task

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
        with self._lock:
            if self._stopped:
                return  # shut down: drop the write, as the degraded gate does
            # Drop the newest, not the oldest, for the reason the sync lane
            # does: evicting a queued write would need a matching ``task_done``
            # to keep ``wait_idle`` accurate.
            dropped = self._work.qsize() >= self._queue_size
            if not dropped:
                self._work.put_nowait(op)
            self._start_lane_locked()

        if dropped:
            self._on_write_dropped()  # outside the lock: it calls the listener

    def _start_lane_locked(self) -> None:
        if self._lane_task is not None or self._stopped:
            return

        self._lane_task = asyncio.get_running_loop().create_task(
            _async_lane(weakref.ref(self), self._work, self._interval),
            name=f'interlock-coordinator-{self._name}',
        )

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
    if coordinator is None or op is _async_stop:
        if op is not None:
            work.task_done()
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
    # pragma-no-branch: on 3.14 the sys.monitoring tracer misses the normal
    # exit arc when the lane task is torn down by loop shutdown in tests.
    while await _async_lane_tick(ref, work, interval):  # pragma: no branch
        pass
