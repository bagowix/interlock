"""Deterministic teardown: the lane stops, the timer is cancelled, nothing leaks.

A coordinated breaker owns a background lane (daemon thread or asyncio task) and
an ``auto_transition`` breaker owns a ``threading.Timer``. Both used to end only
when the object was collected. These tests pin the explicit path added in #98 —
``close()`` / ``aclose()`` / ``Registry.close_all()`` — and the queue invariant
on the lane's drop path (#97), which the shutdown API makes reachable.

Real lane threads run here, so ``poll_interval`` is set huge to park the lane in
``get()``: shutdown must wake it through the sentinel rather than wait it out.
"""

import asyncio
import gc
import threading
import weakref
from typing import TYPE_CHECKING, cast

import pytest

from conftest import FakeClock, RecordingListener
from inmemory_storage import AsyncInMemoryStorage, InMemoryStorage
from interlock import CircuitBreaker, Config, Registry, State
from interlock._coordination import (
    AsyncCoordinator,
    SyncCoordinator,
    _async_lane_tick,
    _sync_lane_tick,
)
from interlock.errors import InterlockError
from interlock.protocols import AsyncStorage, EventListener, Storage
from interlock.shared import SharedState

if TYPE_CHECKING:
    import queue
    from collections.abc import Callable

NAME = 'svc'
WAIT = 5.0
JOIN_TIMEOUT = 5.0


@pytest.fixture
def config() -> Config:
    return Config(
        minimum_number_of_calls=2,
        window_size=10,
        failure_rate_threshold=0.5,
        slow_call_duration_threshold=1.0,
        permitted_calls_in_half_open=2,
        max_concurrent_probes=2,
        wait_duration_in_open=WAIT,
    )


@pytest.fixture
def storage(fake_clock: FakeClock) -> InMemoryStorage:
    store = InMemoryStorage(clock=fake_clock)
    store.poll_interval = 3600.0  # park the lane in get(); only the sentinel wakes it
    return store


@pytest.fixture
def astorage(fake_clock: FakeClock) -> AsyncInMemoryStorage:
    store = AsyncInMemoryStorage(clock=fake_clock)
    store.poll_interval = 3600.0
    return store


def _breaker(
    config: Config, fake_clock: FakeClock, storage: Storage | AsyncStorage | None = None
) -> CircuitBreaker:
    return CircuitBreaker(name=NAME, config=config, clock=fake_clock, storage=storage)


def _sync_coordinator(breaker: CircuitBreaker) -> SyncCoordinator:
    coordinator = breaker._engine._sync_coordinator
    assert coordinator is not None
    return coordinator


def _async_coordinator(breaker: CircuitBreaker) -> AsyncCoordinator:
    coordinator = breaker._engine._async_coordinator
    assert coordinator is not None
    return coordinator


def _boom() -> None:
    msg = 'boom'
    raise ValueError(msg)


def _trip(breaker: CircuitBreaker) -> None:
    """Drive the breaker to OPEN, which enqueues one shared write."""
    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            breaker.call(_boom)


def _joined(work: 'queue.Queue[Callable[[], None]]') -> bool:
    """Whether ``work.join()`` returns — i.e. no dequeued op was left unfinished."""
    done = threading.Event()

    def wait() -> None:
        work.join()
        done.set()

    threading.Thread(target=wait, daemon=True).start()
    return done.wait(timeout=JOIN_TIMEOUT)


# --- the lane's drop path (#97) ----------------------------------------------


def test__sync_lane_tick__dead_coordinator_with_op_in_flight__releases_the_op(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    coordinator = SyncCoordinator(
        name=NAME,
        config=config,
        clock=fake_clock,
        storage=storage,
        on_view=lambda _view: None,
        on_degraded=lambda _error: None,
        on_recovered=lambda: None,
        on_write_dropped=lambda: None,
    )
    work = coordinator._work
    work.put(lambda: None)  # dequeued by the tick, then dropped on the dead ref
    ref = weakref.ref(coordinator)
    del coordinator
    gc.collect()
    assert ref() is None

    assert _sync_lane_tick(ref, work, 0.001) is False

    assert _joined(work)  # task_done ran on the drop path: join() cannot hang


@pytest.mark.asyncio
async def test__async_lane_tick__dead_coordinator_with_op_in_flight__releases_the_op(
    config: Config, fake_clock: FakeClock, astorage: AsyncInMemoryStorage
) -> None:
    coordinator = AsyncCoordinator(
        name=NAME,
        config=config,
        clock=fake_clock,
        storage=astorage,
        on_view=lambda _view: None,
        on_degraded=lambda _error: None,
        on_recovered=lambda: None,
        on_write_dropped=lambda: None,
    )
    work = coordinator._work

    async def op() -> None: ...

    work.put_nowait(op)
    ref = weakref.ref(coordinator)
    del coordinator
    gc.collect()
    assert ref() is None

    assert await _async_lane_tick(ref, work, 0.001) is False

    await asyncio.wait_for(work.join(), timeout=JOIN_TIMEOUT)


# --- sync coordinator shutdown -----------------------------------------------


def test__shutdown__stops_the_lane_thread(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, storage)
    coordinator = _sync_coordinator(breaker)
    coordinator.ensure_lane()
    thread = coordinator._thread
    assert thread is not None

    coordinator.shutdown()

    assert not thread.is_alive()  # shutdown joined it, without waiting out poll_interval


def test__shutdown__drains_queued_writes(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, storage)

    _trip(breaker)  # enqueues the shared trip write
    breaker.close()

    shared = storage.read(NAME)
    assert shared is not None
    assert shared.state is State.OPEN  # the queued write ran before the lane exited


def test__shutdown__is_idempotent(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, storage)
    coordinator = _sync_coordinator(breaker)
    coordinator.ensure_lane()

    coordinator.shutdown()
    coordinator.shutdown()  # second call must not hang, raise or re-enqueue

    thread = coordinator._thread
    assert thread is not None
    assert not thread.is_alive()


def test__shutdown__before_the_lane_started__is_a_noop(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, storage)
    coordinator = _sync_coordinator(breaker)

    coordinator.shutdown()

    assert coordinator._thread is None
    assert _joined(coordinator._work)  # no sentinel was left behind


def test__shutdown__while_degraded__still_stops(config: Config, fake_clock: FakeClock) -> None:
    class Down(InMemoryStorage):
        def read(self, name: str) -> SharedState | None:
            msg = 'storage down'
            raise ConnectionError(msg)

    down = Down(clock=fake_clock)
    down.poll_interval = 3600.0
    breaker = _breaker(config, fake_clock, down)
    coordinator = _sync_coordinator(breaker)
    coordinator.poll_once()  # degrade
    _trip(breaker)  # enqueues a write the degraded gate will drop

    coordinator.shutdown()

    thread = coordinator._thread
    assert thread is not None
    assert not thread.is_alive()
    assert _joined(coordinator._work)  # the dropped write was still marked done


def test__close__on_an_adopted_shared_open__falls_back_to_local_state(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, storage)
    storage.trip_open(name=NAME, ttl=60.0)  # a peer tripped
    _sync_coordinator(breaker).poll_once()  # adopt it
    assert breaker.state is State.OPEN

    breaker.close()

    # Nothing polls any more, so a retained view could never refresh: keeping it
    # would wedge the breaker in a peer's OPEN for good.
    assert breaker.state is State.CLOSED
    assert breaker.call(lambda: 'local') == 'local'


@pytest.mark.asyncio
async def test__aclose__on_an_adopted_shared_open__falls_back_to_local_state(
    config: Config, fake_clock: FakeClock, astorage: AsyncInMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, astorage)
    await astorage.trip_open(name=NAME, ttl=60.0)
    await _async_coordinator(breaker).poll_once()
    assert breaker.state is State.OPEN

    await breaker.aclose()

    assert breaker.state is State.CLOSED


def test__close__reports_the_fallback_as_a_state_change(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    listener = RecordingListener()
    breaker = CircuitBreaker(
        name=NAME,
        config=config,
        clock=fake_clock,
        storage=storage,
        listener=cast('EventListener', listener),
    )
    storage.trip_open(name=NAME, ttl=60.0)
    _sync_coordinator(breaker).poll_once()

    breaker.close()

    assert listener.state_changes[-1] == (State.OPEN, State.CLOSED)


def test__shutdown__then_a_later_write__is_dropped_and_the_lane_stays_down(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, storage)
    coordinator = _sync_coordinator(breaker)
    coordinator.shutdown()

    _trip(breaker)  # would enqueue a shared write and start the lane

    assert coordinator._thread is None  # shutdown is terminal: no restart
    assert storage.read(NAME) is None  # the write was dropped, as when degraded
    assert breaker.state is State.OPEN  # local state keeps working


# --- async coordinator shutdown ----------------------------------------------


async def _boom_async() -> None:
    msg = 'boom'
    raise ValueError(msg)


async def _trip_async(breaker: CircuitBreaker) -> None:
    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            await breaker.call(_boom_async)


@pytest.mark.asyncio
async def test__async_shutdown__stops_the_lane_task(
    config: Config, fake_clock: FakeClock, astorage: AsyncInMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, astorage)
    coordinator = _async_coordinator(breaker)
    coordinator.ensure_lane()
    task = coordinator._lane_task
    assert task is not None

    await coordinator.shutdown()

    assert task.done()


@pytest.mark.asyncio
async def test__async_shutdown__drains_queued_writes(
    config: Config, fake_clock: FakeClock, astorage: AsyncInMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, astorage)

    await _trip_async(breaker)
    await breaker.aclose()

    shared = await astorage.read(NAME)
    assert shared is not None
    assert shared.state is State.OPEN


@pytest.mark.asyncio
async def test__async_shutdown__is_idempotent(
    config: Config, fake_clock: FakeClock, astorage: AsyncInMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, astorage)
    coordinator = _async_coordinator(breaker)
    coordinator.ensure_lane()

    await coordinator.shutdown()
    await coordinator.shutdown()

    task = coordinator._lane_task
    assert task is not None
    assert task.done()


@pytest.mark.asyncio
async def test__async_shutdown__before_the_lane_started__is_a_noop(
    config: Config, fake_clock: FakeClock, astorage: AsyncInMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, astorage)
    coordinator = _async_coordinator(breaker)

    await coordinator.shutdown()

    assert coordinator._lane_task is None
    await asyncio.wait_for(coordinator._work.join(), timeout=JOIN_TIMEOUT)


@pytest.mark.asyncio
async def test__async_shutdown__then_a_later_write__is_dropped(
    config: Config, fake_clock: FakeClock, astorage: AsyncInMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, astorage)
    coordinator = _async_coordinator(breaker)
    await coordinator.shutdown()

    await _trip_async(breaker)

    assert coordinator._lane_task is None
    assert await astorage.read(NAME) is None
    assert breaker.state is State.OPEN


# --- the auto-transition timer -----------------------------------------------


def test__close__cancels_the_auto_transition_timer(fake_clock: FakeClock) -> None:
    config = Config(
        minimum_number_of_calls=2,
        failure_rate_threshold=0.5,
        wait_duration_in_open=3600.0,
        auto_transition=True,
    )
    breaker = _breaker(config, fake_clock)
    _trip(breaker)
    assert breaker._engine._timer is not None

    breaker.close()

    assert breaker._engine._timer is None


def test__close__then_a_new_trip__does_not_rearm_the_timer(fake_clock: FakeClock) -> None:
    config = Config(
        minimum_number_of_calls=2,
        failure_rate_threshold=0.5,
        wait_duration_in_open=3600.0,
        auto_transition=True,
    )
    breaker = _breaker(config, fake_clock)
    breaker.close()

    _trip(breaker)

    assert breaker.state is State.OPEN  # the breaker still works
    assert breaker._engine._timer is None  # but never arms a timer nobody will cancel


def test__close__is_idempotent_without_a_storage(fake_clock: FakeClock, config: Config) -> None:
    breaker = _breaker(config, fake_clock)

    breaker.close()
    breaker.close()


@pytest.mark.asyncio
async def test__aclose__without_a_storage__cancels_the_timer(fake_clock: FakeClock) -> None:
    config = Config(
        minimum_number_of_calls=2,
        failure_rate_threshold=0.5,
        wait_duration_in_open=3600.0,
        auto_transition=True,
    )
    breaker = _breaker(config, fake_clock)
    _trip(breaker)

    await breaker.aclose()

    assert breaker._engine._timer is None


# --- runtime matching --------------------------------------------------------


def test__close__on_an_async_coordinated_breaker__raises(
    config: Config, fake_clock: FakeClock, astorage: AsyncInMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, astorage)

    with pytest.raises(InterlockError, match='async storage'):
        breaker.close()


@pytest.mark.asyncio
async def test__aclose__on_a_sync_coordinated_breaker__raises(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, storage)

    with pytest.raises(InterlockError, match='sync storage'):
        await breaker.aclose()


# --- registry ----------------------------------------------------------------


def test__close_all__closes_every_breaker(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    registry = Registry(config=config, clock=fake_clock, storage=storage)
    first = registry.get('a')
    second = registry.get('b')
    _sync_coordinator(first).ensure_lane()
    _sync_coordinator(second).ensure_lane()

    registry.close_all()

    for breaker in (first, second):
        thread = _sync_coordinator(breaker)._thread
        assert thread is not None
        assert not thread.is_alive()
    assert registry.get('a') is first  # closed breakers stay cached; no silent restart


@pytest.mark.asyncio
async def test__aclose_all__closes_every_breaker(
    config: Config, fake_clock: FakeClock, astorage: AsyncInMemoryStorage
) -> None:
    registry = Registry(config=config, clock=fake_clock, storage=astorage)
    first = registry.get('a')
    second = registry.get('b')
    _async_coordinator(first).ensure_lane()
    _async_coordinator(second).ensure_lane()

    await registry.aclose_all()

    for breaker in (first, second):
        task = _async_coordinator(breaker)._lane_task
        assert task is not None
        assert task.done()
