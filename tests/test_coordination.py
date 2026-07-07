"""Coordinated (distributed) breaker behaviour over a shared ``Storage``.

Deterministic: a shared ``FakeClock`` drives both the engines and the in-memory
storage, background lanes are made inert with a huge ``poll_interval``, poll
ticks run via ``poll_once()``, and fire-and-forget writes are awaited with
``wait_idle()`` — no sleeps, no real Redis.
"""

import asyncio
import gc
import weakref

import pytest

from conftest import FakeClock, RecordingListener
from inmemory_storage import AsyncInMemoryStorage, InMemoryStorage
from interlock import CircuitBreaker, CircuitOpenError, Config, Outcome, Registry, State
from interlock._coordination import (
    AsyncCoordinator,
    SyncCoordinator,
    _async_lane_tick,
    _sync_lane,
    _sync_lane_tick,
)
from interlock.errors import InterlockError
from interlock.shared import ProbeLease, SharedState

NAME = 'svc'
WAIT = 5.0


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
    store.poll_interval = 3600.0  # keep the background lane inert; tests poll manually
    return store


class StorageEventsListener(RecordingListener):
    """RecordingListener extended with the 1.2 storage hooks."""

    def __init__(self) -> None:
        super().__init__()
        self.degraded: list[BaseException] = []
        self.recovered: int = 0

    def on_storage_degraded(self, *, name: str, error: BaseException) -> None:
        self.degraded.append(error)

    def on_storage_recovered(self, *, name: str) -> None:
        self.recovered += 1


class FlakyStorage:
    """In-memory storage whose every operation can be made to raise."""

    def __init__(self, inner: InMemoryStorage) -> None:
        self._inner = inner
        self.fail = False
        self.state_ttl = inner.state_ttl
        self.poll_interval = inner.poll_interval
        self.retry_backoff = inner.retry_backoff

    def _check(self) -> None:
        if self.fail:
            raise ConnectionError('storage down')

    def read(self, name: str) -> SharedState | None:
        self._check()
        return self._inner.read(name)

    def trip_open(
        self, *, name: str, ttl: float, expected_version: int | None = None
    ) -> SharedState:
        self._check()
        return self._inner.trip_open(name=name, ttl=ttl, expected_version=expected_version)

    def begin_half_open_if_elapsed(
        self, *, name: str, wait_duration: float, permitted: int, ttl: float
    ) -> SharedState:
        self._check()
        return self._inner.begin_half_open_if_elapsed(
            name=name, wait_duration=wait_duration, permitted=permitted, ttl=ttl
        )

    def lease_probe(self, *, name: str, ttl: float) -> ProbeLease:
        self._check()
        return self._inner.lease_probe(name=name, ttl=ttl)

    def record_probe(self, *, name: str, outcome: Outcome, ttl: float) -> SharedState:
        self._check()
        return self._inner.record_probe(name=name, outcome=outcome, ttl=ttl)

    def close(self, *, name: str, ttl: float, expected_version: int | None = None) -> SharedState:
        self._check()
        return self._inner.close(name=name, ttl=ttl, expected_version=expected_version)


def _breaker(
    config: Config,
    fake_clock: FakeClock,
    storage: object,
    listener: RecordingListener | None = None,
) -> CircuitBreaker:
    return CircuitBreaker(
        name=NAME, config=config, clock=fake_clock, storage=storage, listener=listener
    )


def _coordinator(breaker: CircuitBreaker) -> SyncCoordinator:
    coordinator = breaker._engine._sync_coordinator
    assert coordinator is not None
    return coordinator


def _boom() -> None:
    raise ValueError('boom')


def _trip(breaker: CircuitBreaker) -> None:
    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            breaker.call(_boom)


# --- propagation and coordinated admission ---


def test__local_trip__propagates_to_shared_storage(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, storage)

    _trip(breaker)
    _coordinator(breaker).wait_idle()

    shared = storage.read(NAME)
    assert shared is not None
    assert shared.state is State.OPEN
    assert breaker.state is State.OPEN


def test__follower__adopts_shared_open__rejects_without_local_failures(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    tripper = _breaker(config, fake_clock, storage)
    listener = RecordingListener()
    follower = _breaker(config, fake_clock, storage, listener)
    _trip(tripper)
    _coordinator(tripper).wait_idle()

    _coordinator(follower).poll_once()

    assert follower.state is State.OPEN
    assert listener.state_changes[-1] == (State.CLOSED, State.OPEN)
    executed = []
    with pytest.raises(CircuitOpenError):
        follower.call(lambda: executed.append(1))
    assert executed == []  # rejected before the callable ran


def test__half_open__probe_budget_is_global(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    a = _breaker(config, fake_clock, storage)
    b = _breaker(config, fake_clock, storage)
    _trip(a)
    _coordinator(a).wait_idle()
    fake_clock.advance(WAIT)
    _coordinator(a).poll_once()  # OPEN -> HALF_OPEN via server-side elapse
    _coordinator(b).poll_once()

    assert a.call(lambda: 'probe-a') == 'probe-a'  # lease 1 of 2
    assert b.call(lambda: 'probe-b') == 'probe-b'  # lease 2 of 2
    with pytest.raises(CircuitOpenError):
        b.call(lambda: 'probe-c')  # budget exhausted across instances


def test__successful_probe_round__closes_all_instances(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    a = _breaker(config, fake_clock, storage)
    b = _breaker(config, fake_clock, storage)
    _trip(a)
    _coordinator(a).wait_idle()
    _coordinator(b).poll_once()
    assert (a.state, b.state) == (State.OPEN, State.OPEN)

    fake_clock.advance(WAIT)
    _coordinator(a).poll_once()
    _coordinator(b).poll_once()
    assert a.call(lambda: 'ok') == 'ok'
    _coordinator(a).wait_idle()  # probe 1 of 2 tallied
    assert b.call(lambda: 'ok') == 'ok'
    _coordinator(b).wait_idle()  # final probe: decision -> close
    _coordinator(a).poll_once()

    assert a.state is State.CLOSED  # the tripper's local OPEN adopted the recovery
    assert b.state is State.CLOSED
    assert a.call(lambda: 'back') == 'back'
    shared = storage.read(NAME)
    assert shared is not None
    assert shared.state is State.CLOSED


def test__failed_probe_round__reopens_globally(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    a = _breaker(config, fake_clock, storage)
    _trip(a)
    _coordinator(a).wait_idle()
    fake_clock.advance(WAIT)
    _coordinator(a).poll_once()

    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            a.call(_boom)
        _coordinator(a).wait_idle()

    shared = storage.read(NAME)
    assert shared is not None
    assert shared.state is State.OPEN
    assert shared.probes_completed == 0  # fresh OPEN, probe accounting cleared
    assert a.state is State.OPEN


def test__registry__passes_storage_through(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    registry = Registry(config=config, clock=fake_clock, storage=storage)
    breaker = registry.get(NAME)

    _trip(breaker)
    _coordinator(breaker).wait_idle()

    shared = storage.read(NAME)
    assert shared is not None
    assert shared.state is State.OPEN


# --- T3.1/T3.2/T3.3: degradation, observability, recovery ---


def test__storage_failure__degrades_to_local_state(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    flaky = FlakyStorage(storage)
    tripper = _breaker(config, fake_clock, storage)
    listener = StorageEventsListener()
    follower = _breaker(config, fake_clock, flaky, listener)
    _trip(tripper)
    _coordinator(tripper).wait_idle()
    _coordinator(follower).poll_once()
    assert follower.state is State.OPEN  # shared OPEN adopted

    flaky.fail = True
    fake_clock.advance(flaky.retry_backoff)
    _coordinator(follower).poll_once()

    assert len(listener.degraded) == 1
    assert isinstance(listener.degraded[0], ConnectionError)
    assert follower.state is State.CLOSED  # local state governs while degraded
    assert follower.call(lambda: 'local') == 'local'


def test__degraded__repeat_failures__single_event(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    flaky = FlakyStorage(storage)
    listener = StorageEventsListener()
    breaker = _breaker(config, fake_clock, flaky, listener)
    flaky.fail = True

    _coordinator(breaker).poll_once()
    fake_clock.advance(flaky.retry_backoff)
    _coordinator(breaker).poll_once()  # retry due, fails again

    assert len(listener.degraded) == 1
    assert listener.recovered == 0


def test__degraded__backoff_gates_storage_access(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    flaky = FlakyStorage(storage)
    listener = StorageEventsListener()
    breaker = _breaker(config, fake_clock, flaky, listener)
    flaky.fail = True
    _coordinator(breaker).poll_once()

    flaky.fail = False
    _coordinator(breaker).poll_once()  # backoff not elapsed: storage untouched

    assert listener.recovered == 0  # would have recovered had the gate been open


def test__degraded__writes_dropped_but_local_protection_works(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    flaky = FlakyStorage(storage)
    breaker = _breaker(config, fake_clock, flaky)
    flaky.fail = True
    _coordinator(breaker).poll_once()  # enter degraded mode

    _trip(breaker)  # local trip still protects this instance
    _coordinator(breaker).wait_idle()

    assert storage.read(NAME) is None  # the trip write was dropped, not raised
    assert breaker.state is State.OPEN
    with pytest.raises(CircuitOpenError):
        breaker.call(lambda: 'nope')


def test__recovery__emits_event_and_shared_becomes_authoritative(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    storage.trip_open(name=NAME, ttl=60.0)
    flaky = FlakyStorage(storage)
    listener = StorageEventsListener()
    breaker = _breaker(config, fake_clock, flaky, listener)
    flaky.fail = True
    _coordinator(breaker).poll_once()
    assert len(listener.degraded) == 1

    flaky.fail = False
    fake_clock.advance(flaky.retry_backoff)
    _coordinator(breaker).poll_once()

    assert listener.recovered == 1
    assert breaker.state is State.OPEN  # shared OPEN authoritative again (T3.3)
    with pytest.raises(CircuitOpenError):
        breaker.call(lambda: 'nope')


def test__lease_failure__falls_back_to_local_admission(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    flaky = FlakyStorage(storage)
    listener = StorageEventsListener()
    breaker = _breaker(config, fake_clock, flaky, listener)
    storage.trip_open(name=NAME, ttl=60.0)
    fake_clock.advance(WAIT)
    _coordinator(breaker).poll_once()  # first tick discovers the external OPEN
    _coordinator(breaker).poll_once()  # second tick drives OPEN -> HALF_OPEN
    assert breaker.state is State.HALF_OPEN

    flaky.fail = True
    assert breaker.call(lambda: 'local') == 'local'  # lease failed -> local CLOSED admits

    assert len(listener.degraded) == 1


def test__degradation__old_style_listener_does_not_break(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    flaky = FlakyStorage(storage)
    listener = RecordingListener()  # pre-1.2 shape: no storage hooks
    breaker = _breaker(config, fake_clock, flaky, listener)
    flaky.fail = True

    _coordinator(breaker).poll_once()  # must not raise AttributeError

    assert breaker.call(lambda: 'ok') == 'ok'


# --- runtime matching (D8) ---


def test__sync_storage__async_api__clear_error(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, storage)

    async def fn() -> None: ...

    with pytest.raises(InterlockError, match='sync storage'):
        asyncio.run(breaker.call(fn))  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test__sync_storage__async_block__clear_error(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, storage)

    with pytest.raises(InterlockError, match='sync storage'):
        async with breaker:
            pass


def test__async_storage__sync_api__clear_error(config: Config, fake_clock: FakeClock) -> None:
    astorage = AsyncInMemoryStorage(clock=fake_clock)
    breaker = _breaker(config, fake_clock, astorage)

    with pytest.raises(InterlockError, match='async storage'):
        breaker.call(lambda: 'nope')
    with pytest.raises(InterlockError, match='async storage'), breaker:
        pass


# --- async mirror ---


def _async_coordinator(breaker: CircuitBreaker) -> AsyncCoordinator:
    coordinator = breaker._engine._async_coordinator
    assert coordinator is not None
    return coordinator


@pytest.mark.asyncio
async def test__async__coordinated_trip_and_recovery(config: Config, fake_clock: FakeClock) -> None:
    astorage = AsyncInMemoryStorage(clock=fake_clock)
    astorage.poll_interval = 3600.0
    breaker = _breaker(config, fake_clock, astorage)

    async def boom() -> None:
        raise ValueError('boom')

    async def ok() -> str:
        return 'ok'

    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            await breaker.call(boom)
    coordinator = _async_coordinator(breaker)
    await coordinator.wait_idle()
    shared = await astorage.read(NAME)
    assert shared is not None
    assert shared.state is State.OPEN
    assert breaker.state is State.OPEN

    fake_clock.advance(WAIT)
    await coordinator.poll_once()
    assert breaker.state is State.HALF_OPEN

    assert await breaker.call(ok) == 'ok'
    await coordinator.wait_idle()
    assert await breaker.call(ok) == 'ok'
    await coordinator.wait_idle()  # final probe -> coordinated close

    assert breaker.state is State.CLOSED
    async with breaker:  # blocks admitted again
        pass


@pytest.mark.asyncio
async def test__async__lease_rejection_when_budget_exhausted(
    config: Config, fake_clock: FakeClock
) -> None:
    astorage = AsyncInMemoryStorage(clock=fake_clock)
    astorage.poll_interval = 3600.0
    breaker = _breaker(config, fake_clock, astorage)
    await astorage.trip_open(name=NAME, ttl=60.0)
    fake_clock.advance(WAIT)
    coordinator = _async_coordinator(breaker)
    await coordinator.poll_once()  # discovers the external OPEN
    await coordinator.poll_once()  # drives OPEN -> HALF_OPEN

    assert await coordinator.try_lease() is True
    assert await coordinator.try_lease() is True
    with pytest.raises(CircuitOpenError):
        async with breaker:
            pass


class AsyncFlakyStorage:
    """Async in-memory storage whose reads can be made to raise."""

    def __init__(self, inner: AsyncInMemoryStorage) -> None:
        self._inner = inner
        self.fail = False
        self.state_ttl = inner.state_ttl
        self.poll_interval = inner.poll_interval
        self.retry_backoff = inner.retry_backoff

    def _check(self) -> None:
        if self.fail:
            raise ConnectionError('storage down')

    async def read(self, name: str) -> SharedState | None:
        self._check()
        return await self._inner.read(name)

    async def trip_open(
        self, *, name: str, ttl: float, expected_version: int | None = None
    ) -> SharedState:
        self._check()
        return await self._inner.trip_open(name=name, ttl=ttl, expected_version=expected_version)

    async def begin_half_open_if_elapsed(
        self, *, name: str, wait_duration: float, permitted: int, ttl: float
    ) -> SharedState:
        self._check()
        return await self._inner.begin_half_open_if_elapsed(
            name=name, wait_duration=wait_duration, permitted=permitted, ttl=ttl
        )

    async def lease_probe(self, *, name: str, ttl: float) -> ProbeLease:
        self._check()
        return await self._inner.lease_probe(name=name, ttl=ttl)

    async def record_probe(self, *, name: str, outcome: Outcome, ttl: float) -> SharedState:
        self._check()
        return await self._inner.record_probe(name=name, outcome=outcome, ttl=ttl)

    async def close(
        self, *, name: str, ttl: float, expected_version: int | None = None
    ) -> SharedState:
        self._check()
        return await self._inner.close(name=name, ttl=ttl, expected_version=expected_version)


@pytest.mark.asyncio
async def test__async__degradation_and_recovery(config: Config, fake_clock: FakeClock) -> None:
    inner = AsyncInMemoryStorage(clock=fake_clock)
    inner.poll_interval = 3600.0
    await inner.trip_open(name=NAME, ttl=60.0)
    flaky = AsyncFlakyStorage(inner)
    listener = StorageEventsListener()
    breaker = _breaker(config, fake_clock, flaky, listener)
    coordinator = _async_coordinator(breaker)

    flaky.fail = True
    await coordinator.poll_once()
    assert len(listener.degraded) == 1
    assert await coordinator.try_lease() is None  # degraded: no storage access
    assert await breaker.call(_async_ok) == 'ok'  # local CLOSED admits

    flaky.fail = False
    fake_clock.advance(flaky.retry_backoff)
    await coordinator.poll_once()
    assert listener.recovered == 1
    assert breaker.state is State.OPEN  # shared authoritative again


async def _async_ok() -> str:
    return 'ok'


# --- background lane plumbing ---


def test__sync_lane_tick__timeout_polls(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, storage)
    storage.trip_open(name=NAME, ttl=60.0)
    coordinator = _coordinator(breaker)

    alive = _sync_lane_tick(weakref.ref(coordinator), coordinator._work, 0.001)

    assert alive is True
    assert breaker.state is State.OPEN  # the timeout tick polled and adopted the view


def test__sync_lane_tick__runs_queued_op(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, storage)
    coordinator = _coordinator(breaker)
    ran = []
    coordinator._work.put(lambda: ran.append(1))

    alive = _sync_lane_tick(weakref.ref(coordinator), coordinator._work, 0.001)

    assert alive is True
    assert ran == [1]
    coordinator.wait_idle()  # task_done was called: join must not hang


def test__sync_lane_tick__dead_coordinator_stops_lane(
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
    )
    work = coordinator._work
    ref = weakref.ref(coordinator)
    del coordinator
    gc.collect()

    assert ref() is None
    assert _sync_lane_tick(ref, work, 0.001) is False


@pytest.mark.asyncio
async def test__async_lane_tick__timeout_polls(config: Config, fake_clock: FakeClock) -> None:
    astorage = AsyncInMemoryStorage(clock=fake_clock)
    await astorage.trip_open(name=NAME, ttl=60.0)
    breaker = _breaker(config, fake_clock, astorage)
    coordinator = _async_coordinator(breaker)

    alive = await _async_lane_tick(weakref.ref(coordinator), coordinator._work, 0.001)

    assert alive is True
    assert breaker.state is State.OPEN


@pytest.mark.asyncio
async def test__async_lane_tick__runs_queued_op_and_dead_ref_stops(
    config: Config, fake_clock: FakeClock
) -> None:
    astorage = AsyncInMemoryStorage(clock=fake_clock)
    breaker = _breaker(config, fake_clock, astorage)
    coordinator = _async_coordinator(breaker)
    ran = []

    async def op() -> None:
        ran.append(1)

    coordinator._work.put_nowait(op)
    assert await _async_lane_tick(weakref.ref(coordinator), coordinator._work, 0.001) is True
    assert ran == [1]
    await coordinator.wait_idle()

    work = coordinator._work
    ref = weakref.ref(coordinator)
    del coordinator
    breaker._engine._async_coordinator = None
    gc.collect()
    assert await _async_lane_tick(ref, work, 0.001) is False


def test__lane_thread__starts_once_and_processes_writes(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    storage.poll_interval = 3600.0
    breaker = _breaker(config, fake_clock, storage)

    _trip(breaker)  # admissions + settle start the real lane thread
    _coordinator(breaker).wait_idle()

    shared = storage.read(NAME)
    assert shared is not None
    assert shared.state is State.OPEN


def test__degraded__lease_gate_closed__falls_back_to_local(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    flaky = FlakyStorage(storage)
    breaker = _breaker(config, fake_clock, flaky)
    storage.trip_open(name=NAME, ttl=60.0)
    fake_clock.advance(WAIT)
    _coordinator(breaker).poll_once()
    _coordinator(breaker).poll_once()  # HALF_OPEN now cached
    flaky.fail = True
    _coordinator(breaker).poll_once()  # degrade; backoff not yet elapsed

    assert _coordinator(breaker).try_lease() is None  # gate closed: no storage access
    assert breaker.call(lambda: 'local') == 'local'


def test__lane_op_failure__degrades(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    listener = StorageEventsListener()
    breaker = _breaker(config, fake_clock, storage, listener)
    coordinator = _coordinator(breaker)

    def bad_op() -> None:
        raise ConnectionError('down mid-write')

    coordinator.execute_op(bad_op)

    assert len(listener.degraded) == 1
    coordinator.execute_op(bad_op)  # gate closed: dropped without a second event
    assert len(listener.degraded) == 1


def test__sync_lane__exits_on_dead_coordinator(
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
    )
    work = coordinator._work
    ref = weakref.ref(coordinator)
    del coordinator
    gc.collect()

    _sync_lane(ref, work, 0.001)  # returns instead of looping forever


@pytest.mark.asyncio
async def test__async__lease_failure_degrades(config: Config, fake_clock: FakeClock) -> None:
    inner = AsyncInMemoryStorage(clock=fake_clock)
    flaky = AsyncFlakyStorage(inner)
    listener = StorageEventsListener()
    breaker = _breaker(config, fake_clock, flaky, listener)
    coordinator = _async_coordinator(breaker)
    flaky.fail = True

    assert await coordinator.try_lease() is None

    assert len(listener.degraded) == 1
    await coordinator.poll_once()  # gate closed: no storage access, no new event
    assert len(listener.degraded) == 1


@pytest.mark.asyncio
async def test__async__failed_probe_round_reopens(config: Config, fake_clock: FakeClock) -> None:
    astorage = AsyncInMemoryStorage(clock=fake_clock)
    astorage.poll_interval = 3600.0
    breaker = _breaker(config, fake_clock, astorage)

    async def boom() -> None:
        raise ValueError('boom')

    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            await breaker.call(boom)
    coordinator = _async_coordinator(breaker)
    await coordinator.wait_idle()
    fake_clock.advance(WAIT)
    await coordinator.poll_once()

    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            await breaker.call(boom)
        await coordinator.wait_idle()

    shared = await astorage.read(NAME)
    assert shared is not None
    assert shared.state is State.OPEN
    assert shared.probes_completed == 0  # fresh OPEN after the failed round


@pytest.mark.asyncio
async def test__async__lane_op_failure_and_degraded_drop(
    config: Config, fake_clock: FakeClock
) -> None:
    inner = AsyncInMemoryStorage(clock=fake_clock)
    listener = StorageEventsListener()
    breaker = _breaker(config, fake_clock, inner, listener)
    coordinator = _async_coordinator(breaker)

    async def bad_op() -> None:
        raise ConnectionError('down mid-write')

    await coordinator.execute_op(bad_op)

    assert len(listener.degraded) == 1
    await coordinator.execute_op(bad_op)  # gate closed: dropped, no second event
    assert len(listener.degraded) == 1


def test__recovery__default_noop_listener__does_not_break(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    flaky = FlakyStorage(storage)
    breaker = _breaker(config, fake_clock, flaky)  # no listener: default noop
    flaky.fail = True
    _coordinator(breaker).poll_once()  # degrade

    flaky.fail = False
    fake_clock.advance(flaky.retry_backoff)
    _coordinator(breaker).poll_once()  # recover through the noop listener

    assert breaker.call(lambda: 'ok') == 'ok'


@pytest.mark.asyncio
async def test__async__lease_failure_falls_back_to_local_admission(
    config: Config, fake_clock: FakeClock
) -> None:
    inner = AsyncInMemoryStorage(clock=fake_clock)
    inner.poll_interval = 3600.0
    await inner.trip_open(name=NAME, ttl=60.0)
    flaky = AsyncFlakyStorage(inner)
    listener = StorageEventsListener()
    breaker = _breaker(config, fake_clock, flaky, listener)
    coordinator = _async_coordinator(breaker)
    fake_clock.advance(WAIT)
    await coordinator.poll_once()  # discovers the external OPEN
    await coordinator.poll_once()  # drives OPEN -> HALF_OPEN
    assert breaker.state is State.HALF_OPEN

    flaky.fail = True
    assert await breaker.call(_async_ok) == 'ok'  # lease failed -> local CLOSED admits

    assert len(listener.degraded) == 1
