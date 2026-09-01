"""Coordinated (distributed) breaker behaviour over a shared ``Storage``.

Deterministic: a shared ``FakeClock`` drives both the engines and the in-memory
storage, background lanes are made inert with a huge ``poll_interval``, poll
ticks run via ``poll_once()``, and fire-and-forget writes are awaited with
``wait_idle()`` — no sleeps, no real Redis.
"""

import asyncio
import gc
import threading
import weakref
from dataclasses import replace
from typing import cast

import pytest

from conftest import ExplodingListener, FakeClock, RecordingListener
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
from interlock.protocols import AsyncStorage, Clock, EventListener, Storage
from interlock.shared import ProbeLease, SharedState

NAME = 'svc'
WAIT = 5.0
LANE_TIMEOUT = 5.0  # real seconds: a lane that never parks must fail, not hang


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
    """RecordingListener extended with the storage hooks."""

    def __init__(self) -> None:
        super().__init__()
        self.degraded: list[BaseException] = []
        self.recovered: int = 0
        self.write_dropped: int = 0

    def on_storage_degraded(self, *, name: str, error: BaseException) -> None:
        self.names.append(name)
        self.degraded.append(error)

    def on_storage_recovered(self, *, name: str) -> None:
        self.names.append(name)
        self.recovered += 1

    def on_storage_write_dropped(self, *, name: str) -> None:
        self.names.append(name)
        self.write_dropped += 1


class FlakyStorage:
    """In-memory storage whose every operation can be made to raise."""

    def __init__(self, inner: InMemoryStorage) -> None:
        self._inner = inner
        self.fail = False
        self.calls = 0
        self.state_ttl = inner.state_ttl
        self.poll_interval = inner.poll_interval
        self.retry_backoff = inner.retry_backoff
        self.retry_backoff_multiplier = inner.retry_backoff_multiplier
        self.retry_backoff_max = inner.retry_backoff_max
        self.retry_jitter = inner.retry_jitter

    def _check(self) -> None:
        self.calls += 1
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
    storage: Storage | AsyncStorage,
    listener: RecordingListener | None = None,
) -> CircuitBreaker:
    return CircuitBreaker(
        name=NAME,
        config=config,
        clock=fake_clock,
        storage=storage,
        listener=cast('EventListener | None', listener),
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


def _adopt_shared_state(
    breaker: CircuitBreaker, storage: InMemoryStorage, fake_clock: FakeClock, state: State
) -> None:
    storage.trip_open(name=NAME, ttl=60.0)
    coordinator = _coordinator(breaker)
    coordinator.poll_once()
    if state is State.HALF_OPEN:
        fake_clock.advance(WAIT)
        coordinator.poll_once()


@pytest.mark.parametrize('shared_state', [State.OPEN, State.HALF_OPEN])
@pytest.mark.parametrize(
    'manual_control',
    [
        ('force_open', State.FORCED_OPEN, False),
        ('disable', State.DISABLED, True),
        ('metrics_only', State.METRICS_ONLY, True),
    ],
)
def test__manual_control__shared_state__governs_local_admission_without_leasing_probe(
    config: Config,
    fake_clock: FakeClock,
    storage: InMemoryStorage,
    shared_state: State,
    manual_control: tuple[str, State, bool],
) -> None:
    control, expected_state, admitted = manual_control
    listener = RecordingListener()
    breaker = _breaker(config, fake_clock, storage, listener)
    _adopt_shared_state(breaker, storage, fake_clock, shared_state)
    listener.state_changes.clear()

    getattr(breaker, control)()

    assert breaker.state is expected_state
    assert listener.state_changes == [(shared_state, expected_state)]
    if admitted:
        assert breaker.call(lambda: 'local') == 'local'
    else:
        with pytest.raises(CircuitOpenError):
            breaker.call(lambda: 'must not run')

    shared = storage.read(NAME)
    assert shared is not None
    expected_remaining = (
        config.permitted_calls_in_half_open if shared_state is State.HALF_OPEN else 0
    )
    assert shared.probes_remaining == expected_remaining


@pytest.mark.parametrize('shared_state', [State.OPEN, State.HALF_OPEN])
def test__reset__shared_state__resumes_shared_admission(
    config: Config,
    fake_clock: FakeClock,
    storage: InMemoryStorage,
    shared_state: State,
) -> None:
    listener = RecordingListener()
    breaker = _breaker(config, fake_clock, storage, listener)
    _adopt_shared_state(breaker, storage, fake_clock, shared_state)
    breaker.force_open()
    listener.state_changes.clear()

    breaker.reset()

    assert breaker.state is shared_state
    assert listener.state_changes == [(State.FORCED_OPEN, shared_state)]
    assert listener.resets == 1
    if shared_state is State.OPEN:
        with pytest.raises(CircuitOpenError):
            breaker.call(lambda: 'must not run')
        return

    assert breaker.call(lambda: 'probe') == 'probe'
    _coordinator(breaker).wait_idle()
    shared = storage.read(NAME)
    assert shared is not None
    assert shared.probes_remaining == config.permitted_calls_in_half_open - 1
    assert shared.probes_completed == 1


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
    state = follower.state
    assert state is State.OPEN  # shared OPEN adopted

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
    assert breaker.state is State.OPEN
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


# --- degraded-storage retry policy: growth, cap, jitter, reset (#100) ---


def test__degraded__backoff_grows_geometrically_with_consecutive_failures(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    flaky = FlakyStorage(storage)
    flaky.retry_backoff = 1.0
    flaky.retry_backoff_multiplier = 2.0
    breaker = _breaker(config, fake_clock, flaky)
    flaky.fail = True

    _coordinator(breaker).poll_once()  # 1st failure: next delay = 1.0 * 2**0 = 1.0
    assert flaky.calls == 1

    fake_clock.advance(0.999)
    _coordinator(breaker).poll_once()  # not due yet: gate stays closed
    assert flaky.calls == 1

    fake_clock.advance(0.002)  # now 1.001, past the 1.0s delay
    _coordinator(breaker).poll_once()  # 2nd failure: next delay = 1.0 * 2**1 = 2.0
    assert flaky.calls == 2

    fake_clock.advance(1.999)  # now 3.0, just short of 1.001 + 2.0 = 3.001
    _coordinator(breaker).poll_once()
    assert flaky.calls == 2  # still gated: proves the delay actually doubled

    fake_clock.advance(0.002)  # now 3.002, past the due time
    _coordinator(breaker).poll_once()  # 3rd failure confirms the geometric growth held
    assert flaky.calls == 3


def test__degraded__backoff_capped_at_retry_backoff_max(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    flaky = FlakyStorage(storage)
    flaky.retry_backoff = 1.0
    flaky.retry_backoff_multiplier = 10.0
    flaky.retry_backoff_max = 5.0
    breaker = _breaker(config, fake_clock, flaky)
    flaky.fail = True

    _coordinator(breaker).poll_once()  # 1st failure: next delay = 1.0 (uncapped)
    assert flaky.calls == 1

    fake_clock.advance(1.0)
    _coordinator(breaker).poll_once()  # 2nd failure: 1.0 * 10**1 = 10, capped to 5.0
    assert flaky.calls == 2

    fake_clock.advance(4.999)
    _coordinator(breaker).poll_once()  # not yet past the capped 5.0s delay
    assert flaky.calls == 2

    fake_clock.advance(0.002)  # now past it: an uncapped 10s delay would still gate this
    _coordinator(breaker).poll_once()
    assert flaky.calls == 3


def test__degraded__attempt_counter_resets_on_recovery(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    flaky = FlakyStorage(storage)
    flaky.retry_backoff = 1.0
    flaky.retry_backoff_multiplier = 2.0
    breaker = _breaker(config, fake_clock, flaky)
    flaky.fail = True

    _coordinator(breaker).poll_once()  # attempt 0: next delay 1.0
    fake_clock.advance(1.0)
    _coordinator(breaker).poll_once()  # attempt 1: next delay 2.0, still failing
    assert flaky.calls == 2

    flaky.fail = False
    fake_clock.advance(2.0)
    _coordinator(breaker).poll_once()  # recovers: attempt counter resets to 0
    assert flaky.calls == 3

    flaky.fail = True
    _coordinator(breaker).poll_once()  # fails again right away: gate was open (not degraded)
    assert flaky.calls == 4  # this failure re-degrades using attempt 0, not 2

    fake_clock.advance(0.999)
    _coordinator(breaker).poll_once()
    assert flaky.calls == 4  # gated at ~1.0s, proving the counter did not keep growing from before

    fake_clock.advance(0.002)
    _coordinator(breaker).poll_once()
    assert flaky.calls == 5


def test__degraded__jitter_is_bounded_and_reproducible_for_the_same_seed(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    flaky_a = FlakyStorage(storage)
    flaky_a.retry_backoff = 10.0
    flaky_a.retry_jitter = 0.5
    breaker_a = _breaker(config, fake_clock, flaky_a)

    flaky_b = FlakyStorage(storage)
    flaky_b.retry_backoff = 10.0
    flaky_b.retry_jitter = 0.5
    breaker_b = _breaker(config, fake_clock, flaky_b)  # different instance, same name + clock

    delay_a = _coordinator(breaker_a)._next_delay(0)
    delay_b = _coordinator(breaker_b)._next_delay(0)

    # Same (name, clock reading, attempt) seed -> the same draw, not process-global
    # randomness, so the delay is reproducible under a fake clock in tests.
    assert delay_a == delay_b
    assert 10.0 <= delay_a < 15.0  # base delay plus up to 50% proportional spread

    fake_clock.advance(1.0)
    delay_later = _coordinator(breaker_a)._next_delay(0)
    assert delay_later != delay_a  # a different clock reading draws differently


@pytest.mark.asyncio
async def test__async__degraded__backoff_grows_geometrically_with_consecutive_failures(
    config: Config, fake_clock: FakeClock
) -> None:
    inner = AsyncInMemoryStorage(clock=fake_clock)
    flaky = AsyncFlakyStorage(inner)
    flaky.retry_backoff = 1.0
    flaky.retry_backoff_multiplier = 2.0
    breaker = _breaker(config, fake_clock, flaky)
    coordinator = _async_coordinator(breaker)
    flaky.fail = True

    await coordinator.poll_once()  # 1st failure: next delay = 1.0
    assert flaky.calls == 1

    fake_clock.advance(0.999)
    await coordinator.poll_once()
    assert flaky.calls == 1  # not due yet

    fake_clock.advance(0.002)
    await coordinator.poll_once()  # 2nd failure: next delay = 2.0
    assert flaky.calls == 2

    fake_clock.advance(1.999)
    await coordinator.poll_once()
    assert flaky.calls == 2  # still gated: the delay doubled, not stayed at 1.0

    fake_clock.advance(0.002)
    await coordinator.poll_once()
    assert flaky.calls == 3


@pytest.mark.asyncio
async def test__async__degraded__attempt_counter_resets_on_recovery(
    config: Config, fake_clock: FakeClock
) -> None:
    inner = AsyncInMemoryStorage(clock=fake_clock)
    flaky = AsyncFlakyStorage(inner)
    flaky.retry_backoff = 1.0
    flaky.retry_backoff_multiplier = 2.0
    breaker = _breaker(config, fake_clock, flaky)
    coordinator = _async_coordinator(breaker)
    flaky.fail = True

    await coordinator.poll_once()  # attempt 0: next delay 1.0
    fake_clock.advance(1.0)
    await coordinator.poll_once()  # attempt 1: next delay 2.0
    assert flaky.calls == 2

    flaky.fail = False
    fake_clock.advance(2.0)
    await coordinator.poll_once()  # recovers: attempt counter resets to 0
    assert flaky.calls == 3

    flaky.fail = True
    await coordinator.poll_once()  # fails again immediately (gate was open)
    assert flaky.calls == 4

    fake_clock.advance(0.999)
    await coordinator.poll_once()
    assert flaky.calls == 4  # gated at ~1.0s again, not continuing from 4.0

    fake_clock.advance(0.002)
    await coordinator.poll_once()
    assert flaky.calls == 5


# --- bounded write queue (#99) ---


class BlockingTripStorage(InMemoryStorage):
    """In-memory storage whose ``trip_open`` parks the lane until released.

    A parked lane is what makes the queue fill up deterministically: it can
    neither drain a write nor poll while it waits on ``release``.
    """

    def __init__(self, *, clock: Clock) -> None:
        super().__init__(clock=clock)
        self.entered = threading.Event()
        self.release = threading.Event()

    def trip_open(
        self, *, name: str, ttl: float, expected_version: int | None = None
    ) -> SharedState:
        self.entered.set()
        self.release.wait()
        return super().trip_open(name=name, ttl=ttl, expected_version=expected_version)


class AsyncBlockingTripStorage(AsyncInMemoryStorage):
    """Async mirror of ``BlockingTripStorage``."""

    def __init__(self, *, clock: Clock) -> None:
        super().__init__(clock=clock)
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def trip_open(
        self, *, name: str, ttl: float, expected_version: int | None = None
    ) -> SharedState:
        self.entered.set()
        await self.release.wait()
        return await super().trip_open(name=name, ttl=ttl, expected_version=expected_version)


def _blocking_storage(fake_clock: FakeClock) -> BlockingTripStorage:
    storage = BlockingTripStorage(clock=fake_clock)
    storage.poll_interval = 3600.0
    storage.write_queue_size = 1
    return storage


def test__write_queue__at_capacity__drops_the_write_and_reports_it(
    config: Config, fake_clock: FakeClock
) -> None:
    storage = _blocking_storage(fake_clock)
    listener = StorageEventsListener()
    breaker = _breaker(config, fake_clock, storage, listener)
    coordinator = _coordinator(breaker)

    coordinator.notify_local_trip()  # the lane takes this one and parks in trip_open
    assert storage.entered.wait(timeout=LANE_TIMEOUT)
    coordinator.notify_local_trip()  # fills the single queue slot
    coordinator.notify_local_trip()  # over capacity: dropped, never queued

    assert listener.write_dropped == 1
    assert listener.names == [NAME]
    assert not coordinator._work.full()  # the slot reserved for the sentinel stays free

    storage.release.set()
    breaker.close()


def test__write_queue__at_capacity__settle_neither_blocks_nor_raises(
    config: Config, fake_clock: FakeClock
) -> None:
    storage = _blocking_storage(fake_clock)
    listener = StorageEventsListener()
    breaker = _breaker(config, fake_clock, storage, listener)
    coordinator = _coordinator(breaker)

    coordinator.notify_local_trip()
    assert storage.entered.wait(timeout=LANE_TIMEOUT)
    coordinator.notify_local_trip()  # queue now full

    _trip(breaker)  # the trip write is dropped inside _settle, not raised there

    assert breaker.state is State.OPEN  # local protection is unaffected
    assert listener.write_dropped == 1

    storage.release.set()
    breaker.close()


@pytest.mark.asyncio
async def test__async__write_queue__at_capacity__drops_the_write_and_reports_it(
    config: Config, fake_clock: FakeClock
) -> None:
    storage = AsyncBlockingTripStorage(clock=fake_clock)
    storage.poll_interval = 3600.0
    storage.write_queue_size = 1
    listener = StorageEventsListener()
    breaker = _breaker(config, fake_clock, storage, listener)
    coordinator = _async_coordinator(breaker)

    coordinator.notify_local_trip()
    await asyncio.wait_for(storage.entered.wait(), timeout=LANE_TIMEOUT)
    coordinator.notify_local_trip()
    coordinator.notify_local_trip()

    assert listener.write_dropped == 1
    assert not coordinator._work.full()

    storage.release.set()
    await breaker.aclose()


def test__write_queue__at_capacity__raising_listener_is_isolated_from_the_call(
    config: Config, fake_clock: FakeClock, exploding: ExplodingListener
) -> None:
    storage = _blocking_storage(fake_clock)
    breaker = CircuitBreaker(
        name=NAME,
        config=config,
        clock=fake_clock,
        storage=storage,
        listener=cast('EventListener', exploding),
    )
    coordinator = _coordinator(breaker)

    coordinator.notify_local_trip()
    assert storage.entered.wait(timeout=LANE_TIMEOUT)
    coordinator.notify_local_trip()  # queue now full

    _trip(breaker)  # the drop hook raises; the calls keep their own exception

    assert 'on_storage_write_dropped' in exploding.events
    assert breaker.state is State.OPEN

    storage.release.set()
    breaker.close()


def test__write_queue__default_size__is_the_documented_bound(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    del storage.write_queue_size  # a storage that predates the knob
    breaker = _breaker(config, fake_clock, storage)

    assert _coordinator(breaker)._queue_size == 128


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
    state = breaker.state
    assert state is State.OPEN

    fake_clock.advance(WAIT)
    await coordinator.poll_once()
    state = breaker.state
    assert state is State.HALF_OPEN

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


async def _adopt_async_shared_state(
    breaker: CircuitBreaker,
    storage: AsyncInMemoryStorage,
    fake_clock: FakeClock,
    state: State,
) -> None:
    await storage.trip_open(name=NAME, ttl=60.0)
    coordinator = _async_coordinator(breaker)
    await coordinator.poll_once()
    if state is State.HALF_OPEN:
        fake_clock.advance(WAIT)
        await coordinator.poll_once()


@pytest.mark.asyncio
@pytest.mark.parametrize('shared_state', [State.OPEN, State.HALF_OPEN])
@pytest.mark.parametrize(
    'manual_control',
    [
        ('force_open', State.FORCED_OPEN, False),
        ('disable', State.DISABLED, True),
        ('metrics_only', State.METRICS_ONLY, True),
    ],
)
async def test__async__manual_control__shared_state__governs_local_admission_without_leasing_probe(
    config: Config,
    fake_clock: FakeClock,
    shared_state: State,
    manual_control: tuple[str, State, bool],
) -> None:
    control, expected_state, admitted = manual_control
    storage = AsyncInMemoryStorage(clock=fake_clock)
    storage.poll_interval = 3600.0
    listener = RecordingListener()
    breaker = _breaker(config, fake_clock, storage, listener)
    await _adopt_async_shared_state(breaker, storage, fake_clock, shared_state)
    listener.state_changes.clear()

    getattr(breaker, control)()

    assert breaker.state is expected_state
    assert listener.state_changes == [(shared_state, expected_state)]
    if admitted:
        assert await breaker.call(_async_ok) == 'ok'
    else:
        with pytest.raises(CircuitOpenError):
            await breaker.call(_async_ok)

    shared = await storage.read(NAME)
    assert shared is not None
    expected_remaining = (
        config.permitted_calls_in_half_open if shared_state is State.HALF_OPEN else 0
    )
    assert shared.probes_remaining == expected_remaining


@pytest.mark.asyncio
@pytest.mark.parametrize('shared_state', [State.OPEN, State.HALF_OPEN])
async def test__async__reset__shared_state__resumes_shared_admission(
    config: Config, fake_clock: FakeClock, shared_state: State
) -> None:
    storage = AsyncInMemoryStorage(clock=fake_clock)
    storage.poll_interval = 3600.0
    listener = RecordingListener()
    breaker = _breaker(config, fake_clock, storage, listener)
    await _adopt_async_shared_state(breaker, storage, fake_clock, shared_state)
    breaker.force_open()
    listener.state_changes.clear()

    breaker.reset()

    assert breaker.state is shared_state
    assert listener.state_changes == [(State.FORCED_OPEN, shared_state)]
    assert listener.resets == 1
    if shared_state is State.OPEN:
        with pytest.raises(CircuitOpenError):
            await breaker.call(_async_ok)
        return

    assert await breaker.call(_async_ok) == 'ok'
    await _async_coordinator(breaker).wait_idle()
    shared = await storage.read(NAME)
    assert shared is not None
    assert shared.probes_remaining == config.permitted_calls_in_half_open - 1
    assert shared.probes_completed == 1


class AsyncFlakyStorage:
    """Async in-memory storage whose reads can be made to raise."""

    def __init__(self, inner: AsyncInMemoryStorage) -> None:
        self._inner = inner
        self.fail = False
        self.calls = 0
        self.state_ttl = inner.state_ttl
        self.poll_interval = inner.poll_interval
        self.retry_backoff = inner.retry_backoff
        self.retry_backoff_multiplier = inner.retry_backoff_multiplier
        self.retry_backoff_max = inner.retry_backoff_max
        self.retry_jitter = inner.retry_jitter

    def _check(self) -> None:
        self.calls += 1
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
        on_write_dropped=lambda: None,
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
        on_write_dropped=lambda: None,
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


def test__interrupted_leased_probe__not_returned_to_shared_budget(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    a = _breaker(config, fake_clock, storage)
    _trip(a)
    _coordinator(a).wait_idle()
    fake_clock.advance(WAIT)
    _coordinator(a).poll_once()

    def interrupted() -> None:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        a.call(interrupted)  # leased probe 1 of 2, interrupted mid-flight
    _coordinator(a).wait_idle()

    # The storage protocol has no un-lease operation: the slot stays consumed
    # (the backend TTL bounds the leak) and no outcome is recorded for it.
    shared = storage.read(NAME)
    assert shared is not None
    assert shared.state is State.HALF_OPEN
    assert shared.probes_remaining == 1
    assert shared.probes_completed == 0


def test__shared_open__rejection__reports_the_breaker_and_its_last_failure(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    listener = RecordingListener()
    breaker = _breaker(config, fake_clock, storage, listener)
    _trip(breaker)
    _coordinator(breaker).wait_idle()
    _coordinator(breaker).poll_once()  # adopt its own trip as the shared view
    listener.rejected = 0

    with pytest.raises(CircuitOpenError) as exc_info:
        breaker.call(lambda: 1)

    # A shared trip carries no local wait to estimate from, but it must still
    # say which breaker rejected the call and why it was open.
    assert listener.rejected == 1
    assert set(listener.names) == {NAME}
    assert exc_info.value.breaker_name == NAME
    assert exc_info.value.retry_after is None
    assert isinstance(exc_info.value.last_failure, ValueError)


def test__denied_lease__rejection__reports_the_breaker_and_its_last_failure(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    listener = RecordingListener()
    a = _breaker(config, fake_clock, storage, listener)
    b = _breaker(config, fake_clock, storage)
    _trip(a)
    _coordinator(a).wait_idle()
    fake_clock.advance(WAIT)
    _coordinator(a).poll_once()
    _coordinator(b).poll_once()
    assert a.call(lambda: 'probe-a') == 'probe-a'  # lease 1 of 2
    assert b.call(lambda: 'probe-b') == 'probe-b'  # lease 2 of 2
    listener.rejected = 0

    with pytest.raises(CircuitOpenError) as exc_info:
        a.call(lambda: 'probe-c')  # global budget spent

    assert listener.rejected == 1
    assert set(listener.names) == {NAME}
    assert exc_info.value.breaker_name == NAME
    assert exc_info.value.retry_after is None
    assert isinstance(exc_info.value.last_failure, ValueError)


def test__storage_events__are_attributed_to_the_breaker(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    flaky = FlakyStorage(storage)
    listener = StorageEventsListener()
    breaker = _breaker(config, fake_clock, flaky, listener)
    flaky.fail = True
    _coordinator(breaker).poll_once()

    flaky.fail = False
    fake_clock.advance(flaky.retry_backoff)
    _coordinator(breaker).poll_once()

    assert len(listener.degraded) == 1
    assert listener.recovered == 1
    assert set(listener.names) == {NAME}


def test__leased_probe__settling_after_a_local_reset__is_dropped(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, storage)
    _trip(breaker)
    _coordinator(breaker).wait_idle()
    fake_clock.advance(WAIT)
    _coordinator(breaker).poll_once()
    engine = breaker._engine
    start, admission = engine.enter_block()  # leased probe, still running

    breaker.reset()  # the operator resets the local machine under it
    engine.exit_block(start=start, admission=admission, exception=ValueError('late'))

    # The lease was granted in the era the reset ended: the outcome belongs to
    # nobody and must not land in the fresh window.
    assert breaker.snapshot().total_calls == 0


# --- adopting a shared view ---


def test__shared_view__newer_open__does_not_reset_the_local_machine(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    flaky = FlakyStorage(storage)
    breaker = _breaker(config, fake_clock, flaky, listener=None)
    _trip(breaker)
    _coordinator(breaker).wait_idle()
    _coordinator(breaker).poll_once()

    storage.close(name=NAME, ttl=60.0)  # a peer closes, then trips again: newer OPEN
    storage.trip_open(name=NAME, ttl=60.0)
    _coordinator(breaker).poll_once()

    # Only a shared *recovery* may cut a local OPEN short. Degrading afterwards
    # hands protection back to the local machine, which is where the difference
    # would show: a machine reset here would admit traffic to a dead dependency.
    flaky.fail = True
    _coordinator(breaker).poll_once()
    assert breaker.state is State.OPEN


def test__shared_view__closed_at_the_same_version__does_not_reset_the_local_machine(
    config: Config, fake_clock: FakeClock, storage: InMemoryStorage
) -> None:
    breaker = _breaker(config, fake_clock, storage)
    _trip(breaker)
    opened = SharedState(
        state=State.OPEN,
        opened_at=0.0,
        version=7,
        probes_permitted=0,
        probes_remaining=0,
        probes_completed=0,
        probe_failures=0,
        probe_slows=0,
    )
    breaker._engine._on_shared_view(opened)

    breaker._engine._on_shared_view(replace(opened, state=State.CLOSED))

    # Same version: a duplicate or out-of-order delivery, not a recovery.
    assert breaker.state is State.OPEN


def test__breaker__backoff_with_shared_storage__rejected(fake_clock: FakeClock) -> None:
    """The combination has to fail loudly: the coordinated lane cannot honour a backoff.

    Reopening is decided by the backend from ``wait_duration_in_open``, and no
    failed-round count crosses the wire, so a multiplier set here would be read,
    validated and then quietly ignored — the exact trap the option exists to
    remove.
    """
    with pytest.raises(ValueError, match='wait_duration_backoff_multiplier'):
        CircuitBreaker(
            name='payments',
            config=Config(wait_duration_backoff_multiplier=2.0),
            clock=fake_clock,
            storage=InMemoryStorage(clock=fake_clock),
        )


def test__registry__backoff_with_shared_storage__rejected(fake_clock: FakeClock) -> None:
    """Fail at registry construction, not at the first ``get`` that happens to run."""
    with pytest.raises(ValueError, match='wait_duration_backoff_multiplier'):
        Registry(
            config=Config(wait_duration_backoff_multiplier=2.0),
            clock=fake_clock,
            storage=InMemoryStorage(clock=fake_clock),
        )


def test__registry__backoff_in_a_per_breaker_config__rejected(fake_clock: FakeClock) -> None:
    """``get`` takes a config override, and that route must not slip past the guard."""
    registry = Registry(clock=fake_clock, storage=InMemoryStorage(clock=fake_clock))

    with pytest.raises(ValueError, match='wait_duration_backoff_multiplier'):
        registry.get('payments', config=Config(wait_duration_backoff_multiplier=2.0))


def test__breaker__backoff_without_storage__accepted(fake_clock: FakeClock) -> None:
    """Only the combination is refused; a local breaker keeps its backoff."""
    breaker = CircuitBreaker(
        name='payments',
        config=Config(wait_duration_backoff_multiplier=2.0),
        clock=fake_clock,
    )

    assert breaker.state is State.CLOSED
