"""Listener failures never reach the protected path, the state machine or a lane.

``EventListener`` implementations are optional observability. A bug in one must
not replace a successful result, mask a dependency's exception, interrupt
transition bookkeeping, or kill a coordinator lane — so every hook is dispatched
through ``interlock._notify.notify``. These tests pin that policy end to end;
the pipeline hooks are covered in ``test_pipeline.py`` / ``test_tenacity.py``.
"""

import logging
import weakref
from typing import cast

import pytest

from conftest import ExplodingListener, FakeClock, RecordingListener
from inmemory_storage import AsyncInMemoryStorage, InMemoryStorage
from interlock import CircuitBreaker, CircuitOpenError, Config, State
from interlock._coordination import (
    AsyncCoordinator,
    SyncCoordinator,
    _async_lane_tick,
    _sync_lane_tick,
)
from interlock._notify import notify
from interlock.protocols import AsyncStorage, EventListener, Storage
from interlock.shared import SharedState

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


def _breaker(
    config: Config,
    fake_clock: FakeClock,
    exploding: ExplodingListener,
    storage: Storage | AsyncStorage | None = None,
) -> CircuitBreaker:
    return CircuitBreaker(
        name=NAME,
        config=config,
        clock=fake_clock,
        storage=storage,
        listener=cast('EventListener', exploding),
    )


@pytest.fixture
def breaker(config: Config, fake_clock: FakeClock, exploding: ExplodingListener) -> CircuitBreaker:
    return _breaker(config, fake_clock, exploding)


def _boom() -> None:
    msg = 'boom'
    raise ValueError(msg)


def _trip(breaker: CircuitBreaker) -> None:
    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            breaker.call(_boom)


# --- the dispatcher itself ---------------------------------------------------


def test__notify__no_listener__does_nothing() -> None:
    notify(None, 'on_rejected', name=NAME)


def test__notify__listener_without_the_hook__skips_dispatch() -> None:
    pre_v2 = cast('EventListener', RecordingListener())

    notify(pre_v2, 'on_fallback', name=NAME, error=ValueError('x'))


def test__notify__raising_hook__is_logged_with_context(
    caplog: pytest.LogCaptureFixture, exploding: ExplodingListener
) -> None:
    with caplog.at_level(logging.ERROR, logger='interlock'):
        notify(cast('EventListener', exploding), 'on_rejected', name=NAME)

    record = caplog.records[-1]
    assert record.levelno == logging.ERROR
    assert 'on_rejected' in record.getMessage()
    assert NAME in record.getMessage()
    assert record.exc_info is not None  # the traceback is not swallowed


def test__notify__base_exception__propagates() -> None:
    class Cancelling:
        def on_rejected(self, *, name: str) -> None:
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        notify(cast('EventListener', Cancelling()), 'on_rejected', name=NAME)


# --- core hooks --------------------------------------------------------------


def test__call__on_call_raises__returns_the_protected_result(
    breaker: CircuitBreaker, exploding: ExplodingListener
) -> None:
    assert breaker.call(lambda: 'ok') == 'ok'
    assert exploding.events == ['on_call']


@pytest.mark.asyncio
async def test__async_call__on_call_raises__returns_the_protected_result(
    breaker: CircuitBreaker, exploding: ExplodingListener
) -> None:
    async def work() -> str:
        return 'ok'

    assert await breaker.call(work) == 'ok'
    assert exploding.events == ['on_call']


def test__call__on_call_raises__propagates_the_original_failure(breaker: CircuitBreaker) -> None:
    with pytest.raises(ValueError, match='boom'):
        breaker.call(_boom)  # the listener error must not mask the dependency's


def test__guarded_block__on_call_raises__leaves_the_block_intact(
    breaker: CircuitBreaker, exploding: ExplodingListener
) -> None:
    with breaker:
        result = 'ok'

    assert result == 'ok'
    assert exploding.events == ['on_call']


def test__on_state_change_raises__transition_still_recorded(
    breaker: CircuitBreaker, exploding: ExplodingListener
) -> None:
    _trip(breaker)

    assert breaker.state is State.OPEN
    assert 'on_state_change' in exploding.events


def test__on_rejected_raises__still_raises_circuit_open_error(breaker: CircuitBreaker) -> None:
    _trip(breaker)

    with pytest.raises(CircuitOpenError):
        breaker.call(lambda: 'never')


def test__on_reset_raises__reset_completes(
    breaker: CircuitBreaker, exploding: ExplodingListener
) -> None:
    _trip(breaker)

    breaker.reset()

    assert breaker.state is State.CLOSED
    assert 'on_reset' in exploding.events


# --- storage hooks and the coordinator lane ----------------------------------


class BrokenStorage(InMemoryStorage):
    """In-memory storage whose reads can be made to fail, driving degradation."""

    def __init__(self, clock: FakeClock) -> None:
        super().__init__(clock=clock)
        self.fail = False
        self.poll_interval = 3600.0  # keep the real lane inert; tests poll manually

    def read(self, name: str) -> SharedState | None:
        if self.fail:
            msg = 'storage down'
            raise ConnectionError(msg)
        return super().read(name)


def _shared_storage(fake_clock: FakeClock) -> InMemoryStorage:
    storage = InMemoryStorage(clock=fake_clock)
    storage.poll_interval = 3600.0
    return storage


def _coordinator(breaker: CircuitBreaker) -> SyncCoordinator:
    coordinator = breaker._engine._sync_coordinator
    assert coordinator is not None
    return coordinator


def _async_coordinator(breaker: CircuitBreaker) -> AsyncCoordinator:
    coordinator = breaker._engine._async_coordinator
    assert coordinator is not None
    return coordinator


def test__on_storage_degraded_raises__degradation_still_applies(
    config: Config, fake_clock: FakeClock, exploding: ExplodingListener
) -> None:
    storage = BrokenStorage(fake_clock)
    breaker = _breaker(config, fake_clock, exploding, storage)
    storage.fail = True

    _coordinator(breaker).poll_once()

    assert exploding.events == ['on_storage_degraded']
    assert breaker.call(lambda: 'ok') == 'ok'  # local state took over


def test__on_storage_recovered_raises__recovery_still_applies(
    config: Config, fake_clock: FakeClock, exploding: ExplodingListener
) -> None:
    storage = BrokenStorage(fake_clock)
    breaker = _breaker(config, fake_clock, exploding, storage)
    storage.fail = True
    _coordinator(breaker).poll_once()

    storage.fail = False
    fake_clock.advance(storage.retry_backoff)
    _coordinator(breaker).poll_once()

    assert exploding.events == ['on_storage_degraded', 'on_storage_recovered']
    assert breaker.call(lambda: 'ok') == 'ok'


def test__poll_once__on_state_change_raises__is_not_reported_as_degradation(
    config: Config, fake_clock: FakeClock, exploding: ExplodingListener
) -> None:
    storage = _shared_storage(fake_clock)
    storage.trip_open(name=NAME, ttl=60.0)  # a peer tripped; this poll adopts it
    breaker = _breaker(config, fake_clock, exploding, storage)

    _coordinator(breaker).poll_once()

    assert exploding.events == ['on_state_change']  # a listener bug is not a storage failure
    assert breaker.state is State.OPEN  # the shared view was still adopted


def test__execute_op__on_state_change_raises__is_not_reported_as_degradation(
    config: Config, fake_clock: FakeClock, exploding: ExplodingListener
) -> None:
    storage = _shared_storage(fake_clock)
    breaker = _breaker(config, fake_clock, exploding, storage)
    coordinator = _coordinator(breaker)

    coordinator.execute_op(lambda: coordinator._accept(storage.trip_open(name=NAME, ttl=60.0)))

    assert exploding.events == ['on_state_change']


def test__denied_shared_probe__on_rejected_raises__still_rejects(
    config: Config, fake_clock: FakeClock, exploding: ExplodingListener
) -> None:
    storage = _shared_storage(fake_clock)
    storage.trip_open(name=NAME, ttl=60.0)
    breaker = _breaker(config, fake_clock, exploding, storage)
    coordinator = _coordinator(breaker)
    coordinator.poll_once()  # adopt the shared OPEN
    fake_clock.advance(WAIT)
    coordinator.poll_once()  # OPEN -> HALF_OPEN
    for _ in range(config.permitted_calls_in_half_open):
        storage.lease_probe(name=NAME, ttl=60.0)  # peers take every probe slot

    with pytest.raises(CircuitOpenError):
        breaker.call(lambda: 'never')

    assert exploding.events[-1] == 'on_rejected'


def test__sync_lane_tick__on_state_change_raises__lane_keeps_running(
    config: Config, fake_clock: FakeClock, exploding: ExplodingListener
) -> None:
    storage = _shared_storage(fake_clock)
    storage.trip_open(name=NAME, ttl=60.0)
    breaker = _breaker(config, fake_clock, exploding, storage)
    coordinator = _coordinator(breaker)

    alive = _sync_lane_tick(weakref.ref(coordinator), coordinator._work, 0.001)

    assert alive is True  # a raising listener must not terminate the lane
    assert exploding.events == ['on_state_change']


@pytest.mark.asyncio
async def test__async_lane_tick__on_state_change_raises__lane_keeps_running(
    config: Config, fake_clock: FakeClock, exploding: ExplodingListener
) -> None:
    storage = AsyncInMemoryStorage(clock=fake_clock)
    storage.poll_interval = 3600.0
    await storage.trip_open(name=NAME, ttl=60.0)
    breaker = _breaker(config, fake_clock, exploding, storage)
    coordinator = _async_coordinator(breaker)

    alive = await _async_lane_tick(weakref.ref(coordinator), coordinator._work, 0.001)

    assert alive is True
    assert exploding.events == ['on_state_change']
