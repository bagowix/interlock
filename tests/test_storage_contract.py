"""Behavioural contract for the ``Storage`` / ``AsyncStorage`` protocols.

These tests pin the semantics every storage backend must uphold, exercised here
against the in-memory reference double. When ``RedisStorage`` lands (T2) the same
expectations apply; the suite is the portable definition of "conforms".
"""

import pytest
from tests.conftest import FakeClock
from tests.inmemory_storage import AsyncInMemoryStorage, InMemoryStorage

from interlock import Outcome, State

WAIT = 30.0
PERMITTED = 3
TTL = 60.0
NAME = 'svc'


def test__read__absent_key__returns_none() -> None:
    storage = InMemoryStorage(clock=FakeClock())

    assert storage.read(NAME) is None


def test__trip_open__from_closed__opens_and_stamps_time() -> None:
    clock = FakeClock()
    clock.advance(5.0)
    storage = InMemoryStorage(clock=clock)

    state = storage.trip_open(name=NAME, ttl=TTL)

    assert state.state is State.OPEN
    assert state.opened_at == 5.0
    assert state.version == 1


def test__trip_open__already_open__is_idempotent() -> None:
    clock = FakeClock()
    storage = InMemoryStorage(clock=clock)
    first = storage.trip_open(name=NAME, ttl=TTL)

    clock.advance(10.0)
    again = storage.trip_open(name=NAME, ttl=TTL)

    assert again.opened_at == first.opened_at
    assert again.version == first.version


def test__begin_half_open__before_wait_elapsed__stays_open() -> None:
    clock = FakeClock()
    storage = InMemoryStorage(clock=clock)
    storage.trip_open(name=NAME, ttl=TTL)

    clock.advance(WAIT - 0.1)
    state = storage.begin_half_open_if_elapsed(
        name=NAME, wait_duration=WAIT, permitted=PERMITTED, ttl=TTL
    )

    assert state.state is State.OPEN
    assert state.probes_remaining == 0


def test__begin_half_open__after_wait_elapsed__seeds_probe_budget() -> None:
    clock = FakeClock()
    storage = InMemoryStorage(clock=clock)
    storage.trip_open(name=NAME, ttl=TTL)

    clock.advance(WAIT)
    state = storage.begin_half_open_if_elapsed(
        name=NAME, wait_duration=WAIT, permitted=PERMITTED, ttl=TTL
    )

    assert state.state is State.HALF_OPEN
    assert state.probes_permitted == PERMITTED
    assert state.probes_remaining == PERMITTED
    assert state.probes_completed == 0


def test__begin_half_open__second_call__does_not_reseed() -> None:
    clock = FakeClock()
    storage = InMemoryStorage(clock=clock)
    storage.trip_open(name=NAME, ttl=TTL)
    clock.advance(WAIT)
    first = storage.begin_half_open_if_elapsed(
        name=NAME, wait_duration=WAIT, permitted=PERMITTED, ttl=TTL
    )
    storage.lease_probe(name=NAME, ttl=TTL)

    again = storage.begin_half_open_if_elapsed(
        name=NAME, wait_duration=WAIT, permitted=PERMITTED, ttl=TTL
    )

    assert again.version == first.version + 1  # only the lease bumped it
    assert again.probes_remaining == PERMITTED - 1


def _half_open(storage: InMemoryStorage, clock: FakeClock) -> None:
    storage.trip_open(name=NAME, ttl=TTL)
    clock.advance(WAIT)
    storage.begin_half_open_if_elapsed(name=NAME, wait_duration=WAIT, permitted=PERMITTED, ttl=TTL)


def test__lease_probe__grants_until_budget_exhausted() -> None:
    clock = FakeClock()
    storage = InMemoryStorage(clock=clock)
    _half_open(storage, clock)

    grants = [storage.lease_probe(name=NAME, ttl=TTL).granted for _ in range(PERMITTED + 1)]

    assert grants == [True, True, True, False]


def test__lease_probe__not_half_open__is_denied() -> None:
    clock = FakeClock()
    storage = InMemoryStorage(clock=clock)
    storage.trip_open(name=NAME, ttl=TTL)

    lease = storage.lease_probe(name=NAME, ttl=TTL)

    assert lease.granted is False
    assert lease.state.state is State.OPEN


def test__record_probe__tallies_outcomes_and_flags_final() -> None:
    clock = FakeClock()
    storage = InMemoryStorage(clock=clock)
    _half_open(storage, clock)

    a = storage.record_probe(name=NAME, outcome=Outcome.SUCCESS, ttl=TTL)
    b = storage.record_probe(name=NAME, outcome=Outcome.SLOW_FAILURE, ttl=TTL)
    c = storage.record_probe(name=NAME, outcome=Outcome.FAILURE, ttl=TTL)

    assert a.probes_completed == 1
    assert b.probes_completed == 2
    assert c.probes_completed == PERMITTED
    assert c.probe_failures == 2
    assert c.probe_slows == 1
    assert c.probes_completed >= c.probes_permitted  # caller's "final" signal


def test__close__returns_to_closed_and_clears_probes() -> None:
    clock = FakeClock()
    storage = InMemoryStorage(clock=clock)
    _half_open(storage, clock)
    storage.lease_probe(name=NAME, ttl=TTL)

    state = storage.close(name=NAME, ttl=TTL)

    assert state.state is State.CLOSED
    assert state.probes_permitted == 0
    assert state.probes_remaining == 0
    assert state.probes_completed == 0


def test__record_probe__not_half_open__is_dropped() -> None:
    clock = FakeClock()
    storage = InMemoryStorage(clock=clock)
    storage.trip_open(name=NAME, ttl=TTL)

    state = storage.record_probe(name=NAME, outcome=Outcome.FAILURE, ttl=TTL)

    assert state.state is State.OPEN
    assert state.probes_completed == 0
    assert state.probe_failures == 0


def test__record_probe__absent_key__does_not_create_state() -> None:
    storage = InMemoryStorage(clock=FakeClock())

    state = storage.record_probe(name=NAME, outcome=Outcome.FAILURE, ttl=TTL)

    assert state.state is State.CLOSED
    assert state.probes_completed == 0
    assert storage.read(NAME) is None


def test__close__matching_expected_version__applies() -> None:
    clock = FakeClock()
    storage = InMemoryStorage(clock=clock)
    _half_open(storage, clock)
    current = storage.record_probe(name=NAME, outcome=Outcome.SUCCESS, ttl=TTL)

    state = storage.close(name=NAME, ttl=TTL, expected_version=current.version)

    assert state.state is State.CLOSED
    assert state.version == current.version + 1


def test__close__stale_expected_version__is_fenced_out() -> None:
    clock = FakeClock()
    storage = InMemoryStorage(clock=clock)
    _half_open(storage, clock)
    stale = storage.record_probe(name=NAME, outcome=Outcome.SUCCESS, ttl=TTL)
    reopened = storage.trip_open(name=NAME, ttl=TTL)  # another instance reopened meanwhile

    state = storage.close(name=NAME, ttl=TTL, expected_version=stale.version)

    assert state.state is State.OPEN  # the delayed close must not win
    assert state.version == reopened.version


def test__trip_open__stale_expected_version__is_fenced_out() -> None:
    clock = FakeClock()
    storage = InMemoryStorage(clock=clock)
    _half_open(storage, clock)
    stale = storage.record_probe(name=NAME, outcome=Outcome.FAILURE, ttl=TTL)
    closed = storage.close(name=NAME, ttl=TTL)  # another instance closed meanwhile

    state = storage.trip_open(name=NAME, ttl=TTL, expected_version=stale.version)

    assert state.state is State.CLOSED  # the delayed reopen must not win
    assert state.version == closed.version


def test__trip_open__matching_expected_version__applies() -> None:
    clock = FakeClock()
    storage = InMemoryStorage(clock=clock)
    _half_open(storage, clock)
    current = storage.record_probe(name=NAME, outcome=Outcome.FAILURE, ttl=TTL)

    state = storage.trip_open(name=NAME, ttl=TTL, expected_version=current.version)

    assert state.state is State.OPEN
    assert state.version == current.version + 1


def test__trip_open__from_half_open__reopens_with_fresh_time() -> None:
    clock = FakeClock()
    storage = InMemoryStorage(clock=clock)
    _half_open(storage, clock)

    clock.advance(7.0)
    state = storage.trip_open(name=NAME, ttl=TTL)

    assert state.state is State.OPEN
    assert state.opened_at == WAIT + 7.0
    assert state.probes_remaining == 0


@pytest.mark.asyncio
async def test__async__full_cycle__mirrors_sync_contract() -> None:
    clock = FakeClock()
    storage = AsyncInMemoryStorage(clock=clock)

    assert await storage.read(NAME) is None

    opened = await storage.trip_open(name=NAME, ttl=TTL)
    assert opened.state is State.OPEN

    clock.advance(WAIT)
    half = await storage.begin_half_open_if_elapsed(
        name=NAME, wait_duration=WAIT, permitted=PERMITTED, ttl=TTL
    )
    assert half.state is State.HALF_OPEN

    lease = await storage.lease_probe(name=NAME, ttl=TTL)
    assert lease.granted is True

    tally = await storage.record_probe(name=NAME, outcome=Outcome.SUCCESS, ttl=TTL)
    assert tally.probes_completed == 1

    fenced = await storage.close(name=NAME, ttl=TTL, expected_version=tally.version - 1)
    assert fenced.state is State.HALF_OPEN  # stale version: fenced out

    closed = await storage.close(name=NAME, ttl=TTL, expected_version=tally.version)
    assert closed.state is State.CLOSED

    reopen_fenced = await storage.trip_open(name=NAME, ttl=TTL, expected_version=tally.version)
    assert reopen_fenced.state is State.CLOSED  # stale version: fenced out
