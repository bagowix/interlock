"""Integration tests for ``RedisStorage`` / ``AsyncRedisStorage``.

By default these run against in-process ``fakeredis`` (Lua-capable), so
``uv run pytest`` needs no server. Set ``INTERLOCK_TEST_REDIS_URL`` to exercise
a real server (Redis or Valkey) instead — CI does this, and it is the
authoritative check for atomicity. Tests marked ``requires_real_redis`` (the
concurrency ones, T2.3) run only against a real server, whose single-threaded
execution fakeredis does not faithfully reproduce.

Shared time is the server's ``TIME``: tests that depend on elapse use a tiny
``wait_duration`` and a short real sleep — the sanctioned exception to the
"no sleep" rule, confined to this Redis layer.
"""

import os
import threading
import time
import uuid
from collections.abc import Iterator

import fakeredis
import pytest
import redis.asyncio as aredis

import redis as redis_mod
from interlock import Outcome, State
from interlock.redis import AsyncRedisStorage, RedisStorage

REDIS_URL = os.environ.get('INTERLOCK_TEST_REDIS_URL')
USE_REAL_REDIS = REDIS_URL is not None
TTL = 60.0

requires_real_redis = pytest.mark.skipif(
    not USE_REAL_REDIS,
    reason='atomicity is verified only against a real server (set INTERLOCK_TEST_REDIS_URL)',
)


def _sync_client() -> redis_mod.Redis:
    if not USE_REAL_REDIS:
        return fakeredis.FakeStrictRedis()

    client = redis_mod.Redis.from_url(REDIS_URL, socket_connect_timeout=0.5)
    try:
        client.ping()
    except (redis_mod.ConnectionError, OSError):
        pytest.skip(f'Redis not reachable at {REDIS_URL}')
    return client


def _async_client() -> aredis.Redis:
    if not USE_REAL_REDIS:
        return fakeredis.FakeAsyncRedis()

    return aredis.Redis.from_url(REDIS_URL)


@pytest.fixture
def prefix() -> str:
    return f'interlock:test:{uuid.uuid4().hex}:'


@pytest.fixture(scope='session')
def _shared_client() -> Iterator[redis_mod.Redis]:
    # One client for the whole session: the fakeredis Lua runtime is expensive to
    # spin up, and tests isolate by unique prefix rather than by connection.
    client = _sync_client()
    yield client
    client.close()


@pytest.fixture
def redis_client(_shared_client: redis_mod.Redis, prefix: str) -> Iterator[redis_mod.Redis]:
    yield _shared_client
    keys = list(_shared_client.scan_iter(match=f'{prefix}*'))
    if keys:
        _shared_client.delete(*keys)


@pytest.fixture
def storage(redis_client: redis_mod.Redis, prefix: str) -> RedisStorage:
    return RedisStorage(redis_client, key_prefix=prefix)


def test__read__absent_key__returns_none(storage: RedisStorage) -> None:
    assert storage.read('svc') is None


def test__read__after_write__reflects_state(storage: RedisStorage) -> None:
    storage.trip_open(name='svc', ttl=TTL)

    state = storage.read('svc')

    assert state is not None
    assert state.state is State.OPEN
    assert state.version == 1


def test__trip_open__opens_and_stamps_server_time(storage: RedisStorage) -> None:
    state = storage.trip_open(name='svc', ttl=TTL)

    assert state.state is State.OPEN
    assert state.version == 1
    assert state.opened_at > 0


def test__trip_open__already_open__is_idempotent(storage: RedisStorage) -> None:
    first = storage.trip_open(name='svc', ttl=TTL)
    again = storage.trip_open(name='svc', ttl=TTL)

    assert again.version == first.version == 1
    assert again.opened_at == first.opened_at


def test__begin_half_open__before_elapsed__stays_open(storage: RedisStorage) -> None:
    storage.trip_open(name='svc', ttl=TTL)

    state = storage.begin_half_open_if_elapsed(name='svc', wait_duration=30.0, permitted=3, ttl=TTL)

    assert state.state is State.OPEN
    assert state.probes_remaining == 0


def test__begin_half_open__after_elapsed__seeds_budget(storage: RedisStorage) -> None:
    storage.trip_open(name='svc', ttl=TTL)
    time.sleep(0.2)

    state = storage.begin_half_open_if_elapsed(name='svc', wait_duration=0.1, permitted=3, ttl=TTL)

    assert state.state is State.HALF_OPEN
    assert state.probes_permitted == 3
    assert state.probes_remaining == 3


def test__lease_probe__not_half_open__denied_on_absent_key(storage: RedisStorage) -> None:
    lease = storage.lease_probe(name='svc', ttl=TTL)

    assert lease.granted is False
    assert lease.state.state is State.CLOSED


def test__lease_probe__grants_until_budget_exhausted(storage: RedisStorage) -> None:
    storage.trip_open(name='svc', ttl=TTL)
    storage.begin_half_open_if_elapsed(name='svc', wait_duration=0.0, permitted=3, ttl=TTL)

    grants = [storage.lease_probe(name='svc', ttl=TTL).granted for _ in range(4)]

    assert grants == [True, True, True, False]


def test__record_probe__tallies_and_flags_final(storage: RedisStorage) -> None:
    storage.trip_open(name='svc', ttl=TTL)
    storage.begin_half_open_if_elapsed(name='svc', wait_duration=0.0, permitted=3, ttl=TTL)

    storage.record_probe(name='svc', outcome=Outcome.SUCCESS, ttl=TTL)
    storage.record_probe(name='svc', outcome=Outcome.SLOW_FAILURE, ttl=TTL)
    final = storage.record_probe(name='svc', outcome=Outcome.FAILURE, ttl=TTL)

    assert final.probes_completed == 3
    assert final.probe_failures == 2
    assert final.probe_slows == 1
    assert final.probes_completed >= final.probes_permitted


def test__close__returns_to_closed(storage: RedisStorage) -> None:
    storage.trip_open(name='svc', ttl=TTL)

    state = storage.close(name='svc', ttl=TTL)

    assert state.state is State.CLOSED
    assert state.probes_remaining == 0


def test__trip_open__from_half_open__reopens_fresh(storage: RedisStorage) -> None:
    storage.trip_open(name='svc', ttl=TTL)
    storage.begin_half_open_if_elapsed(name='svc', wait_duration=0.0, permitted=3, ttl=TTL)

    state = storage.trip_open(name='svc', ttl=TTL)

    assert state.state is State.OPEN
    assert state.probes_remaining == 0


def test__record_probe__not_half_open__is_dropped(storage: RedisStorage) -> None:
    storage.trip_open(name='svc', ttl=TTL)

    state = storage.record_probe(name='svc', outcome=Outcome.FAILURE, ttl=TTL)

    assert state.state is State.OPEN
    assert state.probes_completed == 0
    assert state.probe_failures == 0


def test__record_probe__absent_key__does_not_create_state(storage: RedisStorage) -> None:
    state = storage.record_probe(name='svc', outcome=Outcome.FAILURE, ttl=TTL)

    assert state.state is State.CLOSED
    assert state.probes_completed == 0
    assert storage.read('svc') is None


def test__close__matching_expected_version__applies(storage: RedisStorage) -> None:
    storage.trip_open(name='svc', ttl=TTL)
    storage.begin_half_open_if_elapsed(name='svc', wait_duration=0.0, permitted=1, ttl=TTL)
    current = storage.record_probe(name='svc', outcome=Outcome.SUCCESS, ttl=TTL)

    state = storage.close(name='svc', ttl=TTL, expected_version=current.version)

    assert state.state is State.CLOSED
    assert state.version == current.version + 1


def test__close__stale_expected_version__is_fenced_out(storage: RedisStorage) -> None:
    storage.trip_open(name='svc', ttl=TTL)
    storage.begin_half_open_if_elapsed(name='svc', wait_duration=0.0, permitted=1, ttl=TTL)
    stale = storage.record_probe(name='svc', outcome=Outcome.SUCCESS, ttl=TTL)
    reopened = storage.trip_open(name='svc', ttl=TTL)  # another instance reopened meanwhile

    state = storage.close(name='svc', ttl=TTL, expected_version=stale.version)

    assert state.state is State.OPEN  # the delayed close must not win
    assert state.version == reopened.version


def test__trip_open__stale_expected_version__is_fenced_out(storage: RedisStorage) -> None:
    storage.trip_open(name='svc', ttl=TTL)
    storage.begin_half_open_if_elapsed(name='svc', wait_duration=0.0, permitted=1, ttl=TTL)
    stale = storage.record_probe(name='svc', outcome=Outcome.FAILURE, ttl=TTL)
    closed = storage.close(name='svc', ttl=TTL)  # another instance closed meanwhile

    state = storage.trip_open(name='svc', ttl=TTL, expected_version=stale.version)

    assert state.state is State.CLOSED  # the delayed reopen must not win
    assert state.version == closed.version


def test__ttl__non_positive__rejected(storage: RedisStorage) -> None:
    with pytest.raises(ValueError, match='ttl'):
        storage.trip_open(name='svc', ttl=0.0)


def test__read__ignores_unknown_hash_fields(
    storage: RedisStorage, redis_client: redis_mod.Redis, prefix: str
) -> None:
    # Forward-compat: a newer writer may add fields this version does not know.
    storage.trip_open(name='svc', ttl=TTL)
    redis_client.hset(f'{prefix}svc', 'future_field', 'whatever')

    state = storage.read('svc')

    assert state is not None
    assert state.state is State.OPEN


@requires_real_redis
def test__trip_open__concurrent__single_opener(storage: RedisStorage) -> None:
    threads_count = 20
    barrier = threading.Barrier(threads_count)

    def worker() -> None:
        barrier.wait()
        storage.trip_open(name='svc', ttl=TTL)

    threads = [threading.Thread(target=worker) for _ in range(threads_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    state = storage.read('svc')
    assert state is not None
    assert state.state is State.OPEN
    assert state.version == 1  # only the first eval transitioned; the rest were no-ops


@requires_real_redis
def test__lease_probe__concurrent__grants_exactly_budget(storage: RedisStorage) -> None:
    budget = 5
    threads_count = 25
    storage.trip_open(name='svc', ttl=TTL)
    storage.begin_half_open_if_elapsed(name='svc', wait_duration=0.0, permitted=budget, ttl=TTL)

    granted: list[bool] = []
    lock = threading.Lock()
    barrier = threading.Barrier(threads_count)

    def worker() -> None:
        barrier.wait()
        result = storage.lease_probe(name='svc', ttl=TTL).granted
        with lock:
            granted.append(result)

    threads = [threading.Thread(target=worker) for _ in range(threads_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sum(granted) == budget


def test__ttl__set_on_write(
    storage: RedisStorage, redis_client: redis_mod.Redis, prefix: str
) -> None:
    storage.trip_open(name='svc', ttl=TTL)

    pttl = redis_client.pttl(f'{prefix}svc')

    assert 0 < pttl <= TTL * 1000


def test__ttl__key_expires(storage: RedisStorage) -> None:
    storage.trip_open(name='svc', ttl=0.2)
    time.sleep(0.3)

    assert storage.read('svc') is None


def test__ttl__lease_probe_refreshes_expiry(
    storage: RedisStorage, redis_client: redis_mod.Redis, prefix: str
) -> None:
    storage.trip_open(name='svc', ttl=TTL)
    storage.begin_half_open_if_elapsed(name='svc', wait_duration=0.0, permitted=3, ttl=TTL)

    storage.lease_probe(name='svc', ttl=TTL * 2)

    assert redis_client.pttl(f'{prefix}svc') > TTL * 1000


@pytest.mark.asyncio
@pytest.mark.usefixtures('redis_client')
async def test__async__full_cycle(prefix: str) -> None:
    client = _async_client()
    storage = AsyncRedisStorage(client, key_prefix=prefix)
    try:
        assert await storage.read('svc') is None

        opened = await storage.trip_open(name='svc', ttl=TTL)
        assert opened.state is State.OPEN

        half = await storage.begin_half_open_if_elapsed(
            name='svc', wait_duration=0.0, permitted=2, ttl=TTL
        )
        assert half.state is State.HALF_OPEN

        lease = await storage.lease_probe(name='svc', ttl=TTL)
        assert lease.granted is True

        tally = await storage.record_probe(name='svc', outcome=Outcome.FAILURE, ttl=TTL)
        assert tally.probes_completed == 1
        assert tally.probe_failures == 1

        fenced = await storage.close(name='svc', ttl=TTL, expected_version=tally.version - 1)
        assert fenced.state is State.HALF_OPEN  # stale version: fenced out

        closed = await storage.close(name='svc', ttl=TTL, expected_version=tally.version)
        assert closed.state is State.CLOSED

        reopen_fenced = await storage.trip_open(name='svc', ttl=TTL, expected_version=tally.version)
        assert reopen_fenced.state is State.CLOSED  # stale version: fenced out
    finally:
        await client.aclose()
