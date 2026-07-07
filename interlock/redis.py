"""Redis-backed shared state for coordinated, distributed breakers.

Optional extra ``interlock-cb[redis]`` — not re-exported from the package root;
the core stays zero-dependency. ``RedisStorage`` (sync) and ``AsyncRedisStorage``
(async) implement the ``Storage`` / ``AsyncStorage`` contracts over redis-py.

All state for one breaker lives in a single Redis hash keyed
``<prefix><name>``. Every transition runs as a Lua script so racing instances
stay consistent: the script is the atomic critical section. Lua carries
mechanism only — atomic CAS, server-time elapsed checks (``TIME``, since instance
clocks are not comparable), probe leasing and counters, and TTL refresh.
Threshold *policy* stays in the core state machine, which calls ``trip_open`` /
``close`` once it has decided.

Wire-compatible with Redis, Valkey, and any RESP server: this speaks plain
commands and ``EVAL``, with no server-specific features. The scripts call
``TIME`` before writing, so the server must replicate scripts by effects —
Redis >= 5.0 or any Valkey (the ``redis>=5.0.0`` dependency pin is the
*client* library version, not the server's).
"""

from typing import Any

import redis.asyncio

import redis
from interlock.outcome import Outcome
from interlock.shared import ProbeLease, SharedState
from interlock.state import State

__all__ = ('AsyncRedisStorage', 'RedisStorage')

_DEFAULT_PREFIX = 'interlock:cb:'

# Hash fields, in the fixed order every script returns them.
_FIELDS = (
    "'state','opened_at','version',"
    "'probes_permitted','probes_remaining','probes_completed',"
    "'probe_failures','probe_slows'"
)
_RETURN_STATE = f"return redis.call('HMGET', KEYS[1], {_FIELDS})"

# opened_at is stored as integer microseconds (epoch, from the server's TIME),
# kept under 2^53 so it round-trips exactly through Lua's double numbers.
_NOW_US = (
    "(function() local t = redis.call('TIME') "
    'return tonumber(t[1]) * 1000000 + tonumber(t[2]) end)()'
)

# expected_version < 0 means unfenced; a fenced-out call performs no write at
# all (not even a TTL refresh — a stale actor must not extend the key's life).
_TRIP_OPEN = f"""
local key = KEYS[1]
local ttl_ms = tonumber(ARGV[1])
local expected = tonumber(ARGV[2])
local version = tonumber(redis.call('HGET', key, 'version') or '0')
if expected < 0 or version == expected then
  if redis.call('HGET', key, 'state') ~= 'open' then
    redis.call('HSET', key,
      'state', 'open', 'opened_at', {_NOW_US}, 'version', version + 1,
      'probes_permitted', 0, 'probes_remaining', 0, 'probes_completed', 0,
      'probe_failures', 0, 'probe_slows', 0, 'v', 1)
  end
  redis.call('PEXPIRE', key, ttl_ms)
end
{_RETURN_STATE}
"""

_BEGIN_HALF_OPEN = f"""
local key = KEYS[1]
local wait_us = tonumber(ARGV[1]) * 1000000
local permitted = tonumber(ARGV[2])
local ttl_ms = tonumber(ARGV[3])
if redis.call('HGET', key, 'state') == 'open' then
  local opened_at = tonumber(redis.call('HGET', key, 'opened_at') or '0')
  if {_NOW_US} - opened_at >= wait_us then
    local version = tonumber(redis.call('HGET', key, 'version') or '0') + 1
    redis.call('HSET', key,
      'state', 'half_open', 'version', version,
      'probes_permitted', permitted, 'probes_remaining', permitted,
      'probes_completed', 0, 'probe_failures', 0, 'probe_slows', 0)
  end
end
redis.call('PEXPIRE', key, ttl_ms)
{_RETURN_STATE}
"""

_LEASE_PROBE = f"""
local key = KEYS[1]
local ttl_ms = tonumber(ARGV[1])
local granted = 0
if redis.call('HGET', key, 'state') == 'half_open' then
  if tonumber(redis.call('HGET', key, 'probes_remaining') or '0') > 0 then
    redis.call('HINCRBY', key, 'probes_remaining', -1)
    redis.call('HINCRBY', key, 'version', 1)
    granted = 1
  end
end
redis.call('PEXPIRE', key, ttl_ms)
local r = redis.call('HMGET', KEYS[1], {_FIELDS})
table.insert(r, 1, granted)
return r
"""

# Tallies only while HALF_OPEN: a probe outcome arriving after the state moved
# on (another instance tripped or closed) must not pollute the new accounting.
_RECORD_PROBE = f"""
local key = KEYS[1]
local is_failure = tonumber(ARGV[1])
local is_slow = tonumber(ARGV[2])
local ttl_ms = tonumber(ARGV[3])
if redis.call('HGET', key, 'state') == 'half_open' then
  redis.call('HINCRBY', key, 'probes_completed', 1)
  redis.call('HINCRBY', key, 'version', 1)
  if is_failure == 1 then redis.call('HINCRBY', key, 'probe_failures', 1) end
  if is_slow == 1 then redis.call('HINCRBY', key, 'probe_slows', 1) end
  redis.call('PEXPIRE', key, ttl_ms)
end
{_RETURN_STATE}
"""

_CLOSE = f"""
local key = KEYS[1]
local ttl_ms = tonumber(ARGV[1])
local expected = tonumber(ARGV[2])
local version = tonumber(redis.call('HGET', key, 'version') or '0')
if expected < 0 or version == expected then
  redis.call('HSET', key,
    'state', 'closed', 'opened_at', 0, 'version', version + 1,
    'probes_permitted', 0, 'probes_remaining', 0, 'probes_completed', 0,
    'probe_failures', 0, 'probe_slows', 0, 'v', 1)
  redis.call('PEXPIRE', key, ttl_ms)
end
{_RETURN_STATE}
"""


def _s(value: object) -> str:
    """Decode a Redis reply scalar (bytes or str) to ``str``."""
    return value.decode() if isinstance(value, bytes) else str(value)


def _ms(ttl: float) -> int:
    """Convert a TTL in seconds to whole milliseconds for ``PEXPIRE``.

    Rejects TTLs under one millisecond: ``PEXPIRE key 0`` would delete the key
    outright instead of expiring it.
    """
    ms = int(ttl * 1000)
    if ms <= 0:
        msg = f'ttl must be at least 0.001 seconds, got {ttl!r}'
        raise ValueError(msg)
    return ms


def _fence(expected_version: int | None) -> int:
    """Encode ``expected_version`` for Lua: ``-1`` disables the fence."""
    return -1 if expected_version is None else expected_version


def _shared_from_values(values: list[Any]) -> SharedState:
    """Build a ``SharedState`` from an ``HMGET`` reply in ``_FIELDS`` order."""
    if not values or values[0] is None:
        return SharedState.closed()

    return SharedState(
        state=State(_s(values[0])),
        opened_at=int(_s(values[1])) / 1_000_000,
        version=int(_s(values[2])),
        probes_permitted=int(_s(values[3])),
        probes_remaining=int(_s(values[4])),
        probes_completed=int(_s(values[5])),
        probe_failures=int(_s(values[6])),
        probe_slows=int(_s(values[7])),
    )


def _shared_from_map(mapping: dict[Any, Any]) -> SharedState | None:
    """Build a ``SharedState`` from an ``HGETALL`` reply, or ``None`` if absent."""
    if not mapping:
        return None

    fields = {_s(k): v for k, v in mapping.items()}
    order = (
        'state',
        'opened_at',
        'version',
        'probes_permitted',
        'probes_remaining',
        'probes_completed',
        'probe_failures',
        'probe_slows',
    )
    return _shared_from_values([fields.get(name) for name in order])


def _positive(name: str, value: float) -> float:
    if value <= 0:
        msg = f'{name} must be positive, got {value!r}'
        raise ValueError(msg)
    return value


class RedisStorage:
    """Synchronous ``Storage`` backed by a ``redis.Redis`` client.

    The three keyword floats are the coordination tuning knobs the engine reads
    off the storage object: how long state keys live without a refresh
    (``state_ttl``), how often the cached shared view is refreshed
    (``poll_interval``), and how long the breaker runs purely locally after a
    storage failure before retrying (``retry_backoff``).

    Args:
        client: A connected ``redis.Redis`` instance.
        key_prefix: Namespace prepended to every breaker name.
        state_ttl: Key lifetime in seconds; refreshed on every write.
        poll_interval: Seconds between background view refreshes.
        retry_backoff: Seconds to stay degraded after a storage failure.
    """

    def __init__(
        self,
        client: redis.Redis,
        *,
        key_prefix: str = _DEFAULT_PREFIX,
        state_ttl: float = 300.0,
        poll_interval: float = 1.0,
        retry_backoff: float = 5.0,
    ) -> None:
        self._client = client
        self._prefix = key_prefix
        self.state_ttl = _positive('state_ttl', state_ttl)
        self.poll_interval = _positive('poll_interval', poll_interval)
        self.retry_backoff = _positive('retry_backoff', retry_backoff)

    def _key(self, name: str) -> str:
        return f'{self._prefix}{name}'

    def read(self, name: str) -> SharedState | None:
        """Return the current shared view, or ``None`` if no key exists."""
        return _shared_from_map(self._client.hgetall(self._key(name)))

    def trip_open(
        self, *, name: str, ttl: float, expected_version: int | None = None
    ) -> SharedState:
        """Atomically transition to ``OPEN`` (idempotent while already open).

        With ``expected_version``, fenced: a no-op unless the backend still
        holds that version.
        """
        values = self._client.eval(
            _TRIP_OPEN, 1, self._key(name), _ms(ttl), _fence(expected_version)
        )
        return _shared_from_values(values)

    def begin_half_open_if_elapsed(
        self, *, name: str, wait_duration: float, permitted: int, ttl: float
    ) -> SharedState:
        """Move ``OPEN`` → ``HALF_OPEN`` once the wait has elapsed (server time)."""
        values = self._client.eval(
            _BEGIN_HALF_OPEN, 1, self._key(name), wait_duration, permitted, _ms(ttl)
        )
        return _shared_from_values(values)

    def lease_probe(self, *, name: str, ttl: float) -> ProbeLease:
        """Atomically claim one global probe slot."""
        values = self._client.eval(_LEASE_PROBE, 1, self._key(name), _ms(ttl))
        return ProbeLease(granted=bool(int(_s(values[0]))), state=_shared_from_values(values[1:]))

    def record_probe(self, *, name: str, outcome: Outcome, ttl: float) -> SharedState:
        """Tally one completed probe's outcome (only while ``HALF_OPEN``)."""
        values = self._client.eval(
            _RECORD_PROBE,
            1,
            self._key(name),
            int(outcome.is_failure),
            int(outcome.is_slow),
            _ms(ttl),
        )
        return _shared_from_values(values)

    def close(self, *, name: str, ttl: float, expected_version: int | None = None) -> SharedState:
        """Atomically transition to ``CLOSED`` and clear probe accounting.

        With ``expected_version``, fenced: a delayed "probes passed" decision
        cannot close a breaker that has since re-opened.
        """
        values = self._client.eval(_CLOSE, 1, self._key(name), _ms(ttl), _fence(expected_version))
        return _shared_from_values(values)


class AsyncRedisStorage:
    """Asynchronous ``AsyncStorage`` backed by a ``redis.asyncio.Redis`` client.

    See ``RedisStorage`` for the coordination tuning knobs.

    Args:
        client: A connected ``redis.asyncio.Redis`` instance.
        key_prefix: Namespace prepended to every breaker name.
        state_ttl: Key lifetime in seconds; refreshed on every write.
        poll_interval: Seconds between background view refreshes.
        retry_backoff: Seconds to stay degraded after a storage failure.
    """

    def __init__(
        self,
        client: redis.asyncio.Redis,
        *,
        key_prefix: str = _DEFAULT_PREFIX,
        state_ttl: float = 300.0,
        poll_interval: float = 1.0,
        retry_backoff: float = 5.0,
    ) -> None:
        self._client = client
        self._prefix = key_prefix
        self.state_ttl = _positive('state_ttl', state_ttl)
        self.poll_interval = _positive('poll_interval', poll_interval)
        self.retry_backoff = _positive('retry_backoff', retry_backoff)

    def _key(self, name: str) -> str:
        return f'{self._prefix}{name}'

    async def read(self, name: str) -> SharedState | None:
        """Return the current shared view, or ``None`` if no key exists."""
        return _shared_from_map(await self._client.hgetall(self._key(name)))

    async def trip_open(
        self, *, name: str, ttl: float, expected_version: int | None = None
    ) -> SharedState:
        """Atomically transition to ``OPEN`` (idempotent while already open).

        With ``expected_version``, fenced: a no-op unless the backend still
        holds that version.
        """
        values = await self._client.eval(
            _TRIP_OPEN, 1, self._key(name), _ms(ttl), _fence(expected_version)
        )
        return _shared_from_values(values)

    async def begin_half_open_if_elapsed(
        self, *, name: str, wait_duration: float, permitted: int, ttl: float
    ) -> SharedState:
        """Move ``OPEN`` → ``HALF_OPEN`` once the wait has elapsed (server time)."""
        values = await self._client.eval(
            _BEGIN_HALF_OPEN, 1, self._key(name), wait_duration, permitted, _ms(ttl)
        )
        return _shared_from_values(values)

    async def lease_probe(self, *, name: str, ttl: float) -> ProbeLease:
        """Atomically claim one global probe slot."""
        values = await self._client.eval(_LEASE_PROBE, 1, self._key(name), _ms(ttl))
        return ProbeLease(granted=bool(int(_s(values[0]))), state=_shared_from_values(values[1:]))

    async def record_probe(self, *, name: str, outcome: Outcome, ttl: float) -> SharedState:
        """Tally one completed probe's outcome (only while ``HALF_OPEN``)."""
        values = await self._client.eval(
            _RECORD_PROBE,
            1,
            self._key(name),
            int(outcome.is_failure),
            int(outcome.is_slow),
            _ms(ttl),
        )
        return _shared_from_values(values)

    async def close(
        self, *, name: str, ttl: float, expected_version: int | None = None
    ) -> SharedState:
        """Atomically transition to ``CLOSED`` and clear probe accounting.

        With ``expected_version``, fenced: a delayed "probes passed" decision
        cannot close a breaker that has since re-opened.
        """
        values = await self._client.eval(
            _CLOSE, 1, self._key(name), _ms(ttl), _fence(expected_version)
        )
        return _shared_from_values(values)
