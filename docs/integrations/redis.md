# Redis integration (shared state)

The `interlock-cb[redis]` extra coordinates breaker state across processes and
machines through Redis: when one instance trips, every instance backs off, and
recovery probes are budgeted globally instead of per process.

```bash
uv add 'interlock-cb[redis]'
```

## When to share state — and when not to

Per-instance state is the default for a reason. A local breaker reacts only to
what *this* process observes, cannot be affected by another instance's problem,
and keeps working when Redis does not.

Reach for shared state when all of these hold:

- Many instances call the **same downstream**, and its failure affects all of
  them equally (a shared database, a rate-limited third-party API).
- You want **coordinated back-off**: once the downstream is declared unhealthy,
  no instance should keep hammering it just because its own window has not
  filled yet.
- You want **bounded recovery probing**: N instances should send at most
  `permitted_calls_in_half_open` probes *in total*, not each.

Stay local when instances see genuinely different views of the dependency
(per-AZ endpoints, canary deployments), or when one instance's network problems
must not silence the whole fleet. A shared OPEN gates traffic *everywhere* —
that is the point, and the risk. It is a trade-off you opt into, not a default.

## Usage

Pass a storage to the breaker (or to a `Registry`, which hands it to every
breaker it creates — each coordinates under its own name):

```python
import redis
from interlock import CircuitBreaker, Registry
from interlock.redis import RedisStorage

storage = RedisStorage(redis.Redis(host='redis.internal'))
breaker = CircuitBreaker(name='payments', storage=storage)

registry = Registry(storage=storage)  # or share one storage across many breakers
```

Async services use the async client and storage:

```python
import redis.asyncio
from interlock import CircuitBreaker
from interlock.redis import AsyncRedisStorage

storage = AsyncRedisStorage(redis.asyncio.Redis(host='redis.internal'))
breaker = CircuitBreaker(name='payments', storage=storage)
```

A coordinated breaker matches its storage's runtime: a `RedisStorage` serves
only the sync API (`with`, sync `call`), an `AsyncRedisStorage` only the async
one (`async with`, async `call`); mixing the styles raises `InterlockError`
with a clear message. A breaker *without* a storage stays fully dual.

## How coordination works

The local state machine keeps owning the sliding window and trip detection;
Redis owns the shared OPEN/HALF_OPEN state and the global probe budget. All
state for one breaker lives in a single hash (`interlock:cb:<name>` by
default), and every transition runs as a Lua script, so racing instances stay
consistent.

The protected path stays fast:

- **CLOSED / OPEN admission** reads a locally cached view of the shared state —
  zero inline Redis calls. A background poller refreshes the cache every
  `poll_interval` seconds, so a trip on one instance reaches the others within
  roughly one interval.
- **HALF_OPEN admission** is the single inline Redis operation: an atomic probe
  lease that decrements the shared budget, bounding probes across the fleet.
- **Writes** (propagating a local trip, tallying probe outcomes, the final
  close-or-reopen decision) are fire-and-forget on a background worker; they
  never block a protected call.

Time comparisons ("has `wait_duration_in_open` elapsed?") use the *Redis
server's* clock, since instance clocks are not comparable. After the last probe
of a round, the deciding instance applies the same thresholds as the local
state machine and writes the transition guarded by a version check, so a
delayed decision can never overwrite a newer state.

## Degradation: Redis down ≠ breaker down

A storage error never reaches your calls. On the first failure the breaker
switches to its local state and keeps protecting the process on its own window;
pending shared writes are dropped, and Redis is left alone for `retry_backoff`
seconds before the poller tries again. On the first successful operation the
shared view becomes authoritative again — including adopting a shared OPEN that
happened while this instance was cut off.

Both edges are observable through the listener:

```python
class StorageWatch:
    def on_storage_degraded(self, *, name: str, error: BaseException) -> None:
        ...  # alert: running on local state

    def on_storage_recovered(self, *, name: str) -> None:
        ...  # back to coordinated state
```

`LoggingEventListener` logs degradation at `WARNING` and recovery at `INFO`;
`OTelEventListener` counts both on `interlock.storage.events`. Listeners
written before these hooks existed keep working — the engine calls them only if
present.

## Tuning

All knobs live on the storage constructor; the core `Config` stays
storage-agnostic:

```python
RedisStorage(
    client,
    key_prefix='interlock:cb:',  # hash key namespace
    state_ttl=300.0,             # key lifetime (s); refreshed on every write
    poll_interval=1.0,           # cache refresh cadence (s)
    retry_backoff=5.0,           # local-only time after a storage failure (s)
)
```

- **`state_ttl`** keeps abandoned state from lingering: if every instance
  disappears, the key expires and the breaker starts CLOSED. Keep it well above
  `wait_duration_in_open`.
- **`poll_interval`** is the propagation latency of a coordinated trip. Each
  breaker costs about one Redis read per interval.
- **`retry_backoff`** bounds how often a degraded breaker re-tests Redis.

## Compatibility

`RedisStorage` speaks plain commands and `EVAL` — no server-specific features —
so it works against Redis, [Valkey](https://valkey.io), or any RESP-compatible
server. The scripts call `TIME` before writing, which requires effect-based
script replication: **Redis 5.0 or newer**, or any Valkey release. (The
`redis>=5.0.0` dependency pin is the *client* library's version, not the
server's.)
