# Redis (shared state)

The `interlock-cb[redis]` extra coordinates breaker state across processes and
machines through Redis: when one instance trips, every instance backs off, and
recovery probes are budgeted globally instead of per process.

=== "uv"

    ```bash
    uv add 'interlock-cb[redis]'
    ```

=== "pip"

    ```bash
    pip install 'interlock-cb[redis]'
    ```

=== "poetry"

    ```bash
    poetry add 'interlock-cb[redis]'
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
from interlock.integrations.redis import RedisStorage

storage = RedisStorage(redis.Redis(host='redis.internal'))
breaker = CircuitBreaker(name='payments', storage=storage)

registry = Registry(storage=storage)  # or share one storage across many breakers
```

Async services use the async client and storage:

```python
import redis.asyncio
from interlock import CircuitBreaker
from interlock.integrations.redis import AsyncRedisStorage

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

## Coordinated mode contract

`RedisStorage` implements `Storage` / `AsyncStorage`; a third-party backend can
too. Four behaviours are part of that contract, not implementation detail —
skipping any of them breaks the guarantees the coordinator relies on:

1. **Fencing is mandatory.** `trip_open` and `close` accept `expected_version`
   and must apply the write only when the backend's current version matches
   it, otherwise no-op and return the current state. The coordinator relies on
   this for the probe-round decision: a delayed instance computing "probes
   passed" off a stale view must lose to a state another instance already
   wrote, never overwrite it.
2. **A leaked probe slot is bounded only by `state_ttl`.** `lease_probe`
   decrements a shared budget and has no corresponding un-lease operation. A
   `BaseException` — cancellation, process kill — between `lease_probe` and
   `record_probe` never returns that slot; only the key's TTL expiry does.
   With several instances interrupted mid-probe, `HALF_OPEN` can stall for up
   to `state_ttl`. Size it with this in mind — it is not just an
   abandoned-key cleanup knob.
3. **`record_probe` tallies only while `HALF_OPEN`.** Every coordinated write
   is best-effort *and* bounded: dropped while degraded, dropped when the
   write queue is full, superseded by a later decision, reconciled by the next
   poll rather than retried.
4. **Teardown is explicit, not automatic.** `close()` / `aclose()` stop the
   lane deterministically — see [Shutdown](#shutdown) below. A coordinated
   breaker left to be garbage-collected can outlive its owning scope instead.

Also: a breaker's `name` becomes a storage key (`interlock:cb:<name>` for
`RedisStorage`). Never build one from untrusted input — an attacker-controlled
name can collide with, and corrupt, unrelated breaker state.

### Checklist for a custom `Storage` implementation

- [ ] `trip_open` / `close` honor `expected_version`: apply only on a match,
      otherwise no-op and return the current state.
- [ ] Every operation is atomic against concurrent callers (a Lua script, a
      locked transaction, ...) — no read-modify-write races.
- [ ] `ttl` is refreshed on every write so an abandoned key self-expires.
- [ ] `lease_probe` grants only while `HALF_OPEN` and budget remains.
- [ ] `record_probe` tallies only while `HALF_OPEN`; a late or out-of-round
      outcome is dropped, not applied.
- [ ] No method raises into the protected path — the engine treats any
      exception as a signal to degrade to local state, so a backend that
      raises for a routine outcome (e.g. a fencing miss) breaks the contract;
      that case must return the current state instead.
- [ ] Time comparisons (has `wait_duration_in_open` elapsed?) use the
      backend's own clock, not the caller's — instance clocks are not
      comparable across a fleet.

## Manual controls

Manual controls are local to one process and take precedence over Redis while
they are active. `force_open()` rejects every local call; `disable()` and
`metrics_only()` admit local calls without consuming a Redis HALF_OPEN probe.
`reset()` clears the local override and local metrics, but does not reset Redis
for the fleet: the breaker immediately resumes its cached shared `OPEN` or
`HALF_OPEN` state.

## Degradation: Redis down ≠ breaker down

A storage error never reaches your calls. On the first failure the breaker
switches to its local state and keeps protecting the process on its own window;
pending shared writes are dropped, and Redis is left alone before the poller
tries again. That delay starts at `retry_backoff` seconds and, by default,
stays there for as long as the outage lasts — the same fixed cadence every
release before this one used. Set `retry_backoff_multiplier` above `1.0` to
grow it geometrically with each further consecutive failure (capped at
`retry_backoff_max`), plus `retry_jitter` to spread out the retries of a fleet
of instances recovering from the same outage instead of having them all probe
Redis in the same instant. On the first successful operation the shared view
becomes authoritative again — including adopting a shared OPEN that happened
while this instance was cut off — and the failure count resets, so the next
outage starts back at `retry_backoff`.

Both edges are observable through the listener:

```python
class StorageWatch:
    def on_storage_degraded(
        self, *, name: str, error: BaseException
    ) -> None: ...  # alert: running on local state

    def on_storage_recovered(self, *, name: str) -> None: ...  # back to coordinated state
```

`LoggingEventListener` logs degradation at `WARNING` and recovery at `INFO`;
`OTelEventListener` counts both on `interlock.storage.events`. Listeners
written before these hooks existed keep working — the engine calls them only if
present.

## Backpressure: the write queue is bounded

Coordinated writes are fire-and-forget: a local trip and each probe outcome are
queued for the background lane instead of being written on the protected path.
That queue holds at most `write_queue_size` writes (128 by default).

It is not a per-call queue — a healthy lane keeps it near empty, because writes
happen per *transition*, not per call, and probes are capped by
`permitted_calls_in_half_open`. The bound only matters when the lane stops
draining at all: a Redis client blocking without a timeout, or an async lane
whose event loop is gone. Without it, that lane would grow the queue for as
long as the process lives.

When the queue is full the *arriving* write is dropped — never blocked, never
raised into the call that produced it — and reported:

```python
class StorageWatch:
    def on_storage_write_dropped(self, *, name: str) -> None: ...  # shared state falling behind
```

Nothing is retried: the shared state is reconciled by the next successful poll
and, failing that, by `state_ttl` expiring the key. Locally the breaker keeps
protecting the process on its own window exactly as it does while degraded.

The hook is the signal that a lane is wedged — treat a non-zero rate as an
alert, not a tuning hint. `LoggingEventListener` logs it at `WARNING` and
`OTelEventListener` counts it on `interlock.storage.events`.

## Shutdown

A coordinated breaker owns a background lane — a daemon thread for a sync
storage, an asyncio task for an async one. It polls the shared view and drains
fire-and-forget writes. Left alone, it ends only when the breaker is garbage
collected, which is why an async lane can outlive `asyncio.run()` and log
"task was destroyed but it is pending".

`close()` (or `aclose()` for an async storage) ends it deterministically:

```python
breaker = CircuitBreaker(name='payments', storage=storage)
try:
    ...  # serve traffic
finally:
    breaker.close()  # drains queued writes, stops the lane, joins it
```

`Registry` does the whole set at once:

```python
registry = Registry(storage=storage)
...
registry.close_all()  # or: await registry.aclose_all()
```

What it guarantees:

- **Queued writes are drained first.** Ops already on the queue run in order
  before the lane exits, so a trip recorded just before shutdown still reaches
  Redis. Writes that the degraded gate would drop are still dropped.
- **No waiting out `poll_interval`.** A parked lane is woken immediately.
- **The `auto_transition` timer is cancelled**, and no new one is armed.
- **It is idempotent** and safe to call from any thread.

Two consequences worth knowing:

- **Shutdown is terminal.** The lane never restarts. Afterwards the breaker
  keeps protecting calls on its local state, and shared writes are dropped —
  the same behaviour as a degraded storage. `Registry.get()` keeps returning the
  closed instance rather than silently starting a fresh lane.
- **The cached shared view is dropped.** Nothing refreshes it once the lane is
  gone, so keeping it would pin the breaker in whatever a peer last published —
  a shared `OPEN` would never expire. The fallback to local state is reported
  through `on_state_change` like any other.

`close()` is teardown, not a state change: it does **not** close the circuit.
That is `reset()`.

## Tuning

All knobs live on the storage constructor; the core `Config` stays
storage-agnostic:

```python
RedisStorage(
    client,
    key_prefix='interlock:cb:',  # hash key namespace
    state_ttl=300.0,  # key lifetime (s); refreshed on every write
    poll_interval=1.0,  # cache refresh cadence (s)
    retry_backoff=5.0,  # local-only time after a storage failure (s)
    retry_backoff_multiplier=1.0,  # growth per consecutive failure; 1.0 = fixed delay
    retry_backoff_max=None,  # cap on the delay (s), or None for no cap
    retry_jitter=0.0,  # proportional random spread added to the (capped) delay
    write_queue_size=128,  # max pending coordinated writes; further ones are dropped
)
```

- **`state_ttl`** keeps abandoned state from lingering: if every instance
  disappears, the key expires and the breaker starts CLOSED. Keep it well above
  `wait_duration_in_open`.
- **`poll_interval`** is the propagation latency of a coordinated trip. Each
  breaker costs about one Redis read per interval.
- **`retry_backoff`** is the delay before the first retry after a storage
  failure, and the fixed delay for every retry after that while
  `retry_backoff_multiplier` stays at its default of `1.0`.
- **`retry_backoff_multiplier`** grows the delay geometrically with each
  further *consecutive* failure (`retry_backoff * retry_backoff_multiplier **
  attempts`); a recovery resets the attempt count. Must be `>= 1.0`.
- **`retry_backoff_max`** caps the grown delay in seconds. `None` (the
  default) leaves it uncapped.
- **`retry_jitter`** adds up to this fraction of the (capped) delay as random
  spread, so instances that degraded at the same moment do not all retry
  Redis at the same moment too. Deterministic given the same clock reading,
  attempt number and breaker name — it does not depend on process-global
  random state.
- **`write_queue_size`** bounds the pending coordinated writes; see
  [Backpressure](#backpressure-the-write-queue-is-bounded). The default of 128
  is far above what a draining lane ever holds, so raising it does not buy
  throughput — it only lets a wedged lane hold more memory before dropping.

## Compatibility

`RedisStorage` speaks plain commands and `EVAL` — no server-specific features —
so it works against Redis, [Valkey](https://valkey.io), or any RESP-compatible
server. The scripts call `TIME` before writing, which requires effect-based
script replication: **Redis 5.0 or newer**, or any Valkey release. (The
`redis>=5.0.0` dependency pin is the *client* library's version, not the
server's.)
