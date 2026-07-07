# API reference

Everything below is importable from the top-level `interlock` package, except
the integration adapters, which live in their own modules to keep the core
dependency-free.

## `CircuitBreaker`

```python
CircuitBreaker(*, name, config=None, clock=None, classifier=None, listener=None, storage=None)
```

A named breaker for sync and async callables.

- **Use as** a decorator (`@breaker`), a sync/async context manager
  (`with` / `async with`), or `breaker.call(fn, *args, **kwargs)`.
- **Properties:** `name: str`, `state: State`.
- **`snapshot() -> WindowSnapshot`** — current window aggregates.
- **Manual control:** `reset()`, `force_open()`, `disable()`, `metrics_only()`.
- **`storage`** — optional shared backend (`Storage` or `AsyncStorage`) for
  coordinated state across instances; see the
  [Redis integration](integrations/redis.md). A coordinated breaker matches its
  storage's runtime (sync storage → sync API, async storage → async API);
  without a storage the breaker stays fully dual.

## `Config`

Frozen dataclass of thresholds, window and timing; validated on construction.
See [Configuration](guides/configuration.md) for every field. Raises
`ValueError` on invalid input.

## `Registry`

```python
Registry(*, config=None, clock=None, classifier=None, listener=None, storage=None)
registry.get(name, *, config=None) -> CircuitBreaker
```

Creates and caches named breakers. The same name always returns the same
instance; the per-call `config` override applies only at creation. A `storage`
is handed to every breaker the registry creates; each coordinates under its own
name.

## Enums

- **`State`** — `CLOSED`, `OPEN`, `HALF_OPEN`, `FORCED_OPEN`, `DISABLED`,
  `METRICS_ONLY`. A `StrEnum`; values are stable lowercase identifiers.
- **`Outcome`** — `SUCCESS`, `FAILURE`, `SLOW_SUCCESS`, `SLOW_FAILURE`, with
  `.is_failure` and `.is_slow` properties.
- **`WindowType`** — `COUNT_BASED`, `TIME_BASED`.

## `WindowSnapshot`

Frozen dataclass: `total_calls`, `failed_calls`, `slow_calls`, plus
`.failure_rate` and `.slow_call_rate` properties (both `0.0` when empty).

## Errors & warnings

- **`InterlockError`** — base of all interlock errors.
- **`CircuitOpenError(breaker_name, *, retry_after=None, last_failure=None)`** —
  raised on rejection; attributes `breaker_name`, `retry_after`, `last_failure`.
- **`CallTimeoutError(timeout)`** — raised by `timeout` and `sync_timeout`;
  attribute `timeout`.
- **`InterlockDeprecationWarning`** — subclasses `UserWarning`, visible by
  default.

## `timeout` / `sync_timeout`

```python
async with timeout(seconds): ...   # async block

@sync_timeout(seconds)             # synchronous callable
def work(): ...
```

`timeout` is an async context manager that raises `CallTimeoutError` if the
block exceeds `seconds`. `sync_timeout` is a decorator that runs a synchronous
callable in a daemon worker thread and raises `CallTimeoutError` if it overruns
`seconds`; the worker keeps running after a timeout (Python cannot kill a
thread). See [Timeout](guides/timeout.md).

## Protocols (extension points)

Implement any of these to swap a core behaviour:

- **`Clock`** — `monotonic() -> float`. Inject a fake for deterministic tests.
- **`SlidingWindow`** — `record(outcome)`, `snapshot() -> WindowSnapshot`.
- **`Storage`** / **`AsyncStorage`** — shared-state backend as atomic *intent*
  operations: `read`, `trip_open`, `begin_half_open_if_elapsed`, `lease_probe`,
  `record_probe`, `close`. `trip_open`/`close` take an optional
  `expected_version` (version-fenced CAS); every write carries a `ttl`.
  Mechanism only — threshold policy stays in the core. `AsyncStorage` is the
  awaitable mirror. See the [Redis integration](integrations/redis.md).
- **`FailureClassifier`** — `is_failure(*, result, exception) -> bool`. See
  [Failure classification](guides/failure-classification.md).
- **`EventListener`** — `on_state_change`, `on_call`, `on_rejected`, `on_reset`,
  plus `on_storage_degraded` / `on_storage_recovered` for coordinated breakers
  (dispatched only if present, so pre-1.2 listeners keep working). See
  [Observability](guides/observability.md).

## Shared-state types

- **`SharedState`** — frozen snapshot of one breaker's coordinated state:
  `state`, `opened_at` (backend time), `version` (for fencing), and the
  HALF_OPEN probe accounting (`probes_permitted`, `probes_remaining`,
  `probes_completed`, `probe_failures`, `probe_slows`).
  `SharedState.closed()` is the baseline an absent key implies.
- **`ProbeLease`** — result of `lease_probe`: `granted: bool` plus the
  post-attempt `state: SharedState`.

## Listeners

- **`LoggingEventListener(logger=None)`** — top-level; zero dependencies.
- **`interlock.otel.OTelEventListener(meter=None)`** — extra `interlock-cb[otel]`.

## httpx2 adapters

Extra `interlock-cb[httpx2]`, module `interlock.httpx2`:

- **`CircuitBreakerTransport(transport, *, config=None, clock=None, classifier=None, listener=None)`**
- **`AsyncCircuitBreakerTransport(transport, *, ...)`**
- **`HttpStatusClassifier`** — fails on transport exceptions and statuses
  `429, 500, 502, 503, 504`.

See the [httpx2 integration](integrations/httpx2.md).

## FastAPI adapters

Extra `interlock-cb[fastapi]`, module `interlock.fastapi`:

- **`breaker_dependency(name, *, registry)`** — returns a `Depends`-compatible
  callable yielding the named breaker from a shared `Registry`.
- **`install_exception_handler(app)`** — registers a handler mapping
  `CircuitOpenError` to `503` with a `Retry-After` header.
- **`circuit_open_handler(request, exc)`** — the handler itself, for custom
  registration.

See the [FastAPI integration](integrations/fastapi.md).

## Redis adapters

Extra `interlock-cb[redis]`, module `interlock.redis`:

- **`RedisStorage(client, *, key_prefix='interlock:cb:', state_ttl=300.0, poll_interval=1.0, retry_backoff=5.0)`** —
  sync `Storage` over a `redis.Redis` client.
- **`AsyncRedisStorage(client, *, ...)`** — async mirror over
  `redis.asyncio.Redis`.

One Redis hash per breaker; every transition is a Lua script (atomic across
racing instances), elapse checks use the server's `TIME`. Works against Redis
(5.0+), Valkey, or any RESP-compatible server.

See the [Redis integration](integrations/redis.md).
