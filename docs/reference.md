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
- **`BulkheadFullError(max_concurrent, *, max_wait=0.0)`** — raised by a
  pipeline bulkhead when no concurrency slot frees up in time; attributes
  `max_concurrent`, `max_wait`.
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

## Pipeline

Compose strategies around one call, outermost first — see the
[pipeline guide](guides/pipeline.md):

- **`Pipeline(*strategies)`** — the executor; works as a signature-preserving
  decorator and as `pipeline.call(fn, *args, **kwargs)` (detect-dispatching,
  like the breaker's). No context manager by design.
- **`Pipeline.builder()` / `PipelineBuilder`** — step-by-step assembly:
  `.fallback(...)`, `.retry(...)` (lazy `tenacity` extra),
  `.circuit_breaker(breaker)`, `.bulkhead(...)`, `.timeout(seconds)`,
  `.add(custom)`, `.build()`.
- **`Strategy`** — the structural protocol: `execute(call)` /
  `execute_async(call)`; `execute_async` always receives a real coroutine
  function.
- **`CircuitBreakerStrategy(breaker)`** — wraps a standalone breaker unchanged.
- **`TimeoutStrategy(seconds)`** — bounds every attempt via the v1 primitives.
- **`BulkheadStrategy(max_concurrent, *, max_wait=0.0, name='bulkhead',
  listener=None)`** — concurrency cap; raises `BulkheadFullError`.
- **`FallbackStrategy(fallback, *, on=(Exception,), name='fallback',
  listener=None)`** — explicit substitution for selected failures; result
  typed `T | F`.
- **`RetryStrategy(...)`** — lives in `interlock.integrations.tenacity` (see
  below).

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
  and `on_retry` / `on_bulkhead_rejected` / `on_fallback` for pipeline
  strategies (all optional hooks are dispatched only if present, so older
  listeners keep working). See [Observability](guides/observability.md).

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
- **`interlock.integrations.otel.OTelEventListener(meter=None)`** — extra `interlock-cb[otel]`.

## httpx2 adapters

Extra `interlock-cb[httpx2]`, module `interlock.integrations.httpx2`:

- **`CircuitBreakerTransport(transport, *, config=None, clock=None, classifier=None, listener=None)`**
- **`AsyncCircuitBreakerTransport(transport, *, ...)`**
- **`HttpStatusClassifier(failure_statuses=None)`** — fails on transport
  exceptions and statuses `429, 500, 502, 503, 504` (override the set via
  `failure_statuses`).

See the [httpx2 integration](integrations/httpx2.md).

## aiohttp adapters

Extra `interlock-cb[aiohttp]` (aiohttp ≥ 3.12), module `interlock.integrations.aiohttp`:

- **`CircuitBreakerMiddleware(*, config=None, clock=None, classifier=None, listener=None)`** —
  client middleware for `ClientSession(middlewares=(...,))`; one breaker per
  request host.
- **`HttpStatusClassifier(failure_statuses=None)`** — same policy as the
  httpx2 variant, reading `ClientResponse.status`.

See the [aiohttp integration](integrations/aiohttp.md).

## requests adapters

Extra `interlock-cb[requests]`, module `interlock.integrations.requests`:

- **`CircuitBreakerAdapter(*, config=None, clock=None, classifier=None, listener=None, **adapter_kwargs)`** —
  `HTTPAdapter` subclass for `session.mount(...)`; one breaker per request
  host. Extra kwargs go to `HTTPAdapter`.
- **`HttpStatusClassifier(failure_statuses=None)`** — same policy, reading
  `Response.status_code`.

See the [requests integration](integrations/requests.md).

## tenacity helpers

Extra `interlock-cb[tenacity]`, module `interlock.integrations.tenacity`:

- **`retry_unless_open(*transient)`** — tenacity retry predicate: retries the
  listed transient exception types (default: any `Exception`), never
  `CircuitOpenError`.
- **`wait_probe(fallback, *, jitter=0.1)`** — tenacity wait strategy: sleeps
  `CircuitOpenError.retry_after` (+ up to `jitter` seconds) after a
  rejection, delegates to `fallback` otherwise.
- **`RetryStrategy(*, attempts=3, retry=None, wait=None, sleep=None,
  async_sleep=None, before_sleep=None, name='retry', listener=None)`** — a
  bounded retry layer for the pipeline: policy delegated to tenacity,
  attempts always capped, the original exception re-raised when the budget
  runs out, `CircuitOpenError` not retried by default.

See the [tenacity integration](integrations/tenacity.md) and the
[retries guide](guides/retries.md).

## FastAPI adapters

Extra `interlock-cb[fastapi]`, module `interlock.integrations.fastapi`:

- **`breaker_dependency(name, *, registry)`** — returns a `Depends`-compatible
  callable yielding the named breaker from a shared `Registry`.
- **`install_exception_handler(app)`** — registers a handler mapping
  `CircuitOpenError` to `503` with a `Retry-After` header.
- **`circuit_open_handler(request, exc)`** — the handler itself, for custom
  registration.

See the [FastAPI integration](integrations/fastapi.md).

## Redis adapters

Extra `interlock-cb[redis]`, module `interlock.integrations.redis`:

- **`RedisStorage(client, *, key_prefix='interlock:cb:', state_ttl=300.0, poll_interval=1.0, retry_backoff=5.0)`** —
  sync `Storage` over a `redis.Redis` client.
- **`AsyncRedisStorage(client, *, ...)`** — async mirror over
  `redis.asyncio.Redis`.

One Redis hash per breaker; every transition is a Lua script (atomic across
racing instances), elapse checks use the server's `TIME`. Works against Redis
(5.0+), Valkey, or any RESP-compatible server.

See the [Redis integration](integrations/redis.md).
