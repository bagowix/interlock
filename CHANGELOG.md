# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Resilience pipeline core** (`interlock.pipeline`): a `Strategy` protocol
  (sync + async in one class, mirroring the v1 breaker contract), a `Pipeline`
  executor applying strategies in declaration order (first = outermost, Polly
  semantics), and adapters for the existing primitives —
  `CircuitBreakerStrategy` (wraps a standalone `CircuitBreaker` unchanged) and
  `TimeoutStrategy` (bounds every attempt via `timeout` / `sync_timeout`).
  `BaseException` passes through every layer unswallowed; the standalone
  breaker API is untouched.
- `RetryStrategy` (`interlock.integrations.tenacity`, requires the `tenacity`
  extra): a bounded retry layer for the pipeline delegating all policy to
  tenacity — attempts always capped, the original exception re-raised when the
  budget runs out, `CircuitOpenError` not retried by default
  (`retry_unless_open`), patient mode via `wait_probe`, sync/async sleep
  injectable, `before_sleep` hook passed through. Importing the module without
  tenacity installed now raises an error pointing at the extra.
- `BulkheadStrategy` (`interlock.pipeline`): caps concurrent calls per runtime
  (a `threading.Semaphore` for sync, an `asyncio.Semaphore` for async, one
  config for both). With no free slot it rejects immediately by default or
  waits up to `max_wait` seconds, raising the new `BulkheadFullError`
  (exported from `interlock`) — deliberately distinct from `CircuitOpenError`:
  local saturation is not dependency failure.
- `FallbackStrategy` (`interlock.pipeline`): substitutes an explicit fallback
  value for selected failures only — the `fallback` callable receives the
  caught exception, `on` accepts `Exception` subclasses exclusively (so
  cancellation always propagates), and the strategy's result is typed as the
  honest union `T | F`, not `Any`. Works outermost over `CircuitOpenError` /
  `BulkheadFullError` / `CallTimeoutError`, and never masks shadow-mode
  (`metrics_only`) statistics.
- Pipeline DSL: `Pipeline` is now usable as a signature-preserving decorator
  (`@pipeline`, `ParamSpec`-typed like the breaker's), and
  `Pipeline.builder()` assembles strategies step by step —
  `.fallback(...)`, `.retry(...)` (lazy tenacity import), `.circuit_breaker(...)`,
  `.bulkhead(...)`, `.timeout(...)`, `.add(custom)`, `.build()`. The pipeline
  surface (`Pipeline`, `PipelineBuilder`, `Strategy` and the four shipped
  strategies) is re-exported from `interlock`. There is deliberately no
  context manager: a `with` block cannot be re-run, so retry inside it is
  semantically impossible.
- Pipeline observability: `EventListener` gains three optional hooks —
  `on_retry(name, attempt, delay)`, `on_bulkhead_rejected(name)` and
  `on_fallback(name, error)` — dispatched via safe `getattr` (the v1.2
  pattern), so pre-2.0 listeners keep working unchanged. `RetryStrategy`,
  `BulkheadStrategy` and `FallbackStrategy` (and the matching builder steps)
  accept `name=` and `listener=`. `LoggingEventListener` logs retries at
  INFO and bulkhead rejections / fallbacks at WARNING; `OTelEventListener`
  counts all three in a new `interlock.pipeline.events` counter.

- Docs: a [comparison page](docs/comparison.md) — interlock-cb vs pybreaker,
  circuitbreaker, aiobreaker and purgatory (feature table, honest trade-offs).
- Runnable examples (`examples/`): `lifecycle.py` walks one breaker through
  CLOSED → OPEN → HALF_OPEN → CLOSED around a flaky gateway; `two_clients.py`
  shows two independently guarded clients in one asyncio loop — one dependency
  fails and falls back while the other keeps serving. Zero dependencies, no
  network, deterministic output; kept green by a CI smoke test and explained
  line by line on the new [demo docs page](docs/demo.md).

### Changed

- Docs: integration page titles no longer repeat the word "integration"
  under the *Integrations* nav section (e.g. "httpx2 integration" → "httpx2").

## [1.3.0] - 2026-07-08

### Added

- **tenacity integration** via the `tenacity` extra
  (`interlock.integrations.tenacity`): `retry_unless_open(*transient)` — a
  retry predicate that retries transient exceptions but stops as soon as the
  circuit opens — and `wait_probe(fallback, *, jitter=0.1)` — a wait strategy
  that sleeps exactly `CircuitOpenError.retry_after` (plus jitter) after a
  rejection and delegates to the fallback strategy otherwise.
- **aiohttp integration** via the `aiohttp` extra
  (`interlock.integrations.aiohttp`, requires aiohttp ≥ 3.12):
  `CircuitBreakerMiddleware` — a client middleware applying one breaker per
  request host.
- **requests integration** via the `requests` extra
  (`interlock.integrations.requests`): `CircuitBreakerAdapter` — an
  `HTTPAdapter` for `session.mount(...)` applying one breaker per request
  host.
- `HttpStatusClassifier` (httpx2, aiohttp, requests variants) now accepts
  `failure_statuses` to override the canonical retryable set
  (`429, 500, 502, 503, 504`).
- New docs: integrations overview, "Retries and circuit breakers" guide, and
  recipes for LLM SDKs (OpenAI/Anthropic) and Flask/Django 503 handlers.

### Changed

- Integration modules moved into the `interlock.integrations` subpackage:
  `interlock.integrations.httpx2`, `interlock.integrations.otel`,
  `interlock.integrations.fastapi`, `interlock.integrations.redis`. The old
  top-level import paths (`interlock.httpx2`, `interlock.otel`,
  `interlock.fastapi`, `interlock.redis`) are removed. Update imports
  accordingly; extras names and all public classes are unchanged.

## [1.2.0] - 2026-07-07

### Added

- **Distributed shared state** via the `redis` extra (`interlock.redis`):
  `RedisStorage` (sync) and `AsyncRedisStorage` (async) coordinate breaker
  state across processes and machines through one Redis hash per breaker.
  Every transition runs as a Lua script (atomic across racing instances,
  version-fenced against stale decisions), elapse checks use the Redis
  server's `TIME`, and keys carry a TTL so abandoned state self-expires.
  Works against Redis 5.0+, Valkey, or any RESP-compatible server.
- `CircuitBreaker` and `Registry` accept an optional `storage`
  (`Storage` / `AsyncStorage`). Without one, behaviour is unchanged and purely
  local. With one, a shared OPEN gates admission on every instance, and
  HALF_OPEN recovery probes are budgeted globally
  (`permitted_calls_in_half_open` in total across the fleet) via an atomic
  probe lease — the single inline storage operation on the protected path;
  everything else is a locally cached view refreshed by a background poller
  plus fire-and-forget writes. A coordinated breaker matches its storage's
  runtime: a sync storage serves the sync API, an async storage the async one;
  mixing styles raises `InterlockError`.
- **Graceful degradation:** a storage failure never reaches the protected
  path. The breaker falls back to its local state, leaves the backend alone
  for `retry_backoff` seconds, and resynchronises (shared view authoritative
  again) once the backend recovers.
- `EventListener` gains `on_storage_degraded` / `on_storage_recovered`,
  implemented by `LoggingEventListener` (WARNING/INFO) and `OTelEventListener`
  (new `interlock.storage.events` counter). The engine dispatches the two new
  hooks only if present, so existing listeners keep working unchanged.
- Reworked `Storage` protocol (plus new `AsyncStorage`) as atomic *intent*
  operations — `read`, `trip_open`, `begin_half_open_if_elapsed`,
  `lease_probe`, `record_probe`, `close` — with new public DTOs `SharedState`
  and `ProbeLease`. The previous `Storage` shape (`load`/`save`) was declared
  but never consumed by the engine; this release gives it its first
  functional form.

### Fixed

- Outcomes are now recorded into the state-machine era that admitted them:
  a call admitted in CLOSED can no longer settle as a HALF_OPEN probe, and a
  probe settling after a close or reset no longer pollutes the fresh window.
- `reset()` clears the remembered last failure, so a `CircuitOpenError` raised
  after a reset no longer reports a pre-reset exception.

## [1.1.0] - 2026-06-28

### Added

- `sync_timeout(seconds)` decorator: a synchronous counterpart to `timeout`.
  It runs the wrapped callable in a daemon worker thread joined with a deadline
  and raises `CallTimeoutError` on overrun. Documents the worker-thread
  limitation: Python cannot kill a thread, so the worker keeps running in the
  background after a timeout.
- `Config.auto_transition` (default `False`): opt into a timer that proactively
  moves a breaker `OPEN → HALF_OPEN` once `wait_duration_in_open` elapses,
  emitting the state change without waiting for the next call. The lazy
  transition stays authoritative; the timer admits no probe and is cancelled on
  `reset()`, `force_open()`, or when a call makes the move first.
- FastAPI integration via the `fastapi` extra (`interlock.fastapi`):
  `breaker_dependency(name, *, registry)` injects a shared breaker with
  `Depends`, and `install_exception_handler(app)` maps `CircuitOpenError` to
  `503 Service Unavailable` with a `Retry-After` header.

## [1.0.0] - 2026-06-27

### Added

- Core state machine: `CLOSED` / `OPEN` / `HALF_OPEN` plus the operator
  overrides `FORCED_OPEN`, `DISABLED` and `METRICS_ONLY` (shadow mode).
- Sliding windows behind a `SlidingWindow` protocol, with count-based and
  time-based implementations selected via `Config.window_type`.
- Failure-rate trigger with `failure_rate_threshold` and
  `minimum_number_of_calls`, and **slow-call detection** via
  `slow_call_duration_threshold` and `slow_call_rate_threshold`.
- Lazy `OPEN → HALF_OPEN` transition with a probe limit and a concurrency cap.
- Single public `CircuitBreaker` for sync and async, usable as a decorator, a
  sync/async context manager, and `breaker.call(fn, ...)`. Decorators preserve
  the signature and sync/async nature via `ParamSpec` + `@overload`.
- Manual control: `reset()`, `force_open()`, `disable()`, `metrics_only()`.
- `Registry` of named breakers with a shared default config and per-name
  overrides.
- Immutable `Config` (frozen dataclass) with eager validation.
- `FailureClassifier` protocol with a default policy (any raised exception is a
  failure); classification by result is supported by custom classifiers.
- `CircuitOpenError` carrying the breaker name, an estimated `retry_after`, and
  the last recorded failure.
- Async-first `timeout` primitive that turns a hang into `CallTimeoutError`.
- Observability: `EventListener` protocol, a zero-dependency
  `LoggingEventListener`, and an `OTelEventListener` (extra `interlock-cb[otel]`).
- httpx2 transport integration (extra `interlock-cb[httpx2]`):
  `CircuitBreakerTransport` and `AsyncCircuitBreakerTransport` apply a breaker
  per host, with an `HttpStatusClassifier` treating `429, 500, 502, 503, 504`
  and transport exceptions as failures.
- `InterlockDeprecationWarning` (subclasses `UserWarning`, visible by default).
- `py.typed`; strict mypy and pyright; 100% test coverage.

[Unreleased]: https://github.com/bagowix/interlock/compare/v1.3.0...HEAD
[1.3.0]: https://github.com/bagowix/interlock/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/bagowix/interlock/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/bagowix/interlock/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/bagowix/interlock/releases/tag/v1.0.0
