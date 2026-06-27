# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/bagowix/interlock/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/bagowix/interlock/releases/tag/v1.0.0
