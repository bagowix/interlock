# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.1.4] - 2026-08-03

### Added

- **"Correctness and testing" docs page** (`docs/correctness.md`), linked from
  the docs nav and from `README.md`. `README.md` was honest that interlock-cb
  is young and that pybreaker and circuitbreaker are proven, but left the
  strongest counter-argument unmade: the project already holds a bar most
  libraries in this space do not — 100% branch coverage, three strict type
  checkers, property- and model-based tests for the state machine, mutation
  testing on the state machine and engine, CI on free-threaded CPython, tested
  (not guessed) lower-bound dependency versions, and an auditable supply chain.
  The new page has one section per mechanism, each linking to the enforcing
  config, workflow or test file, plus a "known limits" section stating what is
  not covered.

- **OpenSSF Best Practices badge**, earned at the passing level
  (<https://www.bestpractices.dev/projects/13932>) and added to `README.md`,
  closing out #107.

- **Public-API breakage detection** (`griffe check`) on every pull request.
  v2.0 shipped without breaking changes and the standalone breaker surface
  stays untouched by the pipeline layer, but until now nothing mechanically
  verified either promise — mypy, pyright and pyrefly check that the code is
  internally consistent, not that its public surface is still compatible with
  the previous release, and `tests/typing_surface.py` only pins the shapes
  somebody remembered to write down. `.github/workflows/api-compatibility.yml`
  diffs the working tree against the latest release tag: removed or renamed
  objects, changed parameter kinds, order or defaults, narrowed return types.
  It covers `interlock/integrations/*` too, since that surface is public even
  though it is not re-exported from `__init__`. A breaking change is not
  automatically wrong — a future major release will make one on purpose — so
  the job is failable but overridable: label the pull request `breaking-change`
  to acknowledge it. `griffe` is a dev/CI-only dependency; the core stays at
  zero.

- **Mutation testing** (`mutmut`) over `interlock/_state_machine.py` and
  `interlock/_engine.py` — the two modules where a surviving mutant is a real
  bug. The 100% coverage gate proves every branch executes, not that anything
  would notice if it changed, and the first run made that concrete: 105 of 549
  mutants survived a fully covered suite. Killing them added tests for what the
  suite was taking on trust — that probe accounting frees exactly one slot,
  that a probe round is judged on rates rather than counts, that an era counter
  is never reused, that `retry_after` is clamped and measured from the moment
  the breaker opened, that a coordinated rejection still names its breaker and
  its last failure, and that the `auto_transition` timer is armed and cancelled
  exactly at the two moments it should be and by nothing else. The score is now
  **526 of 549 (95.8%)**; the 23 survivors are equivalent mutants, enumerated by
  class in `CONTRIBUTING.md`. It runs weekly out of band
  (`.github/workflows/mutation.yml`), never as a pull-request gate, and against
  the deterministic suite only — a randomised test that reaches a branch some of
  the time makes the score irreproducible.

- The test suite now runs on **free-threaded CPython (3.14t)** in CI, alongside
  3.11–3.14. interlock has always documented the breaker as thread-safe, but
  every interpreter in the matrix had a GIL, so nothing could falsify that
  claim. A new `tests/test_concurrency.py` drives a single breaker from many
  threads at once and asserts the four properties the lock exists to provide:
  window counts add up under concurrent recording, `snapshot()` never returns a
  torn view, the HALF_OPEN caps (`max_concurrent_probes`,
  `permitted_calls_in_half_open`) are never exceeded, and a `Registry` hands out
  exactly one breaker per name. No races were found; this verifies an existing
  claim rather than making a new one. The package is pure Python, so one wheel
  already serves both build flavours and the only distribution-level signal
  left is metadata: it now declares
  `Programming Language :: Python :: Free Threading :: 3 - Stable`.

- The state machine is now also tested as a **model**
  (`tests/test_state_machine_model.py`): a hypothesis `RuleBasedStateMachine`
  generates the *sequence* of steps — interleaved outcomes, clock advances,
  admissions, probe releases and operator overrides — rather than replaying a
  hand-written one, and checks the machine against an independently predicted
  state, generation, window aggregates and probe budget after every step.
  The existing properties in `tests/test_state_machine_properties.py` target
  documented boundaries directly and stay; this one looks for the transition
  orders nobody thought to write down (an override during a probe round, a
  probe settling an era late). No counterexample was found against the current
  implementation. The one sequence it did shrink pointed at the model instead:
  a failed probe returns to `OPEN` without clearing the probe counters, which
  is stale bookkeeping rather than a leak — nothing reads them outside
  `HALF_OPEN`, and entering it starts the round over. The budget invariant is
  now scoped to `HALF_OPEN`, where the contract defines it, and the sequence is
  pinned as a regression test.

- `CircuitBreaker.close()` / `aclose()` and `Registry.close_all()` /
  `aclose_all()` — a deterministic way to release a breaker's background work.
  Until now a coordinated breaker's lane (a daemon thread for a sync storage,
  an asyncio task for an async one) and the `auto_transition` timer ended only
  when the object was garbage collected, so a service could not flush queued
  shared writes, could not know when its threads were gone, and an async lane
  outliving `asyncio.run()` produced "task was destroyed but it is pending".
  `close()` drains the queued writes in order, wakes the lane instead of
  waiting out `poll_interval`, joins it, and cancels the timer without arming
  another. It is idempotent, safe to call from any thread, and terminal: the
  lane never restarts, and afterwards the breaker keeps protecting calls on
  local state while shared writes are dropped — the same behaviour as a
  degraded storage. The cached shared view is dropped with the lane, since
  nothing would refresh it and a peer's `OPEN` would otherwise never expire.
  This is teardown, not a state change: `close()` does not close the circuit
  (`reset()` does). The weakref-based collection path stays as the safety net
  for abandoned breakers.

- **pyrefly** joins mypy and pyright as a third strict type checker in CI and in
  the pre-commit hooks. Three independent implementations of the same type
  system disagree in the corners, and the corners are exactly where a
  signature-preserving decorator lives; a check the other two miss should fail
  before a release, not in a user's editor. The whole package is clean under its
  `strict` preset. `missing-override-decorator` is the one error kind turned off:
  `typing.override` is 3.12+ and the core carries no `typing_extensions`
  dependency to backport it.

### Changed

- Coverage reporting moved to **Codecov**. `pytest --cov` writes
  `coverage.xml` plus a JUnit report and CI uploads both, so a PR now shows the
  per-file coverage diff and the failing tests themselves rather than a single
  total. The 100% gate is unchanged and still enforced by pytest
  (`fail_under` in `pyproject.toml`); the Codecov statuses mirror it. This
  replaces `py-cov-action/python-coverage-comment-action` and the badge branch
  it maintained — the CI job no longer needs `contents: write` or
  `pull-requests: write`.

### Fixed

- A coordinator lane that exited while an op was in flight left the work queue
  permanently unfinished: the op was dequeued before the weak reference was
  resolved, so a lane stopping on a collected coordinator never called
  `task_done()` and a join on that queue could never return. Both lanes now
  release the dequeued op on the drop path.
- An `EventListener` that raises can no longer damage the breaker it observes.
  Hooks were invoked directly at every call site, so a bug in a listener —
  a metrics exporter, a logging handler, a custom sink — could replace a
  successful protected result with an observability exception or mask the
  dependency's own error. In coordinated (storage-backed) mode the damage was
  worse and silent: a raising `on_state_change` propagated out of the
  background lane's poll tick and terminated the lane for good, after which
  the breaker never refreshed the shared view or flushed queued writes again;
  and the same exception raised during a queued write was reported to the
  application as a *storage* failure through `on_storage_degraded`. Every hook
  now goes through one dispatcher: an `Exception` is logged to the `interlock`
  logger at `ERROR` with its traceback and then ignored, while
  `BaseException` — cancellation, shutdown — still propagates untouched. Hooks
  are dispatched by name and only if defined, so a listener may implement just
  the ones it needs. User-supplied *policy* callbacks keep their previous
  behaviour and still raise: a `FailureClassifier`, a pipeline fallback
  function, a tenacity `before_sleep` hook.

### Security

- **The CI supply chain is now auditable from the outside.** Workflows are the
  most privileged code in the repository and were the least checked part of it:
  every `uses:` resolved to a mutable tag, so a compromised action could have
  changed what a release publishes without a single commit here. Three changes
  close that, and they only work together — a Scorecard grade over unpinned
  actions would have been a badge that says less than it looks like it does:
  - every action is pinned to a full commit SHA with the version in a trailing
    comment (Dependabot updates both, and `zizmor --fix=all` writes the pin for
    a newly added action). Pinning surfaced two actions sitting a major behind
    their upstream, so they were bumped at the same time:
    `astral-sh/setup-uv` 7 → 9 and `codecov/codecov-action` 5 → 7;
  - [zizmor](https://docs.zizmor.sh) audits `.github/workflows/` on every pull
    request and locally through the pre-commit hook. CI hands it the job's own
    token so the four audits that resolve a pin against its upstream repository
    — `impostor-commit`, `ref-confusion`, `known-vulnerable-actions`,
    `stale-action-refs` — actually run; without one they are silently skipped
    and a SHA is only as trustworthy as the person who typed it. Its findings
    are fixed
    rather than muted: `actions/checkout` no longer leaves the job's credentials
    in `.git/config` (`persist-credentials: false`), `docs.yml` grants
    `pages: write` and `id-token: write` to the deploy job instead of to the
    whole workflow, and the release build no longer restores a dependency cache
    that a pull-request run could have written. The one suppression, with its
    reason, lives in `.github/zizmor.yml`;
  - [OpenSSF Scorecard](https://scorecard.dev/viewer/?uri=github.com/bagowix/interlock)
    runs weekly and on every push to `main`, publishing a per-check score to the
    code-scanning dashboard and to a README badge.

  Nothing about the release flow changed: it still builds once and publishes
  that artefact through PyPI's OIDC trusted publisher. The PEP 740 attestations
  it already produced are now requested explicitly rather than inherited from
  the action's default, and `SECURITY.md` documents where to fetch and verify
  them.

### Fixed

- Migration-guide links now use the anchors generated by Zensical, and the
  docs workflow runs in strict mode so broken links fail CI.

## [2.1.3] - 2026-07-31

### Fixed

- `CircuitBreaker.snapshot()` now reads the sliding window under the engine
  lock, so concurrent call settlement cannot expose mixed counters from a
  partially completed update. The count-based snapshot path remains O(1).
- Local manual controls now take precedence over shared state for coordinated
  breakers. `force_open()` rejects locally, while `disable()` and
  `metrics_only()` admit locally without consuming a shared `HALF_OPEN` probe.
  `reset()` clears only local control and metrics, then resumes the cached
  shared state rather than resetting the cluster.

## [2.1.2] - 2026-07-14

### Fixed

- The httpx transports now reject a request whose URL carries no host with an
  eager `ValueError`, matching the aiohttp and requests integrations.
  Previously such a request silently created (and shared) a breaker keyed on
  the empty string.
- Overlapping `with breaker:` / `async with breaker:` blocks on one breaker
  instance no longer mix up each other's timing. The guarded-block bookkeeping
  used a single instance-level stack, so when blocks from different threads or
  interleaved asyncio tasks exited out of LIFO order, each exit settled with
  the *other* block's start time and admission — corrupting durations (and so
  slow-call classification) and probe attribution. The stack now lives in a
  `ContextVar`: every thread and every asyncio task keeps its own, and nested
  blocks on one breaker keep working. The decorator and `call()` surfaces were
  never affected.
- A ``HALF_OPEN`` probe interrupted by a ``BaseException`` — an
  ``asyncio.CancelledError`` (client disconnect, task cancellation, a timeout
  composed *outside* the breaker), ``KeyboardInterrupt``, or any other
  non-``Exception`` — now returns its probe slot instead of leaking it.
  Previously each such interruption permanently shrank the ``HALF_OPEN``
  budget; once every slot had leaked the breaker wedged in ``HALF_OPEN``,
  rejecting all traffic until a manual ``reset()``. The interruption is still
  never recorded as an outcome (the v1 invariant stands: cancellation says
  nothing about the dependency). In coordinated (storage-backed) mode an
  interrupted *leased* probe is not returned to the shared budget — the
  storage protocol has no un-lease operation — and the backend TTL bounds
  that leak.

## [2.1.1] - 2026-07-14

### Fixed

- `CircuitBreaker.call()` and `Pipeline.call()` are now overloaded on the
  callable's sync/async nature, so strict type checkers infer the exact result
  type at the call site. Previously the plain `Awaitable[R] | R` return type
  made `await breaker.call(async_fn)` an error under both mypy `--strict` and
  pyright strict, and left sync results as a union needing a cast. Runtime
  behaviour is unchanged. The user-facing typing surface is now locked by
  static assertions (`tests/typing_surface.py`) checked by both type checkers
  in CI.

## [2.1.0] - 2026-07-11

### Added

- **Litestar integration** via the `litestar` extra
  (`interlock.integrations.litestar`, requires Litestar ≥ 2.23):
  `breaker_dependency(name, *, registry)` — a `Provide` factory injecting a
  shared breaker (annotate handlers with `NamedDependency[CircuitBreaker]`) —
  and `circuit_open_handler`, mapping `CircuitOpenError` to
  `503 Service Unavailable` with a `Retry-After` header.
- `examples/pipeline.py`: a third runnable demo — timeout + breaker +
  fallback composed around a dependency that hangs instead of erroring;
  deterministic output, walked through on the [demo page](docs/demo.md).

### Changed

- PyPI metadata now mentions the resilience pipeline (package description,
  keywords, `Framework :: AsyncIO` classifier).
- Docs navigation restructured: Comparison moved to the top-level block, the
  API reference got its own *Reference* section (both used to render as
  children of *Integrations*), and integration pages are ordered by demand —
  FastAPI and Litestar first.
- Security policy updated for the 2.x line.

## [2.0.0] - 2026-07-10

The resilience-pipeline milestone: interlock grows from one pattern into a
composable resilience framework, while the breaker stays a breaker.

**Backwards compatibility: there are no breaking changes.** The entire v1
public API — `CircuitBreaker`, `Registry`, `Config`, the timeout primitives,
every integration — is untouched; the whole v1 test suite passes unmodified.
The major version marks the scope of what is added, not a migration burden.

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
- New docs: a [resilience pipeline guide](docs/guides/pipeline.md) (strategies,
  recommended ordering with rationale, builder DSL, migration from v1, custom
  strategies); pipeline sections in the API reference, retries guide,
  observability guide, tenacity integration page, README and the comparison
  page (fallback is now shipped).
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

[Unreleased]: https://github.com/bagowix/interlock/compare/v2.1.4...HEAD
[2.1.4]: https://github.com/bagowix/interlock/compare/v2.1.3...v2.1.4
[2.1.3]: https://github.com/bagowix/interlock/compare/v2.1.2...v2.1.3
[2.1.2]: https://github.com/bagowix/interlock/compare/v2.1.1...v2.1.2
[2.1.1]: https://github.com/bagowix/interlock/compare/v2.1.0...v2.1.1
[2.1.0]: https://github.com/bagowix/interlock/compare/v2.0.0...v2.1.0
[2.0.0]: https://github.com/bagowix/interlock/compare/v1.3.0...v2.0.0
[1.3.0]: https://github.com/bagowix/interlock/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/bagowix/interlock/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/bagowix/interlock/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/bagowix/interlock/releases/tag/v1.0.0
