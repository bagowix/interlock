# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`CircuitBreaker.call_sync()` and `call_async()` protect a call without
  deciding what it is.** `call()` inspects every callable it is handed —
  unwrapping `functools.partial`, probing `__call__` — and the decorator copies
  metadata onto a freshly allocated closure. A caller that already knows its own
  nature paid for a decision it could not change. The two new methods run the
  same protected path with the dispatch removed; `call()` keeps dispatching and
  stays the default. `call_async` awaits whatever the callable returns, so a
  callable that is not a coroutine *function* (a middleware handler, say) works
  too; `call_sync` never awaits, so a coroutine function passed to it is
  recorded as an immediate success.
- **`Registry` can now be enumerated: `names()` and `items()`.** The registry
  could only be asked about a name you already knew, which is exactly the case
  that does not hold where it matters most: every HTTP integration creates its
  breakers lazily, one per host, so the set of names is only known at runtime.
  Listing them for a diagnostics endpoint during a `METRICS_ONLY` rollout, or
  applying an operator override to all of them before a maintenance window,
  meant reaching into the private `registry._breakers`. Both methods take a
  point-in-time copy under the registry lock — a breaker created afterwards is
  not in it, and the returned tuple never changes.

### Fixed

- **A bug in your own code no longer opens the circuit of a healthy
  dependency.** The HTTP integrations counted every exception raised inside the
  guarded call as the dependency failing, including the ones the client library
  raises for the *caller's* mistake: a scheme-less URL (`UnsupportedProtocol`)
  or a local protocol violation (`LocalProtocolError`) in httpx2/httpx, a URL
  or proxy URL with no host (`InvalidURL`) in requests. A burst of them was
  enough to trip the breaker and start rejecting real traffic to a host that
  was answering fine — and in `METRICS_ONLY` they polluted the very
  baseline used to pick thresholds. Those exceptions now count as successes and
  still propagate unchanged. `HttpStatusClassifier(excluded_exceptions=...)`
  replaces the set: pass `()` for the old behaviour, or add `PoolTimeout` (kept
  a failure by default — an exhausted pool usually means the dependency is
  holding connections open) when the local pool is sized below your own burst.
  aiohttp excludes nothing by default, since it rejects malformed URLs before
  the middleware chain runs; the knob is there for the exceptions of
  middlewares of your own. The set is validated at construction — an entry that
  is not an `Exception` subclass raises `TypeError` there instead of crashing
  the classifier mid-incident, when the first real failure arrives.

### Changed

- **The HTTP integrations no longer rebuild their guarded wrapper on every
  request.** Each request through the httpx2/httpx transports, the requests
  adapter and the aiohttp middleware re-ran the breaker's sync/async detection
  and built a fresh `functools.wraps` closure before the request could start —
  per-request overhead with no behavioural value, paid exactly when a service is
  busiest. They now call the breaker directly (`call_sync` / `call_async`), and
  the aiohttp middleware no longer needs its per-request coroutine wrapper.
  Against a stubbed transport that answers from memory, an httpx request through
  the wrapper falls from ~10.5 µs to ~7.5 µs (sync) and from ~9.9 µs to ~8.0 µs
  (async); ~3.5 µs of that is the stub itself, so the integration's own overhead
  drops by roughly 40%. Behaviour is unchanged.
- **The pipeline's breaker step stopped rebuilding its wrapper as well.**
  `CircuitBreakerStrategy` decorated the next layer on every call for the same
  reason, although it statically knows whether that layer is sync or async. It
  now routes through `call_sync` / `call_async`: the same protected path, one
  detection and one closure fewer per pipeline call.
- **A `Registry` cache hit no longer takes the registry lock.** Because the
  transport integrations create one breaker per host lazily, every request paid
  a lock acquisition to find a breaker that was already there. The lookup now
  reads the cache first and falls back to the locked, double-checked creation
  path only for a name it has not seen — one name still yields exactly one
  breaker, and a lost creation race returns the winner.
- **`FailureClassifier.is_failure` now declares `exception: Exception | None`.**
  The engine never passed a `BaseException` — cancellation and shutdown are
  released without being classified — so the wider annotation invited
  classifier authors to write cancellation handling that could never run. A
  classifier still annotated `BaseException | None` keeps type-checking.
- **A caller-owned `Registry` needs its own HTTP classifier.** Handing one to the
  httpx2/httpx transports, the aiohttp middleware or the requests adapter moves
  the failure policy to the registry, so a returned `503` counts as a success
  unless the registry is built with `classifier=HttpStatusClassifier()`. The
  `registry` argument now states that where the registry is configured.
- **The opposite trap is documented as well.** A registry configured with
  `HttpStatusClassifier` reads the status off every result it records, so
  borrowing a breaker from it for non-HTTP work raised `AttributeError` on the
  first returned value. The four shared-registry guides now say to keep such a
  registry for HTTP clients only.
- **Rejecting an option the injected registry already owns explains itself.** The
  `ValueError` used to name the conflicting options and stop; it now says the
  registry owns them and to configure them there.

## [2.5.0] - 2026-08-08

### Added

- **Windows and macOS regressions are now caught before release.** A lightweight CI
  matrix exercises the platform-sensitive runtime paths on the oldest and newest
  supported Python versions while the full quality, coverage and Redis matrix stays
  on Ubuntu.

- **Transport integrations can key breakers by logical dependency instead of
  raw host.** Service-discovery suffixes and shared gateway hosts previously
  made the httpx2/httpx transports, aiohttp middleware, and requests adapter
  merge unrelated dependencies or split one dependency across several
  breakers. Their new `name_resolver=` callback receives the native request
  and supplies the single name used by the registry, open-circuit errors, and
  listener events. Host-based naming remains the default, while empty custom
  names and non-string results fail before any network I/O.

- **Listener contracts now match the events a component actually emits.**
  `CoreEventListener`, `StorageEventListener` and `PipelineEventListener` let a
  breaker-only, coordination-only or strategy-only sink pass strict type
  checking without inheriting unrelated hooks. The existing `EventListener`
  remains the complete ten-hook contract and continues to satisfy every narrowed
  annotation, so existing listeners require no migration. Listener documentation
  now makes the `name` namespace explicit: breaker for core/storage events,
  strategy for pipeline events.

- **Partial `EventListener` implementations now pass strict type checking.** A
  listener that only needs one hook previously had to stub all ten before mypy,
  pyright and pyrefly would accept it, despite runtime dispatch already treating
  every hook as optional. `EventListener` now provides inherited no-op hooks, so
  subclasses override only what they observe and remain compatible when later
  releases add hooks; structurally complete listeners continue to work without
  inheritance.

- **Transport integrations can share a caller-owned `Registry` across
  clients.** Previously each httpx2/httpx transport, aiohttp middleware, and
  requests adapter built an isolated registry, so clients calling the same
  host accumulated separate windows and could disagree about dependency
  health. The new `registry=` option gives them one breaker per host and one
  policy source, rejects conflicting construction options, and leaves teardown
  to the registry's owner while still closing each wrapped connection pool.

- **Direct-call baseline and threaded-contention benchmarks**, closing out the
  two gaps left in #84. The CodSpeed suite reported the absolute cost of every
  protected path but not the number a prospective user actually wants — the
  breaker's overhead relative to calling the function directly.
  `test_baseline_direct_call` and its async twin make that ratio derivable
  from any run. `benchmarks/test_contention.py` drives one breaker from four
  worker threads so the work under the shared lock is tracked as a trend; its
  docstring and `CONTRIBUTING.md` spell out that instruction counting runs
  threads one at a time, so the number is lock-path work, not wall-clock
  contention.

### Changed

- **Lowered the `otel` extra's floor from `opentelemetry-api>=1.43.0` back to
  `>=1.20.0`.** The higher floor was never a requirement of `OTelEventListener`
  — it arrived as a side effect of a routine Dependabot bump that moved the dev
  pin and the extra's floor together. `opentelemetry-distro`/SDK releases pin
  the whole OTel stack to one API version, so the stale floor forced adopters
  on an older distro to choose between upgrading their entire OTel stack and
  dropping `interlock-cb[otel]` — for a listener whose calls
  (`get_meter`, `create_histogram`, `create_counter`) have been stable across
  that range. `.github/workflows/ci.yml`'s `extras-min` job now pins and tests
  against `opentelemetry-api==1.20.0`; the dev dependency group stays on the
  current version.

## [2.4.0] - 2026-08-06

### Added

- **Breakers can now start in a safe operator state before serving traffic.**
  `CircuitBreaker` and `Registry` accept `initial_state`; `CLOSED`, `FORCED_OPEN`,
  `DISABLED` and `METRICS_ONLY` are valid, while transitional `OPEN` / `HALF_OPEN`
  fail fast. Lazy registry creation applies the state before publishing a breaker,
  so per-host HTTP integrations can deploy in shadow mode without a first-call race.
  The httpx2 and httpx transports, aiohttp middleware and requests adapter expose
  their registry for diagnostics and accept the same option.

- **httpx now has a first-class transport integration (#82).** Install
  `interlock-cb[httpx]` and wrap `httpx.HTTPTransport` or
  `httpx.AsyncHTTPTransport` to apply one breaker per request host without
  decorators. The sync and async wrappers preserve streaming responses,
  delegate the wrapped transport's full lifecycle (context entry and exit,
  `close()` / `aclose()`), reject host-less URLs before I/O, and use the same
  configurable `429, 500, 502, 503, 504` failure policy as the existing HTTP
  integrations. Unit and real-loopback tests run against both the minimum
  supported httpx 0.27.0 and the locked latest version in CI.

### Changed

- **The README now gets readers from installation to their first guarded call faster.** The core
  API appears before project comparisons, the feature language is less brittle, and optional
  integrations are presented in one compact, linked overview instead of several partial examples.

### Fixed

- **The httpx2 transports now delegate context-manager entry and exit to the
  wrapped transport.** Entering `with client:` (or `async with client:`)
  previously never entered the wrapped transport, so a custom transport that
  acquires resources in `__enter__` / `__aenter__` was not ready before its
  first request. Exit now also releases every per-host breaker, matching
  `close()` / `aclose()`.

- **Closing an HTTP integration now also releases its per-host breakers.** The httpx2
  transports and requests adapter close both their native connection resources and
  the registry; the aiohttp middleware exposes `aclose()` for application shutdown.

## [2.3.0] - 2026-08-05

### Added

- **The coordinator's write queue is now bounded (#99).** The queue feeding a
  coordinated breaker's background lane had no upper bound: a lane that stopped
  draining — a storage client blocking without a timeout, an async lane whose
  event loop is gone — grew it for as long as the process lived. It now holds
  at most `write_queue_size` writes (a new `RedisStorage` /
  `AsyncRedisStorage` keyword, default 128, read off the storage object like
  the other coordination knobs). Over capacity the arriving write is dropped
  rather than queued: it never blocks and never raises inside the protected
  path, and it is reported through the new optional
  `on_storage_write_dropped` listener hook (`LoggingEventListener` logs it at
  `WARNING`, `OTelEventListener` counts it on `interlock.storage.events`).
  Coordinated writes were already best-effort — a dropped one is reconciled by
  the next poll and by `state_ttl` — and healthy lanes never reach the bound,
  since writes are per transition rather than per call. Shutdown keeps working
  with a full queue: one slot stays reserved for the stop sentinel.

## [2.2.0] - 2026-08-03

### Added

- **The Redis integration guide now documents the coordinated-mode contract
  (#101).** Fencing (`expected_version`), the `state_ttl`-bounded probe-lease
  leak on an interrupted probe, the "tallies only while `HALF_OPEN`" rule for
  `record_probe`, and the need for explicit teardown were previously only
  implicit in the code and internal notes. `docs/integrations/redis.md` gains
  a "Coordinated mode contract" section plus a checklist for anyone
  implementing `Storage` / `AsyncStorage` against a backend other than Redis,
  and the `Storage` / `AsyncStorage` protocol docstrings point at it.

- **The degraded-storage retry policy is now configurable (#100).** Every
  release before this one retried a downed `Storage` backend on a single
  fixed cadence (`retry_backoff`) — with many instances sharing that backend,
  they all retried in lockstep, so a recovering backend immediately faced a
  synchronised retry wave. `RedisStorage` / `AsyncRedisStorage` gain three new
  keyword-only knobs: `retry_backoff_multiplier` grows the delay
  geometrically with each further *consecutive* failure, `retry_backoff_max`
  caps it, and `retry_jitter` spreads it with a proportional random draw
  (deterministic under an injected clock, so it stays reproducible in tests).
  The attempt counter resets on recovery. Defaults (`multiplier=1.0`,
  `jitter=0.0`) exactly reproduce the fixed-delay behavior of earlier
  releases — nothing changes unless you opt in. See the Redis integration
  guide's Tuning section for the new knobs.

- **CI now catches two release-only failure modes before they ship (#85).** A
  `packaging-smoke` job builds the wheel, runs `twine check` and
  `check-wheel-contents` against it, then installs it with `pip`-equivalent
  semantics (`--no-deps`, no dev group, no extras) into a clean Python 3.11
  environment and imports `interlock` there. It asserts `interlock.__version__`
  matches the installed distribution's metadata, that `interlock/py.typed`
  survived the build, and that every module under `interlock/integrations/`
  made it into the wheel — an accidental import of an optional dependency from
  the core, or a packaging config that silently dropped a subpackage, would
  pass the dev-env test suite and only break for a plain `pip install
  interlock-cb`. Separately, `release.yml` now verifies the pushed tag is
  exactly `v{interlock.__version__}` as its first step, before `uv sync`,
  tests, or the build run — the cheapest possible place to fail, and the tag
  reaches the shell through `env` rather than direct interpolation.

### Changed

- **The model-based state-machine test now reaches probe rounds.** The
  `RuleBasedStateMachine` from #106 exists for the order-dependent half of the
  machine — the probe budget, generation fencing, out-of-order settles — and it
  was not exercising any of it: measured over 20 seeds, 18 finished a whole run
  without entering `HALF_OPEN` once, and the best seed managed 7 entries. The
  rule mix was the cause. Four of nine rules were operator controls, three of
  them leading into an absorbing override state that only `reset` leaves — and
  `reset` wipes the window on the way out — while tripping a window took an
  uninterrupted run of `2 x minimum_number_of_calls` steps, since every call
  cost one step to admit and another to settle. The walk spent its examples
  bouncing between the overrides and CLOSED. Three changes fix it: a `call`
  rule that admits and settles in one step, the way `Engine.call_*` actually
  works; the four operator controls collapsed into one sampled rule; and a
  `teardown` that reports trips and probe rounds through `target()` so
  hypothesis's search steers toward them. `advance` also draws from a strategy
  that reaches the end of the open wait, which plain `st.floats` — biased
  toward `0.0` — almost never did. 17 of 20 seeds now reach `HALF_OPEN` on
  roughly half of their examples, and every override state is still entered, so
  the #79 invariant keeps its coverage. `max_examples` drops from 200 to 120:
  the walk no longer needs the extra examples to stumble into a probe round,
  which keeps the file's runtime in the same range as before.

- The `Clock` protocol now documents that `monotonic()` must be non-negative.
  `TimeBasedSlidingWindow` reserves a negative second as its "never written"
  bucket sentinel, so a custom clock returning negative values would report a
  permanently empty window and the breaker would never trip. Every stdlib
  monotonic clock already satisfies this; the contract simply said
  "monotonically increasing" and left the rest implied.

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
  to acknowledge it. The one finding it ignores is the release bump of
  `interlock.VERSION`, which griffe reports as a changed attribute value —
  that is the release mechanism working, and every release would otherwise
  have to be labelled a breaking change. `griffe` is a dev/CI-only dependency;
  the core stays at zero.

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

[Unreleased]: https://github.com/bagowix/interlock/compare/v2.5.0...HEAD
[2.5.0]: https://github.com/bagowix/interlock/compare/v2.4.0...v2.5.0
[2.4.0]: https://github.com/bagowix/interlock/compare/v2.3.0...v2.4.0
[2.3.0]: https://github.com/bagowix/interlock/compare/v2.2.0...v2.3.0
[2.2.0]: https://github.com/bagowix/interlock/compare/v2.1.4...v2.2.0
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
