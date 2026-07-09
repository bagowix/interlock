# Comparison

Picking a circuit breaker is mostly about which failure model, runtime and
coordination story you need. This page compares interlock-cb with the
established Python circuit breakers — honestly. interlock-cb is the youngest
of the five (first released in 2026); [pybreaker](https://github.com/danielfm/pybreaker),
[circuitbreaker](https://github.com/fabfuel/circuitbreaker),
[aiobreaker](https://github.com/arlyon/aiobreaker) and
[purgatory](https://github.com/mardiros/purgatory) have carried production
traffic for years, and for many projects one of them is exactly the right
choice.

## Feature table

| Feature | interlock-cb | pybreaker | circuitbreaker | aiobreaker | purgatory |
|---|:---:|:---:|:---:|:---:|:---:|
| Core states (closed / open / half-open) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Choose which exceptions count as failures | ✅ | ✅ | ✅ | ✅ | ✅ |
| Zero-dependency core | ✅ | ✅ | ✅ | ✅ | ✅ |
| `async` / `await` (asyncio) | ✅ | Tornado | ✅ | ✅ | ✅ |
| Sync and async in **one** breaker class | ✅ | — | ✅ | ✅ | separate factories |
| Trip condition | failure **rate** over a window | consecutive count | consecutive count | consecutive count | consecutive count |
| Time-based sliding window | ✅ | — | — | — | — |
| Slow-call detection | ✅ | — | — | — | — |
| Result-based failure classification | ✅ | — | — | — | — |
| Event / state-change listeners | ✅ | ✅ | — | ✅ | ✅ |
| Shared state across processes (Redis) | ✅ | ✅ | — | ✅ | ✅ |
| Globally budgeted recovery probes | ✅ | — | — | — | — |
| Fallback function | planned | — | ✅ | — | — |
| Fully typed API (`py.typed`) | ✅ | — | — | — | ✅ |
| Signature-preserving decorator (`ParamSpec`) | ✅ | — | — | — | — |
| HTTP client integrations (httpx2 / aiohttp / requests) | ✅ | — | — | — | — |
| Retry composition helpers (tenacity) | ✅ | — | — | — | — |
| OpenTelemetry metrics | ✅ | — | — | — | — |
| Operator overrides (force-open / disable / shadow mode) | ✅ | — | — | — | — |
| Years of production use | new | ✅ | ✅ | ✅ | ✅ |
| Latest release (as of July 2026) | 1.3.0 · 2026 | 1.4.1 · 2025 | 2.1.3 | 1.2.0 · 2021 | 3.0.1 · 2024 |
| Python | ≥ 3.11 | ≥ 3.9 | ≥ 3.8 | ≥ 3.6 | ≥ 3.9 |

<sub>Compared against pybreaker 1.4.1, circuitbreaker 2.1.3, aiobreaker 1.2.0
and purgatory 3.0.1, as documented in July 2026. "planned" items are on the
interlock-cb roadmap. Something out of date or unfair? Please
[open a PR](https://github.com/bagowix/interlock/pulls).</sub>

## The four established libraries, honestly

**pybreaker** is the original Python circuit breaker and the most proven one:
a small, stable sync breaker with listeners and optional Redis-backed state,
maintained for well over a decade. Its async support targets Tornado, not
asyncio, and it trips on a consecutive-failure count rather than a failure
rate. If you run a synchronous stack and want the most battle-tested option,
start here.

**circuitbreaker** has the smallest API of the five: one `@circuit` decorator
that also handles async functions, plus the only built-in **fallback
function** in this table. There are no listeners, no shared state and no rate
window — which is precisely its appeal when you need a guard on a handful of
call sites and nothing else.

**aiobreaker** is pybreaker ported to native asyncio, with the same
listener and Redis-storage features. Its last release was in 2021; for new
projects, prefer an actively maintained alternative.

**purgatory** brings a fully typed sync + async breaker with Redis storage
and event hooks. Sync and async live in separate factory classes
(`SyncCircuitBreakerFactory` / `AsyncCircuitBreakerFactory`), and tripping is
a consecutive-failure threshold with a TTL on the open state.

## Where interlock-cb differs

- **Rate over a window, not a streak.** A consecutive-failure counter resets
  on any single success, so a dependency failing 90% of requests under load
  can keep a breaker closed indefinitely. interlock trips on the failure
  *rate* across a count- or time-based sliding window
  ([configuration](guides/configuration.md)).
- **Slow calls are failures too.** A dependency that answers in 30 s can be
  worse than one that errors fast. `slow_call_duration_threshold` +
  `slow_call_rate_threshold` trip the breaker on latency degradation alone.
- **One class, both runtimes.** The same `CircuitBreaker` instance guards sync
  and async callables — decorator, context manager, or `call()` — and the
  decorator preserves the wrapped signature for type checkers.
- **Coordination built for fleets.** With the Redis storage, tripping is
  atomic across racing instances and half-open probes are budgeted globally,
  with graceful degradation to local state when Redis is down
  ([Redis integration](integrations/redis.md)).
- **Meets your stack where it is.** Per-host breakers ship for httpx2,
  aiohttp and requests; tenacity glue composes retries correctly; FastAPI
  maps rejections to `503 + Retry-After`
  ([integrations overview](integrations/index.md)).

The honest trade-off: interlock-cb requires Python ≥ 3.11, has no built-in
fallback yet, and has not had years in production. Reach for an established
library if that maturity matters more than the feature gap; choose
interlock-cb when you want rate-based windows, slow-call detection,
coordinated state and a fully typed API.
