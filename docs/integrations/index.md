# Integrations

interlock plugs into the HTTP client, framework or retry library you already
use — you configure thresholds once and the breaker applies **per host** (or
per named dependency) with no decorators in call sites.

## Supported integrations

| Integration | Extra | What you get |
|---|---|---|
| [FastAPI](fastapi.md) | `interlock-cb[fastapi]` | `Depends`-injected breakers and a `CircuitOpenError → 503 + Retry-After` handler |
| [Litestar](litestar.md) | `interlock-cb[litestar]` | `Provide`-injected breakers and a `CircuitOpenError → 503 + Retry-After` handler (Litestar ≥ 2.23) |
| [httpx2](httpx2.md) | `interlock-cb[httpx2]` | `CircuitBreakerTransport` / `AsyncCircuitBreakerTransport` — per-host breaker at the transport level |
| [aiohttp](aiohttp.md) | `interlock-cb[aiohttp]` | `CircuitBreakerMiddleware` — per-host breaker as a client middleware (aiohttp ≥ 3.12) |
| [requests](requests.md) | `interlock-cb[requests]` | `CircuitBreakerAdapter` — per-host breaker mounted on a `Session` |
| [LLM SDKs](llm.md) | — (recipe) | Guard OpenAI / Anthropic SDK calls with a breaker + bounded retries |
| [tenacity](tenacity.md) | `interlock-cb[tenacity]` | Retry × breaker glue: stop retrying when the circuit opens, or wait exactly until the next probe |
| [Redis](redis.md) | `interlock-cb[redis]` | Shared breaker state across processes with graceful degradation |
| [Flask / Django](frameworks.md) | — (recipe) | Map `CircuitOpenError` to `503 + Retry-After` in other web frameworks |

## How integrations are built

Every integration follows the same rules, so learning one means knowing all:

- **Native extension points only.** A transport (httpx2), a client middleware
  (aiohttp), an adapter (requests), an exception handler (FastAPI). No
  monkey-patching, no private APIs — an integration survives minor releases
  of its host library.
- **One breaker per host.** HTTP integrations key breakers by request host:
  a failing `api.a` never trips `api.b`. Breakers are created lazily in a
  shared [`Registry`](../reference.md).
- **One classification model.** Responses are classified by an
  `HttpStatusClassifier` — by default the canonical retryable set
  (`429, 500, 502, 503, 504`) plus any transport exception counts as a
  failure, while `4xx` client mistakes do not. Pass
  `HttpStatusClassifier(failure_statuses={...})` or your own
  `FailureClassifier` to change the policy.
- **One rejection signal.** An open circuit always raises
  [`CircuitOpenError`](../reference.md) — carrying the breaker name, a
  `retry_after` estimate and the last recorded failure — *before* a
  connection is attempted.
- **Zero-dependency core.** Integrations live in `interlock.integrations.*`
  as optional extras; `import interlock` itself never pulls anything beyond
  the standard library.

## Support tiers

- **Tier 1 — shipped code.** Modules under `interlock.integrations.*`,
  covered by the test suite and CI against both the minimum supported and the
  latest version of the host library. Semver applies.
- **Tier 2 — recipes.** Documented, runnable patterns (LLM SDKs,
  Flask/Django) that need no dedicated glue code. They can graduate to Tier 1
  when demand shows up.

Missing an integration — gRPC, SQLAlchemy, Kafka, Celery?
[Open an issue](https://github.com/bagowix/interlock/issues): the next wave
is prioritised by demand.
