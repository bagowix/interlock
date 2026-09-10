---
name: interlock-cb
description: Add, size, roll out and test circuit breakers in Python 3.11+ code with interlock-cb (import interlock), or migrate to it from pybreaker, circuitbreaker, aiobreaker or purgatory. Covers per-host guards for httpx, httpx2, aiohttp and requests, decorators for SDK and database calls, threshold sizing, shadow-mode rollout, 503 + Retry-After mapping in FastAPI, Litestar, Flask or Django, retry and timeout composition, bulkheads and fallbacks. Use when a Python project needs a circuit breaker, fail-fast protection from a slow or failing dependency, an end to retry storms or cascading failures, or when the user mentions interlock.
license: MIT
metadata:
  author: bagowix
  docs: https://bagowix.github.io/interlock/
---

# interlock-cb: add a circuit breaker the right way

interlock-cb is a circuit breaker for Python 3.11+. One `CircuitBreaker` class guards sync and async callables as a decorator, a context manager or `breaker.call(fn, ...)`. It trips on the **failure rate over a sliding window** (count- or time-based) and can count **slow calls** as failures. The core has zero dependencies; HTTP clients, web frameworks, tenacity, Redis and OpenTelemetry plug in as optional extras.

The library is from 2026, so your training data most likely predates it. Do not guess the API from pybreaker or circuitbreaker: every symbol you need is in this file, and the documentation links at the end cover the rest.

## Ground rules

- Package `interlock-cb`, import `interlock`. Python 3.11 or newer; if the project is older, say so and stop. pybreaker or circuitbreaker remain the right choice there.
- One breaker per dependency, keyed by host or by a named downstream. A failing host must never trip a healthy one.
- Thresholds come from observed traffic. Ship in shadow mode first (step 5), tune, then enforce.
- Every layer stays explicit: no hidden retries, no silent fallbacks, no swallowed `CircuitOpenError`.
- `Config` is a frozen dataclass validated at construction: to change a value, build a new one.
- Show the inventory and the plan to the user before editing code.

## Workflow

### 1. Inventory outbound calls

Search the code for clients and for resilience code that already exists:

- HTTP: `httpx.`, `httpx2.`, `aiohttp.ClientSession`, `requests.` (`Session`, `get`, `post`), `urllib3`.
- SDKs: `openai`, `anthropic`, `boto3`, `google.cloud`, `stripe`, `twilio`.
- Data and messaging: `redis`, `psycopg`, `asyncpg`, `sqlalchemy`, `pymongo`, `motor`, `aiokafka`, `pika`, `grpc`.
- Already present: `pybreaker`, `circuitbreaker`, `aiobreaker`, `purgatory`, `tenacity`, `backoff`, urllib3 `Retry(`, `max_retries=`, `timeout=`.

Produce a table: dependency, call sites, client library, sync or async, existing timeout, existing retries. Each row gets one decision in step 2.

### 2. Pick the guard per dependency

| Client | Guard | Extra |
|---|---|---|
| httpx | `CircuitBreakerTransport(httpx.HTTPTransport())` or `AsyncCircuitBreakerTransport(httpx.AsyncHTTPTransport())` from `interlock.integrations.httpx`, passed as `transport=` to the client | `interlock-cb[httpx]` |
| httpx2 | the same two classes from `interlock.integrations.httpx2` | `interlock-cb[httpx2]` |
| aiohttp 3.12+ | `CircuitBreakerMiddleware()` from `interlock.integrations.aiohttp`, passed as `middlewares=(middleware,)`; call `await middleware.aclose()` at shutdown | `interlock-cb[aiohttp]` |
| requests | `CircuitBreakerAdapter()` from `interlock.integrations.requests`, mounted on the `Session` for both `https://` and `http://` | `interlock-cb[requests]` |
| OpenAI or Anthropic SDK | wrap the SDK's httpx transport (every endpoint of the host shares one breaker), or decorate the call with an SDK error classifier when you need slow-call timing of the whole operation; set `max_retries=0` on the SDK client so one layer owns retries | `interlock-cb[httpx]` |
| anything else | a named `CircuitBreaker` from a shared `Registry`: decorator, `call()` or context manager | none |

HTTP integrations create one breaker per host lazily, so several hosts behind one client get separate breakers from a single transport, middleware or adapter, and its `registry` property lists them. Mount one `CircuitBreakerAdapter` instance for both URL prefixes. Responses are classified with the `HttpStatusClassifier` that each integration module exports (import it from the same module as the transport, middleware or adapter; `interlock` itself does not export it): `429, 500, 502, 503, 504` and transport exceptions count as failures, other `4xx` do not. Change the policy with `classifier=HttpStatusClassifier(failure_statuses={...}, excluded_exceptions=(...))`. A rejection raises a subclass of `CircuitOpenError` that is also a native error of the client (`CircuitOpenTransportError`, `CircuitOpenClientError`, `CircuitOpenRequestError`), so existing `except` blocks keep working.

Every integration takes the same keyword arguments: `config`, `clock`, `initial_state`, `classifier`, `listener` and `name_resolver`, or a ready `registry` in place of the first five (combining both raises `ValueError`). A registry you supply replaces the integration's default classifier, so build it with `classifier=HttpStatusClassifier()` from that module; otherwise a returned `503` counts as a success and the breaker never trips on HTTP failures. Keep such a registry to HTTP clients: the classifier reads `status_code` off every result and raises on anything else.

A service with several HTTP clients keeps one `Registry` per process and gives each client its own transport built on it, so a host reached from two clients is one breaker. With a registry you build yourself, also pass `unreachable_exceptions=(httpx.PoolTimeout,)`: the transport sets it only for a registry it creates, and without it an exhausted pool fails every half-open probe and the breaker stays open. Two httpx facts to check: a client given `transport=` ignores its own `limits=`, `verify=` and the proxy environment, so configure those on the inner transport; and the transport observes only the time to response headers, so a failure while reading a streamed body never reaches it. The factory, settings model, Prometheus listener, rollout stages and tests of a layout that has carried production traffic are in [references/production-httpx-service.md](references/production-httpx-service.md).

Transport level, async httpx:

```python
import httpx

from interlock import Config, State
from interlock.integrations.httpx import AsyncCircuitBreakerTransport

transport = AsyncCircuitBreakerTransport(
    httpx.AsyncHTTPTransport(),
    config=Config(failure_rate_threshold=0.5, minimum_number_of_calls=20),
    initial_state=State.METRICS_ONLY,
)
client = httpx.AsyncClient(transport=transport, timeout=5.0)
```

Call level, any dependency:

```python
from interlock import CircuitOpenError, Config, Registry, State

registry = Registry(
    config=Config(minimum_number_of_calls=20, slow_call_duration_threshold=2.0),
    initial_state=State.METRICS_ONLY,
)
payments = registry.get('payments-gateway')


@payments
async def charge(amount: int) -> str:
    return await gateway.charge(amount)


try:
    await charge(100)
except CircuitOpenError as exc:
    ...  # exc.retry_after: seconds until the next probe, or None
```

The decorator keeps the wrapped signature for type checkers. The context manager (`with breaker:` / `async with breaker:`) sees only exceptions and duration, so use the decorator or `call()` when failure lives in a return value.

### 3. Size the config

| Field | Default | How to choose |
|---|---|---|
| `failure_rate_threshold` | `0.5` | Fraction of failed calls in the window that trips. Range `(0, 1]`. |
| `minimum_number_of_calls` | `10` | Calls needed before the rate is trusted. Raise it for busy dependencies (50), lower it for quiet ones (5). |
| `window_type`, `window_size` | `COUNT_BASED`, `100` | Last N calls, or last N seconds with `WindowType.TIME_BASED`. Time-based suits high throughput. |
| `slow_call_duration_threshold` | `60.0` | Seconds at or above which a call is slow. Set it to the client timeout or just above the p99 latency. |
| `slow_call_rate_threshold` | `1.0` | Fraction of slow calls that trips. `1.0` trips only when every call in the window is slow, so latency is effectively off until you tune it down; try `0.5` to `0.8` once shadow data exists. |
| `wait_duration_in_open` | `60.0` | Seconds to stay open before the first probe. |
| `wait_duration_backoff_multiplier`, `wait_duration_in_open_max` | `1.0`, `None` | Grow the wait after each failed probe round and cap it. The multiplier must stay `1.0` with a shared storage. |
| `permitted_calls_in_half_open`, `max_concurrent_probes` | `10`, `1` | Probe budget per half-open round, and how many probes run at once. `max_concurrent_probes` must stay within `[1, permitted_calls_in_half_open]`. |
| `auto_transition` | `False` | `True` lets a timer flip OPEN to HALF_OPEN as soon as the wait elapses, without waiting for a call; the first real call is still the first probe. |

Validation raises `ValueError` at construction, so a bad value never reaches production.

Slow calls are a second dimension. A call at or above `slow_call_duration_threshold` feeds `slow_call_rate`, and a slow success stays a success in `failure_rate`. Once the window holds `minimum_number_of_calls`, either rate reaching its threshold trips the breaker.

Failure classification is separate from thresholds. The default counts every raised exception as a failure and every returned value as a success. Write a classifier when failure is a return value, or when business errors (a `404`, a validation error) must stay out of the rate:

```python
class IgnoreNotFound:
    def is_failure(self, *, result: object, exception: Exception | None) -> bool:
        if isinstance(exception, NotFoundError):
            return False
        return exception is not None
```

Pass it as `classifier=`. For LLM SDKs, count `429, 500, 502, 503, 504, 529` and the SDK's connection and timeout errors; a `400` or `404` is the caller's bug.

### 4. Compose timeouts and retries

- Put the timeout inside the guarded call so the breaker records `CallTimeoutError`: `async with timeout(2.0):` from `interlock`, or the `sync_timeout(2.0)` decorator for blocking code. For HTTP transports the client timeout does the same job.
- Put retries outside the breaker and stop the moment the circuit opens. `retry_unless_open(*transient)` from `interlock.integrations.tenacity` is the tenacity predicate for that; `CircuitOpenError` is never transient. The one exception is background work that prefers waiting for the next probe over failing: there the predicate must include `CircuitOpenError` (`retry_if_exception_type((TimeoutError, CircuitOpenError))`) and the wait is `wait_probe(wait_exponential_jitter())`, which sleeps `retry_after` after a rejection and defers to the wrapped strategy otherwise. Pick one mode per call site.
- An existing tenacity decorator stays where it is: change its `retry=` to `retry_unless_open(<transient types>)` and keep its `stop`. With a transport, middleware or adapter, a retry decorator on the calling function already sits outside the breaker.
- Keep one retry layer. Disable SDK retries (`max_retries=0`) and urllib3 `Retry` when tenacity owns retrying.
- Retry predicates must not match the rejection. The typed rejections descend from the client library's error hierarchy, so a predicate on `httpx.TransportError`, `requests.exceptions.RequestException` or `aiohttp.ClientError` retries every rejection; key it on leaf types (`httpx.ConnectError`, `httpx.ReadTimeout`) or use `retry_unless_open`.
- When several concerns stack, use the pipeline. Order is explicit and the first strategy is the outermost:

```python
from interlock import CircuitOpenError, Pipeline

pipeline = (
    Pipeline.builder()
    .fallback(lambda exc: [], on=(CircuitOpenError,))  # only for the listed errors
    .retry(attempts=4)  # interlock-cb[tenacity]; never retries an open circuit
    .circuit_breaker(breaker)
    .bulkhead(8)
    .timeout(2.0)
    .build()
)


@pipeline
async def fetch_picks(user: str) -> list[str]:
    return await client.get_picks(user)
```

### 5. Roll out in shadow mode

1. Deploy with `initial_state=State.METRICS_ONLY` on the breaker, `Registry` or transport, plus a listener: `LoggingEventListener()` from `interlock`, or `OTelEventListener()` from `interlock.integrations.otel` (`interlock-cb[otel]`). The breaker records real failure and slow-call rates and rejects nothing.
2. Read `breaker.snapshot()` over real traffic: it carries `total_calls`, `failed_calls`, `slow_calls`, `failure_rate` and `slow_call_rate`. `transport.registry.items()` lists the per-host breakers an HTTP integration has created so far. Adjust `Config`.
3. Deploy again with the default `initial_state=State.CLOSED`. The enforcing instance starts with a fresh window.

Keep the mode and every threshold in deployment configuration, so the switch from shadow to enforcing is a config change and never a release; the settings model in the reference above does this. Leave observability exporters (metrics, tracing, logging backends) unguarded: a breaker there can degrade the process recursively.

Map rejections at the web boundary so callers get `503` with `Retry-After`:

- FastAPI (`interlock-cb[fastapi]`): `install_exception_handler(app)` once, then `breaker_dependency('orders-db', registry=registry)` behind `Depends` injects a breaker into a route.
- Litestar (`interlock-cb[litestar]`): `dependencies={'breaker': breaker_dependency('orders-db', registry=registry)}` and `exception_handlers={CircuitOpenError: circuit_open_handler}` on the app.
- Flask or Django: an error handler that returns `503` and sets `Retry-After` to `math.ceil(exc.retry_after)` when it is not `None`. The framework recipe in the docs has both.

Share state through Redis (`interlock-cb[redis]`) only when many instances call the same downstream and should back off together under one global probe budget: `storage=RedisStorage(redis.Redis(...))` for sync code, `AsyncRedisStorage(redis.asyncio.Redis(...))` for async. A coordinated breaker serves only its storage's runtime. An unreachable Redis degrades the breaker to local state, never to a dead service. A coordinated breaker runs a background lane, so close it deterministically at shutdown with `breaker.close()` / `await breaker.aclose()`, or `registry.close_all()` / `await registry.aclose_all()`. Stay local when instances see different views of the dependency.

### 6. Write tests without sleeping

Time enters the breaker only through the injected `Clock`. Give the breaker a fake one and drive transitions explicitly. After `wait_duration_in_open` the next call is admitted as a probe and the state becomes `HALF_OPEN`; the round ends after `permitted_calls_in_half_open` probes, and the breaker closes when their failure and slow-call rates stay under the thresholds and re-opens otherwise. With the default of 10 probes, one successful probe leaves the breaker `HALF_OPEN`:

```python
import pytest

from interlock import CircuitBreaker, CircuitOpenError, Config, State


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now


def test__five_failures__opens_then_probes_after_wait() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(
        name='svc',
        config=Config(minimum_number_of_calls=5, wait_duration_in_open=30.0),
        clock=clock,
    )

    def fail() -> None:
        raise ConnectionError

    for _ in range(5):
        with pytest.raises(ConnectionError):
            breaker.call(fail)
    assert breaker.state is State.OPEN

    with pytest.raises(CircuitOpenError):
        breaker.call(fail)

    clock.now += 30.0
    assert breaker.call(lambda: 'ok') == 'ok'  # the first probe is admitted
    assert breaker.state is State.HALF_OPEN
```

Async callables get the same treatment through `await breaker.call(...)`.

### 7. Migrate from pybreaker, circuitbreaker, aiobreaker or purgatory

Those libraries trip on a streak of consecutive failures; interlock trips on a rate, so the numbers do not carry over.

| Before | After |
|---|---|
| `fail_max=5` / `failure_threshold=5` | `Config(minimum_number_of_calls=5, failure_rate_threshold=0.8)` as a starting point, then tune in shadow mode |
| `reset_timeout` / `recovery_timeout` | `wait_duration_in_open` |
| pybreaker `exclude=[...]` (exceptions that do not count) | a `FailureClassifier` that returns `False` for them (step 3) |
| circuitbreaker `expected_exception=...` (the inverse: only these count) | a `FailureClassifier` that returns `True` only for them |
| listeners | an `EventListener` passed as `listener=` |
| `fallback_function` | `FallbackStrategy` or `.fallback(...)` in a pipeline, with an explicit `on=` |
| Redis-backed state | `RedisStorage` / `AsyncRedisStorage` |

Keep the old breaker in place until shadow-mode data supports the new thresholds. The migration page has the full mapping per library.

## Anti-patterns to refuse

- One breaker shared by every host or dependency.
- Catching `CircuitOpenError` and retrying at once, or sleeping `2**n` seconds. Use `retry_unless_open`, or `wait_probe` when waiting is intended.
- Counting `4xx`, validation errors or cancellation as failures.
- Two retry layers on one call (SDK plus tenacity, urllib3 plus tenacity).
- A slow-call rate threshold without a duration threshold tied to real latency.
- `except Exception: return default` around a guarded call. Use a fallback with a listed exception type.
- `time.sleep` in tests.
- A shared storage together with `wait_duration_backoff_multiplier` above `1.0`, or a sync storage on an async call path. Both raise.
- Passing `registry=` together with `config`, `clock`, `initial_state`, `classifier` or `listener` to an integration. It raises; configure the registry instead.
- Handing an HTTP integration a registry built without `HttpStatusClassifier`: returned error statuses become successes.
- A retry predicate keyed on the client library's broad error base: it retries the typed rejection.

## Report back

End with the inventory table, the decision per dependency with the chosen thresholds and the reason, the rollout plan (shadow, observe, enforce), the tests added, and anything left out.

## Documentation

- Index: <https://bagowix.github.io/interlock/llms.txt>
- Everything inlined: <https://bagowix.github.io/interlock/llms-full.txt>
- Configuration: <https://bagowix.github.io/interlock/guides/configuration/>
- States and shadow mode: <https://bagowix.github.io/interlock/guides/states/>
- Retries: <https://bagowix.github.io/interlock/guides/retries/>
- Pipeline: <https://bagowix.github.io/interlock/guides/pipeline/>
- Integrations: <https://bagowix.github.io/interlock/integrations/>
- Migration: <https://bagowix.github.io/interlock/migration/>
- API reference: <https://bagowix.github.io/interlock/reference/>
