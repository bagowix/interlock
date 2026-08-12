# Migrating from pybreaker / circuitbreaker

Already using [pybreaker](https://github.com/danielfm/pybreaker) or
[circuitbreaker](https://github.com/fabfuel/circuitbreaker)? Moving to
interlock-cb is mostly mechanical — swap the import, translate the constructor
arguments, and keep the rest of your call sites. This page maps every concept
across, one library at a time.

If you have not decided *whether* to move yet, read the
[Comparison](comparison.md) first — this page assumes you have.

## The one conceptual change

Every established Python breaker trips on a **consecutive-failure count**:
`fail_max` / `failure_threshold` failures *in a row* open the circuit, and a
single success resets the counter. interlock trips on a **failure rate over a
sliding window** instead ([configuration](guides/configuration.md)).

That difference is the whole reason to migrate — a dependency failing 40% of
requests under load never trips a streak counter, because successes keep
resetting it — but it means the threshold numbers do **not** carry over
one-to-one. Everything else (timing, decorator, `call`, listeners) has a direct
equivalent.

Translate the trip condition like this:

| Old (streak) | interlock (rate over a window) |
|---|---|
| `fail_max=5` / `failure_threshold=5` | `minimum_number_of_calls` = how many calls to observe before trusting a rate; `failure_rate_threshold` = the fraction that trips |
| "open on 5 failures in a row" | e.g. `Config(minimum_number_of_calls=5, failure_rate_threshold=0.8)` — trip when ≥ 80% of the last window is failing |
| `reset_timeout` / `recovery_timeout` | `wait_duration_in_open` (seconds) — direct equivalent |

There is no exact arithmetic conversion, because the two models answer
different questions. A safe way to pick numbers is to run in
[shadow mode](guides/states.md#safe-rollout) first (see
[Roll out incrementally](#roll-out-incrementally) below) and read the real
rates off `breaker.snapshot()` before enforcing.

---

## From pybreaker

### Constructor

```python
# before — pybreaker
import pybreaker

breaker = pybreaker.CircuitBreaker(
    fail_max=5,
    reset_timeout=60,
    exclude=[ValueError],
    name='payments',
)
```

```python
# after — interlock
from interlock import CircuitBreaker, Config

breaker = CircuitBreaker(
    name='payments',
    config=Config(
        minimum_number_of_calls=5,
        failure_rate_threshold=0.8,
        wait_duration_in_open=60.0,
    ),
)
```

| pybreaker | interlock | Notes |
|---|---|---|
| `fail_max` | `minimum_number_of_calls` + `failure_rate_threshold` | Streak → rate; see [above](#the-one-conceptual-change). |
| `reset_timeout` | `Config.wait_duration_in_open` | Direct, in seconds. |
| `success_threshold` | `Config.permitted_calls_in_half_open` | Probes admitted before the breaker re-decides. interlock decides on the probe *rate*, not a fixed success count. |
| `exclude=[...]` | a `FailureClassifier` | See [below](#exclude-failureclassifier). |
| `name=` | `name=` | Same. |
| `state_storage=CircuitRedisStorage(...)` | `storage=RedisStorage(...)` | See [below](#redis-shared-state). |
| `listeners=[...]` | `listener=` | See [below](#listeners). |

### Decorator and `call`

Both keep working with the same shape — only the object changes:

```python
# before                         # after
@breaker                         @breaker
def charge(amount): ...          def charge(amount): ...

breaker.call(charge, 100)        breaker.call(charge, 100)
```

interlock's decorator additionally **preserves the wrapped signature** for type
checkers, and the same instance also works as a `with` block and on `async`
functions — no separate class ([getting started](getting-started.md#async)).

### `exclude` → `FailureClassifier`

pybreaker's `exclude` lists exceptions that should *not* count as failures.
interlock expresses the same policy as a
[classifier](guides/failure-classification.md):

```python
# before — pybreaker
breaker = pybreaker.CircuitBreaker(exclude=[ValueError])
```

```python
# after — interlock
class IgnoreValueError:
    def is_failure(self, *, result: object, exception: Exception | None) -> bool:
        if isinstance(exception, ValueError):
            return False  # business error, not a dependency problem
        return exception is not None


breaker = CircuitBreaker(name='payments', classifier=IgnoreValueError())
```

The classifier is strictly more capable: it also sees the **return value**, so
you can count an HTTP `503` response object as a failure — something no
exclude-list can do.

### Listeners

pybreaker's `CircuitBreakerListener` (`before_call` / `state_change` /
`failure` / `success`) maps onto interlock's
[`EventListener`](guides/observability.md):

| pybreaker | interlock |
|---|---|
| `state_change(cb, old, new)` | `on_state_change(*, name, old, new)` |
| `failure(cb, exc)` / `success(cb)` | `on_call(*, name, outcome, duration)` — `outcome.is_failure` distinguishes them |
| `before_call(cb, func, ...)` | — (no per-call pre-hook; use `on_call` after the fact) |
| — | `on_rejected(*, name)` fires when an open circuit rejects a call |

```python
# after — interlock
from interlock import State, Outcome


class Payments:
    def on_state_change(self, *, name: str, old: State, new: State) -> None:
        print(f'{name}: {old} -> {new}')

    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None: ...
    def on_rejected(self, *, name: str) -> None: ...
    def on_reset(self, *, name: str) -> None: ...


breaker = CircuitBreaker(name='payments', listener=Payments())
```

A built-in `LoggingEventListener` covers the common case with zero code.

### Redis (shared state)

```python
# before — pybreaker
import redis
import pybreaker

breaker = pybreaker.CircuitBreaker(
    state_storage=pybreaker.CircuitRedisStorage(pybreaker.STATE_CLOSED, redis.StrictRedis()),
)
```

```python
# after — interlock
import redis
from interlock import CircuitBreaker
from interlock.integrations.redis import RedisStorage

breaker = CircuitBreaker(
    name='payments',
    storage=RedisStorage(redis.Redis(host='redis.internal')),
)
```

interlock's coordination is stronger than shared counters: tripping is atomic
across racing instances, half-open probes are **budgeted globally** (N
instances send at most `permitted_calls_in_half_open` probes *in total*), and a
Redis outage degrades to local state instead of failing your calls
([Redis integration](integrations/redis.md)).

### State inspection and the open error

```python
breaker.current_state  # pybreaker → 'open' / 'half-open' / 'closed'
breaker.state  # interlock → State.OPEN / HALF_OPEN / CLOSED
```

```python
# before                                    # after
from pybreaker import CircuitBreakerError    from interlock import CircuitOpenError

try:                                         try:
    breaker.call(charge, 100)                    breaker.call(charge, 100)
except CircuitBreakerError:                  except CircuitOpenError as exc:
    ...                                          # exc.retry_after, exc.breaker_name,
                                                 # exc.last_failure
    ...
```

`CircuitOpenError` carries a `retry_after` estimate (seconds until the next
probe), which the [FastAPI](integrations/fastapi.md) /
[Litestar](integrations/litestar.md) extras turn into `503 + Retry-After`
automatically.

---

## From circuitbreaker

### Decorator

```python
# before — circuitbreaker
from circuitbreaker import circuit


@circuit(failure_threshold=5, recovery_timeout=30, expected_exception=ConnectionError)
def external_call(): ...
```

```python
# after — interlock
from interlock import CircuitBreaker, Config

breaker = CircuitBreaker(
    name='external_call',
    config=Config(
        minimum_number_of_calls=5,
        failure_rate_threshold=0.8,
        wait_duration_in_open=30.0,
    ),
    classifier=OnlyConnectionError(),  # see below
)


@breaker
def external_call(): ...
```

| circuitbreaker | interlock | Notes |
|---|---|---|
| `failure_threshold` | `minimum_number_of_calls` + `failure_rate_threshold` | Streak → rate. |
| `recovery_timeout` | `Config.wait_duration_in_open` | Direct, in seconds. |
| `expected_exception` | a `FailureClassifier` | Only these count as failures; see [below](#expected_exception-failureclassifier). |
| `fallback_function` | `FallbackStrategy` in a pipeline | See [below](#fallback_function-fallbackstrategy). |
| `name=` | `name=` | interlock requires a name explicitly. |

interlock separates the breaker object from the decorator: build one
`CircuitBreaker` and apply it with `@breaker`, rather than configuring a fresh
circuit at each decoration site. To reuse config across many call sites, share
a [`Registry`](guides/configuration.md#sharing-config-with-a-registry).

### `expected_exception` → `FailureClassifier`

circuitbreaker's `expected_exception` is the *inverse* of pybreaker's
`exclude` — it names the exceptions that **do** count. Same classifier tool:

```python
class OnlyConnectionError:
    def is_failure(self, *, result: object, exception: Exception | None) -> bool:
        return isinstance(exception, ConnectionError)
```

### Class-based breakers

```python
# before — circuitbreaker
from circuitbreaker import CircuitBreaker


class ApiBreaker(CircuitBreaker):
    FAILURE_THRESHOLD = 10
    RECOVERY_TIMEOUT = 60
    EXPECTED_EXCEPTION = ConnectionError
```

There is no subclassing in interlock — the same intent is a reusable `Config`
value (and a classifier), which you pass wherever you need it:

```python
# after — interlock
from interlock import Config

API_CONFIG = Config(
    minimum_number_of_calls=10,
    failure_rate_threshold=0.8,
    wait_duration_in_open=60.0,
)
```

### `fallback_function` → `FallbackStrategy`

circuitbreaker calls `fallback_function` when the circuit is open. interlock
keeps the breaker a pure gate and layers the fallback with the v2.0
[pipeline](guides/pipeline.md), so the substitution is explicit about *which*
failures it stands in for:

```python
# before — circuitbreaker
@circuit(fallback_function=lambda: [])
def recommendations(): ...
```

```python
# after — interlock
from interlock import CircuitBreaker, CircuitOpenError, Pipeline

breaker = CircuitBreaker(name='recommendations')

pipeline = (
    Pipeline.builder()
    .fallback(lambda exc: [], on=(CircuitOpenError,))
    .circuit_breaker(breaker)
    .build()
)


@pipeline
def recommendations(): ...
```

The pipeline is also where you compose retries, bulkheads and timeouts around
the same breaker — none of which the old decorator offers.

### Async and monitoring

`@circuit` handles async functions; so does interlock's `@breaker` — with the
**same instance**, no separate class or import. circuitbreaker's
`CircuitBreakerMonitor` (enumerate all circuits, check `all_closed()`) has no
direct analogue; hold your breakers in a [`Registry`](guides/configuration.md)
and iterate that, or observe transitions through the
[listener](guides/observability.md).

`CircuitBreakerError` → `CircuitOpenError`, exactly as in the
[pybreaker section](#state-inspection-and-the-open-error).

---

## aiobreaker and purgatory

[aiobreaker](https://github.com/arlyon/aiobreaker) is pybreaker ported to
asyncio — follow the [pybreaker section](#from-pybreaker); its `fail_max` /
`timeout_duration` / `CircuitBreakerListener` map the same way, and interlock's
single class removes the need for an asyncio-specific breaker at all.

[purgatory](https://github.com/mardiros/purgatory) splits sync and async into
`SyncCircuitBreakerFactory` / `AsyncCircuitBreakerFactory` with a
`default_threshold` (consecutive) and a `default_ttl` on the open state. Map the
factory + `get_breaker(name)` pattern onto a single interlock
[`Registry`](guides/configuration.md#sharing-config-with-a-registry) whose
`registry.get(name)` returns one dual sync/async breaker; `default_ttl` becomes
`wait_duration_in_open`, and `default_threshold` follows the same streak → rate
translation.

---

## What actually changes at runtime

After migrating, expect these behavioural differences — all intended:

- **Trips reflect the rate, not a streak.** A dependency that fails
  intermittently under load will now trip where a streak counter never did.
  Conversely, a single burst of failures below `minimum_number_of_calls` will
  *not* trip — the window has to fill first.
- **Slow calls can trip too, but only when you opt in.**
  `slow_call_rate_threshold` defaults to `1.0`, so latency alone never trips
  until you tune it down ([configuration](guides/configuration.md#why-slow-calls-matter)).
- **Half-open is a budgeted probe round, not a single trial call.** Up to
  `permitted_calls_in_half_open` probes run (with a concurrency cap), and the
  breaker re-decides from their rate ([states](guides/states.md)).

## Roll out incrementally

You do not have to trust new threshold numbers on day one. Ship the breaker in
[shadow mode](guides/states.md#safe-rollout) — it records real
failure and slow-call rates without rejecting anything — tune against live data,
then switch to enforcing:

```python
breaker.metrics_only()  # observe production, reject nothing
# ... read breaker.snapshot().failure_rate / .slow_call_rate over real traffic ...
breaker.reset()  # start enforcing with a clean window
```

## Next steps

- [Configuration](guides/configuration.md) — pick your thresholds and window
- [Failure classification](guides/failure-classification.md) — port `exclude` / `expected_exception`
- [Observability](guides/observability.md) — port your listeners
- [Resilience pipeline](guides/pipeline.md) — port `fallback_function`, add retries and timeouts
- [Redis integration](integrations/redis.md) — port shared state
