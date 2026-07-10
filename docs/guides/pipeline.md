# Resilience pipeline

v2.0 turns interlock's primitives into composable **strategies**: timeout,
bulkhead, circuit breaker, retry and fallback applied around one call in an
explicit order, mirroring [Polly's](https://www.pollydocs.org/pipelines)
`ResiliencePipeline` semantics.

The pipeline is an additive layer — the standalone
[`CircuitBreaker`](../getting-started.md) remains a first-class primitive, and
existing v1 code keeps working unchanged. Reach for a pipeline when one
concern is not enough.

## At a glance

```python
from interlock import CircuitBreaker, CircuitOpenError, Pipeline

breaker = CircuitBreaker(name='recommendations')

pipeline = (
    Pipeline.builder()
    .fallback(lambda exc: [], on=(CircuitOpenError,))  # outermost
    .retry(attempts=4)                                 # requires interlock-cb[tenacity]
    .circuit_breaker(breaker)
    .bulkhead(8)
    .timeout(2.0)                                      # innermost
    .build()
)


@pipeline
async def fetch_picks(user: str) -> list[str]:
    return await client.get_picks(user)
```

One pipeline serves sync and async callables alike: the decorator and
`pipeline.call(fn, ...)` detect the callable's nature and dispatch, exactly
like `CircuitBreaker.call`. The decorator preserves the wrapped signature for
type checkers.

## Order is explicit — first is outermost

Strategies apply in declaration order: the first strategy sees everything the
inner layers produce. `Pipeline(A, B)` means `A(B(call))`. There is no hidden
"correct" order in the code; the recommended one is a documented default:

| Layer (outer → inner) | Why here |
|---|---|
| `fallback(...)` | Substitutes a value for whatever the inner stack gave up on — including rejections raised by the strategies themselves |
| `retry(...)` | Each attempt below is a complete guarded call: the breaker sees honest per-attempt statistics and stops the retry loop the moment the circuit opens |
| `circuit_breaker(...)` | Counts timeouts and failures of every attempt; open circuit rejects before the bulkhead slot or a connection is touched |
| `bulkhead(...)` | Inside retry — otherwise every backoff-and-retry cycle would multiply slot occupancy |
| `timeout(...)` | Innermost: bounds a single attempt, so one hung attempt cannot eat the whole retry budget |

Deviating is legitimate — e.g. a breaker *outside* retry counts one
aggregated outcome per operation instead of one per attempt (see
[Retries and circuit breakers](retries.md) for that trade-off) — but do it
deliberately.

## The strategies

### `CircuitBreakerStrategy`

Wraps a standalone [`CircuitBreaker`](../reference.md) without touching it:
the window, events, manual controls and the breaker's own listener behave
exactly as in direct use, and the same instance can still be called directly.
An open circuit raises `CircuitOpenError` before any inner layer runs.

```python
from interlock import CircuitBreakerStrategy, Pipeline

pipeline = Pipeline(CircuitBreakerStrategy(breaker))
```

### `TimeoutStrategy`

Bounds every attempt using the v1 primitives: `asyncio.timeout` on the async
path (the attempt is cancelled), `sync_timeout` on the sync path — which
inherits its [worker-thread limitation](timeout.md): the caller gets
`CallTimeoutError` on time, but Python cannot kill the overrunning thread.

### `BulkheadStrategy`

Caps how many calls run through the layer concurrently. With no free slot the
call fails immediately (`max_wait=0`, the default) or waits up to `max_wait`
seconds, then raises `BulkheadFullError`:

```python
from interlock import BulkheadStrategy, Pipeline

pipeline = Pipeline(BulkheadStrategy(8, max_wait=0.5))
```

`BulkheadFullError` is deliberately not `CircuitOpenError`: a full bulkhead
means *this process* is saturated, not that the dependency is unhealthy — the
right reaction is shedding load, not backing off. Sync calls share a
`threading.Semaphore`, async calls an `asyncio.Semaphore`; one configuration,
two independent pools.

### `FallbackStrategy`

Substitutes an explicit value for selected failures — nothing silent:

```python
from interlock import CircuitOpenError, FallbackStrategy, Pipeline

cached: list[str] = []
strategy = FallbackStrategy(lambda exc: cached, on=(CircuitOpenError,))
```

- The substitution happens **only** for exception types named in `on`;
  anything else propagates.
- The `fallback` callable receives the exception it stands in for.
- `on` accepts `Exception` subclasses exclusively — cancellation and
  `KeyboardInterrupt` always propagate.
- The strategy's own result type is the honest union `T | F`, not `Any`. At
  the pipeline level the substitute is expected to be shaped like the call's
  result (the same contract as Polly and resilience4j).
- A fallback never masks shadow-mode statistics: a `metrics_only` breaker
  below it keeps recording every failure.

### `RetryStrategy` (the `tenacity` extra)

interlock ships no retry engine; the strategy delegates all policy to
[tenacity](https://tenacity.readthedocs.io/) and packages the
[retry × breaker glue](../integrations/tenacity.md) for the pipeline:

```python
from interlock.integrations.tenacity import RetryStrategy

strategy = RetryStrategy(attempts=4)  # fail-fast: never retries CircuitOpenError
```

Attempts are always capped, the original exception is re-raised when the
budget runs out, and the default predicate stops retrying the moment the
circuit opens. For the *patient* mode (wait exactly until the breaker's next
probe) pass `wait=wait_probe(...)` — see the
[tenacity integration](../integrations/tenacity.md). The builder step
`.retry(...)` imports the extra lazily, so the pipeline core stays
zero-dependency.

## Two usage forms, not three

A pipeline works as a decorator and as `pipeline.call(fn, *args, **kwargs)` —
the same signature-preserving contracts as the breaker's. There is
deliberately **no context manager**: a `with` block cannot be re-run, so a
retry layer inside it is semantically impossible. This is the same honesty as
the v1 breaker's context manager not supporting result-based classification —
rather than a form that silently ignores half the strategies, the form does
not exist.

## Migrating from v1 (nothing breaks)

The v1 API is untouched — migration is wrapping, not rewriting:

```python
# v1: the breaker guards the call directly
result = breaker.call(fetch_orders, user_id)

# v2: the same breaker, now composed with a timeout
pipeline = Pipeline(CircuitBreakerStrategy(breaker), TimeoutStrategy(2.0))
result = pipeline.call(fetch_orders, user_id)
```

The manual composition recipe from the
[retries guide](retries.md) — `Retrying` wrapped around `breaker.call` —
keeps working and remains the most flexible form; the pipeline is that recipe
made declarative.

## Observability

`RetryStrategy`, `BulkheadStrategy` and `FallbackStrategy` (and their builder
steps) accept `name=` and `listener=`. Three optional
[`EventListener`](observability.md) hooks make the pipeline's decisions
visible — `on_retry(name, attempt, delay)`, `on_bulkhead_rejected(name)` and
`on_fallback(name, error)`:

```python
from interlock import LoggingEventListener, Pipeline

events = LoggingEventListener()

pipeline = (
    Pipeline.builder()
    .fallback(lambda exc: [], on=(CircuitOpenError,), name='recs', listener=events)
    .circuit_breaker(breaker)  # the breaker keeps its own listener
    .bulkhead(8, name='recs', listener=events)
    .timeout(2.0)
    .build()
)
```

The hooks are dispatched via safe `getattr` — listeners written before v2.0
keep working unchanged. `LoggingEventListener` logs retries at INFO and
bulkhead rejections / fallbacks at WARNING; `OTelEventListener` counts all
three in the `interlock.pipeline.events` counter.

## Custom strategies

Any object with `execute` / `execute_async` is a strategy — the `Strategy`
protocol is structural:

```python
import time
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar('T')


class Measuring:
    """Times every layer below it."""

    def execute(self, call: Callable[[], T]) -> T:
        start = time.perf_counter()
        try:
            return call()
        finally:
            print(f'took {time.perf_counter() - start:.3f}s')

    async def execute_async(self, call: Callable[[], Awaitable[T]]) -> T:
        start = time.perf_counter()
        try:
            return await call()
        finally:
            print(f'took {time.perf_counter() - start:.3f}s')


pipeline = Pipeline.builder().add(Measuring()).timeout(2.0).build()
```

The contract, in full:

- Run the zero-argument next layer, return its result, let exceptions
  propagate. Never swallow `BaseException` — cancellation must cross every
  layer untouched.
- `execute_async` always receives a real coroutine function, so
  detect-dispatching primitives (like `breaker.call`) treat it as async.
