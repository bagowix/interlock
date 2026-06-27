# Getting started

## Install

```bash
uv add interlock-cb          # or: pip install interlock-cb
```

The core is pure standard library. External integrations are optional extras:

```bash
uv add 'interlock-cb[otel]'    # OpenTelemetry metrics listener
uv add 'interlock-cb[httpx2]'  # per-host httpx2 transport
```

## Create a breaker

A breaker is named and configured once, then reused:

```python
from interlock import CircuitBreaker, Config

breaker = CircuitBreaker(
    name='payments',
    config=Config(failure_rate_threshold=0.5, minimum_number_of_calls=20),
)
```

The defaults follow resilience4j: trip at a 50% failure rate over at least 10
calls, stay open for 60s before allowing a single probe. See
[Configuration](guides/configuration.md) for every option.

## Three ways to protect work

All three run over the same `call()` primitive.

### Decorator

```python
@breaker
def charge(amount: int) -> str:
    return gateway.charge(amount)
```

The decorator preserves the wrapped signature and its sync/async nature — type
checkers still see `charge` as `(int) -> str`.

### `breaker.call`

```python
result = breaker.call(gateway.charge, 100)
```

### Context manager

```python
with breaker:
    gateway.charge(100)
```

!!! note "Contract difference"
    The decorator and `call` see a callable, so result-based classification and
    slow-call detection both apply. The context manager sees only the block —
    its exception and duration — so classification by **return value** is not
    available there. Need result-based classification? Use the decorator or
    `call`.

## Async

The same instance handles async. The decorator and `call` detect a coroutine
function; the instance is also an async context manager:

```python
@breaker
async def fetch(url: str) -> bytes:
    return await client.get(url)

result = await breaker.call(client.get, url)

async with breaker:
    await client.get(url)
```

## Handle rejections

When the circuit is not closed, the call is rejected with `CircuitOpenError`:

```python
from interlock import CircuitOpenError

try:
    breaker.call(gateway.charge, 100)
except CircuitOpenError as exc:
    # exc.breaker_name, exc.retry_after (seconds, may be None), exc.last_failure
    raise
```

## Inspect state

```python
breaker.state            # State.CLOSED / OPEN / HALF_OPEN / ...
breaker.snapshot()       # WindowSnapshot: total_calls, failed_calls, slow_calls,
                         # .failure_rate, .slow_call_rate
```

## Next steps

- [Configuration](guides/configuration.md)
- [States & manual control](guides/states.md)
- [Failure classification](guides/failure-classification.md)
- [Observability](guides/observability.md)
- [httpx2 integration](integrations/httpx2.md)
