# Timeout

A circuit breaker without a timeout is unsafe. A call that hangs forever is
never counted as slow or failed — it just holds a resource indefinitely.
`timeout` bounds an awaited block and turns a hang into a `CallTimeoutError`,
which a surrounding breaker records as a (slow) failure.

```python
from interlock import timeout

async with timeout(2.0):
    await client.get(url)        # raises CallTimeoutError after 2 seconds
```

## Composing with a breaker

`timeout` is async-only by design (a sync timeout is planned for v1.1). Compose
it with a breaker manually — pipeline composition is a v2 feature. Put the
timeout *inside* the protected callable so the breaker observes the
`CallTimeoutError`:

```python
from interlock import CircuitBreaker, timeout

breaker = CircuitBreaker(name='search')

@breaker
async def search(q: str) -> bytes:
    async with timeout(2.0):
        return await client.get('/search', params={'q': q})
```

Now a request that exceeds 2 seconds raises `CallTimeoutError`; the breaker
counts it as a failure and, once the failure rate crosses the threshold, opens
the circuit — converting slow hangs into fast rejections.

## Why not bake it in?

interlock keeps retry, fallback and timeout as explicit, observable features
rather than hidden magic inside the breaker. You decide the deadline at the call
site, and the failure it produces flows through the same classification and
metrics as any other.
