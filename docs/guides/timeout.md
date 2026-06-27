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

Compose `timeout` with a breaker manually — pipeline composition is a v2
feature. Put the timeout *inside* the protected callable so the breaker observes
the `CallTimeoutError`:

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

## Synchronous code

`timeout` relies on asyncio cancelling the coroutine in place, which has no
synchronous equivalent: a blocking call cannot be interrupted from outside its
own thread, and `signal.SIGALRM` only works in the main thread, so it breaks in
threaded servers. `sync_timeout` instead runs the callable in a daemon worker
thread and joins it with a deadline. It is a decorator, so it wraps a *callable*
rather than a block:

```python
from interlock import CircuitBreaker, sync_timeout

breaker = CircuitBreaker(name='search')

@breaker
@sync_timeout(2.0)
def search(q: str) -> bytes:
    return client.get('/search', params={'q': q}).content
```

A call that exceeds 2 seconds raises `CallTimeoutError`, which the breaker
records exactly as with the async path. The decorator preserves the wrapped
function's signature, arguments and return value.

!!! warning "The worker keeps running after a timeout"
    Python cannot forcibly kill a thread. After `sync_timeout` raises, the
    worker thread keeps running in the background until the call returns on its
    own — it cannot be cancelled, so it may still hold the resource it was
    waiting on. The caller is unblocked immediately, but the underlying work is
    not stopped. Prefer the async `timeout` wherever you control an event loop;
    reach for `sync_timeout` only in genuinely synchronous code.

## Why not bake it in?

interlock keeps retry, fallback and timeout as explicit, observable features
rather than hidden magic inside the breaker. You decide the deadline at the call
site, and the failure it produces flows through the same classification and
metrics as any other.
