# aiohttp integration

The `interlock-cb[aiohttp]` extra guards every request a `ClientSession`
sends with a circuit breaker **per host**, wired in as a client middleware —
no decorators in call sites. Requires aiohttp ≥ 3.12 (client middlewares).

=== "uv"

    ```bash
    uv add 'interlock-cb[aiohttp]'
    ```

=== "pip"

    ```bash
    pip install 'interlock-cb[aiohttp]'
    ```

=== "poetry"

    ```bash
    poetry add 'interlock-cb[aiohttp]'
    ```

## Usage

```python
import aiohttp

from interlock.integrations.aiohttp import CircuitBreakerMiddleware

middleware = CircuitBreakerMiddleware()

async with aiohttp.ClientSession(middlewares=(middleware,)) as session:
    async with session.get('https://api.example.com/orders') as response:
        orders = await response.json()
```

Each host gets its own breaker (a failing `api.a` never trips `api.b`),
created lazily and shared across requests. When a host's circuit is open the
request raises [`CircuitOpenError`](../reference.md) *before* a connection is
made.

The breaker observes the time to *response headers*; reading the body happens
outside the guarded call — the same semantics as the
[httpx2 transport](httpx2.md).

## Failure policy

By default a response counts as a failure when its status is in the canonical
retryable set (`429, 500, 502, 503, 504`) and any exception raised while
sending (connect/read errors) is a failure; `4xx` client mistakes like `404`
are successes. Change the statuses, or the whole policy:

```python
from interlock import Config
from interlock.integrations.aiohttp import CircuitBreakerMiddleware, HttpStatusClassifier

middleware = CircuitBreakerMiddleware(
    config=Config(failure_rate_threshold=0.3),
    classifier=HttpStatusClassifier(failure_statuses={408, 429, 500, 502, 503, 504}),
)
```

Any custom `FailureClassifier` works too — see
[Failure classification](../guides/failure-classification.md).

## Tuning and observability

The middleware accepts the same collaborators as `CircuitBreaker` — `config`,
`clock`, `classifier`, `listener`. One middleware instance holds one registry
of per-host breakers; reuse the instance across sessions to share breaker
state, or create separate instances to isolate them. For application-level
retries combine with the [tenacity integration](tenacity.md) and read
[Retries and circuit breakers](../guides/retries.md) first.
