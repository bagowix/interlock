# aiohttp

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

await middleware.aclose()
```

`ClientSession` does not own middleware resources. Call `middleware.aclose()`
during application shutdown; it releases every per-host breaker and is
idempotent.

Each host gets its own breaker (a failing `api.a` never trips `api.b`),
created lazily and shared across requests. When a host's circuit is open the
request raises [`CircuitOpenError`](../reference.md) *before* a connection is
made.

The breaker observes the time to *response headers*; reading the body happens
outside the guarded call — the same semantics as the
[httpx2 transport](httpx2.md).

## Custom breaker keys

Pass `name_resolver` when host-based isolation does not match the logical
dependencies. The callback receives the native `aiohttp.ClientRequest` and
returns the breaker name:

```python
from interlock.integrations.aiohttp import CircuitBreakerMiddleware

middleware = CircuitBreakerMiddleware(
    name_resolver=lambda request: request.url.host.removesuffix('.query.consul'),
)
```

A resolver can collapse several discovery hosts onto one breaker or derive a
name from the request path to separate upstreams behind a shared gateway. It
must return a non-empty, non-whitespace name; invalid names raise `ValueError`
with the request URL before the handler performs I/O.

The resolved name is used by the registry, `CircuitOpenError`, and every
listener event. Resolve it in the middleware rather than rewriting listener
labels so observed names always match the breaker whose state they describe.

## Share one registry across sessions

Several middleware instances can share one caller-owned registry, so traffic
resolving to the same name contributes to one breaker and one sliding window:

```python
from interlock import Config, Registry
from interlock.integrations.aiohttp import CircuitBreakerMiddleware, HttpStatusClassifier

registry = Registry(
    config=Config(failure_rate_threshold=0.25, minimum_number_of_calls=50),
    classifier=HttpStatusClassifier(),
)

middleware_a = CircuitBreakerMiddleware(registry=registry)
middleware_b = CircuitBreakerMiddleware(registry=registry)
```

Do not omit the classifier when HTTP statuses should affect the breaker. A
bare `Registry` uses exception-only classification, so a returned `503` counts
as a success. The registry owns `config`, `clock`, `initial_state`,
`classifier`, and `listener`; passing any of them to the middleware together
with `registry` raises `ValueError`.

Calling `aclose()` automatically closes the breakers only when the middleware
owns the registry. An injected registry remains open until the application
explicitly calls `await registry.aclose_all()` during shutdown.

## Safe production rollout

Pass `initial_state=State.METRICS_ONLY` to record real outcomes without
rejecting requests. The state is applied to every lazily created host before
its first request:

```python
from interlock import State

middleware = CircuitBreakerMiddleware(
    initial_state=State.METRICS_ONLY,
    listener=metrics_listener,
)
```

The public `middleware.registry` supports local diagnosis with
`get_existing(host)`, `state` and `snapshot()`. Use an `EventListener` for
production metrics, then deploy a new middleware with the default `CLOSED`
state to begin enforcement. See [Safe rollout](../guides/states.md#safe-rollout).

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
`clock`, `initial_state`, `classifier`, `listener`. One middleware instance
holds one registry of resolved breakers; reuse the instance across sessions to
share breaker state, or create separate instances to isolate them. For
application-level retries combine with the [tenacity integration](tenacity.md)
and read [Retries and circuit breakers](../guides/retries.md) first.
