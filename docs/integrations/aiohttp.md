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
request raises `CircuitOpenClientError` *before* a connection is made.

The breaker observes the time to *response headers*; reading the body happens
outside the guarded call — the same semantics as the
[httpx2 transport](httpx2.md).

## What a rejection looks like

An open circuit rejects the request with `CircuitOpenClientError`, which is
both an `aiohttp.ClientConnectionError` and interlock's `CircuitOpenError`:

```python
import aiohttp

from interlock.integrations.aiohttp import CircuitOpenClientError

try:
    response = await session.get('https://api.example.com/v1/users')
except aiohttp.ClientError as exc:
    # The dependency being unreachable and the breaker rejecting both land here.
    if isinstance(exc, CircuitOpenClientError):
        ...  # rejected before any I/O; the next probe is exc.retry_after away
    raise
```

That is the point of the type: the degradation paths an application already
writes in aiohttp's own idiom keep working the day a breaker leaves shadow
mode. The rejection is still a `CircuitOpenError` too, so `except
CircuitOpenError`, a `FallbackStrategy(on=(CircuitOpenError,))` or a framework
exception handler registered for it catch exactly what they caught before. It
carries `breaker_name`, `retry_after` and `last_failure`. aiohttp re-raises a
`ClientError` from the middleware chain untouched, so it reaches the caller
exactly as raised.

The base type is deliberately the broad `ClientConnectionError` and never a
leaf such as `ClientOSError` or `ServerDisconnectedError`: nothing was
connected and no server dropped anything, and those leaves are exactly what
retry predicates key on — a retried rejection burns an attempt against a
circuit that is still open. It also stays outside `ClientResponseError`, which
would claim a response that never arrived.

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
must return a non-empty string containing something other than whitespace;
invalid results raise `ValueError` with the request URL before the handler
performs I/O.

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

Share such a registry with HTTP sessions only. `HttpStatusClassifier` reads
`.status` off every result it records, so a breaker taken from the same
registry for non-HTTP work — `registry.get('db')` — raises `AttributeError` the
first time that call returns. Keep a separate registry for those.

Calling `aclose()` automatically closes the breakers only when the middleware
owns the registry. An injected registry remains open until the application
explicitly calls `await registry.aclose_all()` during shutdown.

## Safe production rollout

Pass `initial_state=State.METRICS_ONLY` to record real outcomes without
rejecting requests. The state is applied to every lazily created host before
its first request:

```python
from interlock import LoggingEventListener, State
from interlock.integrations.aiohttp import CircuitBreakerMiddleware

middleware = CircuitBreakerMiddleware(
    initial_state=State.METRICS_ONLY,
    listener=LoggingEventListener(),
)
```

The public `middleware.registry` supports local diagnosis with
`get_existing(host)`, `state` and `snapshot()`, plus `names()` and `items()` for
the breakers created so far — point-in-time copies, without the ones created
afterwards. `LoggingEventListener` writes every event through stdlib logging;
swap it for an `EventListener` that exports to your metrics backend, then
deploy a new middleware with the default `CLOSED` state to begin enforcement.
See [Safe rollout](../guides/states.md#safe-rollout).

## Failure policy

By default a response counts as a failure when its status is in the canonical
retryable set (`429, 500, 502, 503, 504`) and any exception raised while
sending (connect/read errors) is a failure; `4xx` client mistakes like `404`
are successes.

Nothing is excluded by default: aiohttp rejects a malformed or non-HTTP URL
before the middleware chain runs, so no caller-side error of its own reaches
the classifier. Middlewares of your own that sit inside this one are the
exception — an auth middleware refusing to sign a request is your bug, not the
dependency's, so exclude what it raises with
`excluded_exceptions=(MissingCredentials,)`. An excluded exception is recorded
as a *success*, since the sliding window has no third outcome, and still
propagates to the caller.

Change the statuses, or the whole policy:

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
