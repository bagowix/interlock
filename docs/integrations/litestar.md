# Litestar

The `interlock-cb[litestar]` extra (Litestar ≥ 2.23) protects a route's
outgoing dependency with a shared `Registry` and turns a tripped breaker into
a clean `503 Service Unavailable` response with a `Retry-After` header.

=== "uv"

    ```bash
    uv add 'interlock-cb[litestar]'
    ```

=== "pip"

    ```bash
    pip install 'interlock-cb[litestar]'
    ```

=== "poetry"

    ```bash
    poetry add 'interlock-cb[litestar]'
    ```

## Usage

Litestar wires exception handlers and dependencies at construction time —
declare both on the app (or a router / controller) and annotate the handler
parameter with `NamedDependency`:

```python
from litestar import Litestar, get
from litestar.di import NamedDependency

from interlock import CircuitBreaker, CircuitOpenError, Registry
from interlock.integrations.litestar import breaker_dependency, circuit_open_handler

registry = Registry()


@get('/orders')
async def orders(breaker: NamedDependency[CircuitBreaker]) -> list[dict]:
    return await breaker.call(fetch_orders)


app = Litestar(
    route_handlers=[orders],
    dependencies={'breaker': breaker_dependency('orders-db', registry=registry)},
    exception_handlers={CircuitOpenError: circuit_open_handler},
)
```

When `fetch_orders` fails often enough, the breaker opens. The next request is
rejected with `CircuitOpenError` *before* `fetch_orders` runs, and the handler
converts it into:

```http
HTTP/1.1 503 Service Unavailable
Retry-After: 30
Content-Type: application/json

{"detail": "Circuit 'orders-db' is open"}
```

## How it works

- **`breaker_dependency(name, *, registry)`** returns a Litestar
  [`Provide`](https://docs.litestar.dev/latest/usage/dependency-injection.html)
  that yields the named breaker from the shared `Registry`. The breaker is
  created lazily on first use and reused on every later request, so all
  requests sharing the dependency share one breaker (and one view of the
  downstream's health). Declare it at any layer — app, router, controller or
  handler.
- **`circuit_open_handler`** maps `CircuitOpenError` to `503` and sets
  `Retry-After` to the breaker's `retry_after` estimate, rounded up to whole
  seconds (per RFC 7231). The header is omitted when there is no estimate
  (for example after `force_open()`).

You protect the *outgoing* call (`breaker.call(...)`) rather than the route
itself: only the dependency you wrap counts toward the breaker, and the
breaker's own admission logic (probes, half-open) keeps working.

## Sharing breakers across routes

Reuse the same `name` (and the same `registry`) wherever routes depend on the
same downstream — one `dependencies={...}` declaration on the app covers them
all, and every route sees the same circuit state.

Pass `config`, `clock`, `classifier` or `listener` to the `Registry` to tune
every breaker it creates, or override per name via
`registry.get(name, config=...)`.

## Custom responses

For a different response shape, register your own handler instead:

```python
from litestar import Request, Response

from interlock import CircuitOpenError


def on_open(request: Request, exc: CircuitOpenError) -> Response[dict[str, str]]:
    ...


app = Litestar(..., exception_handlers={CircuitOpenError: on_open})
```
