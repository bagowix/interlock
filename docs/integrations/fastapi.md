# FastAPI

The `interlock-cb[fastapi]` extra protects a route's outgoing dependency with a
shared `Registry` and turns a tripped breaker into a clean
`503 Service Unavailable` response with a `Retry-After` header.

=== "uv"

    ```bash
    uv add 'interlock-cb[fastapi]'
    ```

=== "pip"

    ```bash
    pip install 'interlock-cb[fastapi]'
    ```

=== "poetry"

    ```bash
    poetry add 'interlock-cb[fastapi]'
    ```

## Usage

Install the exception handler once, then inject a per-name breaker into any route
with `Depends`:

```python
from typing import Annotated

from fastapi import Depends, FastAPI
from interlock import CircuitBreaker, Registry
from interlock.integrations.fastapi import breaker_dependency, install_exception_handler

app = FastAPI()
registry = Registry()
install_exception_handler(app)

orders_db = breaker_dependency('orders-db', registry=registry)


@app.get('/orders')
async def orders(breaker: Annotated[CircuitBreaker, Depends(orders_db)]) -> list[dict]:
    return await breaker.call(fetch_orders)
```

When `fetch_orders` fails often enough, the breaker opens. The next request is
rejected with `CircuitOpenError` *before* `fetch_orders` runs, and the installed
handler converts it into:

```http
HTTP/1.1 503 Service Unavailable
Retry-After: 30
Content-Type: application/json

{"detail": "Circuit 'orders-db' is open"}
```

## How it works

- **`breaker_dependency(name, *, registry)`** returns a FastAPI dependency that
  yields the named breaker from the shared `Registry`. The breaker is created
  lazily on first use and reused on every later request, so all requests to that
  route share one breaker (and one view of the dependency's health).
- **`install_exception_handler(app)`** registers a handler for
  `CircuitOpenError`. It responds `503` and sets `Retry-After` to the breaker's
  `retry_after` estimate, rounded up to whole seconds (per RFC 7231). The header
  is omitted when there is no estimate (for example after `force_open()`).

You protect the *outgoing* call (`breaker.call(...)`) rather than the route
itself: only the dependency you wrap counts toward the breaker, and the breaker's
own admission logic (probes, half-open) keeps working.

## Sharing breakers across routes

Reuse the same `name` (and the same `registry`) to share one breaker across
several routes that all depend on the same downstream:

```python
orders_db = breaker_dependency('orders-db', registry=registry)


@app.get('/orders')
async def list_orders(breaker: Annotated[CircuitBreaker, Depends(orders_db)]) -> list[dict]:
    return await breaker.call(fetch_orders)


@app.get('/orders/{order_id}')
async def get_order(
    order_id: int, breaker: Annotated[CircuitBreaker, Depends(orders_db)]
) -> dict:
    return await breaker.call(fetch_order, order_id)
```

Pass `config`, `clock`, `classifier` or `listener` to the `Registry` to tune
every breaker it creates, or override per name via `registry.get(name, config=...)`.

## Custom responses

For a different response shape, register your own handler instead of
`install_exception_handler`:

```python
from fastapi import Request, Response
from interlock import CircuitOpenError


@app.exception_handler(CircuitOpenError)
async def on_open(request: Request, exc: CircuitOpenError) -> Response:
    ...
```
