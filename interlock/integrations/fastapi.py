"""FastAPI integration — requires the ``fastapi`` extra.

This module imports ``fastapi`` and is deliberately *not* re-exported from
``interlock`` so the core stays zero-dependency. Install with
``pip install interlock[fastapi]``.

Protect a route's outgoing dependency with a shared ``Registry`` and let a
breaker that has tripped surface as ``503 Service Unavailable`` with a
``Retry-After`` header::

    from typing import Annotated

    from fastapi import Depends, FastAPI
    from interlock import CircuitBreaker, Registry
    from interlock.integrations.fastapi import breaker_dependency, install_exception_handler

    app = FastAPI()
    registry = Registry()
    install_exception_handler(app)

    orders_db = breaker_dependency('orders-db', registry=registry)

    @app.get('/orders')
    async def orders(breaker: Annotated[CircuitBreaker, Depends(orders_db)]):
        return await breaker.call(fetch_orders)

When ``fetch_orders`` fails often enough the breaker opens; the next request
raises ``CircuitOpenError``, which the installed handler turns into a ``503``
carrying ``Retry-After`` derived from the breaker's ``retry_after`` estimate.
"""

import http
import math
from collections.abc import Callable
from typing import cast

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from interlock.breaker import CircuitBreaker
from interlock.errors import CircuitOpenError
from interlock.registry import Registry

__all__ = ('breaker_dependency', 'circuit_open_handler', 'install_exception_handler')

_ExceptionHandler = Callable[[Request, Exception], Response]


def breaker_dependency(name: str, *, registry: Registry) -> Callable[[], CircuitBreaker]:
    """Build a FastAPI dependency that yields the named breaker from ``registry``.

    Use it with ``Depends`` so a route receives a shared, lazily-created breaker:
    ``Depends(breaker_dependency('orders-db', registry=registry))``.
    """

    def dependency() -> CircuitBreaker:
        return registry.get(name)

    return dependency


def circuit_open_handler(request: Request, exc: CircuitOpenError) -> Response:
    """Map a ``CircuitOpenError`` to ``503`` with a ``Retry-After`` header.

    The header is the breaker's ``retry_after`` estimate rounded up to whole
    seconds (per RFC 7231 ``delay-seconds``); it is omitted when the breaker
    cannot estimate one (for example ``FORCED_OPEN``).
    """
    del request  # required by the handler signature, unused here

    headers: dict[str, str] = {}
    if exc.retry_after is not None:
        headers['Retry-After'] = str(math.ceil(exc.retry_after))

    return JSONResponse(
        status_code=http.HTTPStatus.SERVICE_UNAVAILABLE,
        content={'detail': f'Circuit {exc.breaker_name!r} is open'},
        headers=headers,
    )


def install_exception_handler(app: FastAPI) -> None:
    """Register :func:`circuit_open_handler` for ``CircuitOpenError`` on ``app``."""
    app.add_exception_handler(CircuitOpenError, cast('_ExceptionHandler', circuit_open_handler))
