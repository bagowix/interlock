"""Litestar integration — requires the ``litestar`` extra.

This module imports ``litestar`` and is deliberately *not* re-exported from
``interlock`` so the core stays zero-dependency. Install with
``pip install 'interlock-cb[litestar]'``.

Protect a route's outgoing dependency with a shared ``Registry`` and let a
breaker that has tripped surface as ``503 Service Unavailable`` with a
``Retry-After`` header. Unlike FastAPI, Litestar wires exception handlers at
construction time, so there is no ``install_exception_handler`` — pass the
handler to the app directly::

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

When ``fetch_orders`` fails often enough the breaker opens; the next request
raises ``CircuitOpenError``, which the handler turns into a ``503`` carrying
``Retry-After`` derived from the breaker's ``retry_after`` estimate.
"""

import math
from typing import TYPE_CHECKING

from litestar import Request, Response
from litestar.di import Provide
from litestar.status_codes import HTTP_503_SERVICE_UNAVAILABLE

if TYPE_CHECKING:
    from litestar.datastructures import State

from interlock.breaker import CircuitBreaker
from interlock.errors import CircuitOpenError
from interlock.registry import Registry

__all__ = ('breaker_dependency', 'circuit_open_handler')


def breaker_dependency(name: str, *, registry: Registry) -> Provide:
    """Build a Litestar ``Provide`` that injects the named breaker from ``registry``.

    Declare it under the kwarg name your handlers use, at any layer::

        Litestar(dependencies={'breaker': breaker_dependency('orders-db', registry=registry)})
    """

    def resolve() -> CircuitBreaker:
        return registry.get(name)

    return Provide(resolve, sync_to_thread=False)


def circuit_open_handler(
    request: 'Request[object, object, State]', exc: CircuitOpenError
) -> Response[dict[str, str]]:
    """Map a ``CircuitOpenError`` to ``503`` with a ``Retry-After`` header.

    The header is the breaker's ``retry_after`` estimate rounded up to whole
    seconds (per RFC 7231 ``delay-seconds``); it is omitted when the breaker
    cannot estimate one (for example ``FORCED_OPEN``).
    """
    del request  # required by the handler signature, unused here

    headers: dict[str, str] = {}
    if exc.retry_after is not None:
        headers['Retry-After'] = str(math.ceil(exc.retry_after))

    return Response(
        {'detail': f'Circuit {exc.breaker_name!r} is open'},
        status_code=HTTP_503_SERVICE_UNAVAILABLE,
        headers=headers,
    )
