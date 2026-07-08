"""aiohttp client integration — requires the ``aiohttp`` extra.

Pass ``CircuitBreakerMiddleware`` to a session and every request it sends is
guarded by a circuit breaker **per host** — no decorators in call sites::

    import aiohttp
    from interlock.integrations.aiohttp import CircuitBreakerMiddleware

    middleware = CircuitBreakerMiddleware()
    async with aiohttp.ClientSession(middlewares=(middleware,)) as session:
        async with session.get('https://api.example.com/') as response:
            ...

Client middlewares require aiohttp >= 3.12. Each host gets its own breaker
(a failing ``api.a`` must not trip ``api.b``), created lazily and shared
across requests. When a host's circuit is open the request is rejected with
``CircuitOpenError`` before a connection is made.

By default a response counts as a failure when its status is in the canonical
retryable set (``429, 500, 502, 503, 504``) and any exception raised by the
handler (connect/read errors) is a failure; ``4xx`` client mistakes like
``404`` are successes. Supply a custom ``classifier`` to change that policy.

The breaker observes the time to *response headers*; reading the body happens
outside the guarded call — the same semantics as the httpx2 transport.
"""

import http
from collections.abc import Iterable
from typing import cast

from aiohttp import ClientHandlerType, ClientRequest, ClientResponse

from interlock.config import Config
from interlock.protocols import Clock, EventListener, FailureClassifier
from interlock.registry import Registry

__all__ = ('CircuitBreakerMiddleware', 'HttpStatusClassifier')

# Mirrors urllib3's recommended ``status_forcelist`` (also used by AWS/Google):
# transient server-side conditions where the dependency is unhealthy or
# overloaded. Permanent 5xx (501 Not Implemented, 505) are excluded — tripping
# the breaker cannot help a contract/protocol error.
_FAILURE_STATUSES = frozenset(
    {
        http.HTTPStatus.TOO_MANY_REQUESTS,  # 429
        http.HTTPStatus.INTERNAL_SERVER_ERROR,  # 500
        http.HTTPStatus.BAD_GATEWAY,  # 502
        http.HTTPStatus.SERVICE_UNAVAILABLE,  # 503
        http.HTTPStatus.GATEWAY_TIMEOUT,  # 504
    }
)


class HttpStatusClassifier:
    """Counts handler exceptions and unhealthy-status responses as failures.

    A returned response is a failure when its status is in ``failure_statuses``
    — by default the canonical retryable set (``429, 500, 502, 503, 504``);
    any raised exception is a failure. Other responses — including ``4xx``
    client mistakes like ``404`` — are successes, so they never trip the
    breaker.

    Args:
        failure_statuses: Statuses to count as failures instead of the
            canonical set.
    """

    def __init__(self, *, failure_statuses: Iterable[int] | None = None) -> None:
        self._failure_statuses = (
            frozenset(failure_statuses) if failure_statuses is not None else _FAILURE_STATUSES
        )

    def is_failure(self, *, result: object, exception: BaseException | None) -> bool:
        """Return whether a completed request counts as a failure."""
        if exception is not None:
            return True

        return cast('ClientResponse', result).status in self._failure_statuses


class CircuitBreakerMiddleware:
    """A client middleware that guards each host with a circuit breaker.

    Args:
        config: Thresholds, window and timing for every host's breaker.
        clock: Time source for the breakers; inject a fake for deterministic
            tests.
        classifier: Failure policy. Defaults to ``HttpStatusClassifier``.
        listener: Observability hooks shared by every host's breaker.
    """

    def __init__(
        self,
        *,
        config: Config | None = None,
        clock: Clock | None = None,
        classifier: FailureClassifier | None = None,
        listener: EventListener | None = None,
    ) -> None:
        self._registry = Registry(
            config=config,
            clock=clock,
            classifier=classifier if classifier is not None else HttpStatusClassifier(),
            listener=listener,
        )

    async def __call__(self, request: ClientRequest, handler: ClientHandlerType) -> ClientResponse:
        """Run the request under its host's breaker.

        Raises:
            CircuitOpenError: If the host's breaker is open.
            ValueError: If the request URL carries no host to key a breaker on.
        """
        host = request.url.host
        if not host:
            raise ValueError(f'Request URL has no host to key a breaker on: {request.url!s}')

        breaker = self._registry.get(host)

        # The composed handler is not guaranteed to be a coroutine *function*
        # (middleware chains may hand over plain callables returning
        # awaitables), so don't rely on the breaker's sync/async detection —
        # wrap the send in an explicit coroutine function.
        async def _send() -> ClientResponse:
            return await handler(request)

        guarded = breaker(_send)
        return await guarded()
