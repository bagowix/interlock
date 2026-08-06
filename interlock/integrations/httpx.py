"""httpx transport integration — requires the ``httpx`` extra.

This module imports ``httpx`` and is deliberately not re-exported from
``interlock`` so the core stays zero-dependency. Install with
``pip install interlock-cb[httpx]`` and wrap a transport explicitly::

    import httpx
    from interlock.integrations.httpx import CircuitBreakerTransport

    transport = CircuitBreakerTransport(httpx.HTTPTransport())
    client = httpx.Client(transport=transport)

The wrapper applies one circuit breaker per host. Responses are returned
unchanged, preserving httpx's streaming semantics.
"""

import http
from collections.abc import Iterable
from typing import cast

from httpx import AsyncBaseTransport, BaseTransport, Request, Response

from interlock.config import Config
from interlock.protocols import Clock, EventListener, FailureClassifier
from interlock.registry import Registry

__all__ = (
    'AsyncCircuitBreakerTransport',
    'CircuitBreakerTransport',
    'HttpStatusClassifier',
)

_FAILURE_STATUSES = frozenset(
    {
        http.HTTPStatus.TOO_MANY_REQUESTS,
        http.HTTPStatus.INTERNAL_SERVER_ERROR,
        http.HTTPStatus.BAD_GATEWAY,
        http.HTTPStatus.SERVICE_UNAVAILABLE,
        http.HTTPStatus.GATEWAY_TIMEOUT,
    }
)


class HttpStatusClassifier:
    """Count transport exceptions and unhealthy-status responses as failures.

    Args:
        failure_statuses: Statuses to count as failures instead of the
            canonical set ``429, 500, 502, 503, 504``.
    """

    def __init__(self, *, failure_statuses: Iterable[int] | None = None) -> None:
        self._failure_statuses = (
            frozenset(failure_statuses) if failure_statuses is not None else _FAILURE_STATUSES
        )

    def is_failure(self, *, result: object, exception: BaseException | None) -> bool:
        """Return whether a completed request counts as a failure."""
        if exception is not None:
            return True

        return cast('Response', result).status_code in self._failure_statuses


def _host(request: Request) -> str:
    """Return the host key, rejecting URLs that cannot identify a dependency."""
    host = request.url.host
    if not host:
        raise ValueError(f'Request URL has no host to key a breaker on: {request.url!s}')

    return host


def _build_registry(
    config: Config | None,
    clock: Clock | None,
    classifier: FailureClassifier | None,
    listener: EventListener | None,
) -> Registry:
    return Registry(
        config=config,
        clock=clock,
        classifier=classifier if classifier is not None else HttpStatusClassifier(),
        listener=listener,
    )


class CircuitBreakerTransport(BaseTransport):
    """Guard every host reached by a synchronous httpx transport.

    Args:
        transport: Wrapped transport that performs requests.
        config: Thresholds, window and timing for every host's breaker.
        clock: Time source for the breakers.
        classifier: Failure policy. Defaults to ``HttpStatusClassifier``.
        listener: Observability hooks shared by every host's breaker.
    """

    def __init__(
        self,
        transport: BaseTransport,
        *,
        config: Config | None = None,
        clock: Clock | None = None,
        classifier: FailureClassifier | None = None,
        listener: EventListener | None = None,
    ) -> None:
        self._transport = transport
        self._registry = _build_registry(
            config=config,
            clock=clock,
            classifier=classifier,
            listener=listener,
        )

    def handle_request(self, request: Request) -> Response:
        """Run a request under the breaker for its host.

        Raises:
            CircuitOpenError: If the host's breaker is open.
            ValueError: If the request URL has no host.
        """
        breaker = self._registry.get(_host(request))
        guarded = breaker(self._transport.handle_request)
        return guarded(request)

    def close(self) -> None:
        """Close the wrapped transport and its connection pool."""
        self._transport.close()


class AsyncCircuitBreakerTransport(AsyncBaseTransport):
    """Guard every host reached by an asynchronous httpx transport.

    Args:
        transport: Wrapped async transport that performs requests.
        config: Thresholds, window and timing for every host's breaker.
        clock: Time source for the breakers.
        classifier: Failure policy. Defaults to ``HttpStatusClassifier``.
        listener: Observability hooks shared by every host's breaker.
    """

    def __init__(
        self,
        transport: AsyncBaseTransport,
        *,
        config: Config | None = None,
        clock: Clock | None = None,
        classifier: FailureClassifier | None = None,
        listener: EventListener | None = None,
    ) -> None:
        self._transport = transport
        self._registry = _build_registry(
            config=config,
            clock=clock,
            classifier=classifier,
            listener=listener,
        )

    async def handle_async_request(self, request: Request) -> Response:
        """Run a request under the breaker for its host.

        Raises:
            CircuitOpenError: If the host's breaker is open.
            ValueError: If the request URL has no host.
        """
        breaker = self._registry.get(_host(request))
        guarded = breaker(self._transport.handle_async_request)
        return await guarded(request)

    async def aclose(self) -> None:
        """Close the wrapped transport and its connection pool."""
        await self._transport.aclose()
