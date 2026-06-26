"""httpx2 transport integration — requires the ``httpx2`` extra.

This module imports ``httpx2`` and is deliberately *not* re-exported from
``interlock`` so the core stays zero-dependency. Install with
``pip install interlock[httpx2]`` and wrap your transport explicitly::

    import httpx2
    from interlock.httpx2 import CircuitBreakerTransport

    transport = CircuitBreakerTransport(httpx2.HTTPTransport())
    client = httpx2.Client(transport=transport)

The wrapper applies one circuit breaker **per host** transparently: no
decorators in user code. Each host gets its own breaker (a slow or failing
``api.a`` must not trip ``api.b``), created lazily and shared across requests.

By default a response counts as a failure when its status is one of
``HttpStatusClassifier``'s — the canonical retryable set ``429, 500, 502, 503,
504`` — and any transport exception (connect/read errors) is a failure. Supply
a custom ``classifier`` to change that policy.
"""

import http
from typing import cast

from httpx2 import AsyncBaseTransport, BaseTransport, Request, Response
from interlock.config import Config
from interlock.protocols import Clock, EventListener, FailureClassifier
from interlock.registry import Registry

__all__ = (
    'AsyncCircuitBreakerTransport',
    'CircuitBreakerTransport',
    'HttpStatusClassifier',
)

# Mirrors urllib3's recommended ``status_forcelist`` (also used by AWS/Google):
# transient server-side conditions where the dependency is unhealthy or
# overloaded. Permanent 5xx (501 Not Implemented, 505) are excluded — retrying
# or tripping the breaker cannot help a contract/protocol error.
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
    """Counts transport exceptions and unhealthy-status responses as failures.

    A returned response is a failure when its status is in the canonical
    retryable set (``429, 500, 502, 503, 504``); any raised exception is a
    failure. Other responses — including ``4xx`` client mistakes like ``404`` —
    are successes, so they never trip the breaker.
    """

    def is_failure(self, *, result: object, exception: BaseException | None) -> bool:
        """Return whether a completed request counts as a failure."""
        if exception is not None:
            return True

        return cast('Response', result).status_code in _FAILURE_STATUSES


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
    """A synchronous transport that guards each host with a circuit breaker.

    Args:
        transport: The wrapped transport that performs the actual request.
        config: Thresholds, window and timing for every host's breaker.
        clock: Time source for the breakers; inject a fake for deterministic
            tests.
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
        self._registry = _build_registry(config, clock, classifier, listener)

    def handle_request(self, request: Request) -> Response:
        """Run the request under its host's breaker.

        Raises:
            CircuitOpenError: If the host's breaker is open.
        """
        breaker = self._registry.get(request.url.host)
        guarded = breaker(self._transport.handle_request)
        return guarded(request)

    def close(self) -> None:
        """Close the wrapped transport, releasing its connection pool."""
        self._transport.close()


class AsyncCircuitBreakerTransport(AsyncBaseTransport):
    """An asynchronous transport that guards each host with a circuit breaker.

    Args:
        transport: The wrapped async transport that performs the request.
        config: Thresholds, window and timing for every host's breaker.
        clock: Time source for the breakers; inject a fake for deterministic
            tests.
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
        self._registry = _build_registry(config, clock, classifier, listener)

    async def handle_async_request(self, request: Request) -> Response:
        """Run the request under its host's breaker.

        Raises:
            CircuitOpenError: If the host's breaker is open.
        """
        breaker = self._registry.get(request.url.host)
        guarded = breaker(self._transport.handle_async_request)
        return await guarded(request)

    async def aclose(self) -> None:
        """Close the wrapped transport, releasing its connection pool."""
        await self._transport.aclose()
