"""httpx transport integration — requires the ``httpx`` extra.

This module imports ``httpx`` and is deliberately not re-exported from
``interlock`` so the core stays zero-dependency. Install with
``pip install interlock-cb[httpx]`` and wrap a transport explicitly::

    import httpx
    from interlock.integrations.httpx import CircuitBreakerTransport

    transport = CircuitBreakerTransport(httpx.HTTPTransport())
    client = httpx.Client(transport=transport)

The wrapper applies one circuit breaker **per host** transparently: no
decorators in user code. Each host gets its own breaker (a slow or failing
``api.a`` must not trip ``api.b``), created lazily and shared across requests.
Responses are returned unchanged, preserving httpx's streaming semantics.

By default a response counts as a failure when its status is one of
``HttpStatusClassifier``'s — the canonical retryable set ``429, 500, 502, 503,
504`` — and any transport exception (connect/read errors) is a failure. Supply
a custom ``classifier`` to change that policy.
"""

import http
from collections.abc import Iterable
from contextlib import AsyncExitStack, ExitStack
from types import TracebackType
from typing import Self, cast

from httpx import AsyncBaseTransport, BaseTransport, Request, Response

from interlock.config import Config
from interlock.protocols import Clock, EventListener, FailureClassifier
from interlock.registry import Registry
from interlock.state import State

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

        return cast('Response', result).status_code in self._failure_statuses


def _host(request: Request) -> str:
    """Return the host key, rejecting URLs that cannot identify a dependency."""
    host = request.url.host
    if not host:
        raise ValueError(f'Request URL has no host to key a breaker on: {request.url!s}')

    return host


def _build_registry(
    *,
    config: Config | None,
    clock: Clock | None,
    initial_state: State,
    classifier: FailureClassifier | None,
    listener: EventListener | None,
) -> Registry:
    return Registry(
        config=config,
        clock=clock,
        initial_state=initial_state,
        classifier=classifier if classifier is not None else HttpStatusClassifier(),
        listener=listener,
    )


class CircuitBreakerTransport(BaseTransport):
    """Guard every host reached by a synchronous httpx transport.

    Args:
        transport: Wrapped transport that performs requests.
        config: Thresholds, window and timing for every host's breaker.
        clock: Time source for the breakers.
        initial_state: Stable state assigned before a host's first request.
            Use ``State.METRICS_ONLY`` for shadow mode.
        classifier: Failure policy. Defaults to ``HttpStatusClassifier``.
        listener: Observability hooks shared by every host's breaker.

    Raises:
        ValueError: If ``initial_state`` is not a supported stable state.
    """

    def __init__(
        self,
        transport: BaseTransport,
        *,
        config: Config | None = None,
        clock: Clock | None = None,
        initial_state: State = State.CLOSED,
        classifier: FailureClassifier | None = None,
        listener: EventListener | None = None,
    ) -> None:
        self._transport = transport
        self._registry = _build_registry(
            config=config,
            clock=clock,
            initial_state=initial_state,
            classifier=classifier,
            listener=listener,
        )

    @property
    def registry(self) -> Registry:
        """The per-host registry, exposed for diagnostics and operator control."""
        return self._registry

    def handle_request(self, request: Request) -> Response:
        """Run a request under the breaker for its host.

        Raises:
            CircuitOpenError: If the host's breaker is open.
            ValueError: If the request URL has no host.
        """
        breaker = self._registry.get(_host(request))
        guarded = breaker(self._transport.handle_request)
        return guarded(request)

    def __enter__(self) -> Self:
        """Enter the wrapped transport's context and return this wrapper."""
        self._transport.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        """Exit the wrapped transport's context and release every breaker."""
        with ExitStack() as stack:
            stack.callback(self._registry.close_all)
            self._transport.__exit__(exc_type, exc_value, traceback)

    def close(self) -> None:
        """Release the wrapped transport and every per-host breaker."""
        with ExitStack() as stack:
            stack.callback(self._registry.close_all)
            self._transport.close()


class AsyncCircuitBreakerTransport(AsyncBaseTransport):
    """Guard every host reached by an asynchronous httpx transport.

    Args:
        transport: Wrapped async transport that performs requests.
        config: Thresholds, window and timing for every host's breaker.
        clock: Time source for the breakers.
        initial_state: Stable state assigned before a host's first request.
            Use ``State.METRICS_ONLY`` for shadow mode.
        classifier: Failure policy. Defaults to ``HttpStatusClassifier``.
        listener: Observability hooks shared by every host's breaker.

    Raises:
        ValueError: If ``initial_state`` is not a supported stable state.
    """

    def __init__(
        self,
        transport: AsyncBaseTransport,
        *,
        config: Config | None = None,
        clock: Clock | None = None,
        initial_state: State = State.CLOSED,
        classifier: FailureClassifier | None = None,
        listener: EventListener | None = None,
    ) -> None:
        self._transport = transport
        self._registry = _build_registry(
            config=config,
            clock=clock,
            initial_state=initial_state,
            classifier=classifier,
            listener=listener,
        )

    @property
    def registry(self) -> Registry:
        """The per-host registry, exposed for diagnostics and operator control."""
        return self._registry

    async def handle_async_request(self, request: Request) -> Response:
        """Run a request under the breaker for its host.

        Raises:
            CircuitOpenError: If the host's breaker is open.
            ValueError: If the request URL has no host.
        """
        breaker = self._registry.get(_host(request))
        guarded = breaker(self._transport.handle_async_request)
        return await guarded(request)

    async def __aenter__(self) -> Self:
        """Enter the wrapped transport's context and return this wrapper."""
        await self._transport.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        """Exit the wrapped transport's context and release every breaker."""
        async with AsyncExitStack() as stack:
            stack.push_async_callback(self._registry.aclose_all)
            await self._transport.__aexit__(exc_type, exc_value, traceback)

    async def aclose(self) -> None:
        """Release the wrapped transport and every per-host breaker."""
        async with AsyncExitStack() as stack:
            stack.push_async_callback(self._registry.aclose_all)
            await self._transport.aclose()
