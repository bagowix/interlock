"""httpx transport integration — requires the ``httpx`` extra.

This module imports ``httpx`` and is deliberately not re-exported from
``interlock`` so the core stays zero-dependency. Install with
``pip install interlock-cb[httpx]`` and wrap a transport explicitly::

    import httpx
    from interlock.integrations.httpx import CircuitBreakerTransport

    transport = CircuitBreakerTransport(httpx.HTTPTransport())
    client = httpx.Client(transport=transport)

The wrapper applies one circuit breaker **per host by default** transparently:
no decorators in user code. Each host gets its own breaker (a slow or failing
``api.a`` must not trip ``api.b``), created lazily and shared across requests.
Responses are returned unchanged, preserving httpx's streaming semantics.

By default a response counts as a failure when its status is one of
``HttpStatusClassifier``'s — the canonical retryable set ``429, 500, 502, 503,
504`` — and a transport exception (connect/read errors) is a failure unless it
is caller-side (a malformed URL, a local protocol violation). Supply a custom
``classifier`` to change that policy.
"""

import http
from collections.abc import Callable, Iterable
from contextlib import AsyncExitStack, ExitStack
from types import TracebackType
from typing import Self, cast

from httpx import (
    AsyncBaseTransport,
    BaseTransport,
    LocalProtocolError,
    Request,
    Response,
    UnsupportedProtocol,
)

from interlock.config import Config
from interlock.integrations._registry import reject_registry_options, resolve_registry
from interlock.protocols import Clock, CoreEventListener, FailureClassifier, StorageEventListener
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

# httpx raises these from inside the transport, but they are the caller's own
# fault: a scheme-less or unsupported URL, and a local violation of the HTTP
# protocol. Both are deterministic — no retry and no open circuit can make them
# succeed — and say nothing about the dependency, so counting them would trip
# the breaker of a perfectly healthy host. ``PoolTimeout`` is deliberately not
# here: an exhausted pool is usually the dependency holding connections open,
# and shedding load then is the point.
_EXCLUDED_EXCEPTIONS: tuple[type[Exception], ...] = (
    LocalProtocolError,
    UnsupportedProtocol,
)


class HttpStatusClassifier:
    """Counts transport exceptions and unhealthy-status responses as failures.

    A returned response is a failure when its status is in ``failure_statuses``
    — by default the canonical retryable set (``429, 500, 502, 503, 504``); a
    raised exception is a failure unless it is one of ``excluded_exceptions``,
    by default the caller-side errors ``LocalProtocolError`` and
    ``UnsupportedProtocol``. Other responses — including ``4xx`` client
    mistakes like ``404`` — are successes, so they never trip the breaker.

    An excluded exception still propagates to the caller; it is recorded as a
    success, since the window has no third outcome.

    Args:
        failure_statuses: Statuses to count as failures instead of the
            canonical set.
        excluded_exceptions: Exception types to count as successes instead of
            the caller-side default. Pass ``()`` to count every exception as a
            failure, or add ``PoolTimeout`` when the local pool is sized below
            the caller's own burst.
    """

    def __init__(
        self,
        *,
        failure_statuses: Iterable[int] | None = None,
        excluded_exceptions: Iterable[type[Exception]] | None = None,
    ) -> None:
        self._failure_statuses = (
            frozenset(failure_statuses) if failure_statuses is not None else _FAILURE_STATUSES
        )
        self._excluded_exceptions = (
            tuple(excluded_exceptions) if excluded_exceptions is not None else _EXCLUDED_EXCEPTIONS
        )

    def is_failure(self, *, result: object, exception: Exception | None) -> bool:
        """Return whether a completed request counts as a failure."""
        if exception is not None:
            return not isinstance(exception, self._excluded_exceptions)

        return cast('Response', result).status_code in self._failure_statuses


def _host(request: Request) -> str:
    """Return the host key, rejecting URLs that cannot identify a dependency."""
    host = request.url.host
    if not host:
        raise ValueError(f'Request URL has no host to key a breaker on: {request.url!s}')

    return host


def _breaker_name(request: Request, name_resolver: Callable[[Request], str]) -> str:
    """Resolve and validate the dependency identity before registry access."""
    name = cast('object', name_resolver(request))
    if not isinstance(name, str):
        raise ValueError(  # noqa: TRY004 - resolver contract uses ValueError
            f'Name resolver returned a non-string breaker name for request URL: {request.url!s}'
        )
    if not name.strip():
        raise ValueError(
            f'Name resolver returned an empty breaker name for request URL: {request.url!s}'
        )

    return name


class CircuitBreakerTransport(BaseTransport):
    """Guard every dependency reached by a synchronous httpx transport.

    Args:
        transport: Wrapped transport that performs requests.
        config: Thresholds, window and timing for every breaker.
        clock: Time source for the breakers.
        initial_state: Stable state assigned before a breaker's first request.
            Use ``State.METRICS_ONLY`` for shadow mode.
        classifier: Failure policy. Defaults to ``HttpStatusClassifier``.
        listener: Observability hooks shared by every breaker.
        registry: Caller-owned registry shared with other clients. Mutually
            exclusive with all breaker-construction options above; it owns the
            failure policy too, so pass ``classifier=HttpStatusClassifier()``
            when creating it — a bare ``Registry`` counts only exceptions, and
            a returned ``503`` is a success.
        name_resolver: Maps each request to its breaker name. Defaults to the
            request host.

    Raises:
        ValueError: If ``initial_state`` is unsupported or ``registry`` is
            combined with a breaker-construction option.
    """

    @reject_registry_options
    def __init__(
        self,
        transport: BaseTransport,
        *,
        config: Config | None = None,
        clock: Clock | None = None,
        initial_state: State = State.CLOSED,
        classifier: FailureClassifier | None = None,
        listener: CoreEventListener | StorageEventListener | None = None,
        registry: Registry | None = None,
        name_resolver: Callable[[Request], str] = _host,
    ) -> None:
        self._transport = transport
        self._name_resolver = name_resolver
        self._registry, self._owns_registry = resolve_registry(
            registry=registry,
            config=config,
            clock=clock,
            initial_state=initial_state,
            classifier=classifier,
            listener=listener,
            default_classifier=HttpStatusClassifier(),
        )

    @property
    def registry(self) -> Registry:
        """The breaker registry, exposed for diagnostics and operator control."""
        return self._registry

    def handle_request(self, request: Request) -> Response:
        """Run a request under the breaker for its resolved dependency.

        Raises:
            CircuitOpenError: If the resolved dependency's breaker is open.
            ValueError: If the request URL has no host under the default
                resolver, or the configured resolver returns a non-string or
                empty name.
        """
        breaker = self._registry.get(_breaker_name(request, self._name_resolver))
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
        """Exit the wrapped transport's context and release every owned breaker."""
        with ExitStack() as stack:
            stack.callback(self._close_registry)
            self._transport.__exit__(exc_type, exc_value, traceback)

    def close(self) -> None:
        """Release the wrapped transport and every owned breaker."""
        with ExitStack() as stack:
            stack.callback(self._close_registry)
            self._transport.close()

    def _close_registry(self) -> None:
        if self._owns_registry:
            self._registry.close_all()


class AsyncCircuitBreakerTransport(AsyncBaseTransport):
    """Guard every dependency reached by an asynchronous httpx transport.

    Args:
        transport: Wrapped async transport that performs requests.
        config: Thresholds, window and timing for every breaker.
        clock: Time source for the breakers.
        initial_state: Stable state assigned before a breaker's first request.
            Use ``State.METRICS_ONLY`` for shadow mode.
        classifier: Failure policy. Defaults to ``HttpStatusClassifier``.
        listener: Observability hooks shared by every breaker.
        registry: Caller-owned registry shared with other clients. Mutually
            exclusive with all breaker-construction options above; it owns the
            failure policy too, so pass ``classifier=HttpStatusClassifier()``
            when creating it — a bare ``Registry`` counts only exceptions, and
            a returned ``503`` is a success.
        name_resolver: Maps each request to its breaker name. Defaults to the
            request host.

    Raises:
        ValueError: If ``initial_state`` is unsupported or ``registry`` is
            combined with a breaker-construction option.
    """

    @reject_registry_options
    def __init__(
        self,
        transport: AsyncBaseTransport,
        *,
        config: Config | None = None,
        clock: Clock | None = None,
        initial_state: State = State.CLOSED,
        classifier: FailureClassifier | None = None,
        listener: CoreEventListener | StorageEventListener | None = None,
        registry: Registry | None = None,
        name_resolver: Callable[[Request], str] = _host,
    ) -> None:
        self._transport = transport
        self._name_resolver = name_resolver
        self._registry, self._owns_registry = resolve_registry(
            registry=registry,
            config=config,
            clock=clock,
            initial_state=initial_state,
            classifier=classifier,
            listener=listener,
            default_classifier=HttpStatusClassifier(),
        )

    @property
    def registry(self) -> Registry:
        """The breaker registry, exposed for diagnostics and operator control."""
        return self._registry

    async def handle_async_request(self, request: Request) -> Response:
        """Run a request under the breaker for its resolved dependency.

        Raises:
            CircuitOpenError: If the resolved dependency's breaker is open.
            ValueError: If the request URL has no host under the default
                resolver, or the configured resolver returns a non-string or
                empty name.
        """
        breaker = self._registry.get(_breaker_name(request, self._name_resolver))
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
        """Exit the wrapped transport's context and release every owned breaker."""
        async with AsyncExitStack() as stack:
            stack.push_async_callback(self._aclose_registry)
            await self._transport.__aexit__(exc_type, exc_value, traceback)

    async def aclose(self) -> None:
        """Release the wrapped transport and every owned breaker."""
        async with AsyncExitStack() as stack:
            stack.push_async_callback(self._aclose_registry)
            await self._transport.aclose()

    async def _aclose_registry(self) -> None:
        if self._owns_registry:
            await self._registry.aclose_all()
