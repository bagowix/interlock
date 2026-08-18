"""aiohttp client integration — requires the ``aiohttp`` extra.

Pass ``CircuitBreakerMiddleware`` to a session and every request it sends is
guarded by a circuit breaker **per host by default** — no decorators in call
sites::

    import aiohttp
    from interlock.integrations.aiohttp import CircuitBreakerMiddleware

    middleware = CircuitBreakerMiddleware()
    async with aiohttp.ClientSession(middlewares=(middleware,)) as session:
        async with session.get('https://api.example.com/') as response:
            ...

Client middlewares require aiohttp >= 3.12. Each host gets its own breaker
(a failing ``api.a`` must not trip ``api.b``), created lazily and shared
across requests. When a host's circuit is open the request is rejected before a
connection is made, with ``CircuitOpenClientError``: an
``aiohttp.ClientConnectionError`` *and* a ``CircuitOpenError``, so the aiohttp
idiom (``except aiohttp.ClientError``) degrades on it exactly as it degrades on
the dependency being unreachable, and existing ``except CircuitOpenError``
handlers keep working.

By default a response counts as a failure when its status is in the canonical
retryable set (``429, 500, 502, 503, 504``) and any exception raised by the
handler (connect/read errors) is a failure; ``4xx`` client mistakes like
``404`` are successes. Supply a custom ``classifier`` to change that policy.

The breaker observes the time to *response headers*; reading the body happens
outside the guarded call — the same semantics as the httpx2 transport.
"""

import http
from collections.abc import Callable, Iterable
from typing import NoReturn, cast

from aiohttp import ClientConnectionError, ClientHandlerType, ClientRequest, ClientResponse

from interlock.config import Config
from interlock.errors import CircuitOpenError
from interlock.integrations._registry import reject_registry_options, resolve_registry
from interlock.protocols import Clock, CoreEventListener, FailureClassifier, StorageEventListener
from interlock.registry import Registry
from interlock.state import State

__all__ = ('CircuitBreakerMiddleware', 'CircuitOpenClientError', 'HttpStatusClassifier')

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

# Empty on purpose: aiohttp raises its caller-side errors — a malformed or
# non-HTTP URL — before the middleware chain runs, so none of them can reach
# the classifier and be misread as the dependency failing.
_EXCLUDED_EXCEPTIONS: tuple[type[Exception], ...] = ()


def _exception_types(
    excluded: Iterable[type[Exception]] | None,
) -> tuple[type[Exception], ...]:
    """Freeze the exclusion set, rejecting entries that can never be classified.

    Raises:
        TypeError: If an entry is not an ``Exception`` subclass.
    """
    if excluded is None:
        return _EXCLUDED_EXCEPTIONS

    types = tuple(excluded)
    # The annotation is a promise, not a guarantee: an untyped caller can still
    # pass a string. Checking now beats an ``isinstance`` TypeError raised from
    # inside the guarded call, on the first real failure.
    for entry in cast('tuple[object, ...]', types):
        if not (isinstance(entry, type) and issubclass(entry, Exception)):
            raise TypeError(f'excluded_exceptions must hold Exception subclasses, got: {entry!r}')

    return types


class HttpStatusClassifier:
    """Counts handler exceptions and unhealthy-status responses as failures.

    A returned response is a failure when its status is in ``failure_statuses``
    — by default the canonical retryable set (``429, 500, 502, 503, 504``); a
    raised exception is a failure unless it is one of ``excluded_exceptions``,
    which is empty by default. Other responses — including ``4xx`` client
    mistakes like ``404`` — are successes, so they never trip the breaker.

    aiohttp rejects a malformed or non-HTTP URL before the middleware chain
    runs, so no caller-side error of its own reaches this classifier — hence
    the empty default. Exclude the exceptions raised by middlewares of your
    own that sit inside this one: they are the caller's, not the dependency's.

    An excluded exception still propagates to the caller; it is recorded as a
    success, since the window has no third outcome.

    Args:
        failure_statuses: Statuses to count as failures instead of the
            canonical set.
        excluded_exceptions: Exception types to count as successes.

    Raises:
        TypeError: If ``excluded_exceptions`` holds anything but ``Exception``
            subclasses.
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
        self._excluded_exceptions = _exception_types(excluded_exceptions)

    def is_failure(self, *, result: object, exception: Exception | None) -> bool:
        """Return whether a completed request counts as a failure."""
        if exception is not None:
            return not isinstance(exception, self._excluded_exceptions)

        return cast('ClientResponse', result).status in self._failure_statuses


# Application code degrades on the idiom its http client teaches — for aiohttp
# that is ``except aiohttp.ClientError``. An error raised from outside that
# hierarchy sails past every such handler, so the day a breaker leaves shadow
# mode every degradation path stops working at once. The rejection is therefore
# both an aiohttp error and a ``CircuitOpenError``.
#
# The aiohttp base is the broadest "the connection never happened" type and
# never a leaf like ``ClientOSError`` or ``ServerDisconnectedError``: a leaf
# promises a physical cause that never happened, and leaves are exactly what
# retry predicates key on — a retried rejection burns an attempt against a
# circuit that is still open. It also keeps the rejection outside
# ``ClientResponseError``, which would claim a response that never arrived.
class CircuitOpenClientError(ClientConnectionError, CircuitOpenError):
    """A request rejected by an open circuit, raised in aiohttp's hierarchy.

    Caught by ``except aiohttp.ClientConnectionError`` (and by ``except
    aiohttp.ClientError``) and by ``except CircuitOpenError`` alike. aiohttp
    itself re-raises a ``ClientError`` from the middleware chain untouched, so
    the rejection reaches the caller exactly as raised.

    Args:
        breaker_name: Name of the breaker that rejected the request.
        retry_after: Seconds until the next probe is allowed, or ``None`` when
            the breaker cannot estimate it.
        last_failure: The most recent recorded failure, if any.
    """


def _raise_in_dialect(error: CircuitOpenError) -> NoReturn:
    """Re-raise ``error`` as its aiohttp twin, or unchanged when it has none.

    Only the exact core type is retyped. A subclass — including this dialect
    type, raised by an interlock layer further in — already carries a host
    hierarchy of its own and must not be wrapped a second time.
    """
    if type(error) is CircuitOpenError:
        raise CircuitOpenClientError(
            error.breaker_name,
            retry_after=error.retry_after,
            last_failure=error.last_failure,
        ) from error

    raise error


def _host(request: ClientRequest) -> str:
    """Return the host key, rejecting URLs that cannot identify a dependency."""
    host = request.url.host
    if not host:
        raise ValueError(f'Request URL has no host to key a breaker on: {request.url!s}')

    return host


def _breaker_name(
    request: ClientRequest,
    name_resolver: Callable[[ClientRequest], str],
) -> str:
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


class CircuitBreakerMiddleware:
    """A client middleware that guards each dependency with a circuit breaker.

    Args:
        config: Thresholds, window and timing for every breaker.
        clock: Time source for the breakers; inject a fake for deterministic
            tests.
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
    def __init__(  # noqa: PLR0913 - mirrors Registry's breaker collaborators
        self,
        *,
        config: Config | None = None,
        clock: Clock | None = None,
        initial_state: State = State.CLOSED,
        classifier: FailureClassifier | None = None,
        listener: CoreEventListener | StorageEventListener | None = None,
        registry: Registry | None = None,
        name_resolver: Callable[[ClientRequest], str] = _host,
    ) -> None:
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

    async def aclose(self) -> None:
        """Release every breaker owned by this middleware."""
        if self._owns_registry:
            await self._registry.aclose_all()

    async def __call__(self, request: ClientRequest, handler: ClientHandlerType) -> ClientResponse:
        """Run the request under its resolved dependency's breaker.

        Raises:
            CircuitOpenClientError: If the resolved dependency's breaker is open
                — an ``aiohttp.ClientConnectionError`` and a
                ``CircuitOpenError``.
            ValueError: If the request URL has no host under the default
                resolver, or the configured resolver returns a non-string or
                empty name.
        """
        breaker = self._registry.get(_breaker_name(request, self._name_resolver))

        # The composed handler is not guaranteed to be a coroutine *function*
        # (middleware chains may hand over plain callables returning
        # awaitables), so the breaker's sync/async detection must not decide
        # here: call_async awaits whatever the handler returns.
        try:
            return await breaker.call_async(handler, request)
        except CircuitOpenError as error:
            _raise_in_dialect(error)
