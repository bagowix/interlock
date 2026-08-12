"""requests integration — requires the ``requests`` extra.

Mount ``CircuitBreakerAdapter`` on a session and every request it sends is
guarded by a circuit breaker **per host by default** — no decorators in call
sites::

    import requests
    from interlock.integrations.requests import CircuitBreakerAdapter

    session = requests.Session()
    adapter = CircuitBreakerAdapter()
    session.mount('https://', adapter)
    session.mount('http://', adapter)

Each host gets its own breaker (a failing ``api.a`` must not trip ``api.b``),
created lazily and shared across requests. When a host's circuit is open the
request is rejected with ``CircuitOpenError`` before a connection is made.

By default a response counts as a failure when its status is in the canonical
retryable set (``429, 500, 502, 503, 504``) and a transport exception
(connect/read errors) is a failure unless it is caller-side (a URL with no
host); ``4xx`` client mistakes like ``404`` are successes. Supply a custom
``classifier`` to change that policy.
"""

import http
from collections.abc import Callable, Iterable, Mapping
from contextlib import ExitStack
from typing import cast
from urllib.parse import urlsplit

from requests import PreparedRequest, Response
from requests.adapters import HTTPAdapter
from requests.exceptions import InvalidURL

from interlock.config import Config
from interlock.integrations._registry import reject_registry_options, resolve_registry
from interlock.protocols import Clock, CoreEventListener, FailureClassifier, StorageEventListener
from interlock.registry import Registry
from interlock.state import State

__all__ = ('CircuitBreakerAdapter', 'HttpStatusClassifier')

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

_Timeout = float | tuple[float, float] | tuple[float, None] | None
_Cert = bytes | str | tuple[bytes | str, bytes | str] | None

# The adapter raises ``InvalidURL`` (and its ``InvalidProxyURL`` subclass) when
# the request or proxy URL carries no host — the caller's own bug. It is
# deterministic, so no retry and no open circuit can make it succeed, and it
# says nothing about the dependency: counting it would trip the breaker of a
# perfectly healthy host. ``InvalidHeader`` is *not* here — urllib3 raises it
# for a malformed header sent by the *server*.
_EXCLUDED_EXCEPTIONS: tuple[type[Exception], ...] = (InvalidURL,)


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
    """Counts transport exceptions and unhealthy-status responses as failures.

    A returned response is a failure when its status is in ``failure_statuses``
    — by default the canonical retryable set (``429, 500, 502, 503, 504``); a
    raised exception is a failure unless it is one of ``excluded_exceptions``,
    by default the caller-side ``InvalidURL``. Other responses — including
    ``4xx`` client mistakes like ``404`` — are successes, so they never trip
    the breaker.

    An excluded exception still propagates to the caller; it is recorded as a
    success, since the window has no third outcome.

    Args:
        failure_statuses: Statuses to count as failures instead of the
            canonical set.
        excluded_exceptions: Exception types to count as successes instead of
            the caller-side default. Pass ``()`` to count every exception as a
            failure.

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

        return cast('Response', result).status_code in self._failure_statuses


def _host(request: PreparedRequest) -> str:
    """Return the host key, rejecting URLs that cannot identify a dependency."""
    url = request.url
    if url is None:
        raise ValueError(f'Request URL has no host to key a breaker on: {url!r}')

    host = urlsplit(url).hostname
    if not host:
        raise ValueError(f'Request URL has no host to key a breaker on: {url!r}')

    return host


def _breaker_name(
    request: PreparedRequest,
    name_resolver: Callable[[PreparedRequest], str],
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


class CircuitBreakerAdapter(HTTPAdapter):
    """An ``HTTPAdapter`` that guards each dependency with a circuit breaker.

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
        adapter_kwargs: Passed through to ``HTTPAdapter`` (pool sizes,
            ``max_retries``, ...).

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
        name_resolver: Callable[[PreparedRequest], str] = _host,
        **adapter_kwargs: object,
    ) -> None:
        super().__init__(**adapter_kwargs)  # type: ignore[arg-type]
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

    def close(self) -> None:
        """Release connection pools and every breaker owned by this adapter."""
        with ExitStack() as stack:
            stack.callback(self._close_registry)
            super().close()

    def _close_registry(self) -> None:
        if self._owns_registry:
            self._registry.close_all()

    def send(  # noqa: PLR0913, PLR0917 - mirrors HTTPAdapter.send, the native extension point
        self,
        request: PreparedRequest,
        stream: bool = False,  # noqa: FBT001, FBT002 - mirrors HTTPAdapter.send
        timeout: _Timeout = None,
        verify: bool | str = True,  # noqa: FBT001, FBT002 - mirrors HTTPAdapter.send
        cert: _Cert = None,
        proxies: Mapping[str, str] | None = None,
    ) -> Response:
        """Run the request under its resolved dependency's breaker.

        Raises:
            CircuitOpenError: If the resolved dependency's breaker is open.
            ValueError: If the request URL has no host under the default
                resolver, or the configured resolver returns a non-string or
                empty name.
        """
        breaker = self._registry.get(_breaker_name(request, self._name_resolver))
        guarded = breaker(super().send)
        return guarded(
            request,
            stream=stream,
            timeout=timeout,
            verify=verify,
            cert=cert,
            proxies=proxies,
        )
