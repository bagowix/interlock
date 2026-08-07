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
retryable set (``429, 500, 502, 503, 504``) and any transport exception
(connect/read errors) is a failure; ``4xx`` client mistakes like ``404`` are
successes. Supply a custom ``classifier`` to change that policy.
"""

import http
from collections.abc import Callable, Iterable, Mapping
from contextlib import ExitStack
from typing import cast
from urllib.parse import urlsplit

from requests import PreparedRequest, Response
from requests.adapters import HTTPAdapter

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
            exclusive with all breaker-construction options above.
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
