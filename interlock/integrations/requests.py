"""requests integration — requires the ``requests`` extra.

Mount ``CircuitBreakerAdapter`` on a session and every request it sends is
guarded by a circuit breaker **per host** — no decorators in call sites::

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
from collections.abc import Iterable, Mapping
from typing import cast
from urllib.parse import urlsplit

from requests import PreparedRequest, Response
from requests.adapters import HTTPAdapter

from interlock.config import Config
from interlock.protocols import Clock, EventListener, FailureClassifier
from interlock.registry import Registry

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


class CircuitBreakerAdapter(HTTPAdapter):
    """An ``HTTPAdapter`` that guards each host with a circuit breaker.

    Args:
        config: Thresholds, window and timing for every host's breaker.
        clock: Time source for the breakers; inject a fake for deterministic
            tests.
        classifier: Failure policy. Defaults to ``HttpStatusClassifier``.
        listener: Observability hooks shared by every host's breaker.
        adapter_kwargs: Passed through to ``HTTPAdapter`` (pool sizes,
            ``max_retries``, ...).
    """

    def __init__(
        self,
        *,
        config: Config | None = None,
        clock: Clock | None = None,
        classifier: FailureClassifier | None = None,
        listener: EventListener | None = None,
        **adapter_kwargs: object,
    ) -> None:
        super().__init__(**adapter_kwargs)  # type: ignore[arg-type]
        self._registry = Registry(
            config=config,
            clock=clock,
            classifier=classifier if classifier is not None else HttpStatusClassifier(),
            listener=listener,
        )

    def send(  # noqa: PLR0913 - mirrors HTTPAdapter.send, the native extension point
        self,
        request: PreparedRequest,
        stream: bool = False,  # noqa: FBT001, FBT002 - mirrors HTTPAdapter.send
        timeout: _Timeout = None,
        verify: bool | str = True,  # noqa: FBT001, FBT002 - mirrors HTTPAdapter.send
        cert: _Cert = None,
        proxies: Mapping[str, str] | None = None,
    ) -> Response:
        """Run the request under its host's breaker.

        Raises:
            CircuitOpenError: If the host's breaker is open.
            ValueError: If the request URL carries no host to key a breaker on.
        """
        host = urlsplit(request.url or '').hostname
        if not host:
            raise ValueError(f'Request URL has no host to key a breaker on: {request.url!r}')

        breaker = self._registry.get(host)
        guarded = breaker(super().send)
        return guarded(
            request,
            stream=stream,
            timeout=timeout,
            verify=verify,
            cert=cert,
            proxies=proxies,
        )
