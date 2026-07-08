"""tenacity integration — requires the ``tenacity`` extra.

interlock deliberately ships no retry engine of its own: `tenacity
<https://tenacity.readthedocs.io/>`_ already does backoff, jitter, stop
conditions and predicates well. What this module adds is the glue where
retry × breaker composition goes wrong in practice:

* ``retry_unless_open`` — retry transient failures but stop as soon as the
  breaker opens. ``CircuitOpenError`` is not transient: the breaker rejects
  instantly, so backing off and retrying it only adds latency and log noise.
* ``wait_probe`` — for the *patient* mode (background jobs that would rather
  wait than fail): when the breaker rejects a call, sleep exactly until the
  next probe is allowed (``CircuitOpenError.retry_after``) instead of a blind
  exponential backoff.

Recommended default — fail fast::

    from tenacity import Retrying, stop_after_attempt, wait_exponential_jitter
    from interlock.integrations.tenacity import retry_unless_open

    retrying = Retrying(
        retry=retry_unless_open(TimeoutError, ConnectionError),
        wait=wait_exponential_jitter(),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    retrying(breaker.call, fetch_orders)

Patient mode — wait for the breaker to allow a probe::

    from tenacity import Retrying, retry_if_exception_type, stop_after_attempt
    from tenacity import wait_exponential_jitter
    from interlock import CircuitOpenError
    from interlock.integrations.tenacity import wait_probe

    retrying = Retrying(
        retry=retry_if_exception_type((TimeoutError, CircuitOpenError)),
        wait=wait_probe(wait_exponential_jitter()),
        stop=stop_after_attempt(10),
        reraise=True,
    )
"""

import random

from tenacity import RetryCallState, retry_base, retry_if_exception
from tenacity.wait import wait_base

from interlock.errors import CircuitOpenError

__all__ = ('retry_unless_open', 'wait_probe')


def retry_unless_open(*transient: type[BaseException]) -> retry_base:
    """Build a retry predicate that stops as soon as the circuit opens.

    Retries an attempt only when it failed with one of the ``transient``
    exception types; ``CircuitOpenError`` is never retried, even when it
    matches. Rationale: an open breaker rejects instantly, so retrying it
    burns the attempt budget without ever reaching the dependency — surface
    the rejection to the caller instead.

    Args:
        transient: Exception types worth retrying. Defaults to ``Exception``
            (retry any ordinary error) when omitted.

    Returns:
        A tenacity retry predicate for ``Retrying`` / ``AsyncRetrying``.
    """
    kinds = transient or (Exception,)

    def _is_transient(exception: BaseException) -> bool:
        return not isinstance(exception, CircuitOpenError) and isinstance(exception, kinds)

    return retry_if_exception(_is_transient)


class wait_probe(wait_base):  # noqa: N801 - tenacity wait strategies are lower-case by convention
    """Wait until the breaker's next probe is allowed; otherwise fall back.

    When the last attempt failed with ``CircuitOpenError`` carrying a
    ``retry_after`` estimate, waits exactly that long (plus a small random
    jitter so concurrent waiters do not storm the single probe slot). Any
    other outcome — a transient failure, or a rejection without an estimate
    (``FORCED_OPEN``) — delegates to the ``fallback`` strategy.

    Args:
        fallback: Wait strategy for non-rejection outcomes, e.g.
            ``wait_exponential_jitter()``.
        jitter: Upper bound, in seconds, of the uniform random extra wait
            added on top of ``retry_after``. Must be >= 0.

    Raises:
        ValueError: If ``jitter`` is negative.
    """

    def __init__(self, fallback: wait_base, *, jitter: float = 0.1) -> None:
        if jitter < 0.0:
            raise ValueError(f'jitter must be >= 0, got {jitter!r}')
        self._fallback = fallback
        self._jitter = jitter

    def __call__(self, retry_state: RetryCallState) -> float:
        """Return the seconds to sleep before the next attempt."""
        outcome = retry_state.outcome
        if outcome is not None and outcome.failed:
            exception = outcome.exception()
            if isinstance(exception, CircuitOpenError) and exception.retry_after is not None:
                return exception.retry_after + random.uniform(0.0, self._jitter)  # noqa: S311 - jitter, not crypto
        return self._fallback(retry_state)
