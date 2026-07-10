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
* ``RetryStrategy`` — the same policies packaged as an ``interlock.pipeline``
  strategy: a bounded tenacity retry layer composable with
  ``CircuitBreakerStrategy`` and ``TimeoutStrategy``.

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

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from typing import TypeVar

try:
    from tenacity import (
        AsyncRetrying,
        RetryCallState,
        Retrying,
        retry_base,
        retry_if_exception,
        stop_after_attempt,
        wait_exponential_jitter,
    )
    from tenacity.wait import wait_base
except ImportError as exc:
    raise ImportError(
        'interlock.integrations.tenacity requires the tenacity package: '
        "install it with `pip install 'interlock-cb[tenacity]'`"
    ) from exc

from interlock.errors import CircuitOpenError
from interlock.protocols import EventListener

__all__ = ('RetryStrategy', 'retry_unless_open', 'wait_probe')

T = TypeVar('T')


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


class RetryStrategy:
    """A bounded retry layer for ``interlock.pipeline``, delegating policy to tenacity.

    The strategy owns no retry logic: it builds a tenacity ``Retrying`` /
    ``AsyncRetrying`` controller around the next pipeline layer. Attempts are
    always capped (``stop_after_attempt``) and the original exception is
    re-raised when the budget runs out (``reraise=True``) — no ``RetryError``
    wrapping, no unbounded loops.

    The default policy is the fail-fast composition this module documents:
    retry any ordinary exception, stop immediately on ``CircuitOpenError``
    (``retry_unless_open()``), exponential backoff with jitter. For the
    patient mode pass ``retry=retry_if_exception_type((..., CircuitOpenError))``
    and ``wait=wait_probe(...)``.

    Recommended order in a pipeline — retry outside, breaker inside, so every
    attempt is an honest breaker call::

        pipeline = Pipeline(
            RetryStrategy(attempts=4),
            CircuitBreakerStrategy(breaker),
            TimeoutStrategy(2.0),
        )

    Args:
        attempts: Total attempt cap, including the first call. Must be >= 1.
        retry: A tenacity retry predicate. Defaults to ``retry_unless_open()``.
        wait: A tenacity wait strategy. Defaults to ``wait_exponential_jitter()``.
        sleep: Sleep function for the sync path — injectable in tests.
            Defaults to ``time.sleep``.
        async_sleep: Sleep coroutine function for the async path — injectable
            in tests. Defaults to ``asyncio.sleep``.
        before_sleep: Optional tenacity hook invoked before each backoff sleep,
            e.g. ``tenacity.before_sleep_log(logger, logging.WARNING)``.

    Raises:
        ValueError: If ``attempts`` is not positive.
    """

    __slots__ = (
        '_async_sleep',
        '_attempts',
        '_before_sleep',
        '_listener',
        '_name',
        '_retry',
        '_sleep',
        '_wait',
    )

    def __init__(
        self,
        *,
        attempts: int = 3,
        retry: retry_base | None = None,
        wait: wait_base | None = None,
        sleep: Callable[[int | float], None] | None = None,
        async_sleep: Callable[[float], Awaitable[None]] | None = None,
        before_sleep: Callable[[RetryCallState], None] | None = None,
        name: str = 'retry',
        listener: EventListener | None = None,
    ) -> None:
        if attempts < 1:
            raise ValueError(f'attempts must be >= 1, got {attempts!r}')

        self._attempts = attempts
        self._retry = retry if retry is not None else retry_unless_open()
        self._wait = wait if wait is not None else wait_exponential_jitter()
        self._sleep = sleep if sleep is not None else time.sleep
        self._async_sleep = async_sleep if async_sleep is not None else asyncio.sleep
        self._before_sleep = before_sleep
        self._name = name
        self._listener = listener

    def _on_before_sleep(self, retry_state: RetryCallState) -> None:
        """Emit ``on_retry`` (safe getattr — pre-2.0 listeners fine), then the user hook."""
        if self._listener is not None:
            next_action = retry_state.next_action
            delay = next_action.sleep if next_action is not None else 0.0
            method = getattr(self._listener, 'on_retry', None)
            if callable(method):
                method(name=self._name, attempt=retry_state.attempt_number, delay=delay)
        if self._before_sleep is not None:
            self._before_sleep(retry_state)

    def execute(self, call: Callable[[], T]) -> T:
        """Run the next layer with retries; re-raise the last error when capped."""
        controller = Retrying(
            stop=stop_after_attempt(self._attempts),
            retry=self._retry,
            wait=self._wait,
            sleep=self._sleep,
            before_sleep=self._on_before_sleep,
            reraise=True,
        )
        return controller(call)

    async def execute_async(self, call: Callable[[], Awaitable[T]]) -> T:
        """Run the next async layer with retries; re-raise the last error when capped."""
        controller = AsyncRetrying(
            stop=stop_after_attempt(self._attempts),
            retry=self._retry,
            wait=self._wait,
            sleep=self._async_sleep,
            before_sleep=self._on_before_sleep,
            reraise=True,
        )
        return await controller(call)
