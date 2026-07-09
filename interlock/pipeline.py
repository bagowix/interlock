"""Compose resilience strategies in an explicit order around one protected call.

The pipeline is an additive layer over the v1 primitives (the standalone
``CircuitBreaker`` and the timeout helpers keep their public API unchanged):
each concern is a ``Strategy`` applied around the next layer, outermost first,
mirroring Polly's ``ResiliencePipeline`` semantics::

    from interlock.pipeline import CircuitBreakerStrategy, Pipeline, TimeoutStrategy

    pipeline = Pipeline(
        CircuitBreakerStrategy(breaker),  # outer: counts timeouts as failures
        TimeoutStrategy(2.0),             # inner: bounds every attempt
    )
    result = pipeline.call(fetch_orders, user_id)

One ``Pipeline`` serves sync and async callables alike — ``call`` dispatches
on the callable's nature, the same contract as ``CircuitBreaker.call``.
"""

import asyncio
import threading
from collections.abc import Awaitable, Callable
from typing import Generic, Protocol, TypeVar, cast, runtime_checkable

from interlock._detect import is_async_callable
from interlock._typing import AsyncCallable, P, R, SyncCallable
from interlock.breaker import CircuitBreaker
from interlock.errors import BulkheadFullError
from interlock.timeout import sync_timeout, timeout

__all__ = (
    'BulkheadStrategy',
    'CircuitBreakerStrategy',
    'FallbackStrategy',
    'Pipeline',
    'Strategy',
    'TimeoutStrategy',
)

T = TypeVar('T')
F_co = TypeVar('F_co', covariant=True)


@runtime_checkable
class Strategy(Protocol):
    """One resilience concern applied around the next layer of a pipeline.

    The contract mirrors a plain call: the strategy runs the zero-argument
    next layer, returns its result and lets exceptions propagate. A strategy
    must never swallow ``BaseException`` — cancellation passes through every
    layer untouched (the v1 invariant holds per layer).

    ``execute_async`` always receives a real coroutine function, so
    detect-dispatching primitives (``CircuitBreaker.call``) treat the next
    layer as async.
    """

    def execute(self, call: Callable[[], T]) -> T:
        """Run the next layer synchronously under this strategy."""
        ...

    async def execute_async(self, call: Callable[[], Awaitable[T]]) -> T:
        """Run the next layer asynchronously under this strategy."""
        ...


class Pipeline:
    """Apply strategies in declaration order — first is outermost — around a call.

    A pipeline with no strategies is a plain call. Strategies are stateless
    from the pipeline's perspective; anything stateful (a breaker's window, a
    semaphore) lives inside the strategy, so one pipeline instance is safe to
    reuse across calls and threads exactly as much as its strategies are.
    """

    __slots__ = ('_strategies',)

    def __init__(self, *strategies: 'Strategy | FallbackStrategy[object]') -> None:
        self._strategies = strategies

    def call(
        self,
        fn: AsyncCallable[P, R] | SyncCallable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Awaitable[R] | R:
        """Run ``fn`` through every strategy, dispatching on its sync/async nature.

        Returns an awaitable for a coroutine function and the plain result for
        a synchronous one.
        """
        if is_async_callable(fn):
            return self._run_async(cast('AsyncCallable[P, R]', fn), *args, **kwargs)

        return self._run_sync(cast('SyncCallable[P, R]', fn), *args, **kwargs)

    def _run_sync(self, fn: SyncCallable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        def layer(index: int) -> R:
            if index == len(self._strategies):
                return fn(*args, **kwargs)

            # A FallbackStrategy widens its own result to T | F; at the
            # pipeline level the substitute is the user's contract to shape
            # like R, so the executor keeps the call's type.
            strategy = cast('Strategy', self._strategies[index])
            return strategy.execute(lambda: layer(index + 1))

        return layer(0)

    async def _run_async(self, fn: AsyncCallable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        async def layer(index: int) -> R:
            if index == len(self._strategies):
                return await fn(*args, **kwargs)

            async def next_layer() -> R:
                return await layer(index + 1)

            strategy = cast('Strategy', self._strategies[index])
            return await strategy.execute_async(next_layer)

        return await layer(0)


class CircuitBreakerStrategy:
    """Adapt a standalone :class:`CircuitBreaker` to the ``Strategy`` contract.

    The breaker keeps its full public API and can still be used directly; the
    strategy only routes the next pipeline layer through it, so the window,
    events and manual controls behave exactly as in standalone use.
    """

    __slots__ = ('_breaker',)

    def __init__(self, breaker: CircuitBreaker) -> None:
        self._breaker = breaker

    def execute(self, call: Callable[[], T]) -> T:
        """Run the next layer under the breaker's protection.

        Raises:
            CircuitOpenError: If the breaker rejects the call.
        """
        guarded: Callable[[], T] = self._breaker(call)
        return guarded()

    async def execute_async(self, call: Callable[[], Awaitable[T]]) -> T:
        """Run the next async layer under the breaker's protection.

        Raises:
            CircuitOpenError: If the breaker rejects the call.
        """
        guarded: Callable[[], Awaitable[T]] = self._breaker(call)
        return await guarded()


class TimeoutStrategy:
    """Bound every attempt to ``seconds`` via the v1 timeout primitives.

    Async attempts are cancelled on overrun (``asyncio.timeout``); sync
    attempts inherit the ``sync_timeout`` worker-thread limitation — the
    caller gets ``CallTimeoutError`` on time, but Python cannot kill the
    thread, so the overrunning callable finishes in the background.
    """

    __slots__ = ('_seconds',)

    def __init__(self, seconds: float) -> None:
        if seconds <= 0.0:
            raise ValueError(f'seconds must be > 0, got {seconds!r}')

        self._seconds = seconds

    def execute(self, call: Callable[[], T]) -> T:
        """Run the next layer, raising ``CallTimeoutError`` on overrun."""
        return sync_timeout(self._seconds)(call)()

    async def execute_async(self, call: Callable[[], Awaitable[T]]) -> T:
        """Run the next async layer, raising ``CallTimeoutError`` on overrun."""
        async with timeout(self._seconds):
            return await call()


class BulkheadStrategy:
    """Cap how many calls run through the pipeline layer concurrently.

    A classic bulkhead: at most ``max_concurrent`` calls execute the next
    layer at once. When no slot is free, the call either fails immediately
    (``max_wait=0``, the default) or waits up to ``max_wait`` seconds for a
    slot — and raises :class:`BulkheadFullError` when none frees up in time.
    The rejection is deliberately not ``CircuitOpenError``: a full bulkhead
    signals local saturation, not dependency health.

    One configuration drives both runtimes: sync calls share a
    ``threading.Semaphore``, async calls an ``asyncio.Semaphore``. The two
    pools are independent — a strategy instance guarding both sync and async
    callers admits up to ``max_concurrent`` of each. As with any asyncio
    primitive, the async side of one instance belongs to a single event loop.

    Args:
        max_concurrent: Concurrency limit per runtime. Must be >= 1.
        max_wait: Seconds to wait for a slot before rejecting. ``0`` rejects
            immediately. Must be >= 0.

    Raises:
        ValueError: If ``max_concurrent`` or ``max_wait`` is out of range.
    """

    __slots__ = ('_async_semaphore', '_max_concurrent', '_max_wait', '_sync_semaphore')

    def __init__(self, max_concurrent: int, *, max_wait: float = 0.0) -> None:
        if max_concurrent < 1:
            raise ValueError(f'max_concurrent must be >= 1, got {max_concurrent!r}')
        if max_wait < 0.0:
            raise ValueError(f'max_wait must be >= 0, got {max_wait!r}')

        self._max_concurrent = max_concurrent
        self._max_wait = max_wait
        self._sync_semaphore = threading.Semaphore(max_concurrent)
        self._async_semaphore = asyncio.Semaphore(max_concurrent)

    def execute(self, call: Callable[[], T]) -> T:
        """Run the next layer inside a concurrency slot.

        Raises:
            BulkheadFullError: If no slot frees up within ``max_wait``.
        """
        if self._max_wait > 0.0:
            acquired = self._sync_semaphore.acquire(timeout=self._max_wait)
        else:
            acquired = self._sync_semaphore.acquire(blocking=False)
        if not acquired:
            raise BulkheadFullError(self._max_concurrent, max_wait=self._max_wait)

        try:
            return call()
        finally:
            self._sync_semaphore.release()

    async def execute_async(self, call: Callable[[], Awaitable[T]]) -> T:
        """Run the next async layer inside a concurrency slot.

        Raises:
            BulkheadFullError: If no slot frees up within ``max_wait``.
        """
        try:
            async with asyncio.timeout(self._max_wait):
                await self._async_semaphore.acquire()
        except TimeoutError as exc:
            raise BulkheadFullError(self._max_concurrent, max_wait=self._max_wait) from exc

        try:
            return await call()
        finally:
            self._async_semaphore.release()


class FallbackStrategy(Generic[F_co]):
    """Replace selected failures of the next layer with an explicit substitute.

    Nothing is silent here: the substitution happens only for the exception
    types named in ``on``, the ``fallback`` callable receives the exception it
    is standing in for, and the strategy's return type is honestly ``T | F``
    — the checkers see the union instead of an ``Any``::

        strategy = FallbackStrategy(lambda exc: [], on=(CircuitOpenError,))
        picks = strategy.execute(fetch_picks)   # inferred: list[str] | list[Never]

    ``on`` accepts ``Exception`` subclasses only — ``BaseException`` kinds
    (cancellation, ``KeyboardInterrupt``) always propagate, preserving the v1
    invariant. Place the fallback outermost so it also covers rejections
    raised by the inner strategies (``CircuitOpenError``,
    ``BulkheadFullError``, ``CallTimeoutError``).

    Args:
        fallback: Called with the caught exception; its return value becomes
            the call's result. Keep it cheap and local (a cached value, an
            empty response) — it runs inside the failure path.
        on: Exception types that trigger the substitution. Defaults to
            ``(Exception,)``.

    Raises:
        ValueError: If ``on`` is empty.
        TypeError: If an ``on`` entry is not an ``Exception`` subclass.
    """

    __slots__ = ('_fallback', '_on')

    def __init__(
        self,
        fallback: Callable[[BaseException], F_co],
        *,
        on: tuple[type[Exception], ...] = (Exception,),
    ) -> None:
        if not on:
            raise ValueError('on must name at least one exception type')
        for kind in on:
            # The signature already promises Exception subclasses; this guards
            # untyped callers from silently catching BaseException kinds.
            if not (isinstance(kind, type) and issubclass(kind, Exception)):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise TypeError(f'on entries must be Exception subclasses, got {kind!r}')

        self._fallback = fallback
        self._on = on

    def execute(self, call: Callable[[], T]) -> T | F_co:
        """Run the next layer, substituting the fallback value on a match."""
        try:
            return call()
        except self._on as exc:
            return self._fallback(exc)

    async def execute_async(self, call: Callable[[], Awaitable[T]]) -> T | F_co:
        """Run the next async layer, substituting the fallback value on a match."""
        try:
            return await call()
        except self._on as exc:
            return self._fallback(exc)
