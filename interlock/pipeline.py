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

from collections.abc import Awaitable, Callable
from typing import Protocol, TypeVar, cast, runtime_checkable

from interlock._detect import is_async_callable
from interlock._typing import AsyncCallable, P, R, SyncCallable
from interlock.breaker import CircuitBreaker
from interlock.timeout import sync_timeout, timeout

__all__ = (
    'CircuitBreakerStrategy',
    'Pipeline',
    'Strategy',
    'TimeoutStrategy',
)

T = TypeVar('T')


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

    def __init__(self, *strategies: Strategy) -> None:
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

            return self._strategies[index].execute(lambda: layer(index + 1))

        return layer(0)

    async def _run_async(self, fn: AsyncCallable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        async def layer(index: int) -> R:
            if index == len(self._strategies):
                return await fn(*args, **kwargs)

            async def next_layer() -> R:
                return await layer(index + 1)

            return await self._strategies[index].execute_async(next_layer)

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
