"""The public circuit breaker.

``CircuitBreaker`` is a single class serving both sync and async code. It offers
three ways to protect work over the one ``call()`` primitive:

- **decorator** — ``@breaker`` wraps a function, preserving its signature *and*
  its sync/async nature via ``@overload`` + ``ParamSpec``;
- **context manager** — the same instance is both a sync (``with``) and async
  (``async with``) context manager guarding a block;
- **``breaker.call(fn, ...)``** — the breaker executes the callable.

A documented contract difference: the decorator and ``call`` see a callable, so
result-based classification and slow-call detection both apply. The context
manager sees only the block — its exception and duration — so result-based
classification is unavailable there.
"""

import functools
from collections.abc import Awaitable
from types import TracebackType
from typing import Literal, Self, cast, overload

from interlock._clock import SystemClock
from interlock._detect import is_async_callable
from interlock._engine import Engine
from interlock._typing import AsyncCallable, P, R, SyncCallable
from interlock.config import Config
from interlock.protocols import Clock, EventListener, FailureClassifier
from interlock.state import State
from interlock.window import WindowSnapshot

__all__ = ('CircuitBreaker',)


class CircuitBreaker:
    """A named circuit breaker for sync and async callables.

    Args:
        name: Identifies the breaker; surfaced on ``CircuitOpenError``.
        config: Thresholds, window and timing. Defaults to ``Config()``.
        clock: Time source. Defaults to ``SystemClock`` (real monotonic time);
            inject a fake for deterministic tests.
        classifier: Decides which outcomes count as failures. Defaults to any
            raised exception being a failure.
        listener: Observability hooks (state changes, calls, rejections,
            resets). Defaults to no observation.
    """

    def __init__(
        self,
        *,
        name: str,
        config: Config | None = None,
        clock: Clock | None = None,
        classifier: FailureClassifier | None = None,
        listener: EventListener | None = None,
    ) -> None:
        self._name = name
        self._engine = Engine(
            name=name,
            config=config if config is not None else Config(),
            clock=clock if clock is not None else SystemClock(),
            classifier=classifier,
            listener=listener,
        )
        self._blocks: list[tuple[float, int]] = []

    @property
    def name(self) -> str:
        """The breaker's name."""
        return self._name

    @property
    def state(self) -> State:
        """The breaker's current lifecycle state."""
        return self._engine.state

    def snapshot(self) -> WindowSnapshot:
        """An immutable view of the current window aggregates."""
        return self._engine.snapshot()

    def reset(self) -> None:
        """Return the breaker to ``CLOSED`` with a fresh window."""
        self._engine.reset()

    def force_open(self) -> None:
        """Force the breaker ``FORCED_OPEN``: reject all traffic until reset."""
        self._engine.force_open()

    def disable(self) -> None:
        """Disable the breaker: admit all traffic and record nothing."""
        self._engine.disable()

    def metrics_only(self) -> None:
        """Put the breaker in shadow mode: admit all traffic, record, never trip."""
        self._engine.metrics_only()

    def call(
        self,
        fn: AsyncCallable[P, R] | SyncCallable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Awaitable[R] | R:
        """Execute ``fn`` under protection, dispatching on its sync/async nature.

        Returns an awaitable for a coroutine function and the plain result for a
        synchronous one.

        Raises:
            CircuitOpenError: If the breaker rejects the call.
        """
        return self._engine.call(fn, *args, **kwargs)

    @overload
    def __call__(self, fn: AsyncCallable[P, R]) -> AsyncCallable[P, R]: ...

    @overload
    def __call__(self, fn: SyncCallable[P, R]) -> SyncCallable[P, R]: ...

    # mypy cannot reconcile this union implementation with the ParamSpec
    # overloads above (a known limitation of overloaded decorators); pyright
    # accepts it, and the overloads are what callers see.
    def __call__(  # type: ignore[misc]
        self, fn: AsyncCallable[P, R] | SyncCallable[P, R]
    ) -> AsyncCallable[P, R] | SyncCallable[P, R]:
        """Decorate ``fn``, preserving its signature and sync/async nature."""
        if is_async_callable(fn):
            async_fn = cast('AsyncCallable[P, R]', fn)

            @functools.wraps(async_fn)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                return await self._engine.call_async(async_fn, *args, **kwargs)

            return async_wrapper

        sync_fn = cast('SyncCallable[P, R]', fn)

        @functools.wraps(sync_fn)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return self._engine.call_sync(sync_fn, *args, **kwargs)

        return sync_wrapper

    def __enter__(self) -> Self:
        self._blocks.append(self._engine.enter_block())
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        exc: BaseException | None,
        _traceback: TracebackType | None,
    ) -> Literal[False]:
        start, generation = self._blocks.pop()
        self._engine.exit_block(start=start, generation=generation, exception=exc)
        return False

    async def __aenter__(self) -> Self:
        self._blocks.append(self._engine.enter_block())
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        exc: BaseException | None,
        _traceback: TracebackType | None,
    ) -> Literal[False]:
        start, generation = self._blocks.pop()
        self._engine.exit_block(start=start, generation=generation, exception=exc)
        return False
