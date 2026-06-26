"""Typing primitives for the call() contract and type-safe decorators.

These aliases let the public API preserve a wrapped callable's signature and
its sync/async nature instead of collapsing to ``Callable[..., Any]``.
"""

from collections.abc import Awaitable, Callable
from typing import ParamSpec, Protocol, TypeVar, overload, runtime_checkable

P = ParamSpec('P')
R = TypeVar('R')

SyncCallable = Callable[P, R]
AsyncCallable = Callable[P, Awaitable[R]]

__all__ = ('AsyncCallable', 'Call', 'SyncCallable')


@runtime_checkable
class Call(Protocol):
    """The call() primitive: run a callable under the breaker's protection.

    The return type tracks the callable's nature — a coroutine function yields
    an awaitable, a plain function yields its result — so static typing never
    loses the sync/async distinction. The detect-and-dispatch implementation
    lives in the core (M4); this protocol fixes the contract.
    """

    @overload
    def __call__(
        self, fn: AsyncCallable[P, R], /, *args: P.args, **kwargs: P.kwargs
    ) -> Awaitable[R]: ...

    @overload
    def __call__(self, fn: SyncCallable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R: ...
