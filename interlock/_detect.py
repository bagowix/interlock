"""Detect whether a callable runs synchronously or returns an awaitable.

The public surface accepts any callable and must dispatch to the right path.
A naive ``inspect.iscoroutinefunction(fn)`` misses two common shapes: callables
wrapped in ``functools.partial`` and objects whose ``__call__`` is a coroutine
function. This unwraps partials and inspects ``__call__`` so both are handled.
"""

import functools
import inspect

__all__ = ('is_async_callable',)


def is_async_callable(fn: object) -> bool:
    """Return whether calling ``fn`` produces an awaitable.

    Args:
        fn: Any callable — a function, bound method, ``functools.partial``, or an
            instance with a ``__call__`` method.

    Returns:
        ``True`` if ``fn`` is (or wraps, or is an object whose ``__call__`` is) a
        coroutine function; ``False`` otherwise.
    """
    while isinstance(fn, functools.partial):
        fn = fn.func

    if inspect.iscoroutinefunction(fn):
        return True

    # The __call__ attribute itself is needed to test its async-ness, which
    # callable() cannot report — so B004's suggestion does not apply here.
    return inspect.iscoroutinefunction(getattr(type(fn), '__call__', None))  # noqa: B004
