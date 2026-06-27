"""Timeout primitives for sync and async calls.

A circuit breaker without a timeout is unsafe: a call that hangs forever is
never counted as slow or failed — it just holds a resource. A timeout converts
a hang into a ``CallTimeoutError`` that a surrounding breaker records as a
(slow) failure.

Both primitives compose with a breaker manually — inside a decorated callable
or a ``breaker.call`` target — rather than being baked into the breaker;
pipeline composition is deferred to v2.

``timeout`` bounds an awaited block: asyncio can cancel the coroutine in place.
``sync_timeout`` cannot — a synchronous block has no portable in-place
cancellation (``signal.SIGALRM`` only works in the main thread and breaks in
threaded servers). It therefore wraps a *callable*, running it in a daemon
worker thread joined with a deadline.
"""

import asyncio
import contextlib
import functools
import threading
from collections.abc import AsyncGenerator, Callable

from interlock._typing import P, R, SyncCallable
from interlock.errors import CallTimeoutError

__all__ = ('sync_timeout', 'timeout')


@contextlib.asynccontextmanager
async def timeout(seconds: float) -> AsyncGenerator[None, None]:
    """Bound the awaited block to ``seconds``.

    Raises:
        CallTimeoutError: If the block does not finish within ``seconds``.
    """
    try:
        async with asyncio.timeout(seconds):
            yield
    except TimeoutError as exc:
        raise CallTimeoutError(seconds) from exc


def sync_timeout(seconds: float) -> Callable[[SyncCallable[P, R]], SyncCallable[P, R]]:
    """Bound a synchronous callable to ``seconds`` via a daemon worker thread.

    The decorated callable runs in a worker thread that the caller joins with a
    deadline; overrunning the deadline raises ``CallTimeoutError``. The
    signature, arguments and return value are preserved.

    Limitation: Python cannot forcibly kill a thread. After a timeout the worker
    keeps running in the background until it returns on its own — it cannot be
    cancelled, so it may still hold the resource it was waiting on. Prefer the
    async :func:`timeout` wherever you control an event loop.

    Raises:
        ValueError: If ``seconds`` is not positive.
        CallTimeoutError: If the call does not finish within ``seconds``.
    """
    if seconds <= 0:
        raise ValueError(f'sync_timeout seconds must be positive, got {seconds!r}')

    def decorator(fn: SyncCallable[P, R]) -> SyncCallable[P, R]:
        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            result: list[R] = []
            failure: list[BaseException] = []

            def run() -> None:
                try:
                    result.append(fn(*args, **kwargs))
                except BaseException as exc:  # noqa: BLE001 — re-raised in the caller as-is
                    failure.append(exc)

            worker = threading.Thread(target=run, name='interlock-sync-timeout', daemon=True)
            worker.start()
            worker.join(seconds)
            if worker.is_alive():
                raise CallTimeoutError(seconds)
            if failure:
                raise failure[0]
            return result[0]

        return wrapper

    return decorator
