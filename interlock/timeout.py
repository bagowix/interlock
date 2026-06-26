"""An async-first timeout primitive.

A circuit breaker without a timeout is unsafe: a call that hangs forever is
never counted as slow or failed — it just holds a resource. ``timeout`` bounds
an awaited block, converting a hang into a ``CallTimeoutError`` that a
surrounding breaker records as a (slow) failure.

It composes with a breaker manually — inside a decorated coroutine or a
``breaker.call`` target — rather than being baked into the breaker; pipeline
composition is deferred to v2. Async only by design; a sync timeout is v1.1.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator

from interlock.errors import CallTimeoutError

__all__ = ('timeout',)


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
