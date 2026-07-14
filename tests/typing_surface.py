"""Static assertions for the public typing surface.

Never executed (no ``test_`` prefix): mypy and pyright both include this file,
so a regression in user-facing type inference fails CI even though the runtime
suite cannot observe it.
"""

# The assertion functions are intentionally never called.
# pyright: reportUnusedFunction=false

from typing import assert_type

from interlock import CircuitBreaker, Pipeline

breaker = CircuitBreaker(name='typing-surface')
pipeline = Pipeline()


async def _fetch(x: int) -> str:
    return str(x)


def _fetch_sync(x: int) -> str:
    return str(x)


@breaker
async def _decorated_async(x: int) -> str:
    return str(x)


@breaker
def _decorated_sync(x: int) -> str:
    return str(x)


@pipeline
async def _piped_async(x: int) -> str:
    return str(x)


@pipeline
def _piped_sync(x: int) -> str:
    return str(x)


async def _breaker_call_infers_async_result() -> None:
    assert_type(await breaker.call(_fetch, 1), str)


def _breaker_call_infers_sync_result() -> None:
    assert_type(breaker.call(_fetch_sync, 1), str)


async def _breaker_decorator_preserves_async_signature() -> None:
    assert_type(await _decorated_async(1), str)


def _breaker_decorator_preserves_sync_signature() -> None:
    assert_type(_decorated_sync(1), str)


async def _pipeline_call_infers_async_result() -> None:
    assert_type(await pipeline.call(_fetch, 1), str)


def _pipeline_call_infers_sync_result() -> None:
    assert_type(pipeline.call(_fetch_sync, 1), str)


async def _pipeline_decorator_preserves_async_signature() -> None:
    assert_type(await _piped_async(1), str)


def _pipeline_decorator_preserves_sync_signature() -> None:
    assert_type(_piped_sync(1), str)
