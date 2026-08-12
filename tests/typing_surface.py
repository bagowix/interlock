"""Static assertions for the public typing surface.

Never executed (no ``test_`` prefix): mypy and pyright both include this file,
so a regression in user-facing type inference fails CI even though the runtime
suite cannot observe it.
"""

# The assertion functions are intentionally never called.
# pyright: reportUnusedFunction=false

from typing import assert_type

from interlock import (
    BulkheadStrategy,
    CircuitBreaker,
    CoreEventListener,
    EventListener,
    FallbackStrategy,
    LoggingEventListener,
    Outcome,
    Pipeline,
    PipelineEventListener,
    Registry,
    State,
    StorageEventListener,
)
from interlock.integrations.tenacity import RetryStrategy


class _CallListener(EventListener):
    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None:
        return None


class _CoreCallListener(CoreEventListener):
    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None:
        return None


class _StorageDegradedListener(StorageEventListener):
    def on_storage_degraded(self, *, name: str, error: BaseException) -> None:
        return None


class _RetryListener(PipelineEventListener):
    def on_retry(self, *, name: str, attempt: int, delay: float) -> None:
        return None


partial_listener = _CallListener()
partial_breaker = CircuitBreaker(name='partial-listener', listener=partial_listener)
partial_registry = Registry(listener=partial_listener)
partial_bulkhead = BulkheadStrategy(1, listener=partial_listener)
partial_fallback = FallbackStrategy(lambda _error: None, listener=partial_listener)
partial_retry = RetryStrategy(listener=partial_listener)
complete_listener_breaker = CircuitBreaker(
    name='complete-listener', listener=LoggingEventListener()
)
core_listener = _CoreCallListener()
core_breaker = CircuitBreaker(name='core-listener', listener=core_listener)
core_registry = Registry(listener=core_listener)
storage_listener = _StorageDegradedListener()
storage_breaker = CircuitBreaker(name='storage-listener', listener=storage_listener)
storage_registry = Registry(listener=storage_listener)
pipeline_listener = _RetryListener()
pipeline_bulkhead = BulkheadStrategy(1, listener=pipeline_listener)
pipeline_fallback = FallbackStrategy(lambda _error: None, listener=pipeline_listener)
pipeline_retry = RetryStrategy(listener=pipeline_listener)

breaker = CircuitBreaker(name='typing-surface')
shadow_breaker = CircuitBreaker(name='shadow', initial_state=State.METRICS_ONLY)
registry = Registry(initial_state=State.METRICS_ONLY)
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


def _breaker_call_sync_infers_result() -> None:
    assert_type(breaker.call_sync(_fetch_sync, 1), str)


async def _breaker_call_async_infers_result() -> None:
    assert_type(await breaker.call_async(_fetch, 1), str)


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
