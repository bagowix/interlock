import asyncio
import threading

import pytest

from interlock import CallTimeoutError, CircuitBreaker, Config, sync_timeout, timeout


@pytest.mark.asyncio
async def test__within_deadline__does_not_raise() -> None:
    async with timeout(1.0):
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test__exceeds_deadline__raises_call_timeout_error() -> None:
    with pytest.raises(CallTimeoutError):
        async with timeout(0.01):
            await asyncio.sleep(5)


@pytest.mark.asyncio
async def test__call_timeout_error__carries_deadline() -> None:
    with pytest.raises(CallTimeoutError) as exc_info:
        async with timeout(0.01):
            await asyncio.sleep(5)

    assert exc_info.value.timeout == 0.01


@pytest.mark.asyncio
async def test__body_raises__propagates_unchanged() -> None:
    with pytest.raises(ValueError, match='boom'):
        async with timeout(1.0):
            raise ValueError('boom')


@pytest.mark.asyncio
async def test__composed_with_breaker__records_slow_failure() -> None:
    breaker = CircuitBreaker(
        name='io',
        config=Config(
            minimum_number_of_calls=1,
            window_size=10,
            slow_call_duration_threshold=0.01,
            permitted_calls_in_half_open=1,
            max_concurrent_probes=1,
            wait_duration_in_open=5.0,
        ),
    )

    async def hang() -> None:
        async with timeout(0.05):
            await asyncio.sleep(5)

    with pytest.raises(CallTimeoutError):
        await breaker.call(hang)

    snapshot = breaker.snapshot()
    assert snapshot.failed_calls == 1
    assert snapshot.slow_calls == 1


def test__sync_timeout__within_deadline__returns_result() -> None:
    @sync_timeout(1.0)
    def quick() -> int:
        return 42

    assert quick() == 42


def test__sync_timeout__preserves_arguments() -> None:
    @sync_timeout(1.0)
    def add(a: int, b: int) -> int:
        return a + b

    assert add(2, b=3) == 5


def test__sync_timeout__exceeds_deadline__raises_call_timeout_error() -> None:
    release = threading.Event()

    @sync_timeout(0.05)
    def hang() -> None:
        release.wait()

    try:
        with pytest.raises(CallTimeoutError):
            hang()
    finally:
        release.set()


def test__sync_timeout__call_timeout_error__carries_deadline() -> None:
    release = threading.Event()

    @sync_timeout(0.05)
    def hang() -> None:
        release.wait()

    try:
        with pytest.raises(CallTimeoutError) as exc_info:
            hang()
        assert exc_info.value.timeout == 0.05
    finally:
        release.set()


def test__sync_timeout__body_raises__propagates_unchanged() -> None:
    @sync_timeout(1.0)
    def boom() -> None:
        raise ValueError('boom')

    with pytest.raises(ValueError, match='boom'):
        boom()


def test__sync_timeout__non_positive_seconds__raises_value_error() -> None:
    with pytest.raises(ValueError, match='positive'):
        sync_timeout(0)


def test__sync_timeout__composed_with_breaker__records_slow_failure() -> None:
    breaker = CircuitBreaker(
        name='io',
        config=Config(
            minimum_number_of_calls=1,
            window_size=10,
            slow_call_duration_threshold=0.01,
            permitted_calls_in_half_open=1,
            max_concurrent_probes=1,
            wait_duration_in_open=5.0,
        ),
    )
    release = threading.Event()

    @sync_timeout(0.05)
    def hang() -> None:
        release.wait()

    try:
        with pytest.raises(CallTimeoutError):
            breaker.call(hang)

        snapshot = breaker.snapshot()
        assert snapshot.failed_calls == 1
        assert snapshot.slow_calls == 1
    finally:
        release.set()
