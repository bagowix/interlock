"""Concrete sliding-window implementations injected into the core.

Two strategies satisfy the ``SlidingWindow`` protocol: ``CountBasedSlidingWindow``
keeps the last N calls in a ring buffer; ``TimeBasedSlidingWindow`` keeps calls
from the last N seconds in per-second buckets. Both expose running aggregates so
``snapshot`` stays cheap, and both are constructed by the core from a validated
``Config`` — they trust their ``size`` and never validate it themselves.
"""

from dataclasses import dataclass

from interlock.config import Config
from interlock.outcome import Outcome
from interlock.protocols import Clock, SlidingWindow
from interlock.window import WindowSnapshot, WindowType

__all__ = ('CountBasedSlidingWindow', 'TimeBasedSlidingWindow', 'build_window')


def build_window(*, config: Config, clock: Clock) -> SlidingWindow:
    """Construct the window implementation selected by ``config``.

    Time-based windows need the clock; count-based ones ignore it. The core
    calls this whenever it needs fresh metrics — on construction and whenever a
    breaker returns to ``CLOSED``.
    """
    if config.window_type is WindowType.COUNT_BASED:
        return CountBasedSlidingWindow(size=config.window_size)

    return TimeBasedSlidingWindow(size=config.window_size, clock=clock)


class CountBasedSlidingWindow:
    """Aggregates the most recent ``size`` outcomes via a ring buffer.

    Running counters are adjusted on each record — incremented for the new
    outcome, decremented for the one it evicts — so ``snapshot`` is O(1).
    """

    def __init__(self, *, size: int) -> None:
        self._buffer: list[Outcome | None] = [None] * size
        self._head = 0
        self._total = 0
        self._failed = 0
        self._slow = 0

    def record(self, outcome: Outcome) -> None:
        evicted = self._buffer[self._head]
        if evicted is not None:
            self._total -= 1
            self._failed -= evicted.is_failure
            self._slow -= evicted.is_slow

        self._buffer[self._head] = outcome
        self._total += 1
        self._failed += outcome.is_failure
        self._slow += outcome.is_slow

        self._head = (self._head + 1) % len(self._buffer)

    def snapshot(self) -> WindowSnapshot:
        return WindowSnapshot(
            total_calls=self._total,
            failed_calls=self._failed,
            slow_calls=self._slow,
        )


@dataclass(slots=True)
class _Bucket:
    """Per-second aggregate. ``epoch_second`` tags which second it holds."""

    epoch_second: int
    total: int = 0
    failed: int = 0
    slow: int = 0


class TimeBasedSlidingWindow:
    """Aggregates outcomes from the last ``size`` seconds in per-second buckets.

    Bucket ``i`` holds the second ``epoch % size == i``; touching a bucket whose
    tag no longer matches the current second resets it, so a wrapped index never
    carries a stale count. ``snapshot`` sums only buckets whose tag still falls
    inside the window, which also expires calls as time passes without records.
    """

    def __init__(self, *, size: int, clock: Clock) -> None:
        self._size = size
        self._clock = clock
        self._buckets = [_Bucket(epoch_second=-1) for _ in range(size)]

    def _now_second(self) -> int:
        return int(self._clock.monotonic())

    def record(self, outcome: Outcome) -> None:
        now = self._now_second()
        bucket = self._buckets[now % self._size]
        if bucket.epoch_second != now:
            bucket.epoch_second = now
            bucket.total = bucket.failed = bucket.slow = 0

        bucket.total += 1
        bucket.failed += outcome.is_failure
        bucket.slow += outcome.is_slow

    def snapshot(self) -> WindowSnapshot:
        now = self._now_second()
        oldest = max(0, now - self._size + 1)

        total = failed = slow = 0
        for bucket in self._buckets:
            if oldest <= bucket.epoch_second <= now:
                total += bucket.total
                failed += bucket.failed
                slow += bucket.slow

        return WindowSnapshot(total_calls=total, failed_calls=failed, slow_calls=slow)
