"""Benchmark for modest threaded contention around one shared breaker.

Every protected call takes the breaker's single ``threading.Lock`` twice —
once to admit, once to record the outcome — so calls from several threads all
funnel through it. This benchmark drives one closed breaker from four worker
threads so a change in the work done under that lock shows up as a trend.

Read the number with care: CodSpeed's simulation mode counts CPU instructions
under Valgrind, which runs threads one at a time. What is measured is the
instruction cost of the shared lock path when calls arrive from many threads —
not the waiting, scheduling or cache traffic real contention adds to
wall-clock time. Behaviour under real contention is asserted by
``tests/test_concurrency.py``, including on free-threaded CPython.
"""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

import pytest
from pytest_codspeed import BenchmarkFixture

from interlock import CircuitBreaker, Config, State

_THREADS = 4
_CALLS_PER_THREAD = 25
_CLOSED_CONFIG = Config(window_size=100, minimum_number_of_calls=10)


def _work(left: int, right: int) -> int:
    """The protected payload: cheap enough to expose the breaker's own cost."""
    return left + right


@pytest.fixture
def executor() -> Iterator[ThreadPoolExecutor]:
    """A worker pool reused across iterations, so thread startup is excluded."""
    with ThreadPoolExecutor(max_workers=_THREADS) as pool:
        yield pool


def test_contended_calls(benchmark: BenchmarkFixture, executor: ThreadPoolExecutor) -> None:
    """Four threads sharing one closed breaker: the lock path under contention."""
    breaker = CircuitBreaker(name='bench-contended', config=_CLOSED_CONFIG)

    def hammer() -> int:
        total = 0
        for _ in range(_CALLS_PER_THREAD):
            total += breaker.call(_work, 1, 2)
        return total

    def contended_round() -> int:
        futures = [executor.submit(hammer) for _ in range(_THREADS)]
        return sum(future.result() for future in futures)

    assert contended_round() == _THREADS * _CALLS_PER_THREAD * 3
    assert breaker.state is State.CLOSED
    benchmark(contended_round)
