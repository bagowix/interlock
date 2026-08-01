"""Invariants that must survive real thread contention.

The rest of the suite drives one breaker from one thread, so it can only prove
the logic, never the locking. These tests run many threads through a *single*
breaker at once and assert the four properties the `threading.Lock` in
`_engine.py` exists to provide: window counts add up, no snapshot is torn,
HALF_OPEN probe caps hold, and one name means one breaker.

They pass under the GIL too, but they are weak there — bytecode-level atomicity
hides most of what they look for. Their real venue is free-threaded CPython
(3.14t), where CI runs the suite as well (#102). Nothing here sleeps: threads
rendezvous on a `Barrier` and time still comes from `FakeClock`.
"""

import contextlib
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from interlock import CircuitBreaker, CircuitOpenError, Config, Registry, State

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests.conftest import FakeClock, RecordingListener

THREADS = 8
CALLS_PER_THREAD = 24
TIMEOUT = 10.0


def _run(worker: 'Callable[[], None]', *, threads: int = THREADS) -> None:
    """Run ``worker`` on ``threads`` real threads; re-raise whatever they raise."""
    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(worker) for _ in range(threads)]
        for future in futures:
            future.result(timeout=TIMEOUT)


def _boom() -> None:
    raise RuntimeError('boom')


def _ok() -> None:
    pass


def _alternating(breaker: CircuitBreaker, *, calls: int) -> None:
    """Run ``calls`` calls, every other one failing; swallow what comes back."""
    for index in range(calls):
        with contextlib.suppress(RuntimeError, CircuitOpenError):
            breaker.call(_boom if index % 2 else _ok)


def test__window__concurrent_records__counts_add_up(fake_clock: 'FakeClock') -> None:
    breaker = CircuitBreaker(
        name='counts',
        config=Config(window_size=THREADS * CALLS_PER_THREAD),
        clock=fake_clock,
    )
    # METRICS_ONLY records every outcome but never trips, so the expected
    # totals are arithmetic rather than a race with the threshold.
    breaker.metrics_only()
    start = threading.Barrier(THREADS)

    def worker() -> None:
        start.wait(timeout=TIMEOUT)
        _alternating(breaker, calls=CALLS_PER_THREAD)

    _run(worker)

    snapshot = breaker.snapshot()
    assert snapshot.total_calls == THREADS * CALLS_PER_THREAD
    assert snapshot.failed_calls == THREADS * CALLS_PER_THREAD // 2


def test__snapshot__taken_during_concurrent_records__is_never_torn(
    fake_clock: 'FakeClock',
) -> None:
    window_size = THREADS * CALLS_PER_THREAD
    breaker = CircuitBreaker(
        name='torn',
        config=Config(window_size=window_size),
        clock=fake_clock,
    )
    breaker.metrics_only()
    recording_done = threading.Event()
    torn: list[object] = []

    def read() -> None:
        while not recording_done.is_set():
            snapshot = breaker.snapshot()
            if not (
                0 <= snapshot.failed_calls <= snapshot.total_calls <= window_size
                and 0 <= snapshot.slow_calls <= snapshot.total_calls
                and 0.0 <= snapshot.failure_rate <= 1.0
            ):
                torn.append(snapshot)

    def write() -> None:
        _alternating(breaker, calls=CALLS_PER_THREAD)

    reader = threading.Thread(target=read)
    reader.start()
    try:
        _run(write)
    finally:
        recording_done.set()
        reader.join(timeout=TIMEOUT)

    assert not reader.is_alive()
    assert torn == []


def test__half_open__concurrent_probes__never_exceeds_the_caps(fake_clock: 'FakeClock') -> None:
    config = Config(
        window_size=2,
        minimum_number_of_calls=2,
        wait_duration_in_open=30.0,
        permitted_calls_in_half_open=3,
        max_concurrent_probes=2,
    )
    breaker = CircuitBreaker(name='probes', config=config, clock=fake_clock)
    for _ in range(2):
        with contextlib.suppress(RuntimeError):
            breaker.call(_boom)
    assert breaker.state is State.OPEN
    fake_clock.advance(31.0)

    # Every thread reaches the barrier: admitted ones while their probe is still
    # in flight, rejected ones right after CircuitOpenError. So the peak is
    # measured while the maximum possible number of probes is running at once.
    rendezvous = threading.Barrier(THREADS)
    counters = threading.Lock()
    in_flight = 0
    peak = 0
    admitted = 0

    def probe() -> None:
        nonlocal in_flight, peak, admitted
        with counters:
            in_flight += 1
            admitted += 1
            peak = max(peak, in_flight)
        rendezvous.wait(timeout=TIMEOUT)
        with counters:
            in_flight -= 1

    def worker() -> None:
        try:
            breaker.call(probe)
        except CircuitOpenError:
            rendezvous.wait(timeout=TIMEOUT)

    _run(worker)

    assert admitted > 0, 'no probe was admitted — the test proved nothing'
    assert peak <= config.max_concurrent_probes
    assert admitted <= config.permitted_calls_in_half_open


def test__registry__concurrent_get__hands_out_one_breaker_per_name(
    fake_clock: 'FakeClock',
) -> None:
    registry = Registry(clock=fake_clock)
    start = threading.Barrier(THREADS)
    seen: list[CircuitBreaker] = []
    collected = threading.Lock()

    def worker() -> None:
        start.wait(timeout=TIMEOUT)
        breaker = registry.get('shared')
        with collected:
            seen.append(breaker)

    _run(worker)

    assert len(seen) == THREADS
    assert len({id(breaker) for breaker in seen}) == 1


def test__trip__concurrent_failures__emits_one_state_change(
    fake_clock: 'FakeClock',
    listener: 'RecordingListener',
) -> None:
    breaker = CircuitBreaker(
        name='once',
        config=Config(window_size=THREADS * CALLS_PER_THREAD, minimum_number_of_calls=2),
        clock=fake_clock,
        listener=listener,
    )
    start = threading.Barrier(THREADS)

    def worker() -> None:
        start.wait(timeout=TIMEOUT)
        for _ in range(CALLS_PER_THREAD):
            with contextlib.suppress(RuntimeError, CircuitOpenError):
                breaker.call(_boom)

    _run(worker)

    assert breaker.state is State.OPEN
    assert listener.state_changes == [(State.CLOSED, State.OPEN)]
