from conftest import FakeClock
from interlock import Outcome
from interlock._windows import CountBasedSlidingWindow, TimeBasedSlidingWindow
from interlock.protocols import SlidingWindow


def test__count_based__empty__snapshot_all_zero() -> None:
    window = CountBasedSlidingWindow(size=5)
    snapshot = window.snapshot()

    assert (snapshot.total_calls, snapshot.failed_calls, snapshot.slow_calls) == (0, 0, 0)


def test__count_based__under_capacity__counts_every_call() -> None:
    window = CountBasedSlidingWindow(size=5)
    window.record(Outcome.SUCCESS)
    window.record(Outcome.FAILURE)
    window.record(Outcome.SLOW_SUCCESS)
    snapshot = window.snapshot()

    assert snapshot.total_calls == 3
    assert snapshot.failed_calls == 1
    assert snapshot.slow_calls == 1


def test__count_based__slow_failure__counts_both_dimensions() -> None:
    window = CountBasedSlidingWindow(size=3)
    window.record(Outcome.SLOW_FAILURE)
    snapshot = window.snapshot()
    assert snapshot.failed_calls == 1
    assert snapshot.slow_calls == 1


def test__count_based__over_capacity__evicts_oldest() -> None:
    window = CountBasedSlidingWindow(size=3)
    for _ in range(3):
        window.record(Outcome.FAILURE)
    for _ in range(3):
        window.record(Outcome.SUCCESS)
    snapshot = window.snapshot()

    assert snapshot.total_calls == 3
    assert snapshot.failed_calls == 0


def test__count_based__partial_eviction__keeps_last_n() -> None:
    window = CountBasedSlidingWindow(size=3)
    window.record(Outcome.FAILURE)  # evicted by the fourth record
    window.record(Outcome.SUCCESS)
    window.record(Outcome.SUCCESS)
    window.record(Outcome.FAILURE)
    snapshot = window.snapshot()

    assert snapshot.total_calls == 3
    assert snapshot.failed_calls == 1


def test__count_based__satisfies_protocol() -> None:
    assert isinstance(CountBasedSlidingWindow(size=1), SlidingWindow)


def test__time_based__empty__snapshot_all_zero(fake_clock: FakeClock) -> None:
    window = TimeBasedSlidingWindow(size=5, clock=fake_clock)
    snapshot = window.snapshot()

    assert (snapshot.total_calls, snapshot.failed_calls, snapshot.slow_calls) == (0, 0, 0)


def test__time_based__same_second__accumulates(fake_clock: FakeClock) -> None:
    window = TimeBasedSlidingWindow(size=5, clock=fake_clock)
    window.record(Outcome.SUCCESS)
    window.record(Outcome.FAILURE)
    snapshot = window.snapshot()

    assert snapshot.total_calls == 2
    assert snapshot.failed_calls == 1


def test__time_based__slow_failure__counts_both_dimensions(fake_clock: FakeClock) -> None:
    window = TimeBasedSlidingWindow(size=5, clock=fake_clock)
    window.record(Outcome.SLOW_FAILURE)
    snapshot = window.snapshot()

    assert snapshot.failed_calls == 1
    assert snapshot.slow_calls == 1


def test__time_based__within_window__counts_all_seconds(fake_clock: FakeClock) -> None:
    window = TimeBasedSlidingWindow(size=5, clock=fake_clock)
    window.record(Outcome.FAILURE)
    fake_clock.advance(1)
    window.record(Outcome.SUCCESS)
    fake_clock.advance(1)
    window.record(Outcome.SUCCESS)
    snapshot = window.snapshot()

    assert snapshot.total_calls == 3
    assert snapshot.failed_calls == 1


def test__time_based__beyond_window__evicts_old_seconds(fake_clock: FakeClock) -> None:
    window = TimeBasedSlidingWindow(size=3, clock=fake_clock)
    window.record(Outcome.FAILURE)  # second 0
    fake_clock.advance(3)  # window now covers seconds 1..3
    window.record(Outcome.SUCCESS)  # second 3
    snapshot = window.snapshot()

    assert snapshot.total_calls == 1
    assert snapshot.failed_calls == 0


def test__time_based__time_passing__expires_calls_without_new_records(
    fake_clock: FakeClock,
) -> None:
    window = TimeBasedSlidingWindow(size=2, clock=fake_clock)
    window.record(Outcome.FAILURE)  # second 0

    assert window.snapshot().total_calls == 1
    fake_clock.advance(2)  # second 0 falls outside the 2-second window
    assert window.snapshot().total_calls == 0


def test__time_based__large_time_jump__stale_buckets_do_not_resurface(
    fake_clock: FakeClock,
) -> None:
    window = TimeBasedSlidingWindow(size=3, clock=fake_clock)
    for _ in range(3):
        window.record(Outcome.FAILURE)
        fake_clock.advance(1)

    fake_clock.advance(100)
    assert window.snapshot().total_calls == 0

    window.record(Outcome.SUCCESS)
    snapshot = window.snapshot()
    assert snapshot.total_calls == 1
    assert snapshot.failed_calls == 0


def test__time_based__satisfies_protocol(fake_clock: FakeClock) -> None:
    assert isinstance(TimeBasedSlidingWindow(size=1, clock=fake_clock), SlidingWindow)
