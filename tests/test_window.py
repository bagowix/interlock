from dataclasses import FrozenInstanceError

import pytest

from interlock import WindowSnapshot, WindowType


def test__window_type__members__count_and_time_based() -> None:
    assert set(WindowType) == {WindowType.COUNT_BASED, WindowType.TIME_BASED}
    assert WindowType.COUNT_BASED == 'count_based'


def test__window_snapshot__empty__rates_are_zero() -> None:
    snapshot = WindowSnapshot(total_calls=0, failed_calls=0, slow_calls=0)
    assert snapshot.failure_rate == 0.0
    assert snapshot.slow_call_rate == 0.0


def test__window_snapshot__failure_rate__is_failed_over_total() -> None:
    snapshot = WindowSnapshot(total_calls=10, failed_calls=3, slow_calls=0)
    assert snapshot.failure_rate == 0.3


def test__window_snapshot__slow_call_rate__is_slow_over_total() -> None:
    snapshot = WindowSnapshot(total_calls=8, failed_calls=0, slow_calls=2)
    assert snapshot.slow_call_rate == 0.25


def test__window_snapshot__is_frozen__rejects_mutation() -> None:
    snapshot = WindowSnapshot(total_calls=1, failed_calls=0, slow_calls=0)
    with pytest.raises(FrozenInstanceError):
        snapshot.total_calls = 5
