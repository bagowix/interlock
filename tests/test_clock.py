from interlock._clock import SystemClock


def test__system_clock__returns_float() -> None:
    assert isinstance(SystemClock().monotonic(), float)


def test__system_clock__is_non_decreasing() -> None:
    clock = SystemClock()
    first = clock.monotonic()
    second = clock.monotonic()

    assert second >= first
