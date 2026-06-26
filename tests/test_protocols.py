from interlock import Clock


def test__clock__fake_clock__satisfies_protocol(fake_clock: Clock) -> None:
    assert isinstance(fake_clock, Clock)


def test__clock__object_without_monotonic__does_not_satisfy_protocol() -> None:
    assert not isinstance(object(), Clock)
