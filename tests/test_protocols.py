from tests.conftest import FakeClock
from tests.inmemory_storage import AsyncInMemoryStorage, InMemoryStorage

from interlock import AsyncStorage, Clock, Storage


def test__clock__fake_clock__satisfies_protocol(fake_clock: Clock) -> None:
    assert isinstance(fake_clock, Clock)


def test__clock__object_without_monotonic__does_not_satisfy_protocol() -> None:
    assert not isinstance(object(), Clock)


def test__storage__in_memory__satisfies_protocol() -> None:
    assert isinstance(InMemoryStorage(clock=FakeClock()), Storage)


def test__storage__object_without_methods__does_not_satisfy_protocol() -> None:
    assert not isinstance(object(), Storage)


def test__async_storage__in_memory__satisfies_protocol() -> None:
    assert isinstance(AsyncInMemoryStorage(clock=FakeClock()), AsyncStorage)
