import pytest

from conftest import FakeClock
from interlock import CircuitBreaker, Config, Registry, State


def _fail_twice(breaker: CircuitBreaker) -> None:
    def boom() -> None:
        raise ValueError('boom')

    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            breaker.call(boom)


def test__get__returns_circuit_breaker_with_name() -> None:
    registry = Registry()

    breaker = registry.get('payments')

    assert isinstance(breaker, CircuitBreaker)
    assert breaker.name == 'payments'


def test__get__same_name__returns_same_instance() -> None:
    registry = Registry()

    assert registry.get('a') is registry.get('a')


def test__get__different_names__returns_distinct_instances() -> None:
    registry = Registry()

    assert registry.get('a') is not registry.get('b')


def test__get__per_name_override__applies_custom_config(fake_clock: FakeClock) -> None:
    registry = Registry(config=Config(minimum_number_of_calls=10), clock=fake_clock)
    override = Config(minimum_number_of_calls=2, window_size=10)

    breaker = registry.get('sensitive', config=override)
    _fail_twice(breaker)

    assert breaker.state is State.OPEN


def test__get__default_config__is_shared(fake_clock: FakeClock) -> None:
    registry = Registry(config=Config(minimum_number_of_calls=10), clock=fake_clock)

    breaker = registry.get('lenient')
    _fail_twice(breaker)

    assert breaker.state is State.CLOSED  # 2 < default minimum_number_of_calls (10)
