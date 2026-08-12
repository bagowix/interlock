import pytest
from pytest_mock import MockerFixture

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


@pytest.mark.parametrize(
    'initial_state',
    [State.CLOSED, State.FORCED_OPEN, State.DISABLED, State.METRICS_ONLY],
)
def test__get__supported_initial_state__applies_to_every_new_breaker(
    initial_state: State,
) -> None:
    registry = Registry(initial_state=initial_state)

    assert registry.get('a').state is initial_state
    assert registry.get('b').state is initial_state


@pytest.mark.parametrize('initial_state', [State.OPEN, State.HALF_OPEN])
def test__init__transitional_initial_state__raises_value_error(initial_state: State) -> None:
    with pytest.raises(ValueError, match='initial_state'):
        Registry(initial_state=initial_state)


def test__get_existing__missing_name__does_not_create_breaker() -> None:
    registry = Registry()

    assert registry.get_existing('missing') is None


def test__get_existing__created_name__returns_cached_breaker() -> None:
    registry = Registry()
    breaker = registry.get('payments')

    assert registry.get_existing('payments') is breaker


def test__names__no_breaker_created__returns_empty_tuple() -> None:
    registry = Registry()

    assert registry.names() == ()


def test__names__created_breakers__returns_every_name() -> None:
    registry = Registry()
    registry.get('payments')
    registry.get('search')

    assert set(registry.names()) == {'payments', 'search'}


def test__names__breaker_created_after_the_call__is_absent_from_the_snapshot() -> None:
    registry = Registry()
    registry.get('payments')

    names = registry.names()
    registry.get('search')

    assert names == ('payments',)


def test__items__no_breaker_created__returns_empty_tuple() -> None:
    registry = Registry()

    assert registry.items() == ()


def test__items__created_breakers__pairs_each_name_with_its_breaker() -> None:
    registry = Registry()
    payments = registry.get('payments')
    search = registry.get('search')

    assert set(registry.items()) == {('payments', payments), ('search', search)}


def test__items__breaker_created_after_the_call__is_absent_from_the_snapshot() -> None:
    registry = Registry()
    payments = registry.get('payments')

    items = registry.items()
    registry.get('search')

    assert items == (('payments', payments),)


def test__close_all__one_breaker_raises__still_closes_the_rest(
    mocker: MockerFixture,
) -> None:
    registry = Registry()
    first = registry.get('first')
    second = registry.get('second')
    first_close = mocker.patch.object(first, 'close', autospec=True)
    mocker.patch.object(second, 'close', autospec=True, side_effect=RuntimeError('close failed'))

    with pytest.raises(RuntimeError, match='close failed'):
        registry.close_all()

    first_close.assert_called_once_with()


@pytest.mark.asyncio
async def test__aclose_all__one_breaker_raises__still_closes_the_rest(
    mocker: MockerFixture,
) -> None:
    registry = Registry()
    first = registry.get('first')
    second = registry.get('second')
    first_close = mocker.patch.object(first, 'aclose', autospec=True)
    mocker.patch.object(
        second,
        'aclose',
        autospec=True,
        side_effect=RuntimeError('close failed'),
    )

    with pytest.raises(RuntimeError, match='close failed'):
        await registry.aclose_all()

    first_close.assert_awaited_once_with()


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
