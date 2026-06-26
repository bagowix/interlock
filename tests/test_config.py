from dataclasses import FrozenInstanceError

import pytest

from interlock import Config, WindowType


def test__config__defaults__construct_without_error() -> None:
    config = Config()
    assert 0.0 < config.failure_rate_threshold <= 1.0
    assert config.window_type is WindowType.COUNT_BASED
    assert config.max_concurrent_probes <= config.permitted_calls_in_half_open


def test__config__valid_custom_values__are_stored() -> None:
    config = Config(
        failure_rate_threshold=0.25,
        window_type=WindowType.TIME_BASED,
        window_size=30,
    )
    assert config.failure_rate_threshold == 0.25
    assert config.window_type is WindowType.TIME_BASED
    assert config.window_size == 30


def test__config__is_frozen__rejects_mutation() -> None:
    config = Config()
    with pytest.raises(FrozenInstanceError):
        config.failure_rate_threshold = 0.9


@pytest.mark.parametrize(
    ('kwargs', 'match'),
    [
        ({'failure_rate_threshold': 0.0}, 'failure_rate_threshold'),
        ({'failure_rate_threshold': 1.5}, 'failure_rate_threshold'),
        ({'slow_call_rate_threshold': 0.0}, 'slow_call_rate_threshold'),
        ({'slow_call_rate_threshold': 2.0}, 'slow_call_rate_threshold'),
        ({'minimum_number_of_calls': 0}, 'minimum_number_of_calls'),
        ({'slow_call_duration_threshold': 0.0}, 'slow_call_duration_threshold'),
        ({'wait_duration_in_open': 0.0}, 'wait_duration_in_open'),
        ({'permitted_calls_in_half_open': 0}, 'permitted_calls_in_half_open'),
        ({'max_concurrent_probes': 0}, 'max_concurrent_probes'),
        ({'window_size': 0}, 'window_size'),
    ],
)
def test__config__invalid_value__raises_value_error(
    kwargs: dict[str, object],
    match: str,
) -> None:
    with pytest.raises(ValueError, match=match):
        Config(**kwargs)


def test__config__concurrent_probes_above_permitted__raises_value_error() -> None:
    with pytest.raises(ValueError, match='max_concurrent_probes'):
        Config(permitted_calls_in_half_open=3, max_concurrent_probes=5)
