from typing import cast

import pytest

from interlock import CircuitBreaker, State


def test__circuit_breaker__non_state_initial_value__raises_value_error() -> None:
    with pytest.raises(ValueError, match='initial_state'):
        CircuitBreaker(name='invalid', initial_state=cast('State', 'metrics_only'))
