from interlock import State


def test__state__members__has_six_canonical_states() -> None:
    assert set(State) == {
        State.CLOSED,
        State.OPEN,
        State.HALF_OPEN,
        State.FORCED_OPEN,
        State.DISABLED,
        State.METRICS_ONLY,
    }


def test__state__values__are_stable_lowercase_identifiers() -> None:
    assert State.CLOSED == 'closed'
    assert State.HALF_OPEN == 'half_open'
    assert State.METRICS_ONLY == 'metrics_only'


def test__state__is_str__serialises_as_its_value() -> None:
    assert isinstance(State.OPEN, str)
    assert f'{State.OPEN}' == 'open'
