import pytest

from interlock import Outcome


def test__outcome__members__cross_success_and_latency() -> None:
    assert set(Outcome) == {
        Outcome.SUCCESS,
        Outcome.FAILURE,
        Outcome.SLOW_SUCCESS,
        Outcome.SLOW_FAILURE,
    }


@pytest.mark.parametrize(
    ('outcome', 'expected'),
    [
        (Outcome.SUCCESS, False),
        (Outcome.SLOW_SUCCESS, False),
        (Outcome.FAILURE, True),
        (Outcome.SLOW_FAILURE, True),
    ],
)
def test__outcome__is_failure__counts_only_failures(outcome: Outcome, expected: bool) -> None:
    assert outcome.is_failure is expected


@pytest.mark.parametrize(
    ('outcome', 'expected'),
    [
        (Outcome.SUCCESS, False),
        (Outcome.FAILURE, False),
        (Outcome.SLOW_SUCCESS, True),
        (Outcome.SLOW_FAILURE, True),
    ],
)
def test__outcome__is_slow__counts_only_slow_variants(outcome: Outcome, expected: bool) -> None:
    assert outcome.is_slow is expected
