"""Tests for the tenacity integration (``interlock.integrations.tenacity``)."""

import pytest
from tenacity import (
    RetryError,
    Retrying,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
    wait_fixed,
)
from tests.conftest import FakeClock

from interlock import CircuitBreaker, CircuitOpenError, Config, State
from interlock.integrations.tenacity import retry_unless_open, wait_probe

_TRIP_FAST = Config(
    minimum_number_of_calls=2,
    failure_rate_threshold=0.5,
    wait_duration_in_open=10.0,
    permitted_calls_in_half_open=1,
    max_concurrent_probes=1,
)


def _noop_sleep(_seconds: float) -> None:
    return None


# --- retry_unless_open -------------------------------------------------------


def test__retry_unless_open__transient_failure__is_retried() -> None:
    attempts = 0

    def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError('transient')
        return 'ok'

    retrying = Retrying(
        retry=retry_unless_open(),
        stop=stop_after_attempt(5),
        sleep=_noop_sleep,
        reraise=True,
    )

    assert retrying(flaky) == 'ok'
    assert attempts == 3


def test__retry_unless_open__circuit_open_error__stops_immediately() -> None:
    attempts = 0

    def rejected() -> None:
        nonlocal attempts
        attempts += 1
        raise CircuitOpenError('payments', retry_after=1.0)

    retrying = Retrying(
        retry=retry_unless_open(),
        stop=stop_after_attempt(5),
        sleep=_noop_sleep,
        reraise=True,
    )

    with pytest.raises(CircuitOpenError):
        retrying(rejected)
    assert attempts == 1


def test__retry_unless_open__explicit_transient_types__other_exception_not_retried() -> None:
    attempts = 0

    def wrong_kind() -> None:
        nonlocal attempts
        attempts += 1
        raise KeyError('not transient')

    retrying = Retrying(
        retry=retry_unless_open(TimeoutError, ConnectionError),
        stop=stop_after_attempt(5),
        sleep=_noop_sleep,
        reraise=True,
    )

    with pytest.raises(KeyError):
        retrying(wrong_kind)
    assert attempts == 1


def test__retry_unless_open__explicit_transient_types__listed_exception_retried() -> None:
    attempts = 0

    def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise TimeoutError('transient')
        return 'ok'

    retrying = Retrying(
        retry=retry_unless_open(TimeoutError),
        stop=stop_after_attempt(5),
        sleep=_noop_sleep,
        reraise=True,
    )

    assert retrying(flaky) == 'ok'
    assert attempts == 2


# --- wait_probe --------------------------------------------------------------


def test__wait_probe__circuit_open_with_retry_after__waits_retry_after() -> None:
    waits: list[float] = []
    retrying = Retrying(
        retry=retry_if_exception_type(CircuitOpenError),
        wait=wait_probe(wait_fixed(2.0), jitter=0.0),
        stop=stop_after_attempt(3),
        sleep=waits.append,
        reraise=True,
    )

    def rejected() -> None:
        raise CircuitOpenError('payments', retry_after=7.5)

    with pytest.raises(CircuitOpenError):
        retrying(rejected)
    assert waits == [7.5, 7.5]


def test__wait_probe__jitter__bounded_above_retry_after() -> None:
    waits: list[float] = []
    retrying = Retrying(
        retry=retry_if_exception_type(CircuitOpenError),
        wait=wait_probe(wait_fixed(2.0), jitter=0.5),
        stop=stop_after_attempt(2),
        sleep=waits.append,
        reraise=True,
    )

    def rejected() -> None:
        raise CircuitOpenError('payments', retry_after=5.0)

    with pytest.raises(CircuitOpenError):
        retrying(rejected)
    assert len(waits) == 1
    assert 5.0 <= waits[0] <= 5.5


def test__wait_probe__retry_after_unknown__falls_back() -> None:
    waits: list[float] = []
    retrying = Retrying(
        retry=retry_if_exception_type(CircuitOpenError),
        wait=wait_probe(wait_fixed(2.0), jitter=0.0),
        stop=stop_after_attempt(2),
        sleep=waits.append,
        reraise=True,
    )

    def forced_open() -> None:
        raise CircuitOpenError('payments', retry_after=None)

    with pytest.raises(CircuitOpenError):
        retrying(forced_open)
    assert waits == [2.0]


def test__wait_probe__other_exception__falls_back() -> None:
    waits: list[float] = []
    retrying = Retrying(
        retry=retry_if_exception_type(ValueError),
        wait=wait_probe(wait_fixed(2.0), jitter=0.0),
        stop=stop_after_attempt(2),
        sleep=waits.append,
        reraise=True,
    )

    def transient() -> None:
        raise ValueError('transient')

    with pytest.raises(ValueError, match='transient'):
        retrying(transient)
    assert waits == [2.0]


def test__wait_probe__negative_jitter__raises_value_error() -> None:
    with pytest.raises(ValueError, match='jitter'):
        wait_probe(wait_fixed(1.0), jitter=-0.1)


# --- composition with a real breaker -----------------------------------------


def test__retry_around_breaker__fail_fast__stops_when_circuit_opens(
    fake_clock: FakeClock,
) -> None:
    breaker = CircuitBreaker(name='dep', config=_TRIP_FAST, clock=fake_clock)
    calls = 0

    def failing() -> None:
        nonlocal calls
        calls += 1
        raise ValueError('down')

    retrying = Retrying(
        retry=retry_unless_open(),
        stop=stop_after_attempt(10),
        sleep=_noop_sleep,
        reraise=True,
    )

    with pytest.raises(CircuitOpenError):
        retrying(breaker.call, failing)

    # Two recorded failures trip the breaker; the third attempt is rejected
    # before reaching the callable and must not be retried further.
    assert calls == 2
    assert breaker.state is State.OPEN


def test__retry_around_breaker__patient_mode__recovers_after_probe(
    fake_clock: FakeClock,
) -> None:
    breaker = CircuitBreaker(name='dep', config=_TRIP_FAST, clock=fake_clock)
    calls = 0

    def recovering() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ValueError('down')
        return 'ok'

    retrying = Retrying(
        retry=retry_if_exception_type((ValueError, CircuitOpenError)),
        wait=wait_probe(wait_fixed(0.5), jitter=0.0),
        stop=stop_after_attempt(10),
        sleep=fake_clock.advance,
        reraise=True,
    )

    result = retrying(breaker.call, recovering)

    # Attempts 1-2 fail and trip the breaker, attempt 3 is rejected and
    # wait_probe sleeps exactly until the next probe is allowed; attempt 4
    # runs as the probe, succeeds, and closes the circuit.
    assert result == 'ok'
    assert calls == 3
    assert breaker.state is State.CLOSED


def test__wait_probe__result_based_retry__falls_back() -> None:
    waits: list[float] = []
    retrying = Retrying(
        retry=retry_if_result(lambda result: result == 'pending'),
        wait=wait_probe(wait_fixed(2.0), jitter=0.0),
        stop=stop_after_attempt(2),
        sleep=waits.append,
    )

    def pending() -> str:
        return 'pending'

    with pytest.raises(RetryError):
        retrying(pending)
    assert waits == [2.0]
