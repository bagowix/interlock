"""Tests for the tenacity integration (``interlock.integrations.tenacity``)."""

import importlib
import sys

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
from interlock.integrations.tenacity import RetryStrategy, retry_unless_open, wait_probe
from interlock.pipeline import CircuitBreakerStrategy, Pipeline, Strategy

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


# --- RetryStrategy -----------------------------------------------------------


def test__retry_strategy__attempts_below_one__raises_value_error() -> None:
    with pytest.raises(ValueError, match='attempts'):
        RetryStrategy(attempts=0)


def test__retry_strategy__conforms_to_strategy_protocol() -> None:
    assert isinstance(RetryStrategy(sleep=_noop_sleep), Strategy)


def test__retry_strategy__transient_failures__retries_to_success() -> None:
    attempts = 0

    def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError('transient')
        return 'ok'

    pipeline = Pipeline(RetryStrategy(attempts=3, sleep=_noop_sleep))

    assert pipeline.call(flaky) == 'ok'
    assert attempts == 3


def test__retry_strategy__attempts_exhausted__reraises_the_original_error() -> None:
    attempts = 0

    def failing() -> None:
        nonlocal attempts
        attempts += 1
        raise ValueError('still down')

    pipeline = Pipeline(RetryStrategy(attempts=2, sleep=_noop_sleep))

    with pytest.raises(ValueError, match='still down'):
        pipeline.call(failing)
    assert attempts == 2


def test__retry_strategy__circuit_open__not_retried_by_default() -> None:
    attempts = 0

    def rejected() -> None:
        nonlocal attempts
        attempts += 1
        raise CircuitOpenError('payments', retry_after=1.0)

    pipeline = Pipeline(RetryStrategy(attempts=5, sleep=_noop_sleep))

    with pytest.raises(CircuitOpenError):
        pipeline.call(rejected)
    assert attempts == 1


@pytest.mark.asyncio
async def test__retry_strategy__async_transient_failures__retries_to_success() -> None:
    attempts = 0
    naps: list[float] = []

    async def record_nap(seconds: float) -> None:
        naps.append(seconds)

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError('transient')
        return 'ok'

    pipeline = Pipeline(RetryStrategy(attempts=3, wait=wait_fixed(1.5), async_sleep=record_nap))

    assert await pipeline.call(flaky) == 'ok'
    assert attempts == 3
    assert naps == [1.5, 1.5]


@pytest.mark.asyncio
async def test__retry_strategy__async_circuit_open__not_retried_by_default() -> None:
    attempts = 0

    async def record_nap(_seconds: float) -> None:
        return None

    async def rejected() -> None:
        nonlocal attempts
        attempts += 1
        raise CircuitOpenError('payments', retry_after=1.0)

    pipeline = Pipeline(RetryStrategy(attempts=5, async_sleep=record_nap))

    with pytest.raises(CircuitOpenError):
        await pipeline.call(rejected)
    assert attempts == 1


def test__retry_strategy__custom_wait__drives_the_delays() -> None:
    waits: list[float] = []
    attempts = 0

    def failing() -> None:
        nonlocal attempts
        attempts += 1
        raise ValueError('down')

    pipeline = Pipeline(RetryStrategy(attempts=3, wait=wait_fixed(2.5), sleep=waits.append))

    with pytest.raises(ValueError, match='down'):
        pipeline.call(failing)
    assert waits == [2.5, 2.5]


def test__retry_strategy__before_sleep__observes_every_retry() -> None:
    observed: list[int] = []

    def failing() -> None:
        raise ValueError('down')

    strategy = RetryStrategy(
        attempts=3,
        sleep=_noop_sleep,
        before_sleep=lambda retry_state: observed.append(retry_state.attempt_number),
    )

    with pytest.raises(ValueError, match='down'):
        Pipeline(strategy).call(failing)
    assert observed == [1, 2]


def test__retry_strategy__retry_outside_breaker__stops_once_the_circuit_opens(
    fake_clock: FakeClock,
) -> None:
    """The recommended order: every attempt is a breaker call, rejection ends the retry."""
    breaker = CircuitBreaker(name='dep', config=_TRIP_FAST, clock=fake_clock)
    pipeline = Pipeline(
        RetryStrategy(attempts=5, sleep=_noop_sleep),
        CircuitBreakerStrategy(breaker),
    )
    reached = 0

    def failing() -> None:
        nonlocal reached
        reached += 1
        raise ValueError('down')

    with pytest.raises(CircuitOpenError):
        pipeline.call(failing)

    assert reached == 2  # attempts 1-2 trip the breaker; attempt 3 is rejected and not retried
    assert breaker.state is State.OPEN


def test__retry_strategy__patient_mode__waits_for_the_probe_and_recovers(
    fake_clock: FakeClock,
) -> None:
    breaker = CircuitBreaker(name='dep', config=_TRIP_FAST, clock=fake_clock)
    pipeline = Pipeline(
        RetryStrategy(
            attempts=5,
            retry=retry_if_exception_type((ValueError, CircuitOpenError)),
            wait=wait_probe(wait_fixed(0.0), jitter=0.0),
            sleep=fake_clock.advance,
        ),
        CircuitBreakerStrategy(breaker),
    )
    reached = 0

    def recovering() -> str:
        nonlocal reached
        reached += 1
        if reached < 3:
            raise ValueError('down')
        return 'ok'

    # Attempts 1-2 fail and trip the breaker; attempt 3 is rejected and
    # wait_probe advances the clock to the probe window; attempt 4 is the
    # probe, succeeds, and closes the circuit.
    assert pipeline.call(recovering) == 'ok'
    assert reached == 3
    assert breaker.state is State.CLOSED


def test__module__imported_without_tenacity__raises_a_helpful_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module('interlock.integrations.tenacity')
    try:
        monkeypatch.setitem(sys.modules, 'tenacity', None)
        with pytest.raises(ImportError, match=r'interlock-cb\[tenacity\]'):
            importlib.reload(module)
    finally:
        monkeypatch.undo()
        importlib.reload(module)
