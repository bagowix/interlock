from interlock import (
    BulkheadFullError,
    CircuitOpenError,
    InterlockDeprecationWarning,
    InterlockError,
)


def test__circuit_open_error__type__is_an_interlock_error() -> None:
    assert issubclass(CircuitOpenError, InterlockError)


def test__circuit_open_error__message__names_the_breaker() -> None:
    error = CircuitOpenError('payments')
    assert 'payments' in str(error)


def test__circuit_open_error__context__is_exposed_as_attributes() -> None:
    cause = TimeoutError('upstream timed out')
    error = CircuitOpenError('payments', retry_after=2.5, last_failure=cause)
    assert error.breaker_name == 'payments'
    assert error.retry_after == 2.5
    assert error.last_failure is cause


def test__circuit_open_error__retry_after__appears_in_message_when_known() -> None:
    error = CircuitOpenError('payments', retry_after=2.5)
    assert '2.5' in str(error)


def test__interlock_deprecation_warning__visibility__subclasses_user_warning() -> None:
    assert issubclass(InterlockDeprecationWarning, UserWarning)


def test__bulkhead_full_error__message__names_the_limit() -> None:
    error = BulkheadFullError(3, max_wait=0.0)

    assert error.max_concurrent == 3
    assert error.max_wait == 0.0
    assert str(error) == 'Bulkhead is full: 3 calls in flight'


def test__bulkhead_full_error__with_wait__mentions_the_wait() -> None:
    error = BulkheadFullError(2, max_wait=0.5)

    assert str(error) == 'Bulkhead is full: 2 calls in flight; no slot freed within 0.500s'


def test__bulkhead_full_error__is_an_interlock_error() -> None:
    assert issubclass(BulkheadFullError, InterlockError)
