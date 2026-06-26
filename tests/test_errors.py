from interlock import CircuitOpenError, InterlockDeprecationWarning, InterlockError


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
