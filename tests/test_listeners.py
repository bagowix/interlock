import logging

import pytest

from interlock import LoggingEventListener, Outcome, State


def test__on_state_change__logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    listener = LoggingEventListener(logging.getLogger('interlock.test'))

    with caplog.at_level(logging.WARNING, logger='interlock.test'):
        listener.on_state_change(name='svc', old=State.CLOSED, new=State.OPEN)

    assert 'closed -> open' in caplog.text
    assert 'svc' in caplog.text


def test__on_rejected__logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    listener = LoggingEventListener(logging.getLogger('interlock.test'))

    with caplog.at_level(logging.WARNING, logger='interlock.test'):
        listener.on_rejected(name='svc')

    assert 'rejected' in caplog.text


def test__on_reset__logs_info(caplog: pytest.LogCaptureFixture) -> None:
    listener = LoggingEventListener(logging.getLogger('interlock.test'))

    with caplog.at_level(logging.INFO, logger='interlock.test'):
        listener.on_reset(name='svc')

    assert 'reset' in caplog.text


def test__on_call__logs_debug(caplog: pytest.LogCaptureFixture) -> None:
    listener = LoggingEventListener(logging.getLogger('interlock.test'))

    with caplog.at_level(logging.DEBUG, logger='interlock.test'):
        listener.on_call(name='svc', outcome=Outcome.SUCCESS, duration=0.5)

    assert 'success' in caplog.text


def test__default_logger__uses_interlock_namespace(caplog: pytest.LogCaptureFixture) -> None:
    listener = LoggingEventListener()

    with caplog.at_level(logging.INFO, logger='interlock'):
        listener.on_reset(name='svc')

    assert any(record.name == 'interlock' for record in caplog.records)


def test__on_storage_degraded__logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    listener = LoggingEventListener(logging.getLogger('interlock.test'))

    with caplog.at_level(logging.WARNING, logger='interlock.test'):
        listener.on_storage_degraded(name='svc', error=ConnectionError('down'))

    assert 'degraded' in caplog.text
    assert 'down' in caplog.text


def test__on_storage_recovered__logs_info(caplog: pytest.LogCaptureFixture) -> None:
    listener = LoggingEventListener(logging.getLogger('interlock.test'))

    with caplog.at_level(logging.INFO, logger='interlock.test'):
        listener.on_storage_recovered(name='svc')

    assert 'recovered' in caplog.text


def test__on_retry__logs_info(caplog: pytest.LogCaptureFixture) -> None:
    listener = LoggingEventListener(logging.getLogger('interlock.test'))

    with caplog.at_level(logging.INFO, logger='interlock.test'):
        listener.on_retry(name='payments', attempt=2, delay=1.5)

    assert 'payments' in caplog.text
    assert 'attempt 2' in caplog.text


def test__on_bulkhead_rejected__logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    listener = LoggingEventListener(logging.getLogger('interlock.test'))

    with caplog.at_level(logging.WARNING, logger='interlock.test'):
        listener.on_bulkhead_rejected(name='db-pool')

    assert 'db-pool' in caplog.text
    assert 'bulkhead' in caplog.text


def test__on_fallback__logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    listener = LoggingEventListener(logging.getLogger('interlock.test'))

    with caplog.at_level(logging.WARNING, logger='interlock.test'):
        listener.on_fallback(name='recs', error=ValueError('down'))

    assert 'recs' in caplog.text
    assert 'fallback' in caplog.text
