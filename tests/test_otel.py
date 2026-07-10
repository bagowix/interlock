from unittest.mock import Mock

from interlock import Outcome, State
from interlock.integrations.otel import OTelEventListener


def _meter_with_named_instruments() -> tuple[Mock, dict[str, Mock]]:
    instruments: dict[str, Mock] = {}

    def make(name: str, **_: object) -> Mock:
        instrument = Mock()
        instruments[name] = instrument
        return instrument

    meter = Mock()
    meter.create_histogram.side_effect = make
    meter.create_counter.side_effect = make
    return meter, instruments


def test__on_call__records_duration_histogram() -> None:
    meter, instruments = _meter_with_named_instruments()
    listener = OTelEventListener(meter=meter)

    listener.on_call(name='svc', outcome=Outcome.SLOW_SUCCESS, duration=2.5)

    instruments['interlock.call.duration'].record.assert_called_once_with(
        2.5, {'breaker': 'svc', 'outcome': 'slow_success'}
    )


def test__on_rejected__increments_rejected_counter() -> None:
    meter, instruments = _meter_with_named_instruments()
    listener = OTelEventListener(meter=meter)

    listener.on_rejected(name='svc')

    instruments['interlock.call.rejected'].add.assert_called_once_with(1, {'breaker': 'svc'})


def test__on_state_change__increments_state_counter_with_labels() -> None:
    meter, instruments = _meter_with_named_instruments()
    listener = OTelEventListener(meter=meter)

    listener.on_state_change(name='svc', old=State.CLOSED, new=State.OPEN)

    instruments['interlock.state.changes'].add.assert_called_once_with(
        1, {'breaker': 'svc', 'from': 'closed', 'to': 'open'}
    )


def test__on_reset__increments_reset_counter() -> None:
    meter, instruments = _meter_with_named_instruments()
    listener = OTelEventListener(meter=meter)

    listener.on_reset(name='svc')

    instruments['interlock.reset'].add.assert_called_once_with(1, {'breaker': 'svc'})


def test__default_meter__constructs_and_is_usable() -> None:
    listener = OTelEventListener()

    listener.on_call(name='svc', outcome=Outcome.SUCCESS, duration=0.1)


def test__on_storage_degraded__counts_event_with_error_label() -> None:
    meter, instruments = _meter_with_named_instruments()
    listener = OTelEventListener(meter=meter)

    listener.on_storage_degraded(name='svc', error=ConnectionError('down'))

    instruments['interlock.storage.events'].add.assert_called_once_with(
        1, {'breaker': 'svc', 'event': 'degraded', 'error': 'ConnectionError'}
    )


def test__on_storage_recovered__counts_event() -> None:
    meter, instruments = _meter_with_named_instruments()
    listener = OTelEventListener(meter=meter)

    listener.on_storage_recovered(name='svc')

    instruments['interlock.storage.events'].add.assert_called_once_with(
        1, {'breaker': 'svc', 'event': 'recovered'}
    )


def test__on_retry__counts_a_pipeline_event() -> None:
    meter, instruments = _meter_with_named_instruments()
    listener = OTelEventListener(meter=meter)

    listener.on_retry(name='payments-retry', attempt=2, delay=1.5)

    instruments['interlock.pipeline.events'].add.assert_called_once_with(
        1, {'strategy': 'payments-retry', 'event': 'retry'}
    )


def test__on_bulkhead_rejected__counts_a_pipeline_event() -> None:
    meter, instruments = _meter_with_named_instruments()
    listener = OTelEventListener(meter=meter)

    listener.on_bulkhead_rejected(name='db-pool')

    instruments['interlock.pipeline.events'].add.assert_called_once_with(
        1, {'strategy': 'db-pool', 'event': 'bulkhead_rejected'}
    )


def test__on_fallback__counts_a_pipeline_event_with_the_error_type() -> None:
    meter, instruments = _meter_with_named_instruments()
    listener = OTelEventListener(meter=meter)

    listener.on_fallback(name='recs', error=ValueError('down'))

    instruments['interlock.pipeline.events'].add.assert_called_once_with(
        1, {'strategy': 'recs', 'event': 'fallback', 'error': 'ValueError'}
    )
