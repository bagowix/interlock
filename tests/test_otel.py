from unittest.mock import Mock

from interlock import Outcome, State
from interlock.otel import OTelEventListener


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
