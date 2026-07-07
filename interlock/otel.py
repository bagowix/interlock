"""OpenTelemetry metrics adapter — requires the ``otel`` extra.

This module imports ``opentelemetry`` and is deliberately *not* re-exported from
``interlock`` so the core stays zero-dependency. Install with
``pip install interlock[otel]`` and import explicitly::

    from interlock.otel import OTelEventListener

It maps breaker events onto three instruments: a duration histogram per call, a
counter of rejected calls, and a counter of state transitions (plus resets).
"""

from opentelemetry import metrics
from opentelemetry.metrics import Meter

from interlock.outcome import Outcome
from interlock.state import State

__all__ = ('OTelEventListener',)


class OTelEventListener:
    """Records breaker events as OpenTelemetry metrics.

    Args:
        meter: Meter to create instruments on. Defaults to
            ``opentelemetry.metrics.get_meter('interlock')``.
    """

    def __init__(self, meter: Meter | None = None) -> None:
        meter = meter if meter is not None else metrics.get_meter('interlock')
        self._calls = meter.create_histogram(
            'interlock.call.duration',
            unit='s',
            description='Duration of protected calls.',
        )
        self._rejected = meter.create_counter(
            'interlock.call.rejected',
            description='Calls rejected because the circuit was open.',
        )
        self._state_changes = meter.create_counter(
            'interlock.state.changes',
            description='Circuit breaker state transitions.',
        )
        self._resets = meter.create_counter(
            'interlock.reset',
            description='Manual breaker resets.',
        )
        self._storage_events = meter.create_counter(
            'interlock.storage.events',
            description='Shared storage degradations and recoveries.',
        )

    def on_state_change(self, *, name: str, old: State, new: State) -> None:
        """Count a state transition, labelled with the breaker and direction."""
        self._state_changes.add(1, {'breaker': name, 'from': str(old), 'to': str(new)})

    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None:
        """Record the call's duration, labelled with the breaker and outcome."""
        self._calls.record(duration, {'breaker': name, 'outcome': str(outcome)})

    def on_rejected(self, *, name: str) -> None:
        """Count a rejected call, labelled with the breaker."""
        self._rejected.add(1, {'breaker': name})

    def on_reset(self, *, name: str) -> None:
        """Count a manual reset, labelled with the breaker."""
        self._resets.add(1, {'breaker': name})

    def on_storage_degraded(self, *, name: str, error: BaseException) -> None:
        """Count a storage degradation, labelled with the breaker and error type."""
        self._storage_events.add(
            1, {'breaker': name, 'event': 'degraded', 'error': type(error).__name__}
        )

    def on_storage_recovered(self, *, name: str) -> None:
        """Count a storage recovery, labelled with the breaker."""
        self._storage_events.add(1, {'breaker': name, 'event': 'recovered'})
