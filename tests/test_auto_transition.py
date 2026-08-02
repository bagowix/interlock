"""Tests for the auto-transition timer.

The first half is end-to-end: unlike the deterministic FakeClock tests, they
exercise the real ``threading.Timer`` with a short real ``wait_duration_in_open``
and wait on a thread-safe ``Event`` the listener sets when the breaker reaches
``HALF_OPEN``.

The second half is about the timer's *lifecycle* rather than its firing — armed
exactly when the local machine enters ``OPEN``, cancelled exactly when it leaves,
and untouched by everything else. Those tests inspect ``Engine._timer`` directly
and use a wait long enough that the real timer can never fire during them.
"""

import asyncio
import threading
from collections.abc import Iterator

import pytest

from conftest import FakeClock
from interlock import CircuitBreaker, CircuitOpenError, Config, State
from interlock._engine import Engine
from interlock.shared import SharedState

_WAIT = 0.05


class _HalfOpenSignal:
    """Listener that flags when the breaker proactively reaches HALF_OPEN."""

    def __init__(self) -> None:
        self.reached_half_open = threading.Event()
        self.state_changes: list[tuple[State, State]] = []

    def on_state_change(self, *, name: str, old: State, new: State) -> None:
        self.state_changes.append((old, new))
        if new is State.HALF_OPEN:
            self.reached_half_open.set()

    def on_call(self, *, name: str, outcome: object, duration: float) -> None: ...
    def on_rejected(self, *, name: str) -> None: ...
    def on_reset(self, *, name: str) -> None: ...


def _config(*, auto_transition: bool) -> Config:
    return Config(
        minimum_number_of_calls=2,
        window_size=10,
        permitted_calls_in_half_open=2,
        max_concurrent_probes=2,
        wait_duration_in_open=_WAIT,
        auto_transition=auto_transition,
    )


def _trip_sync(breaker: CircuitBreaker) -> None:
    def boom() -> None:
        raise ValueError('boom')

    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            breaker.call(boom)


def test__auto_transition__sync__timer_moves_to_half_open() -> None:
    signal = _HalfOpenSignal()
    breaker = CircuitBreaker(name='auto', config=_config(auto_transition=True), listener=signal)
    _trip_sync(breaker)
    assert breaker.state is State.OPEN

    assert signal.reached_half_open.wait(2.0)
    assert breaker.state is State.HALF_OPEN
    assert (State.OPEN, State.HALF_OPEN) in signal.state_changes


@pytest.mark.asyncio
async def test__auto_transition__async__timer_moves_to_half_open() -> None:
    signal = _HalfOpenSignal()
    breaker = CircuitBreaker(
        name='auto-async', config=_config(auto_transition=True), listener=signal
    )

    async def boom() -> None:
        raise ValueError('boom')

    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            await breaker.call(boom)
    assert breaker.state is State.OPEN

    # The timer fires on its own thread; wait off the event loop.
    assert await asyncio.to_thread(signal.reached_half_open.wait, 2.0)
    assert breaker.state is State.HALF_OPEN


def test__auto_transition_disabled__stays_open_until_a_call() -> None:
    signal = _HalfOpenSignal()
    breaker = CircuitBreaker(name='lazy', config=_config(auto_transition=False), listener=signal)
    _trip_sync(breaker)

    assert not signal.reached_half_open.wait(_WAIT * 4)
    assert breaker.state is State.OPEN


def test__auto_transition__reset_before_timer__no_transition() -> None:
    signal = _HalfOpenSignal()
    breaker = CircuitBreaker(
        name='auto-reset', config=_config(auto_transition=True), listener=signal
    )
    _trip_sync(breaker)

    breaker.reset()

    assert not signal.reached_half_open.wait(_WAIT * 4)
    assert breaker.state is State.CLOSED


def test__auto_transition__force_open_before_timer__no_transition() -> None:
    signal = _HalfOpenSignal()
    breaker = CircuitBreaker(
        name='auto-force', config=_config(auto_transition=True), listener=signal
    )
    _trip_sync(breaker)

    breaker.force_open()

    assert not signal.reached_half_open.wait(_WAIT * 4)
    assert breaker.state is State.FORCED_OPEN


# The timer is armed exactly when the local machine enters OPEN and cancelled
# exactly when it leaves; nothing else may arm, cancel or re-arm it. A long real
# wait keeps it from firing, while the FakeClock drives the machine's own view of
# time, so every transition below happens on demand.

_LONG_WAIT = 3600.0


@pytest.fixture
def timed_engine(fake_clock: FakeClock) -> Iterator[Engine]:
    engine = Engine(
        name='timed',
        config=Config(
            minimum_number_of_calls=2,
            window_size=10,
            permitted_calls_in_half_open=2,
            max_concurrent_probes=2,
            wait_duration_in_open=_LONG_WAIT,
            auto_transition=True,
        ),
        clock=fake_clock,
    )
    yield engine
    engine.close()


def _trip_engine(engine: Engine) -> None:
    def boom() -> None:
        raise ValueError('boom')

    for _ in range(2):
        with pytest.raises(ValueError, match='boom'):
            engine.call_sync(boom)


def _armed(engine: Engine) -> threading.Timer:
    timer = engine._timer
    assert timer is not None
    return timer


def test__timer__armed_when_the_machine_opens(timed_engine: Engine) -> None:
    _trip_engine(timed_engine)

    assert _armed(timed_engine).daemon is True  # never blocks interpreter shutdown


def test__timer__cancelled_when_a_probe_moves_the_machine_out_of_open(
    timed_engine: Engine, fake_clock: FakeClock
) -> None:
    _trip_engine(timed_engine)
    fake_clock.advance(_LONG_WAIT)

    assert timed_engine.call_sync(lambda: 'probe') == 'probe'  # lazy OPEN -> HALF_OPEN

    assert timed_engine._timer is None


def test__timer__cancelled_by_reset(timed_engine: Engine) -> None:
    _trip_engine(timed_engine)

    timed_engine.reset()

    assert timed_engine._timer is None


def test__timer__cancelled_by_force_open(timed_engine: Engine) -> None:
    _trip_engine(timed_engine)

    timed_engine.force_open()

    assert timed_engine._timer is None


def test__timer__cleared_after_it_fires(timed_engine: Engine, fake_clock: FakeClock) -> None:
    _trip_engine(timed_engine)
    fake_clock.advance(_LONG_WAIT)

    timed_engine._fire_auto_transition()

    assert timed_engine.state is State.HALF_OPEN
    assert timed_engine._timer is None


def test__timer__survives_firing_before_the_wait_elapsed(timed_engine: Engine) -> None:
    _trip_engine(timed_engine)
    armed = _armed(timed_engine)

    timed_engine._fire_auto_transition()  # the clock has not advanced yet

    # The lazy path stays authoritative: an early wake-up transitions nothing,
    # so it must not throw away the timer that is still owed a transition.
    assert timed_engine.state is State.OPEN
    assert timed_engine._timer is armed


def test__timer__survives_a_rejected_call(timed_engine: Engine) -> None:
    _trip_engine(timed_engine)
    armed = _armed(timed_engine)

    with pytest.raises(CircuitOpenError):
        timed_engine.call_sync(lambda: 1)

    assert timed_engine._timer is armed


def test__timer__survives_a_stale_call_settling_after_the_trip(timed_engine: Engine) -> None:
    start, admission = timed_engine.enter_block()  # admitted while CLOSED
    _trip_engine(timed_engine)
    armed = _armed(timed_engine)

    timed_engine.exit_block(start=start, admission=admission, exception=ValueError('late'))

    assert timed_engine._timer is armed


def test__timer__survives_a_shared_view_update(timed_engine: Engine) -> None:
    _trip_engine(timed_engine)
    armed = _armed(timed_engine)

    timed_engine._on_shared_view(SharedState.closed())

    assert timed_engine._timer is armed


def test__timer__survives_storage_degradation_and_recovery(timed_engine: Engine) -> None:
    _trip_engine(timed_engine)
    armed = _armed(timed_engine)

    timed_engine._on_storage_degraded(ConnectionError('down'))
    assert timed_engine._timer is armed

    timed_engine._on_storage_recovered()
    assert timed_engine._timer is armed
