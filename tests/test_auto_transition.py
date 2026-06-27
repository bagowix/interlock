"""End-to-end tests for the auto-transition timer.

Unlike the deterministic FakeClock tests, these exercise the real
``threading.Timer``: they use a short real ``wait_duration_in_open`` and wait on
a thread-safe ``Event`` the listener sets when the breaker reaches ``HALF_OPEN``.
"""

import asyncio
import threading

import pytest

from interlock import CircuitBreaker, Config, State

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
