"""Benchmarks for state transitions, sliding windows and object construction.

The call paths are covered elsewhere; what matters here is the cost of the
decisions around them — tripping open, probing in half-open, closing again — plus
the two sliding-window implementations that feed those decisions and the
construction of the objects themselves.
"""

from pytest_codspeed import BenchmarkFixture

from interlock import CircuitBreaker, CircuitOpenError, Config, Registry, State, WindowType
from interlock._windows import build_window
from interlock.outcome import Outcome

_LIFECYCLE_CONFIG = Config(
    failure_rate_threshold=0.5,
    minimum_number_of_calls=10,
    window_size=10,
    permitted_calls_in_half_open=5,
    max_concurrent_probes=1,
    wait_duration_in_open=60.0,
)
_WINDOW_RECORDS = 1_000
_OUTCOMES = tuple(
    Outcome.FAILURE if index % 4 == 0 else Outcome.SUCCESS for index in range(_WINDOW_RECORDS)
)


class BenchClock:
    """A clock advanced by hand, so window ageing is deterministic."""

    def __init__(self, *, step: float = 0.0) -> None:
        self._now = 0.0
        self._step = step

    def monotonic(self) -> float:
        """Return the current time, then advance it by the configured step."""
        now = self._now
        self._now += self._step
        return now

    def advance(self, seconds: float) -> None:
        """Move the clock forward by ``seconds``."""
        self._now += seconds


def _work() -> int:
    """The protected payload."""
    return 42


def _boom() -> None:
    """Fail the way a broken dependency would."""
    raise RuntimeError('dependency down')


def test_trip_and_recover(benchmark: BenchmarkFixture) -> None:
    """A full lifecycle: closed -> open -> half-open probes -> closed again."""
    clock = BenchClock()
    breaker = CircuitBreaker(name='bench-lifecycle', config=_LIFECYCLE_CONFIG, clock=clock)

    def lifecycle() -> State:
        breaker.reset()
        for _ in range(10):
            try:
                breaker.call(_boom)
            except (RuntimeError, CircuitOpenError):
                pass

        clock.advance(_LIFECYCLE_CONFIG.wait_duration_in_open + 1.0)
        for _ in range(_LIFECYCLE_CONFIG.permitted_calls_in_half_open):
            breaker.call(_work)

        return breaker.state

    assert lifecycle() is State.CLOSED
    benchmark(lifecycle)


def test_window_record_count_based(benchmark: BenchmarkFixture) -> None:
    """Ring-buffer bookkeeping: 1000 outcomes through a 100-call window."""
    window = build_window(config=Config(window_size=100), clock=BenchClock())

    def record_all() -> None:
        for outcome in _OUTCOMES:
            window.record(outcome)

    benchmark(record_all)


def test_window_record_time_based(benchmark: BenchmarkFixture) -> None:
    """Per-second bucketing: 1000 outcomes spread over a 60s window."""
    config = Config(window_type=WindowType.TIME_BASED, window_size=60)
    window = build_window(config=config, clock=BenchClock(step=0.1))

    def record_all() -> None:
        for outcome in _OUTCOMES:
            window.record(outcome)

    benchmark(record_all)


def test_window_snapshot_time_based(benchmark: BenchmarkFixture) -> None:
    """Aggregating a full time-based window, which sums every live bucket."""
    clock = BenchClock()
    window = build_window(
        config=Config(window_type=WindowType.TIME_BASED, window_size=60), clock=clock
    )
    for outcome in _OUTCOMES:
        window.record(outcome)
        clock.advance(0.1)

    benchmark(window.snapshot)


def test_config_validation(benchmark: BenchmarkFixture) -> None:
    """Building a ``Config``: eager validation of every threshold."""
    benchmark(
        lambda: Config(
            failure_rate_threshold=0.25,
            minimum_number_of_calls=20,
            slow_call_duration_threshold=0.5,
            permitted_calls_in_half_open=4,
            max_concurrent_probes=2,
            wait_duration_in_open=30.0,
            window_type=WindowType.TIME_BASED,
            window_size=120,
        )
    )


def test_breaker_construction(benchmark: BenchmarkFixture) -> None:
    """Constructing a breaker: engine, state machine, window and ContextVar."""
    benchmark(lambda: CircuitBreaker(name='bench-construct', config=_LIFECYCLE_CONFIG))


def test_registry_get_cached(benchmark: BenchmarkFixture) -> None:
    """Resolving an existing breaker by name — the per-request lookup."""
    registry = Registry(config=_LIFECYCLE_CONFIG)
    registry.get('payments')

    benchmark(lambda: registry.get('payments'))
