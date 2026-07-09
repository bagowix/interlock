"""Walk one breaker through its full lifecycle: CLOSED -> OPEN -> HALF_OPEN -> CLOSED.

Zero dependencies, no network — run it directly and watch every transition:

    python examples/lifecycle.py

The gateway is deterministic (healthy, then down, then recovered), so the
output is always the same. Tweak the ``Config`` values and re-run to see how
the thresholds change the story. Explained line by line in
https://bagowix.github.io/interlock/demo/.
"""

import time

from interlock import CircuitBreaker, CircuitOpenError, Config, Outcome, State


class GatewayError(Exception):
    """The fake dependency's failure mode."""


class FlakyGateway:
    """A payment gateway you can switch between healthy and down."""

    def __init__(self) -> None:
        self.healthy = True

    def charge(self, amount: int) -> str:
        if not self.healthy:
            raise GatewayError('503 from gateway')

        return f'charged ${amount}'


class PrintListener:
    """An EventListener that narrates everything the breaker does."""

    def on_state_change(self, *, name: str, old: State, new: State) -> None:
        print(f'  [listener] {name}: state {old.name} -> {new.name}')

    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None:
        print(f'  [listener] {name}: recorded {outcome.name} ({duration:.3f}s)')

    def on_rejected(self, *, name: str) -> None:
        print(f'  [listener] {name}: call rejected — circuit is open')

    def on_reset(self, *, name: str) -> None:
        print(f'  [listener] {name}: manual reset')


gateway = FlakyGateway()
breaker = CircuitBreaker(
    name='payments-api',
    config=Config(
        failure_rate_threshold=0.5,  # trip at >= 50% failures ...
        minimum_number_of_calls=4,  # ... once the window holds 4 calls
        window_size=4,
        wait_duration_in_open=1.0,  # stay OPEN for 1s, then allow probes
        permitted_calls_in_half_open=2,  # 2 good probes close the circuit
    ),
    listener=PrintListener(),
)


@breaker
def charge(amount: int) -> str:
    """The protected call: the decorator preserves this signature."""
    return gateway.charge(amount)


def attempt(number: int, amount: int) -> None:
    """Make one charge attempt and print what came back."""
    try:
        print(f'call {number}: {charge(amount)}')
    except GatewayError as exc:
        print(f'call {number}: dependency failed — {exc}')
    except CircuitOpenError as exc:
        print(
            f'call {number}: REJECTED in ~0ms, the gateway was never called '
            f'(retry_after={exc.retry_after:.1f}s)'
        )


def main() -> None:
    """Run the four phases of the lifecycle."""
    print('phase 1 — healthy dependency, breaker CLOSED')
    attempt(1, 10)
    attempt(2, 20)

    print()
    print('phase 2 — the gateway goes down; failures fill the window')
    gateway.healthy = False
    attempt(3, 30)
    attempt(4, 40)  # 2 failures / 4 calls = 50% -> the breaker trips

    print()
    print('phase 3 — circuit OPEN: calls fail fast, the gateway gets a break')
    attempt(5, 50)

    print()
    print('phase 4 — after wait_duration_in_open the breaker probes')
    gateway.healthy = True  # ops fixed the gateway meanwhile
    time.sleep(1.1)
    attempt(6, 60)  # probe 1 (OPEN -> HALF_OPEN on admission)
    attempt(7, 70)  # probe 2 -> both succeeded -> HALF_OPEN -> CLOSED

    print()
    print(f'final state: {breaker.state.name}, window reset: {breaker.snapshot()}')


if __name__ == '__main__':
    main()
