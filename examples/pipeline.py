"""Compose timeout + breaker + fallback around one dependency that dies quietly.

Zero dependencies, no network — run it directly:

    python examples/pipeline.py

The inventory service never raises: it *hangs*. Alone, that starves callers
and trips nothing. The pipeline turns the hang into a failure chain: the
timeout cancels the attempt, the breaker counts it and opens, the fallback
serves a cached snapshot — and while the circuit is open, requests cost
~0.0s instead of a timeout each. Add `.retry(...)` between the fallback and
the breaker for the full stack (needs `interlock-cb[tenacity]`). Explained
line by line in https://bagowix.github.io/interlock/demo/.
"""

import asyncio
import time

from interlock import (
    CallTimeoutError,
    CircuitBreaker,
    CircuitOpenError,
    Config,
    Outcome,
    Pipeline,
    State,
)

CACHED_SNAPSHOT = ['widget (cached)', 'gadget (cached)']


class PrintListener:
    """Narrates the breaker's transitions and the pipeline's decisions."""

    def on_state_change(self, *, name: str, old: State, new: State) -> None:
        print(f'  [listener] {name}: state {old.name} -> {new.name}')

    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None:
        pass  # per-call noise is off; see examples/lifecycle.py for it

    def on_rejected(self, *, name: str) -> None:
        print(f'  [listener] {name}: call rejected — circuit is open')

    def on_reset(self, *, name: str) -> None:
        print(f'  [listener] {name}: manual reset')

    def on_fallback(self, *, name: str, error: BaseException) -> None:
        print(f'  [listener] {name}: fallback served instead of {type(error).__name__}')


events = PrintListener()
hanging = False
breaker = CircuitBreaker(
    name='inventory',
    config=Config(
        failure_rate_threshold=0.5,  # trip at >= 50% failures ...
        minimum_number_of_calls=4,  # ... once the window holds 4 calls
        window_size=4,
        wait_duration_in_open=1.0,  # stay OPEN for 1s, then allow probes
        permitted_calls_in_half_open=2,  # 2 good probes close the circuit
    ),
    listener=events,
)

pipeline = (
    Pipeline.builder()
    .fallback(
        lambda _exc: CACHED_SNAPSHOT,
        on=(CircuitOpenError, CallTimeoutError),
        name='inventory',
        listener=events,
    )
    .circuit_breaker(breaker)
    .timeout(0.2)  # a hanging attempt becomes CallTimeoutError after 0.2s
    .build()
)


@pipeline
async def fetch_inventory() -> list[str]:
    """The protected call: the dependency hangs instead of erroring."""
    if hanging:
        await asyncio.sleep(5)  # never finishes within the timeout
    return ['widget', 'gadget']


async def request(number: int) -> None:
    """Serve one request and print what it cost."""
    start = time.perf_counter()
    items = await fetch_inventory()
    elapsed = time.perf_counter() - start
    print(f'request {number}: {items} in ~{elapsed:.1f}s')


async def main() -> None:
    """Run the outage story end to end."""
    global hanging  # noqa: PLW0603 - a module-level switch keeps the demo flat

    print('phase 1 — healthy and fast, breaker CLOSED')
    await request(1)
    await request(2)

    print()
    print('phase 2 — the service starts hanging; timeouts become failures')
    hanging = True
    await request(3)  # waits the full 0.2s timeout, then serves the cache
    await request(4)  # 2 timeouts / 4 calls = 50% -> the breaker trips

    print()
    print('phase 3 — circuit OPEN: no timeout is burned, the cache is instant')
    await request(5)

    print()
    print('phase 4 — the service recovers; probes close the circuit')
    hanging = False
    await asyncio.sleep(1.1)  # let wait_duration_in_open elapse
    await request(6)  # probe 1 (OPEN -> HALF_OPEN on admission)
    await request(7)  # probe 2 -> both succeeded -> HALF_OPEN -> CLOSED

    print()
    print(f'final state: {breaker.state.name}')


if __name__ == '__main__':
    asyncio.run(main())
