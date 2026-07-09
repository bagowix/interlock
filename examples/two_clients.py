"""Two guarded clients in one event loop: one dependency dies, the other keeps serving.

Zero dependencies, no network — run it directly:

    python examples/two_clients.py

A ``Registry`` hands out one independent breaker per dependency. The
``recommendations`` service goes down during rounds 3-6: its breaker opens
and the app falls back to a cached list, while ``payments`` — its own
breaker untouched — keeps charging without a hiccup. Explained line by line
in https://bagowix.github.io/interlock/demo/.
"""

import asyncio

from interlock import CircuitOpenError, Config, Outcome, Registry, State


class RecsDownError(Exception):
    """The recommendations service's failure mode."""


class PrintListener:
    """An EventListener that narrates state changes and rejections."""

    def on_state_change(self, *, name: str, old: State, new: State) -> None:
        print(f'  [listener] {name}: state {old.name} -> {new.name}')

    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None:
        pass  # per-call noise is off; see examples/lifecycle.py for it

    def on_rejected(self, *, name: str) -> None:
        print(f'  [listener] {name}: call rejected — circuit is open')

    def on_reset(self, *, name: str) -> None:
        print(f'  [listener] {name}: manual reset')


registry = Registry(
    config=Config(
        failure_rate_threshold=0.5,
        minimum_number_of_calls=4,
        window_size=4,
        wait_duration_in_open=1.0,
        permitted_calls_in_half_open=2,
    ),
    listener=PrintListener(),
)

outage = False


@registry.get('recommendations')
async def fetch_recommendations(user: str) -> list[str]:
    """Call the flaky recommendations service."""
    if outage:
        raise RecsDownError('recommendations service timed out')
    return [f'{user}-pick-1', f'{user}-pick-2']


@registry.get('payments')
async def charge(user: str, amount: int) -> str:
    """Call the payments service, which stays healthy throughout."""
    return f'charged {user} ${amount}'


async def round_trip(number: int, user: str) -> None:
    """One request round: hit both dependencies concurrently."""
    print(f'round {number}:')
    payment, recs = await asyncio.gather(
        charge(user, 25),
        fetch_recommendations(user),
        return_exceptions=True,
    )
    print(f'  payments        -> {payment}')
    if isinstance(recs, CircuitOpenError):
        print('  recommendations -> rejected instantly -> fallback: cached picks')
    elif isinstance(recs, RecsDownError):
        print(f'  recommendations -> failed ({recs}) -> fallback: cached picks')
    else:
        print(f'  recommendations -> {recs}')


async def main() -> None:
    """Run eight rounds across the outage and the recovery."""
    global outage  # noqa: PLW0603 - a module-level switch keeps the demo flat

    print('rounds 1-2 — both dependencies healthy')
    await round_trip(1, 'alice')
    await round_trip(2, 'bob')

    print()
    print('rounds 3-6 — recommendations goes down; payments must not care')
    outage = True
    await round_trip(3, 'carol')
    await round_trip(4, 'dave')  # 50% failures -> recommendations breaker opens
    await round_trip(5, 'erin')  # rejected in ~0ms: no timeout is burned
    await round_trip(6, 'frank')

    print()
    print('rounds 7-8 — the outage is over; the breaker probes and closes')
    outage = False
    await asyncio.sleep(1.1)  # let wait_duration_in_open elapse
    await round_trip(7, 'grace')
    await round_trip(8, 'heidi')

    print()
    for name in ('payments', 'recommendations'):
        print(f'final state of {name}: {registry.get(name).state.name}')


if __name__ == '__main__':
    asyncio.run(main())
