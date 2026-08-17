# interlock

[![PyPI](https://img.shields.io/pypi/v/interlock-cb.svg)](https://pypi.org/project/interlock-cb/)
[![Downloads](https://img.shields.io/pypi/dm/interlock-cb.svg)](https://pypi.org/project/interlock-cb/)
[![Python versions](https://img.shields.io/pypi/pyversions/interlock-cb.svg)](https://pypi.org/project/interlock-cb/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/bagowix/interlock/blob/main/LICENSE)
[![CI](https://github.com/bagowix/interlock/actions/workflows/ci.yml/badge.svg)](https://github.com/bagowix/interlock/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/bagowix/interlock/branch/main/graph/badge.svg)](https://codecov.io/gh/bagowix/interlock)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/bagowix/interlock/badge)](https://scorecard.dev/viewer/?uri=github.com/bagowix/interlock)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/13932/badge)](https://www.bestpractices.dev/projects/13932)
[![CodSpeed](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://app.codspeed.io/bagowix/interlock?utm_source=badge)
[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-2f6f55.svg)](https://bagowix.github.io/interlock/)
[![llms.txt](https://img.shields.io/badge/-llms.txt-brightgreen)](https://bagowix.github.io/interlock/llms.txt)
[![Context7](https://img.shields.io/badge/docs-Context7-1f6feb.svg)](https://context7.com/bagowix/interlock)

A modern circuit breaker for Python — sync and async in a single class,
sliding-window rate and slow-call detection, a type-safe API, and transparent
integrations at the transport level.

![CLOSED passes calls through and trips to OPEN once the failure or slow-call rate reaches its threshold; OPEN rejects calls without touching the dependency and moves to HALF_OPEN after the wait duration; HALF_OPEN admits probes only — a failing probe re-opens the circuit, successful probes close it](https://raw.githubusercontent.com/bagowix/interlock/main/docs/img/state-machine.svg)

## Installation

```bash
uv add interlock-cb          # or: pip install interlock-cb
```

interlock-cb supports Python 3.11 and newer. The core uses only the standard
library; external integrations are installed as [optional extras](#integrations).

## Quickstart

Create one named breaker per dependency and reuse it around every call to it:

```python
from interlock import CircuitBreaker, CircuitOpenError, Config

payments = CircuitBreaker(
    name='payments',
    config=Config(
        failure_rate_threshold=0.5,  # trip at 50% failures...
        minimum_number_of_calls=20,  # ...once the window holds 20 calls
        slow_call_duration_threshold=2.0,  # a call slower than 2s counts as slow
        slow_call_rate_threshold=0.3,  # 30% slow calls trip it just as well
    ),
)


@payments
def charge(amount: int) -> str:
    return gateway.charge(amount)


try:
    receipt = charge(100)
except CircuitOpenError as exc:
    print(exc)  # Circuit 'payments' is open; retry in ~60.000s
```

The slow-call thresholds matter as much as the failure ones: a dependency that
answers every call in 30 seconds raises nothing, so a consecutive-failure
counter keeps the circuit closed while your own request queue fills up.

The same instance protects async callables — there is no second class to
configure and no separate state to reason about:

```python
@payments
async def refund(charge_id: str) -> None:
    await gateway.refund(charge_id)
```

The decorator preserves the wrapped signature and whether it is sync or async.
`breaker.call(fn, ...)`, `with breaker` and `async with breaker` protect the
same call in other shapes — see
[Getting started](https://bagowix.github.io/interlock/getting-started/) for all
calling styles and
[Configuration](https://bagowix.github.io/interlock/guides/configuration/) for
every threshold.

## Why interlock

- **Sync and async, one class.** `CircuitBreaker` dispatches to separate sync
  and async paths without duplicating the public API.
- **Failure rates over sliding windows.** Choose count- or time-based windows
  instead of relying only on consecutive failures.
- **Slow calls and returned values count.** Detect latency degradation and
  classify unsuccessful results even when no exception is raised.
- **Type-safe decorators.** Wrapped signatures and their sync/async nature are
  preserved; the package ships `py.typed` and passes three strict type checkers.
- **Zero-dependency core.** Optional clients, frameworks, storage and
  observability integrations never leak into the core package.
- **Composable resilience.** Combine timeout, bulkhead, breaker, retry and
  fallback explicitly, or coordinate breaker state across instances with Redis.

## Safe production rollout

Start a new integration in `METRICS_ONLY` to observe real failure and slow-call
rates without rejecting traffic. The initial state is applied before a lazy
per-host breaker can admit its first request:

```python
import httpx2

from interlock import Config, LoggingEventListener, State
from interlock.integrations.httpx2 import AsyncCircuitBreakerTransport

transport = AsyncCircuitBreakerTransport(
    httpx2.AsyncHTTPTransport(),
    initial_state=State.METRICS_ONLY,
    config=Config(failure_rate_threshold=0.25, minimum_number_of_calls=50),
    listener=LoggingEventListener(),
)
```

`LoggingEventListener` writes every event through stdlib logging; swap it for
an `EventListener` that exports to your metrics backend. Hosts are only known at
runtime, so `transport.registry.items()` lists every breaker created so far and
`get_existing(host)` inspects one without creating it. After tuning thresholds,
deploy a new transport with the default `initial_state=State.CLOSED`; the
enforcing instance starts with a fresh window. See
[States and manual control](https://bagowix.github.io/interlock/guides/states/#safe-rollout).

## Shared state across instances

A local breaker only reacts to what its own process saw. Back it with Redis and
the whole fleet backs off together:

```python
import redis

from interlock import CircuitBreaker
from interlock.integrations.redis import RedisStorage

payments = CircuitBreaker(
    name='payments',
    storage=RedisStorage(redis.Redis(host='redis.internal')),
)
```

Tripping is atomic across racing instances, recovery probes are budgeted
globally rather than per process, and a Redis outage degrades to local state
instead of failing calls. Sharing state gates traffic everywhere at once — that
is the point, and the risk, so the
[Redis integration](https://bagowix.github.io/interlock/integrations/redis/)
page starts with when *not* to use it.

## Resilience pipeline

Compose strategies in an explicit order (first is outermost) while keeping the
breaker useful as a standalone primitive:

```python
from interlock import CircuitBreaker, CircuitOpenError, Pipeline

breaker = CircuitBreaker(name='recommendations')

pipeline = (
    Pipeline.builder()
    .fallback(lambda exc: [], on=(CircuitOpenError,))
    .retry(attempts=4)  # requires interlock-cb[tenacity]
    .circuit_breaker(breaker)
    .bulkhead(8)
    .timeout(2.0)
    .build()
)


@pipeline
async def fetch_picks(user: str) -> list[str]:
    return await client.get_picks(user)
```

Retries never hammer an open circuit, one hung attempt cannot eat the retry
budget, and every decision is observable — see the
[pipeline guide](https://bagowix.github.io/interlock/guides/pipeline/).

## Integrations

The `httpx2` transport applies one breaker per host with no decorators at call
sites:

```python
import httpx2
from interlock.integrations.httpx2 import CircuitBreakerTransport

transport = CircuitBreakerTransport(httpx2.HTTPTransport())
client = httpx2.Client(transport=transport)
```

By default, transport exceptions and the canonical retryable statuses
(`429, 500, 502, 503, 504`) count as failures; `4xx` client errors do not.

| Integration | Install | Documentation |
|---|---|---|
| httpx2 | `interlock-cb[httpx2]` | [Per-host transport](https://bagowix.github.io/interlock/integrations/httpx2/) |
| httpx | `interlock-cb[httpx]` | [Per-host transport](https://bagowix.github.io/interlock/integrations/httpx/) |
| aiohttp | `interlock-cb[aiohttp]` | [Client middleware](https://bagowix.github.io/interlock/integrations/aiohttp/) |
| requests | `interlock-cb[requests]` | [Session adapter](https://bagowix.github.io/interlock/integrations/requests/) |
| FastAPI | `interlock-cb[fastapi]` | [`503 + Retry-After` handler](https://bagowix.github.io/interlock/integrations/fastapi/) |
| Litestar | `interlock-cb[litestar]` | [`503 + Retry-After` handler](https://bagowix.github.io/interlock/integrations/litestar/) |
| tenacity | `interlock-cb[tenacity]` | [Retry composition](https://bagowix.github.io/interlock/integrations/tenacity/) |
| Redis | `interlock-cb[redis]` | [Shared state](https://bagowix.github.io/interlock/integrations/redis/) |
| OpenTelemetry | `interlock-cb[otel]` | [Metrics listener](https://bagowix.github.io/interlock/guides/observability/) |

The [integrations overview](https://bagowix.github.io/interlock/integrations/)
also includes recipes for LLM SDKs and Flask/Django.

## How it compares

interlock-cb is young: its first release was in 2026. Established libraries
such as [pybreaker](https://github.com/danielfm/pybreaker) and
[circuitbreaker](https://github.com/fabfuel/circuitbreaker) have carried
production traffic for years and remain a better fit when maturity matters
more than the feature differences.

| Feature | interlock-cb | pybreaker | circuitbreaker |
|---|:---:|:---:|:---:|
| Core states (closed / open / half-open) | ✅ | ✅ | ✅ |
| Native asyncio | ✅ | Tornado | ✅ |
| Trip condition | failure rate | consecutive failures | consecutive failures |
| Time-based sliding window | ✅ | — | — |
| Slow-call detection | ✅ | — | — |
| Shared state across processes | ✅ | ✅ | — |
| Composable resilience pipeline | ✅ | — | — |
| Fully typed API (`py.typed`) | ✅ | — | — |

The [full comparison](https://bagowix.github.io/interlock/comparison/) covers
more features as well as aiobreaker and purgatory. Something out of date or
unfair? Please open a PR.

The reliability work compensating for the project's shorter production history
includes 100% branch coverage, three strict type checkers, mutation testing of
the state machine and engine, property- and model-based tests, and CI on
free-threaded CPython. The
[correctness and testing](https://bagowix.github.io/interlock/correctness/) page
documents what is verified and where the limits are.

## Documentation

The full documentation is hosted at **<https://bagowix.github.io/interlock/>**.
Start with:

- [Getting started](https://bagowix.github.io/interlock/getting-started/)
- [Configuration](https://bagowix.github.io/interlock/guides/configuration/) and
  [states](https://bagowix.github.io/interlock/guides/states/)
- [Timeouts](https://bagowix.github.io/interlock/guides/timeout/) and
  [retries](https://bagowix.github.io/interlock/guides/retries/)
- [Resilience pipeline](https://bagowix.github.io/interlock/guides/pipeline/)
- [Integrations](https://bagowix.github.io/interlock/integrations/)
- [Correctness and testing](https://bagowix.github.io/interlock/correctness/)
- [API reference](https://bagowix.github.io/interlock/reference/)

For a deterministic, network-free demonstration of every state transition, run
the [`examples/`](https://github.com/bagowix/interlock/tree/main/examples)
scripts or follow the
[walkthrough](https://bagowix.github.io/interlock/demo/).

## Contributing

Bug reports and pull requests are welcome. See
[`CONTRIBUTING.md`](https://github.com/bagowix/interlock/blob/main/CONTRIBUTING.md)
for the local setup and the checks a change must pass, and
[`CODE_OF_CONDUCT.md`](https://github.com/bagowix/interlock/blob/main/CODE_OF_CONDUCT.md)
for community expectations. Security issues: please follow
[`SECURITY.md`](https://github.com/bagowix/interlock/blob/main/SECURITY.md).

## License

interlock is released under the
[MIT License](https://github.com/bagowix/interlock/blob/main/LICENSE).
