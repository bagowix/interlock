# interlock

[![CI](https://github.com/bagowix/interlock/actions/workflows/ci.yml/badge.svg)](https://github.com/bagowix/interlock/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/bagowix/interlock/branch/main/graph/badge.svg)](https://codecov.io/gh/bagowix/interlock)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/bagowix/interlock/badge)](https://scorecard.dev/viewer/?uri=github.com/bagowix/interlock)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/13932/badge)](https://www.bestpractices.dev/projects/13932)
[![PyPI](https://img.shields.io/pypi/v/interlock-cb.svg)](https://pypi.org/project/interlock-cb/)
[![Downloads](https://img.shields.io/pypi/dm/interlock-cb.svg)](https://pypi.org/project/interlock-cb/)
[![Python versions](https://img.shields.io/pypi/pyversions/interlock-cb.svg)](https://pypi.org/project/interlock-cb/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![llms.txt](https://img.shields.io/badge/-llms.txt-brightgreen)](docs/llms.txt)
[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-2f6f55.svg)](https://bagowix.github.io/interlock/)
[![Context7](https://img.shields.io/badge/docs-Context7-1f6feb.svg)](https://context7.com/bagowix/interlock)
[![CodSpeed](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://app.codspeed.io/bagowix/interlock?utm_source=badge)

A modern circuit breaker for Python — sync and async in a single class,
sliding-window rate and slow-call detection, a type-safe API, and transparent
integrations at the transport level.

## Installation

```bash
uv add interlock-cb          # or: pip install interlock-cb
```

interlock-cb supports Python 3.11 and newer. The core uses only the standard
library; external integrations are installed as [optional extras](#integrations).

## Quickstart

Create one named breaker and reuse it around calls to the same dependency:

```python
from interlock import CircuitBreaker, CircuitOpenError, Config

breaker = CircuitBreaker(
    name='payments',
    config=Config(failure_rate_threshold=0.5, minimum_number_of_calls=20),
)


@breaker
def charge(amount: int) -> str:
    return gateway.charge(amount)


try:
    receipt = charge(100)
except CircuitOpenError as exc:
    print(exc)
```

The decorator preserves the function's signature and whether it is sync or
async. The same breaker also supports `breaker.call(fn, ...)`, `with breaker`,
and `async with breaker`. See [Getting started](docs/getting-started.md) for all
calling styles and [Configuration](docs/guides/configuration.md) for every
threshold.

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
[pipeline guide](docs/guides/pipeline.md).

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
| httpx2 | `interlock-cb[httpx2]` | [Per-host transport](docs/integrations/httpx2.md) |
| httpx | `interlock-cb[httpx]` | [Per-host transport](docs/integrations/httpx.md) |
| aiohttp | `interlock-cb[aiohttp]` | [Client middleware](docs/integrations/aiohttp.md) |
| requests | `interlock-cb[requests]` | [Session adapter](docs/integrations/requests.md) |
| FastAPI | `interlock-cb[fastapi]` | [`503 + Retry-After` handler](docs/integrations/fastapi.md) |
| Litestar | `interlock-cb[litestar]` | [`503 + Retry-After` handler](docs/integrations/litestar.md) |
| tenacity | `interlock-cb[tenacity]` | [Retry composition](docs/integrations/tenacity.md) |
| Redis | `interlock-cb[redis]` | [Shared state](docs/integrations/redis.md) |
| OpenTelemetry | `interlock-cb[otel]` | [Metrics listener](docs/guides/observability.md) |

The [integrations overview](docs/integrations/index.md) also includes recipes
for LLM SDKs and Flask/Django.

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

The [full comparison](docs/comparison.md) covers more features as well as
aiobreaker and purgatory. Something out of date or unfair? Please open a PR.

The reliability work compensating for the project's shorter production history
includes 100% branch coverage, three strict type checkers, mutation testing of
the state machine and engine, property- and model-based tests, and CI on
free-threaded CPython. The [correctness and testing](docs/correctness.md) page
documents what is verified and where the limits are.

## Documentation

The full documentation is hosted at **<https://bagowix.github.io/interlock/>**.
Start with:

- [Getting started](docs/getting-started.md)
- [Configuration](docs/guides/configuration.md) and [states](docs/guides/states.md)
- [Resilience pipeline](docs/guides/pipeline.md)
- [Integrations](docs/integrations/index.md)
- [Correctness and testing](docs/correctness.md)
- [API reference](docs/reference.md)

For a deterministic, network-free demonstration of every state transition, run
the [`examples/`](examples/) scripts or follow the [walkthrough](docs/demo.md).

## Contributing

Bug reports and pull requests are welcome. See
[`CONTRIBUTING.md`](CONTRIBUTING.md) for the local setup and the checks a change
must pass, and [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md) for community
expectations. Security issues: please follow [`SECURITY.md`](SECURITY.md).

## License

interlock is released under the [MIT License](LICENSE).
