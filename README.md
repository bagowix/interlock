# interlock

[![CI](https://github.com/bagowix/interlock/actions/workflows/ci.yml/badge.svg)](https://github.com/bagowix/interlock/actions/workflows/ci.yml)
[![Coverage](https://raw.githubusercontent.com/bagowix/interlock/python-coverage-comment-action-data/badge.svg)](https://github.com/bagowix/interlock/tree/python-coverage-comment-action-data)
[![PyPI](https://img.shields.io/pypi/v/interlock-cb.svg)](https://pypi.org/project/interlock-cb/)
[![Downloads](https://img.shields.io/pypi/dm/interlock-cb.svg)](https://pypi.org/project/interlock-cb/)
[![Python versions](https://img.shields.io/pypi/pyversions/interlock-cb.svg)](https://pypi.org/project/interlock-cb/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![llms.txt](https://img.shields.io/badge/-llms.txt-brightgreen)](docs/llms.txt)

A modern circuit breaker for Python — sync and async in a single class,
sliding-window rate and slow-call detection, a type-safe API, and transparent
integrations at the transport level.

## Why interlock

- **Sync and async, one class.** A single `CircuitBreaker` detects coroutine
  callables and dispatches to the right path — no `Sync*`/`Async*` twins.
- **Sliding windows by rate.** Both count-based and time-based windows, not the
  naive consecutive-failure counter found elsewhere in the ecosystem.
- **Slow-call detection.** Treat calls slower than a threshold as failures —
  not available in any other Python circuit breaker.
- **Type-safe.** `ParamSpec` + `TypeVar` decorators preserve the wrapped
  signature *and* its sync/async nature; ships `py.typed`, passes mypy and
  pyright in strict mode.
- **Zero-dependency core.** Standard library only; everything external lives in
  optional extras (`httpx2`, `aiohttp`, `requests`, `tenacity`, `fastapi`,
  `redis`, `otel`).

## How it compares

interlock-cb is young (first released in 2026). [pybreaker][pybreaker] and
[circuitbreaker][circuitbreaker] are mature, well-documented and proven in
production for years — for many projects they are exactly the right choice. Each
library is strong in different places:

| Feature | interlock-cb | pybreaker | circuitbreaker |
|---|:---:|:---:|:---:|
| Core states (closed / open / half-open) | ✅ | ✅ | ✅ |
| Choose which exceptions count as failures | ✅ | ✅ | ✅ |
| Zero-dependency core | ✅ | ✅ | ✅ |
| `async` / `await` (asyncio) | ✅ | Tornado | ✅ |
| Event / state-change listeners | ✅ | ✅ | — |
| Shared state across processes (Redis) | ✅ | ✅ | — |
| Fallback function | planned | — | ✅ |
| Years of production use | new | ✅ | ✅ |
| Failure-**rate** sliding window | ✅ | — | — |
| Time-based window | ✅ | — | — |
| Slow-call detection | ✅ | — | — |
| Result-based failure classification | ✅ | — | — |
| Type-safe decorator (preserves signature) | ✅ | — | — |
| Built-in httpx transport | ✅ | — | — |
| OpenTelemetry metrics | ✅ | — | — |

<sub>Compared against pybreaker 1.x and circuitbreaker 2.1 as documented in mid-2026.
pybreaker's async support is Tornado-based, not asyncio. "planned" items are on
the interlock-cb roadmap (fallback). Both established
libraries trip on a consecutive-failure count rather than a rate window.
Something out of date? Please open a PR.</sub>

Reach for an established library if you want a small, proven breaker today or a
built-in fallback. Choose interlock-cb when you want rate-based windows,
slow-call detection, coordinated state with graceful degradation and a fully
typed API.

[pybreaker]: https://github.com/danielfm/pybreaker
[circuitbreaker]: https://github.com/fabfuel/circuitbreaker

## Installation

```bash
uv add interlock-cb          # or: pip install interlock-cb
```

Optional extras:

```bash
uv add 'interlock-cb[otel]'     # OpenTelemetry metrics listener
uv add 'interlock-cb[httpx2]'   # per-host httpx2 transport
uv add 'interlock-cb[aiohttp]'  # per-host aiohttp client middleware
uv add 'interlock-cb[requests]' # per-host requests session adapter
uv add 'interlock-cb[tenacity]' # retry × breaker composition helpers
uv add 'interlock-cb[fastapi]'  # FastAPI dependency + 503 Retry-After handler
uv add 'interlock-cb[redis]'    # shared breaker state across processes
```

## Quickstart

Protect a callable three ways over the one `call()` primitive.

```python
from interlock import CircuitBreaker, Config

breaker = CircuitBreaker(
    name='payments',
    config=Config(failure_rate_threshold=0.5, minimum_number_of_calls=20),
)

# 1. Decorator — preserves the signature and sync/async nature.
@breaker
def charge(amount: int) -> str:
    return gateway.charge(amount)

# 2. breaker.call — the breaker runs the callable.
result = breaker.call(gateway.charge, 100)

# 3. Context manager — guards a block (exceptions + duration only).
with breaker:
    gateway.charge(100)
```

The same instance works for async — the decorator and `call` detect a coroutine
function, and the instance is also an async context manager:

```python
@breaker
async def fetch(url: str) -> bytes:
    return await client.get(url)

async with breaker:
    await client.get(url)
```

When the circuit is open, the call is rejected with `CircuitOpenError`, which
carries the breaker name, an estimate of when the next probe is allowed, and the
last recorded failure:

```python
from interlock import CircuitOpenError

try:
    breaker.call(gateway.charge, 100)
except CircuitOpenError as exc:
    print(exc.breaker_name, exc.retry_after, exc.last_failure)
```

## httpx2 integration

Apply a breaker **per host** transparently, with no decorators in call sites:

```python
import httpx2
from interlock.integrations.httpx2 import CircuitBreakerTransport

transport = CircuitBreakerTransport(httpx2.HTTPTransport())
client = httpx2.Client(transport=transport)
```

By default, transport exceptions and the canonical retryable statuses
(`429, 500, 502, 503, 504`) count as failures; `4xx` client errors do not.

## FastAPI integration

Inject a per-name breaker with `Depends` and map an open circuit to a clean
`503` with `Retry-After`:

```python
from typing import Annotated

from fastapi import Depends, FastAPI
from interlock import CircuitBreaker, Registry
from interlock.integrations.fastapi import breaker_dependency, install_exception_handler

app = FastAPI()
registry = Registry()
install_exception_handler(app)

orders_db = breaker_dependency('orders-db', registry=registry)


@app.get('/orders')
async def orders(breaker: Annotated[CircuitBreaker, Depends(orders_db)]) -> list[dict]:
    return await breaker.call(fetch_orders)
```

## Redis integration (shared state)

Coordinate breaker state across processes and machines: when one instance
trips, every instance backs off, and recovery probes are budgeted globally.
Redis failures never reach the protected call — the breaker degrades to local
state and re-syncs when Redis recovers:

```python
import redis
from interlock import CircuitBreaker, Registry
from interlock.integrations.redis import RedisStorage

storage = RedisStorage(redis.Redis(host='redis.internal'))
breaker = CircuitBreaker(name='payments', storage=storage)

registry = Registry(storage=storage)  # or share one storage across many breakers
```

Async services use `AsyncRedisStorage` with `redis.asyncio.Redis` the same way.

## More integrations

The same per-host pattern ships for **aiohttp** (client middleware) and
**requests** (session adapter), and the **tenacity** extra composes retries
with the breaker correctly (stop retrying once the circuit opens, or wait
exactly until the next probe). Recipes cover **OpenAI / Anthropic SDK** calls
and **Flask / Django** handlers — see the
[integrations overview](docs/integrations/index.md) and the
[retries guide](docs/guides/retries.md).

## Documentation

The full documentation is hosted at **<https://bagowix.github.io/interlock/>**.
The sources live in [`docs/`](docs/):

- [Getting started](docs/getting-started.md)
- [Configuration](docs/guides/configuration.md)
- [States & manual control](docs/guides/states.md)
- [Failure classification](docs/guides/failure-classification.md)
- [Observability](docs/guides/observability.md)
- [Timeout](docs/guides/timeout.md)
- [Retries and circuit breakers](docs/guides/retries.md)
- [Integrations overview](docs/integrations/index.md) — httpx2, aiohttp,
  requests, tenacity, FastAPI, Redis, LLM SDKs, Flask/Django
- [API reference](docs/reference.md)

## Contributing

Bug reports and pull requests are welcome. See
[`CONTRIBUTING.md`](CONTRIBUTING.md) for the local setup and the checks a change
must pass, and [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md) for community
expectations. Security issues: please follow [`SECURITY.md`](SECURITY.md).

## License

interlock is released under the [MIT License](LICENSE).
