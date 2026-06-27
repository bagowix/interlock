# interlock

[![CI](https://github.com/bagowix/interlock/actions/workflows/ci.yml/badge.svg)](https://github.com/bagowix/interlock/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/interlock-cb.svg)](https://pypi.org/project/interlock-cb/)
[![Python versions](https://img.shields.io/pypi/pyversions/interlock-cb.svg)](https://pypi.org/project/interlock-cb/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

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
  optional extras (`interlock-cb[otel]`, `interlock-cb[httpx2]`).

## Installation

```bash
uv add interlock-cb          # or: pip install interlock-cb
```

Optional extras:

```bash
uv add 'interlock-cb[otel]'    # OpenTelemetry metrics listener
uv add 'interlock-cb[httpx2]'  # per-host httpx2 transport
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
from interlock.httpx2 import CircuitBreakerTransport

transport = CircuitBreakerTransport(httpx2.HTTPTransport())
client = httpx2.Client(transport=transport)
```

By default, transport exceptions and the canonical retryable statuses
(`429, 500, 502, 503, 504`) count as failures; `4xx` client errors do not.

## Documentation

Full guides, integration recipes and the API reference live in [`docs/`](docs/):

- [Getting started](docs/getting-started.md)
- [Configuration](docs/guides/configuration.md)
- [States & manual control](docs/guides/states.md)
- [Failure classification](docs/guides/failure-classification.md)
- [Observability](docs/guides/observability.md)
- [Timeout](docs/guides/timeout.md)
- [httpx2 integration](docs/integrations/httpx2.md)
- [API reference](docs/reference.md)

## Contributing

Bug reports and pull requests are welcome. See
[`CONTRIBUTING.md`](CONTRIBUTING.md) for the local setup and the checks a change
must pass, and [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md) for community
expectations. Security issues: please follow [`SECURITY.md`](SECURITY.md).

## License

interlock is released under the [MIT License](LICENSE).
