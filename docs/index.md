# interlock

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
- **Type-safe.** `ParamSpec` + `TypeVar` decorators that preserve signatures;
  ships `py.typed` and passes mypy in strict mode.
- **Zero-dependency core.** Standard library only; everything external lives in
  optional extras.

## Installation

```bash
uv add interlock
```

## Status

interlock ships v1.0-first: a polished core (state machine, windows,
sync/async, slow-call detection) before breadth. Distributed state, retries,
and a full resilience pipeline are planned for later releases.
