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
- **Distributed state (optional).** Coordinate tripping and recovery probing
  across instances through Redis/Valkey, with graceful degradation to local
  state — see the [Redis integration](integrations/redis.md).

## Installation

```bash
uv add interlock-cb
```

## Status

interlock shipped a polished core first (state machine, windows, sync/async,
slow-call detection), then grew deliberately: v1.1 added timeouts, proactive
`OPEN → HALF_OPEN` and FastAPI; v1.2 adds coordinated distributed state over
Redis. Retries and a full resilience pipeline are planned for v2.
