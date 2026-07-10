# Observability

A breaker reports everything it does through an `EventListener`. The same
hooks back logging, metrics, and any custom sink.

## The hooks

```python
class EventListener(Protocol):
    def on_state_change(self, *, name: str, old: State, new: State) -> None: ...
    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None: ...
    def on_rejected(self, *, name: str) -> None: ...
    def on_reset(self, *, name: str) -> None: ...
    def on_storage_degraded(self, *, name: str, error: BaseException) -> None: ...
    def on_storage_recovered(self, *, name: str) -> None: ...
    def on_retry(self, *, name: str, attempt: int, delay: float) -> None: ...
    def on_bulkhead_rejected(self, *, name: str) -> None: ...
    def on_fallback(self, *, name: str, error: BaseException) -> None: ...
```

Listeners are called **outside** the breaker's lock, after the protected call
returns, so a slow listener never serialises throughput. Implementations must
not raise back into the core.

The two storage hooks fire only for breakers coordinated through a shared
[storage](../integrations/redis.md); the three pipeline hooks fire from
[pipeline strategies](pipeline.md) given a `listener=`. All optional hooks
are dispatched only if present — a listener without them keeps working.

Attach one per breaker, or share one across a `Registry`:

```python
breaker = CircuitBreaker(name='payments', listener=my_listener)
registry = Registry(listener=my_listener)   # every breaker reports here
```

## Logging (zero dependencies)

`LoggingEventListener` is built in. State changes and rejections log at
`WARNING`, resets at `INFO`, and individual calls at `DEBUG`:

```python
from interlock import CircuitBreaker, LoggingEventListener

breaker = CircuitBreaker(name='payments', listener=LoggingEventListener())
```

Pass your own logger to control routing:

```python
import logging

LoggingEventListener(logging.getLogger('myapp.breakers'))
```

## OpenTelemetry metrics

The OTel listener lives in the `interlock-cb[otel]` extra and is imported
explicitly, so the core stays dependency-free:

```bash
uv add 'interlock-cb[otel]'
```

```python
from interlock import CircuitBreaker
from interlock.integrations.otel import OTelEventListener

breaker = CircuitBreaker(name='payments', listener=OTelEventListener())
```

It records five instruments on the `interlock` meter (or a meter you pass in):

| Instrument | Type | Labels |
|------------|------|--------|
| `interlock.call.duration` | histogram (s) | `breaker`, `outcome` |
| `interlock.call.rejected` | counter | `breaker` |
| `interlock.state.changes` | counter | `breaker`, `from`, `to` |
| `interlock.reset` | counter | `breaker` |
| `interlock.storage.events` | counter | `breaker`, `event` (`degraded`/`recovered`), `error` |

## Custom listeners

Any object with the four core methods satisfies the protocol — no base class to
inherit (the two storage hooks are dispatched only if present). The core calls
all four, so define each one, leaving the hooks you do not need as no-ops:

```python
class RejectionCounter:
    def __init__(self) -> None:
        self.rejected = 0

    def on_rejected(self, *, name: str) -> None:
        self.rejected += 1

    def on_state_change(self, *, name, old, new) -> None: ...
    def on_call(self, *, name, outcome, duration) -> None: ...
    def on_reset(self, *, name) -> None: ...
```
