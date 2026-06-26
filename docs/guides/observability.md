# Observability

A breaker reports everything it does through an `EventListener`. The same four
hooks back logging, metrics, and any custom sink.

## The hooks

```python
class EventListener(Protocol):
    def on_state_change(self, *, name: str, old: State, new: State) -> None: ...
    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None: ...
    def on_rejected(self, *, name: str) -> None: ...
    def on_reset(self, *, name: str) -> None: ...
```

Listeners are called **outside** the breaker's lock, after the protected call
returns, so a slow listener never serialises throughput. Implementations must
not raise back into the core.

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

The OTel listener lives in the `interlock[otel]` extra and is imported
explicitly, so the core stays dependency-free:

```bash
uv add 'interlock[otel]'
```

```python
from interlock import CircuitBreaker
from interlock.otel import OTelEventListener

breaker = CircuitBreaker(name='payments', listener=OTelEventListener())
```

It records four instruments on the `interlock` meter (or a meter you pass in):

| Instrument | Type | Labels |
|------------|------|--------|
| `interlock.call.duration` | histogram (s) | `breaker`, `outcome` |
| `interlock.call.rejected` | counter | `breaker` |
| `interlock.state.changes` | counter | `breaker`, `from`, `to` |
| `interlock.reset` | counter | `breaker` |

## Custom listeners

Any object with the four methods satisfies the protocol — no base class to
inherit. The core calls all four, so define each one, leaving the hooks you do
not need as no-ops:

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
