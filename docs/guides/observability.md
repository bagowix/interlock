# Observability

A breaker reports everything it does through an `EventListener`. The same
hooks back logging, metrics, and any custom sink.

## The hooks

```python
class CoreEventListener(Protocol):
    def on_state_change(self, *, name: str, old: State, new: State) -> None: ...
    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None: ...
    def on_rejected(self, *, name: str) -> None: ...
    def on_reset(self, *, name: str) -> None: ...


class StorageEventListener(Protocol):
    def on_storage_degraded(self, *, name: str, error: BaseException) -> None: ...
    def on_storage_recovered(self, *, name: str) -> None: ...
    def on_storage_write_dropped(self, *, name: str) -> None: ...


class PipelineEventListener(Protocol):
    def on_retry(self, *, name: str, attempt: int, delay: float) -> None: ...
    def on_bulkhead_rejected(self, *, name: str) -> None: ...
    def on_fallback(self, *, name: str, error: BaseException) -> None: ...


class EventListener(
    CoreEventListener,
    StorageEventListener,
    PipelineEventListener,
    Protocol,
): ...
```

Listeners are called **outside** the breaker's lock, after the protected call
returns, so a slow listener never serialises throughput.

The protocols follow the owner of each event. `CoreEventListener` observes the
breaker itself, `StorageEventListener` observes breakers coordinated through a
shared [storage](../integrations/redis.md), and `PipelineEventListener` observes
[pipeline strategies](pipeline.md) given a `listener=`. `EventListener` combines
all three for sinks that observe the complete library.

The `name` namespace follows the same boundary: core and storage hooks receive
the **breaker name**, while pipeline hooks receive the **strategy name**. A
listener can therefore use `name` directly as its breaker or strategy label
without rediscovering the distinction from the call site.

Every hook is dispatched only if present, and each protocol supplies no-op
implementations for subclasses. A listener can override just the hooks it cares
about and keeps working when a later interlock version adds a new hook.

## Listener failures are isolated

Observability is optional; the protected call is not. A listener runs on the
breaker's own paths, so a bug in one — a metrics exporter with a stale label,
a logging handler with a full queue — would otherwise become a new failure
source. interlock guarantees it cannot:

- an `Exception` raised by a hook is logged to the `interlock` logger at
  `ERROR` (with the traceback) and then ignored;
- the protected call keeps its result, and a failing call keeps *its own*
  exception — a listener never masks the dependency's error;
- state transitions, probe accounting and the coordinated-mode background lane
  continue unaffected;
- a raising hook is never reported as storage degradation.

`BaseException` is **not** caught: `KeyboardInterrupt` and
`asyncio.CancelledError` propagate from a hook exactly as they do everywhere
else in interlock.

This covers `EventListener` hooks only. User-supplied *policy* callbacks are
part of the call's behaviour rather than observations of it, and keep raising
as before: a [`FailureClassifier`](failure-classification.md), a
[fallback](pipeline.md) function, a tenacity `before_sleep` hook.

To be notified when your own listener misbehaves, watch the `interlock`
logger:

```python
import logging

logging.getLogger('interlock').setLevel(logging.ERROR)
```

Attach one per breaker, or share one across a `Registry`:

```python
breaker = CircuitBreaker(name='payments', listener=my_listener)
registry = Registry(listener=my_listener)  # every breaker reports here
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

Supports `opentelemetry-api>=1.20.0` — the listener only calls
`get_meter`/`create_histogram`/`create_counter`, stable across any
`opentelemetry-distro`/SDK release from that version on.

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
| `interlock.storage.events` | counter | `breaker`, `event` (`degraded`/`recovered`/`write_dropped`), `error` |

## Custom listeners

For a partial listener that strict type checkers can verify, inherit the narrowest
protocol for its owner and override only the hooks you need. Every inherited hook
is a no-op:

```python
from interlock import CoreEventListener


class RejectionCounter(CoreEventListener):
    def __init__(self) -> None:
        self.rejected = 0

    def on_rejected(self, *, name: str) -> None:
        self.rejected += 1
```

Use `StorageEventListener` for storage-only sinks, `PipelineEventListener` for
strategy-only sinks, or `EventListener` when one object handles every group.
Inheritance is optional for a listener that structurally implements the relevant
protocol. At runtime, dispatch is still by name and skips any missing hook,
including on older listener objects that inherit none of them.
