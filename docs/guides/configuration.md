# Configuration

`Config` is an immutable (frozen) dataclass validated on construction. Pass it
to a `CircuitBreaker` or share it across a `Registry`. All fields are
keyword-only.

```python
from interlock import Config
from interlock import WindowType

config = Config(
    failure_rate_threshold=0.5,
    minimum_number_of_calls=20,
    slow_call_duration_threshold=2.0,
    slow_call_rate_threshold=1.0,
    permitted_calls_in_half_open=10,
    max_concurrent_probes=1,
    wait_duration_in_open=30.0,
    window_type=WindowType.COUNT_BASED,
    window_size=100,
)
```

## Fields

| Field | Default | Meaning |
|-------|---------|---------|
| `failure_rate_threshold` | `0.5` | Trip when the failure rate reaches this fraction. Range `(0, 1]`. |
| `minimum_number_of_calls` | `10` | Minimum calls in the window before a rate is trusted. Guards against `1/1 = 100%`. |
| `slow_call_duration_threshold` | `60.0` | Calls at or above this many seconds are **slow**. |
| `slow_call_rate_threshold` | `1.0` | Trip when the slow-call rate reaches this fraction. Range `(0, 1]`. |
| `permitted_calls_in_half_open` | `10` | Probe calls allowed while `HALF_OPEN`. |
| `max_concurrent_probes` | `1` | Cap on **simultaneous** probes in `HALF_OPEN`. Must be in `[1, permitted_calls_in_half_open]`. |
| `wait_duration_in_open` | `60.0` | Seconds to stay `OPEN` before the first probe is allowed. |
| `window_type` | `COUNT_BASED` | `COUNT_BASED` or `TIME_BASED`. |
| `window_size` | `100` | Last N calls (count-based) or last N seconds (time-based). |

Validation raises `ValueError` eagerly for out-of-range or inconsistent values,
so a misconfigured breaker fails at construction rather than in production.

## Windows

- **Count-based** keeps the last `window_size` calls. Predictable memory,
  independent of traffic rate. The default.
- **Time-based** keeps calls from the last `window_size` seconds. The right
  choice for high-throughput services where "last N calls" is a moving target.

```python
from interlock import Config, WindowType

# Trip on a 50% failure rate observed over the last 30 seconds.
Config(window_type=WindowType.TIME_BASED, window_size=30)
```

## Why slow calls matter

A dependency that answers slowly but never errors will never trip a
failure-rate breaker, yet it still exhausts your timeouts and threads.
Slow-call detection treats latency as a first-class failure signal. By default
`slow_call_rate_threshold=1.0` means slowness alone never trips the breaker
until you tune it down — safe to leave on while you observe.

## Sharing config with a Registry

```python
from interlock import Config, Registry

registry = Registry(config=Config(minimum_number_of_calls=20))

payments = registry.get('payments')                       # shared default
search = registry.get('search', config=Config(window_size=500))  # per-name override
```

The override applies only when the breaker is first created; later `get` calls
with the same name return the existing instance and ignore the `config`
argument.
