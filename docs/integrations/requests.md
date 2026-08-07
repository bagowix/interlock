# requests

The `interlock-cb[requests]` extra guards every request a `Session` sends
with a circuit breaker **per host** — mounted once, no decorators in call
sites.

=== "uv"

    ```bash
    uv add 'interlock-cb[requests]'
    ```

=== "pip"

    ```bash
    pip install 'interlock-cb[requests]'
    ```

=== "poetry"

    ```bash
    poetry add 'interlock-cb[requests]'
    ```

## Usage

`CircuitBreakerAdapter` subclasses `requests.adapters.HTTPAdapter` — the
library's native transport extension point — so it mounts like any adapter:

```python
import requests

from interlock.integrations.requests import CircuitBreakerAdapter

session = requests.Session()
adapter = CircuitBreakerAdapter()
session.mount('https://', adapter)
session.mount('http://', adapter)

response = session.get('https://api.example.com/orders')
```

Closing the session closes the adapter's connection pools and every breaker it
created.

Each host gets its own breaker (a failing `api.a` never trips `api.b`),
created lazily and shared across requests. When a host's circuit is open the
request raises [`CircuitOpenError`](../reference.md) *before* a connection is
made.

## Share one registry across sessions

Inject a caller-owned `Registry` when independent sessions should use one
breaker and one sliding window for the same host:

```python
import requests

from interlock import Config, Registry
from interlock.integrations.requests import CircuitBreakerAdapter, HttpStatusClassifier

registry = Registry(
    config=Config(failure_rate_threshold=0.25, minimum_number_of_calls=50),
    classifier=HttpStatusClassifier(),
)

session_a = requests.Session()
session_a.mount('https://', CircuitBreakerAdapter(registry=registry))

session_b = requests.Session()
session_b.mount('https://', CircuitBreakerAdapter(registry=registry))
```

The explicit classifier preserves the adapter's normal status policy. Without
it, a bare `Registry` classifies exceptions only and a returned `503` counts
as a success. The registry owns `config`, `clock`, `initial_state`,
`classifier`, and `listener`; combining `registry` with any of those adapter
options raises `ValueError`.

Closing either session still releases its adapter's connection pools, but it
does not close the injected registry. The application owns the registry and
must call `registry.close_all()` once during shutdown.

## Safe production rollout

Pass `initial_state=State.METRICS_ONLY` to record real outcomes without
rejecting requests. The state is applied to every lazily created host before
its first request:

```python
from interlock import State

adapter = CircuitBreakerAdapter(
    initial_state=State.METRICS_ONLY,
    listener=metrics_listener,
)
```

The public `adapter.registry` supports local diagnosis with
`get_existing(host)`, `state` and `snapshot()`. Use an `EventListener` for
production metrics, then deploy a new adapter with the default `CLOSED` state
to begin enforcement. See [Safe rollout](../guides/states.md#safe-rollout).

## Failure policy

By default a response counts as a failure when its status is in the canonical
retryable set (`429, 500, 502, 503, 504`) and any transport exception
(connect/read errors) is a failure; `4xx` client mistakes like `404` are
successes. Change the statuses, or the whole policy:

```python
from interlock import Config
from interlock.integrations.requests import CircuitBreakerAdapter, HttpStatusClassifier

adapter = CircuitBreakerAdapter(
    config=Config(failure_rate_threshold=0.3),
    classifier=HttpStatusClassifier(failure_statuses={408, 429, 500, 502, 503, 504}),
)
```

Any custom `FailureClassifier` works too — see
[Failure classification](../guides/failure-classification.md).

## Tuning and observability

The adapter accepts the same collaborators as `CircuitBreaker` — `config`,
`clock`, `initial_state`, `classifier`, `listener` — and forwards everything
else
(`pool_connections`, `max_retries`, ...) to `HTTPAdapter`. Note that
`max_retries` is urllib3's connection-level retry; for application-level
retries combine with the [tenacity integration](tenacity.md) and read
[Retries and circuit breakers](../guides/retries.md) first.
