# httpx

The `interlock-cb[httpx]` extra wraps an
[httpx](https://www.python-httpx.org/) transport so a circuit breaker is
applied **per host** transparently. It supports httpx 0.27.0 and newer.

=== "uv"

    ```bash
    uv add 'interlock-cb[httpx]'
    ```

=== "pip"

    ```bash
    pip install 'interlock-cb[httpx]'
    ```

=== "poetry"

    ```bash
    poetry add 'interlock-cb[httpx]'
    ```

## Synchronous client

```python
import httpx
from interlock.integrations.httpx import CircuitBreakerTransport

transport = CircuitBreakerTransport(httpx.HTTPTransport())
client = httpx.Client(transport=transport)

response = client.get('https://api.example.com/v1/users')
```

## Asynchronous client

```python
import httpx
from interlock.integrations.httpx import AsyncCircuitBreakerTransport

transport = AsyncCircuitBreakerTransport(httpx.AsyncHTTPTransport())
client = httpx.AsyncClient(transport=transport)

response = await client.get('https://api.example.com/v1/users')
```

Use the client as a context manager. Closing it delegates `close()` or
`aclose()` to the wrapped transport and releases both the connection pool and
every breaker created by the transport.

## Safe production rollout

Start in shadow mode when introducing the integration to existing traffic:

```python
import httpx

from interlock import State
from interlock.integrations.httpx import AsyncCircuitBreakerTransport

transport = AsyncCircuitBreakerTransport(
    httpx.AsyncHTTPTransport(),
    initial_state=State.METRICS_ONLY,
    listener=metrics_listener,
)
```

Every host created later starts in `METRICS_ONLY` before its first request: it
records outcomes but never raises `CircuitOpenError`. Use the listener for
production metrics. For local diagnosis,
`transport.registry.get_existing(host)` returns an existing breaker without
creating one; inspect its `state` and `snapshot()`.

After tuning thresholds, deploy a new transport with the default
`initial_state=State.CLOSED`. Do not reset only the currently known hosts: a
transport configured for shadow mode would still create future hosts in
`METRICS_ONLY`. See the complete [safe-rollout guide](../guides/states.md#safe-rollout).

## Per-host isolation

Each host gets its own lazily created breaker. A failing
`api.a.example.com` trips only that host; traffic to `api.b.example.com`
continues normally. An open breaker raises `CircuitOpenError` before the
wrapped transport performs I/O. A request URL without a host raises
`ValueError` for the same reason: there is no dependency identity to key on.

## What counts as a failure

The default `HttpStatusClassifier` counts these as failures:

- transport exceptions raised before a response is returned;
- response statuses `429, 500, 502, 503, 504`.

Other responses, including caller errors such as `404`, count as successes.
Pass `HttpStatusClassifier(failure_statuses={...})` or another
`FailureClassifier` to change the policy.

## Streaming responses

The wrapper returns the original `httpx.Response` unchanged, so sync and async
streaming remain lazy and connection cleanup keeps httpx's normal semantics.
Context entry and exit are delegated to the wrapped transport, including for
custom transports that acquire resources in `__enter__` or `__aenter__`.
Because the circuit-breaker call completes when response headers arrive, an
exception raised later while consuming a streaming body is outside that call
and is not recorded by the breaker.

## Tuning

`config`, `clock`, `initial_state`, `classifier`, and `listener` are shared by
every per-host breaker created by the transport:

```python
import httpx

from interlock import Config, LoggingEventListener
from interlock.integrations.httpx import CircuitBreakerTransport

transport = CircuitBreakerTransport(
    httpx.HTTPTransport(),
    config=Config(failure_rate_threshold=0.25, minimum_number_of_calls=50),
    listener=LoggingEventListener(),
)
```

The transport never retries. If another layer owns retries, keep them bounded
and stop retrying when the breaker raises `CircuitOpenError`.
