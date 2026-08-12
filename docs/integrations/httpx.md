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
creating one; inspect its `state` and `snapshot()`. Hosts are only known at
runtime, so `transport.registry.items()` lists the breakers created so far and
`names()` just the names they were created under. Both are point-in-time copies:
a breaker created afterwards is not in them.

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

## Custom breaker keys

Pass `name_resolver` when the request host is transport plumbing rather than
the logical dependency identity. The callback receives the native
`httpx.Request` and returns the breaker name:

```python
import httpx

from interlock.integrations.httpx import AsyncCircuitBreakerTransport

transport = AsyncCircuitBreakerTransport(
    httpx.AsyncHTTPTransport(),
    name_resolver=lambda request: request.url.host.removesuffix('.query.consul'),
)
```

The same callback can split one gateway host into independent breakers, for
example by returning a name derived from the first path segment. It must return
a non-empty string containing something other than whitespace; invalid results
raise `ValueError` with the request URL before the wrapped transport performs
I/O.

The resolved name is the registry key and the name carried by
`CircuitOpenError` and every listener event. Use the resolver, rather than
rewriting labels in a listener, so breaker state and observability labels stay
aligned. Both synchronous and asynchronous transports accept the option.

## Share one registry across clients

Inject one caller-owned `Registry` when several clients should observe the
same dependency health. The transports then resolve the same name to the same
breaker and contribute to one sliding window:

```python
import httpx

from interlock import Config, Registry
from interlock.integrations.httpx import AsyncCircuitBreakerTransport, HttpStatusClassifier

registry = Registry(
    config=Config(failure_rate_threshold=0.25, minimum_number_of_calls=50),
    classifier=HttpStatusClassifier(),
)

client_a = httpx.AsyncClient(
    transport=AsyncCircuitBreakerTransport(httpx.AsyncHTTPTransport(), registry=registry)
)
client_b = httpx.AsyncClient(
    transport=AsyncCircuitBreakerTransport(httpx.AsyncHTTPTransport(), registry=registry)
)
```

The classifier is intentional: a bare `Registry` classifies raised exceptions
but treats returned responses, including `503`, as successes. Configure
`HttpStatusClassifier` to retain the transport's default status policy. A
supplied registry also owns `config`, `clock`, `initial_state`, `classifier`,
and `listener`; combining `registry` with any of those transport options raises
`ValueError` instead of silently ignoring one source of configuration.

Share such a registry with HTTP clients only. `HttpStatusClassifier` reads
`.status_code` off every result it records, so a breaker taken from the same
registry for non-HTTP work — `registry.get('db')` — raises `AttributeError` the
first time that call returns. Keep a separate registry for those.

Closing a client automatically closes its breakers only when the transport
owns the registry. An injected registry remains open while the wrapped
connection pool closes; the application must explicitly call
`await registry.aclose_all()` during async shutdown, or `registry.close_all()`
when every guarded client is synchronous.

## What counts as a failure

The default `HttpStatusClassifier` counts these as failures:

- transport exceptions raised before a response is returned;
- response statuses `429, 500, 502, 503, 504`.

Other responses, including caller errors such as `404`, count as successes.
So are the transport exceptions httpx raises for the *caller's* own bug —
`UnsupportedProtocol` (a scheme-less or unsupported URL) and
`LocalProtocolError` (the local side violating HTTP). They are deterministic
and say nothing about the dependency, so a burst of them must not open the
circuit of a healthy host. They still propagate to the caller unchanged.

`PoolTimeout` is *not* excluded: an exhausted pool is usually the dependency
holding connections open, and shedding load then is the point. Exclude it
explicitly when your pool is sized below your own burst:

```python
import httpx
from interlock.integrations.httpx import HttpStatusClassifier

classifier = HttpStatusClassifier(
    excluded_exceptions=(httpx.LocalProtocolError, httpx.UnsupportedProtocol, httpx.PoolTimeout),
)
```

`excluded_exceptions` replaces the default set — pass `()` to count every
exception as a failure. An excluded exception is recorded as a *success*: the
sliding window has no third outcome.

Pass `HttpStatusClassifier(failure_statuses={...})` or another
`FailureClassifier` to change the status side of the policy.

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
every breaker created by the transport:

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
