# httpx2

The `interlock-cb[httpx2]` extra wraps an [httpx2](https://pypi.org/project/httpx2/)
transport so a circuit breaker is applied **per host** transparently — no
decorators or `call` wrappers in your request code.

`interlock-cb` is listed in httpx2's official
[third-party packages directory](https://github.com/pydantic/httpx2/blob/main/docs/third_party_packages.md#interlock-cb).

=== "uv"

    ```bash
    uv add 'interlock-cb[httpx2]'
    ```

=== "pip"

    ```bash
    pip install 'interlock-cb[httpx2]'
    ```

=== "poetry"

    ```bash
    poetry add 'interlock-cb[httpx2]'
    ```

## Synchronous client

```python
import httpx2
from interlock.integrations.httpx2 import CircuitBreakerTransport

transport = CircuitBreakerTransport(httpx2.HTTPTransport())
client = httpx2.Client(transport=transport)

response = client.get('https://api.example.com/v1/users')
```

## Asynchronous client

```python
import httpx2
from interlock.integrations.httpx2 import AsyncCircuitBreakerTransport

transport = AsyncCircuitBreakerTransport(httpx2.AsyncHTTPTransport())
client = httpx2.AsyncClient(transport=transport)

response = await client.get('https://api.example.com/v1/users')
```

Use the client as a context manager. Context entry and exit are delegated to
the wrapped transport, including for custom transports that acquire resources
in `__enter__` or `__aenter__`. Closing the client closes both the wrapped
connection pool and every breaker created by the transport.

## Safe production rollout

Start in shadow mode when introducing the integration to existing traffic:

```python
import httpx2

from interlock import State
from interlock.integrations.httpx2 import AsyncCircuitBreakerTransport

transport = AsyncCircuitBreakerTransport(
    httpx2.AsyncHTTPTransport(),
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
`names()` just their hosts. Both are point-in-time copies: a host first seen
afterwards is not in them.

After tuning thresholds, deploy a new transport with the default
`initial_state=State.CLOSED`. Do not reset only the currently known hosts: a
transport configured for shadow mode would still create future hosts in
`METRICS_ONLY`. See the complete [safe-rollout guide](../guides/states.md#safe-rollout).

## Per-host isolation

Each host gets its own breaker, created lazily and cached. A failing
`api.a.example.com` trips only its own breaker; requests to
`api.b.example.com` are unaffected. Per-instance, per-host state is usually more
correct than global state — each host's health is observed independently.

When a host's breaker is open, its requests raise `CircuitOpenError` before
reaching the network.

## Custom breaker keys

Pass `name_resolver` when the request host is not the logical dependency
identity. The callback receives the native `httpx2.Request` and returns the
breaker name:

```python
import httpx2

from interlock.integrations.httpx2 import AsyncCircuitBreakerTransport

transport = AsyncCircuitBreakerTransport(
    httpx2.AsyncHTTPTransport(),
    name_resolver=lambda request: request.url.host.removesuffix('.query.consul'),
)
```

Returning the same name for several discovery hosts gives them one breaker;
deriving a name from the path can split independent upstreams behind one
gateway host. The result must be a non-empty string containing something other
than whitespace. Invalid results raise `ValueError` with the request URL before
the wrapped transport performs I/O.

The resolved name is used consistently as the registry key, in
`CircuitOpenError`, and in every listener event. Resolve the name here instead
of rewriting listener labels so metrics always identify the breaker whose
state they report. Both synchronous and asynchronous transports accept the
option.

## Share one registry across clients

Pass a caller-owned `Registry` when several clients reach the same dependency.
Requests resolving to the same name then use one breaker instance and one
sliding window, even when they travel through different transports:

```python
import httpx2

from interlock import Config, Registry
from interlock.integrations.httpx2 import AsyncCircuitBreakerTransport, HttpStatusClassifier

registry = Registry(
    config=Config(failure_rate_threshold=0.25, minimum_number_of_calls=50),
    classifier=HttpStatusClassifier(),
)

client_a = httpx2.AsyncClient(
    transport=AsyncCircuitBreakerTransport(httpx2.AsyncHTTPTransport(), registry=registry)
)
client_b = httpx2.AsyncClient(
    transport=AsyncCircuitBreakerTransport(httpx2.AsyncHTTPTransport(), registry=registry)
)
```

`Registry` uses exception-only classification by default. Always configure
`HttpStatusClassifier` as above when you want the integration's normal HTTP
status policy; otherwise a returned `503` counts as a success. The supplied
registry owns `config`, `clock`, `initial_state`, `classifier`, and `listener`,
so none of those options may also be passed to the transport.

Share such a registry with HTTP clients only. `HttpStatusClassifier` reads
`.status_code` off every result it records, so a breaker taken from the same
registry for non-HTTP work — `registry.get('db')` — raises `AttributeError` the
first time that call returns. Keep a separate registry for those.

Closing a client automatically closes its breakers only when the transport
owns the registry. An injected registry remains open while the wrapped
connection pool closes; the application must explicitly call
`await registry.aclose_all()` during async shutdown (or `registry.close_all()`
when all guarded clients are synchronous).

## What counts as a failure

By default the transport uses `HttpStatusClassifier`:

- a transport exception (connect/read errors) → failure;
- a response with status `429, 500, 502, 503, 504` → failure;
- everything else, including `4xx` client errors like `404`, → success;
- `UnsupportedProtocol` and `LocalProtocolError` → success.

This mirrors the retryable set used by urllib3, AWS and Google clients.
Permanent `5xx` (`501`, `505`) are deliberately excluded — retrying or tripping
the breaker cannot fix a contract or protocol error.

The two excluded exceptions are the caller's own bug — a scheme-less or
unsupported URL, and the local side violating HTTP. They are deterministic and
say nothing about the dependency, so a burst of them must not open the circuit
of a healthy host; they still propagate to the caller unchanged.

`PoolTimeout` is *not* excluded: an exhausted pool is usually the dependency
holding connections open, and shedding load then is the point. Exclude it
explicitly when your pool is sized below your own burst:

```python
import httpx2
from interlock.integrations.httpx2 import HttpStatusClassifier

classifier = HttpStatusClassifier(
    excluded_exceptions=(
        httpx2.LocalProtocolError,
        httpx2.UnsupportedProtocol,
        httpx2.PoolTimeout,
    ),
)
```

`excluded_exceptions` replaces the default set — pass `()` to count every
exception as a failure. An excluded exception is recorded as a *success*: the
sliding window has no third outcome.

## Tuning

Pass any of `config`, `clock`, `classifier`, `listener` to the transport; they
flow to every breaker:

```python
from interlock import Config, LoggingEventListener
from interlock.integrations.httpx2 import CircuitBreakerTransport

transport = CircuitBreakerTransport(
    httpx2.HTTPTransport(),
    config=Config(failure_rate_threshold=0.25, minimum_number_of_calls=50),
    listener=LoggingEventListener(),
)
```

Supply your own `classifier` to change the failure policy — for example to also
fail on `408 Request Timeout`.
