# httpx2 integration

The `interlock-cb[httpx2]` extra wraps an [httpx2](https://pypi.org/project/httpx2/)
transport so a circuit breaker is applied **per host** transparently — no
decorators or `call` wrappers in your request code.

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
from interlock.httpx2 import CircuitBreakerTransport

transport = CircuitBreakerTransport(httpx2.HTTPTransport())
client = httpx2.Client(transport=transport)

response = client.get('https://api.example.com/v1/users')
```

## Asynchronous client

```python
import httpx2
from interlock.httpx2 import AsyncCircuitBreakerTransport

transport = AsyncCircuitBreakerTransport(httpx2.AsyncHTTPTransport())
client = httpx2.AsyncClient(transport=transport)

response = await client.get('https://api.example.com/v1/users')
```

## Per-host isolation

Each host gets its own breaker, created lazily and cached. A failing
`api.a.example.com` trips only its own breaker; requests to
`api.b.example.com` are unaffected. Per-instance, per-host state is usually more
correct than global state — each host's health is observed independently.

When a host's breaker is open, its requests raise `CircuitOpenError` before
reaching the network.

## What counts as a failure

By default the transport uses `HttpStatusClassifier`:

- any transport exception (connect/read errors) → failure;
- a response with status `429, 500, 502, 503, 504` → failure;
- everything else, including `4xx` client errors like `404`, → success.

This mirrors the retryable set used by urllib3, AWS and Google clients.
Permanent `5xx` (`501`, `505`) are deliberately excluded — retrying or tripping
the breaker cannot fix a contract or protocol error.

## Tuning

Pass any of `config`, `clock`, `classifier`, `listener` to the transport; they
flow to every per-host breaker:

```python
from interlock import Config, LoggingEventListener
from interlock.httpx2 import CircuitBreakerTransport

transport = CircuitBreakerTransport(
    httpx2.HTTPTransport(),
    config=Config(failure_rate_threshold=0.25, minimum_number_of_calls=50),
    listener=LoggingEventListener(),
)
```

Supply your own `classifier` to change the failure policy — for example to also
fail on `408 Request Timeout`.
