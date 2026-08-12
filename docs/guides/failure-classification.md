# Failure classification

What counts as a failure is a separate concern from *when to trip* (thresholds,
in [Config](configuration.md)). It is decided by a `FailureClassifier`.

## Default policy

By default, a call is a failure exactly when it **raises**, and any returned
value is a success:

```python
from interlock import CircuitBreaker

breaker = CircuitBreaker(name='svc')  # DefaultFailureClassifier
```

This is right for code that signals errors by raising. It is *not* enough when
failure is encoded in a **return value** — for example an HTTP response object
whose `503` status means the dependency is unhealthy.

## Classify by result

A classifier implements one method. The `result`/`exception` pair is mutually
exclusive: when `exception` is not `None` the call raised; otherwise `result`
holds the return value. `exception` is always an `Exception` — a
`BaseException` such as `CancelledError` says nothing about the dependency, so
the breaker releases the call without classifying it.

```python
from interlock import CircuitBreaker


class StatusClassifier:
    def is_failure(self, *, result: object, exception: Exception | None) -> bool:
        if exception is not None:
            return True
        return getattr(result, 'status_code', 200) >= 500


breaker = CircuitBreaker(name='api', classifier=StatusClassifier())
result = breaker.call(client.get, url)  # a 503 response now counts as a failure
```

Result-based classification needs the return value, so it works with the
**decorator** and **`call`**, but not the context manager (which only sees
exceptions and duration).

## Ignore expected errors

Business errors — a `404`, a validation failure — should not open the circuit.
Encode that by treating only the exceptions you care about as failures:

```python
class IgnoreNotFound:
    def is_failure(self, *, result: object, exception: Exception | None) -> bool:
        if isinstance(exception, NotFoundError):
            return False  # expected, not a dependency problem
        return exception is not None
```

An ignored exception is recorded as a **success** — the sliding window has only
two outcomes — and still propagates to the caller. It therefore dilutes the
failure rate rather than being invisible to it.

## HTTP out of the box

For HTTP clients, you do not need to write this yourself — the
[httpx2](../integrations/httpx2.md), [httpx](../integrations/httpx.md),
[aiohttp](../integrations/aiohttp.md), and
[requests](../integrations/requests.md) integrations ship
`HttpStatusClassifier`, which treats the canonical retryable statuses
(`429, 500, 502, 503, 504`) and every non-excluded transport exception as
failures.

Errors the *caller* caused are excluded for the same reason as a `404`: a
scheme-less URL or a local protocol violation is a bug in your code, not
evidence that the dependency is unhealthy, and counting it would open the
circuit of a host that is answering fine. The default set differs per
integration, because each client library raises different things inside the
guarded call:

| Integration | Excluded by default |
|---|---|
| [httpx2](../integrations/httpx2.md), [httpx](../integrations/httpx.md) | `UnsupportedProtocol`, `LocalProtocolError` |
| [requests](../integrations/requests.md) | `InvalidURL` (and its `InvalidProxyURL` subclass) |
| [aiohttp](../integrations/aiohttp.md) | nothing — it rejects malformed URLs before the middleware chain runs, so every handler exception counts unless you exclude it |

All four take `excluded_exceptions=(...)` to replace that set — `()` counts
every exception as a failure. Entries must be `Exception` subclasses;
anything else raises `TypeError` at construction.
