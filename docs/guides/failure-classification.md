# Failure classification

What counts as a failure is a separate concern from *when to trip* (thresholds,
in [Config](configuration.md)). It is decided by a `FailureClassifier`.

## Default policy

By default, a call is a failure exactly when it **raises**, and any returned
value is a success:

```python
from interlock import CircuitBreaker

breaker = CircuitBreaker(name='svc')   # DefaultFailureClassifier
```

This is right for code that signals errors by raising. It is *not* enough when
failure is encoded in a **return value** — for example an HTTP response object
whose `503` status means the dependency is unhealthy.

## Classify by result

A classifier implements one method. The `result`/`exception` pair is mutually
exclusive: when `exception` is not `None` the call raised; otherwise `result`
holds the return value.

```python
from interlock import CircuitBreaker

class StatusClassifier:
    def is_failure(self, *, result: object, exception: BaseException | None) -> bool:
        if exception is not None:
            return True
        return getattr(result, 'status_code', 200) >= 500

breaker = CircuitBreaker(name='api', classifier=StatusClassifier())
result = breaker.call(client.get, url)   # a 503 response now counts as a failure
```

Result-based classification needs the return value, so it works with the
**decorator** and **`call`**, but not the context manager (which only sees
exceptions and duration).

## Ignore expected errors

Business errors — a `404`, a validation failure — should not open the circuit.
Encode that by treating only the exceptions you care about as failures:

```python
class IgnoreNotFound:
    def is_failure(self, *, result: object, exception: BaseException | None) -> bool:
        if isinstance(exception, NotFoundError):
            return False          # expected, not a dependency problem
        return exception is not None
```

## HTTP out of the box

For httpx2, you do not need to write this yourself — the
[httpx2 integration](../integrations/httpx2.md) ships `HttpStatusClassifier`,
which treats transport exceptions and the canonical retryable statuses
(`429, 500, 502, 503, 504`) as failures.
