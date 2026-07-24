# Retries and circuit breakers

Retries and circuit breakers pull in opposite directions: a retry *adds*
load to a struggling dependency, a breaker *sheds* it. Combined carelessly
they cancel each other out — retries hammer a dependency the breaker is
trying to protect, or the breaker's window never sees the real failure rate.
This guide fixes the composition; the ready-made tenacity helpers live in the
[tenacity integration](../integrations/tenacity.md).

## Which goes on the outside?

Both orders are valid — they answer different questions. What changes is what
the breaker's sliding window *sees*:

| Order | What the window sees | When to choose |
|---|---|---|
| **Retry outside → breaker inside** (recommended) | Every attempt individually — honest failure rate, the breaker trips as early as the dependency deserves | Default. Also the order used by Polly and resilience4j |
| Breaker outside → retry inside | One aggregated outcome per *operation* (all attempts folded into it) | When thresholds are tuned per business operation, not per request |

With retry outside, a rejected attempt is also visible to the retry loop —
which is exactly where the two failure modes below come from.

## Failure mode 1: retrying an open circuit

`CircuitOpenError` is not a transient error. The breaker rejects instantly,
so an exponential backoff loop around it burns its attempt budget in
milliseconds, never reaches the dependency, and buries the real signal in log
noise. Stop retrying the moment the circuit opens:

```python
from tenacity import Retrying, stop_after_attempt, wait_exponential_jitter

from interlock.integrations.tenacity import retry_unless_open

retrying = Retrying(
    retry=retry_unless_open(TimeoutError, ConnectionError),
    wait=wait_exponential_jitter(),
    stop=stop_after_attempt(5),
    reraise=True,
)
```

## Failure mode 2: blind waiting

Sometimes waiting *is* the right call — a nightly job would rather sleep
than fail. But `2^n` seconds is the wrong amount: the breaker already knows
when it will allow the next probe (`CircuitOpenError.retry_after`). Wait
exactly that long:

```python
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt
from tenacity import wait_exponential_jitter

from interlock import CircuitOpenError
from interlock.integrations.tenacity import wait_probe

retrying = AsyncRetrying(
    retry=retry_if_exception_type((TimeoutError, CircuitOpenError)),
    wait=wait_probe(wait_exponential_jitter()),
    stop=stop_after_attempt(10),
    reraise=True,
)
```

Pick one mode per call site. Fail fast at request/latency-sensitive
boundaries; be patient in background work.

## Retrying on HTTP statuses

The HTTP integrations classify statuses for the *breaker* without raising —
a `503` response is returned to you, recorded as a failure. tenacity,
however, is exception-driven. Do **not** reach for `retry_if_result`: with
aiohttp a retried-away response is never released and leaks its connection.
Turn bad statuses into exceptions instead, then retry exceptions:

```python
import requests
from tenacity import Retrying, stop_after_attempt, wait_exponential_jitter

from interlock.integrations.requests import CircuitBreakerAdapter
from interlock.integrations.tenacity import retry_unless_open

session = requests.Session()
session.mount('https://', CircuitBreakerAdapter())


def fetch_orders() -> dict:
    response = session.get('https://api.example.com/orders')
    response.raise_for_status()
    return response.json()


retrying = Retrying(
    retry=retry_unless_open(requests.HTTPError, requests.ConnectionError),
    wait=wait_exponential_jitter(),
    stop=stop_after_attempt(5),
    reraise=True,
)

orders = retrying(fetch_orders)
```

The breaker still classifies by status (no exception needed), the retry loop
reacts to `raise_for_status()` — each tool sees the signal in its native
form. To align which statuses trip the breaker, pass
`HttpStatusClassifier(failure_statuses={...})` to the integration.

## Anti-patterns

- **Unbounded retries.** Always set a `stop` condition. A breaker caps
  concurrent damage, not the lifetime of a stubborn loop.
- **Retries without a breaker.** N clients × M retries is an N·M-fold
  amplification aimed at a dependency that is already failing — the classic
  retry storm. The breaker inside the loop is what breaks it.
- **Retrying non-transient errors.** A `404` or a validation error will not
  succeed on attempt five. List transient exception types explicitly in
  `retry_unless_open(...)` rather than retrying everything.
- **Nested retry layers.** urllib3's `max_retries`, your service mesh and
  tenacity each multiply attempts. Budget them together — one deliberate
  retry layer beats three accidental ones.

## Declarative composition

Everything on this page stays valid with the v2 [resilience
pipeline](pipeline.md) — `RetryStrategy` packages the same predicates and
wait strategies as a layer, so the manual recipe becomes::

```python
pipeline = (
    Pipeline.builder()
    .retry(attempts=4)  # retry outside — the recommended order
    .circuit_breaker(breaker)
    .timeout(2.0)
    .build()
)
```
