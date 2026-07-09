# tenacity (retries)

interlock deliberately ships no retry engine of its own:
[tenacity](https://tenacity.readthedocs.io/) already does backoff, jitter,
stop conditions and predicates well. The `interlock-cb[tenacity]` extra adds
the glue where retry × breaker composition goes wrong in practice.

=== "uv"

    ```bash
    uv add 'interlock-cb[tenacity]'
    ```

=== "pip"

    ```bash
    pip install 'interlock-cb[tenacity]'
    ```

=== "poetry"

    ```bash
    poetry add 'interlock-cb[tenacity]'
    ```

Read [Retries and circuit breakers](../guides/retries.md) first if you are
deciding *how* to combine the two patterns; this page documents the helpers.

## Fail fast (recommended default)

`retry_unless_open(*transient)` retries the listed transient exceptions but
stops as soon as the breaker opens. `CircuitOpenError` is not transient: the
breaker rejects instantly, so backing off and retrying it only burns the
attempt budget without ever reaching the dependency.

```python
from tenacity import Retrying, stop_after_attempt, wait_exponential_jitter

from interlock import CircuitBreaker
from interlock.integrations.tenacity import retry_unless_open

breaker = CircuitBreaker(name='payments')


@breaker
def charge(amount: int) -> str:
    return gateway.charge(amount)


retrying = Retrying(
    retry=retry_unless_open(TimeoutError, ConnectionError),
    wait=wait_exponential_jitter(),
    stop=stop_after_attempt(5),
    reraise=True,
)

result = retrying(charge, 100)
```

Called without arguments, `retry_unless_open()` retries any ordinary
`Exception` — still never `CircuitOpenError`.

## Patient mode (wait for the probe)

Background jobs often prefer waiting over failing. `wait_probe(fallback)` is
a wait strategy: when the last attempt was rejected with a `retry_after`
estimate, it sleeps *exactly* until the breaker allows the next probe (plus a
small jitter so concurrent waiters do not storm the single probe slot). Any
other outcome delegates to the `fallback` strategy.

```python
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from interlock import CircuitOpenError
from interlock.integrations.tenacity import wait_probe

retrying = AsyncRetrying(
    retry=retry_if_exception_type((TimeoutError, CircuitOpenError)),
    wait=wait_probe(wait_exponential_jitter()),
    stop=stop_after_attempt(10),
    reraise=True,
)

report = await retrying(nightly_export)
```

Note the retry predicate: patient mode deliberately *does* retry
`CircuitOpenError` — that is what makes `wait_probe` see the rejection and
wait the right amount. Keep a `stop` condition anyway; a dependency can stay
down longer than any job should wait.

`wait_probe(..., jitter=0.5)` widens the random extra wait (seconds) added on
top of `retry_after`; when the rejection carries no estimate (for example
after `force_open()`), the fallback strategy decides.

## Everything else is plain tenacity

`Retrying`, `AsyncRetrying`, the `@retry` decorator, stop and wait strategies
compose as usual — the helpers are ordinary tenacity predicates and wait
objects, so you can combine them with `|`, `retry_any`, `wait_chain` and
friends.
