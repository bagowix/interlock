# LLM SDKs (OpenAI, Anthropic) — recipe

LLM APIs fail in exactly the ways circuit breakers exist for: rate limits
(`429`), overloaded backends (`529`/`503`), long hangs. A breaker around your
LLM calls stops a degraded provider from stalling every request thread, and
bounded retries recover from blips without amplifying an outage.

This is a **recipe** — no extra needed beyond `interlock-cb[tenacity]`; the
SDKs raise typed exceptions, which is all the breaker needs.

## Classify SDK errors

Both SDKs raise `APIStatusError` subclasses carrying `status_code`, plus
connection/timeout errors. Not every error should trip the circuit: an
invalid request (`400`) or a missing model (`404`) is your bug, not the
provider's outage.

```python
import anthropic


class LLMFailureClassifier:
    """Trip on provider-side trouble, not on caller mistakes."""

    _FAILURE_STATUSES = frozenset({429, 500, 502, 503, 504, 529})

    def is_failure(self, *, result: object, exception: BaseException | None) -> bool:
        if exception is None:
            return False
        if isinstance(exception, anthropic.APIStatusError):
            return exception.status_code in self._FAILURE_STATUSES
        return isinstance(exception, (anthropic.APIConnectionError, anthropic.APITimeoutError))
```

For OpenAI, swap the exception types (`openai.APIStatusError`,
`openai.APIConnectionError`, `openai.APITimeoutError`) — the shape is
identical.

## Guard the calls

```python
import anthropic
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential_jitter

from interlock import CircuitBreaker, Config
from interlock.integrations.tenacity import retry_unless_open

client = anthropic.AsyncAnthropic()

breaker = CircuitBreaker(
    name='anthropic',
    config=Config(slow_call_duration_threshold=30.0),
    classifier=LLMFailureClassifier(),
)


@breaker
async def complete(prompt: str) -> str:
    message = await client.messages.create(
        model='claude-sonnet-5',
        max_tokens=1024,
        messages=[{'role': 'user', 'content': prompt}],
    )
    return message.content[0].text


retrying = AsyncRetrying(
    retry=retry_unless_open(
        anthropic.APIStatusError,
        anthropic.APIConnectionError,
        anthropic.APITimeoutError,
    ),
    wait=wait_exponential_jitter(initial=1.0, max=30.0),
    stop=stop_after_attempt(4),
    reraise=True,
)

answer = await retrying(complete, 'Summarise this document...')
```

What each layer contributes:

- **Slow-call detection** (`slow_call_duration_threshold`) counts calls
  slower than 30s as failures — a provider that still answers but takes a
  minute per completion trips the breaker too. No other signal catches this.
- **The breaker** stops sending after the failure rate crosses the threshold;
  while open, callers get `CircuitOpenError` in microseconds instead of
  hanging — fail over to a second provider or degrade gracefully.
- **`retry_unless_open`** retries provider blips with jittered backoff but
  stops the moment the circuit opens. The SDK's own retries overlap here —
  either set `max_retries=0` on the client and let tenacity own retries, or
  keep the SDK's and drop the tenacity layer; running both multiplies
  attempts.

## Multiple providers, one pattern

Give each provider its own breaker name (`anthropic`, `openai`, ...) via a
shared `Registry` and check `breaker.state` to route around an open provider.
The [states guide](../guides/states.md) covers manual failover controls.

!!! note "Transport-level alternative"
    The SDKs are built on classic httpx, which does not take httpx2
    transports; once they migrate to httpx2 you will be able to drop the
    decorator entirely and pass a client wrapped with the
    [httpx2 transport](httpx2.md) instead.
