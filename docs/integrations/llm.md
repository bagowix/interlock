# LLM SDKs (OpenAI, Anthropic) — recipe

LLM APIs fail in exactly the ways circuit breakers exist for: rate limits
(`429`), overloaded backends (`529`/`503`), long hangs. A breaker around your
LLM calls stops a degraded provider from stalling every request thread, and
bounded retries recover from blips without amplifying an outage.

You can protect an SDK at either its call boundary, which enables SDK-specific
classification and slow-call detection, or at the httpx transport,
which applies transparently to every request made by that client.

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

    def is_failure(self, *, result: object, exception: Exception | None) -> bool:
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

## Transport-level protection

OpenAI and Anthropic clients accept an httpx client. Install
`interlock-cb[httpx]`, wrap the SDK's underlying transport, and every endpoint
on the provider host shares the same breaker:

```python
import httpx
from openai import DefaultHttpxClient, OpenAI

from interlock.integrations.httpx import CircuitBreakerTransport

transport = CircuitBreakerTransport(httpx.HTTPTransport())

with OpenAI(
    http_client=DefaultHttpxClient(transport=transport),
    max_retries=0,
) as client:
    response = client.responses.create(model='gpt-5.5', input='Summarise this document...')
```

Use `AsyncCircuitBreakerTransport`, `httpx.AsyncHTTPTransport`, and the SDK's
async client for async applications. `max_retries=0` gives the breaker one
observable attempt per SDK call; if retries are required, keep one explicit,
bounded retry owner instead of stacking SDK and application retries.

The transport classifier sees HTTP statuses directly, so no SDK exception
classifier is needed. Choose the call-boundary recipe above when you also need
to classify SDK-specific exceptions, measure the complete SDK operation as a
slow call, or use a breaker name that is not derived from the request host.
