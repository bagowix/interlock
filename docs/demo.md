# Runnable demo

Three self-contained scripts in [`examples/`](https://github.com/bagowix/interlock/tree/main/examples)
show a breaker doing its job — standard library plus `interlock-cb` only, no
network, no services to stand up. The output is deterministic: what you see
below is exactly what you get, so set a breakpoint anywhere and step through.

```bash
pip install interlock-cb          # or: uv add interlock-cb

python examples/lifecycle.py      # one breaker through its full state cycle
python examples/two_clients.py    # two clients, one outage, no collateral damage
python examples/pipeline.py       # timeout + breaker + fallback around a hanging service
```

Both scripts print through an `EventListener` — every line tagged
`[listener]` comes from the breaker itself, not from the demo code. See
[Observability](guides/observability.md) for the hook reference.

## `lifecycle.py` — one breaker, full cycle

A fake payment gateway is healthy, goes down, and recovers. The breaker is
configured tightly so the whole story fits in seven calls: trip at 50%
failures over a 4-call window, stay open 1 second, close after 2 good probes.

??? example "lifecycle.py — full source"

    ```python
    --8 < --'examples/lifecycle.py'
    ```

Running it prints:

```text
phase 1 — healthy dependency, breaker CLOSED
  [listener] payments-api: recorded SUCCESS (0.000s)
call 1: charged $10
  [listener] payments-api: recorded SUCCESS (0.000s)
call 2: charged $20
```

Every completed call is recorded into the sliding window — the listener's
`on_call` hook fires with the classified `Outcome` and the duration.

```text
phase 2 — the gateway goes down; failures fill the window
  [listener] payments-api: recorded FAILURE (0.000s)
call 3: dependency failed — 503 from gateway
  [listener] payments-api: recorded FAILURE (0.000s)
  [listener] payments-api: state CLOSED -> OPEN
call 4: dependency failed — 503 from gateway
```

Call 4 is the moment the window reaches `minimum_number_of_calls=4` with 2
failures out of 4 — exactly the 50% threshold — so recording it trips the
circuit: `on_state_change` fires *inside* call 4's bookkeeping, before the
demo's own `call 4:` line prints.

```text
phase 3 — circuit OPEN: calls fail fast, the gateway gets a break
  [listener] payments-api: call rejected — circuit is open
call 5: REJECTED in ~0ms, the gateway was never called (retry_after=1.0s)
```

Call 5 never reaches the gateway. `CircuitOpenError` is raised immediately
and carries `retry_after` — the estimate until the next probe is allowed.
This is the entire point of a breaker: while the dependency is down, callers
spend no timeouts on it and it gets quiet time to recover.

```text
phase 4 — after wait_duration_in_open the breaker probes
  [listener] payments-api: state OPEN -> HALF_OPEN
  [listener] payments-api: recorded SUCCESS (0.000s)
call 6: charged $60
  [listener] payments-api: recorded SUCCESS (0.000s)
  [listener] payments-api: state HALF_OPEN -> CLOSED
call 7: charged $70

final state: CLOSED, window reset: WindowSnapshot(total_calls=0, failed_calls=0, slow_calls=0)
```

The transition to `HALF_OPEN` is lazy: it happens when call 6 asks for
admission after the 1-second wait, not on a timer (set
`Config.auto_transition=True` for the eager variant). Calls 6 and 7 are the
two permitted probes; both succeed, so recording the second one closes the
circuit and starts a fresh window.

**Things to try:** set `gateway.healthy = False` before phase 4 and watch the
failed probe reopen the circuit; raise `permitted_calls_in_half_open`; put a
breakpoint in `PrintListener.on_state_change` and inspect
`breaker.snapshot()` at each transition.

## `two_clients.py` — isolation under a partial outage

One asyncio app talks to two dependencies. A [`Registry`](reference.md) hands
each its own breaker, so when `recommendations` goes down during rounds 3–6,
only its circuit opens — the app serves a cached fallback — while `payments`
keeps charging as if nothing happened.

??? example "two_clients.py — full source"

    ```python
    --8 < --'examples/two_clients.py'
    ```

The interesting part of the output:

```text
round 4:
  [listener] recommendations: state CLOSED -> OPEN
  payments        -> charged dave $25
  recommendations -> failed (recommendations service timed out) -> fallback: cached picks
round 5:
  [listener] recommendations: call rejected — circuit is open
  payments        -> charged erin $25
  recommendations -> rejected instantly -> fallback: cached picks
```

Round 4's failure is the second in a 4-call window — the `recommendations`
breaker trips. From round 5 on the difference matters: the round-4 request
*waited* for the dependency to fail, the round-5 request is rejected in
microseconds, so the user still gets their page (with cached picks) at full
speed. Notice what is absent: the `payments` breaker never logs a single
state change for the whole run.

```text
rounds 7-8 — the outage is over; the breaker probes and closes
round 7:
  [listener] recommendations: state OPEN -> HALF_OPEN
  payments        -> charged grace $25
  recommendations -> ['grace-pick-1', 'grace-pick-2']
round 8:
  [listener] recommendations: state HALF_OPEN -> CLOSED
  payments        -> charged heidi $25
  recommendations -> ['heidi-pick-1', 'heidi-pick-2']
```

Recovery is gradual by design: two successful probes (rounds 7 and 8) must
complete before the circuit closes again.

**Things to try:** extend the outage past round 7 and watch a failed probe
send the circuit straight back to `OPEN`; give the two breakers different
configs via `registry.get(name, config=...)`; replace the hand-rolled
fallback with a [tenacity retry](integrations/tenacity.md) that waits
exactly `retry_after`.

## `pipeline.py` — composition against a quiet death

The nastiest failure mode gets the [v2 pipeline](guides/pipeline.md)
treatment: the inventory service never raises — it *hangs*. On its own that
trips nothing and starves every caller. Three composed strategies turn it
into a non-event: a timeout makes hangs classifiable, the breaker counts
them, a fallback keeps serving.

??? example "pipeline.py — full source"

    ```python
    --8 < --'examples/pipeline.py'
    ```

The interesting part of the output:

```text
phase 2 — the service starts hanging; timeouts become failures
  [listener] inventory: fallback served instead of CallTimeoutError
request 3: ['widget (cached)', 'gadget (cached)'] in ~0.2s
  [listener] inventory: state CLOSED -> OPEN
  [listener] inventory: fallback served instead of CallTimeoutError
request 4: ['widget (cached)', 'gadget (cached)'] in ~0.2s

phase 3 — circuit OPEN: no timeout is burned, the cache is instant
  [listener] inventory: call rejected — circuit is open
  [listener] inventory: fallback served instead of CircuitOpenError
request 5: ['widget (cached)', 'gadget (cached)'] in ~0.0s
```

Watch the latency column. Requests 3–4 each pay the full 0.2 s timeout —
that is the *detection* cost while the window fills. Request 4 tips the
failure rate to 50% and the circuit opens; from request 5 on the rejection
is immediate and the cached snapshot costs ~0.0 s. The user never saw an
error: every response during the outage came from the fallback, and the
listener logged each substitution.

```text
phase 4 — the service recovers; probes close the circuit
  [listener] inventory: state OPEN -> HALF_OPEN
request 6: ['widget', 'gadget'] in ~0.0s
  [listener] inventory: state HALF_OPEN -> CLOSED
request 7: ['widget', 'gadget'] in ~0.0s
```

**Things to try:** raise `wait_duration_in_open` and watch how long the
cache serves; put a `.retry(...)` step between the fallback and the breaker
(needs `interlock-cb[tenacity]`) and see attempts in the listener via
`on_retry`; drop the `.timeout(...)` step and watch the outage become
invisible again — no strategy ever fires.

## Where to go next

These demos hand-roll their guarded clients so you can debug the mechanics.
In real code you usually don't have to: the [integrations](integrations/index.md)
apply the same per-dependency pattern to httpx2, aiohttp, requests and
FastAPI transparently, and the [resilience pipeline](guides/pipeline.md)
composes the breaker with timeout, bulkhead, retry and fallback declaratively.
