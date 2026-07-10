# Runnable examples

Self-contained demos — standard library plus `interlock-cb` only, no network,
no services to stand up. Each run prints the same deterministic output, so
they are ideal for stepping through with a debugger.

```bash
pip install interlock-cb   # or: uv add interlock-cb

python examples/lifecycle.py    # one breaker: CLOSED -> OPEN -> HALF_OPEN -> CLOSED
python examples/two_clients.py  # two guarded clients: one fails, the other keeps serving
python examples/pipeline.py     # timeout + breaker + fallback around a hanging service
```

| Script | What it shows |
|---|---|
| [`lifecycle.py`](lifecycle.py) | The full state cycle of a single breaker around a flaky gateway: failures fill the window, the circuit trips, calls fail fast with `retry_after`, probes close it again. Every event is narrated by an `EventListener`. |
| [`two_clients.py`](two_clients.py) | A `Registry` giving two dependencies independent breakers inside one asyncio loop: the `recommendations` outage trips only its own circuit (the app falls back to a cache), while `payments` keeps charging. |
| [`pipeline.py`](pipeline.py) | The v2 pipeline in one story: a dependency that *hangs* instead of erroring — the timeout turns hangs into failures, the breaker opens, the fallback serves a cached snapshot instantly, probes close the circuit. |

The expected output is walked through line by line in the
[demo page of the documentation](https://bagowix.github.io/interlock/demo/).
