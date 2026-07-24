# AGENTS.md

A modern circuit breaker for Python: sync + async in a single class, sliding-window rate,
slow-call detection, a type-safe API and transparent transport-level integrations.
This is a foundational library — people put it on the critical path of their services, so the
bar for simplicity, reliability and dependency hygiene is higher than usual.

## Decision-making principles

- Start from the problem: understand it first, pick tools second.
- Occam's razor: no new entities or abstractions without necessity.
- Never break existing functionality while making a change.
- A solution must be simple, reliable and readable without extra explanation.
- Minimise cognitive load — for the human and for the next agent.
- The upsides of a decision must heavily outweigh the downsides. When combining approaches, take
  the best of each.
- Write docstrings in English.

## Technology

- **Python 3.11+** — the minimum supported version (`asyncio.timeout`, `TaskGroup`, exception
  groups, `Self` are required). Development and CI run on 3.11–3.14. The floor is deliberately
  below the usual 3.12 default: a foundational library values reach.
- **Zero-dependency core** — stdlib only. A fault-tolerance tool must not depend on someone
  else's reliability. Everything external goes through optional extras
  (`interlock-cb[httpx2]`, `[aiohttp]`, `[requests]`, `[tenacity]`, `[fastapi]`, `[litestar]`, `[redis]`, `[otel]`).
- **Pydantic ≥ 2.0 — extras only, NEVER in the core.** The core config is a frozen dataclass with
  eager validation. Pydantic is acceptable only where it is optional.
- **uv** — package manager.
- **hatchling** — build backend. The version is **static** in `interlock/version.py`
  (`[tool.hatch.version] path`), bumped manually on release and exposed as `interlock.__version__`.
- **ruff** — formatting and linting.
- **mypy + pyright** in strict mode — static analysis.
- **pytest** + `pytest-asyncio`, `pytest-cov`, `pytest-mock`, `pytest-sugar`, `faker`, `hypothesis`.
- **Zensical** — documentation (plain-Markdown content, portable).

## Environment and commands

- Virtual environment: `uv venv --python 3.12 .venv`
- Run commands: `uv run <command>`
- Dependencies: `uv add <package>` / dev: `uv add --dev <package>`
- Format: `uv run ruff format`
- Lint: `uv run ruff check --fix`
- Types: `uv run mypy` and `uv run pyright`
- Tests: `uv run pytest` / with coverage: `uv run pytest --cov`

ruff is configured in `pyproject.toml` (`line-length = 100`, `target-version = "py311"`), not via
CLI flags.

## Architectural rules

- **The core is an I/O-free state machine.** State, window, thresholds and transitions do no I/O
  and know nothing about sync vs async. Around the await-free critical section sits a single
  `threading.Lock` (correct for both threads and the event loop); the lock is never held across
  the protected call.
- **Extension points are `Protocol`s, not inheritance of internal classes:**
  `Clock`, `SlidingWindow`, `Storage`, `FailureClassifier`, `EventListener`.
- **One public `CircuitBreaker` class for sync and async.** No `Sync*`/`Async*` twins. The class
  detects a coroutine function and dispatches to the right path.
- **Time only through the injected `Clock`.** No direct `time.monotonic()` / `sleep` in logic:
  it breaks test determinism.
- **Group by feature, not by kind** (no `models/`, `services/`, `utils/`). Each concept — window,
  state machine, classifier — gets its own module.
- **The public API goes through the package `__init__.py`.** Helpers are underscore-prefixed and
  hidden.
- **Encapsulate external dependencies** behind wrappers; extras never leak into the core.

## Code style (Python)

### Formatting

- Maximum line length: 100 characters.
- Single quotes for strings.
- f-strings instead of `.format()` and `%`.
- `pathlib.Path` instead of `os.path`.
- Context managers instead of `try/finally`.

### Imports

- Always at the top of the file.
- Absolute imports only.
- Group order: stdlib → third-party → local (blank line between groups).

### Typing

- Annotations on every parameter and return value.
- Modern syntax: `list[str]`, `dict[str, int]`, `str | None`.
- Never `Optional[X]` — only `X | None`.
- No magic constants — use `StrEnum` or module-level constants.

### Calls and constructors

With 3+ arguments, keyword arguments only.

```python
# bad
breaker = CircuitBreaker('payments', 5, 60, True)

# good
breaker = CircuitBreaker(name='payments', failure_threshold=5, reset_timeout=60, half_open=True)
```

### Functions

- One function — one job.
- 20–30 lines at most.
- Minimal side effects; prefer pure functions over stateful ones.
- Repeated code in a loop gets extracted into a variable or function.

## Async code

- `async/await` is the standard for I/O-bound work.
- `asyncio.TaskGroup` instead of `asyncio.gather` (3.11+).
- Do not mix sync and async in one function: if a function calls async code, it is async itself.
  This does not contradict the unified `CircuitBreaker`: it has **separate** internal sync/async
  paths selected by the detector. We never await a sync callable and never block on an async one.
- CPU-bound work goes through `asyncio.to_thread` or `ProcessPoolExecutor`.

## Error handling

- **Fail fast**: on invalid input or state — `raise` immediately, never continue with a partial
  result.
- Custom exception classes with informative messages and context (e.g. `CircuitOpenError` carries
  the breaker name, an estimate for the next probe, the last failure).
- Catch only expected exceptions, log with context, re-raise.

```python
# bad — the error is hidden behind a fallback
def get_state(name: str) -> State:
    try:
        return storage.load(name)
    except Exception:
        return State.CLOSED


# good — fail fast, transparent error
def get_state(name: str) -> State:
    if not name:
        raise ValueError(f'Empty breaker name: {name!r}')
    return storage.load(name)
```

## Tests

- pytest, functions only (no test classes).
- Naming: `test__unit_of_work__state_under_test__expected_behavior` (lower case).
  Example: `test__sliding_window__failure_rate_above_threshold__opens_circuit`.
- Test files mirror the package layout: `interlock/window.py` → `tests/test_window.py`.
- One test — one behaviour. Arrange-Act-Assert structure.
- Fixtures for repeated setup. `pytest-mock` to isolate external dependencies.
- `pytest-asyncio` for async (`@pytest.mark.asyncio`). `faker` for test data.
- **Deterministic time via the injected `Clock`** — no `sleep` in tests.
- **Property-based tests (`hypothesis`) for the state machine**; 100% coverage of all transitions
  and races.

```python
def test__sliding_window__failure_rate_above_threshold__opens_circuit(
    fake_clock: Clock,
) -> None:
    window = CountBasedWindow(size=10, clock=fake_clock)
    for _ in range(6):
        window.record(Outcome.FAILURE)

    assert window.failure_rate() >= 0.5
```

## Documentation and naming

- Expressive names instead of comments.
- Docstrings for the public API: what it does, arguments, return value, raised exceptions.
- Comments only for non-obvious logic, trade-offs and limitations.
- Visible deprecations via `InterlockDeprecationWarning(UserWarning)`.

```python
# bad — states the obvious
# open the circuit
self._state = State.OPEN

# good — explains what is not obvious
# In HALF_OPEN admit at most N probes at a time, otherwise the whole concurrent load
# rushes in as probes and knocks over the barely recovered dependency.
```

## Hard rules (violation = bug)

- **No fallback values**: never invent defaults to paper over missing data.
- **No silent exceptions**: catch only what is expected, log with context, re-raise.
- **No default chains in business logic**: `a or b or c` is for UI labels only, never for
  required config or data.
- **No hidden retries**: retries are acceptable only when explicitly requested, idempotent,
  aimed at transient errors, bounded in attempts and logged.
- **Fail fast**: on invalid input or state — `raise`.

> Specific to this project: retry / fallback / timeout are library **features** (v1–v2), but they
> must be exactly what the rules above demand — explicit, bounded, observable. No hidden
> "it will retry / substitute something by itself" magic inside the breaker.
