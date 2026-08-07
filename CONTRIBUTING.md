# Contributing to interlock

Thanks for your interest in improving interlock. This guide covers the local
setup and the checks your change must pass.

## Development setup

interlock uses [uv](https://docs.astral.sh/uv/) for environment and dependency
management. With uv installed:

```bash
git clone https://github.com/bagowix/interlock
cd interlock
uv sync            # creates the venv and installs dev + all extras
```

## Running the checks

CI runs exactly these on Python 3.11–3.14 and on the free-threaded 3.14t. Run
them locally before opening a PR:

```bash
uv run ruff format --check    # formatting
uv run ruff check             # linting
uv run mypy                   # type checking
uv run pyright                # type checking (strict)
uv run pyrefly check          # type checking (strict, third opinion)
uv run pytest --cov           # tests with coverage
uv run zizmor .github/workflows/   # GitHub Actions audit
uv run zensical build --strict # docs, including broken links
```

`zizmor` needs a GitHub token for four of its audits (`impostor-commit`,
`ref-confusion`, `known-vulnerable-actions`, `stale-action-refs`) — the ones
that check what a pinned SHA actually points at. CI passes the job's built-in
token; locally, export `GH_TOKEN=$(gh auth token)` to run the same set. Without
one zizmor warns and runs the offline subset, which is enough for everything
except a bad pin.

Every action in `.github/workflows/` is pinned to a commit SHA with the version
in a trailing comment. Adding one? Write the tag and let zizmor rewrite it —
`GH_TOKEN=$(gh auth token) uv run zizmor --fix=all .github/workflows/` resolves
the SHA and the comment for you. Routine bumps come from Dependabot.

The pre-commit hooks run the fast subset automatically:

```bash
uv run prek install           # one-time, installs the git hook
```

### Supported platforms

interlock supports Python 3.11–3.14 on Linux, Windows and macOS. The full test,
coverage, static-analysis and Redis matrix runs on Ubuntu. A smaller runtime matrix
runs on Windows and macOS with the oldest and newest supported Python versions. It
covers the core breaker, concurrency and shutdown paths, timeouts, the resilience
pipeline, state-machine invariants, the runtime typing surface and loopback HTTP
integrations.

Redis tests are the only platform-smoke exclusion tied to an operating-system
constraint: GitHub-hosted Windows and macOS runners do not support service
containers. They remain covered against a real Redis server in every Python version
of the Ubuntu matrix. Ruff and the static type checkers also stay in that full matrix
because their results are platform-independent.

### Public API compatibility

`.github/workflows/api-compatibility.yml` runs [`griffe check`](https://mkdocstrings.github.io/griffe/)
on every pull request, diffing the public API of the working tree against the
latest release tag — removed or renamed objects, changed parameter kinds,
order or defaults, narrowed return types. It covers everything importable from
`interlock/__init__.py` plus `interlock/integrations/*`, which is public even
though it is not re-exported from `__init__`. Run it locally with:

```bash
uv run griffe check interlock --search .
```

A breaking change is not automatically wrong — it must be deliberate, though,
and reflected in `CHANGELOG.md`. If it is intentional, label the pull request
`breaking-change`; the job still reports the diff but no longer fails on it.

### Redis-backed tests

`tests/test_redis_storage.py` runs against in-process **`fakeredis`** by default —
no server needed, coverage stays 100%. A few concurrency tests assert atomicity
only a real server guarantees; they are skipped unless you point at one (Redis or
any RESP server such as [Valkey](https://valkey.io)):

```bash
docker run --rm -p 6379:6379 redis    # or valkey/valkey, or a local redis-server
INTERLOCK_TEST_REDIS_URL=redis://localhost:6379/0 uv run pytest
```

CI runs both.

### Free-threaded CPython

The breaker's thread-safety rests on a single `threading.Lock`, and a GIL-enabled
interpreter cannot falsify that claim — so CI runs the whole suite on the
free-threaded build (`3.14t`) as well. `tests/test_concurrency.py` is the part
that matters there: it drives one breaker from many threads and asserts what the
lock exists to provide (window counts add up, no torn snapshot, HALF_OPEN probe
caps hold, one name means one breaker). It runs on every interpreter, but only
on `3.14t` does failing it become likely.

Use a second environment so your main `.venv` is left alone:

```bash
UV_PROJECT_ENVIRONMENT=.venv-ft uv sync --python 3.14t
UV_PROJECT_ENVIRONMENT=.venv-ft uv run pytest
```

One caveat: `tests/test_redis_storage.py` needs a **real** server on `3.14t`.
Its `fakeredis` fallback pulls in `lupa`, whose Lua engine has not declared
free-threading support, so importing it re-enables the GIL — and
`filterwarnings = "error"` turns that warning into a collection error. Set
`INTERLOCK_TEST_REDIS_URL` (CI does) and `fakeredis` is never imported.

### Mutation testing

The 100% coverage gate proves every line and branch runs; it does not prove any
assertion would notice if the behaviour changed. `mutmut` closes that gap for
the two modules where a surviving mutant is a real bug — `interlock/_state_machine.py`
(thresholds, transitions, probe admission) and `interlock/_engine.py` (lock scope,
dispatch, recording order):

```bash
uv run mutmut run                 # ~1 min; writes mutants/ (gitignored)
uv run mutmut results             # what survived
uv run mutmut show <mutant-name>  # the diff that survived
```

It is deliberately out of band: `.github/workflows/mutation.yml` runs it weekly
and on demand, never as a pull-request gate. The score is measured against the
deterministic suite only — the hypothesis suites are excluded, because a test
that reaches a branch only sometimes makes both mutmut's test-selection mapping
and the score itself irreproducible.

Baseline: **526 of 549 mutants killed (95.8%)**. Every survivor is an equivalent
mutant, in one of five classes:

| Class | Why it cannot be killed |
|---|---|
| `_generation += 2`, and the initial `_generation = 1` | The era counter is only ever compared with `!=`. Any strictly increasing sequence behaves identically — what matters is that a value is never reused, which `test__generation__never_repeats_across_transitions` does check. |
| `_opened_at = None` / `= 1.0` in `StateMachine.__init__` | Read only in `OPEN`, and `_open()` always writes it first. |
| `_closed = None`, `_storage_degraded = None` | Used only as a boolean; `None` is as falsy as `False`. |
| Dropping `retry_after=None` from `CircuitOpenError(...)` | The keyword repeats the constructor default. |
| `_emit_transitions(..., after=None)` on paths that never enter `OPEN`, and the `_detach_shared_view` / storage-callback variants | The timer branch only reacts to `OPEN`; on these paths both arguments are equal, or the timer is already shut down. |

If a run turns up a survivor outside those classes, it is a missing test — write
it rather than raising `SURVIVOR_BUDGET` in the workflow.

### Benchmarks

`benchmarks/` holds the performance suite, measured by
[CodSpeed](https://codspeed.io). It lives outside `testpaths`, so `uv run pytest`
never collects it; run it explicitly. `-n 0` opts out of the xdist default —
measurement needs a single process:

```bash
uv run pytest benchmarks -n 0                                   # smoke run
codspeed run --mode simulation -- uv run pytest benchmarks --codspeed -n 0
```

Every pull request runs the same suite through
`.github/workflows/codspeed.yml`, which reports the difference against `main`.
Add a benchmark when you touch a hot path: the call paths, the state machine,
the sliding windows or the pipeline.

`test_baseline_direct_call` (and its async twin) measure the unwrapped
callable — divide any call-path result by the matching baseline to read the
breaker's overhead as a ratio rather than an absolute count. The contention
benchmark (`benchmarks/test_contention.py`) carries a caveat: CodSpeed counts
instructions under Valgrind, which runs threads one at a time, so it tracks
the work done under the breaker's lock when many threads call through it — not
the wall-clock cost of real contention.

## Expectations

- **Tests first.** New behaviour and bug fixes come with tests; the suite keeps
  100% coverage. Time-dependent logic must be tested through an injected
  `Clock`, never `sleep`.
- **Types are part of the API.** Public surface stays fully typed; mypy and
  pyright must pass in strict mode.
- **Keep the core dependency-free.** Anything external belongs in an extra
  (e.g. `interlock-cb[httpx2]`, `interlock-cb[tenacity]`, `interlock-cb[redis]`),
  imported lazily.
- **An extra's floor is a compatibility statement, not a dev pin.** It says the
  oldest host-library version the integration works on, and only an API the
  code actually needs justifies raising it. A routine Dependabot bump lands on
  the `dependency-groups.dev` pin (what we develop against) — never let it
  carry the `[project.optional-dependencies]` floor along with it. Raising a
  floor on purpose means updating the matching pin in the `extras-min` job in
  `.github/workflows/ci.yml` too.
- **Conventional commits.** Use `feat:`, `fix:`, `docs:`, `refactor:`, `test:`,
  `chore:`, `perf:`, `ci:` prefixes.
- **Update the docs and CHANGELOG.** User-facing changes update the relevant
  page under `docs/` and the `[Unreleased]` section of `CHANGELOG.md`.

## Proposing larger changes

For anything beyond a small fix, open an issue first so we can agree on the
approach before you invest time. interlock is deliberately scoped: a circuit
breaker core, the composable
[resilience pipeline](https://bagowix.github.io/interlock/guides/pipeline/)
and thin [integrations](https://bagowix.github.io/interlock/integrations/) on
native extension points, prioritised by demand. Retry stays delegated to
tenacity; caching and own backoff engines are out of scope.

## Code of conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By
participating you agree to uphold it.
