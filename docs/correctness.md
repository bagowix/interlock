# Correctness and testing

interlock-cb is young — [Comparison](comparison.md) says so plainly. What it
can offer instead of years in production is a bar most libraries in this space
do not hold themselves to, and a way to check that the bar is real rather than
promised. Every claim below links to the config, workflow or test file that
enforces it, so it can be verified against the repository at any commit, not
just taken on trust.

## Types

- **Three independent checkers, strict mode**: [mypy](https://mypy-lang.org/)
  (`strict = true`), [pyright](https://microsoft.github.io/pyright/)
  (`typeCheckingMode = "strict"`) and
  [pyrefly](https://pyrefly.org/) (`preset = "strict"`) all run over
  `interlock/` and `tests/typing_surface.py` on every pull request
  ([`pyproject.toml`](https://github.com/bagowix/interlock/blob/main/pyproject.toml),
  [`ci.yml`](https://github.com/bagowix/interlock/blob/main/.github/workflows/ci.yml)).
  Three implementations rather than one because they disagree often enough for
  it to matter — each catches inference gaps the others miss.
- **The public surface is asserted, not just checked.**
  [`tests/typing_surface.py`](https://github.com/bagowix/interlock/blob/main/tests/typing_surface.py)
  uses `assert_type` on `call()`'s overloads and the decorator's signature
  preservation. It is never executed; it is included in the mypy/pyright/pyrefly
  scope, so a regression in what a type checker *infers* at a call site fails
  CI even though nothing here runs at test time.
- **Public API breakage is diffed, not just typed.**
  [`api-compatibility.yml`](https://github.com/bagowix/interlock/blob/main/.github/workflows/api-compatibility.yml)
  runs [`griffe check`](https://mkdocstrings.github.io/griffe/) against the
  latest release tag on every pull request — covering `interlock/__init__.py`'s
  re-exports and `interlock/integrations/*`, which is public without being
  re-exported. A detected breakage fails the build unless the PR carries the
  `breaking-change` label.
- Ships `py.typed`; `ParamSpec` + `TypeVar` decorators preserve both the
  wrapped signature and its sync/async nature.

## Coverage

- **100% branch coverage, enforced, not aspirational**:
  `fail_under = 100` under `[tool.coverage.report]`
  ([`pyproject.toml`](https://github.com/bagowix/interlock/blob/main/pyproject.toml)),
  checked with `branch = true` so a branch taken only one way still fails the
  gate. Reported through [Codecov](https://codecov.io/gh/bagowix/interlock)
  (project **and** patch targets at 100% —
  [`codecov.yml`](https://github.com/bagowix/interlock/blob/main/.github/codecov.yml)).
- **What coverage doesn't prove** is exactly why the next four sections exist —
  see [Mutation testing](#mutation-testing) below.

## Property-based and model-based tests

Coverage proves every line ran; it says nothing about whether a test would
notice if the logic were wrong. Two Hypothesis suites target the state machine
specifically, both driven by an injected `FakeClock`
([`tests/conftest.py`](https://github.com/bagowix/interlock/blob/main/tests/conftest.py)),
never real time:

- [`test_state_machine_properties.py`](https://github.com/bagowix/interlock/blob/main/tests/test_state_machine_properties.py) —
  `@given` properties over hand-written sequences, checking four invariants:
  the minimum-calls gate, a saturated window, the `OPEN` wait, and the probe
  caps.
- [`test_state_machine_model.py`](https://github.com/bagowix/interlock/blob/main/tests/test_state_machine_model.py) —
  a Hypothesis `RuleBasedStateMachine` that generates the *sequence itself*:
  interleaved outcomes, clock advances, admissions and operator overrides,
  checked against an independently predicted state, generation, window and
  probe budget after every step. This is the suite that catches
  order-dependent bugs — an override landing mid-probe-round, a probe settling
  a generation late — that a fixed sequence cannot reach.

When the model finds a counterexample, the shrunk sequence gets pinned as a
named regression test next to the model, so the reproducer survives the model
being changed later.

## Mutation testing

100% branch coverage proves every line executes; it does not prove an
assertion would catch a wrong value. [`mutmut`](https://mutmut.readthedocs.io/)
closes that gap for the two modules where a surviving mutant is a real bug:
[`interlock/_state_machine.py`](https://github.com/bagowix/interlock/blob/main/interlock/_state_machine.py)
(threshold arithmetic, transition ordering, probe admission) and
[`interlock/_engine.py`](https://github.com/bagowix/interlock/blob/main/interlock/_engine.py)
(lock scope, dispatch, recording order) —
[`[tool.mutmut]`](https://github.com/bagowix/interlock/blob/main/pyproject.toml)
in `pyproject.toml`.

Baseline: **526 of 549 mutants killed (95.8%)**. Every survivor is an
equivalent mutant in one of five enumerated classes — an unread sentinel
default, a comparison that only ever uses `!=`, a value used solely as a
boolean — documented with the reasoning in
[`CONTRIBUTING.md`](https://github.com/bagowix/interlock/blob/main/CONTRIBUTING.md#mutation-testing).
A survivor outside those classes is treated as a missing test, not as grounds
to raise the budget.

Run weekly and on demand
([`mutation.yml`](https://github.com/bagowix/interlock/blob/main/.github/workflows/mutation.yml)),
never as a pull-request gate — the signal is a slowly-moving score, not
something that should block an unrelated change. The hypothesis suites are
excluded from the run: `mutmut` maps tests to functions from a single stats
pass, and a randomised test that reaches a branch only sometimes would make
both that mapping and the score irreproducible.

## An I/O-free state machine, driven by an injected clock

[`interlock/_state_machine.py`](https://github.com/bagowix/interlock/blob/main/interlock/_state_machine.py)
never touches a socket, a file, or `time.time()` — all time comes from a
`Clock` passed in at construction. Every test in the suite that exercises
timed transitions does so with `FakeClock.advance(seconds)`
([`tests/conftest.py`](https://github.com/bagowix/interlock/blob/main/tests/conftest.py)),
never `sleep`; `test_examples.py` is the one deliberate exception, since it
runs the `examples/` scripts themselves as a subprocess smoke test. This is
what makes the property and model-based suites above practical at all — a
`RuleBasedStateMachine` generating thousands of transition sequences would be
too slow to run against a real clock.

## Concurrency, including free-threaded CPython

- The `threading.Lock` in
  [`interlock/_engine.py`](https://github.com/bagowix/interlock/blob/main/interlock/_engine.py)
  covers only the two await-free critical sections (admission and recording);
  the protected callable itself runs outside it.
- [`tests/test_concurrency.py`](https://github.com/bagowix/interlock/blob/main/tests/test_concurrency.py)
  drives one breaker from many real threads at once and checks window counts
  add up, `snapshot()` is never torn, the `HALF_OPEN` caps hold, and a
  concurrent trip emits exactly one `CLOSED → OPEN` event.
- **CI runs the full matrix on `3.14t`, the free-threaded build, as a required
  job** — Python 3.11, 3.12, 3.13, 3.14 and 3.14t, `fail-fast: false`
  ([`ci.yml`](https://github.com/bagowix/interlock/blob/main/.github/workflows/ci.yml)).
  A GIL-enabled interpreter cannot falsify a thread-safety claim resting on one
  lock: with `Engine._lock` removed, `test_concurrency.py` fails on 3.14t and
  passes everywhere else. This is the reason the job exists rather than being
  aspirational — it is the only Python build in the matrix that can actually
  disprove the claim.

## Storage contract and coordinated (distributed) breakers

- [`tests/test_storage_contract.py`](https://github.com/bagowix/interlock/blob/main/tests/test_storage_contract.py)
  runs a single behavioural contract suite against the in-memory reference
  storage
  ([`tests/inmemory_storage.py`](https://github.com/bagowix/interlock/blob/main/tests/inmemory_storage.py)) —
  the same contract every `Storage` / `AsyncStorage` implementation, including
  Redis, is expected to satisfy.
- [`tests/test_coordination.py`](https://github.com/bagowix/interlock/blob/main/tests/test_coordination.py)
  covers trip propagation, the global probe budget, coordinated close, and
  degradation-and-recovery across a shared storage — fully deterministic, on a
  shared `FakeClock` with a manually driven `poll_once()`.
- [`tests/test_redis_storage.py`](https://github.com/bagowix/interlock/blob/main/tests/test_redis_storage.py)
  runs against in-process `fakeredis` by default (so `uv run pytest` needs no
  server) and against a real Redis service container in CI, which is the
  authoritative check for Lua-script atomicity under concurrency. `3.14t`
  specifically requires the real server: `fakeredis`'s Lua engine (`lupa`)
  re-enables the GIL on import, which `filterwarnings = "error"` turns into a
  collection failure.

## Lower-bound dependency versions are tested, not guessed

Every optional extra declares a minimum version in `pyproject.toml`
(`httpx2>=2.4.0`, `redis>=5.0.0`, and so on). The `extras-min` job re-pins each
one to exactly that floor and runs the integration suite against it
([`ci.yml`](https://github.com/bagowix/interlock/blob/main/.github/workflows/ci.yml)) —
so a lower bound is a tested claim, not an untested guess about how far back
compatibility actually reaches. The latest versions of each extra are covered
separately by the main matrix.

## Warnings are errors

`filterwarnings = ["error"]`
([`pyproject.toml`](https://github.com/bagowix/interlock/blob/main/pyproject.toml))
turns every `DeprecationWarning` and `RuntimeWarning` raised during a test run
into a failure, with one narrow, commented exception for a third-party import
warning on CPython 3.11. This is what makes the `3.14t` / `fakeredis` /
`lupa` interaction above a hard collection failure instead of a silent GIL
re-enable, and it means an internal deprecation (`InterlockDeprecationWarning`)
must be explicitly asserted in the test that triggers it, never silenced.

## Linting

`ruff` runs with `select = ["ALL"]`
([`pyproject.toml`](https://github.com/bagowix/interlock/blob/main/pyproject.toml)),
including the `S` (flake8-bandit) security ruleset and `flake8-tidy-imports`
bans on legacy `typing` aliases. Ignored rules are listed individually in
`pyproject.toml`, each implicitly scoped to what it actually silences, not
disabled wholesale.

## Hot-path performance

[CodSpeed](https://codspeed.io) measures CPU instructions (not wall-clock
time, so results stay stable on shared CI runners) over the call paths, the
state machine, the sliding windows and the pipeline on every pull request
([`codspeed.yml`](https://github.com/bagowix/interlock/blob/main/.github/workflows/codspeed.yml)).
A regression is a signal, not silence — this is a performance floor, not a
correctness one, and is listed here for completeness rather than as a
correctness claim.

## Supply chain

Correctness of the code is only half the trust question; the other half is
whether what gets published is what was reviewed. See
[`SECURITY.md`](https://github.com/bagowix/interlock/blob/main/SECURITY.md#supply-chain)
for the full breakdown — trusted publishing with no long-lived PyPI token,
Sigstore-backed [PEP 740](https://peps.python.org/pep-0740/) provenance on
every release artifact, every GitHub Actions `uses:` pinned to a full commit
SHA and audited by [zizmor](https://docs.zizmor.sh) on every pull request, and
CodeQL default setup over both Python and the workflows themselves. The
[OpenSSF Scorecard](https://scorecard.dev/viewer/?uri=github.com/bagowix/interlock)
badge in the README is the standing summary of this section, refreshed weekly
and on every push to `main`. The project also holds the
[OpenSSF Best Practices](https://www.bestpractices.dev/projects/13932) badge —
the self-certified checklist covering release process, vulnerability
reporting, static/dynamic analysis and secure-development practices, most of
it satisfied by the mechanisms this page already documents.

## Known limits

An honest boundary is more persuasive than a claimed clean sweep:

- **Mutation testing covers two modules.** `_state_machine.py` and `_engine.py`
  are where a surviving mutant is unambiguously a bug; the rest of the package
  (integrations, the pipeline strategies, the registry) is covered by branch
  coverage and example-based tests only, not mutation testing.
- **Mutation testing excludes the Hypothesis suites**, for reproducibility —
  see [Mutation testing](#mutation-testing) above. A branch reached only by a
  property or model-based test can show up as a mutation survivor even when a
  property test does in fact cover it.
- **Free-threaded coverage is one test file's job.** `test_concurrency.py` is
  the suite specifically designed to fail without the engine's lock; the rest
  of the suite runs on 3.14t too, but is not designed to detect races the way
  that file is.
- **No production track record.** interlock-cb was first released in 2026 —
  see [Comparison](comparison.md). None of the above substitutes for years of
  real-world traffic; it is what can be verified today in place of that
  history.
- **Fuzzing is not part of this suite.** The OpenSSF Scorecard `Fuzzing` check
  is expected to stay red — the input surface here is typed function calls and
  config values, not a parser or protocol decoder, so structure-aware fuzzing
  does not apply the way it would to a format library.
- **Branch protection is not machine-verifiable from the public Scorecard
  run.** The `Branch-Protection` check needs a PAT with read access to
  repository settings that the workflow does not have; the settings themselves
  are configured in GitHub, not in a file this page can link to.
