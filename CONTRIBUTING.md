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

CI runs exactly these on Python 3.11–3.14. Run them locally before opening a PR:

```bash
uv run ruff format --check    # formatting
uv run ruff check             # linting
uv run mypy                   # type checking
uv run pyright                # type checking (strict)
uv run pytest --cov           # tests with coverage
```

The pre-commit hooks run the fast subset automatically:

```bash
uv run prek install           # one-time, installs the git hook
```

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

## Expectations

- **Tests first.** New behaviour and bug fixes come with tests; the suite keeps
  100% coverage. Time-dependent logic must be tested through an injected
  `Clock`, never `sleep`.
- **Types are part of the API.** Public surface stays fully typed; mypy and
  pyright must pass in strict mode.
- **Keep the core dependency-free.** Anything external belongs in an extra
  (`interlock-cb[otel]`, `interlock-cb[httpx2]`, `interlock-cb[redis]`), imported
  lazily.
- **Conventional commits.** Use `feat:`, `fix:`, `docs:`, `refactor:`, `test:`,
  `chore:`, `perf:`, `ci:` prefixes.
- **Update the docs and CHANGELOG.** User-facing changes update the relevant
  page under `docs/` and the `[Unreleased]` section of `CHANGELOG.md`.

## Proposing larger changes

For anything beyond a small fix, open an issue first so we can agree on the
approach before you invest time. interlock is deliberately scoped (see the
roadmap in `planning/PLAN.md`); features outside the v1 core may be a better fit
for a later milestone.

## Code of conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By
participating you agree to uphold it.
