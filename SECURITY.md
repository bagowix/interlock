# Security Policy

## Supported versions

interlock follows semantic versioning. Security fixes target the latest released
minor of the current major.

| Version | Supported |
| ------- | --------- |
| 2.x     | ✅        |
| < 2.0   | ❌ (2.0 has no breaking changes — upgrading is a version bump) |

## Reporting a vulnerability

Please **do not** open a public issue for security problems.

Report privately through GitHub's
[private vulnerability reporting](https://github.com/bagowix/interlock/security/advisories/new),
or by email to **galushko355@gmail.com**. Include a description, affected
versions, and a reproduction if you have one.

You can expect an acknowledgement within a few days. Once a fix is ready we will
release it and credit the reporter unless you prefer to stay anonymous.

## Supply chain

The release path is auditable end to end, and the
[OpenSSF Scorecard](https://scorecard.dev/viewer/?uri=github.com/bagowix/interlock)
badge in the README is the summary of it.

- **Trusted publishing.** `.github/workflows/release.yml` builds and publishes
  through PyPI's OIDC trusted publisher — there is no long-lived API token that
  could leak.
- **Signed provenance.** Every artefact carries a Sigstore-backed
  [PEP 740](https://peps.python.org/pep-0740/) attestation generated at publish
  time. PyPI serves it from
  `https://pypi.org/integrity/interlock-cb/<version>/<filename>/provenance`.
- **Pinned actions.** Every `uses:` in `.github/workflows/` is pinned to a full
  commit SHA with the version in a trailing comment; Dependabot bumps both.
- **Audited workflows.** [zizmor](https://docs.zizmor.sh) runs over
  `.github/workflows/` on every pull request, with a token so that the audits
  which resolve a pinned SHA against the upstream repository (`impostor-commit`,
  `ref-confusion`, `known-vulnerable-actions`, `stale-action-refs`) are included.
  Suppressions live in `.github/zizmor.yml`, each with a reason.
- **Static analysis.** CodeQL default setup (`python` and `actions`) plus
  ruff's `S` ruleset (flake8-bandit), which is part of `select = ["ALL"]`.
- **Zero-dependency core.** `interlock.*` imports only the standard library;
  every third-party library sits behind an optional extra, so the default
  install has no transitive attack surface.
