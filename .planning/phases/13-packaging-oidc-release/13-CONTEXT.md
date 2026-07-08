# Phase 13: Packaging & OIDC Release - Context

**Gathered:** 2026-07-09
**Status:** Ready for planning
**Mode:** Auto-generated (autonomous — low-ambiguity hardening phase; changes already shipped)

<domain>
## Phase Boundary

Lock the distribution identity and release path:
- PKG-01: the distribution ships as `kafka-events-mcp` with consistent identity —
  `pyproject.toml [project].name`, the README `pip install` line, and CHANGELOG
  agree, and `hatch build` produces `kafka_events_mcp-*` wheel + sdist.
- PKG-02: releases publish to PyPI via OIDC Trusted Publishing — the release
  workflow requests `id-token: write`, references NO stored PyPI/TestPyPI token
  secrets, and the publish path (build → TestPyPI → gated PyPI) is verified.

Out of scope: actually publishing to PyPI (human-gated live push); changing the
tool surface or runtime behavior.
</domain>

<decisions>
## Implementation Decisions

### Already shipped (verify, do NOT redo)
- `pyproject.toml [project].name = "kafka-events-mcp"`, version `0.2.0`.
- README install line: `pip install kafka-events-mcp`.
- CHANGELOG `[0.2.0]` documents the rename + OIDC publishing.
- `.github/workflows/release.yml` uses `pypa/gh-action-pypi-publish` via OIDC
  (`permissions: id-token: write`, no `password:`/`secrets.*_TOKEN`), gated
  `environment: pypi`, tag-triggered.

### Claude's discretion
- PKG-01: a test that parses `pyproject.toml` and asserts name == kafka-events-mcp,
  that the README install line matches the distribution name, and that the
  CHANGELOG references the current version. Optionally a build smoke check that
  `hatch build` (or `python -m build`) yields `kafka_events_mcp-*` artifacts —
  keep it env-gated / skippable if the build backend isn't available in CI.
- PKG-02: a test that parses `.github/workflows/release.yml` and asserts the
  publish jobs request `id-token: write` and reference NO stored token secrets
  (`secrets.PYPI_TOKEN` / `secrets.TEST_PYPI_TOKEN` / `TWINE_PASSWORD`), i.e. the
  OIDC Trusted Publishing contract holds and can't silently regress to tokens.
</decisions>

<code_context>
## Existing Code Insights

- `pyproject.toml` (hatchling backend), `README.md`, `CHANGELOG.md`,
  `.github/workflows/release.yml` are the artifacts under test.
- Build produces `kafka_events_mcp-0.2.0-py3-none-any.whl` + `.tar.gz`
  (verified locally this session).
- The OIDC migration and rename were committed this session (0.2.0 release prep).
</code_context>

<specifics>
## Specific Ideas

- PKG-01 test: `tomllib.load(open("pyproject.toml","rb"))` → name/version;
  grep README for `pip install kafka-events-mcp`; grep CHANGELOG for the version.
- PKG-02 test: parse release.yml YAML; for the publish job(s) assert
  `permissions.id-token == write` and no token-secret references anywhere in the
  file (regex/string scan for `PYPI_TOKEN`, `TEST_PYPI_TOKEN`, `TWINE_PASSWORD`).
- Keep any actual `hatch build` invocation env-gated so CI without the backend
  stays green.
</specifics>

<deferred>
## Deferred Ideas

- Published-package smoke test — install `kafka-events-mcp` from PyPI in a clean
  env (v1.3 Future; needs a real publish first).
</deferred>
