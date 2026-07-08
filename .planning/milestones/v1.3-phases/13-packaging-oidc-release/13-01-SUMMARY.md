---
plan: 13-01
phase: 13
completed: 2026-07-09
tasks: 2/2
requirements_completed: [PKG-01, PKG-02]
files_modified: [tests/test_packaging.py]
---

# Plan 13-01 Summary — Packaging & OIDC Release

Test-only hardening. The `kafka-events-mcp` rename and the OIDC release workflow
already shipped this session; this plan adds regression locks. No shipped
artifacts (pyproject/README/CHANGELOG/release.yml) were edited.

> Execution note: the original executor subagent died on a mid-response API error
> before writing the file; the orchestrator implemented the plan directly per the
> PLAN.md and verified it green.

## Tasks

- **Task 1 — PKG-01** (`tests/test_packaging.py::TestDistributionIdentity`): parses
  `pyproject.toml` (tomllib, tomli/skip fallback) → name == `kafka-events-mcp`;
  cross-checks the README `pip install kafka-events-mcp` line and the CHANGELOG
  `[0.2.0]` heading; an opt-in (`RUN_BUILD_SMOKE=1`) hatch build smoke asserts
  `kafka_events_mcp-*` wheel+sdist. 3 passed + 1 skipped (build smoke).
- **Task 2 — PKG-02** (`tests/test_packaging.py::TestReleaseWorkflowOidc`):
  `yaml.safe_load`s `.github/workflows/release.yml`; asserts every
  `pypa/gh-action-pypi-publish` job sets `permissions.id-token: write`, that the
  parsed workflow contains no stored token secret (`PYPI_TOKEN`/`TEST_PYPI_TOKEN`/
  `TWINE_PASSWORD` — scanned over parsed values, so comments don't false-positive),
  and that no publish step passes a `password:` input. 4 passed.

## Verification

- `uv run python -m pytest tests/test_packaging.py` → 7 passed, 1 skipped.
- `RUN_BUILD_SMOKE=1 ... -k build_produces` → 1 passed (real `kafka_events_mcp-*`).
- Full default suite → **355 passed, 2 skipped, 27 deselected**.
- `git diff --name-only` outside tests/ → empty (no shipped-artifact edits).

Commit: `test(13-01): lock packaging identity (PKG-01) and OIDC release contract (PKG-02)`.
