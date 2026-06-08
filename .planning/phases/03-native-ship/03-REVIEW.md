---
phase: 03-native-ship
reviewed: 2026-06-08T00:00:00Z
depth: standard
files_reviewed: 8
files_reviewed_list:
  - src/kafka_mcp/scanner.py
  - tests/test_scanner.py
  - tests/benchmarks/test_scan_benchmark.py
  - tests/test_inbound.py
  - .github/workflows/ci.yml
  - glama.json
  - server.json
  - pyproject.toml
findings:
  critical: 0
  warning: 1
  info: 2
  total: 3
status: issues_found
---

# Phase 3: Code Review Report

**Reviewed:** 2026-06-08T00:00:00Z
**Depth:** standard
**Files Reviewed:** 8
**Status:** issues_found

## Summary

Iteration-2 adversarial re-review of Phase 3 (Native + Ship). All four
iteration-1 fixes were verified against the live tree and confirmed sound:

- **WR-01 (scanner seam relocation):** `tests/test_scanner.py` contains 5 real
  unit tests that exercise the seam. Collection confirmed in BOTH the default
  invocation and the CI invocation (`pytest tests/ -q --ignore=tests/benchmarks`):
  all 5 scanner tests are collected (`--collect-only` count = 5), and the CI
  invocation runs 202 tests green. `test_scan_partition_no_native_fallback`
  genuinely forces the `except ImportError` branch by monkeypatching
  `builtins.__import__`, and `kafka_mcp._native` is confirmed absent in the
  environment, so the pure-Python fallback is the real active path (not a
  masked native import).
- **WR-02 (OWNER placeholders):** `server.json`, `glama.json`, and
  `pyproject.toml` all reference `github.com/mshegolev/kafka-mcp` consistently.
  Both JSON files parse cleanly. `server.json` declares `transport.type =
  stdio` and the `kafka-mcp` PyPI package with `registryType: pypi`. `glama.json`
  declares `transport: stdio` + `command: kafka-mcp`. Required fields present.
- **WR-03 (single pytest source):** No `[dependency-groups]` (PEP 735) table
  remains. `pytest>=9` appears exactly once, under
  `[project.optional-dependencies].dev`. All dev constraints are independently
  satisfiable and mutually compatible against PyPI (pytest 9.0.3,
  pytest-asyncio 1.4.0, pytest-benchmark 5.2.3). Runtime constraint
  `protobuf>=6.30` resolves to 6.33.6, compatible with `grpcio-tools`
  (protobuf<7 pin satisfied).
- **WR-04 (reload teardown):** `test_scan_partition_no_native_fallback` restores
  the real seam via `importlib.reload(scanner_mod)` in a `finally` block,
  outside the patch context — so the genuine (native-absent) fallback is
  restored. No cross-test leakage: the full 205-test suite passes in a single
  process.

**Security posture (intact):** `publish` job gated on
`github.event_name == 'release' || github.event_name == 'workflow_dispatch'`
only; `PYPI_TOKEN` read exclusively from `secrets`; `id-token: write` scoped to
the publish job; default `permissions: contents: read`. Native `build-wheels`
matrix is dormant via `if: hashFiles('native/Cargo.toml') != ''` — confirmed no
`native/Cargo.toml` (and no `Cargo.toml` anywhere) exists, so the Rust toolchain
is never silently installed (KAFKA-07 / T-03-03-D honored). Actions are pinned
(checkout@v4, setup-python@v5, upload-artifact@v4, cibuildwheel@v2.22,
gh-action-pypi-publish@release/v1).

**Gate checks:** `ruff check .` → "All checks passed!". `python3 -m pytest -q`
→ 205 passed. CI-equivalent `pytest tests/ -q --ignore=tests/benchmarks`
→ 202 passed (205 minus 3 benchmark tests).

One WARNING and two INFO items below. No Critical issues. No regressions
introduced by the iteration-1 fixes.

## Warnings

### WR-01: Local dev environment does not satisfy declared dev constraints — green run was against stale versions

**File:** `pyproject.toml:48-49`
**Issue:** The declared dev dependencies are `pytest>=9` and
`pytest-asyncio>=1.3`, but the environment the suite was validated in has
`pytest 8.3.4` and `pytest-asyncio 0.24.0` installed (verified via
`pip show`). The local "205 passed" run therefore did NOT exercise the pinned
versions; a fresh CI `pip install -e ".[dev]"` will resolve to pytest 9.0.3 and
pytest-asyncio 1.4.0 — a major-version bump for pytest-asyncio (0.x → 1.x) that
was never actually run locally. The risk is partially mitigated: there are zero
`async def test_*` functions in the suite (all async work goes through
`asyncio.run(...)`), so `asyncio_mode = "auto"` semantics do not gate any test,
and pytest 9 introduces no collection-breaking change for this suite's
patterns. Still, the green signal you are trusting was produced on different
machinery than CI will use.
**Fix:** Refresh the local environment to match CI before relying on the pass
signal, so the pinned major versions are actually exercised:
```bash
pip install -e ".[dev]" --upgrade
python3 -m pytest -q   # confirm green on pytest 9.x / pytest-asyncio 1.x
```
Alternatively, if a lower floor is acceptable and you want CI/local parity
without forcing pytest 9, relax to `pytest>=8.3` — but only after deciding
which floor Phase 3 intends to ship.

## Info

### IN-01: `_make_messages` helper duplicated verbatim across two test modules

**File:** `tests/test_scanner.py:25-62`, `tests/benchmarks/test_scan_benchmark.py:38-75`
**Issue:** The `_make_messages` synthetic-data factory is copy-pasted
byte-for-byte into both the relocated unit-test module and the benchmark
module. This is a direct consequence of the WR-01 relocation. Divergence risk:
a future edit to one copy (e.g. changing the message schema) silently desyncs
the benchmark subject from the correctness subject.
**Fix:** Extract the helper into a shared module (e.g.
`tests/_scan_fixtures.py` or a `conftest.py` fixture) imported by both. Low
priority — both copies are currently identical and small.

### IN-02: server.json / pyproject.toml version drift hazard (manual triple-source version)

**File:** `server.json:9`, `server.json:13`, `pyproject.toml:7`
**Issue:** The version `0.1.0` is hand-maintained in three places
(`pyproject.toml` `version`, `server.json` top-level `version`, and
`server.json` `packages[0].version`). On the next release these must be bumped
in lockstep; a missed `server.json` bump would publish a registry entry
pointing at a stale PyPI version. Not a defect today (all three read `0.1.0`),
but a latent maintenance trap.
**Fix:** Document the multi-file bump in a release checklist, or derive
`server.json` versions from `pyproject.toml` in the release workflow. No code
change required for this phase.

---

_Reviewed: 2026-06-08T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
