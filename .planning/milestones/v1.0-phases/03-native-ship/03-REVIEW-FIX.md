---
phase: 03-native-ship
fixed_at: 2026-06-08T00:00:00Z
review_path: .planning/phases/03-native-ship/03-REVIEW.md
iteration: 1
findings_in_scope: 4
fixed: 4
skipped: 0
status: all_fixed
---

# Phase 3: Code Review Fix Report

**Fixed at:** 2026-06-08T00:00:00Z
**Source review:** .planning/phases/03-native-ship/03-REVIEW.md
**Iteration:** 1

**Summary:**
- Findings in scope: 4 (WR-01, WR-02, WR-03, WR-04 — 0 critical, 4 warning)
- Fixed: 4
- Skipped: 0

**Gates (run after all fixes, in the isolated worktree):**
- `ruff check .` → All checks passed!
- `python3 -m pytest -q` → 205 passed (3 benchmarks measured)
- CI-style collection (`--ignore=tests/benchmarks`) now collects the 5
  relocated scanner unit tests (previously 0) — WR-01 verified fixed.

## Fixed Issues

### WR-01: Module-level `importorskip` silently skips the scanner unit tests when pytest-benchmark is absent

**Files modified:** `tests/test_scanner.py` (new), `tests/benchmarks/test_scan_benchmark.py`
**Commit:** c88948d (shared with WR-04)
**Applied fix:** Relocated the five non-benchmark scanner seam unit tests
(`test_scan_partition_pure_python_importable`,
`test_scan_partition_no_native_fallback`,
`test_scan_partition_returns_correct_subset`,
`test_scan_partition_empty_input`, `test_scan_partition_no_matches`) out of
the pytest-benchmark-gated `tests/benchmarks/test_scan_benchmark.py` into a
new `tests/test_scanner.py` that is collected by the default `pytest` run and
by CI (which uses `--ignore=tests/benchmarks`). The benchmark module now
contains only the three `benchmark`-fixture tests behind the module-level
`importorskip` guard. Verified: with `--ignore=tests/benchmarks`, the 5
scanner unit tests are now collected (previously 0).

### WR-02: Published manifests ship literal `OWNER` placeholders in repository/source URLs

**Files modified:** `server.json`, `glama.json`, `pyproject.toml`
**Commit:** d011987
**Applied fix:** Replaced every `OWNER` placeholder with the canonical GitHub
owner `mshegolev` (per README `io.github.mshegolev/kafka-mcp`). Updated
`server.json` (`name` → `io.github.mshegolev/kafka-mcp`, `repository.url`),
`glama.json` (`sourceUrl`, `homepage`), and `pyproject.toml`
(`[project.urls]` Homepage/Repository/Issues). Both JSON manifests re-validated
as parseable; no `OWNER` token remains in any of the three files.

### WR-03: Duplicate, conflicting dev-dependency declarations in `pyproject.toml`

**Files modified:** `pyproject.toml`
**Commit:** 3ee7c6d
**Applied fix:** Removed the PEP 735 `[dependency-groups].dev` block that
conflicted with the PEP 621 `[project.optional-dependencies].dev` (the group
pinned `pytest>=9.0.3` / `pytest-asyncio>=1.4.0`, both unsatisfiable against
the installed pytest 9.0.2 / pytest-asyncio 1.3.0). CI installs `.[dev]`, so
`[project.optional-dependencies].dev` is kept as the single source of truth and
its floors lifted to a consistent `pytest>=9` / `pytest-asyncio>=1.3` (both
satisfied by the installed/locked versions). The contradictory second "dev"
group is gone.

### WR-04: `test_scan_partition_no_native_fallback` leaves the scanner module reloaded with no teardown

**Files modified:** `tests/test_scanner.py` (relocated test)
**Commit:** c88948d (shared with WR-01)
**Applied fix:** Wrapped the `importlib.reload(scanner_mod)` under the blocked
`__import__` patch in a `try`/`finally`, restoring the real (possibly-native)
`kafka_mcp.scanner` seam in the `finally` block so the test no longer leaks the
pure-Python fallback state into subsequent tests in the same process. The fix
travelled with the WR-01 relocation into `tests/test_scanner.py`.

## Skipped Issues

None — all in-scope findings were fixed.

---

_Fixed: 2026-06-08T00:00:00Z_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
