---
phase: 03-native-ship
reviewed: 2026-06-08T00:00:00Z
depth: standard
files_reviewed: 7
files_reviewed_list:
  - src/kafka_mcp/scanner.py
  - tests/benchmarks/test_scan_benchmark.py
  - tests/test_inbound.py
  - .github/workflows/ci.yml
  - glama.json
  - server.json
  - pyproject.toml
findings:
  critical: 0
  warning: 4
  info: 4
  total: 8
status: issues_found
---

# Phase 3: Code Review Report

**Reviewed:** 2026-06-08T00:00:00Z
**Depth:** standard
**Files Reviewed:** 7
**Status:** issues_found

## Summary

Phase 3 (Native + Ship) covers the benchmark-gated scanner seam, the
distribution manifests, packaging metadata, the SC-4 regression suite, and a
security-sensitive CI workflow. Evidence captured during review:

- `ruff check .` → **All checks passed!**
- `python3 -m pytest -q` → **205 passed** (3 benchmarks measured, no threshold
  asserts).

**Security verdict (CI workflow): clean.** Each focus-area claim was verified
against the file:

- Publish job is gated `if: github.event_name == 'release' || ... ==
  'workflow_dispatch'` — it cannot run on push-to-main. `needs:
  [build-pure-python]` and `release`/`workflow_dispatch` triggers confirmed.
- PyPI credential flows exclusively through `${{ secrets.PYPI_TOKEN }}`, plus
  `id-token: write` scoped to the publish job only (OIDC Trusted Publisher).
  No hardcoded token anywhere.
- Actions are version-pinned (`checkout@v4`, `setup-python@v5`,
  `upload/download-artifact@v4`, `cibuildwheel@v2.22`,
  `gh-action-pypi-publish@release/v1`).
- `build-wheels` (cibuildwheel + Rust toolchain install) is dormant:
  `if: hashFiles('native/Cargo.toml') != ''`. Confirmed no `native/` dir and no
  `Cargo.toml` in the tree — so the Rust toolchain is never installed for the
  pure-Python outcome (KAFKA-07 gate honored: I/O-bound → Rust correctly NOT
  added).
- Top-level `permissions: contents: read` (least privilege). No `github.event.*`
  interpolation in any `run:` step → no script-injection surface.

The scanner seam is correct: only `ImportError` is caught, the pure-Python
fallback imports and runs with no Rust toolchain, and it preserves the
read-only property (it filters/copies dicts, never mutating inputs or touching
a broker).

No blockers. The findings below are correctness/maintainability gaps and
ship-hygiene issues that should be fixed before tagging a public release.

## Warnings

### WR-01: Module-level `importorskip` silently skips the scanner *unit* tests when pytest-benchmark is absent

**File:** `tests/benchmarks/test_scan_benchmark.py:29-32`
**Issue:** The skip guard is at module scope:

```python
pytest_benchmark = pytest.importorskip("pytest_benchmark", ...)
```

`importorskip` raises `Skipped` at collection time for the **entire module**.
That means the five non-benchmark unit tests in this file —
`test_scan_partition_pure_python_importable`,
`test_scan_partition_no_native_fallback`,
`test_scan_partition_returns_correct_subset`,
`test_scan_partition_empty_input`, `test_scan_partition_no_matches` — also get
skipped whenever `pytest-benchmark` is not installed. The module docstring
explicitly claims these are "always run, no benchmark fixture", but they are
not. The scanner seam (the headline deliverable of plan 03-01, including the
`ImportError`-fallback correctness test) has **zero coverage** in any
environment without the optional benchmark dependency. CI happens to install
`[dev]` (which pulls `pytest-benchmark`) *and* runs with
`--ignore=tests/benchmarks`, so CI never executes these unit tests at all — the
seam's correctness is effectively untested in the pipeline.
**Fix:** Move the unit tests out of the benchmark-gated module (e.g. into
`tests/test_scanner.py`, collected by the normal suite), and keep only the
`benchmark`-fixture tests behind the `importorskip` guard. Alternatively, apply
the skip per-test with `@pytest.mark.skipif` on just the benchmark functions so
the unit tests always run:

```python
# tests/test_scanner.py — always collected, no benchmark dep
from kafka_mcp.scanner import scan_partition

def test_scan_partition_no_native_fallback() -> None:
    ...
```

### WR-02: Published manifests ship literal `OWNER` placeholders in repository/source URLs

**File:** `server.json:3,6`, `glama.json:7-8` (also `pyproject.toml:59-61`)
**Issue:** The MCP registry manifest declares
`"name": "io.github.OWNER/kafka-mcp"` and
`"url": "https://github.com/OWNER/kafka-mcp"`; the Glama manifest declares
`"sourceUrl"`/`"homepage": "https://github.com/OWNER/kafka-mcp"`. `OWNER` is an
un-substituted template placeholder. For the MCP registry, the
`io.github.OWNER/...` namespace must match the real GitHub org for namespace
verification — publishing with `OWNER` will fail validation or claim a
namespace the publisher does not own. Glama `sourceUrl` will 404. These are
ship-blocking for the "Ship" half of the phase even though they don't affect
the test suite.
**Fix:** Replace every `OWNER` with the real GitHub owner/org before tagging a
release, or wire a release-time substitution step. Add a CI guard that fails if
any committed manifest still contains the literal `OWNER`:

```bash
! grep -rEn 'github\.com/OWNER|io\.github\.OWNER' server.json glama.json pyproject.toml
```

### WR-03: Duplicate, conflicting dev-dependency declarations in `pyproject.toml`

**File:** `pyproject.toml:46-53` and `pyproject.toml:77-81`
**Issue:** Dev dependencies are declared twice with divergent pins:

```toml
[project.optional-dependencies]
dev = [ "pytest>=8", "pytest-asyncio>=0.23", "ruff>=0.5", "responses>=0.25",
        "pytest-benchmark>=4.0" ]
...
[dependency-groups]
dev = [ "pytest>=9.0.3", "pytest-asyncio>=1.4.0" ]
```

`[project.optional-dependencies].dev` (PEP 621, what `pip install -e ".[dev]"`
in CI uses) requires `pytest>=8`, while the PEP 735 `[dependency-groups].dev`
requires `pytest>=9.0.3`. Two "dev" groups with different floors is a
maintenance trap: a contributor using `uv`/`pip`'s dependency-group resolution
gets a different, stricter pytest than CI, and the lists are not kept in sync
(the group omits `ruff`, `responses`, `pytest-benchmark`). This is the kind of
drift that produces "works in CI, fails locally" (or vice-versa).
**Fix:** Pick one mechanism. Since CI installs `.[dev]`, keep
`[project.optional-dependencies].dev` as the single source of truth and delete
the `[dependency-groups]` block — or make the group reference the optional
group so they cannot diverge. At minimum reconcile the `pytest` floor.

### WR-04: `test_scan_partition_no_native_fallback` leaves the `kafka_mcp.scanner` module reloaded in fallback state with no teardown

**File:** `tests/benchmarks/test_scan_benchmark.py:92-118`
**Issue:** The test deletes `kafka_mcp.scanner`/`kafka_mcp._native` from
`sys.modules`, patches `builtins.__import__` to block `_native`, then
`importlib.reload(scanner_mod)`. The reload mutates the real, cached
`kafka_mcp.scanner` module object in place (binding `scan_partition` to the
pure-Python fallback) and there is **no `finally`/fixture that reloads it back**
to its original state once the patch exits. Today this is benign because the
native extension is never present (the fallback is already the active path), so
the reload is a no-op in effect. But the test is order-dependent and fragile:
the moment a compiled `kafka_mcp._native` exists (the entire point of the
preserved seam), this test would silently flip every subsequent test in the
process onto the pure-Python path, masking the native code under test. A test
that mutates global import state must restore it.
**Fix:** Wrap the reload in a teardown that restores the module:

```python
import importlib
import kafka_mcp.scanner as scanner_mod
try:
    with patch("builtins.__import__", side_effect=_block_native):
        importlib.reload(scanner_mod)
        sp = scanner_mod.scan_partition
        assert callable(sp)
finally:
    importlib.reload(scanner_mod)  # restore real (possibly-native) seam
```

## Info

### IN-01: `scan_partition` docstring promises a dict-typed `value`, but passes it unvalidated to `_extract_evidence_keys`

**File:** `src/kafka_mcp/scanner.py:55-73`
**Issue:** The docstring states the schema is `{"value": dict, ...}` and the
fallback calls `_extract_evidence_keys(msg.get("value"), msg.get("headers",
{}))`. `_extract_evidence_keys` only guards `value is not None` and then calls
`value.get(alias)`; if a caller passes a `value` that is a non-`None`, non-dict
(e.g. a JSON array, string, or `int` from a decoded scalar message), it raises
`AttributeError` rather than returning empty evidence. The scanner is positioned
as a reusable seam, so an undocumented hard failure on a plausible input is a
latent footgun. Not exploitable and not hit by current tests (which always pass
dict values).
**Fix:** Either tighten the type contract or guard defensively, e.g. in
`_extract_evidence_keys`: `if isinstance(value, dict):` instead of
`if value is not None:`.

### IN-02: `pytest_benchmark = pytest.importorskip(...)` binds a module that is never used

**File:** `tests/benchmarks/test_scan_benchmark.py:29`
**Issue:** The return value of `importorskip` is assigned to `pytest_benchmark`
but never referenced; only the side effect (skip-if-absent) is wanted. ruff
does not flag it because module-level assignments aren't unused-import checks,
but the binding is dead and slightly misleading.
**Fix:** Drop the assignment: `pytest.importorskip("pytest_benchmark",
reason=...)`.

### IN-03: CI `build-wheels` artifacts are never consumed by `publish` (latent gap for when Rust is added)

**File:** `.github/workflows/ci.yml:81-123,138-155`
**Issue:** `publish` does `needs: [build-pure-python]` and downloads only the
`dist-pure-python` artifact. When `native/Cargo.toml` is later added and
`build-wheels` activates, its per-OS wheels (`dist-${{ matrix.os }}`) are built
but never published — the release would still ship only the pure-Python wheel,
silently dropping the compiled wheels the matrix exists to produce. Harmless
today (job dormant), but the seam is "wired for when Rust is added," so the
publish wiring should match.
**Fix:** When enabling native builds, add `build-wheels` to `publish.needs` and
download all `dist-*` artifacts (e.g. `download-artifact` with `pattern: dist-*`
and `merge-multiple: true`), or document that `publish` is pure-Python-only.

### IN-04: `gh-action-pypi-publish@release/v1` is a moving tag (supply-chain hardening)

**File:** `.github/workflows/ci.yml:158`
**Issue:** All other actions are pinned to a version tag, but the publish action
uses the floating `release/v1` branch. For a job that holds PyPI publish rights,
pinning to a commit SHA (or at least an immutable `vX.Y.Z` tag) removes the
mutable-ref risk. Low severity because this is the PyPA-maintained official
action.
**Fix:** Pin to an immutable ref, e.g.
`pypa/gh-action-pypi-publish@<full-sha> # v1.x.y`.

---

_Reviewed: 2026-06-08T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
