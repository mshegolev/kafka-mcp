---
phase: 03-native-ship
reviewed: 2026-06-08T12:00:00Z
depth: standard
files_reviewed: 3
files_reviewed_list:
  - src/kafka_mcp/scanner.py
  - .github/workflows/ci.yml
  - pyproject.toml
findings:
  critical: 0
  warning: 3
  info: 3
  total: 6
status: issues_found
---

# Phase 3: Code Review Report

**Reviewed:** 2026-06-08T12:00:00Z
**Depth:** standard
**Files Reviewed:** 3
**Status:** issues_found

## Summary

Adversarial review of the three Phase 3 production change targets: the scanner
seam (`scanner.py`), the CI workflow (`ci.yml`), and the package manifest
(`pyproject.toml`). Supporting context read includes `search_service.py` (the
domain module the seam imports from), `tests/test_scanner.py`, and
`tests/benchmarks/test_scan_benchmark.py`.

**Verified good — scanner fallback:** The `try/except ImportError` seam in
`scanner.py` is correctly constructed. Only `ImportError` is caught; no bare
`except`. An `ImportError` raised inside the `except` block (e.g., if
`kafka_mcp.domain.search_service` is absent) propagates normally and is NOT
silently swallowed. Read-only guarantee holds: no `produce`, `commit`, `send`,
or `publish` call is present in `scanner.py`. The `_extract_evidence_keys`
import target is correctly typed and accepts `None` as `value` (no crash on
missing `"value"` key in a message dict).

**Verified good — CI publish gating:** The `publish` job fires only on
`github.event_name == 'release'` or `workflow_dispatch`. `PYPI_TOKEN` is read
exclusively from `${{ secrets.PYPI_TOKEN }}` — never hardcoded. The
`build-wheels` (cibuildwheel) native matrix is correctly dormant via
`if: hashFiles('native/Cargo.toml') != ''`; no `native/Cargo.toml` exists, so
the Rust toolchain is never silently installed (KAFKA-07 / T-03-03-D). The
`id-token: write` permission is scoped to the `publish` job only; top-level
default is `contents: read`.

Three warnings and three info items follow. No critical issues.

## Warnings

### WR-01: `workflow_dispatch` allows untagged PyPI publish from any branch

**File:** `.github/workflows/ci.yml:14,142-144`
**Issue:** `workflow_dispatch` is declared as a top-level trigger with no
`inputs` requiring a version or tag confirmation. The `publish` job's guard
(`github.event_name == 'workflow_dispatch'`) therefore fires on a manual
dispatch run from *any* branch — including feature branches with unreviewed
code — without any version tag present. An operator running the workflow
manually from a non-release branch (e.g., to debug CI) will publish whatever
`dist-pure-python` artifact the `build-pure-python` job produced from that
branch's HEAD to the live PyPI index. The comment says "configure environment
`pypi` with manual-approval rule in GitHub Settings — recommended but not
enforced by this file," but that protection is external and unenforced.
**Fix:** Either (a) add a `workflow_dispatch` input requiring explicit version
confirmation and validate it inside the publish job:
```yaml
on:
  workflow_dispatch:
    inputs:
      version_tag:
        description: "PyPI version to publish (must match pyproject.toml)"
        required: true
```
and add to the publish job:
```yaml
- name: Verify version tag matches pyproject.toml
  run: |
    TOML_VER=$(grep '^version' pyproject.toml | head -1 \
      | sed 's/.*= *"//' | sed 's/".*//')
    [ "${{ inputs.version_tag }}" = "$TOML_VER" ] \
      || (echo "Tag mismatch"; exit 1)
```
or (b) restrict the publish `if:` condition to only release events:
```yaml
if: github.event_name == 'release'
```
and remove `workflow_dispatch` from the publish path entirely, keeping it
available for CI runs only (builds + tests but no publish).

---

### WR-02: Duplicate conflicting `dev` dependency declarations — CI uses lower floor than uv

**File:** `pyproject.toml:47-53` vs `pyproject.toml:78-81` (second table
removed in latest tree iteration; verified absent). **If** a `[dependency-groups]`
table with `pytest>=9.0.3` is still present or reintroduced, this becomes
active.

**Current state:** `[project.optional-dependencies].dev` specifies `pytest>=8`
and `pytest-asyncio>=0.23`. CI installs with `pip install -e ".[dev]"` which
resolves against these floors. The uv-based local workflow consults the
`uv.lock`, which pins `pytest 9.0.3` and `pytest-asyncio 1.4.0`. This creates
a version-environment gap: CI can install `pytest 8.x` while local runs
`pytest 9.0.3`. The major-version jump `pytest-asyncio 0.x → 1.x` is a
significant API break (plugin mode semantics, fixture signatures) that is
never tested in CI if pip resolves to `0.23.x`.
**Fix:** Align the floor in `[project.optional-dependencies].dev` with the
`uv.lock` resolved version to ensure CI and local run the same toolchain:
```toml
[project.optional-dependencies]
dev = [
    "pytest>=9",
    "pytest-asyncio>=1.4",
    "ruff>=0.5",
    "responses>=0.25",
    "pytest-benchmark>=4.0",
]
```
Alternatively, update CI to use `uv` so `uv.lock` pins are honoured:
```yaml
- name: Install uv
  uses: astral-sh/setup-uv@v5
- name: Install dependencies
  run: uv sync --extra dev
```

---

### WR-03: `cachetools>=7.1.4` pins to the exact current latest — any resolver that treats `>=` as "at least" will stall if 7.1.4 is yanked

**File:** `pyproject.toml:38`
**Issue:** `cachetools>=7.1.4` is valid today because 7.1.4 is the current
latest release. However, pinning `>=` to the exact latest version means any
resolver will install exactly 7.1.4 until a newer version is published.
This is functionally equivalent to `==7.1.4` for new installs today. More
critically: if PyPI yanks 7.1.4 (e.g., for a critical CVE) with no 7.1.5
available yet, resolution will hard-fail. The `authlib>=1.7.2` constraint has
the same pattern (1.7.2 is currently the latest).
**Fix:** Relax to the minor floor that introduced the API surface actually used
(cachetools 5.x is the widely stable series; 7.x jumps major versions from the
well-known API):
```toml
"cachetools>=5.3",
"authlib>=1.3",
```
If the code genuinely depends on 7.x APIs, document the reason and keep the
floor. Otherwise use the oldest version whose API the code requires.

---

## Info

### IN-01: Private symbol `_extract_evidence_keys` imported across module boundary in scanner seam

**File:** `src/kafka_mcp/scanner.py:41`
**Issue:** The pure-Python fallback inside the `except ImportError` block
imports `_extract_evidence_keys` by its private (underscore-prefixed) name
from `kafka_mcp.domain.search_service`. Private symbols have no stability
contract; a rename or move of that function during a future domain refactor
will break the scanner import at runtime with a cryptic `ImportError`. The
`noqa: E402` comment suppresses the linter flag but not the coupling.
**Fix:** Either (a) promote the function to the module's public API by removing
the underscore prefix and adding it to `__all__`, or (b) define a small
`extract_evidence_keys` wrapper in `kafka_mcp.domain.search_service` that is
public and documented as part of the scanner contract. Option (a) is one-line:
```python
# search_service.py — rename _extract_evidence_keys to extract_evidence_keys
# and add to __all__
```

---

### IN-02: `scan_partition` lacks `key_field` parameter — narrower than the domain service it wraps

**File:** `src/kafka_mcp/scanner.py:43-46`
**Issue:** `scan_partition(messages, key)` only performs direct `msg["key"] ==
key` equality. `TopicService.search_messages` supports three matching
strategies: plain key, `"header:<name>"`, and `"value:<dotted.path>"`. If a
future native extension or caller expects the scanner seam to honour `key_field`
semantics (to be a proper hot-path replacement for the full domain loop), the
current signature is a dead end that requires a breaking API change. The
docstring documents this omission implicitly but not explicitly.
**Fix (documentation):** Add to the docstring:
```
Note:
    This function implements key-equality matching only.  The
    header:/value: key_field strategies from TopicService.search_messages
    are NOT supported.  A future native extension that adds those strategies
    must extend the signature.
```
**Fix (code, if extension is planned):** Add an optional `key_field` parameter
now so the API is stable:
```python
def scan_partition(
    messages: list[dict[str, Any]],
    key: str,
    key_field: str | None = None,
) -> list[dict[str, Any]]:
```

---

### IN-03: `_make_messages` helper duplicated verbatim in two test modules

**File:** `tests/test_scanner.py:25-62` and
`tests/benchmarks/test_scan_benchmark.py:38-75`
**Issue:** The synthetic message factory is copy-pasted byte-for-byte in both
the unit-test module and the benchmark module. A future schema change to the
message dict (e.g., adding a `"partition"` field to match the domain model)
must be applied to both copies; missing one will silently decouple the
benchmark from the correctness tests.
**Fix:** Extract into a shared fixture:
```python
# tests/conftest.py
def make_messages(n, target_key, match_every=None):
    ...
```
or a `tests/_fixtures.py` module imported by both. Low urgency — both copies
are currently identical.

---

_Reviewed: 2026-06-08T12:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
