---
phase: 04-extended-decode-transport
fixed_at: 2026-06-08T15:45:00Z
review_path: .planning/phases/04-extended-decode-transport/04-REVIEW.md
iteration: 1
findings_in_scope: 5
fixed: 5
skipped: 0
status: all_fixed
---

# Phase 04: Code Review Fix Report

**Fixed at:** 2026-06-08T15:45:00Z
**Source review:** .planning/phases/04-extended-decode-transport/04-REVIEW.md
**Iteration:** 1

**Summary:**
- Findings in scope: 5 (CR-01, WR-01, WR-02, WR-03, IN-01; IN-02 excluded per instructions)
- Fixed: 5
- Skipped: 0

## Fixed Issues

### CR-01: Naive datetime comparison crash in HTTP MCP search_messages tool

**Files modified:** `src/kafka_mcp/adapters/inbound/rest_api.py`, `src/kafka_mcp/adapters/inbound/mcp_stdio.py`, `tests/test_inbound.py`
**Commit:** f4584d9
**Applied fix:**
- Added `from datetime import datetime, timezone` at module level in both `rest_api.py` and `mcp_stdio.py` (replacing bare `datetime` import).
- In the HTTP MCP `search_messages` tool closure (`rest_api._create_http_mcp_server`): replaced the local `from datetime import datetime as _dt` re-import with the module-level `datetime`, and added the UTC fallback guard after each `fromisoformat()` call:
  ```
  if tf is not None and tf.tzinfo is None:
      tf = tf.replace(tzinfo=timezone.utc)
  ```
  Same guard for `tt`.
- Applied the identical fix to the MCP stdio `search_messages` tool in `mcp_stdio.py`.
- Added `TestNaiveDatetimeGuard` class with 6 regression tests: naive `time_from`, naive `time_to`, and aware `+00:00` string for both HTTP MCP and stdio MCP faces.
- Note: Python 3.10's `fromisoformat()` does not accept the `Z` suffix (that was added in 3.11); the test uses `+00:00` offset format as the "already-aware" case, consistent with the project's `requires-python = ">=3.10"` constraint.

---

### WR-01: limit has no upper-bound guard on MCP stdio and HTTP MCP paths

**Files modified:** `src/kafka_mcp/adapters/inbound/rest_api.py`, `src/kafka_mcp/adapters/inbound/mcp_stdio.py`, `tests/test_inbound.py`
**Commit:** c462e67
**Applied fix:**
- Added `limit = max(1, min(limit, 10_000))` as the first statement inside both `search_messages` tool functions (HTTP MCP and stdio MCP), before the datetime parsing and client call.
- Added `TestLimitUpperBound` class with 4 tests: oversized limit (10_000_001) and normal limit (100) for the HTTP MCP face; oversized limit and zero limit (clamped to 1) for the stdio MCP face.

---

### WR-02: server.json remotes entry missing SASL/SSL environment variable declarations

**Files modified:** `server.json`
**Commit:** bb744d6
**Applied fix:**
- Added the five missing environment variable declarations to `remotes[0].environmentVariables` in `server.json`:
  - `KAFKA_SECURITY_PROTOCOL` (with `"default": "PLAINTEXT"`)
  - `KAFKA_SASL_MECHANISM`
  - `KAFKA_SASL_USERNAME`
  - `KAFKA_SASL_PASSWORD` (with `"isSecret": true`)
  - `KAFKA_SSL_VERIFY` (with `"default": "true"`)
- Names, descriptions, `isRequired`, `isSecret`, `format`, and `default` values match the corresponding entries in the `packages` (stdio) section exactly.
- JSON validated with `node -e JSON.parse(...)` — no syntax errors.

---

### WR-03: Redundant local re-imports of already-imported names inside HTTP MCP tool closures

**Files modified:** `src/kafka_mcp/adapters/inbound/rest_api.py`
**Commit:** 8eecf8f
**Applied fix:**
- Removed `from kafka_mcp.domain.errors import TopicNotFoundError as _TopicNotFoundError` from the `describe_topic` closure; replaced `_TopicNotFoundError` usage with the module-level `TopicNotFoundError`.
- Removed the three local re-imports (`_DecodeError`, `_MNF`, `_TransientError`) from the `get_message` closure; replaced all usages with the module-level names `DecodeError`, `MessageNotFoundError`, and `TransientError`.
- Note: the local `from datetime import datetime as _dt` in `search_messages` was already removed as part of the CR-01 fix (commit f4584d9).
- `ruff check` passes clean.

---

### IN-01: glama.json HTTP option uses "transport": "http" rather than "streamable-http"

**Files modified:** `glama.json`
**Commit:** e451018
**Applied fix:**
- Changed `"transport": "http"` to `"transport": "streamable-http"` in the `serverConfigOptions[0]` entry, aligning with the `"type": "streamable-http"` used in `server.json`'s remotes entry.
- JSON validated — no syntax errors.

---

## Skipped Issues

None.

---

## Test and lint results

**Full test suite (267 tests):** all passed  
`python3 -m pytest tests/ --ignore=tests/benchmarks` — 267 passed in 1.70s

**Ruff:** clean  
`python3 -m ruff check src/ tests/` — All checks passed!

**New tests added:** 10 (6 for CR-01 in `TestNaiveDatetimeGuard`, 4 for WR-01 in `TestLimitUpperBound`)

---

_Fixed: 2026-06-08T15:45:00Z_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
