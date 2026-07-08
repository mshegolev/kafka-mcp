---
phase: 12-tool-surface-robustness-coverage
plan: 01
subsystem: testing
tags: [mcp, fastmcp, tool-annotations, iso8601, pytest, hardening]

# Dependency graph
requires:
  - phase: 04-http-mcp-transport
    provides: rest_api._create_http_mcp_server + shared _READ_ONLY ToolAnnotations
  - phase: 02-investigator-contract
    provides: mcp_stdio.create_mcp_server + _parse_iso_utc timestamp guard
provides:
  - Test-only lock on read-only hint advertisement across stdio + HTTP MCP faces
  - Pinned _READ_ONLY ToolAnnotations constants (mcp_stdio + rest_api)
  - Regression coverage for _parse_iso_utc actionable-error + naive/trailing-Z UTC handling
affects: [tool-surface-robustness-coverage, mtls-packaging-hardening]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Derive tool set from list_tools() (not a hardcoded list) so new tools cannot skip hint checks"
    - "Parametrized MCP-face fixtures via module-level _build_servers() for stdio + HTTP symmetry"

key-files:
  created:
    - tests/test_tool_surface_hints.py
  modified: []

key-decisions:
  - "Single test artifact for both HINT-01 and PARSE-01 (plan declares one files_modified entry); committed atomically"
  - "Assert trailing-Z example via the substring \"Z'\" the function actually emits, not a hardcoded literal message"

patterns-established:
  - "Pattern: parametrize over (face, server) so stdio + HTTP MCP faces share one assertion body"
  - "Pattern: frozen-tool-set subset check catches accidental tool removal without over-constraining tool count"

requirements-completed: [HINT-01, PARSE-01]

# Metrics
duration: 6min
completed: 2026-07-09
status: complete
---

# Phase 12 Plan 01: Tool-Surface Hint & Timestamp-Parse Hardening Summary

**Test-only lock proving every read-only tool on the stdio + HTTP MCP faces advertises readOnlyHint/idempotentHint/openWorldHint (all True) and that `_parse_iso_utc` rejects bad time_from/time_to with an actionable param-named error while accepting trailing-Z / naive input as tz-aware UTC.**

## Performance

- **Duration:** ~6 min
- **Completed:** 2026-07-09
- **Tasks:** 2
- **Files modified:** 1 (created)

## Accomplishments
- HINT-01: parametrized coverage over both MCP faces (`create_mcp_server`, `_create_http_mcp_server`) asserting all three hints True for every tool discovered via `list_tools()`; frozen-tool-set subset check guards against removals.
- HINT-01: both shared module-level `_READ_ONLY` ToolAnnotations constants (mcp_stdio + rest_api) pinned to carry the three hints — the single object mounted on every REST/HTTP-MCP tool at registration.
- PARSE-01: bad `time_from`/`time_to` each raise a `ValueError` naming the param + ISO-8601 + trailing-Z example; valid trailing-Z parses to tz-aware UTC; naive timestamp defaults to UTC.
- No src/ behavior modified (assertion-only, per plan constraint).

## Task Commits

Both tasks target the single declared artifact `tests/test_tool_surface_hints.py`, committed atomically:

1. **Task 1 (HINT-01) + Task 2 (PARSE-01)** - `74be6dc` (test)

## Files Created/Modified
- `tests/test_tool_surface_hints.py` - 10 tests: 6 HINT-01 (per-face hint loop, frozen-set subset, two `_READ_ONLY` constant pins) + 4 PARSE-01 (bad time_from, bad time_to, trailing-Z UTC, naive→UTC).

## Decisions Made
- Committed both tasks in one atomic commit because the plan declares a single `files_modified` artifact created as one unit; the message documents both requirements.
- Asserted the trailing-Z example via the `"Z'"` substring the function actually emits (`'2026-06-01T00:00:00Z'`) rather than pasting the full literal error text, per the plan's instruction to match on concept.

## Deviations from Plan
None - plan executed exactly as written.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Test Results
- `uv run python -m pytest tests/test_tool_surface_hints.py -v` → 10 passed.
- `uv run python -m pytest tests/test_tool_surface_hints.py -k "hint or annotation" -q` → 10 passed.
- `uv run python -m pytest tests/test_tool_surface_hints.py -k "parse_iso or timestamp" -q` → 4 passed.
- `uv run python -m pytest -m 'not integration' -o addopts=""` → 341 passed, 1 skipped, 27 deselected.
- `git diff --name-only src/` → empty (no source changes from this plan).

## Next Phase Readiness
- HINT-01 and PARSE-01 hardening locked. Plan 12-02 (COV-01/COV-02) remains for phase completion.

## Self-Check: PASSED
- FOUND: tests/test_tool_surface_hints.py
- FOUND: commit 74be6dc

---
*Phase: 12-tool-surface-robustness-coverage*
*Completed: 2026-07-09*
