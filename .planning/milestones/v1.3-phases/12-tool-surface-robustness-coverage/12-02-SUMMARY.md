---
phase: 12-tool-surface-robustness-coverage
plan: 02
subsystem: testing
tags: [pytest, fastapi, testclient, fastmcp, base64, correlate_messages, consumer_group_lag]

# Dependency graph
requires:
  - phase: 05-consumer-group-lag
    provides: consumer_group_lag tool across stdio MCP / HTTP MCP / REST faces
  - phase: enhance-correlation-engine
    provides: correlate_messages tool with base64 raw + timestamp inverse-serialize decode path
provides:
  - COV-01 face coverage for consumer_group_lag (stdio MCP, HTTP MCP, REST + topics filter)
  - COV-02 face coverage for correlate_messages (stdio MCP, HTTP MCP, REST) with base64 raw + timestamp round-trip verified inbound and outbound
affects: [tool-surface-robustness-coverage, milestone-audit, correlate_messages, consumer_group_lag]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Per-tool fake client capturing delegated args for round-trip assertions"
    - "FastMCP call_tool result-shape normalization (tuple vs list) via _content() helper"

key-files:
  created:
    - tests/test_tool_face_coverage.py
  modified: []

key-decisions:
  - "Defined a dedicated CorrelateFakeClient (test_inbound.py MockKafkaClient has no correlate_messages) that captures initial_results for round-trip assertions"
  - "Shared payload dict construction; passed under initial_results_data for MCP faces and initial_results for REST per the adapters' differing param names"

patterns-established:
  - "Round-trip assertion: base64-encode known raw bytes + ISO-8601 timestamp inbound, assert exact bytes/tz-aware datetime reconstructed on the fake, and assert base64 decode back to original bytes on the REST response outbound"

requirements-completed: [COV-01, COV-02]

# Metrics
duration: 6min
completed: 2026-07-08
status: complete
---

# Phase 12 Plan 02: Tool-Surface Robustness Coverage Summary

**Test-only face coverage for consumer_group_lag and correlate_messages across stdio MCP, HTTP MCP, and REST, with the correlate_messages base64 raw + timestamp round-trip verified both inbound (reconstructed KafkaMessage on the fake) and outbound (REST response raw decodes to original bytes)**

## Performance

- **Duration:** ~6 min
- **Started:** 2026-07-08T18:37:42Z
- **Completed:** 2026-07-08T18:43:47Z
- **Tasks:** 2
- **Files modified:** 1 (created)

## Accomplishments
- COV-01: consumer_group_lag exercised on stdio MCP, HTTP MCP, and REST faces asserting delegation to the client (group/topic/lag/source/event_type); REST topics filter narrows the result to the orders record only.
- COV-02: correlate_messages exercised on stdio MCP, HTTP MCP, and REST faces. The fake client captures the received `initial_results` so the inbound base64 raw + timestamp round-trip is asserted (exact bytes + tz-aware datetime reconstructed). REST additionally proves the outbound round-trip (response `raw` decodes back to the original sample bytes).
- New self-contained module `tests/test_tool_face_coverage.py` (309 lines, 7 tests) built on the test_inbound.py fake-client + TestClient + `call_tool` patterns. No src/ file modified.

## Task Commits

Each task was committed atomically:

1. **Task 1: COV-01 consumer_group_lag face coverage** - `c624458` (test)
2. **Task 2: COV-02 correlate_messages face coverage with round-trip** - `cdeb27a` (test)

## Files Created/Modified
- `tests/test_tool_face_coverage.py` - New test module. LagFakeClient + CorrelateFakeClient, `_content()` call_tool shape normalizer, shared payload/round-trip helpers; 4 COV-01 tests + 3 COV-02 tests.

## Decisions Made
- Inlined a minimal `LagFakeClient` and a dedicated `CorrelateFakeClient` rather than importing MockKafkaClient — the existing MockKafkaClient in test_inbound.py has no `correlate_messages` method, and CorrelateFakeClient must additionally capture `initial_results` to prove the inbound round-trip.
- Kept payload dict construction shared and passed it under the correct key per face (`initial_results_data` for MCP tool params, `initial_results` for the REST CorrelateMessagesRequest body).

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None.

## Test Results
- `uv run python -m pytest tests/test_tool_face_coverage.py -v` → 7 passed.
- COV-01 selector `-k "consumer_group_lag or lag"` → 4 passed.
- COV-02 selector `-k "correlate"` → 3 passed.
- Full default suite `uv run python -m pytest -m 'not integration' -o addopts=""` → 348 passed, 1 skipped, 27 deselected. No regressions.
- `git diff --name-only src/` → empty (no source changes).

## Next Phase Readiness
- COV-01 and COV-02 requirements satisfied; both previously-uncovered tools now have automated face coverage. Ready for milestone audit.

## Self-Check: PASSED
- tests/test_tool_face_coverage.py exists
- Commit c624458 (Task 1) exists
- Commit cdeb27a (Task 2) exists
- 12-02-SUMMARY.md exists

---
*Phase: 12-tool-surface-robustness-coverage*
*Completed: 2026-07-08*
