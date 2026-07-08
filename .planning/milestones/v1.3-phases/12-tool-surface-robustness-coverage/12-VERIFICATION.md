---
phase: 12
name: Tool-Surface Robustness & Coverage
verified: 2026-07-09
status: passed
scores:
  requirements: 4/4
  tests: green
requirements:
  - id: HINT-01
    status: satisfied
    evidence: "tests/test_tool_surface_hints.py — parametrized over both MCP faces (create_mcp_server, _create_http_mcp_server) via list_tools(): every tool advertises readOnlyHint/idempotentHint/openWorldHint all True; both _READ_ONLY ToolAnnotations constants (mcp_stdio + rest_api) pinned; frozen-tool-set guard. 6 tests."
  - id: PARSE-01
    status: satisfied
    evidence: "tests/test_tool_surface_hints.py — bad time_from/time_to raise ValueError naming the param + ISO-8601/trailing-Z; valid trailing-Z parses to tz-aware UTC; naive defaults UTC. 4 tests."
  - id: COV-01
    status: satisfied
    evidence: "tests/test_tool_face_coverage.py — consumer_group_lag on stdio MCP + HTTP MCP + REST POST /tools/consumer_group_lag (incl. topics filter), asserting delegation. 4 tests."
  - id: COV-02
    status: satisfied
    evidence: "tests/test_tool_face_coverage.py — correlate_messages on all three faces; CorrelateFakeClient proves inbound base64 raw + timestamp round-trip; REST proves outbound raw decodes to original bytes. 3 tests."
tech_debt: []
---

# Phase 12 Verification — Tool-Surface Robustness & Coverage

**Status: PASSED** — all 4 requirements satisfied; full default suite green.

## Evidence

- **Full suite:** `uv run python -m pytest -m 'not integration' -o addopts=""` → **348 passed, 1 skipped, 27 deselected** (up from 331 after Phase 11 — +17 new tests).
- **No `src/` changes** across both plans (`git diff --name-only src/` empty) — pure assertion-only hardening of already-shipped behavior.
- Two new test files with zero overlap ran in parallel: `test_tool_surface_hints.py` (10) and `test_tool_face_coverage.py` (7).

## Notes

- HINT-01 tool set is derived from `list_tools()` (not a hardcoded list), so a future tool cannot silently skip the hint check.
- COV-02 was genuinely uncovered on the inbound face before this phase (the legacy MockKafkaClient had no `correlate_messages`); the new fake formalizes it.
