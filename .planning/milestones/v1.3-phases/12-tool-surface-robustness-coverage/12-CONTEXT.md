# Phase 12: Tool-Surface Robustness & Coverage - Context

**Gathered:** 2026-07-09
**Status:** Ready for planning
**Mode:** Auto-generated (autonomous — low-ambiguity hardening phase; code already exists)

<domain>
## Phase Boundary

Prove the frozen read-only tool surface is correct and well-covered:
- Every read-only tool advertises `idempotentHint=true` + `openWorldHint=true`
  (plus `readOnlyHint`) across stdio MCP, HTTP MCP, and REST faces (HINT-01).
- `search_messages` returns an actionable error for invalid ISO-8601 timestamps
  naming the param + accepted format, trailing `Z` accepted (PARSE-01).
- `consumer_group_lag` (COV-01) and `correlate_messages` (COV-02) gain automated
  coverage across the faces they are exposed on.

Out of scope: any new tool or behavior change — hardening/coverage only.
</domain>

<decisions>
## Implementation Decisions

### Already implemented (verify with tests, do NOT change behavior)
- `_READ_ONLY = ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=True)`
  in both src/kafka_mcp/adapters/inbound/mcp_stdio.py and .../rest_api.py.
- `_parse_iso_utc(value, param)` in mcp_stdio.py raises a ValueError naming the
  param and accepted format; used by `search_messages` for time_from/time_to.

### Claude's discretion
- HINT-01: a test that introspects the registered tools' ToolAnnotations on the
  stdio MCP server, the HTTP MCP server, and (where applicable) the REST wiring,
  asserting readOnlyHint/idempotentHint/openWorldHint all True for every tool.
- PARSE-01: a unit test that a bad `time_from`/`time_to` yields the actionable
  ValueError (message mentions the param name and ISO-8601 / `Z`), and that a
  valid trailing-`Z` timestamp parses to a tz-aware UTC datetime.
- COV-01 / COV-02: face-level tests for `consumer_group_lag` and
  `correlate_messages` — assert request/response shape and delegation to the
  client, covering the faces each is exposed on (stdio, HTTP MCP, REST).
</decisions>

<code_context>
## Existing Code Insights

- Faces: mcp_stdio.py (FastMCP stdio), rest_api.py (FastAPI + HTTP MCP), cli.py,
  lib.py. Existing tests: tests/test_inbound.py, tests/test_adapters.py,
  tests/test_correlation*.py.
- Tool builders live inside create_mcp_server / _create_http_mcp_server /
  create_app; ToolAnnotations are attached at registration.
- Serialization helpers (_serialize_message / _serialize_lag_record) already
  tested for search/get; lag + correlate need explicit face coverage.
</code_context>

<specifics>
## Specific Ideas

- HINT-01: parametrize over the tool set; fetch annotations from the FastMCP
  tool registry and the REST route metadata.
- PARSE-01: cover time_from AND time_to, invalid + trailing-Z-valid.
- COV-01: consumer_group_lag on stdio/HTTP MCP + REST (POST /tools/consumer_group_lag).
- COV-02: correlate_messages on stdio/HTTP MCP + REST (round-trip of base64 raw + timestamp).
</specifics>

<deferred>
## Deferred Ideas

- Published-package smoke test (v1.3 Future).
- Schema Registry / SASL end-to-end (v1.3 Future).
</deferred>
