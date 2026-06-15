---
phase: 05-consumer-lag-tooling
plan: 02
subsystem: adapters-inbound + server + tests
tags: [consumer-lag, 4-face-symmetry, mcp-tool, fastapi-route, cli-subcommand, read-only]
dependency_graph:
  requires: [LagRecord, KafkaClient.consumer_group_lag]
  provides: [MCP consumer_group_lag tool, POST /tools/consumer_group_lag, CLI consumer-group-lag, HTTP MCP consumer_group_lag tool]
  affects: []
tech_stack:
  added: []
  patterns: [_serialize_lag_record helper, ConsumerGroupLagRequest pydantic model, 4-face symmetry test]
key_files:
  created: []
  modified:
    - src/kafka_mcp/adapters/inbound/mcp_stdio.py
    - src/kafka_mcp/adapters/inbound/rest_api.py
    - src/kafka_mcp/adapters/inbound/cli.py
    - src/kafka_mcp/server.py
    - tests/test_inbound.py
decisions:
  - "_serialize_lag_record duplicated per adapter (not shared module) — follows established _serialize_message pattern where each face owns its serializer"
  - "FastMCP call_tool returns (content_list, is_error) tuple; MCP tests adapted to handle tuple unpacking with isinstance guard"
metrics:
  duration: "543s (~9m)"
  completed: "2026-06-15T18:16:37Z"
  tasks: 2/2
  tests_added: 21
  tests_total: 298
  tests_passed: 298
---

# Phase 5 Plan 02: Wire consumer_group_lag Across All 4 Inbound Faces + Tests Summary

**One-liner:** consumer_group_lag exposed via MCP stdio, FastAPI POST, HTTP MCP, and CLI with readOnlyHint=True, ConsumerGroupLagRequest validation, and 21 new 4-face tests including symmetry verification

## Tasks Completed

| Task | Name | Commit | Key Changes |
|------|------|--------|-------------|
| 1 | MCP stdio + FastAPI + HTTP MCP face adapters | 284ca37 | `_serialize_lag_record` + `consumer_group_lag` tool in mcp_stdio.py, `ConsumerGroupLagRequest` + `_serialize_lag_record` + POST route + HTTP MCP tool in rest_api.py |
| 2 | CLI subcommand + server.py dispatch + full 4-face test suite | 284ca37 | `_serialize_lag_record_for_cli` + `run_consumer_group_lag` + parser + dispatch in cli.py, `consumer-group-lag` in server.py dispatch set, 21 new tests in test_inbound.py |

## Implementation Details

### MCP stdio (`src/kafka_mcp/adapters/inbound/mcp_stdio.py`)
- Added `LagRecord` import from domain models
- Added `_serialize_lag_record` helper: `model_dump()` + `timestamp_utc` ISO-8601 conversion (no bytes/base64 needed — LagRecord has no raw fields)
- Registered `consumer_group_lag` tool with `annotations=_READ_ONLY` (readOnlyHint=True)
- Tool signature: `consumer_group_lag(group: str, topics: list[str] | None = None) -> list[dict]`
- Updated module docstring and `create_mcp_server` docstring to list 5 tools

### FastAPI REST (`src/kafka_mcp/adapters/inbound/rest_api.py`)
- Added `LagRecord` import from domain models
- Added `ConsumerGroupLagRequest(BaseModel)` with `group: str` and `topics: list[str] | None = None` (T-05-05 Pydantic validation)
- Added `_serialize_lag_record` helper (identical to mcp_stdio version)
- Added `POST /tools/consumer_group_lag` route returning `{"result": [...]}`
- Registered `consumer_group_lag` tool in `_create_http_mcp_server` with `annotations=_READ_ONLY`
- Updated module docstring and `create_app` docstring to list 5 routes

### CLI (`src/kafka_mcp/adapters/inbound/cli.py`)
- Added `LagRecord` import from domain models
- Added parser registration: `consumer-group-lag` subcommand with `--group` (required), `--topics` (optional comma-separated), `--json` (optional)
- Added `_serialize_lag_record_for_cli` helper (identical pattern to other faces)
- Added `run_consumer_group_lag`: comma-separated topics parsing, table output (Group/Topic/Partition/Current/End/Lag columns), JSON output, empty-group message
- Added dispatch branch in `main()` after get-message
- Updated module docstring to list 5 subcommands

### server.py (`src/kafka_mcp/server.py`)
- Added `"consumer-group-lag"` to `_cli_subcommands` set (line 62–68)
- CLI dispatch now routes `consumer-group-lag` to `cli.main()` instead of HTTP server

### Tests (`tests/test_inbound.py`)
- Added `LagRecord` import
- Added `_SAMPLE_LAG_TS` and `_SAMPLE_LAG_RECORD` fixtures
- Added `MockKafkaClient.consumer_group_lag`: returns 2 LagRecords (orders p0 lag=50, payments p0 lag=170) for "my-group", supports topics filter, returns [] for unknown groups
- Updated `TestServerCliDispatch` parametrize list to include "consumer-group-lag"
- **TestFastapiConsumerGroupLag** (5 tests): 200 response, topics filter, empty group, timestamp_utc is string, route exists
- **TestMcpConsumerGroupLag** (4 tests): tool registered, readOnlyHint=True, returns lag records, empty group returns empty
- **TestCliConsumerGroupLag** (6 tests): parse_args, parse_args_with_topics, table output, JSON output, empty group table, topics filter
- **TestServerConsumerGroupLagDispatch** (1 test): consumer-group-lag routes to CLI runner not uvicorn
- **TestHttpMcpConsumerGroupLag** (2 tests): tool registered, readOnlyHint=True
- **TestConsumerGroupLagFourFaceSymmetry** (2 tests): lib face returns list[LagRecord], all 4 faces produce identical field set (group, topic, partition, current_offset, end_offset, lag, timestamp_utc, source, event_type)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed MCP tool test to handle FastMCP call_tool return shape**
- **Found during:** Task 2 (test_returns_lag_records)
- **Issue:** Plan's test code assumed `result[0].text` contains JSON-serialized list, but FastMCP `call_tool` returns `(content_list, is_error)` tuple, and list[dict] return creates separate TextContent per dict item (not a single JSON array)
- **Fix:** Added `isinstance(result, tuple)` guard to extract content list; parse first TextContent item as single dict instead of list; empty list returns empty content list (len==0)
- **Files modified:** `tests/test_inbound.py` (3 test methods adjusted)
- **Commit:** 284ca37

## Verification Results

```
✅ _serialize_lag_record exists in mcp_stdio.py, rest_api.py, cli.py (timestamp_utc → ISO-8601)
✅ MCP stdio consumer_group_lag tool registered with readOnlyHint=True
✅ HTTP MCP consumer_group_lag tool registered with readOnlyHint=True
✅ FastAPI POST /tools/consumer_group_lag route exists, accepts ConsumerGroupLagRequest
✅ CLI consumer-group-lag subcommand parses --group, --topics, --json
✅ server.py routes consumer-group-lag to CLI runner (not HTTP server)
✅ MockKafkaClient.consumer_group_lag returns 2 records for "my-group"
✅ 4-face symmetry: all faces return identical field set (9 fields)
✅ pytest -k consumer_group_lag: 13 passed (7 adapter + 6 inbound)
✅ Full test suite: 298 passed, 0 failed
✅ Ruff: All checks passed
```

## Known Stubs

None — all code is fully wired and functional.

## Self-Check: PASSED

- [x] `src/kafka_mcp/adapters/inbound/mcp_stdio.py` — FOUND
- [x] `src/kafka_mcp/adapters/inbound/rest_api.py` — FOUND
- [x] `src/kafka_mcp/adapters/inbound/cli.py` — FOUND
- [x] `src/kafka_mcp/server.py` — FOUND
- [x] `tests/test_inbound.py` — FOUND
- [x] Commit `284ca37` — FOUND
