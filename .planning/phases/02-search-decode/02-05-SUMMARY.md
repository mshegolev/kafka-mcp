---
phase: "02-search-decode"
plan: "05"
subsystem: "inbound adapter faces — MCP stdio, FastAPI REST, CLI"
tags: ["mcp-tools", "fastapi-rest", "cli", "search-messages", "get-message", "base64", "tdd", "phase2-complete", "kafka-02", "kafka-03"]
dependency_graph:
  requires:
    - "02-04 (KafkaClient.search_messages/get_message public methods)"
    - "02-01 (KafkaMessage, DecodeError, MessageNotFoundError domain models)"
  provides:
    - "MCP stdio search_messages tool (readOnlyHint=True, base64 raw, ValueError error mapping)"
    - "MCP stdio get_message tool (readOnlyHint=True, ValueError on NotFound/DecodeError)"
    - "FastAPI POST /tools/search_messages (SearchMessagesRequest with limit ge=1 le=10000)"
    - "FastAPI POST /tools/get_message (404 MessageNotFoundError, 422 DecodeError)"
    - "CLI search-messages subcommand (--key/--key-field/--topics/--time-from/--time-to/--limit/--json)"
    - "CLI get-message subcommand (positional topic/partition/offset, --json, SystemExit(1/2))"
    - "_serialize_message() helpers in mcp_stdio.py and rest_api.py"
    - "_serialize_message_for_cli() helper in cli.py"
    - "Phase 2 SC-5 complete: all four inbound faces expose search_messages/get_message"
  affects:
    - "src/kafka_mcp/adapters/inbound/mcp_stdio.py"
    - "src/kafka_mcp/adapters/inbound/rest_api.py"
    - "src/kafka_mcp/adapters/inbound/cli.py"
    - "tests/test_inbound.py"
tech_stack:
  added: []
  patterns:
    - "_serialize_message() helper: model_dump() + base64.b64encode(raw).decode('ascii') + datetime.isoformat()"
    - "SearchMessagesRequest.limit: Field(ge=1, le=10000) for T-02-05-A DoS mitigation"
    - "CLI datetime parsing: fromisoformat() + UTC-aware fallback (replace tzinfo=UTC)"
    - "CLI topics: comma-split + strip on --topics string argument"
    - "MCP error mapping: MessageNotFoundError/DecodeError → ValueError (FastMCP requirement)"
    - "REST error mapping: MessageNotFoundError → HTTP 404; DecodeError → HTTP 422"
key_files:
  created: []
  modified:
    - "src/kafka_mcp/adapters/inbound/mcp_stdio.py"
    - "src/kafka_mcp/adapters/inbound/rest_api.py"
    - "src/kafka_mcp/adapters/inbound/cli.py"
    - "tests/test_inbound.py"
decisions:
  - "base64 encoding happens in _serialize_message() helper in each face adapter (not in KafkaMessage model)"
  - "MCP raises ValueError (not domain error) — FastMCP requires ValueError for tool errors"
  - "SearchMessagesRequest.limit uses pydantic Field(ge=1, le=10000) per T-02-05-A threat mitigation"
  - "CLI --topics accepts comma-separated string (argparse type=str), split/stripped in runner"
  - "CLI get-message exits 1 for MessageNotFoundError and 2 for DecodeError (distinct exit codes)"
  - "datetime.fromisoformat() used in both MCP and CLI for T-02-05-D safe ISO8601 parsing"
metrics:
  duration: "~18 minutes"
  completed: "2026-06-05T20:50:00Z"
  tasks: 2
  files: 4
---

# Phase 2 Plan 5: search_messages / get_message — All Inbound Faces Summary

**One-liner:** search_messages and get_message wired into MCP stdio (readOnlyHint=True, ValueError mapping), FastAPI REST (404/422 structured errors, limit=Field(ge=1,le=10000)), and CLI (human table + --json, SystemExit(1/2)) with base64 raw serialization in all faces — completing Phase 2 SC-5.

## Tasks Completed

| Task | Name | RED Commit | GREEN Commit | Tests Added |
|------|------|-----------|-------------|-------------|
| 1 | MCP stdio + FastAPI REST — search_messages and get_message | b012b9f | d7fe992 | 11 |
| 2 | CLI — search-messages and get-message subcommands | 4c0c71d | f9770cf | 9 |

**Total new tests:** 20 (177 total after plan; up from 157 baseline)
**Full suite:** 177 tests pass

## What Was Built

### Task 1: MCP stdio + FastAPI REST

**`mcp_stdio.py` — search_messages tool:**
- `@app.tool(name="search_messages", annotations=_READ_ONLY)`
- Parameters: `key: str`, `key_field: str | None`, `topics: list[str] | None`, `time_from: str | None` (ISO8601), `time_to: str | None` (ISO8601), `limit: int = 500`
- `datetime.fromisoformat()` parsing for time_from/time_to (T-02-05-D safe)
- Returns `[_serialize_message(m) for m in results]`

**`mcp_stdio.py` — get_message tool:**
- `@app.tool(name="get_message", annotations=_READ_ONLY)`
- `MessageNotFoundError` → `ValueError("Message not found: ...")`
- `DecodeError` → `ValueError("Decode failed: ...")`

**`mcp_stdio.py` — `_serialize_message(msg)` helper:**
- `model_dump()` then `raw = base64.b64encode(msg.raw).decode("ascii")`
- `timestamp_utc` converted to ISO-8601 string if datetime instance

**`rest_api.py` — `SearchMessagesRequest` model:**
- `limit: int = Field(default=500, ge=1, le=10000)` — T-02-05-A DoS mitigation
- `time_from/time_to: datetime | None` — pydantic auto-parses ISO8601 (T-02-05-D)

**`rest_api.py` — POST /tools/search_messages:**
- Returns `{"result": [_serialize_message(m) for m in results]}`

**`rest_api.py` — POST /tools/get_message:**
- `MessageNotFoundError` → `HTTPException(404, detail={"error": "MessageNotFoundError", "topic", "partition", "offset"})`
- `DecodeError` → `HTTPException(422, detail={"error": "DecodeError", "topic", "partition", "offset", "reason"})`

**`rest_api.py` — `_serialize_message(msg)` helper:**
- Same base64 raw + ISO-8601 datetime logic as MCP version

### Task 2: CLI

**`cli.py` — search-messages subcommand:**
- `--key` (required), `--key-field`, `--topics` (comma-separated string), `--time-from`, `--time-to`, `--limit`, `--json`
- `run_search_messages()`: fromisoformat() + UTC-aware fallback; comma-split/strip topics; human table columns: Timestamp | Topic | Partition | Offset | Key | Value (truncated 80 chars)

**`cli.py` — get-message subcommand:**
- Positional: `topic`, `partition` (int), `offset` (int); `--json`
- `run_get_message()`: `MessageNotFoundError` → `sys.exit(1)` + stderr; `DecodeError` → `sys.exit(2)` + stderr with reason
- Human output: topic/partition/offset/key/timestamp_utc/headers/evidence keys/value

**`cli.py` — `_serialize_message_for_cli(msg)` helper:**
- Same base64 raw logic; orjson_dumps handles the serialized dict

## Phase 2 SC-5 Verification

All four inbound faces expose search_messages and get_message:

| Face | search_messages | get_message |
|------|----------------|-------------|
| lib (KafkaClient) | PASS (plan 02-04) | PASS (plan 02-04) |
| MCP stdio | PASS (this plan) | PASS (this plan) |
| FastAPI REST | PASS (this plan) | PASS (this plan) |
| CLI | PASS (this plan) | PASS (this plan) |

## Deviations from Plan

None — plan executed exactly as written. All 20 new tests follow the TDD RED/GREEN pattern specified.

## Known Stubs

None — all serialization helpers fully implemented. No placeholder values, no TODO/FIXME, no empty data sources. All four inbound faces wire directly to KafkaClient.search_messages / get_message.

## Threat Surface Scan

All STRIDE threats from the plan's threat register mitigated:

| Threat ID | Status | Implementation |
|-----------|--------|----------------|
| T-02-05-A | Mitigated | `SearchMessagesRequest.limit: Field(ge=1, le=10000)`; invalid values → 422 from pydantic before reaching KafkaClient |
| T-02-05-B | Accepted | `DecodeError.reason` in 422 — plan 02-02 SR adapter does not put credentials in reason strings |
| T-02-05-C | Mitigated | All three serialized faces base64-encode raw via `_serialize_message()` helpers; in-process KafkaMessage retains bytes |
| T-02-05-D | Mitigated | REST: pydantic datetime fields (auto-parse ISO8601); MCP: `datetime.fromisoformat()` (stdlib safe); CLI: `fromisoformat()` + UTC-aware fallback |
| T-02-05-E | Accepted | CLI topics comma-split — invalid names return TopicNotFoundError from broker; no injection surface |
| T-02-05-F | Accepted | CLI stderr reason from DecodeError — plan 02-02 SR adapter does not put credentials in reason |
| T-02-05-SC | Accepted | No new packages; base64 and datetime are Python stdlib |

No new threat surface introduced beyond what was planned.

## Full Phase 2 Verification

```
pytest tests/ -v → 177 passed
grep -c "search_messages" src/kafka_mcp/adapters/inbound/mcp_stdio.py → 5
grep -c "search_messages" src/kafka_mcp/adapters/inbound/rest_api.py → 6
grep -c "search-messages" src/kafka_mcp/adapters/inbound/cli.py → 6
grep -c "readOnlyHint" src/kafka_mcp/adapters/inbound/mcp_stdio.py → 4
Hexagonal boundary: CLEAN (no I/O imports in domain/ or ports/)
Phase 2 SC-1 through SC-4: all pass in test_lib.py
```

## TDD Gate Compliance

| Task | RED commit | GREEN commit | Compliant |
|------|-----------|-------------|-----------|
| Task 1 (MCP + REST) | b012b9f | d7fe992 | Yes |
| Task 2 (CLI) | 4c0c71d | f9770cf | Yes |

## Self-Check: PASSED

Files exist:
- FOUND: src/kafka_mcp/adapters/inbound/mcp_stdio.py (contains search_messages, get_message, _serialize_message)
- FOUND: src/kafka_mcp/adapters/inbound/rest_api.py (contains search_messages, get_message, SearchMessagesRequest, GetMessageRequest)
- FOUND: src/kafka_mcp/adapters/inbound/cli.py (contains search-messages, get-message, run_search_messages, run_get_message)
- FOUND: tests/test_inbound.py (37 tests, 20 new)

Commits exist: b012b9f, d7fe992, 4c0c71d, f9770cf

Tests: 177 passed, 0 failed
- 20 new Phase 2 plan 05 tests
- All 157 prior tests still pass
- Phase 2 SC-5 verified: all four faces expose search_messages and get_message
- Hexagonal boundary clean
