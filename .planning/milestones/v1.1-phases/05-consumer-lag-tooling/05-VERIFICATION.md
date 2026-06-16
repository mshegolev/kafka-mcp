---
phase: 05-consumer-lag-tooling
verified: 2026-06-16T15:30:00Z
status: passed
score: 4/4
overrides_applied: 0
---

# Phase 5: Consumer Lag Tooling — Verification Report

**Phase Goal:** Users can query per-partition consumer-group lag through any of the four faces; every lag row carries Investigator-Contract Evidence fields and the operation is structurally read-only
**Verified:** 2026-06-16T15:30:00Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | `KafkaClient.consumer_group_lag(group, topics)` returns a list of lag records containing `group`, `topic`, `partition`, `current_offset`, `end_offset`, `lag`, and `timestamp_utc` — no offset commits or writes | ✓ VERIFIED | `LagRecord` model at `models.py:82-103` has all 9 fields (7 data + 2 evidence). `KafkaClient.consumer_group_lag` at `lib.py:221-235` delegates to `self._consumer.consumer_group_lag(group, topics)`. `ConfluentConsumerAdapter.consumer_group_lag` at `confluent_consumer.py:450-519` uses `AdminClient.list_consumer_group_offsets` (read-only, no group join) + `get_watermark_offsets`. `enable.auto.commit=False` at line 73, assign-only pattern (no `subscribe()` or `commit()` calls anywhere in the method). |
| 2 | Lag reachable identically via MCP stdio tool `consumer_group_lag`, FastAPI `POST /tools/consumer_group_lag`, and `kafka-mcp consumer-group-lag` CLI subcommand — all return same schema | ✓ VERIFIED | MCP stdio: `mcp_stdio.py:225-236` registers tool calling `client.consumer_group_lag`. FastAPI: `rest_api.py:427-435` POST route with `ConsumerGroupLagRequest`. HTTP MCP: `rest_api.py:258-269` tool with `_READ_ONLY`. CLI: `cli.py:472-519` `run_consumer_group_lag` with table/JSON output. `server.py:67` includes `"consumer-group-lag"` in `_cli_subcommands`. `test_four_face_field_parity` (line 1701) confirms all 4 faces produce identical 9-field set: `{group, topic, partition, current_offset, end_offset, lag, timestamp_utc, source, event_type}`. |
| 3 | `pytest -k consumer_group_lag` passes against mock adapter suite (all four faces) | ✓ VERIFIED | 28 tests collected and passed: 7 adapter-level (`TestConfluentConsumerAdapterLag`), 5 FastAPI REST, 4 MCP stdio, 6 CLI, 1 server dispatch, 2 HTTP MCP, 2 four-face symmetry, 1 parametrized dispatch. Full suite: 298/298 passed, 0 failures. Ruff clean. |
| 4 | `ToolAnnotations(readOnlyHint=True)` on MCP tool, FastAPI carries same read-only declaration | ✓ VERIFIED | MCP stdio tool: `mcp_stdio.py:231` `annotations=_READ_ONLY` where `_READ_ONLY = ToolAnnotations(readOnlyHint=True)` (line 41). HTTP MCP tool: `rest_api.py:264` same `annotations=_READ_ONLY` (line 49). Verified programmatically: both tools return `readOnlyHint=True` via `list_tools()`. FastAPI's read-only guarantee is structural: the `ConsumerGroupLagRequest` is validated by Pydantic, and the underlying `KafkaClient` delegates to the assign-only consumer adapter with `enable.auto.commit=False`. The HTTP MCP tool mounted at `/mcp` carries `readOnlyHint=True` within the FastAPI app. |

**Score:** 4/4 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/kafka_mcp/domain/models.py` | LagRecord pydantic model with Evidence fields | ✓ VERIFIED | Lines 82-103: `class LagRecord(BaseModel)` with 9 fields, `source="kafka"`, `event_type="consumer_lag"`. No I/O imports — hexagonal boundary preserved. |
| `src/kafka_mcp/ports/consumer.py` | `consumer_group_lag` protocol method | ✓ VERIFIED | Lines 141-158: method stub with `LagRecord` import, full docstring, `list[str] | None` topics param. No broker imports. |
| `src/kafka_mcp/adapters/outbound/confluent_consumer.py` | AdminClient-backed lag implementation | ✓ VERIFIED | Lines 96-110: `AdminClient` created from same broker/SASL config. Lines 450-519: `consumer_group_lag` implementation using `list_consumer_group_offsets` + `get_watermark_offsets`. Handles: no committed offset (current_offset=0), empty/non-existent group (return []), topic filter, TopicNotFoundError on watermark. |
| `src/kafka_mcp/adapters/inbound/lib.py` | `KafkaClient.consumer_group_lag` facade method | ✓ VERIFIED | Lines 221-235: delegates to `self._consumer.consumer_group_lag(group, topics)`. Direct port delegation, not via TopicService. `LagRecord` imported at line 38. |
| `src/kafka_mcp/adapters/inbound/mcp_stdio.py` | `consumer_group_lag` MCP tool | ✓ VERIFIED | Lines 225-236: tool with `annotations=_READ_ONLY`, calls `client.consumer_group_lag`, serializes with `_serialize_lag_record`. `LagRecord` imported line 39. |
| `src/kafka_mcp/adapters/inbound/rest_api.py` | POST `/tools/consumer_group_lag` + HTTP MCP tool | ✓ VERIFIED | Lines 98-106: `ConsumerGroupLagRequest(BaseModel)`. Lines 427-435: POST route returning `{"result": [...]}`. Lines 258-269: HTTP MCP tool with `_READ_ONLY`. `_serialize_lag_record` at lines 138-154. |
| `src/kafka_mcp/adapters/inbound/cli.py` | `consumer-group-lag` CLI subcommand | ✓ VERIFIED | Lines 163-183: parser with `--group` (required), `--topics` (optional), `--json` (optional). Lines 472-519: `run_consumer_group_lag` with table/JSON output. Lines 572-578: dispatch branch in `main()`. |
| `src/kafka_mcp/server.py` | CLI dispatch routing for `consumer-group-lag` | ✓ VERIFIED | Line 67: `"consumer-group-lag"` in `_cli_subcommands` set. Dispatch routes to `cli.main(args)` instead of HTTP server. |
| `tests/test_adapters.py` | Unit tests for adapter-level `consumer_group_lag` | ✓ VERIFIED | Lines 1631-1792: `TestConfluentConsumerAdapterLag` with 7 tests covering lag computation, no-committed-offset, empty group, topic filter, topics=None, evidence fields, UTC timestamp. All mock AdminClient + Consumer. |
| `tests/test_inbound.py` | 4-face tests for `consumer_group_lag` | ✓ VERIFIED | Lines 1399-1749: 21 new tests across 7 test classes (FastAPI 5, MCP stdio 4, CLI 6, server dispatch 1, HTTP MCP 2, symmetry 2, parametrized dispatch 1). `MockKafkaClient.consumer_group_lag` at lines 116-134. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `mcp_stdio.py` | `KafkaClient.consumer_group_lag` | `client.consumer_group_lag(group, topics)` | ✓ WIRED | Line 235: `records = client.consumer_group_lag(group, topics)` |
| `rest_api.py` | `KafkaClient.consumer_group_lag` | `client.consumer_group_lag(req.group, req.topics)` | ✓ WIRED | Line 434: POST route; Line 268: HTTP MCP tool |
| `cli.py` | `KafkaClient.consumer_group_lag` | `client.consumer_group_lag(group, topics_list)` | ✓ WIRED | Line 491: `records = client.consumer_group_lag(group, topics_list)` |
| `lib.py` | `ConsumerPort.consumer_group_lag` | `self._consumer.consumer_group_lag(group, topics)` | ✓ WIRED | Line 235: direct port delegation |
| `confluent_consumer.py` | `confluent_kafka.admin.AdminClient` | `AdminClient.list_consumer_group_offsets` | ✓ WIRED | Line 469: `self._admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])` |
| `server.py` | `cli.py` | `"consumer-group-lag"` in `_cli_subcommands` set | ✓ WIRED | Line 67: dispatches to `cli.main(args)` at line 72 |

### Data-Flow Trace (Level 4)

Not applicable — `consumer_group_lag` is a domain query operation, not a rendering component. Data flows from AdminClient → LagRecord → serialization → response. The 4-face symmetry test (`test_four_face_field_parity`) confirms real data flows through all four faces with correct field sets.

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| All 28 consumer_group_lag tests pass | `python3 -m pytest tests/ -k "consumer_group_lag or ConsumerGroupLag or consumer-group-lag" -x -v` | 28/28 passed in 0.84s | ✓ PASS |
| Full test suite passes | `python3 -m pytest tests/ -x --timeout=30` | 298/298 passed in 1.66s | ✓ PASS |
| LagRecord instantiation with evidence defaults | `python3 -c "from kafka_mcp.domain.models import LagRecord; ..."` | source="kafka", event_type="consumer_lag" | ✓ PASS |
| MCP stdio readOnlyHint=True | `asyncio.run(server.list_tools())` → `tool.annotations.readOnlyHint` | True | ✓ PASS |
| HTTP MCP readOnlyHint=True | `asyncio.run(http_server.list_tools())` → `tool.annotations.readOnlyHint` | True | ✓ PASS |
| Ruff clean | `python3 -m ruff check src/kafka_mcp/ tests/` | All checks passed | ✓ PASS |
| Hexagonal boundary | `grep -rn "^from confluent_kafka" src/kafka_mcp/domain/ src/kafka_mcp/ports/` | No matches | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| LAG-01 | 05-01, 05-02 | Read-only `consumer_group_lag` reports per-partition lag with no writes/commits | ✓ SATISFIED | `enable.auto.commit=False` in adapter `__init__`. `consumer_group_lag` uses `AdminClient.list_consumer_group_offsets` (read-only) + `get_watermark_offsets`. No `subscribe()`, no `commit()` calls. `ToolAnnotations(readOnlyHint=True)` on both MCP tools. |
| LAG-02 | 05-02 | Lag exposed identically across all four faces (lib, MCP stdio, FastAPI, CLI) | ✓ SATISFIED | All four faces call `client.consumer_group_lag(group, topics)` and serialize LagRecords identically. `test_four_face_field_parity` confirms all faces produce the same 9-field schema. CLI dispatched via `server.py`. |
| LAG-03 | 05-01, 05-02 | Lag output carries Investigator-Contract evidence fields | ✓ SATISFIED | `LagRecord` has `source="kafka"` and `event_type="consumer_lag"` defaults. Fields `group`, `topic`, `partition`, `current_offset`, `end_offset`, `lag`, `timestamp_utc` present. `test_consumer_group_lag_evidence_fields` verifies. |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| (none) | — | — | — | No TODO/FIXME/PLACEHOLDER/stub patterns found in any modified file |

### Human Verification Required

No items require human verification. All success criteria are verifiable programmatically, and all have been verified via code inspection and test execution.

### Gaps Summary

No gaps found. All 4 roadmap success criteria are verified. All 3 requirements (LAG-01, LAG-02, LAG-03) are satisfied. All artifacts exist, are substantive, and are wired. 28 consumer_group_lag tests and 298 total tests pass. No anti-patterns detected.

---

_Verified: 2026-06-16T15:30:00Z_
_Verifier: OpenCode (gsd-verifier)_
