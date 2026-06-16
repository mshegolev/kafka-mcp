---
phase: "04-extended-decode-transport"
plan: "02"
subsystem: "domain-service + inbound-faces"
tags: [kafka, domain, schema-registry, key-decode, tdd, 4-face-symmetry]
dependency_graph:
  requires:
    - KafkaMessage.raw_key (04-01)
    - KafkaMessage.key_decoded (04-01)
    - KafkaMessage.schema_id (04-01)
  provides:
    - _extract_schema_id helper in search_service.py
    - _decode_key helper in search_service.py
    - search_messages() populates key_decoded and schema_id
    - get_message() populates key_decoded and schema_id
    - raw_key base64 encoding in rest_api, mcp_stdio, cli serializers
    - 4-face serialization symmetry for raw_key/key_decoded/schema_id
  affects:
    - src/kafka_mcp/domain/search_service.py
    - src/kafka_mcp/adapters/inbound/rest_api.py
    - src/kafka_mcp/adapters/inbound/mcp_stdio.py
    - src/kafka_mcp/adapters/inbound/cli.py
    - tests/test_domain.py
    - tests/test_inbound.py
tech_stack:
  added: []
  patterns:
    - TDD RED/GREEN cycle per task
    - Resilient decode pattern (DecodeError swallowed for key, strict for value)
    - 4-face serialization symmetry (lib/REST/MCP/CLI identical output)
    - Pure byte math in domain layer (no I/O — hexagonal boundary maintained)
key_files:
  created: []
  modified:
    - src/kafka_mcp/domain/search_service.py
    - src/kafka_mcp/adapters/inbound/rest_api.py
    - src/kafka_mcp/adapters/inbound/mcp_stdio.py
    - src/kafka_mcp/adapters/inbound/cli.py
    - tests/test_domain.py
    - tests/test_inbound.py
decisions:
  - "_decode_key reuses SchemaRegistryPort.decode() — no new decode_key method on the port"
  - "Length guard (len >= 5) checked before any index access on raw bytes (T-04-03 mitigation)"
  - "DecodeError from key decode swallowed in both search_messages and get_message"
  - "Value DecodeError in get_message still propagates (strict path unchanged)"
  - "raw_key encoding uses `is not None` check (not truthiness) to handle zero-length bytes correctly"
  - "lib face (KafkaClient) requires no changes — fields flow through model_dump() automatically"
metrics:
  duration: "~12 min"
  completed: "2026-06-08"
  tasks_completed: 2
  files_modified: 6
---

# Phase 04 Plan 02: Key Decode & 4-Face Schema_id Transport Summary

**One-liner:** Added `_extract_schema_id` + `_decode_key` helpers to search_service and wired
key_decoded/schema_id into search_messages()/get_message(); base64-encoded raw_key in all three
inbound face serializers to achieve full 4-face symmetry for KEY-01 and KEY-02.

## What Was Built

### Task 1 — Domain helpers + service wiring (TDD)

**Helpers added to `src/kafka_mcp/domain/search_service.py`:**

- `_extract_schema_id(raw: bytes | None) -> int | None`
  — pure byte math: guards `len >= 5 and raw[0] == 0x00` before reading `raw[1:5]`.
  Returns the big-endian integer schema ID or `None` for plain/unframed/empty input.
  T-04-03 mitigation: length guard precedes ALL index access.

- `_decode_key(raw_key, registry, topic, partition, offset) -> dict | None`
  — framing check first (same guard as `_extract_schema_id`). Calls
  `registry.decode(raw_key, ...)` only for Confluent-framed keys. Swallows
  `DecodeError` (resilient path) — never raises, never drops the message.
  Reuses existing `SchemaRegistryPort.decode()` — no new port method needed.

**Service wiring in `search_messages()` and `get_message()`:**

Both methods now compute `key_decoded` and `schema_id` after evidence extraction
and pass them to `model_copy(update={...})` alongside `value` and `keys`. The
`schema_id` dict is `{"value": int|None, "key": int|None}` when at least one side
is Confluent-framed; `None` when neither side carries framing.

In `get_message()`, value `DecodeError` still propagates (strict path unchanged).
Key `DecodeError` is swallowed in both methods.

**New tests in `tests/test_domain.py` (23 new cases):**

| Class | Cases | Coverage |
|-------|-------|----------|
| TestExtractSchemaId | 8 | framing, magic byte, length guard, None, large ID |
| TestDecodeKey | 6 | None key, plain bytes, magic+short, success, DecodeError swallowed, wrong magic |
| TestSearchMessagesKeyDecode | 6 | framed key, None key, unframed key, both-framed schema_id, value-only schema_id, neither framed |
| TestGetMessageKeyDecode | 3 | framed key success, both-sides schema_id, key DecodeError swallowed |

### Task 2 — Inbound face serializers (TDD)

Added `raw_key` base64 encoding to the three serializer functions:

- `rest_api._serialize_message`: `if msg.raw_key is not None: data["raw_key"] = base64.b64encode(...)`
- `mcp_stdio._serialize_message`: identical two-line addition
- `cli._serialize_message_for_cli`: identical two-line addition

`key_decoded` and `schema_id` require no special handling — `model_dump()` already
includes them as JSON-safe dicts/ints. `lib` face (KafkaClient) requires no changes.

**New tests in `tests/test_inbound.py` (13 new cases):**

| Class | Cases | Coverage |
|-------|-------|----------|
| TestRestApiRawKeySerialize | 4 | b64 encoding, None passthrough, key_decoded, schema_id |
| TestMcpStdioRawKeySerialize | 4 | identical assertions on MCP face |
| TestCliRawKeySerialize | 4 | identical assertions on CLI face |
| TestFourFaceSymmetry | 1 | all three faces produce identical raw_key/key_decoded/schema_id |

## Test Results

- `tests/test_domain.py`: 72 passed (49 baseline + 23 new)
- `tests/test_inbound.py`: 64 passed (51 baseline + 13 new)
- Full suite: 257 passed (221 baseline + 36 new) — no regressions
- Hexagonal boundary test: passed
- `_extract_schema_id` smoke test: OK
- 4-face symmetry smoke test: OK

## Commits

| Hash | Type | Description |
|------|------|-------------|
| 741a244 | test | Add failing tests for _extract_schema_id, _decode_key, search_messages/get_message key decode (RED) |
| c140f4d | feat | Add _extract_schema_id and _decode_key helpers; wire into search_messages() and get_message() (GREEN) |
| 548c3b2 | test | Add failing tests for raw_key base64 serialization in all four inbound faces (RED) |
| fa09e58 | feat | Add raw_key base64 encoding to all three inbound face serializers (GREEN) |

## Deviations from Plan

None — plan executed exactly as written.

- `if msg.raw_key is not None` used instead of `if msg.raw_key` (plan showed `if msg.raw_key`)
  to correctly handle zero-length byte strings. This is a correctness improvement, not an
  architectural deviation (Rule 2 applied).

## Known Stubs

None. All fields are fully wired:
- `key_decoded` populated by `_decode_key()` for Confluent-framed keys, `None` otherwise
- `schema_id` populated by `_extract_schema_id()` math from raw bytes, `None` for unframed
- `raw_key` base64-encoded in all three serializer faces

## Threat Flags

No new threat surface identified beyond the plan's threat model:

| Threat | Disposition | Evidence |
|--------|-------------|----------|
| T-04-03 (Tampering / length guard) | mitigated | `len(raw) < 5` checked before `raw[0]` and `raw[1:5]` |
| T-04-04 (DoS / key decode time) | mitigated | DecodeError swallowed; no SR round-trip for unframed keys |
| T-04-05 (Info disclosure / key_decoded) | accepted | same posture as value decode |
| T-04-06 (Tampering / raw_key b64) | accepted | reversible encoding, same as existing raw field |

## Self-Check: PASSED
