---
phase: "04-extended-decode-transport"
plan: "01"
subsystem: "domain-models + consumer-adapter"
tags: [kafka, domain, pydantic, raw-key, tdd, additive]
dependency_graph:
  requires: []
  provides:
    - KafkaMessage.raw_key (bytes | None)
    - KafkaMessage.key_decoded (dict | None)
    - KafkaMessage.schema_id (dict | None)
    - confluent_consumer raw_key threading
  affects:
    - src/kafka_mcp/domain/models.py
    - src/kafka_mcp/adapters/outbound/confluent_consumer.py
    - tests/test_domain.py
    - tests/test_adapters.py
tech_stack:
  added: []
  patterns:
    - Additive pydantic field with None default (no Field wrapper)
    - TDD RED/GREEN cycle per task
    - Byte threading only in adapter (no decode logic)
key_files:
  created: []
  modified:
    - src/kafka_mcp/domain/models.py
    - src/kafka_mcp/adapters/outbound/confluent_consumer.py
    - tests/test_domain.py
    - tests/test_adapters.py
decisions:
  - raw_key/key_decoded/schema_id placed between raw and Evidence block (additive, not in Evidence section)
  - raw_key=raw_key in both fetch_messages() and fetch_message() — no framing logic in adapter
  - key: str UTF-8 decode path preserved as-is (backward compat contract)
metrics:
  duration: "~10 min"
  completed: "2026-06-08"
  tasks_completed: 2
  files_modified: 4
---

# Phase 04 Plan 01: Domain Model Extension & raw_key Threading Summary

**One-liner:** Added three optional fields (raw_key/key_decoded/schema_id) to KafkaMessage and
threaded raw key bytes through both confluent_consumer fetch paths — data-layer foundation for
KEY-01 key decode and KEY-02 schema_id surfacing in Plans 02-03.

## What Was Built

### Task 1 — KafkaMessage field extension (TDD)
- Added `raw_key: bytes | None = None`, `key_decoded: dict[str, Any] | None = None`,
  `schema_id: dict[str, int | None] | None = None` to `KafkaMessage` in `domain/models.py`
- Fields inserted between `raw: bytes` and the Evidence block — additive only
- `model_dump()` includes all three keys automatically
- 11 new test cases in `TestKafkaMessageNewFields` covering defaults, stored values,
  model_dump presence, and backward compat

### Task 2 — raw_key threading (TDD)
- `confluent_consumer.fetch_messages()`: added `raw_key=raw_key` to `KafkaMessage(...)` call
- `confluent_consumer.fetch_message()`: added `raw_key=raw_key` to `KafkaMessage(...)` call
- The local variable `raw_key = msg.key()` was already assigned in both paths; change is
  a single keyword argument addition per path, no logic changes
- 5 new test cases across `TestFetchMessagesRawKey` and `TestFetchMessageRawKey`

## Test Results

- `tests/test_domain.py`: 49 passed (38 baseline + 11 new)
- `tests/test_adapters.py`: 71 passed (66 baseline + 5 new)
- Full suite: 221 passed (205 baseline + 16 new) — no regressions
- Hexagonal boundary test: passed

## Commits

| Hash | Type | Description |
|------|------|-------------|
| a4e9df5 | test | Add failing tests for KafkaMessage raw_key/key_decoded/schema_id (RED) |
| 3251670 | feat | Add raw_key, key_decoded, schema_id fields to KafkaMessage (GREEN) |
| c53c911 | test | Add failing tests for raw_key threading in confluent_consumer (RED) |
| 3c2d4e7 | feat | Thread raw_key bytes through fetch_messages() and fetch_message() (GREEN) |

## Deviations from Plan

None — plan executed exactly as written.

## Known Stubs

None. No stub fields or placeholder values introduced. All three new fields default to `None`
which is the correct domain value (not a placeholder) — they will be populated by Plan 02
service layer logic.

## Threat Flags

No new threat surface identified. Fields confirmed against plan threat model:
- T-04-01 (Tampering / raw_key bytes): accepted as specified — opaque bytes stored, no execution
- T-04-02 (DoS / large raw_key): accepted — mirrors raw: bytes, librdkafka message.max.bytes cap applies
- T-04-SC: no new package installs

## Self-Check: PASSED
