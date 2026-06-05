---
phase: "02-search-decode"
plan: "01"
subsystem: "domain + ports"
tags: ["domain-model", "ports", "tdd", "hexagonal", "pydantic-v2"]
dependency_graph:
  requires: []
  provides:
    - "KafkaMessage pydantic v2 model with Evidence fields"
    - "DecodeError domain exception with topic/partition/offset/reason"
    - "MessageNotFoundError domain exception with topic/partition/offset"
    - "ConsumerPort.fetch_messages() and ConsumerPort.fetch_message() signatures"
    - "SchemaRegistryPort.decode() signature"
  affects:
    - "src/kafka_mcp/domain/models.py"
    - "src/kafka_mcp/domain/errors.py"
    - "src/kafka_mcp/ports/consumer.py"
    - "src/kafka_mcp/ports/schema_registry.py"
tech_stack:
  added: []
  patterns:
    - "pydantic v2 BaseModel with Field(default_factory) for mutable defaults"
    - "runtime_checkable Protocol extension: add methods, update all mock classes"
    - "TDD RED->GREEN per task with individual commits"
key_files:
  created: []
  modified:
    - "src/kafka_mcp/domain/models.py"
    - "src/kafka_mcp/domain/errors.py"
    - "src/kafka_mcp/ports/consumer.py"
    - "src/kafka_mcp/ports/schema_registry.py"
    - "src/kafka_mcp/adapters/outbound/confluent_consumer.py"
    - "src/kafka_mcp/adapters/outbound/schema_registry_http.py"
    - "tests/test_domain.py"
decisions:
  - "KafkaMessage.keys default_factory produces {order_id,msisdn,customer_id,product_id:None}"
  - "DecodeError and MessageNotFoundError inherit Exception (not ValueError) for clean catch hierarchy"
  - "Adapter stubs (NotImplementedError) added in 02-01 so Protocol isinstance checks pass; real impl in 02-02/02-03"
  - "raw: bytes stored as-is in domain model; base64 encoding is the face layer's responsibility"
  - "Protocol extension broke existing local mocks: updated them in-place (Rule 1 auto-fix)"
metrics:
  duration: "~29 minutes"
  completed: "2026-06-05T18:02:11Z"
  tasks: 2
  files: 7
---

# Phase 2 Plan 1: Domain Contracts + Port Extensions Summary

**One-liner:** KafkaMessage pydantic v2 model with Evidence fields (source, event_type, keys) + DecodeError/MessageNotFoundError typed errors + ConsumerPort/SchemaRegistryPort extended with Phase 2 method signatures.

## Tasks Completed

| Task | Name | RED Commit | GREEN Commit | Tests Added |
|------|------|-----------|-------------|-------------|
| 1 | KafkaMessage + DecodeError + MessageNotFoundError | 6c799dc | 90b7f69 | 13 |
| 2 | Extend ConsumerPort + SchemaRegistryPort | c083eb6 | 804f574 | 5 |

**Total new tests:** 18 (33 domain tests after Task 1; 38 after Task 2)
**Full suite:** 115 tests pass (up from 97 baseline)

## What Was Built

### Task 1: KafkaMessage + Error Types

`KafkaMessage(BaseModel)` in `domain/models.py`:
- Wire fields: `topic`, `partition`, `offset`, `key: str | None`, `headers: dict[str,str]`, `value: dict|None`, `timestamp_utc: datetime`, `raw: bytes`
- Evidence surface: `source="kafka"` (default), `event_type="kafka_message"` (default), `keys: dict[str,str|None]` defaulting to `{order_id,msisdn,customer_id,product_id: None}`
- `value` typed as `dict[str, Any] | None` to handle heterogeneous payloads

`DecodeError(Exception)` in `domain/errors.py`:
- Attributes: `topic`, `partition`, `offset`, `reason: str`
- Message: `Decode failed for {topic}[{partition}]@{offset}: {reason}`

`MessageNotFoundError(Exception)` in `domain/errors.py`:
- Attributes: `topic`, `partition`, `offset`
- Message: `No message at {topic}[{partition}]@{offset}`

### Task 2: Port Extensions

`ConsumerPort` gains:
- `fetch_messages(topic, partition, start_offset, stop_offset, time_to, limit) -> list[KafkaMessage]`
- `fetch_message(topic, partition, offset) -> KafkaMessage` (raises MessageNotFoundError)

`SchemaRegistryPort` gains:
- `decode(raw: bytes) -> dict | None`

`ConfluentConsumerAdapter` and `SchemaRegistryHttpAdapter` both received placeholder
`NotImplementedError` stubs so the Protocol `isinstance` checks pass. Real
implementations come in plans 02-02 and 02-03 respectively.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Updated pre-existing test mock classes after Protocol extension**
- **Found during:** Task 2 GREEN phase
- **Issue:** Adding `fetch_messages`, `fetch_message` to `ConsumerPort` and `decode` to
  `SchemaRegistryPort` caused the local `MockConsumer` and `MockRegistry` inside
  `TestConsumerPort` and `TestSchemaRegistryPort` in `tests/test_domain.py` to fail
  `isinstance` checks — same pattern as Phase 1 deviation #3 (get_partition_ids).
- **Fix:** Updated both local mock classes to include stub implementations of the new
  Protocol methods. Also added stub implementations to `ConfluentConsumerAdapter` and
  `SchemaRegistryHttpAdapter` to pass the adapter `isinstance` tests in `test_adapters.py`.
- **Files modified:** `tests/test_domain.py`, `src/kafka_mcp/adapters/outbound/confluent_consumer.py`, `src/kafka_mcp/adapters/outbound/schema_registry_http.py`
- **Commit:** 804f574

## Known Stubs

| File | Method | Reason |
|------|--------|--------|
| `src/kafka_mcp/adapters/outbound/confluent_consumer.py` | `fetch_messages` | Full implementation in plan 02-02 |
| `src/kafka_mcp/adapters/outbound/confluent_consumer.py` | `fetch_message` | Full implementation in plan 02-02 |
| `src/kafka_mcp/adapters/outbound/schema_registry_http.py` | `decode` | Full implementation in plan 02-03 |

These stubs are intentional. Their purpose is to satisfy Protocol isinstance checks
so adapter tests pass while the real logic is deferred to the plans that own it.

## Threat Surface Scan

No new threat surface introduced. Changes are confined to:
- `domain/` and `ports/`: pure data types and protocol signatures, zero I/O
- Adapter stubs: raise NotImplementedError, no network calls, no credential exposure

Hexagonal boundary verification (must return empty):
```
grep -rn "import confluent_kafka|import fastavro|import avro|import google.protobuf|import httpx" \
  src/kafka_mcp/domain/ src/kafka_mcp/ports/
```
Result: CLEAN (no output)

## TDD Gate Compliance

| Plan | RED commit | GREEN commit | Compliant |
|------|-----------|-------------|-----------|
| Task 1 | 6c799dc (test(02-01): add failing tests for KafkaMessage...) | 90b7f69 (feat(02-01): add KafkaMessage...) | Yes |
| Task 2 | c083eb6 (test(02-01): add failing tests for extended port protocols) | 804f574 (feat(02-01): extend ConsumerPort...) | Yes |

## Self-Check: PASSED

Files exist:
- FOUND: src/kafka_mcp/domain/models.py (contains `class KafkaMessage`)
- FOUND: src/kafka_mcp/domain/errors.py (contains `class DecodeError`, `class MessageNotFoundError`)
- FOUND: src/kafka_mcp/ports/consumer.py (contains `fetch_messages`)
- FOUND: src/kafka_mcp/ports/schema_registry.py (contains `decode`)

Commits exist: 6c799dc, 90b7f69, c083eb6, 804f574
Tests: 115 passed, 0 failed
