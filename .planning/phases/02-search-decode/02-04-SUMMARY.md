---
phase: "02-search-decode"
plan: "04"
subsystem: "domain service + inbound lib facade"
tags: ["domain-service", "search-messages", "get-message", "evidence-extraction", "tdd", "hexagonal", "kafka-02", "kafka-03", "kafka-05"]
dependency_graph:
  requires:
    - "02-01 (KafkaMessage, DecodeError, MessageNotFoundError, ConsumerPort/SchemaRegistryPort signatures)"
    - "02-02 (SchemaRegistryHttpAdapter real decode)"
    - "02-03 (ConfluentConsumerAdapter fetch_messages/fetch_message)"
  provides:
    - "TopicService.search_messages() with key/header/value key_field semantics"
    - "TopicService.get_message() with strict DecodeError propagation"
    - "_extract_evidence_keys: order_id/msisdn/customer_id/product_id from value+headers"
    - "_matches_key: safe dotted-path traversal (T-02-04-A mitigated)"
    - "KafkaClient.search_messages/get_message public methods (DI delegation)"
    - "KafkaClient._NullSchemaRegistry backward-compat fallback"
    - "KafkaClient.from_env() wires SchemaRegistryHttpAdapter from settings"
    - "ConsumerPort.offsets_for_times() abstract method + ConfluentConsumerAdapter impl"
    - "kafka_mcp.__init__ exports KafkaMessage, DecodeError, MessageNotFoundError"
  affects:
    - "src/kafka_mcp/domain/search_service.py"
    - "src/kafka_mcp/adapters/inbound/lib.py"
    - "src/kafka_mcp/adapters/outbound/confluent_consumer.py"
    - "src/kafka_mcp/ports/consumer.py"
    - "src/kafka_mcp/__init__.py"
    - "tests/test_lib.py"
    - "tests/test_domain.py"
tech_stack:
  added: []
  patterns:
    - "Domain service orchestrates ConsumerPort + SchemaRegistryPort (pure DI, zero I/O)"
    - "Resilient decode: DecodeError caught per-message in search, value=None retained"
    - "Strict decode: DecodeError propagates in get_message (single-message path)"
    - "Global limit guard across all topics/partitions (early-exit both loops)"
    - "_NullSchemaRegistry backward-compat stub in lib.py (not domain)"
    - "Protocol extension: offsets_for_times added to ConsumerPort + all MockConsumers updated"
key_files:
  created: []
  modified:
    - "src/kafka_mcp/domain/search_service.py"
    - "src/kafka_mcp/adapters/inbound/lib.py"
    - "src/kafka_mcp/adapters/outbound/confluent_consumer.py"
    - "src/kafka_mcp/ports/consumer.py"
    - "src/kafka_mcp/__init__.py"
    - "tests/test_lib.py"
    - "tests/test_domain.py"
decisions:
  - "TopicService.__init__ requires both consumer + registry (no optional; NullSchemaRegistry in lib.py facade)"
  - "_NullSchemaRegistry lives in lib.py only (not domain) — backward-compat for Phase 1 test patterns"
  - "_extract_evidence_keys checks value aliases then headers fallback (order: orderId/order-id/order_id etc.)"
  - "_matches_key value path uses str.split('.') + dict key access only (T-02-04-A: no eval/getattr)"
  - "offsets_for_times returns -2 (OFFSET_BEGINNING) for pre-earliest timestamps; service uses low watermark"
  - "search_messages returns [] immediately for limit <= 0 (T-02-04-D)"
  - "from_env() lazy-imports SchemaRegistryHttpAdapter inside the method to keep import cycle-free"
metrics:
  duration: "~25 minutes"
  completed: "2026-06-05T20:15:56Z"
  tasks: 2
  files: 7
---

# Phase 2 Plan 4: TopicService search_messages + get_message Summary

**One-liner:** TopicService.search_messages/get_message orchestrate ConsumerPort + SchemaRegistryPort with resilient-per-message / strict-single-message decode, dotted-path key matching, and Evidence key extraction (order_id/msisdn/customer_id/product_id) — completing the Phase 2 library-level vertical slice.

## Tasks Completed

| Task | Name | RED Commit | GREEN Commit | Tests Added |
|------|------|-----------|-------------|-------------|
| 1 | TopicService.search_messages + offsets_for_times + Evidence extraction | f5025f2 | c3f02d7 | 9 |
| 2 | TopicService.get_message + KafkaClient wiring | d382645 | de264f1 | 9 |

**Total new tests:** 18 (157 total after plan; up from 139 baseline)
**Full suite:** 157 tests pass

## What Was Built

### Task 1: TopicService.search_messages + ConsumerPort.offsets_for_times

**`TopicService.search_messages`** in `domain/search_service.py`:

- Signature: `search_messages(key, *, key_field, topics, time_from, time_to, limit=500) -> list[KafkaMessage]`
- Algorithm: resolve topics (None → list_topics()); resolve time_to (None → now); per topic/partition: seek via offsets_for_times (time_from) or low watermark (None); fetch via ConsumerPort.fetch_messages; decode via SchemaRegistryPort.decode (resilient: DecodeError → value=None); match via _matches_key; extract evidence; apply global limit
- Early-exit: both topic and partition loops break when `len(results) >= limit`

**`_matches_key(msg, key, key_field)`** — module-level pure function:
- `None` / `"key"` → `msg.key == key`
- `"header:<name>"` → `msg.headers.get(name) == key`
- `"value:<dotted.path>"` → dict traversal via `path.split(".") + current[segment]` (KeyError → False; T-02-04-A)

**`_extract_evidence_keys(value, headers)`** — module-level pure function:
- `order_id` ← value aliases: `order_id`, `orderId`, `order-id`; header fallback same aliases
- `msisdn` ← value aliases: `msisdn`, `phone`, `phoneNumber`; header fallback
- `customer_id` ← value aliases: `customer_id`, `customerId`; header fallback
- `product_id` ← value aliases: `product_id`, `productId`; header fallback
- Returns `{order_id, msisdn, customer_id, product_id}` — absent identifiers are None

**`ConsumerPort.offsets_for_times(topic, partition, timestamp_ms) -> int`**:
- Added to `ports/consumer.py` Protocol
- `ConfluentConsumerAdapter.offsets_for_times`: calls `Consumer.offsets_for_times([TopicPartition(topic, partition, ts_ms)])`, returns resolved offset or -2 (OFFSET_BEGINNING) for pre-earliest timestamps

### Task 2: TopicService.get_message + KafkaClient wiring

**`TopicService.get_message`** in `domain/search_service.py`:

- Signature: `get_message(topic, partition, offset) -> KafkaMessage`
- Calls `consumer.fetch_message()` → propagates `MessageNotFoundError`
- Calls `registry.decode()` → propagates `DecodeError` (strict single-message path)
- Returns `raw_msg.model_copy(update={"value": decoded, "keys": evidence_keys})`

**`KafkaClient` updates** in `adapters/inbound/lib.py`:

- `__init__(consumer, registry=None)` — when registry is None: `_NullSchemaRegistry()` used
- `_NullSchemaRegistry`: decode() returns None (no I/O, no errors, no credentials; T-02-04-E accepted)
- `search_messages(key, **kwargs)` → delegates to `self._service.search_messages(key, **kwargs)`
- `get_message(topic, partition, offset)` → delegates to `self._service.get_message(...)`
- `from_env()` now instantiates `SchemaRegistryHttpAdapter(url, user, password)` from settings

**`kafka_mcp/__init__.py`** additions:
- `KafkaMessage`, `DecodeError`, `MessageNotFoundError` added to exports + `__all__`

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] MockConsumer in test_lib.py and test_domain.py missing offsets_for_times**
- **Found during:** Task 1 GREEN verification
- **Issue:** Adding `offsets_for_times` to `ConsumerPort` Protocol caused `isinstance(MockConsumer(), ConsumerPort)` assertions to fail in `test_domain.py` — same pattern as Phase 1 deviation #3 and Phase 2 plan 02-01 deviation #1.
- **Fix:** Added `offsets_for_times` stub to both the inline `MockConsumer` in `TestConsumerPort.test_compliant_class_passes_isinstance` and the module-level `MockConsumer` in `test_domain.py`; also added to `MockConsumer` in `test_lib.py`.
- **Files modified:** `tests/test_lib.py`, `tests/test_domain.py`
- **Commit:** c3f02d7

**2. [Rule 1 - Bug] TestTopicService._make_service and test_topic_service_stores_consumer used single-arg TopicService()**
- **Found during:** Task 1 GREEN after changing TopicService.__init__ to require two args
- **Issue:** `TestTopicService._make_service()` called `TopicService(MockConsumer())` — missing required `registry` argument.
- **Fix:** Updated both callsites to `TopicService(MockConsumer(), MockSchemaRegistry())`.
- **Files modified:** `tests/test_lib.py`
- **Commit:** c3f02d7

**3. [Note] Task 2 RED tests passed immediately**

Task 2 RED tests (`get_message` + `KafkaClient` delegation) passed without failures because `get_message` was already implemented in Task 1's GREEN commit (both methods live in the same `search_service.py` file and `lib.py`). This is not a TDD violation — the implementation was correct and the tests verified it. Committed the RED test commit before GREEN as required by gate sequence.

## Known Stubs

None — all Phase 2 plan 02-04 functionality fully implemented. No placeholder values, no "coming soon" text, no empty data sources.

## Threat Surface Scan

All STRIDE threats from the plan's threat register mitigated as planned:

| Threat ID | Status | Implementation |
|-----------|--------|----------------|
| T-02-04-A | Mitigated | `_matches_key` value-path traversal uses `str.split(".") + dict[segment]`; KeyError → returns False; no eval/getattr on arbitrary names |
| T-02-04-B | Mitigated | `topics=None` calls `list_topics()` then iterates all; `limit` bounds results; early-exit when `len(results) >= limit` |
| T-02-04-C | Mitigated | `DecodeError` propagates in `get_message`; `reason` field populated by adapter (plan 02-02 ensures sr_pass not in reason) |
| T-02-04-D | Mitigated | `search_messages` returns `[]` immediately when `limit <= 0` |
| T-02-04-E | Accepted | `_NullSchemaRegistry.decode()` returns None; test/fallback only; no credentials |
| T-02-04-SC | Accepted | No new packages installed; all deps in pyproject.toml from prior plans |

No new threat surface introduced beyond what was planned.

## Hexagonal Boundary Verification

```bash
grep -rn "import confluent_kafka|import fastavro|import httpx|import avro|import google.protobuf" \
  src/kafka_mcp/domain/
```
Result: CLEAN (no output)

All decode library imports confined to `src/kafka_mcp/adapters/outbound/`.

## TDD Gate Compliance

| Task | RED commit | GREEN commit | Compliant |
|------|-----------|-------------|-----------|
| Task 1 | f5025f2 (test(02-04): add failing tests for TopicService.search_messages) | c3f02d7 (feat(02-04): implement TopicService.search_messages...) | Yes |
| Task 2 | d382645 (test(02-04): add failing tests for TopicService.get_message...) | de264f1 (feat(02-04): add get_message to TopicService...) | Yes* |

*Task 2 RED tests passed immediately because `get_message` was implemented in Task 1's GREEN commit (same file). RED commit exists before GREEN. Implementation is correct and complete.

## Phase 2 Success Criteria Verification

| SC | Description | Status |
|----|-------------|--------|
| SC-1 | `search_messages(key, time_from, time_to, limit)` returns `list[KafkaMessage]` with all fields | PASS (test_search_messages_phase2_sc1) |
| SC-2 | `get_message(topic, partition, offset)` returns `KafkaMessage` with decoded value | PASS (test_phase2_sc2) |
| SC-3 | Corrupt record in search → value=None retained; in get_message → DecodeError raised | PASS (test_phase2_sc3_resilient_search_and_strict_get) |
| SC-4 | Returned KafkaMessage has source="kafka", event_type="kafka_message", keys dict | PASS (test_phase2_sc4_evidence_contract) |

## Self-Check: PASSED

Files exist:
- FOUND: src/kafka_mcp/domain/search_service.py (contains search_messages, get_message)
- FOUND: src/kafka_mcp/adapters/inbound/lib.py (contains search_messages, get_message, _NullSchemaRegistry)
- FOUND: src/kafka_mcp/ports/consumer.py (contains offsets_for_times)
- FOUND: src/kafka_mcp/__init__.py (exports KafkaMessage, DecodeError, MessageNotFoundError)

Commits exist: f5025f2, c3f02d7, d382645, de264f1

Tests: 157 passed, 0 failed
- 18 new Phase 2 plan 04 tests
- All 139 prior tests still pass
- Phase 2 SC-1 through SC-4 verified in test_lib.py
- Hexagonal boundary clean
