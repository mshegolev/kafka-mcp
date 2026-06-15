---
phase: 05-consumer-lag-tooling
plan: 01
subsystem: domain + ports + adapter-outbound + facade
tags: [consumer-lag, lag-record, admin-client, hexagonal]
dependency_graph:
  requires: []
  provides: [LagRecord, ConsumerPort.consumer_group_lag, ConfluentConsumerAdapter.consumer_group_lag, KafkaClient.consumer_group_lag]
  affects: [05-02-PLAN (inbound faces)]
tech_stack:
  added: [confluent_kafka.admin.AdminClient, confluent_kafka.ConsumerGroupTopicPartitions]
  patterns: [AdminClient read-only query, protocol extension, facade delegation]
key_files:
  created: []
  modified:
    - src/kafka_mcp/domain/models.py
    - src/kafka_mcp/ports/consumer.py
    - src/kafka_mcp/adapters/outbound/confluent_consumer.py
    - src/kafka_mcp/adapters/inbound/lib.py
    - tests/test_adapters.py
    - tests/test_domain.py
decisions:
  - "KafkaClient.consumer_group_lag delegates directly to self._consumer (port), not self._service (TopicService), because lag is a read-only query with no domain orchestration"
  - "AdminClient created in ConfluentConsumerAdapter.__init__ from same broker/SASL config minus consumer-specific keys (enable.auto.commit, group.id)"
  - "Partitions with tp.offset < 0 (OFFSET_INVALID) or tp.error report current_offset=0, lag=end_offset"
metrics:
  duration: "576s (~10m)"
  completed: "2026-06-15T18:05:00Z"
  tasks: 2/2
  tests_added: 7
  tests_total: 277
  tests_passed: 277
---

# Phase 5 Plan 01: LagRecord + ConsumerPort + AdminClient Adapter + Facade Summary

**One-liner:** Per-partition consumer-group lag query via AdminClient.list_consumer_group_offsets + existing watermark offsets, with LagRecord model carrying Investigator-Contract Evidence fields

## Tasks Completed

| Task | Name | Commit | Key Changes |
|------|------|--------|-------------|
| 1 | LagRecord model + ConsumerPort extension + KafkaClient facade | 89d09c3 | Added LagRecord pydantic model (9 fields), consumer_group_lag to ConsumerPort protocol, KafkaClient facade method |
| 2 | ConfluentConsumerAdapter implementation + adapter unit tests | 89d09c3 | AdminClient creation in __init__, consumer_group_lag implementation, 7 adapter-level unit tests |

## Implementation Details

### LagRecord Model (`src/kafka_mcp/domain/models.py`)
- 7 data fields: `group`, `topic`, `partition`, `current_offset`, `end_offset`, `lag`, `timestamp_utc`
- 2 Evidence fields: `source="kafka"`, `event_type="consumer_lag"`
- Follows exact KafkaMessage pattern (BaseModel, typed fields, Evidence section)
- No I/O imports — hexagonal boundary preserved

### ConsumerPort Extension (`src/kafka_mcp/ports/consumer.py`)
- Added `consumer_group_lag(self, group: str, topics: list[str] | None = None) -> list[LagRecord]`
- Full docstring with Args/Returns sections
- Protocol method stub (`...`) — no broker imports

### ConfluentConsumerAdapter (`src/kafka_mcp/adapters/outbound/confluent_consumer.py`)
- AdminClient created in `__init__` from same broker/SASL config (minus `enable.auto.commit`, `group.id`)
- `consumer_group_lag` calls `AdminClient.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])` (read-only, no group join)
- Uses existing `self.get_watermark_offsets()` for end offsets per partition
- Lag computed as `end_offset - current_offset`
- Handles: no committed offset (current_offset=0), empty/non-existent group (returns []), topic filter, TopicNotFoundError on watermark fetch

### KafkaClient Facade (`src/kafka_mcp/adapters/inbound/lib.py`)
- `consumer_group_lag(group, topics)` delegates directly to `self._consumer.consumer_group_lag(group, topics)`
- Direct port delegation — no TopicService involvement (read-only pass-through, no decode/search logic)

### Tests (`tests/test_adapters.py`, `tests/test_domain.py`)
- 7 new tests in `TestConfluentConsumerAdapterLag`:
  - `test_consumer_group_lag_returns_lag_records` — 2 partitions, correct lag computation
  - `test_consumer_group_lag_no_committed_offset` — OFFSET_INVALID → current_offset=0
  - `test_consumer_group_lag_empty_group` — KafkaException → empty list
  - `test_consumer_group_lag_topics_filter` — topics=["orders"] excludes "payments"
  - `test_consumer_group_lag_topics_none_returns_all` — topics=None returns all
  - `test_consumer_group_lag_evidence_fields` — source="kafka", event_type="consumer_lag"
  - `test_consumer_group_lag_timestamp_utc_is_utc_aware` — tzinfo == UTC
- Updated all existing adapter test helpers to also mock AdminClient (preventing regressions from __init__ change)
- Updated 3 mock consumer classes in test_domain.py for protocol compliance

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Updated existing test mock helpers to patch AdminClient**
- **Found during:** Task 2
- **Issue:** Adding AdminClient to `ConfluentConsumerAdapter.__init__` broke all existing adapter tests that only patched Consumer — AdminClient constructor was called unmocked
- **Fix:** Added `patch("...AdminClient", return_value=MagicMock())` to all `_make_adapter` helpers, `_make_consumer_adapter`, context manager tests, config tests, and max_scan test
- **Files modified:** `tests/test_adapters.py` (15 adapter construction sites updated)
- **Commit:** 89d09c3

**2. [Rule 3 - Blocking] Updated mock consumer classes for protocol compliance**
- **Found during:** Task 2 (full test run)
- **Issue:** `test_domain.py::TestConsumerPort::test_compliant_class_passes_isinstance` failed because MockConsumer classes lacked `consumer_group_lag` method, breaking `isinstance(mock, ConsumerPort)` checks
- **Fix:** Added `consumer_group_lag` method to 4 mock consumer classes: inline MockConsumer (line 94), module-level MockConsumer (line 385), _MockConsumerWithMsg (line 691), _MockConsumerGetMsg (line 835)
- **Files modified:** `tests/test_domain.py`
- **Commit:** 89d09c3

## Verification Results

```
✅ LagRecord imports and instantiates correctly (9 fields, defaults verified)
✅ ConsumerPort has consumer_group_lag method
✅ KafkaClient has consumer_group_lag method
✅ All 7 consumer_group_lag adapter tests pass
✅ No hexagonal boundary violations (no broker imports in domain/ports)
✅ Full test suite: 277 passed, 0 failed
✅ Ruff: All checks passed
✅ Protocol compliance: isinstance(adapter, ConsumerPort) holds
```

## Known Stubs

None — all code is fully wired and functional.

## Self-Check: PASSED

- [x] `src/kafka_mcp/domain/models.py` — FOUND
- [x] `src/kafka_mcp/ports/consumer.py` — FOUND
- [x] `src/kafka_mcp/adapters/outbound/confluent_consumer.py` — FOUND
- [x] `src/kafka_mcp/adapters/inbound/lib.py` — FOUND
- [x] `tests/test_adapters.py` — FOUND
- [x] `tests/test_domain.py` — FOUND
- [x] Commit `89d09c3` — FOUND
