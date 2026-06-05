---
phase: "02-search-decode"
plan: "03"
subsystem: "outbound adapter — consumer scan"
tags: ["consumer", "fetch-messages", "fetch-message", "tdd", "hexagonal", "read-only", "kafka-06"]
dependency_graph:
  requires:
    - "02-01 (KafkaMessage, MessageNotFoundError, ConsumerPort.fetch_messages/fetch_message signatures)"
    - "02-02 (ConfluentConsumerAdapter with stubs replaced)"
  provides:
    - "ConfluentConsumerAdapter.fetch_messages() — offsets-based seek + bounded forward scan"
    - "ConfluentConsumerAdapter.fetch_message() — single-offset lookup with MessageNotFoundError"
    - "Read-only guarantee preserved: assign()-only, no commit, no group-based subscription"
    - "T-02-03 STRIDE threats A/B/C/D all mitigated"
  affects:
    - "src/kafka_mcp/adapters/outbound/confluent_consumer.py"
    - "tests/test_adapters.py"
tech_stack:
  added:
    - "confluent_kafka.TopicPartition (used for assign-based seek)"
    - "confluent_kafka.TIMESTAMP_CREATE_TIME constant"
    - "datetime.timezone.utc (stdlib)"
  patterns:
    - "assign(TopicPartition(topic, partition, offset)) for positional seek (no subscribe)"
    - "Five-condition loop exit: None poll, msg.error(), offset>=stop, scan_count>max_scan, ts_utc>time_to"
    - "TIMESTAMP_CREATE_TIME -> UTC datetime.fromtimestamp; fallback datetime.now(utc)"
    - "UTF-8 decode with errors='replace' for key/header bytes (T-02-03-D)"
    - "5x poll_timeout for single-message fetch (T-02-03-B)"
key_files:
  created: []
  modified:
    - "src/kafka_mcp/adapters/outbound/confluent_consumer.py"
    - "tests/test_adapters.py"
decisions:
  - "scan loop terminates on five conditions — stop_offset, limit, max_scan, time_to, None poll — all five independently tested"
  - "fetch_message uses 5x poll_timeout for single-message fetch to handle broker latency"
  - "subscribe() absent from entire source file; test_no_subscribe_in_source verifies this at test-time"
  - "docstring wording changed to avoid 'subscribe()' text to pass the source-scan test (Rule 1 auto-fix)"
  - "value=None in returned KafkaMessage — decode performed by SchemaRegistryHttpAdapter in domain service (plan 02-04)"
metrics:
  duration: "~25 minutes"
  completed: "2026-06-06T00:30:00Z"
  tasks: 2
  files: 2
---

# Phase 2 Plan 3: Consumer Scan Implementation Summary

**One-liner:** ConfluentConsumerAdapter.fetch_messages() (offsets-based seek + five-condition bounded scan) and fetch_message() (watermark-checked single fetch with MessageNotFoundError) fully replacing NotImplementedError stubs.

## Tasks Completed

| Task | Name | RED Commit | GREEN Commit | Tests Added |
|------|------|-----------|-------------|-------------|
| 1 | fetch_messages — offsets_for_times seek + bounded forward consume | 3776848 | cb509a0 | 9 |
| 2 | fetch_message — single point lookup with MessageNotFoundError | 2c31b80 | 0b5db9b | 5 |

**Total new tests:** 14 (TestFetchMessages x9 + TestFetchMessage x5)
**Full suite after plan:** 139 tests pass (up from 125 baseline)

## What Was Built

### fetch_messages (Task 1)

`ConfluentConsumerAdapter.fetch_messages(topic, partition, start_offset, stop_offset, time_to, limit)`:

- `TopicPartition(topic, partition, start_offset)` → `self._consumer.assign([tp])` (no subscribe)
- Loop: `poll(timeout=poll_timeout)` → five exit conditions checked in order:
  1. `msg is None` (end of partition/timeout) → break
  2. `msg.error()` truthy (transient error) → break with partial result
  3. `msg.offset() >= stop_offset` → break
  4. `scan_count > max_scan` (T-02-03-A DoS guard) → break
  5. `time_to is not None and ts_utc > time_to` → break
  6. `len(result) >= limit` → break
- **Timestamp extraction:** `TIMESTAMP_CREATE_TIME` + `ts_ms > 0` → `datetime.fromtimestamp(ts_ms/1000, utc)`; any other type (LogAppendTime, NOT_AVAILABLE) → `datetime.now(utc)` fallback
- **key:** `msg.key()` bytes decoded UTF-8 `errors="replace"`; `None` if key is None
- **headers:** `msg.headers()` list of `(name, bytes)` → `dict[str, str]` UTF-8 decoded; `{}` if None
- **KafkaMessage:** `value=None` (decode not performed here); `raw=msg.value() or b""`

### fetch_message (Task 2)

`ConfluentConsumerAdapter.fetch_message(topic, partition, offset)`:

- `low, high = self.get_watermark_offsets(topic, partition)` range check
- `offset < low or offset >= high` → raise `MessageNotFoundError(topic, partition, offset)`
- `TopicPartition(topic, partition, offset)` → `self._consumer.assign([tp])`
- `msg = poll(timeout=poll_timeout * 5)` (5× budget for broker latency, T-02-03-B)
- `msg is None` or `msg.error()` → raise `MessageNotFoundError(topic, partition, offset)`
- Same timestamp/key/headers extraction as fetch_messages
- Returns `KafkaMessage(value=None, raw=msg.value() or b"")`

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Docstring wording triggered test_no_subscribe_in_source**
- **Found during:** Task 1 GREEN verification
- **Issue:** fetch_messages docstring contained the phrase "never calls subscribe() and" — the test scans all non-comment lines for "subscribe" and the docstring is not a comment.
- **Fix:** Rewrote docstring to "never uses the group-based subscription API" — preserves intent without triggering the source-scan test.
- **Files modified:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py`
- **Commit:** cb509a0 (included in GREEN commit)

## Known Stubs

None — all NotImplementedError stubs from plan 02-01 replaced. fetch_messages and fetch_message are fully implemented.

## Threat Surface Scan

All T-02-03 threats mitigated as planned:

| Threat ID | Status | Implementation |
|-----------|--------|----------------|
| T-02-03-A | Mitigated | `scan_count > max_scan` exits loop (per-partition cap); `len(result) >= limit` exits loop (global cap) |
| T-02-03-B | Mitigated | `poll_timeout` from settings (default 1.0s); fetch_message uses `5 * poll_timeout`; no infinite poll |
| T-02-03-C | Mitigated | subscribe() absent from source; `test_no_subscribe_in_source` verifies at test-time; assign()-only throughout |
| T-02-03-D | Mitigated | `errors="replace"` on both key and header bytes — malformed UTF-8 produces U+FFFD, never raises |
| T-02-03-E | Accepted | Message size bounded by broker configuration; scan count bounded; as-planned |

## TDD Gate Compliance

| Task | RED commit | GREEN commit | Compliant |
|------|-----------|-------------|-----------|
| Task 1 | 3776848 (test(02-03): add failing tests for...fetch_messages) | cb509a0 (feat(02-03): add fetch_messages...) | Yes |
| Task 2 | 2c31b80 (test(02-03): add failing tests for...fetch_message) | 0b5db9b (feat(02-03): add fetch_message...) | Yes |

## Self-Check: PASSED

Files exist:
- FOUND: src/kafka_mcp/adapters/outbound/confluent_consumer.py (contains fetch_messages and fetch_message)

Commits exist: 3776848, cb509a0, 2c31b80, 0b5db9b

Tests: 139 passed, 0 failed
- 14 new consumer scan tests (TestFetchMessages x9, TestFetchMessage x5)
- All 125 prior tests still pass
- subscribe absent from adapter source (grep -c returns 0)
- fetch_messages/fetch_message present (grep -c returns 4)
