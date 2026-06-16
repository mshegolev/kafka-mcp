---
phase: "06-real-broker-e2e-contour"
plan: "02"
subsystem: "integration-tests"
tags: [integration-tests, avro, protobuf, json, schema-registry, key-decode, consumer-lag, testcontainers]
dependency_graph:
  requires: [integration-test-infrastructure, session-scoped-containers, seed-json-topic]
  provides: [avro-decode-tests, protobuf-decode-tests, json-decode-tests, key-decode-tests, lag-tests, search-get-tests]
  affects: [tests/integration/conftest.py]
tech_stack:
  added: []
  patterns: [confluent-wire-framing, protobuf-dynamic-compile, avro-serializer-seeding, partial-offset-commit]
key_files:
  created:
    - tests/integration/test_search_get.py
    - tests/integration/test_decode.py
    - tests/integration/test_v11_surfaces.py
  modified:
    - tests/integration/conftest.py
decisions:
  - "Used AvroSerializer for Avro seeding (real Confluent wire framing, not manual bytes)"
  - "Used manual Confluent Protobuf framing (struct.pack + grpc_tools protoc compile) since ProtobufSerializer requires a pre-generated Python class"
  - "Lag consumer consumes 2 of 5 messages to guarantee positive lag for LAG-01 assertions"
metrics:
  duration: "8m 21s"
  completed: "2026-06-15T19:25:10Z"
  tests_added: 20
  tests_passed: 25
  unit_tests_passed: 298
  unit_tests_deselected: 25
---

# Phase 06 Plan 02: Search/Decode/v1.1 Real-Wire Integration Tests Summary

**One-liner:** 20 real-wire integration tests covering search/get round-trips, Avro/Protobuf/JSON decode via live Schema Registry, key decode (KEY-01/KEY-02), and consumer_group_lag (LAG-01/LAG-03).

## What Was Done

### Task 1: Seed fixtures for encoded topics + search/get round-trip tests

**conftest.py** — added 5 new session-scoped fixtures:
- **`sr_client`**: SchemaRegistryClient for schema registration in seed fixtures
- **`seed_avro_topic`**: Seeds "test-avro" with 3 Avro-encoded messages via AvroSerializer (real Confluent wire framing)
- **`seed_protobuf_topic`**: Seeds "test-proto" with 3 Protobuf-framed messages via manual Confluent wire format (grpc_tools protoc compile + struct.pack framing)
- **`seed_avro_key_topic`**: Seeds "test-avro-key" with 2 messages having Avro-encoded keys AND values (for KEY-01/KEY-02 tests)
- **`seed_lag_consumer`**: Creates consumer group "test-lag-group" by consuming 2 of 5 messages from seed_json_topic, committing offsets, then closing (for LAG-01/LAG-03 tests)

**test_search_get.py** — 6 tests:
1. `test_search_messages_finds_seeded_json` — search by key finds decoded JSON value
2. `test_search_messages_no_match` — nonexistent key returns empty list
3. `test_search_messages_respects_limit` — limit=1 returns at most 1 result
4. `test_get_message_returns_real_message` — fetch by exact coordinates, verify decoded value
5. `test_get_message_evidence_fields` — Evidence fields (source, event_type, keys) populated
6. `test_get_message_not_found_raises` — out-of-range offset raises MessageNotFoundError

### Task 2: Decode tests (Avro/Protobuf/JSON) + v1.1 surface tests

**test_decode.py** — 8 tests:
1. `test_avro_decode_round_trip` — search finds Avro message, values match seeded data
2. `test_avro_get_message_decode` — get_message decodes Avro value correctly
3. `test_avro_schema_id_populated` — schema_id dict has value schema ID > 0
4. `test_protobuf_decode_round_trip` — search finds Protobuf message, values match (snake_case)
5. `test_protobuf_get_message_decode` — get_message decodes Protobuf correctly
6. `test_json_decode_round_trip` — plain JSON decode matches seeded data
7. `test_json_get_message_decode` — JSON schema_id is None (no Confluent framing)
8. `test_three_formats_all_decode` — summary: at least one message from each format decodes

**test_v11_surfaces.py** — 6 tests:
1. `test_key_decode_avro_key` — KEY-01: Avro-encoded key → key_decoded dict populated
2. `test_key_decode_schema_id_includes_key` — KEY-02: schema_id.key AND schema_id.value both > 0
3. `test_plain_key_no_crash` — KEY-01 fallback: plain string key → key_decoded is None
4. `test_consumer_group_lag_real_group` — LAG-01/LAG-03: LagRecord with Evidence fields
5. `test_consumer_group_lag_has_positive_lag` — LAG-01: total lag > 0 for partially consumed group
6. `test_consumer_group_lag_unknown_group` — unknown group returns empty list

## Deviations from Plan

None — plan executed exactly as written.

## Verification Results

| Check | Result |
|-------|--------|
| `pytest tests/ -x --timeout=30` | 298 passed, 25 deselected |
| `pytest -m integration tests/integration/ -x -v --timeout=300` | 25 passed in 263s |
| `ruff check src/ tests/` | All checks passed |
| Avro decoded: order_id="AVRO-ORD-0", amount=200, customer_id="CUST-0" | ✅ Match |
| Protobuf decoded: event_id="EVT-0", priority=10, description="Test event 0" | ✅ Match |
| JSON decoded: order_id="ORD-1", amount=101 | ✅ Match |
| key_decoded: order_id="KEY-ORD-0", region="EU" | ✅ Match |
| schema_id.key > 0, schema_id.value > 0 for avro-key topic | ✅ |
| consumer_group_lag total_lag > 0 | ✅ |

## Decisions Made

1. **AvroSerializer for Avro seeding**: Used `confluent_kafka.schema_registry.avro.AvroSerializer` which produces real Confluent wire-framed bytes (magic + schema_id + Avro), matching production paths exactly.
2. **Manual Protobuf framing**: `ProtobufSerializer` requires a pre-generated Python message class (not available from raw .proto string). Instead, compiled the schema via `grpc_tools.protoc`, serialized with `google.protobuf.message_factory`, and manually constructed the Confluent wire framing (`struct.pack(">bI", 0, schema_id) + b"\x00" + payload`). This mirrors the decode adapter's reverse path.
3. **Lag consumer 2/5 partial consume**: Consuming exactly 2 of 5 guarantees `lag >= 3` (may vary by partition distribution), ensuring `total_lag > 0` assertion is deterministic.

## Self-Check: PASSED
