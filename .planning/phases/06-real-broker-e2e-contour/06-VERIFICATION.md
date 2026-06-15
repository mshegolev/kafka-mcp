---
phase: 06-real-broker-e2e-contour
verified: 2026-06-16T12:45:00Z
status: passed
score: 4/4 must-haves verified
overrides_applied: 0
---

# Phase 6: Real-Broker E2E Contour — Verification Report

**Phase Goal:** An automated integration suite running against a real Kafka broker and Schema Registry (testcontainers) verifies all tools — including the v1.1 surfaces (key decode, schema_id, lag) — with real-wire round-trips and real schema-encoded decode.

**Verified:** 2026-06-16T12:45:00Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth (Success Criterion) | Status | Evidence |
|---|---------------------------|--------|----------|
| 1 | `pytest -m integration` boots real Kafka + Schema Registry via testcontainers, seeds messages, passes without external broker; `pytest` (no flag) skips integration tests | ✓ VERIFIED | `pytest tests/ -x --timeout=30` → 298 passed, 25 deselected. `pytest -m integration --co -q` → 25 collected. `pytest -m integration -x -v --timeout=300` → 25 passed in 267.60s. `addopts = "-m 'not integration'"` in pyproject.toml. Graceful Docker skip via try/except in `kafka_container` fixture. |
| 2 | Real-wire round-trip tests for list_topics, describe_topic, search_messages, get_message pass against live broker; read-only throughout | ✓ VERIFIED | `test_basic_ops.py`: 5 tests (list_topics×3, describe_topic×2). `test_search_get.py`: 6 tests (search×3, get_message×3). All pass with decoded values matching seeded payloads (`ORD-0`, `amount=100`). ConfluentConsumerAdapter uses `assign()` only, `enable.auto.commit=False` hardcoded — confirmed read-only. |
| 3 | Real-wire decode tests pass for at least one Avro, one Protobuf, one JSON round-trip against live SR; decoded values match seeded payloads | ✓ VERIFIED | `test_decode.py`: 8 tests — Avro: `order_id=="AVRO-ORD-0"`, `amount==200`, `customer_id=="CUST-0"`. Protobuf: `event_id=="EVT-0"`, `priority==10`, `description=="Test event 0"`. JSON: `order_id=="ORD-1"`, `amount==101`. `test_three_formats_all_decode` cross-checks all three. `schema_id` populated for Avro (int > 0), None for plain JSON. |
| 4 | v1.1 surfaces covered: key decode (KEY-01/KEY-02) and consumer lag (LAG-01/LAG-03) each have at least one integration test | ✓ VERIFIED | `test_v11_surfaces.py`: 6 tests. KEY-01: `test_key_decode_avro_key` asserts `key_decoded["order_id"]=="KEY-ORD-0"`, `key_decoded["region"]=="EU"`. KEY-02: `test_key_decode_schema_id_includes_key` asserts `schema_id["key"] > 0` AND `schema_id["value"] > 0`. LAG-01: `test_consumer_group_lag_has_positive_lag` asserts `total_lag > 0`. LAG-03: `test_consumer_group_lag_real_group` asserts `LagRecord` Evidence fields (`source=="kafka"`, `event_type=="consumer_lag"`, `timestamp_utc`). |

**Score:** 4/4 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `tests/integration/__init__.py` | Integration test package | ✓ VERIFIED | Exists (empty package marker) |
| `tests/integration/conftest.py` | Testcontainers fixtures (Kafka + SR), test producer, pytest marker; ≥80 lines | ✓ VERIFIED | 486 lines. 12 fixtures: `_ensure_docker_host`, `kafka_container`, `sr_container`, `bootstrap_servers`, `schema_registry_url`, `kafka_settings`, `kafka_client`, `seed_json_topic`, `sr_client`, `seed_avro_topic`, `seed_protobuf_topic`, `seed_avro_key_topic`, `seed_lag_consumer`. All session-scoped. `pytestmark = pytest.mark.integration`. |
| `tests/integration/test_basic_ops.py` | list_topics and describe_topic real-wire tests; ≥40 lines | ✓ VERIFIED | 55 lines, 5 tests in 2 classes. `pytestmark = pytest.mark.integration`. Uses `kafka_client` and `seed_json_topic` fixtures. |
| `tests/integration/test_search_get.py` | search_messages and get_message real-wire tests; ≥60 lines | ✓ VERIFIED | 78 lines, 6 tests in 2 classes. `pytestmark = pytest.mark.integration`. Asserts decoded JSON values, evidence fields, error handling. |
| `tests/integration/test_decode.py` | Avro, Protobuf, JSON decode tests against live SR; ≥80 lines | ✓ VERIFIED | 108 lines, 8 tests in 4 classes. Specific value assertions per format. Cross-format summary test. |
| `tests/integration/test_v11_surfaces.py` | v1.1 surface tests: key decode, schema_id, consumer_group_lag; ≥60 lines | ✓ VERIFIED | 88 lines, 6 tests in 2 classes. KEY-01/02 and LAG-01/03 coverage. |
| `pyproject.toml` | integration marker registration + testcontainers dev dep | ✓ VERIFIED | `addopts = "-m 'not integration'"` (line 78), `markers = ["integration: ..."]` (line 79-80), `testcontainers[kafka]>=4.4` (line 54), `pytest-timeout>=2.3` (line 50). |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `conftest.py` | `testcontainers.kafka.KafkaContainer` | fixture yields running container | ✓ WIRED | `KafkaContainer("confluentinc/cp-kafka:7.6.0")` at line 81, `.start(timeout=120)`, `yield container`, `.stop()` |
| `conftest.py` | `confluent_kafka.schema_registry` | AvroSerializer for seeding | ✓ WIRED | `AvroSerializer` used in `seed_avro_topic` (line 286) and `seed_avro_key_topic` (lines 426-427). 6 total references. |
| `test_basic_ops.py` | `kafka_mcp.adapters.inbound.lib.KafkaClient` | constructs client from real broker | ✓ WIRED | `kafka_client` fixture injected into all 5 tests; fixture builds `KafkaClient(consumer, registry)` with real adapters (conftest line 205) |
| `test_decode.py` | `kafka_mcp.adapters.inbound.lib.KafkaClient` | searches seeded messages, asserts decoded values | ✓ WIRED | `kafka_client` fixture used in all 8 tests; value assertions verify decoded content matches seeded payloads |
| `test_v11_surfaces.py` | `kafka_mcp.domain.models.LagRecord` | asserts lag records from real consumer group | ✓ WIRED | `from kafka_mcp.domain.models import LagRecord` (line 14), `isinstance(rec, LagRecord)` assertion (line 66), 3 references total |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|--------------------|--------|
| `test_basic_ops.py` | `topics`, `info` | `kafka_client.list_topics()` / `.describe_topic()` → `ConfluentConsumerAdapter` → real Kafka broker | Yes — 25 tests pass with topic names and partition offsets from live broker | ✓ FLOWING |
| `test_search_get.py` | `results`, `msg` | `kafka_client.search_messages()` / `.get_message()` → real broker → JSON decode | Yes — decoded values match seeded payloads (ORD-0, amount=100) | ✓ FLOWING |
| `test_decode.py` | `msg.value` | `kafka_client.search/get` → real SR → Avro/Protobuf/JSON decode | Yes — AVRO-ORD-0, EVT-0, ORD-1 match seeded data exactly | ✓ FLOWING |
| `test_v11_surfaces.py` | `msg.key_decoded`, `records` | `kafka_client.search/get` → real SR key decode; `.consumer_group_lag()` → real broker | Yes — key_decoded dict populated, lag > 0 for partial consume | ✓ FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Unit suite hermetic (no integration leakage) | `python3 -m pytest tests/ -x --timeout=30` | 298 passed, 25 deselected in 1.79s | ✓ PASS |
| Integration collection count | `python3 -m pytest -m integration --co -q` | 25/323 tests collected (298 deselected) | ✓ PASS |
| All integration tests pass against real containers | `python3 -m pytest tests/integration/ -m integration -x -v --timeout=300` | 25 passed in 267.60s | ✓ PASS |
| Linter clean | `python3 -m ruff check tests/integration/` | All checks passed | ✓ PASS |
| testcontainers importable | `python3 -c "from testcontainers.kafka import KafkaContainer"` | OK | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| E2E-01 | 06-01 | Integration test contour boots Kafka + SR via testcontainers, `-m integration` marker, skipped by default | ✓ SATISFIED | `conftest.py` fixtures, `addopts`, `markers` in pyproject.toml, 298 unit tests unaffected |
| E2E-02 | 06-01, 06-02 | Real-wire round-trips for list_topics, describe_topic, search_messages, get_message; read-only paths | ✓ SATISFIED | 11 tests across `test_basic_ops.py` (5) and `test_search_get.py` (6); assign-only consumer confirmed |
| E2E-03 | 06-02 | Real-wire decode for Avro, Protobuf, JSON against live SR; decoded values match seeded payloads | ✓ SATISFIED | 8 tests in `test_decode.py`; exact value assertions per format; `test_three_formats_all_decode` cross-check |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| — | — | No TODO/FIXME/placeholder/stub patterns found | — | — |

No anti-patterns detected across all 5 integration test files.

### Human Verification Required

No items require human verification. All success criteria are verifiable through automated test execution, which was performed and passed.

### Gaps Summary

No gaps found. All 4 success criteria are verified with concrete evidence from running tests:

- **25 integration tests pass** against real Kafka + Schema Registry containers (267.60s)
- **298 unit tests pass** with integration tests correctly deselected (1.79s)
- **All three decode formats** (Avro, Protobuf, JSON) produce exact value matches against seeded payloads
- **v1.1 surfaces** (KEY-01/02, LAG-01/03) each have dedicated integration tests that exercise live broker paths
- **Read-only guarantee** confirmed: `ConfluentConsumerAdapter` uses `assign()` only, `enable.auto.commit=False`
- **Graceful Docker skip** implemented via try/except in `kafka_container` fixture

### Disconfirmation Pass (Confirmation Bias Counter)

1. **Partially met requirement check:** `test_search_messages_respects_limit` asserts `len(results) <= 1` which would pass with 0 results — however, the companion test `test_search_messages_finds_seeded_json` proves the key exists, and the test's purpose is limit enforcement, not key existence. Acceptable.

2. **Test-not-testing check:** All decode tests assert specific seeded values (not just `is not None`) — e.g., `msg.value["order_id"] == "AVRO-ORD-0"`. These values can only match if the full encode→produce→consume→decode pipeline works.

3. **Uncovered error path:** Schema Registry connectivity failure during decode is not integration-tested (would require killing the SR container mid-test). This is acceptable — it's a resilience edge case beyond the phase goal of "real-wire round-trips."

---

_Verified: 2026-06-16T12:45:00Z_
_Verifier: OpenCode (gsd-verifier)_
