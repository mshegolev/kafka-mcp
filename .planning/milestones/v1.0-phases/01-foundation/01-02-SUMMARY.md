---
phase: "01"
plan: "01-02"
subsystem: "outbound-adapters"
tags: [adapters, confluent-kafka, schema-registry, orjson, tdd, read-only, KAFKA-06]
dependency_graph:
  requires:
    - kafka_mcp.ports.consumer (ConsumerPort)
    - kafka_mcp.ports.schema_registry (SchemaRegistryPort)
    - kafka_mcp.config (KafkaMcpSettings)
    - kafka_mcp.domain.errors (TopicNotFoundError)
  provides:
    - kafka_mcp.adapters.outbound.confluent_consumer (ConfluentConsumerAdapter)
    - kafka_mcp.adapters.outbound.schema_registry_http (SchemaRegistryHttpAdapter)
    - kafka_mcp.adapters.outbound.json_orjson (orjson_loads, orjson_dumps)
  affects:
    - plan 01-03 (KafkaClient lib facade will inject ConfluentConsumerAdapter via ConsumerPort)
    - plan 01-04 (inbound adapters call KafkaClient which calls these outbound adapters)
tech_stack:
  added:
    - confluent_kafka.Consumer (librdkafka) for broker access (assign-based only)
    - confluent_kafka.KafkaException for partition-level error handling
    - orjson for compact JSON encode/decode
    - httpx imported in schema_registry_http (Phase 2 will wire it to real calls)
  patterns:
    - assign()-only Consumer (never subscribe) — structural read-only guarantee
    - throwaway group.id kafka-mcp-ro-{uuid4} per instantiation
    - SecretStr.get_secret_value() extracted immediately; conf dict never stored
    - Protocol duck-typing (no explicit Protocol inheritance needed for isinstance)
    - Phase 1 stub pattern: method body returns None; Phase 2 impl commented inline
key_files:
  created:
    - src/kafka_mcp/adapters/outbound/confluent_consumer.py
    - src/kafka_mcp/adapters/outbound/schema_registry_http.py
    - src/kafka_mcp/adapters/outbound/json_orjson.py
    - tests/test_adapters.py
  modified:
    - src/kafka_mcp/adapters/outbound/__init__.py
decisions:
  - "assign()-only pattern enforced at source level; word 'subscribe' absent from non-comment code lines (verified by automated source scan and test)"
  - "SchemaRegistryHttpAdapter Phase 1 returns None immediately; Phase 2 stub code is commented inline so the contract is visible without requiring an import-time httpx connection"
  - "orjson_loads/orjson_dumps are thin wrappers (not re-exported from the domain layer) — keeps domain/ free of orjson dependency"
  - "All 9 docstring/comment references to broker subscription removed to satisfy automated no-subscribe source scan (T-02-03 mitigation)"
metrics:
  completed_date: "2026-06-05T11:37:00Z"
  duration_minutes: 20
  tasks_completed: 2
  files_created: 4
  files_modified: 1
  tests_added: 24
  tests_passing: 44
---

# Phase 01 Plan 01-02: Outbound Adapters Summary

**One-liner:** Confluent-kafka Consumer adapter with assign-only read-only guarantee (KAFKA-06) + SchemaRegistryHttpAdapter Phase-1 stub + orjson codec helpers, tested with mocks — no real broker required.

## What Was Built

### Task 1 — ConfluentConsumerAdapter (TDD RED then GREEN)

RED gate: wrote 24 failing tests in `tests/test_adapters.py` covering list_topics filtering, watermark offsets, context manager, Protocol isinstance, and config dict correctness.

GREEN gate: implemented `confluent_consumer.py`:

- Constructor builds a librdkafka `conf` dict from `KafkaMcpSettings`.
  Always includes `enable.auto.commit=False` and `group.id=kafka-mcp-ro-{uuid4()}`.
  When `security_protocol != "PLAINTEXT"`, adds `security.protocol`, `sasl.mechanism`,
  `sasl.username`, `sasl.password` (extracted via `SecretStr.get_secret_value()`).
- `list_topics(include_internal=False)`: calls `Consumer.list_topics(timeout=10.0)`,
  filters `__`-prefixed names when `include_internal=False`, returns sorted list.
- `get_watermark_offsets(topic, partition)`: delegates to `Consumer.get_watermark_offsets()`;
  raises `TopicNotFoundError` on `KafkaException`.
- `__enter__` / `__exit__`: context manager calls `Consumer.close()` on exit.
- `Consumer.subscribe()` is entirely absent — automated source scan confirms no violation.

### Task 2 — SchemaRegistryHttpAdapter + orjson helpers (GREEN)

- `schema_registry_http.py`: `SchemaRegistryHttpAdapter` implements `SchemaRegistryPort`.
  Phase 1 `get_schema()` returns `None` immediately; Phase 2 HTTP stub commented inline.
  `httpx` imported at module level (declared dep); no real HTTP calls in Phase 1.
- `json_orjson.py`: `orjson_loads(bytes | str) -> dict` and `orjson_dumps(dict) -> bytes` —
  thin wrappers producing compact JSON (no spaces after `:` or `,`).
- `adapters/outbound/__init__.py` updated to export all three adapters.

## Verification

```
pytest tests/test_adapters.py -v   →  24 passed in 0.11s
pytest tests/ -v                   →  44 passed in 0.14s (incl. domain tests)
no subscribe() source scan         →  PASS
all adapters importable            →  PASS
```

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Docstring mention of "subscribe" triggered no-subscribe source scan**
- **Found during:** Task 1 verification
- **Issue:** Module docstring contained `"Consumer.subscribe() is intentionally absent"` — the automated source check (`lines where 'subscribe' in line and not line.strip().startswith('#')`) flagged docstring prose as a violation
- **Fix:** Rephrased to `"subscription-based method is intentionally absent"` — removes the word entirely from the file
- **Files modified:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py`
- **Commit:** 9c1778b (part of GREEN commit)

## TDD Gate Compliance

- RED commit (`test(01-02): add failing tests...`): `4ef5b74` — 24 fail, 0 pass
- GREEN Task 1 commit (`feat(01-02): implement ConfluentConsumerAdapter`): `9c1778b` — 15/15 consumer adapter tests pass
- GREEN Task 2 commit (`feat(01-02): implement SchemaRegistryHttpAdapter...`): `7999c1b` — 24/24 adapter tests pass; 44/44 total

## Known Stubs

`SchemaRegistryHttpAdapter.get_schema()` always returns `None` in Phase 1. This is intentional — the stub is the Phase 1 deliverable. Full HTTP decode (KAFKA-05) is Phase 2 scope. No UI or data-flow rendering depends on this in Phase 1.

## Threat Flags

No new threat surface beyond the plan's `<threat_model>`:
- T-02-01 mitigated: `sasl_password` extracted via `SecretStr.get_secret_value()` immediately; conf dict local to `__init__`, never stored or logged.
- T-02-02 mitigated: Phase 1 stub makes no HTTP calls; password stored in attribute but never logged.
- T-02-03 mitigated: `subscribe()` absent from source; automated scan in `test_no_subscribe_in_source` runs in CI.
- T-02-04 noted: `list_topics` returns filtered/sorted list; max_scan cap applies in plan 01-03.
- T-02-05 mitigated: `uuid4()` called per instantiation.

## Self-Check: PASSED

Files confirmed present:
- src/kafka_mcp/adapters/outbound/confluent_consumer.py ✓
- src/kafka_mcp/adapters/outbound/schema_registry_http.py ✓
- src/kafka_mcp/adapters/outbound/json_orjson.py ✓
- tests/test_adapters.py ✓

Commits confirmed present:
- 4ef5b74 (RED) ✓
- 9c1778b (GREEN Task 1) ✓
- 7999c1b (GREEN Task 2) ✓
