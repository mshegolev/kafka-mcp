---
phase: "02-search-decode"
plan: "02"
subsystem: "outbound adapter"
tags: ["schema-registry", "avro", "protobuf", "json-fallback", "decode", "tdd", "hexagonal"]
dependency_graph:
  requires:
    - "02-01 (DecodeError, SchemaRegistryPort.decode signature)"
  provides:
    - "Real SchemaRegistryHttpAdapter replacing Phase 1 stub"
    - "Confluent magic-byte framing detection (0x00 + 4-byte schema_id)"
    - "AvroDeserializer dispatch for AVRO schema type"
    - "ProtobufDeserializer dispatch for PROTOBUF schema type (with TODO for pre-generated types)"
    - "json.loads fallback for non-framed payloads"
    - "DecodeError on all failure paths (T-02-02-A)"
    - "Adapter-level schema cache keyed by schema_id"
  affects:
    - "src/kafka_mcp/adapters/outbound/schema_registry_http.py"
    - "tests/test_adapters.py"
    - "pyproject.toml"
    - "uv.lock"
tech_stack:
  added:
    - "confluent_kafka.schema_registry.SchemaRegistryClient (real cached client)"
    - "confluent_kafka.schema_registry.avro.AvroDeserializer"
    - "confluent_kafka.schema_registry.protobuf.ProtobufDeserializer"
    - "authlib>=1.7.2 (confluent-kafka SR dependency, was missing)"
    - "cachetools>=7.1.4 (confluent-kafka SR dependency, was missing)"
    - "fastavro>=1.12.2 (required by AvroDeserializer)"
    - "protobuf>=7.35.0 (required by ProtobufDeserializer)"
    - "googleapis-common-protos>=1.75.0 (required by ProtobufDeserializer)"
  patterns:
    - "Adapter-level schema cache (dict[int, Schema]) prevents repeated SR round-trips"
    - "sr_pass passed directly to conf dict, never stored as instance attribute"
    - "All deserialization wrapped in try/except → DecodeError (T-02-02-A)"
    - "TDD RED->GREEN with individual commits per phase"
key_files:
  created: []
  modified:
    - "src/kafka_mcp/adapters/outbound/schema_registry_http.py"
    - "tests/test_adapters.py"
    - "pyproject.toml"
    - "uv.lock"
decisions:
  - "sr_pass passed directly to SchemaRegistryClient conf dict as basic.auth.user.info, not stored as instance attribute (T-02-02-B)"
  - "Adapter-level schema cache (self._schema_cache: dict[int, Schema]) ensures single SR lookup per schema_id per adapter lifetime"
  - "AvroDeserializer receives FULL raw bytes (magic+schema_id+payload) since it handles Confluent framing internally"
  - "ProtobufDeserializer limited: without pre-generated message classes, raises DecodeError with actionable reason (T-02-02-D); TODO Phase 3"
  - "authlib, cachetools, fastavro, protobuf, googleapis-common-protos added to pyproject.toml as direct dependencies (confluent-kafka SR requires them)"
metrics:
  duration: "~25 minutes"
  completed: "2026-06-06T00:00:00Z"
  tasks: 1
  files: 4
---

# Phase 2 Plan 2: SchemaRegistryHttpAdapter Real Implementation Summary

**One-liner:** Real SchemaRegistryHttpAdapter replacing Phase 1 stub — Confluent magic-byte framing detection dispatches to AvroDeserializer/ProtobufDeserializer with JSON fallback and typed DecodeError on all failure paths.

## Tasks Completed

| Task | Name | RED Commit | GREEN Commit | Tests Added |
|------|------|-----------|-------------|-------------|
| 1 | Real SchemaRegistryHttpAdapter decode pipeline | 0f8d6fd | 78e955b | 10 |

**Total new tests:** 10
**Full suite after plan:** 125 tests pass (up from 115 baseline)

## What Was Built

### Real SchemaRegistryHttpAdapter

`SchemaRegistryHttpAdapter` in `adapters/outbound/schema_registry_http.py`:

**Constructor:**
- `url=None` → `_client = None` (SR not configured; Confluent-framed payloads raise DecodeError)
- `url` provided → `SchemaRegistryClient({"url": url, ...})` instantiated once
- `basic.auth.user.info = f"{user}:{password}"` added to conf when both credentials provided
- Password NOT stored as instance attribute (T-02-02-B)

**`decode(raw, topic, partition, offset)` logic:**
1. `len(raw) >= 5 and raw[0] == 0x00` → Confluent framing path:
   - `_client is None` → raise `DecodeError(..., reason="Schema Registry not configured")`
   - Extract `schema_id = int.from_bytes(raw[1:5], "big")`
   - Look up `_schema_cache[schema_id]` (miss → `_client.get_schema(schema_id)` + cache)
   - `schema_type == "AVRO"` → `AvroDeserializer(_client, schema_str)` → call with full `raw`
   - `schema_type == "PROTOBUF"` → `ProtobufDeserializer(None, conf)` → call with full `raw`
   - Other schema_type → raise `DecodeError(..., reason="unknown schema type: {type}")`
   - Any exception → wrapped as `DecodeError` (T-02-02-A)
2. Else → `json.loads(raw)` → return dict; on failure raise `DecodeError(..., reason="json decode failed: ...")`

**Caching:** `self._schema_cache: dict[int, Schema]` — adapter-level cache prevents repeated SR HTTP calls for the same schema_id within a single adapter lifetime. Separate from SchemaRegistryClient's own internal LRU cache.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Missing confluent-kafka Schema Registry dependencies**
- **Found during:** Task 1 GREEN phase setup
- **Issue:** `confluent_kafka.schema_registry` failed to import with `ModuleNotFoundError: No module named 'authlib'`; similarly `fastavro` for AvroDeserializer and `protobuf`/`googleapis-common-protos` for ProtobufDeserializer were missing.
- **Root cause:** `confluent-kafka>=2.14` depends on these packages for Schema Registry support but doesn't declare them in its own package metadata; they must be explicitly added.
- **Fix:** Added `authlib>=1.7.2`, `cachetools>=7.1.4`, `fastavro>=1.12.2`, `protobuf>=7.35.0`, `googleapis-common-protos>=1.75.0` to `pyproject.toml` dependencies and generated `uv.lock`.
- **Files modified:** `pyproject.toml`, `uv.lock`
- **Commit:** 0f8d6fd (included with RED tests)

## Known Stubs

| File | Method | Reason |
|------|--------|--------|
| `src/kafka_mcp/adapters/outbound/schema_registry_http.py` | `_decode_protobuf()` | Protobuf decode with ProtobufDeserializer requires a pre-generated message class (generated Protobuf Python class from schema). Without one, DecodeError is raised with actionable reason. Full support deferred to Phase 3 (T-02-02-D). |

The Protobuf stub does not prevent the plan's goal (KAFKA-05 decode pipeline): Avro and JSON paths are fully functional. Protobuf decode without pre-generated types is a known Phase 3 enhancement, documented with a TODO comment.

## Threat Surface Scan

All threat mitigations from the plan's STRIDE register implemented:

| Threat ID | Status | Implementation |
|-----------|--------|----------------|
| T-02-02-A | Mitigated | All deserialization paths wrapped in `try/except`; any exception from AvroDeserializer/ProtobufDeserializer/json.loads caught and re-raised as typed `DecodeError` |
| T-02-02-B | Mitigated | `sr_pass` passed directly to `conf["basic.auth.user.info"]`; not stored as instance attribute; default object repr shows no attribute values |
| T-02-02-C | N/A for this plan | Message scan limits enforced in plan 02-03 consumer adapter, not here |
| T-02-02-D | Mitigated | `ProtobufDeserializer` with `None` message_type raises exception on unknown payload; wrapped as `DecodeError` with reason "protobuf decode failed: ..."; no arbitrary Python import or eval |

No new threat surface introduced beyond the planned decode boundary.

## Hexagonal Boundary Verification

```
grep -rn "import confluent_kafka|import fastavro|import avro|import google.protobuf" \
  src/kafka_mcp/domain/ src/kafka_mcp/ports/
```
Result: CLEAN (no output)

All decode library imports confined to `src/kafka_mcp/adapters/outbound/schema_registry_http.py`.

## TDD Gate Compliance

| Plan | RED commit | GREEN commit | Compliant |
|------|-----------|-------------|-----------|
| Task 1 | 0f8d6fd (test(02-02): add failing tests...) | 78e955b (feat(02-02): implement real...) | Yes |

## Self-Check: PASSED

Files exist:
- FOUND: src/kafka_mcp/adapters/outbound/schema_registry_http.py (contains SchemaRegistryClient, AvroDeserializer, DecodeError)

Commits exist: 0f8d6fd, 78e955b

Tests: 125 passed, 0 failed
- 10 new decode pipeline tests (TestSchemaRegistryDecodeJson, TestSchemaRegistryDecodeAvro, TestSchemaRegistryDecodeProtobuf, TestSchemaRegistryAdapterSecurity)
- All 37 prior adapter tests still pass
