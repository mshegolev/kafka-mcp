---
phase: "01"
plan: "01-03"
subsystem: "lib-facade"
tags: [domain, service, lib-facade, kafka-client, tdd, hexagonal, SC-1, SC-2, SC-3]
dependency_graph:
  requires:
    - kafka_mcp.domain.models (TopicInfo, PartitionInfo)
    - kafka_mcp.domain.errors (TopicNotFoundError, ConfigError)
    - kafka_mcp.ports.consumer (ConsumerPort)
    - kafka_mcp.config (KafkaMcpSettings)
    - kafka_mcp.adapters.outbound.confluent_consumer (ConfluentConsumerAdapter)
  provides:
    - kafka_mcp.domain.search_service (TopicService)
    - kafka_mcp.adapters.inbound.lib (KafkaClient)
    - kafka_mcp (top-level public API: KafkaClient, TopicInfo, PartitionInfo, errors)
  affects:
    - plan 01-04 (MCP stdio, FastAPI, CLI adapters all delegate to KafkaClient)
tech_stack:
  added:
    - TopicService domain service pattern (pure orchestration, zero I/O)
    - KafkaClient lib facade (DI constructor + from_env() classmethod)
    - get_partition_ids(topic) method on ConsumerPort + ConfluentConsumerAdapter
  patterns:
    - Constructor dependency injection for ConsumerPort (enables mock-based testing)
    - Classmethod factory (from_env) for production wiring
    - hexagonal boundary enforced by test SC-3 (subprocess grep check)
    - leader=0 placeholder per D-06 scope; AdminClient deferred to Phase 2
key_files:
  created:
    - src/kafka_mcp/domain/search_service.py
    - src/kafka_mcp/adapters/inbound/lib.py
    - tests/test_lib.py
  modified:
    - src/kafka_mcp/ports/consumer.py
    - src/kafka_mcp/adapters/outbound/confluent_consumer.py
    - src/kafka_mcp/adapters/inbound/__init__.py
    - src/kafka_mcp/__init__.py
    - src/kafka_mcp/config.py
    - tests/test_domain.py
decisions:
  - "get_partition_ids added to ConsumerPort and ConfluentConsumerAdapter as additive extension (not breaking)"
  - "leader=0 placeholder in PartitionInfo; AdminClient for real leader is Phase 2 per D-06"
  - "KafkaClient uses DI constructor + from_env() classmethod per Investigator Contract"
  - "config.py ConfigError wrapping extended to cover pydantic 'missing' type in addition to 'value_error'"
metrics:
  completed_date: "2026-06-05T12:20:39Z"
  duration_minutes: 38
  tasks_completed: 2
  files_created: 3
  files_modified: 6
  tests_added: 17
  tests_passing: 61
---

# Phase 01 Plan 01-03: TopicService + KafkaClient Lib Facade Summary

**One-liner:** TopicService domain service + KafkaClient lib facade with constructor DI, proving the hexagonal vertical slice end-to-end via 17 mock-based tests (no broker required).

## What Was Built

### Task 1 — TopicService domain service (TDD RED then GREEN)

RED gate: wrote 17 tests in `tests/test_lib.py` covering TopicService, KafkaClient, and Phase 1 success criteria SC-1/SC-2/SC-3. 16 tests failing (search_service.py and lib.py absent).

Also added `get_partition_ids(topic: str) -> list[int]` to:
- `ports/consumer.py` — abstract Protocol method
- `adapters/outbound/confluent_consumer.py` — implementation using `Consumer.list_topics(topic=topic)` metadata; raises `TopicNotFoundError` if topic absent

GREEN gate: created `domain/search_service.py`:
- `TopicService(consumer: ConsumerPort)` stores consumer as `_consumer`
- `list_topics(include_internal=False)` delegates to `_consumer.list_topics()`
- `describe_topic(topic)` calls `get_partition_ids(topic)` then `get_watermark_offsets(topic, pid)` for each partition; assembles `TopicInfo` with `PartitionInfo` objects; `leader=0` placeholder per D-06 (AdminClient deferred)
- Zero I/O imports — hexagonal boundary enforced

### Task 2 — KafkaClient lib facade (TDD GREEN)

Created `adapters/inbound/lib.py`:
- `KafkaClient(consumer: ConsumerPort)` stores consumer + instantiates `TopicService`
- `from_env()` classmethod: calls `KafkaMcpSettings()` → `ConfluentConsumerAdapter(settings)` → `KafkaClient(adapter)`
- `list_topics(include_internal=False)` and `describe_topic(topic)` delegate to `TopicService`

Updated `adapters/inbound/__init__.py` to export `KafkaClient`.

Updated `src/kafka_mcp/__init__.py`: replaced `TYPE_CHECKING` guard with real import from `adapters/inbound/lib`; all five public names importable at top level.

## Verification

```
pytest tests/ -v           →  61 passed in 0.12s
pytest tests/test_lib.py -k "success_criterion"  →  3/3 passed
grep -r "import confluent_kafka" src/kafka_mcp/domain/ → SC-3 boundary clean
python -c "from kafka_mcp import KafkaClient, ..."      → top-level imports OK
```

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Plan verify script false-positive: string `mcp` matches `kafka_mcp`**
- **Found during:** Task 1 verification (plan's `if bad in src` check)
- **Issue:** The plan's Task 1 verify script uses `if bad in src` with `'mcp'` in the bad list. Since `search_service.py` imports from `kafka_mcp.*`, the string `mcp` appears in every import statement, triggering a false positive. Same root cause as deviation #2 in plan 01-01.
- **Fix:** Used import-aware regex check instead of naive string match for actual verification. Module docstring rewrote to not mention I/O library names. The real hexagonal boundary assertion (`grep -r "import confluent_kafka" domain/`) passes cleanly.
- **Files modified:** `src/kafka_mcp/domain/search_service.py` (docstring)
- **Commit:** 344522f

**2. [Rule 1 - Bug] ConfigError wrapping didn't handle pydantic `missing` field type**
- **Found during:** Task 2 (test_from_env_raises_config_error_when_no_broker failed)
- **Issue:** When `KAFKA_MCP_BOOTSTRAP_SERVERS` is absent from environment, pydantic raises `ValidationError` with error type `missing` (not `value_error`). The `__init__` override in `config.py` only converted `value_error` type to `ConfigError`, so the `ValidationError` was re-raised as-is — violating the D-04 fail-fast contract.
- **Fix:** Extended `__init__` to also handle `missing` type errors by constructing `ConfigError("KAFKA_MCP_<FIELD> is required but was not set")`. Message names key only, never value (T-03-01 STRIDE mitigation).
- **Files modified:** `src/kafka_mcp/config.py`
- **Commit:** 4d76a6b

**3. [Rule 1 - Bug] test_domain.py MockConsumer missing get_partition_ids broke isinstance check**
- **Found during:** Task 2 full suite run (test_compliant_class_passes_isinstance failed)
- **Issue:** Adding `get_partition_ids` to `ConsumerPort` protocol meant the 2-method MockConsumer in `test_domain.py` no longer satisfied `isinstance(obj, ConsumerPort)` — runtime_checkable Protocol checks all methods.
- **Fix:** Added `get_partition_ids(self, topic: str) -> list[int]: return [0]` to MockConsumer in `tests/test_domain.py`.
- **Files modified:** `tests/test_domain.py`
- **Commit:** 4d76a6b

## TDD Gate Compliance

- RED commit (`test(01-03): add failing tests...`): `6a510f9` — 16 fail, 1 pass (SC-3 was already verifiable)
- GREEN Task 1 commit (`feat(01-03): implement TopicService...`): `344522f` — 7/7 TopicService tests pass
- GREEN Task 2 commit (`feat(01-03): implement KafkaClient lib facade...`): `4d76a6b` — 61/61 all tests pass
- REFACTOR: not needed (code is clean)

## Known Stubs

- `PartitionInfo.leader` is always `0`. This is an intentional Phase 1 placeholder per D-06 (AdminClient-based leader metadata deferred to Phase 2). No UI rendering depends on this value in Phase 1.

## Threat Flags

No new threat surface beyond the plan's `<threat_model>`:
- T-03-01 mitigated: ConfigError message names only the missing key name (e.g. `KAFKA_MCP_BOOTSTRAP_SERVERS`); never includes any value. Verified in test_from_env_raises_config_error_when_no_broker.
- T-03-02 mitigated: hexagonal boundary assertion lives in tests/test_lib.py as test_phase1_success_criterion_3_hexagonal_boundary — runs on every CI pass.
- T-03-03 mitigated: get_partition_ids scoped to named topic; TopicNotFoundError prevents describe of non-existent topics; test_describe_topic_unknown_raises verifies this.
- T-03-04 mitigated: partition list bounded by actual broker metadata; no unbounded loop.

## Self-Check: PASSED

Files confirmed present:
- src/kafka_mcp/domain/search_service.py ✓
- src/kafka_mcp/adapters/inbound/lib.py ✓
- tests/test_lib.py ✓

Commits confirmed:
- 6a510f9 (RED) ✓
- 344522f (GREEN Task 1) ✓
- 4d76a6b (GREEN Task 2) ✓
