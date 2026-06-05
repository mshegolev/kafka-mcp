---
phase: "01"
plan: "01-01"
subsystem: "domain-scaffold"
tags: [scaffold, domain, ports, config, pydantic, hexagonal]
dependency_graph:
  requires: []
  provides:
    - kafka_mcp.domain.models (TopicInfo, PartitionInfo)
    - kafka_mcp.domain.errors (TopicNotFoundError, ConfigError)
    - kafka_mcp.ports.consumer (ConsumerPort)
    - kafka_mcp.ports.schema_registry (SchemaRegistryPort)
    - kafka_mcp.config (KafkaMcpSettings)
  affects:
    - all subsequent plans depend on these domain contracts and pyproject.toml
tech_stack:
  added:
    - pydantic v2 BaseModel (domain models)
    - pydantic-settings v2 BaseSettings (KafkaMcpSettings)
    - hatchling >= 1.24 (build backend)
    - confluent-kafka >= 2.14 (declared in deps; not used in domain layer)
    - mcp >= 1.27 (declared in deps; not used in domain layer)
    - fastapi + uvicorn, orjson, httpx (declared in deps)
    - pytest 8, pytest-asyncio 0.23, ruff 0.5 (dev)
  patterns:
    - hexagonal architecture (domain/ has zero I/O imports)
    - runtime_checkable Protocol for port contracts
    - pydantic @model_validator(mode="after") for fail-fast validation
    - SecretStr for SASL/SR credentials (T-01-01 STRIDE mitigation)
    - custom __init__ to unwrap pydantic ValidationError into ConfigError
key_files:
  created:
    - pyproject.toml
    - .gitignore
    - .env.example
    - src/kafka_mcp/__init__.py
    - src/kafka_mcp/domain/__init__.py
    - src/kafka_mcp/domain/models.py
    - src/kafka_mcp/domain/errors.py
    - src/kafka_mcp/ports/__init__.py
    - src/kafka_mcp/ports/consumer.py
    - src/kafka_mcp/ports/schema_registry.py
    - src/kafka_mcp/adapters/__init__.py
    - src/kafka_mcp/adapters/inbound/__init__.py
    - src/kafka_mcp/adapters/outbound/__init__.py
    - src/kafka_mcp/config.py
    - tests/__init__.py
    - tests/test_domain.py
  modified: []
decisions:
  - "bootstrap_servers required field; empty/whitespace raises ConfigError immediately (D-04 fail-fast)"
  - "sasl_password and sr_pass use SecretStr to prevent credential leakage in repr/logs (T-01-01)"
  - "pydantic ValidationError unwrapped to ConfigError in __init__ so callers have single domain type"
  - "port docstrings use neutral language (no banned lib names) to pass boundary grep check"
metrics:
  completed_date: "2026-06-05T10:52:57Z"
  duration_minutes: 42
  tasks_completed: 2
  files_created: 16
  tests_added: 20
  tests_passing: 20
---

# Phase 01 Plan 01-01: Project Scaffold + Domain Contracts Summary

**One-liner:** Hexagonal project scaffold with pydantic v2 domain models, typed port protocols, and fail-fast `KafkaMcpSettings` using `KAFKA_MCP_` env prefix.

## What Was Built

### Task 1 (TDD RED + scaffold)
Built the full hexagonal directory layout under `src/kafka_mcp/` and defined all domain contracts:

- `pyproject.toml` — hatchling build backend, all runtime deps (`mcp`, `confluent-kafka`, `fastapi`, `uvicorn`, `pydantic`, `pydantic-settings`, `orjson`, `httpx`), dev deps, ruff config, pytest config
- `domain/models.py` — `PartitionInfo(id, leader, earliest, latest)` and `TopicInfo(name, partition_count, partitions)` as pydantic v2 BaseModel
- `domain/errors.py` — `ConfigError(ValueError)` and `TopicNotFoundError(Exception)` with `.topic` attribute
- `ports/consumer.py` — `ConsumerPort` runtime_checkable Protocol with `list_topics()` and `get_watermark_offsets()`
- `ports/schema_registry.py` — `SchemaRegistryPort` runtime_checkable Protocol with `get_schema()`
- All placeholder `__init__.py` files for hexagonal layers
- `tests/test_domain.py` — 20 no-broker tests written (RED: 14 passing, 6 failing for config)

### Task 2 (TDD GREEN — config)
Implemented `KafkaMcpSettings`:

- `config.py` — `KafkaMcpSettings(BaseSettings)` with `KAFKA_MCP_` prefix
- All 10 fields: `bootstrap_servers`, `security_protocol`, `sasl_mechanism`, `sasl_username`, `sasl_password` (SecretStr), `schema_registry_url`, `sr_user`, `sr_pass` (SecretStr), `max_scan=100_000`, `poll_timeout=1.0`
- `@model_validator(mode="after")` raises `ConfigError` on missing/empty `bootstrap_servers`
- `__init__` catches pydantic `ValidationError` and re-raises as pure `ConfigError` (D-04 contract)

## Verification

```
pytest tests/test_domain.py -v    →  20 passed in 0.09s
hexagonal boundary check          →  hexagonal boundary OK
pip install -e .[dev]             →  succeeds (confluent-kafka 2.14.2 installed)
```

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] README.md referenced in pyproject.toml but absent**
- **Found during:** Task 1 (pip install -e .[dev])
- **Issue:** `pyproject.toml` had `readme = "README.md"` but the file doesn't exist
- **Fix:** Removed `readme` field from pyproject.toml; not required for Phase 1
- **Files modified:** `pyproject.toml`
- **Commit:** 5b8d66f

**2. [Rule 1 - Bug] Boundary grep check caught docstring false-positives**
- **Found during:** Task 1 verification
- **Issue:** The plan's boundary check uses simple `if bad in src` string search. Docstring phrases like "kafka-mcp domain", "no confluent_kafka import here", "no httpx or requests import here" triggered false positives
- **Fix:** Rewrote docstrings to use neutral language: "Kafka domain layer", "no broker library imports", "no HTTP library imports"
- **Files modified:** `domain/errors.py`, `ports/consumer.py`, `ports/schema_registry.py`
- **Commit:** 1676e79

**3. [Rule 1 - Bug] pydantic wraps ConfigError in ValidationError**
- **Found during:** Task 2 (test_empty_bootstrap_servers_raises_config_error failed)
- **Issue:** `@model_validator` raises `ConfigError` but pydantic catches all `ValueError` subclasses and wraps them in `ValidationError`, breaking the D-04 fail-fast contract
- **Fix:** Added `__init__` override that catches `ValidationError` and re-raises the inner `ConfigError`
- **Files modified:** `src/kafka_mcp/config.py`
- **Commit:** 1676e79

## TDD Gate Compliance

- RED commit (`test(01-01): add failing tests...`): `5b8d66f` — 14 pass, 6 fail
- GREEN commit (`feat(01-01): implement KafkaMcpSettings...`): `1676e79` — 20/20 pass
- REFACTOR: not needed (code is clean)

## Known Stubs

None — this plan defines contracts, not data-flow. No UI rendering, no placeholder data.

## Threat Flags

No new threat surface beyond what the plan's `<threat_model>` identified.
T-01-01 (SecretStr), T-01-02 (boundary assertion), T-01-SC (known packages) — all mitigated as specified.

## Self-Check: PASSED

Files confirmed present:
- src/kafka_mcp/domain/models.py ✓
- src/kafka_mcp/domain/errors.py ✓
- src/kafka_mcp/ports/consumer.py ✓
- src/kafka_mcp/ports/schema_registry.py ✓
- src/kafka_mcp/config.py ✓
- tests/test_domain.py ✓
- pyproject.toml ✓

Commits confirmed:
- 5b8d66f (RED) ✓
- 1676e79 (GREEN) ✓
