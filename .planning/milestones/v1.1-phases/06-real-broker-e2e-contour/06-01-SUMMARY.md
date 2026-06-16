---
phase: "06-real-broker-e2e-contour"
plan: "01"
subsystem: "integration-tests"
tags: [testcontainers, kafka, schema-registry, integration-tests, pytest]
dependency_graph:
  requires: []
  provides: [integration-test-infrastructure, session-scoped-containers, seed-json-topic]
  affects: [pyproject.toml, confluent_consumer.py]
tech_stack:
  added: [testcontainers-python, pytest-timeout]
  patterns: [session-scoped-fixtures, docker-host-auto-detection, graceful-skip]
key_files:
  created:
    - tests/integration/__init__.py
    - tests/integration/conftest.py
    - tests/integration/test_basic_ops.py
  modified:
    - pyproject.toml
    - src/kafka_mcp/adapters/outbound/confluent_consumer.py
decisions:
  - "Increased Kafka container startup timeout to 120s for Colima/emulated environments"
  - "Added DOCKER_HOST auto-detection from docker context for Colima compatibility"
  - "SR health check uses 120s timeout (SR takes ~90s on emulated x86_64)"
  - "Fixed pre-existing get_watermark_offsets bug: now uses TopicPartition object"
metrics:
  duration: "40m 17s"
  completed: "2026-06-15T19:14:52Z"
  tests_added: 5
  tests_passed: 5
  unit_tests_passed: 298
  unit_tests_deselected: 5
---

# Phase 06 Plan 01: Testcontainers Fixtures + Basic Integration Tests Summary

**One-liner:** Session-scoped Kafka + Schema Registry testcontainers with Docker-host auto-detection, 5 real-wire list/describe tests, and graceful Docker-unavailable skip.

## What Was Done

### Task 1: Declare testcontainers dep + register integration marker
- Added `testcontainers[kafka]>=4.4` and `pytest-timeout>=2.3` to `[project.optional-dependencies] dev`
- Registered `integration` marker in `[tool.pytest.ini_options].markers`
- Set `addopts = "-m 'not integration'"` so `pytest` (no args) auto-excludes integration tests
- Installed dependencies via `pip install -e ".[dev]"`

### Task 2: Integration conftest fixtures + basic connectivity tests
- **`tests/integration/__init__.py`**: Package marker (empty)
- **`tests/integration/conftest.py`** (234 lines, 7 fixtures):
  - `_ensure_docker_host()`: Auto-detects Docker socket from `docker context inspect` for Colima/non-default Docker setups
  - `kafka_container` (session): KafkaContainer with 120s startup timeout; graceful skip on Docker unavailable
  - `sr_container` (session): Schema Registry connected to Kafka via internal bridge IP; 120s health check
  - `bootstrap_servers` / `schema_registry_url`: Address convenience fixtures
  - `kafka_settings`: KafkaMcpSettings wired to real containers (poll_timeout=5.0)
  - `kafka_client`: Full KafkaClient with real ConfluentConsumerAdapter + SchemaRegistryHttpAdapter
  - `seed_json_topic`: Seeds "test-json" with 5 JSON messages via confluent_kafka.Producer
- **`tests/integration/test_basic_ops.py`** (55 lines, 5 tests):
  - `test_list_topics_returns_seeded_topic` — seeded topic appears in list
  - `test_list_topics_excludes_internal` — no `__`-prefixed topics
  - `test_list_topics_includes_internal_when_requested` — `__consumer_offsets` visible
  - `test_describe_topic_returns_real_partition_info` — TopicInfo with real offsets
  - `test_describe_topic_unknown_raises` — TopicNotFoundError for nonexistent topic

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Docker host auto-detection for Colima**
- **Found during:** Task 2, initial fixture run
- **Issue:** `docker.from_env()` uses `/var/run/docker.sock` by default; Colima uses `~/.colima/amd64/docker.sock`. KafkaContainer constructor failed with `ConnectionRefusedError`.
- **Fix:** Added `_ensure_docker_host()` helper that queries `docker context inspect` and sets `DOCKER_HOST` env var if needed.
- **Files modified:** tests/integration/conftest.py

**2. [Rule 3 - Blocking] Network IP extraction for inter-container communication**
- **Found during:** Task 2, SR container setup
- **Issue:** `container_info.attrs["NetworkSettings"]["IPAddress"]` returns empty/None on some Docker backends. IP is inside `Networks.bridge` sub-dict.
- **Fix:** Fall back to scanning `Networks` dict entries for a valid `IPAddress`.
- **Files modified:** tests/integration/conftest.py

**3. [Rule 1 - Bug] get_watermark_offsets API mismatch**
- **Found during:** Task 2, `describe_topic` integration test
- **Issue:** `Consumer.get_watermark_offsets(topic, partition, timeout=X)` fails with `TypeError: argument for function given by name ('timeout') and position (2)`. The confluent-kafka 2.x API expects `TopicPartition` object, not `(topic, partition)` positional args.
- **Fix:** Changed to `get_watermark_offsets(TopicPartition(topic, partition), timeout=X)`.
- **Files modified:** src/kafka_mcp/adapters/outbound/confluent_consumer.py
- **Commit:** f4e6b7d

**4. [Rule 3 - Blocking] Container startup timeouts for emulated environments**
- **Found during:** Task 2, multiple test runs
- **Issue:** Default KafkaContainer 30s timeout and SR 30s health check too short for Colima x86_64-on-ARM emulation (~60s for Kafka, ~90s for SR).
- **Fix:** Kafka startup timeout → 120s, SR health check → 120s with diagnostics on failure.
- **Files modified:** tests/integration/conftest.py

## Verification Results

| Check | Result |
|-------|--------|
| `pytest --co -q` (no args) | 303 collected, 5 deselected → 298 unit tests |
| `pytest tests/ -x --timeout=30` | 298 passed, 5 deselected |
| `pytest -m integration -x -v --timeout=300` | 5 passed in 172s |
| `ruff check src/ tests/` | All checks passed |
| Docker-unavailable skip | Verified: graceful pytest.skip on Docker errors |

## Decisions Made

1. **120s container startup budget**: Colima emulation is significantly slower than native Docker; generous timeouts prevent false test failures while remaining bounded.
2. **Auto-detect Docker host**: Rather than requiring manual `DOCKER_HOST` setup, the conftest auto-detects the active Docker context endpoint — zero-config for CI and local dev.
3. **Fixed pre-existing watermark API bug**: `get_watermark_offsets` was passing `(topic, partition)` positionally instead of `TopicPartition(topic, partition)`. Unit tests never caught this because they mock the consumer.

## Self-Check: PASSED
