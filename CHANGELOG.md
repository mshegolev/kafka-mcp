# Changelog

All notable changes to `kafka-mcp` are documented in this file.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
versioning: [SemVer](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2] - 2026-08-12

### Removed

- **`metadata.request.timeout.ms` dropped from the Consumer and AdminClient
  configs, and `metadata_timeout_ms` dropped from `KafkaMcpSettings`.**
  librdkafka accepts the property and then ignores it, saying so at client
  construction: `CONFWARN Configuration property metadata.request.timeout.ms
  is deprecated: Not used.` (observed in ep-stage pod logs and reproduced
  locally on confluent-kafka/librdkafka 2.14.2). It had been added alongside
  `socket.timeout.ms` as a second guard against librdkafka's unbounded
  reconnect loop — the loop that once held a search for 352 seconds under a
  60s budget — but it never guarded anything. Two timeouts read as two layers
  of defence; there was only ever one, and a reader looking for the cause of
  a hang would have stopped looking. `socket.timeout.ms` is unchanged and
  remains the live cap; it is now pinned by tests on both conf builders, with
  a companion test asserting the deprecated key stays out.

## [0.2.1] - 2026-08-07

### Fixed

- **`mcp` dependency bounded to `>=1.27,<2`.** `mcp 2.0.0` removed
  `mcp.server.fastmcp`, which `adapters/inbound/mcp_stdio.py` and
  `adapters/inbound/rest_api.py` import. Without an upper bound a clean
  install (`pip install -e '.[dev]'`, `pip install kafka-events-mcp`)
  resolved to `mcp 2.0.0` and failed at import time with
  `ModuleNotFoundError: No module named 'mcp.server.fastmcp'` — breaking
  both test collection and the installed `kafka-mcp` console script.
  `uv.lock` still pins `mcp 1.27.2`, so `uv sync` masked the breakage
  locally while pip-based CI and PyPI installs were broken.

## [0.2.0] - 2026-07-07

### Added

- **`correlate_messages` tool** (MCP stdio, HTTP MCP, REST) — correlate messages
  across topics by following extracted IDs, with advanced pattern matching
  (regex / JSONPath), bidirectional traversal, and configurable depth/breadth limits.
- **`consumer_group_lag` tool** — per-partition consumer lag (committed vs end offset)
  for a consumer group.
- **SSL client-certificate (mTLS) settings** on `KafkaMcpSettings` for
  authenticated broker connections.
- **Richer tool annotations** — all read-only tools now carry `idempotentHint=True`
  and `openWorldHint=True` in addition to `readOnlyHint=True`.

### Changed

- **PyPI distribution renamed to `kafka-events-mcp`** (`pip install kafka-events-mcp`);
  the import package `kafka_mcp` and the `kafka-mcp` CLI command are unchanged. The
  bare `kafka-mcp` name on PyPI belongs to an unrelated project.
- Publishing now uses OIDC Trusted Publishing instead of stored API tokens.
- Modernized domain typing to PEP 604 built-in generics (`list[str]`, `X | None`),
  dropping the legacy `typing.List/Optional/...` aliases.

### Fixed

- `search_messages` now raises an actionable `ValueError` (naming the parameter and
  expected format, accepting a trailing `Z`) instead of leaking the raw
  `datetime.fromisoformat` error to the MCP host.

## [0.1.0] - 2026-06-08

### Added

- **KafkaClient lib facade** — `from kafka_mcp import KafkaClient` for in-process use
  - `list_topics`: list all topic names visible on the broker
  - `describe_topic`: return partition count, replication factor, per-partition offsets
  - `search_messages`: scan partitions for messages matching a key in a time window
  - `get_message`: retrieve a single message by topic/partition/offset
  - Typed domain models: `KafkaMessage`, `TopicInfo`, `PartitionInfo`
- **Hexagonal architecture** — domain/, ports/, adapters/inbound/, adapters/outbound/
  - Clean domain core with no I/O or framework imports
  - Port interfaces: `KafkaBrokerPort`, `SchemaRegistryPort`
- **ConfluentConsumerAdapter** — read-only partition scan
  - Assign-based consumer (no `subscribe`, no offset commits, no group coordination)
  - Five-condition bounded loop: key-match, time-window, max-messages, partition-end, timeout
  - librdkafka-backed via confluent-kafka; no pure-Python fallback needed for the I/O layer
- **SchemaRegistryHttpAdapter** — schema-aware decode
  - Avro decode via fastavro + Confluent wire format (5-byte magic prefix)
  - Protobuf decode via grpcio-tools + googleapis-common-protos
  - JSON Schema passthrough with orjson parse
  - Schema caching by schema-id to avoid repeated registry fetches
- **All four inbound faces** (SC-4):
  - `KafkaClient` Python library — importable, no server needed
  - MCP stdio via FastMCP — compatible with Claude Desktop / Claude Code
  - FastAPI REST — POST /tools/list_topics, /tools/describe_topic,
    /tools/search_messages, /tools/get_message
  - CLI subcommands — `kafka-mcp list-topics`, `describe-topic`, `search`, `get`
- **Evidence extraction** — every `KafkaMessage` carries:
  - `source="kafka"`, `event_type="kafka_message"`
  - `keys`: `{order_id, msisdn, customer_id, product_id}` extracted from payload
  - Investigator Contract fields for cross-system evidence correlation
- **Scanner seam** (`scanner.py`) — automatic pure-Python fallback
  - `try: from kafka_mcp._native import scan_partition` guard
  - Falls back to pure-Python implementation transparently
  - Seam preserved for future Rust/pyo3 drop-in without API change
- **EVALUATION.md** — pytest-benchmark baseline and KAFKA-07 gate decision
  - Measured: ~25–50 ns/msg key-compare, ~446–479 ns/msg with orjson decode
  - Hot path is I/O-bound (librdkafka poll: 1–10 ms/batch); Rust not added in v1
  - Benchmark gate (KAFKA-07): Rust scanner deferred — CPU speedup not achievable
- **MIT License** — standard OSI canonical text

[0.1.0]: https://github.com/OWNER/kafka-mcp/releases/tag/v0.1.0
[0.2.0]: https://github.com/OWNER/kafka-mcp/compare/v0.1.0...v0.2.0
[0.2.1]: https://github.com/OWNER/kafka-mcp/compare/v0.2.0...v0.2.1
[0.2.2]: https://github.com/OWNER/kafka-mcp/compare/v0.2.1...v0.2.2
[Unreleased]: https://github.com/OWNER/kafka-mcp/compare/v0.2.2...HEAD
