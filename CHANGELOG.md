# Changelog

All notable changes to `kafka-mcp` are documented in this file.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
versioning: [SemVer](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
[Unreleased]: https://github.com/OWNER/kafka-mcp/compare/v0.1.0...HEAD
