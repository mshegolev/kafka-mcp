# Phase 1: Foundation - Context

**Gathered:** 2026-06-05
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 1 stands up the hexagonal v2 skeleton for a read-only Kafka MCP brick and
delivers the first two read-only operations end-to-end across all four inbound
faces. In scope:

- `src/kafka_mcp/` hexagonal layout: `domain/` (pure), `ports/` (protocols),
  `adapters/inbound/` (lib, mcp_stdio, rest_api, cli), `adapters/outbound/`
  (confluent consumer, schema_registry_http stub), `config.py`, `server.py`.
- `KafkaClient` lib facade with `list_topics()` and `describe_topic(name)`.
- confluent-kafka (librdkafka) broker adapter behind a `ConsumerPort` protocol.
- Schema Registry adapter wired but only stubbed/connected (full decode is
  Phase 2 — KAFKA-05).
- Structural read-only guarantee: assign-based temporary consumer-group, no
  offset commits, `readOnlyHint: true` on every MCP tool.
- All four faces reachable: lib import, MCP stdio, FastAPI `POST /tools/*`, CLI.

Out of scope (later phases): `search_messages`, `get_message`, message-body
decode (Phase 2); Rust scanner, CI wheels, Glama publish (Phase 3).

Requirements: KAFKA-01 (list_topics), KAFKA-04 (describe_topic), KAFKA-06
(read-only guarantee).

</domain>

<decisions>
## Implementation Decisions

### Connection & Configuration
- `KafkaClient.from_env()` reads config via a pydantic v2 `BaseSettings` class
  with env prefix `KAFKA_MCP_` (e.g. `KAFKA_MCP_BOOTSTRAP_SERVERS`,
  `KAFKA_MCP_SCHEMA_REGISTRY_URL`). Aligns with the pydantic-v2 stack decision.
- Broker auth: support `PLAINTEXT`, `SASL_PLAINTEXT`, `SASL_SSL` via passthrough
  fields (`security_protocol`, `sasl_mechanism`, `sasl_username`,
  `sasl_password`) handed to librdkafka. No mTLS in v1.
- Schema Registry auth: optional basic-auth (`KAFKA_MCP_SR_USER`,
  `KAFKA_MCP_SR_PASS`); unset = anonymous.
- Invalid/missing config fails fast inside `from_env()` raising a typed
  `ConfigError` that names the missing/invalid keys (not a lazy failure on first
  broker call).

### Read-only Scan Mechanics (KAFKA-06)
- Use confluent-kafka `Consumer.assign()` directly to partitions (never
  `subscribe()`), with `enable.auto.commit=false` and a throwaway group id
  `kafka-mcp-ro-{uuid4}`. Assign-based consumption structurally cannot commit
  offsets to production groups — this is the primary read-only guarantee.
- `describe_topic` derives earliest/latest offsets via
  `get_watermark_offsets()` per partition.
- Scan is bounded by a global `limit` plus a configurable per-partition cap
  `KAFKA_MCP_MAX_SCAN` (default 100_000).
- Consumer poll uses a configurable `poll_timeout` (default 1.0s) and an overall
  scan deadline so scans always terminate.

### Tool Output Contracts
- `list_topics` excludes internal (`__`-prefixed) topics by default; exposes an
  `include_internal: bool = False` flag.
- `describe_topic` returns a pydantic `TopicInfo` model:
  `TopicInfo{ name: str, partition_count: int,
  partitions: list[PartitionInfo{ id: int, leader: int, earliest: int,
  latest: int }] }`.
- Unknown topic raises a typed `TopicNotFoundError` domain error (not `None`).
- All response models live in `domain/models.py` as pydantic v2 models; the
  `domain/` layer imports zero I/O or framework libraries (hexagonal boundary).

### Inbound Faces & MCP Annotations
- MCP tool names are snake_case and match the lib method names exactly
  (`list_topics`, `describe_topic`).
- Read-only is surfaced two ways (defense in depth): `readOnlyHint: true` on
  every MCP tool annotation AND the structural assign-based no-commit consumer.
- CLI prints a human-readable table by default and accepts a `--json` flag for
  machine-readable output.
- FastAPI exposes `POST /tools/{tool_name}` taking a JSON body, mirroring the
  MCP tool-call convention (not REST-style resource routes).

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- The referenced "v2 hexagonal skeleton" in `graphql-mcp` is itself still
  planning-only (no source yet), so this phase builds the skeleton fresh rather
  than copying files. Use the layout described in PROJECT.md as the template.

### Established Patterns
- Sibling bricks in `/opt/develop/aiqa/mcps/` (jaeger-mcp, kibana-mcp,
  ordering-mcp) share the Investigation MCP Suite conventions — consult their
  structure during plan research for naming/packaging consistency.

### Integration Points
- Inbound adapters (lib/stdio/FastAPI/CLI) all call into the same
  `domain/search_service`-style service + `ports`; outbound adapters implement
  the `ConsumerPort` / `SchemaRegistryPort` protocols.

</code_context>

<specifics>
## Specific Ideas

- Stack is fixed by umbrella decisions: `confluent-kafka>=2.14` (librdkafka, NOT
  kafka-python), `mcp>=1.27`, FastAPI+uvicorn, pydantic v2, orjson.
- Schema Registry adapter should be wired in Phase 1 but full Avro/Protobuf/JSON
  decode is deferred to Phase 2 (KAFKA-05).

</specifics>

<deferred>
## Deferred Ideas

- mTLS for broker and Schema Registry — out of scope for v1.
- AdminClient-based offset/metadata APIs — watermark offsets suffice for v1.

</deferred>
