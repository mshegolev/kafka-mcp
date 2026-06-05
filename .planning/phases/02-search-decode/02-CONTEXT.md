# Phase 2: Search + Decode - Context

**Gathered:** 2026-06-05
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 2 makes investigators able to find events by key within a time window and
retrieve decoded message bodies. It builds directly on the Phase 1 hexagonal
skeleton (KafkaClient, ConsumerPort, SchemaRegistryPort, assign-based read-only
consumer). In scope:

- `search_messages(key, *, key_field, topics, time_from, time_to, limit)` on the
  domain search service + `KafkaClient`, reachable via all four faces.
- `get_message(topic, partition, offset)` returning a single decoded message.
- A decode pipeline that turns Avro / Protobuf / plain-JSON wire payloads into a
  plain `dict`, via Schema Registry, behind the `SchemaRegistryPort` (the Phase 1
  stub is replaced with a real implementation here).
- A `KafkaMessage` domain model carrying the Investigator-Contract Evidence
  fields (`source`, `event_type`, `timestamp_utc`, `keys{...}`, decoded `value`,
  `raw`).

Requirements: KAFKA-02 (search_messages), KAFKA-03 (get_message), KAFKA-05
(Avro/Protobuf/JSON decode via Schema Registry).

Out of scope (Phase 3): Rust partition scanner, benchmark gating, CI wheels,
Glama publish. Produce / consumer-group management / schema writes remain out of
scope for the whole v1.

</domain>

<decisions>
## Implementation Decisions

### Search Strategy & Key Matching
- Seek to `time_from` via confluent-kafka `offsets_for_times()` per partition,
  then `assign()` and consume forward until `time_to` or `limit` is reached
  (re-uses the Phase 1 assign-based read-only consumer; never `subscribe`).
- `key_field` semantics: by default the search matches the message **key**.
  `key_field` may be `header:<name>` to match a header value, or
  `value:<dotted.path>` to match a field inside the decoded value.
- When `topics` is omitted (None), scan all non-internal topics, bounded by the
  global `limit` and the per-partition `KAFKA_MCP_MAX_SCAN` cap.
- `limit` is GLOBAL across the whole search (all topics/partitions); the scan
  stops early as soon as `limit` matches are collected.

### Decode Pipeline (KAFKA-05)
- Wire-format detection uses the Confluent framing convention: a leading magic
  byte `0x00` followed by a 4-byte big-endian schema id means a registry-backed
  payload — look the schema up via Schema Registry and decode (Avro or Protobuf
  per the registered schema type). Otherwise attempt `json.loads`; if that
  fails, raise a typed `DecodeError` domain error.
- Use confluent-kafka's `AvroDeserializer` / `ProtobufDeserializer` plus
  `SchemaRegistryClient`, wired inside the outbound Schema Registry adapter
  behind `SchemaRegistryPort` (domain stays decode-library-agnostic).
- Decode-failure behavior differs by operation: in `search_messages`, a decode
  failure is captured per message (`value=None`, `raw` retained, error noted) so
  a single bad record does not abort the whole scan; in `get_message` (single
  record), the typed `DecodeError` is raised. This satisfies success criterion 3
  (unknown format → typed domain error) for the single-message path while
  keeping bulk search resilient.
- Decode the value only; surface `key` as a best-effort UTF-8 string. Keys are
  not run through Schema Registry in v1.

### KafkaMessage / Evidence Contract
- `KafkaMessage` is a pydantic v2 model in `domain/models.py`:
  `{topic, partition, offset, key: str | None, headers: dict[str, str],
  value: dict | None, timestamp_utc: datetime, raw: bytes}`. Domain stays
  I/O-free (hexagonal boundary holds).
- Evidence identifiers (`order_id`, `msisdn`, `customer_id`, `product_id`) are
  extracted from the decoded `value` and `headers` via a configurable
  well-known-name map and surfaced as a `keys` dict; absent identifiers are
  None. `source="kafka"` and `event_type="kafka_message"` are constant.
- In serialized faces (REST / MCP / CLI), `raw` is base64-encoded; the in-process
  domain object keeps `bytes`.
- `timestamp_utc` is derived from the message CreateTime converted to a
  UTC-aware datetime; when the timestamp type is LogAppendTime or unset (-1),
  fall back gracefully and note it (clock-skew pitfall from the brief).

### get_message, Errors & Limits
- `get_message` with an out-of-range offset (beyond watermarks / no message)
  raises a typed `MessageNotFoundError` domain error (not None).
- When the search window is omitted, default `time_from` to earliest and
  `time_to` to now; the scan is always additionally bounded by `limit` and
  `KAFKA_MCP_MAX_SCAN`.
- Reuse the Phase 1 scan-bound knobs (`KAFKA_MCP_MAX_SCAN` per-partition cap,
  global `limit`, `poll_timeout` deadline); do not introduce new scan knobs.
- Wire a real confluent `SchemaRegistryClient` (which caches schemas by id)
  behind the Schema Registry adapter, replacing the Phase 1 stub.

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- Phase 1 delivered: `domain/models.py` (TopicInfo/PartitionInfo — add
  KafkaMessage here), `domain/errors.py` (ConfigError/TopicNotFoundError — add
  DecodeError/MessageNotFoundError here), `ports/consumer.py` (ConsumerPort —
  extend with a message-fetch/scan method), `ports/schema_registry.py`
  (SchemaRegistryPort — implement for real), `adapters/outbound/
  confluent_consumer.py` (assign-based read-only consumer), `config.py`
  (KafkaMcpSettings, KAFKA_MCP_ prefix, max_scan/poll_timeout), and the four
  inbound faces delegating to `KafkaClient`.

### Established Patterns
- Hexagonal boundary: domain/ + ports/ import zero I/O libs (enforced by a
  test in tests/test_lib.py). Decode libraries live in the outbound adapter.
- Read-only guarantee: assign-only, `enable.auto.commit=false`,
  `kafka-mcp-ro-{uuid4}` group id. Search must consume the same way.
- Typed domain errors over None; pydantic v2 models for all returned shapes;
  orjson helpers for compact serialization; base64 already a natural fit for raw.
- Per-message error discrimination pattern already established in Phase 1
  (TopicNotFound only on unknown-topic codes; transient errors re-raise).

### Integration Points
- Inbound adapters add `search_messages` and `get_message` tools/routes/CLI
  subcommands mirroring the Phase 1 list_topics/describe_topic wiring
  (snake_case MCP names + readOnlyHint:true, POST /tools/{tool_name},
  CLI table + --json).

</code_context>

<specifics>
## Specific Ideas

- Investigator Contract (PROJECT.md): each returned message becomes a timeline
  event. `timestamp_utc` correctness is critical (clock-skew pitfall). Evidence
  shape: `Evidence{ source="kafka", event_type="kafka_message", timestamp_utc,
  keys{order_id, msisdn, customer_id, product_id}, payload=value, raw }`.
- Stack remains fixed: confluent-kafka>=2.14, pydantic v2, orjson. Use
  confluent-kafka's Schema Registry + Avro/Protobuf deserializers rather than
  hand-rolling wire parsing.

</specifics>

<deferred>
## Deferred Ideas

- Decoding message keys via Schema Registry — value-only in v1.
- Exposing `schema_id` on KafkaMessage — not required by the contract in v1.
- Rust-accelerated scan — Phase 3, benchmark-gated (KAFKA-07).

</deferred>
