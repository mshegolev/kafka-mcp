# Phase 5: Consumer Lag Tooling - Context

**Gathered:** 2026-06-16
**Status:** Ready for planning

<domain>
## Phase Boundary

Add a new read-only `consumer_group_lag` capability that reports per-partition lag
(committed offset vs end offset) for a given consumer group. Exposed identically
across all four faces (lib KafkaClient, MCP stdio, FastAPI `/tools/*`, CLI). Every
lag record carries Investigator-Contract Evidence fields. No writes, no offset
commits — the operation reads committed offsets via AdminClient and end offsets via
existing watermark queries.

In scope: new `LagRecord` model, `ConsumerPort.consumer_group_lag()`, AdminClient
integration in `ConfluentConsumerAdapter`, domain-level orchestration in
`TopicService` (or direct pass-through), all four inbound face adapters, unit tests
with mock adapter.

Out of scope: consumer group lifecycle management (create/delete/reset), live
offset monitoring/streaming, alert thresholds, historical lag tracking.
</domain>

<decisions>
## Implementation Decisions

### Consumer Port & librdkafka API Design
- Use `AdminClient.list_consumer_group_offsets()` to fetch committed offsets — reads without joining the group (read-only safe)
- Reuse existing `ConsumerPort.get_watermark_offsets()` per partition for end offsets (high watermarks) — already implemented and tested
- Add `consumer_group_lag(group, topics)` to `ConsumerPort` protocol + implement in `ConfluentConsumerAdapter` — follows hexagonal pattern, keeps admin client inside adapter
- Create `AdminClient` inside `ConfluentConsumerAdapter.__init__()` from the same broker config — no new port/adapter, one connection pool

### Response Model & Evidence Shape
- New `LagRecord(BaseModel)` with fields: `group`, `topic`, `partition`, `current_offset`, `end_offset`, `lag`, `timestamp_utc` — matches REQUIREMENTS LAG-03 exactly
- Partitions with no committed offset: report `current_offset=0, lag=end_offset` — shows full lag, alerting-friendly (consumer never committed = full backlog)
- `timestamp_utc` = current UTC time at query execution — lag is a point-in-time snapshot, not a message timestamp
- `topics: list[str] | None = None` — when None, report lag for ALL topics the group has committed offsets on

### 4-Face API Surface
- `KafkaClient.consumer_group_lag(group: str, topics: list[str] | None = None) -> list[LagRecord]` — matches existing pattern (simple args, list return)
- `ToolAnnotations(readOnlyHint=True)` on the MCP tool — required by success criterion #4
- CLI: `kafka-mcp consumer-group-lag --group GROUP [--topics T1,T2]` — tabular output, matches existing commands
- FastAPI: `POST /tools/consumer_group_lag` with pydantic request model — matches existing `/tools/*` pattern

### OpenCode's Discretion
- Exact AdminClient config dict construction, error handling for non-existent groups, test factoring, and internal method decomposition are at OpenCode's discretion, provided the 4-face symmetry and read-only guarantee hold.
</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `ConsumerPort` protocol (6 methods) — extend with `consumer_group_lag`
- `ConfluentConsumerAdapter` — already holds broker config; AdminClient uses same config dict
- `KafkaMessage` model in `domain/models.py` — pattern for `LagRecord`
- `_serialize_message` helpers in each inbound adapter — pattern for `_serialize_lag_record`
- Existing test mock adapter in test suite — extend with `consumer_group_lag` mock

### Established Patterns
- Hexagonal boundary: domain/ports stay import-free; librdkafka calls live in adapters/outbound
- 4-face symmetry: every capability has lib method, MCP tool, FastAPI route, CLI subcommand
- Resilient error handling: catch and wrap librdkafka errors into domain errors
- Read-only guarantee: assign-only, `enable.auto.commit=false`, throwaway group

### Integration Points
- `ports/consumer.py`: add `consumer_group_lag` method to `ConsumerPort` protocol
- `adapters/outbound/confluent_consumer.py`: implement with `AdminClient.list_consumer_group_offsets()` + `get_watermark_offsets()`
- `domain/models.py`: add `LagRecord` model
- `adapters/inbound/lib.py`: add `consumer_group_lag()` to `KafkaClient`
- `adapters/inbound/mcp_stdio.py`: register `consumer_group_lag` tool
- `adapters/inbound/rest_api.py`: add `POST /tools/consumer_group_lag`
- `adapters/inbound/cli.py`: add `consumer-group-lag` subcommand
</code_context>

<specifics>
## Specific Ideas

- `AdminClient.list_consumer_group_offsets()` returns `{TopicPartition: OffsetAndMetadata}` — iterate partitions, compute `lag = end_offset - current_offset`
- `LagRecord` Evidence fields: `source="kafka"`, `event_type="consumer_lag"` (distinct from `kafka_message`)
- When `topics=None`, derive topic list from committed offsets response (keys)
- Guard against empty group (no committed offsets) — return empty list, not error
</specifics>

<deferred>
## Deferred Ideas

- Historical lag tracking / time-series lag monitoring — out of scope, separate capability
- Consumer group lifecycle management (create/delete/reset) — write operations, violates read-only
- Alert thresholds for lag — application-level concern, not a brick feature
</deferred>
