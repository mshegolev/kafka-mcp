# Requirements: kafka-mcp — Milestone v1.2

**Defined:** 2026-06-16
**Core Value:** Read-only Kafka MCP brick — find events by key in a time window,
decode Avro/Protobuf/JSON via Schema Registry, surface evidence for incident
timelines. Library-first: works in pytest without MCP or FastAPI.

> v1.0 (KAFKA-01..07) shipped 2026-06-08. v1.1 (KEY-01..03, HTTP-01, LAG-01..03,
> E2E-01..03, REL-01..02) shipped 2026-06-16. v1.2 extends the investigation
> workflow from single-topic search to cross-topic entity tracing.

## v1.2 Requirements

Requirements for the v1.2 release. Each maps to exactly one roadmap phase.

### Multi-topic search (MTS)

- [ ] **MTS-01**: `search_messages` accepts `topics: list[str]` (in addition to
  the existing single-topic path) and returns results merged and sorted by
  `timestamp_utc` across all specified topics in a single call.
- [ ] **MTS-02**: Multi-topic search preserves all existing single-topic behavior
  (key filter, `time_from`/`time_to`, `limit`, Avro/Protobuf/JSON decode) with
  no regressions — existing callers passing a single topic see identical results.

### Header filtering (HDR)

- [ ] **HDR-01**: `search_messages` accepts an optional `headers: dict[str, str]`
  parameter to filter messages whose Kafka headers contain all specified
  key-value pairs (e.g. `headers={"trace_id": "abc-123"}`).
- [ ] **HDR-02**: Header filtering combines with the existing key + time window +
  topics filters using AND semantics — a message must match all active filters
  to appear in results.

### Correlation (COR)

- [ ] **COR-01**: A new `correlate_messages` capability accepts initial search
  results (or a starting key + topics) and extracts correlated entity IDs by
  scanning message values and headers for configurable ID field patterns
  (e.g. `trace_id`, `order_id`, `parent_id`).
- [ ] **COR-02**: `correlate_messages` follows extracted IDs into additional
  topics (specified or auto-discovered from the data) to build a cross-service
  event chain, returning all correlated messages merged and sorted by timestamp.
- [ ] **COR-03**: Correlation output carries Investigator-Contract Evidence fields
  (`source="kafka"`, `event_type="correlated_message"`, `timestamp_utc`, keys)
  and includes a `correlation_chain` field linking each message to the ID path
  that discovered it, so each result is usable as a timeline data point.

### 4-face symmetry (SYM)

- [ ] **SYM-01**: All new and extended capabilities (multi-topic search, header
  filter, `correlate_messages`) are exposed identically across lib `KafkaClient`,
  MCP stdio tool, FastAPI `/tools/*` POST endpoint, and `kafka-mcp` CLI
  subcommand — all return the same schema.

## Future Requirements

Deferred to a later milestone. Tracked but not in this roadmap.

### Correlation — advanced

- **COR-04**: Recursive correlation depth control — limit how many hops
  `correlate_messages` follows to prevent unbounded fan-out.
- **COR-05**: Correlation caching — cache extracted ID mappings to speed
  repeated correlation queries for the same entity.

### Decode / Transport (carried from v1.1)

- **KEY-03**: Cache Schema Registry schema lookups to cut per-message registry
  round-trips on large scans.
- **HTTP-02**: Auth/TLS hardening for the HTTP transport (token, mTLS).

## Out of Scope

Explicitly excluded for v1.2. Documented to prevent scope creep.

| Feature | Reason |
|---------|--------|
| Timeline visualization / rendering | v1.2 provides the data; rendering is a UI concern outside the brick |
| Produce / write paths | Violates the read-only guarantee |
| Consumer-group management | Write operations; out of the read-only contract |
| Rust native scanner | Gated on a future CPU-bound benchmark (KAFKA-07, I/O-bound) |
| Auto-discovery of all topics for correlation | Risk of unbounded fan-out; v1.2 requires explicit topic lists |
| Recursive multi-hop correlation (>1 hop depth) | Deferred to COR-04; v1.2 supports single-hop follow from initial results |

## Traceability

Which phases cover which requirements. Populated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| MTS-01 | TBD | Pending |
| MTS-02 | TBD | Pending |
| HDR-01 | TBD | Pending |
| HDR-02 | TBD | Pending |
| COR-01 | TBD | Pending |
| COR-02 | TBD | Pending |
| COR-03 | TBD | Pending |
| SYM-01 | TBD | Pending |

**Coverage:**
- v1.2 requirements: 8 total
- Mapped to phases: 0 (pending roadmap)
- Unmapped: 8

---
*Requirements defined: 2026-06-16*
