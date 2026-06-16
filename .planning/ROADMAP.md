# Roadmap: kafka-mcp

## Milestones

- ✅ **v1.0 MVP** — Phases 1–3 (shipped 2026-06-08) — read-only Kafka MCP brick:
  topic inspection, key+time-window search, Avro/Protobuf/JSON decode, four
  inbound faces, benchmark-gated native decision, distribution artifacts.
  Full detail: [`milestones/v1.0-ROADMAP.md`](milestones/v1.0-ROADMAP.md).
- ✅ **v1.1 Production-Ready & Extended** — Phases 4–7 (shipped 2026-06-16) —
  key decode, schema_id surfacing, HTTP transport, consumer_group_lag tool,
  testcontainers E2E suite, CI release pipeline.
- 🚧 **v1.2 Cross-Topic Investigation** — Phases 8–10 (in progress) —
  multi-topic search, header filtering, correlation extraction & follow,
  4-face symmetry for all new capabilities.

## Phases

<details>
<summary>✅ v1.0 MVP (Phases 1–3) — SHIPPED 2026-06-08</summary>

- [x] Phase 1: Foundation (4/4 plans) — completed 2026-06-05
- [x] Phase 2: Search + Decode (5/5 plans) — completed 2026-06-06
- [x] Phase 3: Native + Ship (3/3 plans) — completed 2026-06-08

</details>

<details>
<summary>✅ v1.1 Production-Ready & Extended (Phases 4–7) — SHIPPED 2026-06-16</summary>

- [x] Phase 4: Extended Decode & Transport (3/3 plans) — completed 2026-06-08
- [x] Phase 5: Consumer Lag Tooling (2/2 plans) — completed 2026-06-16
- [x] Phase 6: Real-Broker E2E Contour (2/2 plans) — completed 2026-06-16
- [x] Phase 7: Release Pipeline (2/2 plans) — completed 2026-06-16

</details>

### 🚧 v1.2 Cross-Topic Investigation (In Progress)

**Milestone Goal:** Enable investigators to trace entities across multiple Kafka
topics — searching a key across topics simultaneously, filtering by message
headers, extracting correlated IDs from payloads/headers, and following those IDs
into other topics to build cross-service event chains. All new capabilities
honor the 4-face symmetry and the Investigator-Contract Evidence shape.

- [ ] **Phase 8: Multi-Topic Search & Header Filtering** — Extend `search_messages` to accept multiple topics and header key-value filters at the domain/lib layer
- [ ] **Phase 9: Correlation Engine** — New `correlate_messages` capability that extracts correlated IDs from search results and follows them into additional topics
- [ ] **Phase 10: 4-Face Symmetry & Integration Tests** — Wire all v1.2 capabilities across MCP/FastAPI/CLI faces; update integration test suite for cross-topic scenarios

## Phase Details

### Phase 8: Multi-Topic Search & Header Filtering
**Goal**: An investigator can search for a key across multiple Kafka topics in a single call and filter results by header key-value pairs, getting a merged-by-timestamp result set — without breaking any existing single-topic callers
**Depends on**: Phase 7 (v1.1 foundation)
**Requirements**: MTS-01, MTS-02, HDR-01, HDR-02
**Success Criteria** (what must be TRUE):
  1. `KafkaClient.search_messages(key="order-123", topics=["orders", "payments", "shipments"])` returns messages from all three topics merged and sorted by `timestamp_utc`; each result carries its source `topic` field — verifiable via `pytest tests/ -k "multi_topic"` with mocked consumer returning messages from different topics
  2. `KafkaClient.search_messages(key="order-123", topics=["orders"])` produces identical results to the pre-v1.2 single-topic path; existing tests in `tests/` that call `search_messages` with a single topic pass without modification — verifiable via `pytest tests/ -k "search"` showing zero regressions
  3. `KafkaClient.search_messages(key="order-123", headers={"trace_id": "abc-123"})` returns only messages whose Kafka headers contain the specified key-value pairs; messages missing the header or having a different value are excluded — verifiable via `pytest tests/ -k "header_filter"`
  4. Header filtering combines with key + time window + multi-topic filters using AND semantics: `search_messages(key="order-123", topics=["orders", "payments"], headers={"trace_id": "abc"}, time_from=..., time_to=...)` applies all filters simultaneously — verifiable via `pytest tests/ -k "combined_filter"`
  5. All new parameters (`topics`, `headers`) are optional with backward-compatible defaults (`topics=None` falls back to the existing single-topic `topic` parameter; `headers=None` means no header filtering) — verifiable via `pytest tests/ -k "search" --tb=short` showing full green on existing + new tests
**Plans**: TBD
**UI hint**: no

### Phase 9: Correlation Engine
**Goal**: An investigator can extract correlated entity IDs from search results and follow those IDs into additional topics to build a cross-service event chain — all within a single `correlate_messages` call that returns Evidence-shaped results linked by a correlation chain
**Depends on**: Phase 8
**Requirements**: COR-01, COR-02, COR-03
**Success Criteria** (what must be TRUE):
  1. `KafkaClient.correlate_messages(key="order-123", topics=["orders"], follow_topics=["payments", "shipments"])` extracts correlated IDs (e.g. `trace_id`, `payment_id`, `shipment_id`) from the initial search results by scanning message values and headers for configurable ID field patterns, then searches for those IDs in `follow_topics` — verifiable via `pytest tests/ -k "correlate"` with mocked consumer data containing cross-references
  2. The correlation output is a list of messages sorted by `timestamp_utc` across all topics (initial + follow), where each message carries `source="kafka"`, `event_type="correlated_message"`, `timestamp_utc`, and `keys` fields conforming to the Investigator-Contract Evidence shape — verifiable via `pytest tests/ -k "evidence_shape"`
  3. Each message in the correlation output includes a `correlation_chain` field that records the ID path that discovered it (e.g. `[{"field": "order_id", "value": "order-123", "hop": 0}, {"field": "trace_id", "value": "abc-123", "hop": 1}]`), so an investigator can reconstruct *why* each message was included — verifiable via `pytest tests/ -k "correlation_chain"`
  4. `correlate_messages` reuses the Phase 8 multi-topic search + header filtering internally (no duplicated scan logic); the correlation layer is additive on top of the search domain — verifiable by inspecting that `correlate_messages` delegates to `search_messages` in the implementation and via `pytest tests/ -k "correlate" -v` showing the call chain
**Plans**: TBD
**UI hint**: no

### Phase 10: 4-Face Symmetry & Integration Tests
**Goal**: All v1.2 capabilities (multi-topic search, header filtering, correlate_messages) are accessible identically through MCP stdio, FastAPI REST, and CLI faces — and the integration test suite covers cross-topic scenarios against a real broker
**Depends on**: Phase 9
**Requirements**: SYM-01
**Success Criteria** (what must be TRUE):
  1. The MCP stdio face exposes multi-topic search (`topics` param), header filtering (`headers` param) on the `search_messages` tool, and a new `correlate_messages` tool — all returning the same JSON schema as the lib face; verifiable via `pytest tests/ -k "mcp" -v` exercising the MCP adapter with the new parameters and tool
  2. The FastAPI REST face exposes `/tools/search_messages` accepting `topics` + `headers` body fields, and a new `/tools/correlate_messages` endpoint — response schemas match the lib face; verifiable via `pytest tests/ -k "rest" -v` or `pytest tests/ -k "fastapi" -v`
  3. The CLI face (`kafka-mcp search-messages --topics ... --headers ...` and `kafka-mcp correlate-messages ...`) produces the same output as the lib face for equivalent inputs; verifiable via `pytest tests/ -k "cli" -v`
  4. The existing 323 tests pass without modification alongside the new v1.2 tests — verifiable via `pytest tests/ --tb=short` showing zero regressions and total count ≥ 323
  5. Integration tests (`pytest -m integration`) include at least one cross-topic search scenario and one correlation scenario against the real testcontainers broker — verifiable via `pytest -m integration -k "multi_topic or correlate" -v`
**Plans**: TBD
**UI hint**: no

## Progress

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1. Foundation | v1.0 | 4/4 | Complete | 2026-06-05 |
| 2. Search + Decode | v1.0 | 5/5 | Complete | 2026-06-06 |
| 3. Native + Ship | v1.0 | 3/3 | Complete | 2026-06-08 |
| 4. Extended Decode & Transport | v1.1 | 3/3 | Complete | 2026-06-08 |
| 5. Consumer Lag Tooling | v1.1 | 2/2 | Complete | 2026-06-16 |
| 6. Real-Broker E2E Contour | v1.1 | 2/2 | Complete | 2026-06-16 |
| 7. Release Pipeline | v1.1 | 2/2 | Complete | 2026-06-16 |
| 8. Multi-Topic Search & Header Filtering | v1.2 | 0/0 | Not started | - |
| 9. Correlation Engine | v1.2 | 0/0 | Not started | - |
| 10. 4-Face Symmetry & Integration Tests | v1.2 | 0/0 | Not started | - |
