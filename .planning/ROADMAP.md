# Roadmap: kafka-mcp

## Milestones

- ✅ **v1.0 MVP** — Phases 1–3 (shipped 2026-06-08) — read-only Kafka MCP brick:
  topic inspection, key+time-window search, Avro/Protobuf/JSON decode, four
  inbound faces, benchmark-gated native decision, distribution artifacts.
  Full detail: [`milestones/v1.0-ROADMAP.md`](milestones/v1.0-ROADMAP.md).
- ✅ **v1.1 Production-Ready & Extended** — Phases 4–7 (shipped 2026-06-16) —
  key decode, schema_id surfacing, HTTP transport, consumer_group_lag tool,
  testcontainers E2E suite, CI release pipeline.
- ✅ **v1.2 Cross-Topic Investigation** — Phases 8–10 (shipped 2026-06-18) —
  multi-topic search, header filtering, correlation extraction & follow,
  4-face symmetry for all new capabilities.
- 🚧 **v1.3 mTLS & Packaging Hardening** — Phases 11–13 (in progress) —
  client-certificate mTLS wired + verified end-to-end + documented, tool-surface
  robustness (annotations, timestamp errors, lag/correlate coverage), and
  `kafka-events-mcp` packaging via OIDC Trusted Publishing.

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

<details>
<summary>✅ v1.2 Cross-Topic Investigation (Phases 8–10) — SHIPPED 2026-06-18</summary>

- [x] Phase 8: Multi-Topic Search & Header Filtering (1/1 plans) — completed 2026-06-18
- [x] Phase 9: Correlation Engine (1/1 plans) — completed 2026-06-18
- [x] Phase 10: 4-Face Symmetry & Integration Tests (1/1 plans) — completed 2026-06-18

</details>

### 🚧 v1.3 mTLS & Packaging Hardening (Phases 11–13)

**Milestone Goal:** Harden the brick's secure transport, input robustness, and
PyPI packaging/release. Much of the code already landed during development
(mTLS wiring in the consumer/admin config, `idempotentHint`/`openWorldHint`
annotations, `_parse_iso_utc`, the `kafka-events-mcp` rename, the OIDC release
workflow). v1.3 verifies these end-to-end, locks them behind tests, documents
them, and adds the genuinely new work: a real-broker mTLS integration test,
mTLS README docs, and `consumer_group_lag` / `correlate_messages` coverage. The
brick stays structurally read-only and its tool surface is frozen.

- [ ] **Phase 11: mTLS Transport Hardening** — Verify client-certificate mTLS is wired into both consumer and admin config, prove it end-to-end against a real SSL broker, and document setup in README
- [ ] **Phase 12: Tool-Surface Robustness & Coverage** — Assert `idempotentHint`/`openWorldHint` annotations across all faces, harden `search_messages` timestamp errors, and add coverage for `consumer_group_lag` and `correlate_messages`
- [ ] **Phase 13: Packaging & OIDC Release** — Lock the `kafka-events-mcp` distribution identity end-to-end and verify the OIDC Trusted Publishing release path with no stored tokens

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

### Phase 11: mTLS Transport Hardening
**Goal**: An operator can secure the brick's Kafka connection with client-certificate mTLS using `KAFKA_MCP_SSL_*` env vars — wired into both the consumer and the admin path, proven to work against a real SSL broker, and documented well enough to configure from README alone
**Depends on**: Phase 10 (v1.2 foundation)
**Requirements**: MTLS-01, MTLS-02, MTLS-03
**Success Criteria** (what must be TRUE):
  1. Setting `KAFKA_MCP_SECURITY_PROTOCOL=SSL` plus `KAFKA_MCP_SSL_CERTIFICATE_LOCATION` / `SSL_KEY_LOCATION` / `SSL_CA_LOCATION` / `SSL_KEY_PASSWORD` produces a librdkafka config carrying `ssl.certificate.location`, `ssl.key.location`, `ssl.ca.location`, and `ssl.key.password` on BOTH the consumer and the AdminClient config builders — verifiable via `pytest tests/ -k "ssl or mtls"` asserting the rendered config dicts for both paths
  2. Absent SSL env vars, the config builders emit no `ssl.*` keys and existing PLAINTEXT/SASL behavior is unchanged — verifiable via `pytest tests/ -k "config" --tb=short` showing zero regressions on the existing config suite
  3. An integration test brings up a real SSL-enabled broker (testcontainers) with a server cert + client cert, connects the brick over mTLS, and successfully performs a read-only operation (`list_topics` and/or `describe_topic`) — verifiable via `pytest -m integration -k "mtls or ssl" -v` going green against the SSL broker
  4. README documents mTLS setup end-to-end: the `KAFKA_MCP_SSL_*` env var names, expected cert/key/CA file paths, `SECURITY_PROTOCOL=SSL`, and key-password handling — verifiable by a human following README to configure mTLS with no reference to source code
**Plans**: 2 plans
- [ ] 11-01-PLAN.md — Assert mTLS ssl.* wiring on the AdminClient conf (MTLS-01) + README mTLS docs (MTLS-03)
- [ ] 11-02-PLAN.md — Env-gated real-broker mTLS end-to-end read-only integration test (MTLS-02)
**UI hint**: no

### Phase 12: Tool-Surface Robustness & Coverage
**Goal**: The frozen read-only tool surface is provably correct and well-covered — every tool advertises the right hints across all faces, `search_messages` gives actionable errors for bad timestamps, and the two previously-uncovered tools (`consumer_group_lag`, `correlate_messages`) have automated tests across the faces they expose
**Depends on**: Phase 11
**Requirements**: HINT-01, PARSE-01, COV-01, COV-02
**Success Criteria** (what must be TRUE):
  1. Every read-only tool advertises `idempotentHint=true` and `openWorldHint=true` (alongside `readOnlyHint=true`) on the stdio MCP, HTTP MCP, and REST faces — verifiable via `pytest tests/ -k "annotation or hint"` asserting the annotations on each tool across all three faces
  2. `search_messages` called with an invalid `time_from`/`time_to` value rejects it with an actionable error that names the offending parameter and states the accepted ISO-8601 format (trailing `Z` accepted), instead of leaking the raw `fromisoformat` exception — verifiable via `pytest tests/ -k "parse_iso or timestamp"` asserting the error message content
  3. `consumer_group_lag` has automated coverage exercising it across each face it is exposed on (lib + MCP + REST + CLI as applicable) — verifiable via `pytest tests/ -k "consumer_group_lag or lag" -v`
  4. `correlate_messages` has automated coverage exercising it across each face it is exposed on — verifiable via `pytest tests/ -k "correlate" -v`
  5. The full suite stays green with the added tests and no regressions — verifiable via `pytest tests/ --tb=short`
**Plans**: TBD
**UI hint**: no

### Phase 13: Packaging & OIDC Release
**Goal**: The distribution ships cleanly as `kafka-events-mcp` with a consistent identity across all metadata, and releases publish to PyPI via OIDC Trusted Publishing with no stored API tokens — with the publish path verified end-to-end short of a live production push
**Depends on**: Phase 12
**Requirements**: PKG-01, PKG-02
**Success Criteria** (what must be TRUE):
  1. The distribution name `kafka-events-mcp` is consistent across `pyproject.toml` (`name`), the README install line, and CHANGELOG — verifiable by inspection plus `pytest`/CI check asserting the three agree
  2. `hatch build` (or the configured build backend) produces `kafka_events_mcp-*` sdist and wheel artifacts at version 0.2.0 — verifiable by running the build and observing the artifact filenames
  3. The release workflow publishes via PyPI OIDC Trusted Publishing (`id-token: write`, no `password`/`PYPI_API_TOKEN` secret referenced) — verifiable by inspecting `.github/workflows/release.yml` and confirming no stored token is used
  4. The TestPyPI dry-run job and the PyPI publish job are wired and the publish path is exercised end-to-end up to the live-production gate (human-gated per the "prepare-don't-live-publish" posture) — verifiable via a successful TestPyPI publish run or workflow dry-run evidence
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
| 8. Multi-Topic Search & Header Filtering | v1.2 | 1/1 | Complete | 2026-06-18 |
| 9. Correlation Engine | v1.2 | 1/1 | Complete | 2026-06-18 |
| 10. 4-Face Symmetry & Integration Tests | v1.2 | 1/1 | Complete | 2026-06-18 |
| 11. mTLS Transport Hardening | v1.3 | 0/? | Not started | - |
| 12. Tool-Surface Robustness & Coverage | v1.3 | 0/? | Not started | - |
| 13. Packaging & OIDC Release | v1.3 | 0/? | Not started | - |
</content>
</invoke>
