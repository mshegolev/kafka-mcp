# Requirements: kafka-mcp — Milestone v1.1

**Defined:** 2026-06-08
**Core Value:** Read-only Kafka MCP brick — find events by key in a time window,
decode Avro/Protobuf/JSON via Schema Registry, surface evidence for incident
timelines. Library-first: works in pytest without MCP or FastAPI.

> v1.0 (KAFKA-01..07) shipped 2026-06-08 — see PROJECT.md and
> `milestones/v1.0-ROADMAP.md`. v1.1 extends that contract without breaking it.

## v1.1 Requirements

Requirements for the v1.1 release. Each maps to exactly one roadmap phase.

### Release (REL)

- [ ] **REL-01**: Pushing a `v*` git tag triggers a CI job that builds the
  sdist + wheels and publishes to PyPI; the publish step is verified end-to-end
  against TestPyPI (dry-run upload succeeds) so a maintainer's real tag push
  publishes without further code changes.
- [x] **REL-02**: A maintainer can register the server on Glama from the
  in-repo metadata (`glama.json` / `server.json`) following a documented release
  runbook (`RELEASE.md`) that covers tagging, credential/secret setup, and the
  Glama submission steps.

### Real-broker E2E (E2E)

- [ ] **E2E-01**: An integration test contour boots a real Kafka broker + Schema
  Registry (testcontainers) and is runnable via a dedicated marked pytest target
  (e.g. `-m integration`), skipped by default so the unit suite stays hermetic.
- [ ] **E2E-02**: Real-wire round-trip tests verify `list_topics`,
  `describe_topic`, `search_messages`, and `get_message` against the live broker,
  reading messages seeded by a test-only producer — the brick's own paths remain
  read-only (assign-only, no commits to prod groups).
- [ ] **E2E-03**: Real-wire decode tests verify Avro, Protobuf, and JSON message
  decoding against the live Schema Registry (not mocks), covering at least one
  schema-encoded round-trip per format.

### Extended decode & transport (KEY / HTTP)

- [x] **KEY-01**: `search_messages` and `get_message` decode the message **key**
  via Schema Registry when the key is schema-encoded, falling back to the raw /
  string key when it is not (no crash on plain keys).
- [x] **KEY-02**: `KafkaMessage` surfaces the Schema Registry `schema_id` for the
  decoded value (and key when key-decoded), exposed identically across all four
  faces.
- [x] **HTTP-01**: `server.json` declares a streamable-HTTP transport entry
  alongside stdio, so the FastAPI face is discoverable as an MCP HTTP server; the
  declared endpoint matches the actual FastAPI route.

### Triage tooling — consumer lag (LAG)

- [ ] **LAG-01**: A new read-only `consumer_group_lag` capability reports per
  topic/partition lag (committed offset vs end offset) for a given consumer
  group, with no writes and no commits to that group.
- [ ] **LAG-02**: The lag capability is exposed identically across all four faces
  (lib `KafkaClient.consumer_group_lag(...)`, MCP stdio tool, FastAPI
  `/tools/consumer_group_lag`, and the `kafka-mcp` CLI subcommand).
- [ ] **LAG-03**: Lag output carries Investigator-Contract-style evidence fields
  (`group`, `topic`, `partition`, `current_offset`, `end_offset`, `lag`,
  `timestamp_utc`) so each row is usable as a timeline data point.

## Future Requirements

Deferred to a later milestone. Tracked but not in this roadmap.

### Decode / Transport

- **KEY-03**: Cache Schema Registry schema lookups to cut per-message registry
  round-trips on large scans.
- **HTTP-02**: Auth/TLS hardening for the HTTP transport (token, mTLS).

## Out of Scope

Explicitly excluded for v1.1. Documented to prevent scope creep.

| Feature | Reason |
|---------|--------|
| Actual live credentialed PyPI publish + Glama account submission | Outward-facing, requires maintainer credentials and a human tag push — REL-01/REL-02 deliver the verified pipeline + runbook; the live push stays a human action (v1.0 "prepare-don't-live-publish" posture). |
| Rust native scanner | Gated on a future CPU-bound benchmark; v1.0 benchmark proved the scan is I/O-bound (KAFKA-07). Not anticipated. |
| Produce / write paths, offset-commit management | Violates the read-only guarantee — the brick is read-only by design. |
| Consumer-group lifecycle management (create/delete/reset offsets) | Write operations; out of the read-only contract. LAG is read-only reporting only. |

## Traceability

Which phases cover which requirements. Populated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| KEY-01 | Phase 4 | Complete |
| KEY-02 | Phase 4 | Complete |
| HTTP-01 | Phase 4 | Complete |
| LAG-01 | Phase 5 | Pending |
| LAG-02 | Phase 5 | Pending |
| LAG-03 | Phase 5 | Pending |
| E2E-01 | Phase 6 | Pending |
| E2E-02 | Phase 6 | Pending |
| E2E-03 | Phase 6 | Pending |
| REL-01 | Phase 7 | Pending |
| REL-02 | Phase 7 | Complete |

**Coverage:**
- v1.1 requirements: 11 total
- Mapped to phases: 11 ✓
- Unmapped: 0 ✓

---
*Requirements defined: 2026-06-08*
*Last updated: 2026-06-08 after roadmap creation (Phases 4–7 assigned)*
