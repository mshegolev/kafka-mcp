# Roadmap: kafka-mcp

## Milestones

- ✅ **v1.0 MVP** — Phases 1–3 (shipped 2026-06-08) — read-only Kafka MCP brick:
  topic inspection, key+time-window search, Avro/Protobuf/JSON decode, four
  inbound faces, benchmark-gated native decision, distribution artifacts.
  Full detail: [`milestones/v1.0-ROADMAP.md`](milestones/v1.0-ROADMAP.md).
- 🚧 **v1.1 Production-Ready & Extended** — Phases 4–7 (in progress)

## Phases

<details>
<summary>✅ v1.0 MVP (Phases 1–3) — SHIPPED 2026-06-08</summary>

- [x] Phase 1: Foundation (4/4 plans) — completed 2026-06-05
- [x] Phase 2: Search + Decode (5/5 plans) — completed 2026-06-06
- [x] Phase 3: Native + Ship (3/3 plans) — completed 2026-06-08

</details>

### 🚧 v1.1 Production-Ready & Extended (In Progress)

**Milestone Goal:** Take the read-only Kafka brick from prepared-MVP to
live-published, real-broker-verified, with extended decode + new triage tooling
— without breaking the v1.0 contract or the read-only guarantee.

- [x] **Phase 4: Extended Decode & Transport** - Decode message keys via Schema Registry, surface schema_id on KafkaMessage across all four faces, and add HTTP transport entry to server.json (completed 2026-06-08)
- [ ] **Phase 5: Consumer Lag Tooling** - New read-only consumer_group_lag tool exposing per-partition lag with Evidence fields, delivered identically across lib / MCP / FastAPI / CLI
- [ ] **Phase 6: Real-Broker E2E Contour** - Testcontainers integration suite verifying all tools (including v1.1 surfaces) against a live Kafka broker + Schema Registry with real-wire decode
- [ ] **Phase 7: Release Pipeline** - Verified tag-triggered CI publish workflow + documented RELEASE.md runbook covering PyPI upload and Glama submission steps

## Phase Details

### Phase 4: Extended Decode & Transport
**Goal**: Users of all four faces can decode schema-encoded message keys, see schema_id on every KafkaMessage, and discover the FastAPI face as an MCP HTTP server via server.json
**Depends on**: Phase 3 (v1.0 shipped)
**Requirements**: KEY-01, KEY-02, HTTP-01
**Success Criteria** (what must be TRUE):
  1. `KafkaClient.get_message()` and `search_messages()` return a decoded key dict when the key is schema-encoded (Avro/Protobuf/JSON via Schema Registry), and fall back to the raw/string key without raising an error when the key is not schema-encoded
  2. `KafkaMessage` carries a `schema_id` field (value schema id, and key schema id when key-decoded); the field appears identically in lib return value, MCP stdio tool response, FastAPI `/tools/*` JSON, and CLI output
  3. `server.json` declares a streamable-HTTP transport entry in the `remotes` array whose declared endpoint matches the actual FastAPI `/mcp` route; verified by: `python -c "import json,pathlib; d=json.loads(pathlib.Path('server.json').read_text()); assert any(r.get('type')=='streamable-http' for r in d.get('remotes',[]))"` — NOTE: the real MCP server.json schema uses `remotes` (not `transports`) for streamable-HTTP transports; the original criterion's `d['transports']` assertion is incorrect and has been reconciled here.
  4. The unit test suite passes with no regressions (existing `search_messages` / `get_message` tests for value decoding still green)
**Plans**: 3 plans
Plans:
- [x] 04-01-PLAN.md — Add raw_key/key_decoded/schema_id fields to KafkaMessage; thread raw_key through confluent_consumer
- [x] 04-02-PLAN.md — Implement key decode + schema_id extraction in search_service; update all four face serializers
- [x] 04-03-PLAN.md — Mount FastMCP /mcp endpoint; declare HTTP transport in server.json remotes and glama.json
**UI hint**: no

### Phase 5: Consumer Lag Tooling
**Goal**: Users can query per-partition consumer-group lag through any of the four faces; every lag row carries Investigator-Contract Evidence fields and the operation is structurally read-only
**Depends on**: Phase 4
**Requirements**: LAG-01, LAG-02, LAG-03
**Success Criteria** (what must be TRUE):
  1. `KafkaClient.consumer_group_lag(group, topics)` returns a list of lag records containing `group`, `topic`, `partition`, `current_offset`, `end_offset`, `lag`, and `timestamp_utc` — no offset commits or writes are performed (verified by `enable.auto.commit=false` + assign-only pattern)
  2. The lag capability is reachable identically via MCP stdio tool `consumer_group_lag`, FastAPI `POST /tools/consumer_group_lag`, and `kafka-mcp consumer-group-lag` CLI subcommand — all return the same schema
  3. `pytest -k consumer_group_lag` passes against the mock adapter suite (unit coverage for all four faces)
  4. A `ToolAnnotations(readOnlyHint=True)` annotation is applied to the MCP tool, and the FastAPI route carries the same read-only declaration in its response model
**Plans**: 2 plans
Plans:
- [ ] 05-01-PLAN.md — LagRecord model + ConsumerPort extension + ConfluentConsumerAdapter implementation + KafkaClient facade + adapter unit tests
- [ ] 05-02-PLAN.md — All four inbound faces (MCP stdio, FastAPI REST, HTTP MCP, CLI) + server.py dispatch + full 4-face test suite
**UI hint**: no

### Phase 6: Real-Broker E2E Contour
**Goal**: An automated integration suite running against a real Kafka broker and Schema Registry (testcontainers) verifies all tools — including the v1.1 surfaces (key decode, schema_id, lag) — with real-wire round-trips and real schema-encoded decode
**Depends on**: Phase 5
**Requirements**: E2E-01, E2E-02, E2E-03
**Success Criteria** (what must be TRUE):
  1. `pytest -m integration` boots real Kafka + Schema Registry via testcontainers, seeds messages with a test-only producer, then passes without requiring any external broker; `pytest` (no `-m integration` flag) skips all integration tests so the unit suite remains hermetic
  2. Real-wire round-trip tests for `list_topics`, `describe_topic`, `search_messages`, and `get_message` pass against the live broker; the brick paths remain read-only throughout (assign-only, no offset commits to the producer's group)
  3. Real-wire decode tests pass for at least one Avro-encoded, one Protobuf-encoded, and one JSON-encoded message round-trip against the live Schema Registry (not mocks); decoded values match the seeded payloads
  4. The v1.1 surfaces are covered by the real-wire suite: key decode (`KEY-01`/`KEY-02`) and consumer lag (`LAG-01`/`LAG-03`) each have at least one integration test that exercises the live broker path
**Plans**: TBD
**UI hint**: no

### Phase 7: Release Pipeline
**Goal**: A maintainer can publish a versioned release to PyPI and submit to Glama by pushing a git tag; the CI pipeline is verified end-to-end against TestPyPI, and a RELEASE.md runbook documents every human-gated step
**Depends on**: Phase 6
**Requirements**: REL-01, REL-02
**Success Criteria** (what must be TRUE):
  1. Pushing a `v*` tag (or triggering the release workflow manually against TestPyPI) produces a CI job that builds sdist + wheels and executes a dry-run upload to TestPyPI (`twine check dist/*` passes; `twine upload --repository testpypi` succeeds) — verifiable in CI logs without live PyPI credentials
  2. `RELEASE.md` exists and documents: (a) tagging convention, (b) required GitHub/PyPI secrets setup, (c) how to verify the TestPyPI dry-run, (d) the manual Glama submission steps using `glama.json` / `server.json`
  3. `glama.json` and `server.json` accurately reflect the v1.1 artifact (updated tool list including `consumer_group_lag`, updated transport list including HTTP entry from Phase 4)
  4. `python -m twine check dist/*` passes locally after `hatch build`, confirming the distribution artifact is well-formed (human-gated: the actual live credentialed PyPI push and Glama account submission remain a human action and are explicitly out of scope)
**Plans**: TBD
**UI hint**: no

## Progress

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1. Foundation | v1.0 | 4/4 | Complete | 2026-06-05 |
| 2. Search + Decode | v1.0 | 5/5 | Complete | 2026-06-06 |
| 3. Native + Ship | v1.0 | 3/3 | Complete | 2026-06-08 |
| 4. Extended Decode & Transport | v1.1 | 3/3 | Complete   | 2026-06-08 |
| 5. Consumer Lag Tooling | v1.1 | 0/TBD | Not started | - |
| 6. Real-Broker E2E Contour | v1.1 | 0/TBD | Not started | - |
| 7. Release Pipeline | v1.1 | 0/TBD | Not started | - |
