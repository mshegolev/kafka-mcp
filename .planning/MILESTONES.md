# Milestones

## v1.3 v1.3 (Shipped: 2026-07-08)

**Phases completed:** 3 phases, 5 plans, 7 tasks

**Key accomplishments:**

- 1. [Rule 1/3 — Bug + Blocking test isolation] `.env`-leak broke hermetic config tests
- Env-gated real-broker mTLS integration test that runs read-only list_topics()/describe_topic() over a client-certificate TLS handshake, skipping cleanly when broker + cert env are absent so CI-without-broker stays green.
- Test-only lock proving every read-only tool on the stdio + HTTP MCP faces advertises readOnlyHint/idempotentHint/openWorldHint (all True) and that `_parse_iso_utc` rejects bad time_from/time_to with an actionable param-named error while accepting trailing-Z / naive input as tz-aware UTC.
- Test-only face coverage for consumer_group_lag and correlate_messages across stdio MCP, HTTP MCP, and REST, with the correlate_messages base64 raw + timestamp round-trip verified both inbound (reconstructed KafkaMessage on the fake) and outbound (REST response raw decodes to original bytes)

---

## v1.0 MVP — Read-only Kafka MCP brick (Shipped: 2026-06-08)

**Phases completed:** 3 phases, 12 plans, 20 tasks

**Key accomplishments:**

- One-liner:
- One-liner:
- One-liner:
- One-liner:
- One-liner:
- One-liner:
- One-liner:
- One-liner:
- One-liner:
- One-liner:
- One-liner:
- One-liner:

---

## v1.1 Production-Ready & Extended (Shipped: 2026-06-16)

**Phases completed:** 4 phases, 9 plans
**Requirements:** 11 (KEY-01, KEY-02, HTTP-01, LAG-01, LAG-02, LAG-03, E2E-01, E2E-02, E2E-03, REL-01, REL-02)
**Tests:** 298 unit + 25 integration = 323 total

**Key accomplishments:**

- Phase 4: Decode message keys via Schema Registry; surface schema_id on KafkaMessage; add HTTP transport in server.json
- Phase 5: New consumer_group_lag tool with 4-face symmetry and Evidence fields; AdminClient-based read-only implementation
- Phase 6: Testcontainers integration suite (Kafka + Schema Registry) with 25 real-wire tests covering Avro/Protobuf/JSON decode
- Phase 7: Tag-triggered CI release workflow, RELEASE.md runbook, glama.json/server.json reflect v1.1

---

## v1.2 Cross-Topic Investigation (Shipped: 2026-06-18)

**Phases completed:** 3 phases, 3 plans
**Requirements:** 8 (MTS-01, MTS-02, HDR-01, HDR-02, COR-01, COR-02, COR-03, SYM-01)
**Tests:** 317 unit + 25 integration = 342 total

**Key accomplishments:**

- Phase 8: Multi-topic search with header filtering — search across multiple topics simultaneously with header key-value filters
- Phase 9: Correlation engine — extract correlated entity IDs from search results and follow them into additional topics to build cross-service event chains
- Phase 10: 4-face symmetry and integration tests — ensure all v1.2 capabilities accessible identically through MCP stdio, FastAPI REST, and CLI faces

---
