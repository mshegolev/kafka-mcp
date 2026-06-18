# Milestones

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
