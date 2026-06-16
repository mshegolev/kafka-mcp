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
