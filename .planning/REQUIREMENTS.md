# Requirements: kafka-mcp — Milestone v1.3

**Defined:** 2026-07-08
**Milestone:** v1.3 — mTLS & Packaging Hardening
**Core Value:** Read-only Kafka MCP brick — find events by key in a time window,
correlate across topics, exposed identically through MCP / REST / CLI / library
faces. v1.3 hardens secure transport (mTLS), input robustness, and PyPI
packaging/release, and locks the session's shipped changes behind tests + docs.

**Scope note:** Several capabilities landed as code during development; v1.3
verifies them end-to-end and documents them, and adds the genuinely new work
(real-broker mTLS integration test, mTLS docs, lag/correlate coverage).

## v1.3 Requirements

### mTLS client certificates (MTLS)

- [x] **MTLS-01**: Operator can configure client-certificate mTLS via
  `KAFKA_MCP_SSL_CERTIFICATE_LOCATION` / `SSL_KEY_LOCATION` / `SSL_CA_LOCATION` /
  `SSL_KEY_PASSWORD`, wired into BOTH the consumer and the admin librdkafka config.

- [x] **MTLS-02**: mTLS connectivity is verified end-to-end against a real SSL
  broker — an integration test lists topics / describes a topic over mTLS.

- [x] **MTLS-03**: mTLS setup is documented in README (env vars, cert paths,
  `SECURITY_PROTOCOL=SSL`, key-password handling).

### Tool annotations (HINT)

- [x] **HINT-01**: All read-only tools advertise `idempotentHint=true` and
  `openWorldHint=true` (in addition to `readOnlyHint`) across the stdio MCP,
  HTTP MCP, and REST faces, with a test asserting the annotations.

### Input robustness (PARSE)

- [x] **PARSE-01**: `search_messages` rejects invalid ISO-8601 timestamps with an
  actionable error naming the parameter and accepted format (trailing `Z`
  accepted), instead of leaking the raw `fromisoformat` error.

### Packaging & release (PKG)

- [ ] **PKG-01**: The distribution is packaged and installable as
  `kafka-events-mcp` — pyproject name, README install line, and CHANGELOG agree,
  and `hatch build` produces `kafka_events_mcp-*` artifacts.

- [ ] **PKG-02**: Releases publish to PyPI via OIDC Trusted Publishing with no
  stored API tokens; the TestPyPI + PyPI jobs and publish path are verified.

### Test coverage (COV)

- [x] **COV-01**: `consumer_group_lag` has automated coverage across the faces it
  is exposed on.

- [x] **COV-02**: `correlate_messages` has automated coverage across the faces it
  is exposed on.

## Future Requirements

Deferred beyond v1.3:

- SASL mechanisms end-to-end verification (PLAIN / SCRAM) against a live broker.
- Schema Registry mTLS / basic-auth end-to-end verification.
- Published-package smoke test (install `kafka-events-mcp` from PyPI in a clean env).

## Out of Scope

- New read/write Kafka capabilities — v1.3 is hardening only; the brick stays
  read-only and its tool surface is frozen for this milestone.

- Rust/pyo3 native scanner — deferred per v1.0 KAFKA-07 benchmark decision.
- Multi-broker / cluster-management features — outside the investigator remit.

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| MTLS-01 | Phase 11 | Complete |
| MTLS-02 | Phase 11 | Complete |
| MTLS-03 | Phase 11 | Complete |
| HINT-01 | Phase 12 | Complete |
| PARSE-01 | Phase 12 | Complete |
| COV-01 | Phase 12 | Complete |
| COV-02 | Phase 12 | Complete |
| PKG-01 | Phase 13 | Pending |
| PKG-02 | Phase 13 | Pending |

**Coverage:** 9/9 mapped ✓

*Requirements defined: 2026-07-08 | Traceability updated by roadmap 2026-07-08.*
</content>
