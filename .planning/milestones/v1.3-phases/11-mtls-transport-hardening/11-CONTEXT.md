# Phase 11: mTLS Transport Hardening - Context

**Gathered:** 2026-07-08
**Status:** Ready for planning
**Mode:** Auto-generated (autonomous — low-ambiguity hardening phase; code already exists)

<domain>
## Phase Boundary

Secure the brick's Kafka connection with client-certificate mTLS via
`KAFKA_MCP_SSL_CERTIFICATE_LOCATION` / `SSL_KEY_LOCATION` / `SSL_CA_LOCATION` /
`SSL_KEY_PASSWORD`, wired into BOTH the consumer and the AdminClient path,
proven against a real SSL broker, and documented in README.

Requirements: MTLS-01 (config wiring, consumer + admin), MTLS-02 (real-broker
end-to-end integration test), MTLS-03 (README docs).

In scope: verifying/locking the existing mTLS wiring with tests, adding a
real-broker mTLS integration test, and writing operator-facing docs.
Out of scope: SASL end-to-end, Schema Registry mTLS, any produce/write path.
</domain>

<decisions>
## Implementation Decisions

### Already implemented (verify, do NOT re-implement)
- `KafkaMcpSettings` (src/kafka_mcp/config.py) exposes `ssl_certificate_location`,
  `ssl_key_location`, `ssl_ca_location`, `ssl_key_password` (SecretStr),
  `security_protocol`.
- `ConfluentConsumerAdapter` (src/kafka_mcp/adapters/outbound/confluent_consumer.py)
  maps these into librdkafka `ssl.*` keys on BOTH the Consumer conf and the
  AdminClient conf, only when set; `security.protocol` set when != PLAINTEXT.

### Claude's discretion
- Unit test asserts `ssl.*` keys are emitted on both consumer and admin conf when
  `SSL_*` set, and absent when unset (no regression to PLAINTEXT/SASL).
- Integration test: gate behind an env-guarded marker (e.g. skip unless a
  `KAFKA_MCP_BOOTSTRAP_SERVERS` + certs are provided), mirroring the existing
  real-broker E2E contour from v1.1 Phase 6. A working staging mTLS contour
  exists (broker `10.35.158.66:9094`, stage certs, `SECURITY_PROTOCOL=SSL`),
  proven this session via `kafka-mcp list-topics` / `describe-topic eord`.
- README: add an mTLS subsection under Install/Config (env vars, cert paths,
  `SECURITY_PROTOCOL=SSL`, key-password handling; never commit real certs/keys).
</decisions>

<code_context>
## Existing Code Insights

- Hexagonal layout: config in `config.py`, outbound adapter in
  `adapters/outbound/confluent_consumer.py`, lib facade in
  `adapters/inbound/lib.py` (`KafkaClient.from_env()`).
- v1.1 Phase 6 established a real-broker E2E contour pattern (tests/integration/)
  and env-gated markers — reuse that shape for MTLS-02.
- SecretStr means the key password never appears in `__repr__`/logs (T-01-01).
</code_context>

<specifics>
## Specific Ideas

- MTLS-01: parametrized unit test over consumer + admin conf builders.
- MTLS-02: `tests/integration/test_mtls_e2e.py`, skipped unless mTLS env present;
  connects and runs a read-only op (list_topics / describe_topic).
- MTLS-03: README mTLS section; cross-link the `SSL_*` env vars to the paths.
</specifics>

<deferred>
## Deferred Ideas

- SASL PLAIN/SCRAM end-to-end verification (v1.3 Future).
- Schema Registry mTLS/basic-auth end-to-end (v1.3 Future).
- Published-package smoke test (v1.3 Future).
</deferred>
