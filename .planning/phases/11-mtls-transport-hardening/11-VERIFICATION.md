---
phase: 11
name: mTLS Transport Hardening
verified: 2026-07-09
status: passed
scores:
  requirements: 3/3
  tests: green
requirements:
  - id: MTLS-01
    status: satisfied
    evidence: "tests/test_adapters.py — parametrized [consumer|admin] assertions that ssl.certificate.location / ssl.key.location / ssl.ca.location / ssl.key.password (unwrapped SecretStr) + security.protocol=SSL land on BOTH the Consumer and AdminClient conf when SSL_* set, are omitted when unset, and no sasl.* regression under SSL-only. 7 passed."
  - id: MTLS-02
    status: satisfied
    evidence: "tests/integration/test_mtls_e2e.py — @pytest.mark.integration, builds a KafkaClient from an SSL KafkaMcpSettings and runs a read-only op over the mTLS handshake; env-gated (skipif on broker+cert+key+CA), skips cleanly (2 skipped) with no creds so the default run and CI stay green. Reads env only, no committed secrets."
  - id: MTLS-03
    status: satisfied
    evidence: "README.md — '### mTLS (client-certificate TLS)' subsection under ## Configure: SECURITY_PROTOCOL=SSL, all four KAFKA_MCP_SSL_* vars, cert/key/CA PEM paths, SecretStr key-password handling, both-paths (consumer + admin) note, never-commit-secrets caution."
tech_debt: []
human_verification:
  count: 1
  items:
    - "MTLS-02 exercises a real handshake only when the mTLS broker + cert env vars are provided (proven this session against staging 10.35.158.66:9094). CI/default runs skip it by design; a live run is optional manual validation."
---

# Phase 11 Verification — mTLS Transport Hardening

**Status: PASSED** — all 3 requirements satisfied; full default suite green.

## Evidence

- **Full suite:** `uv run python -m pytest -m 'not integration'` → **331 passed, 1 skipped, 27 deselected**. (The integration mTLS test is deselected by the default `not integration` marker and skips cleanly when run without creds.)
- **MTLS-01** (config → librdkafka `ssl.*` on consumer AND admin): the real gap was the AdminClient conf — the pre-existing tests only inspected the Consumer conf. New parametrized assertions cover both builders. The adapter code already existed; this phase locked it behind tests.
- **MTLS-02** (real-broker e2e): env-gated integration test added; skips without creds, never fails the default/CI run. The staging mTLS contour was proven live this session (`kafka-mcp list-topics` / `describe-topic eord` over `SECURITY_PROTOCOL=SSL`).
- **MTLS-03** (docs): README mTLS section added.

## Notes

- Closed a test-hermeticity gap the executors deferred: `tests/test_lib.py` two tests leaked an ambient developer `.env` (which this session created to reach staging). Fixed with `_env_file=None` / `monkeypatch.chdir(tmp_path)` — suite now green regardless of a repo-root `.env`.
- No `src/` changes were needed — this is a hardening milestone; the mTLS implementation already shipped during development.
- `deferred-items.md` in this phase dir tracks the `.env`-leak root cause (now resolved) and the tooling note (`uv run python -m pytest`, not `uv run pytest`).
