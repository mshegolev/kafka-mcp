---
phase: 11-mtls-transport-hardening
plan: 02
subsystem: testing
tags: [mtls, ssl, integration-test, kafka, pytest, confluent-kafka]

# Dependency graph
requires:
  - phase: 11-mtls-transport-hardening (plan 11-01)
    provides: "ssl_* fields on KafkaMcpSettings and ConfluentConsumerAdapter mapping ssl.* onto Consumer + AdminClient conf"
  - phase: 06 (v1.1 real-broker E2E contour)
    provides: "tests/integration/ pattern — @pytest.mark.integration, graceful skip when broker absent"
provides:
  - "env-gated real-broker mTLS end-to-end integration test (MTLS-02)"
  - "Read-only list_topics()/describe_topic() proof over a client-cert TLS handshake"
affects: [mtls-transport-hardening, packaging, ci]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Env-gated integration test: module-level pytest.mark.skipif on presence of broker+cert+key+CA env, so absent creds SKIP (never FAIL)"
    - "Explicit KafkaMcpSettings(**kwargs) construction so the SSL fields under test are unambiguous and self-contained"

key-files:
  created:
    - tests/integration/test_mtls_e2e.py
  modified: []

key-decisions:
  - "Env-gated (skipif) rather than an inline SSL broker, per plan D — reuses the proven staging mTLS contour (broker read from env), no cert material committed"
  - "Null registry (default KafkaClient(consumer)) — decode not needed for list_topics/describe_topic read-only ops"
  - "describe_topic leg is opt-in via KAFKA_MCP_MTLS_TEST_TOPIC; list_topics is the always-run mTLS-handshake proof"

patterns-established:
  - "mTLS e2e proof = successful read-only metadata round-trip (list_topics) over the client-cert TLS channel"

requirements-completed: [MTLS-02]

# Metrics
duration: ~12min
completed: 2026-07-09
status: complete
---

# Phase 11 Plan 02: mTLS End-to-End Integration Test Summary

**Env-gated real-broker mTLS integration test that runs read-only list_topics()/describe_topic() over a client-certificate TLS handshake, skipping cleanly when broker + cert env are absent so CI-without-broker stays green.**

## Performance

- **Duration:** ~12 min
- **Started:** 2026-07-08T17:19:37Z
- **Completed:** 2026-07-09T00:21:00Z
- **Tasks:** 1
- **Files modified:** 1 (created)

## Accomplishments
- Added `tests/integration/test_mtls_e2e.py` (MTLS-02): builds a `KafkaClient` from an SSL-configured `KafkaMcpSettings` (`security_protocol="SSL"` + `ssl_certificate_location`/`ssl_key_location`/`ssl_ca_location`/`ssl_key_password`) and runs read-only `list_topics()` (and opt-in `describe_topic()`) over the mTLS handshake.
- Test is `@pytest.mark.integration` + module-level `pytest.mark.skipif` keyed on "broker AND cert AND key AND CA all present" — SKIPS with a descriptive reason when the mTLS env is absent, never FAILS/ERRORS.
- Verified: `uv run python -m pytest -m integration tests/integration/test_mtls_e2e.py` → **2 skipped** with the reason string; default `-m 'not integration'` run leaves the new file deselected.
- No cert material committed; broker + cert paths read from env (T-11-03 mitigated).

## Task Commits

1. **Task 1: env-gated real-broker mTLS end-to-end read-only test (MTLS-02)** — `e8eae01` (test)

## Files Created/Modified
- `tests/integration/test_mtls_e2e.py` — env-gated mTLS e2e test; `KafkaClient` over `ConfluentConsumerAdapter(settings)` with `security_protocol="SSL"` + the four `ssl_*` fields; `list_topics()` proves the handshake, opt-in `describe_topic()` via `KAFKA_MCP_MTLS_TEST_TOPIC`.

## Decisions Made
- Env-gated skip (reuse the proven staging contour via env) instead of standing up an inline SSL broker — per plan D and the Phase 6 contour convention.
- `KafkaClient(consumer)` with the default `_NullSchemaRegistry` — decode is not needed for the read-only metadata ops.
- `describe_topic` is opt-in (`KAFKA_MCP_MTLS_TEST_TOPIC`) so the test never depends on a specific topic existing on an arbitrary broker; `list_topics()` is the guaranteed handshake proof.

## Deviations from Plan

None — plan executed exactly as written. The single file `tests/integration/test_mtls_e2e.py` was created as specified; no src/ changes.

## Issues Encountered
- **Tooling:** `uv run pytest` binds to a stale system Python 3.10 that lacks the editable install (`ModuleNotFoundError: No module named 'kafka_mcp'`), which affects the pre-existing `test_basic_ops.py` identically. Resolved by invoking `uv run python -m pytest` (the Python 3.14 venv where `kafka_mcp` resolves via the `src/` path). Logged in `deferred-items.md`.
- **Recovery note:** During pre-existing-failure investigation, a `git stash push -- <untracked path> || mv ...` chain relocated the new (untracked) test file to `/tmp`; it was restored intact from the `mv` backup and re-verified (2 skipped). No data lost.

## Deferred Issues (out of scope)
- 7 pre-existing failures in the default (`-m 'not integration'`) suite live in `tests/test_adapters.py` and `tests/test_lib.py` (from prior, still-uncommitted plan 11-01 work — `tests/test_adapters.py` is modified in the working tree). **Proven unrelated to this plan:** running the default suite with the new file absent still shows 7 failed. These belong to plan 11-01's verification, not 11-02. Logged in `.planning/phases/11-mtls-transport-hardening/deferred-items.md`.

## Threat Surface
- T-11-03 (Information Disclosure — client key/passphrase): mitigated. Cert/key/CA/passphrase all sourced from env paths; nothing committed. Passphrase flows through `SecretStr` in `KafkaMcpSettings` and is never logged.
- T-11-04 (DoS — hang on unreachable broker): accepted per plan; env-gated + adapter's fixed metadata timeout fails fast.
- No new threat surface introduced beyond the plan's `<threat_model>`.

## User Setup Required
To actually run (not skip) the test, provide the mTLS contour via env:
`KAFKA_MCP_MTLS_TEST_BOOTSTRAP` (or `KAFKA_MCP_BOOTSTRAP_SERVERS`), `KAFKA_MCP_SSL_CERTIFICATE_LOCATION`, `KAFKA_MCP_SSL_KEY_LOCATION`, `KAFKA_MCP_SSL_CA_LOCATION` (and optionally `KAFKA_MCP_SSL_KEY_PASSWORD`, `KAFKA_MCP_MTLS_TEST_TOPIC`). Proven staging contour: broker `10.35.158.66:9094` with stage certs, `SECURITY_PROTOCOL=SSL`.

## Next Phase Readiness
- MTLS-02 delivered and verified (env-gated, skips clean). Ready for the remaining Phase 11 requirements (MTLS-01/03) and milestone v1.3 packaging work.
- Blocker/concern: plan 11-01's unit-test failures (deferred above) should be closed before the phase is marked verified.

## Self-Check: PASSED
- FOUND: tests/integration/test_mtls_e2e.py
- FOUND commit: e8eae01

---
*Phase: 11-mtls-transport-hardening*
*Completed: 2026-07-09*
