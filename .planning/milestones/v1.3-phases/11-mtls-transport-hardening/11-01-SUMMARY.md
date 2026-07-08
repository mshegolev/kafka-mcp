---
phase: 11-mtls-transport-hardening
plan: 01
subsystem: kafka-adapter / docs
tags: [mtls, ssl, testing, documentation, security]
requires:
  - src/kafka_mcp/adapters/outbound/confluent_consumer.py (mTLS wiring, pre-existing)
  - src/kafka_mcp/config.py (ssl_* + security_protocol fields, pre-existing)
provides:
  - AdminClient-conf mTLS assertions (previously unasserted gap)
  - README mTLS operator documentation
affects:
  - tests/test_adapters.py
  - README.md
tech-stack:
  added: []
  patterns:
    - "Hermetic settings tests via _env_file=None + ambient KAFKA_MCP_* clearing"
    - "Parametrized conf-builder coverage (consumer + admin) from one test body"
key-files:
  created: []
  modified:
    - tests/test_adapters.py
    - README.md
    - .planning/phases/11-mtls-transport-hardening/deferred-items.md
decisions:
  - "Assert ssl.* on BOTH consumer and admin conf via a single parametrized test (per plan D)."
  - "Fix pre-existing .env-leak in test_adapters.py (hermetic settings) — in-scope for this file; leave identical test_lib.py leak deferred (not in files_modified)."
metrics:
  duration: ~28m
  completed: 2026-07-09
status: complete
---

# Phase 11 Plan 01: mTLS AdminClient Assertions + README Docs Summary

Locked the existing mTLS config wiring behind asserting tests on the previously-unasserted
AdminClient conf path (MTLS-01) and documented end-to-end mTLS operator setup in the README
(MTLS-03) — no adapter/config source was re-implemented.

## What Was Built

### Task 1 — MTLS-01: AdminClient-conf mTLS assertions (commit `a09d7a5`)
Added two parametrized tests to `TestConfluentConsumerAdapterConfig` in
`tests/test_adapters.py`, plus a `_capture_confs(settings)` helper that patches BOTH
`Consumer` and `AdminClient` with conf-capturing `side_effect`s (mirroring the existing
`fake_consumer(conf)` idiom) and returns `{"consumer": conf, "admin": conf}`:

- `test_mtls_ssl_keys_added_on_both_confs_when_fields_set[consumer|admin]` — with
  `SECURITY_PROTOCOL=SSL` and all four `ssl_*` fields set, asserts
  `ssl.certificate.location`, `ssl.key.location`, `ssl.ca.location` are present,
  `ssl.key.password` is unwrapped to plaintext (`SecretStr.get_secret_value()`),
  `security.protocol == "SSL"`, and no `sasl.*` key exists — on **both** builders.
- `test_mtls_ssl_keys_omitted_on_both_confs_when_unset[consumer|admin]` — with a plain
  PLAINTEXT baseline, asserts none of the four `ssl.*` keys land on **either** builder.

The AdminClient conf was previously patched with a bare `return_value=MagicMock()`, so its
`ssl.*` wiring was untested; these parametrized tests close that gap.

### Task 2 — MTLS-03: README mTLS section (commit `7f80d17`)
Added an `### mTLS (client-certificate TLS)` subsection under `## Configure` (before `## Run`)
documenting `KAFKA_MCP_SECURITY_PROTOCOL=SSL` plus the four `KAFKA_MCP_SSL_*` env vars
(certificate/key/CA PEM locations + key password) as a copy-pasteable `bash` export block,
a prose paragraph explaining that SSL is the activation switch and the keys wire into both
the consumer and admin/lag paths, that the key password is a `SecretStr` (never logged), and
a never-commit-secrets caution pointing cert/key/CA at mounted secret paths.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1/3 — Bug + Blocking test isolation] `.env`-leak broke hermetic config tests**
- **Found during:** Task 1 (running the plan's own verify command).
- **Issue:** `KafkaMcpSettings` auto-loads a project `.env` (`env_file=".env"`). This
  developer machine's `.env` sets `KAFKA_MCP_SECURITY_PROTOCOL=SSL` and points
  `KAFKA_MCP_SSL_*` at real, machine-local cert paths. Any test constructing a "plain"
  `KafkaMcpSettings(bootstrap_servers=...)` inherited those `ssl.*` values, so the
  "omitted when unset" assertions failed and the real-`Consumer` test failed trying to
  `fopen` a missing CA file. This affected my new tests AND the pre-existing
  `test_ssl_cert_keys_omitted_when_fields_unset` and
  `test_real_librdkafka_accepts_built_conf[kwargs1|kwargs2]`.
- **Fix:** Added a `_clear_ambient_ssl_env(monkeypatch)` helper and passed `_env_file=None`
  to the hermetic-baseline settings constructions in `tests/test_adapters.py` (the only file
  this plan owns). All 84 tests in the module now pass.
- **Files modified:** `tests/test_adapters.py`
- **Commit:** `a09d7a5`

### Deferred (out of scope)

Two identical `.env`-leak failures live in `tests/test_lib.py`
(`test_from_env_raises_config_error_when_no_broker`,
`TestKafkaMcpSettingsSSL::test_ssl_fields_default_none`). `tests/test_lib.py` is **not** in
this plan's `files_modified`, so per the scope boundary they are logged to
`.planning/phases/11-mtls-transport-hardening/deferred-items.md` and left for a plan that
owns that file. The fix is the same `_env_file=None` / ambient-clear idiom.

## Verification

- `uv run pytest tests/test_adapters.py -k "ssl or admin or mtls" -v` → **7 passed**
  (consumer + admin mTLS assertions).
- `uv run pytest tests/test_adapters.py -k "config" --tb=short` → **14 passed** (zero
  regressions on the existing config suite; PLAINTEXT/SASL unchanged).
- Full module `uv run pytest tests/test_adapters.py` → **84 passed**.
- README grep gate (all four `KAFKA_MCP_SSL_*` vars + `SECURITY_PROTOCOL=SSL`) → **OK**.

## Threat Coverage

- **T-11-01 (key password disclosure):** test asserts `ssl.key.password` is unwrapped only
  into the local conf; README warns never to commit the passphrase. No new logging surface.
- **T-11-02 (broker spoofing):** README documents `ssl.ca.location` as the CA that verifies
  the broker (accepted; verification is librdkafka's responsibility).
- **T-11-SC (install legitimacy):** no package installs in this plan (tests + docs only).

No new security-relevant surface introduced beyond the already-planned threat model.

## Notes

- Environment: `uv run pytest` initially bound to a system Python 3.10 lacking the editable
  install; running `uv sync --extra dev` provisioned pytest into the project `.venv`
  (Python 3.14) so `kafka_mcp` and the tests resolve. `uv.lock` was regenerated by the sync
  but is intentionally NOT part of this plan's commits (build-env artifact, left in the
  working tree).

## Self-Check: PASSED

- FOUND: tests/test_adapters.py
- FOUND: README.md
- FOUND: .planning/phases/11-mtls-transport-hardening/11-01-SUMMARY.md
- FOUND commit: a09d7a5 (Task 1)
- FOUND commit: 7f80d17 (Task 2)
