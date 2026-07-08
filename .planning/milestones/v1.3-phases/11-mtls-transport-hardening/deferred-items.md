# Deferred Items — Phase 11 (mTLS Transport Hardening)

Out-of-scope discoveries logged during execution. NOT fixed by the plan that
found them (scope boundary: only auto-fix issues directly caused by the current
task's changes).

## Discovered during plan 11-02 execution (2026-07-09)

7 pre-existing failures in the DEFAULT (`-m 'not integration'`) suite, present
before and after adding `tests/integration/test_mtls_e2e.py` (proven by running
the default suite with the new file absent — still 7 failed). These live in
`tests/test_adapters.py` and `tests/test_lib.py`, which plan 11-02 does not
touch (`tests/test_adapters.py` is modified in the working tree from prior,
uncommitted plan 11-01 work).

- `tests/test_adapters.py::TestConfluentConsumerAdapterConfig::test_mtls_ssl_keys_omitted_on_both_confs_when_unset[admin]`
- `tests/test_adapters.py::TestConfluentConsumerAdapterConfig::test_real_librdkafka_accepts_built_conf[kwargs1]`
- `tests/test_adapters.py::TestConfluentConsumerAdapterConfig::test_real_librdkafka_accepts_built_conf[kwargs2]`
- `tests/test_lib.py::TestKafkaClient::test_from_env_raises_config_error_when_no_broker`
- `tests/test_lib.py::TestKafkaMcpSettingsSSL::test_ssl_fields_default_none`
- (plus 2 additional failures under the same `test_adapters.py` /
  `test_lib.py` modules — full list from
  `uv run python -m pytest tests/`)

These belong to plan 11-01 (mTLS config wiring) and its associated unit tests,
which are still uncommitted in the working tree. They should be resolved as part
of completing/verifying plan 11-01, not 11-02.

Note on tooling: `uv run pytest` binds to a stale Python 3.10 interpreter that
lacks the editable install; use `uv run python -m pytest` (Python 3.14 venv) so
`kafka_mcp` imports resolve.

## Update during plan 11-01 execution (2026-07-09)

Root cause of the above `test_adapters.py` failures identified: `KafkaMcpSettings`
auto-loads a project `.env` (`env_file=".env"` in its `model_config`). This
developer machine's local `.env` sets `KAFKA_MCP_SECURITY_PROTOCOL=SSL` and points
`KAFKA_MCP_SSL_*` at real, machine-local cert paths
(`/opt/develop/integration-tests-ci/...`). Any test constructing a "plain"
`KafkaMcpSettings(bootstrap_servers=...)` therefore inherited those ssl.* values,
breaking "omitted when unset" assertions and causing the REAL-Consumer test to
`fopen` a missing CA file.

Resolved within plan 11-01 scope (`tests/test_adapters.py` only):
- Added a `_clear_ambient_ssl_env(monkeypatch)` helper and passed `_env_file=None`
  to the hermetic-baseline settings constructions. All 84 tests in
  `tests/test_adapters.py` now pass, including the two
  `test_real_librdkafka_accepts_built_conf[kwargs1|kwargs2]` cases.

Still deferred (OUT of plan 11-01 scope — `tests/test_lib.py` is not in this
plan's `files_modified`):
- `tests/test_lib.py::TestKafkaClient::test_from_env_raises_config_error_when_no_broker`
- `tests/test_lib.py::TestKafkaMcpSettingsSSL::test_ssl_fields_default_none`

  Same `.env`-leak root cause; the fix is the identical `_env_file=None` /
  ambient-clear idiom, but applied to `tests/test_lib.py`. Should be addressed by
  a plan that owns `tests/test_lib.py`.
