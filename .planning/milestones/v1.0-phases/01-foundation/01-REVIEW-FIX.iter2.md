---
phase: 01-foundation
fixed_at: 2026-06-05T00:00:00Z
review_path: .planning/phases/01-foundation/01-REVIEW.md
iteration: 1
findings_in_scope: 5
fixed: 5
skipped: 0
status: all_fixed
---

# Phase 1: Code Review Fix Report

**Fixed at:** 2026-06-05
**Source review:** .planning/phases/01-foundation/01-REVIEW.md
**Iteration:** 1

**Summary:**
- Findings in scope: 5 (1 Critical + 4 Warning; Info deferred per `critical_warning` scope)
- Fixed: 5
- Skipped: 0
- Full suite after fixes: `python3 -m pytest -q` → **92 passed**

## Fixed Issues

### CR-01: `SSL`/`SASL_SSL`-without-mechanism crashes at construction

**Files modified:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py`, `tests/test_adapters.py`
**Commit:** ad0c0b0
**Applied fix:** SASL configuration is now gated on `if settings.sasl_mechanism:`
(truthy) rather than `if settings.security_protocol != "PLAINTEXT":`. The
`security.protocol` key is still set for any non-PLAINTEXT protocol, but `sasl.*`
keys are added only when a mechanism is requested, and each individual
`sasl.username` / `sasl.password` key is omitted when its value is `None`. This
means a TLS-only deployment (`KAFKA_MCP_SECURITY_PROTOCOL=SSL`, no SASL) no
longer injects `sasl.mechanism=None` and no longer trips librdkafka's
`_INVALID_ARG` rejection at `Consumer()` construction.

Added non-mocked regression coverage that constructs a **real**
`confluent_kafka.Consumer` from the built conf (no `Consumer` mock), asserting no
`KafkaException` for PLAINTEXT, SSL-only, and SASL_SSL-with-explicit-mechanism.
Note on scope: `SASL_SSL` with **no** mechanism is intentionally not asserted as
"constructs cleanly" — SASL inherently requires a mechanism and librdkafka
rejects that configuration on its own merits (it defaults to GSSAPI/Kerberos and
demands a keytab), independent of this adapter. The fix's contribution is that
our code no longer manufactures a misleading `sasl.mechanism=None`; it leaves
genuinely-invalid SASL-without-mechanism configs to fail on librdkafka's own,
clearer terms. Two additional mocked tests assert (a) `sasl.*` keys are omitted
for SSL-only and (b) all three `sasl.*` values are present for SASL_SSL+PLAIN.

### WR-01: Invalid `max_scan` / `poll_timeout` env vars leaked raw `ValidationError`

**Files modified:** `src/kafka_mcp/config.py`, `tests/test_lib.py`
**Commit:** 9c3c6ec
**Applied fix:** Added a catch-all conversion in `KafkaMcpSettings.__init__` after
the typed `missing` / `value_error` branches. Any remaining pydantic error type
(e.g. `int_parsing` for `KAFKA_MCP_MAX_SCAN=abc`, `float_parsing` for
`KAFKA_MCP_POLL_TIMEOUT=xyz`) is now converted into a `ConfigError` naming the
offending `KAFKA_MCP_<FIELD>` key with the pydantic message, honoring the D-04
single-exception contract. Added `TestConfigErrorContract` with three tests
asserting invalid `max_scan` / `poll_timeout` raise `ConfigError` (not a raw
`ValidationError`) and that the env-key name appears in the message.

### WR-02: `from_env()`-constructed consumers were never closed (librdkafka leak)

**Files modified:** `src/kafka_mcp/adapters/inbound/lib.py`,
`src/kafka_mcp/adapters/outbound/confluent_consumer.py`,
`src/kafka_mcp/adapters/inbound/cli.py`, `src/kafka_mcp/server.py`,
`src/kafka_mcp/adapters/inbound/rest_api.py`, `tests/test_lib.py`
**Commits:** 7c7d8bd, c532032
**Applied fix:**
- Added a standalone `close()` to `ConfluentConsumerAdapter` and routed `__exit__`
  through it.
- Gave `KafkaClient` a `close()` plus `__enter__`/`__exit__` that delegate to the
  injected consumer's `close()` when present (no-op for mock consumers without
  `close()`, so DI test doubles keep working).
- Wired callers to release the consumer on shutdown: `cli.main` now wraps dispatch
  in `try/finally: client.close()`; the MCP stdio path in `server.py` wraps
  `server.run("stdio")` likewise; the FastAPI app closes the consumer via a
  `lifespan` shutdown handler (commit c532032 migrated this from the deprecated
  `@app.on_event("shutdown")` to `lifespan`, eliminating the FastAPI
  `DeprecationWarning`).
- Added `TestKafkaClientLifecycle` (3 tests) covering `close()` delegation, the
  context-manager protocol, and the no-`close()` mock no-op.

This also incidentally resolves the IN-03 concern (`KafkaClient._consumer` was
dead state): the field is now read by `close()`, giving it a clear lifecycle
purpose. IN-03 itself remains out of scope for this `critical_warning` pass.

### WR-03: `get_watermark_offsets` used `poll_timeout` as the metadata budget

**Files modified:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py`,
`tests/test_adapters.py`
**Commit:** a6662d2 (combined with WR-04 — same method, same hunk)
**Applied fix:** Introduced a module-level `_METADATA_TIMEOUT_SECONDS = 10.0` and
used it for the `get_watermark_offsets` broker round-trip, matching the hardcoded
10.0s budget already used by `list_topics` / `get_partition_ids`. `poll_timeout`
is now reserved for actual `consumer.poll()` calls (Phase 2). Added a test
asserting the watermark fetch is called with `timeout=10.0`, not the 1.0s
`poll_timeout` default.

### WR-04: `get_watermark_offsets` collapsed all `KafkaException`s into `TopicNotFoundError`

**Files modified:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py`,
`tests/test_adapters.py`
**Commit:** a6662d2 (combined with WR-03 — same method, same hunk)
**Applied fix:** The `except KafkaException` handler now inspects
`exc.args[0].code()` and maps only the genuine not-found codes
(`UNKNOWN_TOPIC_OR_PART`, `_UNKNOWN_TOPIC`, `_UNKNOWN_PARTITION`) to
`TopicNotFoundError`; all other `KafkaException`s (timeout, transport, broker
unavailable, auth) are re-raised unchanged so a broker outage is no longer
mis-reported as a missing topic (and 404'd by the REST face). Updated the
existing not-found test to raise a coded `KafkaError` and added two tests:
unknown-partition → `TopicNotFoundError`, and a transient `_TRANSPORT` error →
re-raised `KafkaException` (explicitly asserting it is NOT mistranslated).

> **Note on combined commit (WR-03 + WR-04):** both findings live in the same
> `get_watermark_offsets` method and the edits are interleaved in a single hunk
> (the timeout argument and the surrounding `except` block). They were committed
> together as a6662d2 rather than split, because an atomic split would have left
> one commit referencing code the other commit rewrites in the same lines.

## Skipped Issues

None — all in-scope (Critical + Warning) findings were fixed. The four Info
findings (IN-01 unused `httpx` import, IN-02 `orjson` type hints, IN-03 duplicated
`_consumer` state, IN-04 `leader=0` placeholder) were out of scope for the
`critical_warning` pass. IN-03 is now effectively neutralized as a side effect of
WR-02 (the `_consumer` field gained a real lifecycle purpose via `close()`).

## Verification Notes

- **Tier 1 (re-read)** applied to every edited file.
- **Tier 2 (syntax/semantic):** `python -c "ast.parse(...)"` on every modified
  `.py`, plus targeted `pytest` runs per finding, plus the full suite at the end.
- **Environment caveat:** the package is installed editable pointing at the main
  repo `src/`, so test runs inside the worktree were executed with
  `PYTHONPATH=<worktree>/src python3 -m pytest -q` to exercise the fixed source
  rather than the stale editable path. Final result: **92 passed**.

---

_Fixed: 2026-06-05_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
