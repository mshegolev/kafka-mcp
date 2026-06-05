---
phase: 01-foundation
fixed_at: 2026-06-05T00:00:00Z
review_path: .planning/phases/01-foundation/01-REVIEW.md
iteration: 2
findings_in_scope: 4
fixed: 6
skipped: 0
status: all_fixed
---

# Phase 1: Code Review Fix Report

**Fixed at:** 2026-06-05T00:00:00Z
**Source review:** .planning/phases/01-foundation/01-REVIEW.md
**Iteration:** 2

**Summary:**
- Findings in scope (critical + warning): 4
- Fixed: 6 (4 warnings + 2 info pulled in to satisfy the WR-02 lint-gate mandate)
- Skipped: 0

All four Warning findings were fixed. The WR-02 fix mandate explicitly
required `ruff check src/` to be clean ("resolve the E402/F401/I001/UP037
violations"). Clearing the remaining UP037 and I001 violations meant also
applying IN-02 and IN-03, which are otherwise Info-tier. They are committed
as separate atomic commits and reported below for completeness.

**Gates (run after all fixes):**
- `ruff check src/` → All checks passed (clean, was 14 errors).
- `python3 -m pytest -q` → 92 passed.

## Fixed Issues

### WR-02: WR-03 fix introduced an E402 lint regression

**Files modified:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py`
**Commit:** 3c256e3
**Applied fix:** Moved `_METADATA_TIMEOUT_SECONDS = 10.0` from between the
third-party `confluent_kafka` import and the first-party imports to below all
imports, clearing the three E402 violations. The remaining auto-fixable
violations (F401/I001/UP037) flagged by the same lint gate are addressed in the
WR-03, IN-02, and IN-03 commits below; `ruff check src/` is fully clean after
the complete set.

### WR-01: `get_partition_ids` mis-maps transient metadata errors to TopicNotFound

**Files modified:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py`
**Commit:** e1f3d81
**Applied fix:** Replaced the broad `topic_meta.error is not None ->
TopicNotFoundError` logic with the same code-discrimination WR-04 applied to
`get_watermark_offsets`. Now `topic_meta is None` raises `TopicNotFoundError`;
when `topic_meta.error` is set, only `UNKNOWN_TOPIC_OR_PART`, `_UNKNOWN_TOPIC`,
and `_UNKNOWN_PARTITION` map to `TopicNotFoundError`, and every other
(transient/operational) error re-raises as `KafkaException(err)` so a live
topic in a transient state is not reported as missing / 404'd. **Requires
human verification** — this is a logic change to broker error handling; the 4
existing partition-related tests pass, but confirm the transient-error
re-raise path matches intended behavior.

### WR-03: Dead imports across multiple modules (F401)

**Files modified:** `src/kafka_mcp/adapters/outbound/schema_registry_http.py`,
`src/kafka_mcp/domain/search_service.py`,
`src/kafka_mcp/adapters/inbound/lib.py`,
`src/kafka_mcp/adapters/outbound/confluent_consumer.py`
**Commit:** 7eb1515
**Applied fix:** Removed genuinely unused imports:
- `schema_registry_http.py` — removed `import httpx` (referenced only in a
  commented-out Phase-2 block) and `SchemaRegistryPort`. Verified safe: the
  adapter satisfies the Protocol *structurally* (no subclassing), the symbol is
  referenced only in docstrings, and it is NOT part of any `__all__` public
  contract (grep confirmed no use beyond the import and docstrings).
- `search_service.py` — removed `TopicNotFoundError` (raised by the injected
  consumer, not by this module).
- `lib.py` — removed `ConfigError, TopicNotFoundError` (mentioned only in
  docstrings; the package re-exports them from `domain.errors` in
  `__init__.py`, not via `lib`).
- `confluent_consumer.py` — removed `ConsumerPort` (structural conformance,
  referenced only in docstrings, not in `__all__`).

### WR-04: SC-3 boundary test only catches `import confluent_kafka`

**Files modified:** `tests/test_lib.py`
**Commit:** a77da72
**Applied fix:** Broadened the grep to `-rE
"(import confluent_kafka|from confluent_kafka)"` so the boundary guard also
catches the `from confluent_kafka import X` form (the form the outbound adapter
actually uses). Extended the scan to `src/kafka_mcp/ports/` in addition to
`domain/`, replaced the hard-coded absolute `cwd` with
`pathlib.Path(__file__).resolve().parents[1]` (added the `pathlib` import), and
updated the assertion message. Test passes; `ports/` confirmed clean.

### IN-02: Quoted forward-reference type annotations now redundant (UP037)

**Files modified:** `src/kafka_mcp/config.py`,
`src/kafka_mcp/adapters/inbound/lib.py`,
`src/kafka_mcp/adapters/outbound/confluent_consumer.py`
**Commit:** 3fceb2e
**Applied fix:** Dropped the quotes from the three self-referential return
annotations (`-> KafkaMcpSettings`, `-> KafkaClient`,
`-> ConfluentConsumerAdapter`); valid because every module has
`from __future__ import annotations`. Pulled in to satisfy the WR-02 lint-gate
mandate (UP037 was part of the 14-error count).

### IN-03: Import blocks unsorted in CLI and REST adapters (I001)

**Files modified:** `src/kafka_mcp/adapters/inbound/cli.py`,
`src/kafka_mcp/adapters/inbound/rest_api.py`
**Commit:** c3b077b
**Applied fix:** Ran `ruff check --fix --select I001` on the two files
(removed a stray extra blank line after each import block). Pulled in to
satisfy the WR-02 lint-gate mandate.

---

_Fixed: 2026-06-05T00:00:00Z_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 2_
