---
phase: 02-search-decode
fixed_at: 2026-06-05T22:23:34Z
review_path: .planning/phases/02-search-decode/02-REVIEW.md
iteration: 1
findings_in_scope: 7
fixed: 7
skipped: 0
status: all_fixed
---

# Phase 2: Code Review Fix Report

**Fixed at:** 2026-06-05T22:23:34Z
**Source review:** .planning/phases/02-search-decode/02-REVIEW.md
**Iteration:** 1

**Summary:**
- Findings in scope (Critical + Warning): 7
- Fixed: 7
- Skipped: 0

All in-scope Critical (CR-01, CR-02) and Warning (WR-01..WR-05) findings were
fixed. Both gates pass on the fix branch: `ruff check .` is clean and
`python3 -m pytest -q` reports **181 passed**.

Note on test execution: the package is installed editable against the MAIN repo
`src/` (a `.pth` entry), so the worktree's edited sources are only exercised
when `PYTHONPATH=<worktree>/src` is prepended. All gate runs above used that
override; after the branch fast-forwards into the main checkout, the editable
install resolves to the same (now-updated) sources.

## Fixed Issues

### CR-01: Protobuf decode is structurally broken — raises AttributeError at construction

**Files modified:** `src/kafka_mcp/adapters/outbound/schema_registry_http.py`, `tests/test_adapters.py`
**Commit:** 250d7d2
**Applied fix:** Replaced the broken `ProtobufDeserializer(None, ...)` construction
(which dereferenced `None.DESCRIPTOR` and failed every Protobuf payload) with a
REAL generic Protobuf decode driven by the Schema-Registry-registered schema:
- Strips the Confluent framing (magic + 4-byte schema id) and the Protobuf
  message-index header (varint count + indices, with the single-`0x00`
  "first message" shorthand) — new pure helpers `_read_varint`,
  `_strip_protobuf_index_header`, `_resolve_message_by_index`.
- Compiles `schema.schema_str` (the registered `.proto`) to a
  `FileDescriptorSet` via the `protoc` binary, loads it into a private
  `DescriptorPool`, builds the message class via `message_factory`, resolves
  the concrete message type by the message-index path, `ParseFromString`s, and
  renders to a plain dict via `MessageToDict`. Compiled descriptors are cached
  per `schema_str`.
- Removed the dead/unreachable branches (IN-02 also resolved as a side effect)
  and dropped the now-unused `ProtobufDeserializer` import.
- Documented the precise limitation in the module + method docstrings: generic
  decode requires a `protoc` binary on PATH; when absent (or message-index
  resolution fails) a typed `DecodeError` is raised — never an unhandled
  exception. Pre-compiled message classes are NOT required.
- Added real (non-mock) tests that compile a real `.proto`, build a real
  Confluent-framed payload, and assert (a) construction no longer raises
  AttributeError and (b) a full round-trip decodes to the expected dict
  (`test_protobuf_construction_does_not_raise_attributeerror`,
  `test_decode_magic_byte_protobuf_roundtrip`). Tests skip cleanly if `protoc`
  is unavailable.

**SC-2 status:** Protobuf decode now WORKS end-to-end for the common single-file,
single-message case (and nested/index-path cases), so ROADMAP Phase 2 Success
Criterion 2 ("decoded `value` for all three wire formats: Avro, Protobuf, JSON")
is met **conditional on a `protoc` binary being present on PATH** at runtime.
This runtime dependency is the documented residual limitation the orchestrator/
verifier should confirm in the deployment environment; schemas with unresolved
imports are not decodable from `schema_str` alone and surface a typed
`DecodeError`.

### CR-02: DecodeError on the get_message path always carries wrong coordinates

**Files modified:** `src/kafka_mcp/domain/search_service.py`, `tests/test_lib.py`
**Commit:** dbeb4b7
**Applied fix:** Both decode call sites in the domain service now forward the
message coordinates: `self._registry.decode(raw_msg.raw, raw_msg.topic,
raw_msg.partition, raw_msg.offset)` (in `get_message` and inside the
`search_messages` per-message loop). Added a regression test
(`test_get_message_decode_error_carries_real_coordinates`) where the registry
mock echoes the coordinates it actually received and the propagated
`DecodeError.topic/partition/offset` are asserted to equal the requested
`payments[3]@1500` (also closes the IN-04 coverage gap).

**Requires human verification:** No — verified by an end-to-end coordinate-echo
test, not just syntax.

## Fixed Issues — Warnings

### WR-01: SchemaRegistryPort.decode signature did not match the adapter

**Files modified:** `src/kafka_mcp/ports/schema_registry.py`, `src/kafka_mcp/adapters/inbound/lib.py`, `tests/test_lib.py`, `tests/test_domain.py`
**Commit:** 147dd7c
**Applied fix:** Made the Protocol the source of truth — `decode(self, raw,
topic="", partition=0, offset=0)`. Updated `_NullSchemaRegistry.decode` and
every test `MockSchemaRegistry`/inline mock (including the two positional-only
subclasses in `test_lib.py`) to accept the same optional coordinate params, so a
caller passing coordinates to a mock/null registry no longer hits a `TypeError`.
This was the structural prerequisite for CR-02.

### WR-02: search_messages time window could truncate prematurely

**Files modified:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py`, `tests/test_adapters.py`
**Commit:** 0d62fc7 (combined with WR-03 — same line)
**Applied fix:** Changed the out-of-window guard from `break` to `continue` so a
single out-of-order/future-dated message no longer terminates the scan and drops
later in-window messages; termination is driven by `stop_offset`/`max_scan`/
`limit`. Added `test_fetch_messages_out_of_order_timestamp_does_not_truncate`
(offsets `[0, 2]` returned with a future-dated offset 1 skipped).

### WR-03: time_to documented exclusive but compared inclusively

**Files modified:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py`, `tests/test_adapters.py`
**Commit:** 0d62fc7 (combined with WR-02 — same line)
**Applied fix:** Changed the boundary comparison from `> time_to` to
`>= time_to` so a message whose timestamp equals `time_to` is excluded, honoring
the documented exclusive semantics. Replaced the old `stops_at_time_to` test
with `test_fetch_messages_excludes_messages_at_or_after_time_to` asserting a
message at exactly `time_to` is dropped.

### WR-04: JSON fallback fabricated a {"value": ...} envelope

**Files modified:** `src/kafka_mcp/adapters/outbound/schema_registry_http.py`, `tests/test_adapters.py`
**Commit:** 8468778
**Applied fix:** `_decode_json` now returns `None` for non-object JSON
(list/int/str/bool/null) instead of wrapping it as `{"value": result}`, so `raw`
remains the source of truth and `value:<path>` matching / Evidence extraction
are not fed a synthetic shape. Added
`test_decode_json_fallback_non_object_returns_none` covering array and scalar
payloads.

### WR-05: in-range poll timeout mapped to MessageNotFoundError

**Files modified:** `src/kafka_mcp/domain/errors.py`, `src/kafka_mcp/adapters/outbound/confluent_consumer.py`, `src/kafka_mcp/adapters/inbound/rest_api.py`, `src/kafka_mcp/adapters/inbound/cli.py`, `src/kafka_mcp/adapters/inbound/mcp_stdio.py`, `tests/test_adapters.py`
**Commit:** 21953f5
**Applied fix:** Introduced a new typed `TransientError` (topic/partition/offset/
reason). In `fetch_message`, a `None` poll result or `msg.error()` for an offset
already proven in-range (`low <= offset < high`) now raises `TransientError`
instead of `MessageNotFoundError`; `MessageNotFoundError` is reserved for the
watermark range-check. Wired graceful handling at all three inbound faces so the
new error does not become an unhandled crash: REST → HTTP 503, CLI → exit 3,
MCP → `ValueError("Transient read failure: ...")`. Updated the timeout test to
assert `TransientError` (and that it is not a `MessageNotFoundError`).

## Info findings handled to satisfy the gates

### IN-01: ruff F401/I001 nits (required for the `ruff check .` gate)

**Files modified:** `src/kafka_mcp/ports/consumer.py`, `src/kafka_mcp/ports/schema_registry.py`, `src/kafka_mcp/adapters/outbound/confluent_consumer.py`, `tests/test_adapters.py`, `tests/test_domain.py`
**Commit:** d5e0818
**Applied fix:** Although IN-01 is Info-tier (outside the critical_warning scope),
the orchestrator-mandated `ruff check .` gate must be clean. Added
`# noqa: F401  # referenced in docstring` to the two Port imports that are
intentionally documentation-only (`MessageNotFoundError`, `DecodeError`), and
applied `ruff --fix` for the import-sort (I001) and the genuinely-unused
`KafkaMessage` import in `tests/test_domain.py`. `ruff check .` now reports
"All checks passed!".

## Skipped Issues

None — all in-scope findings were fixed.

## Info findings deferred (not in scope, no gate impact)

- **IN-02** (dead/unreachable code in `_decode_protobuf`) — resolved as a side
  effect of the CR-01 rewrite (the dead branches were removed).
- **IN-03** (`PartitionInfo.leader=0` placeholder) — deferred to Phase 3 per the
  review (AdminClient wiring); no code change.
- **IN-04** (coordinate bug untestable with current mocks) — closed by the CR-02
  regression test that echoes received coordinates.
- **IN-05** (redundant limit guards in `search_messages`) — low-priority
  readability nit; not addressed (no correctness or gate impact).

---

_Fixed: 2026-06-05T22:23:34Z_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
