---
phase: 02-search-decode
reviewed: 2026-06-06T00:00:00Z
depth: standard
files_reviewed: 17
files_reviewed_list:
  - src/kafka_mcp/domain/models.py
  - src/kafka_mcp/domain/errors.py
  - src/kafka_mcp/domain/search_service.py
  - src/kafka_mcp/ports/consumer.py
  - src/kafka_mcp/ports/schema_registry.py
  - src/kafka_mcp/adapters/outbound/confluent_consumer.py
  - src/kafka_mcp/adapters/outbound/schema_registry_http.py
  - src/kafka_mcp/adapters/inbound/lib.py
  - src/kafka_mcp/adapters/inbound/mcp_stdio.py
  - src/kafka_mcp/adapters/inbound/rest_api.py
  - src/kafka_mcp/adapters/inbound/cli.py
  - src/kafka_mcp/__init__.py
  - tests/test_domain.py
  - tests/test_adapters.py
  - tests/test_lib.py
  - tests/test_inbound.py
  - pyproject.toml
findings:
  critical: 2
  warning: 5
  info: 5
  total: 12
status: issues_found
---

# Phase 2: Code Review Report

**Reviewed:** 2026-06-06T00:00:00Z
**Depth:** standard
**Files Reviewed:** 17
**Status:** issues_found

## Summary

Phase 2 (Search + Decode) is well-structured and the hexagonal boundary holds:
`domain/` and `ports/` contain no I/O or decode-library imports, decode libs
(`confluent_kafka.schema_registry`, `fastavro`, `ProtobufDeserializer`,
`google.protobuf`) live only in `schema_registry_http.py`, the consumer is
strictly `assign()`-based with `enable.auto.commit=False` and a throwaway
uuid4 group id, the REST `limit` is bounded (`ge=1, le=10000`), the
`value:<path>` matcher uses split + dict-key access only (no `eval`/`getattr`
on payloads), and credentials are extracted via `SecretStr` / passed straight
into client conf without being stored or logged.

However, the review surfaced two BLOCKER-class defects:

1. **Protobuf decode never works** — `ProtobufDeserializer(None, ...)` raises
   `AttributeError` at construction (confirmed empirically), so every Protobuf
   payload becomes a `DecodeError`. This directly fails ROADMAP Phase 2
   Success Criterion 2 ("decoded `value` for all three wire formats: Avro,
   Protobuf, JSON"). The "success" branch below it is unreachable dead code.
2. **DecodeError coordinates are always wrong** — the domain service calls
   `registry.decode(raw)` with no topic/partition/offset, so every decode
   failure surfaced to the user (REST 422, CLI exit 2, MCP error) reports
   `[0]@0` for an empty topic instead of the real coordinates.

Both undermine the stated decode-failure contract and the SC-2 acceptance
criterion. The remaining warnings concern a Protocol/implementation signature
mismatch, premature time-window truncation, a watermark assumption in the mock
that masks a real edge case, and JSON-fallback returning non-dict shapes.

## Critical Issues

### CR-01: Protobuf decode is structurally broken — raises AttributeError at construction, fails SC-2

**File:** `src/kafka_mcp/adapters/outbound/schema_registry_http.py:209-228`
**Issue:**
`_decode_protobuf` constructs `ProtobufDeserializer(None, {"use.deprecated.format": False})`.
Passing `None` as the `message_type` does not produce a benign sentinel — the
confluent deserializer dereferences `message_type.DESCRIPTOR` during
`__init__`, so construction raises
`AttributeError: 'NoneType' object has no attribute 'DESCRIPTOR'` (verified by
running it against the installed `confluent_kafka`). That `AttributeError` is
caught by the broad `except Exception` and re-wrapped as `DecodeError`, so
**every Confluent-framed Protobuf payload always fails to decode**.

This violates ROADMAP Phase 2 Success Criterion 2, which requires
`get_message` to return a decoded `value` "for all three wire formats (Avro,
Protobuf, JSON)." Avro and JSON work; Protobuf does not. The "KNOWN LIMITATION"
framing (a typed stub) is accurate in effect, but it is a real correctness gap
against an accepted, checked-off ("completed 2026-06-05") success criterion —
not a benign deferral. Severity is BLOCKER because the phase is marked complete
while a named SC is unmet.

The unit test `test_decode_magic_byte_protobuf` does not catch this: it patches
`ProtobufDeserializer` with a `MagicMock`, so the real `None`-construction path
is never exercised. No test decodes a real Protobuf payload end-to-end.

Additionally, lines 215-221 are **dead/unreachable code**: because construction
on line 213 always raises, control never reaches the `hasattr(result,
"DESCRIPTOR")` / `MessageToDict` / `isinstance(result, dict)` branches.

**Fix:** Either (a) make the limitation explicit and honest by raising a typed
`DecodeError` directly without the misleading deserializer construction, and
update the ROADMAP/SUMMARY to record Protobuf as deferred to Phase 3; or
(b) implement real generic Protobuf decode. Minimal honest stub:

```python
def _decode_protobuf(
    self, raw: bytes, schema: Schema, topic: str, partition: int, offset: int
) -> dict:
    # Generic Protobuf decode requires a pre-compiled message class, which
    # we do not have for arbitrary subjects. Fail with an actionable typed
    # error instead of constructing a deserializer that cannot work.
    raise DecodeError(
        topic, partition, offset,
        reason=(
            "protobuf decode requires a pre-compiled message type; "
            "generic decode is not supported (deferred to Phase 3)"
        ),
    )
```

If SC-2 must hold now, wire a message-type registry keyed by schema subject and
construct `ProtobufDeserializer(<concrete_class>, ...)`.

### CR-02: DecodeError raised on the get_message path always carries wrong coordinates ([]@0)

**File:** `src/kafka_mcp/domain/search_service.py:363` (and `:319`)
**Issue:**
The `SchemaRegistryHttpAdapter.decode` implementation accepts
`decode(raw, topic="", partition=0, offset=0)` and uses those to build the
`DecodeError`. But the domain service calls it with the raw bytes only:

```python
decoded = self._registry.decode(raw_msg.raw)        # get_message, line 363
decoded = self._registry.decode(raw_msg.raw)        # search_messages, line 319
```

So `topic`/`partition`/`offset` always fall back to the defaults `""`/`0`/`0`.
On the strict `get_message` path the `DecodeError` propagates all the way to the
inbound faces, which surface the (wrong) coordinates to the user:
- REST `/tools/get_message` returns
  `{"error": "DecodeError", "topic": "", "partition": 0, "offset": 0, ...}`
  (rest_api.py:230-240) for a real message at, e.g., `payments[3]@1500`.
- CLI prints `Error: decode failed for [0]@0: ...` (cli.py:395-401).
- MCP raises `ValueError("Decode failed: [0]@0: ...")` (mcp_stdio.py:184-187).

This is a correctness/observability defect: a decode failure during an
investigation reports the wrong message location, which can send a responder to
the wrong topic/offset. It is masked in tests because `MockSchemaRegistry`
hardcodes `DecodeError(self._decode_topic, 0, 0, ...)` and the inbound tests use
the `corrupt` topic shortcut, so no test asserts that real coordinates flow
through.

**Fix:** Pass the message coordinates through at the call sites and align the
Protocol signature (see WR-01):

```python
# get_message
decoded = self._registry.decode(
    raw_msg.raw, raw_msg.topic, raw_msg.partition, raw_msg.offset
)

# search_messages (inside the per-message loop)
try:
    decoded = self._registry.decode(
        raw_msg.raw, raw_msg.topic, raw_msg.partition, raw_msg.offset
    )
except DecodeError:
    decoded = None
```

## Warnings

### WR-01: SchemaRegistryPort.decode signature does not match the adapter (and the NullRegistry/mocks)

**File:** `src/kafka_mcp/ports/schema_registry.py:33` vs
`src/kafka_mcp/adapters/outbound/schema_registry_http.py:88-94`
**Issue:**
The Protocol declares `def decode(self, raw: bytes) -> dict[str, Any] | None`,
but the real adapter implements `decode(self, raw, topic="", partition=0,
offset=0)`. `_NullSchemaRegistry.decode` (lib.py:59) and the test mocks
(`MockSchemaRegistry.decode`) implement only `(self, raw)`. Because the extra
adapter params are keyword-defaulted, `runtime_checkable` `isinstance` still
passes and the single-arg call site works — but the contract is ambiguous and
is the root cause of CR-02 (the domain layer cannot pass coordinates without
the Protocol blessing it). The mismatch also means a future caller who passes
coordinates to a mock/null registry will get a `TypeError`.
**Fix:** Make the Protocol the source of truth:

```python
def decode(
    self, raw: bytes, topic: str = "", partition: int = 0, offset: int = 0
) -> dict[str, Any] | None: ...
```

Then update `_NullSchemaRegistry.decode` and both test `MockSchemaRegistry`
classes to accept the same optional parameters.

### WR-02: search_messages time window can truncate prematurely (offset order != timestamp order)

**File:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py:258-259`
(driven by `search_service.py:307-314`)
**Issue:**
`fetch_messages` breaks the scan on the **first** message whose
`timestamp_utc > time_to`:

```python
if time_to is not None and ts_utc > time_to:
    break
```

Kafka offsets are not strictly ordered by `CreateTime` (producers can backdate
timestamps, and partitions interleave producer clocks). A single out-of-order
message with a future timestamp will stop the scan early and silently drop
later in-window messages on that partition. For an investigator searching a
time window, this is a correctness gap (false negatives). It is invisible in
tests because the mock messages are monotonically timestamped.
**Fix:** Use `continue` (skip out-of-window messages) instead of `break`, and
rely on `stop_offset`/`max_scan`/`limit` for termination; or document the
"timestamps assumed monotonic per partition" assumption explicitly as an
accepted limitation. At minimum, do not silently terminate on the first
over-bound message.

### WR-03: time_to is documented as exclusive but compared with `>` (inclusive boundary)

**File:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py:258`;
`src/kafka_mcp/domain/search_service.py:245`
**Issue:**
The service docstring states `time_to` is the "Exclusive end of time window",
but the adapter keeps a message when `ts_utc <= time_to` (it only breaks on
`ts_utc > time_to`). A message whose timestamp equals `time_to` is therefore
**included**, contradicting the documented exclusive semantics. Off-by-one at
the boundary.
**Fix:** Use `if time_to is not None and ts_utc >= time_to:` to honor the
documented exclusive bound (combined with the WR-02 `continue` fix), or update
the docstring to say the bound is inclusive.

### WR-04: JSON fallback can return a non-dict-shaped wrapper, violating the value contract

**File:** `src/kafka_mcp/adapters/outbound/schema_registry_http.py:234-239`
**Issue:**
When `json.loads(raw)` yields a non-dict (list/int/str/bool/null), the adapter
wraps it as `{"value": result}`. `KafkaMessage.value` is typed
`dict[str, Any] | None`, so this keeps types happy, but it silently injects a
synthetic `value` key that did not exist in the payload. Downstream
`value:<path>` matching and Evidence extraction then operate on a fabricated
shape (e.g. a JSON array payload becomes `{"value": [...]}`), which can produce
surprising/incorrect matches and makes raw vs decoded reconciliation harder.
**Fix:** Decide explicitly: either (a) return `None` for non-object JSON (the
payload had no object body to decode), keeping `raw` as the source of truth, or
(b) keep the wrapper but document the `"value"` envelope key clearly in the
Port contract and in the matcher semantics so callers know to expect it.

### WR-05: get_message uses `poll_timeout * 5` while fetch_messages uses 1× — a None poll is mapped to MessageNotFoundError

**File:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py:376-382`
**Issue:**
`fetch_message` already range-checks the offset against watermarks, then polls
once with `poll_timeout * 5` and treats `poll() is None` (timeout) **and**
`msg.error()` as `MessageNotFoundError`. A transient broker timeout for an
offset that is genuinely in range (low <= offset < high) is therefore reported
to the user as "message not found" (REST 404, CLI exit 1), conflating a
transient I/O failure with a real absence — the same not-found-vs-transient
discrimination the code is careful about in `get_watermark_offsets` (WR-04
there) and `get_partition_ids` (WR-01 there). This is inconsistent and can
mislead an investigator into believing a known offset has no message.
**Fix:** Distinguish a poll timeout from a true absence. For example, retry/poll
in a short loop until the budget is exhausted, and raise a transient/IO error
(or re-raise the underlying `KafkaException`) on timeout rather than
`MessageNotFoundError`; reserve `MessageNotFoundError` for the watermark
range-check and `msg.error()` cases.

## Info

### IN-01: Unused imports flagged by ruff (5 fixable nits)

**File:** multiple — `ruff check .` reports 5 errors:
- `src/kafka_mcp/ports/consumer.py:12` — `MessageNotFoundError` imported but
  unused (F401). It is referenced only in the docstring `Raises:` block; keep it
  importable for documentation or drop it. As written it is unused code.
- `src/kafka_mcp/ports/schema_registry.py:11` — `DecodeError` imported but
  unused (F401). Same situation. (Note: aligning WR-01 by adding the coordinate
  params does not change this; it is referenced only in the docstring.)
- `src/kafka_mcp/adapters/outbound/confluent_consumer.py:17` — import block
  un-sorted (I001); `TIMESTAMP_CREATE_TIME` should be merged into the preceding
  `from confluent_kafka import ...` line.
- `tests/test_adapters.py:1050` — import block un-sorted (I001).
- `tests/test_domain.py:278` — `KafkaMessage` imported but unused (F401).
**Fix:** `ruff check . --fix` resolves all 5. For the two Port imports, prefer a
deliberate decision (drop them, or add `# noqa: F401  # referenced in docstring`)
rather than leaving genuinely dead imports.

### IN-02: Dead/unreachable code in _decode_protobuf

**File:** `src/kafka_mcp/adapters/outbound/schema_registry_http.py:215-221`
**Issue:** Given CR-01 (construction on line 213 always raises), the
`hasattr(result, "DESCRIPTOR")` / `MessageToDict` / `isinstance(result, dict)`
/ `dict(result)` branches are unreachable. The lazy `from google.protobuf...`
import inside the function is also never executed.
**Fix:** Remove the dead branches as part of the CR-01 fix (the honest-stub form
above removes them entirely).

### IN-03: PartitionInfo.leader is a hardcoded 0 placeholder (carried over, still TODO)

**File:** `src/kafka_mcp/domain/search_service.py:202`
**Issue:** `leader=0` with a `# TODO: AdminClient` note. Harmless for Phase 2
search/decode, but the describe output reports an incorrect leader for every
partition. Noting so it is not forgotten before the brick ships (Phase 3).
**Fix:** Track in Phase 3 backlog; wire real leader via AdminClient or drop the
field from the public contract if it cannot be populated.

### IN-04: get_message error-path coordinate bug is untestable with current mocks

**File:** `tests/test_lib.py:162-165`, `tests/test_inbound.py:96-98`
**Issue:** `MockSchemaRegistry.decode` hardcodes `DecodeError(self._decode_topic,
0, 0, ...)` and the inbound `MockKafkaClient.get_message` raises a pre-built
`DecodeError(topic, partition, offset, ...)` with correct coordinates. Neither
exercises the real adapter call path, so CR-02 (coordinates dropped at the call
site) passes all tests. This is a test-coverage gap, not a test bug.
**Fix:** After fixing CR-02/WR-01, add a test where the registry mock echoes the
coordinates it actually received and assert the propagated `DecodeError.topic/
partition/offset` match the requested message.

### IN-05: search_messages re-checks `len(results) >= limit` / `remaining <= 0` redundantly

**File:** `src/kafka_mcp/domain/search_service.py:277-334`
**Issue:** The limit guard is duplicated at the topic loop (277), partition loop
(283), `remaining` computation (303-305), and inner append (332-333). Logic is
correct but the repeated guards add cyclomatic noise and make the
single-source-of-truth for the limit harder to follow. Low priority.
**Fix:** Consolidate to a single early-exit check per loop iteration, or extract
a small helper/generator that yields raw messages until the budget is hit.

---

_Reviewed: 2026-06-06T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
