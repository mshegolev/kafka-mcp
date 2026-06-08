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
  critical: 0
  warning: 5
  info: 3
  total: 8
status: issues_found
---

# Phase 2: Code Review Report

**Reviewed:** 2026-06-06
**Depth:** standard
**Status:** issues_found
**Files Reviewed:** 17

## Summary

Iteration-2 re-review of the Search + Decode phase. Evidence gathered:

- `ruff check .` → **All checks passed!**
- `python3 -m pytest -q` → **181 passed in 1.17s**
- `protoc` is present on the review host (`/opt/homebrew/bin/protoc`).

**Iteration-1 fixes verified sound:**
- **CR-01 (generic Protobuf decode):** Confirmed real. `_decode_protobuf` strips the
  Confluent framing (magic+schema_id) and the message-index header, compiles the
  registered `.proto` via `protoc` → `FileDescriptorSet` → private `DescriptorPool`,
  resolves the message by index path, builds a dynamic class via `message_factory`,
  and renders with `MessageToDict`. The real-wire round-trip test passes
  (`test_decode_magic_byte_protobuf_roundtrip`). The old `ProtobufDeserializer(None,...)`
  AttributeError path is gone.
- **CR-02 (DecodeError coordinates):** `get_message` and the search loop pass the real
  `raw_msg.topic/partition/offset` into `registry.decode(...)` (`search_service.py:319-324`
  and `:370-375`).
- **5 warnings:** out-of-window `continue` not `break` (`confluent_consumer.py:267-268`);
  exclusive `time_to` via `>=`; JSON non-object→`None` (`schema_registry_http.py:342-346`);
  new typed `TransientError` for in-range poll timeout (`confluent_consumer.py:399-411`);
  decode-signature alignment across port/adapter/null-stub — all confirmed.

**Hexagonal boundary:** intact. All decode libs (`confluent_kafka.schema_registry`,
`fastavro`, `google.protobuf`, `subprocess`/`protoc`) live only in the outbound adapter;
`domain/` and `ports/` stay import-free (boundary test passes). **Read-only guarantee:**
intact — assign-only, `enable.auto.commit=False`, uuid4 throwaway group id, no `subscribe`
in source (regression test asserts this). **Credentials:** `sasl_password`/`sr_pass` are
`SecretStr`, extracted only at the call boundary, never stored on an attribute or logged.

Remaining concerns: the new `protoc` runtime dependency (portability — WR-01), inconsistent
`TransientError` surfacing (WR-02), untested inbound-face mapping of `TransientError`
(WR-03), an unbounded `protoc` subprocess (WR-04), and `MessageToDict` casing/int64
divergence on the Protobuf path (WR-05). No Critical issues.

## Narrative Findings (AI reviewer)

## Warnings

### WR-01: Generic Protobuf decode introduces an undeclared runtime dependency on a `protoc` system binary

**File:** `src/kafka_mcp/adapters/outbound/schema_registry_http.py:274-304`
**Issue:** `_compile_proto_descriptor` shells out to `shutil.which("protoc")` and invokes
the `protoc` binary at decode time. This is a **pip-installable brick** — `pyproject.toml`
declares only Python deps (`protobuf`, `googleapis-common-protos`); it cannot declare a
system binary. On any host where `protoc` is not on PATH (the common case for a
`pip install kafka-mcp` into a slim container or bare venv), **every Protobuf message fails
closed with `DecodeError`** ("protoc binary not found on PATH"). In the resilient
`search_messages` path that DecodeError is swallowed → `value=None`, so Protobuf topics
silently return no decoded fields and no Evidence keys (`order_id`, `msisdn`, ...) — the
investigator's core use case degrades invisibly. In the strict `get_message` path it becomes
an HTTP 422 / CLI exit-2, presenting a portability/ops gap as a payload-decode failure.
Portability concern, not a correctness bug — hence WARNING — but it must be made explicit
before shipping. A pure-Python alternative exists (`grpcio-tools` vendors a `protoc` wheel,
fully `pip`-installable) and removes the system-binary dependency.
**Fix:** Pick one:
1. **Preferred — make it pip-installable** via `grpcio-tools` (`grpc_tools.protoc` is a
   Python module, no PATH binary):
   ```python
   from grpc_tools import protoc as _protoc
   rc = _protoc.main([
       "protoc", f"--proto_path={tmpdir}",
       f"--descriptor_set_out={out_path}", "--include_imports", proto_path,
   ])
   if rc != 0:
       raise RuntimeError("protoc (grpc_tools) failed")
   ```
   Add `grpcio-tools>=1.60` to `[project].dependencies` (or a `proto` extra).
2. **Or document + degrade loudly:** declare the `protoc` requirement prominently in
   README/install docs, gate it behind an optional extra, and surface the "protoc missing"
   reason distinctly (e.g. a dedicated `reason` prefix the REST face maps to 501/503 instead
   of 422) so an ops gap is not reported as a corrupt payload.

### WR-02: `TransientError` is not exported from the package public API, unlike its sibling exceptions

**File:** `src/kafka_mcp/__init__.py:16-33`
**Issue:** `__init__.py` re-exports `ConfigError`, `DecodeError`, `MessageNotFoundError`, and
`TopicNotFoundError` in `__all__`, but **omits `TransientError`** — a new public exception
`get_message` can now raise and that every inbound face (REST 503, MCP `ValueError`, CLI
exit-3) catches by name. A consumer using `from kafka_mcp import KafkaClient` who wants to
distinguish "transient/retryable" from "not found" cannot `from kafka_mcp import
TransientError`; they must reach into the internal `kafka_mcp.domain.errors`. Inconsistent,
leaky public contract for an exception that is the centerpiece of the new retry semantics.
**Fix:**
```python
from kafka_mcp.domain.errors import (
    ConfigError, DecodeError, MessageNotFoundError,
    TopicNotFoundError, TransientError,
)
__all__ = [
    "KafkaClient", "TopicInfo", "PartitionInfo", "KafkaMessage",
    "TopicNotFoundError", "ConfigError", "DecodeError",
    "MessageNotFoundError", "TransientError",
]
```

### WR-03: New `TransientError` inbound-face mapping (REST 503 / MCP / CLI exit-3) has no test coverage

**File:** `src/kafka_mcp/adapters/inbound/rest_api.py:231-242`,
`src/kafka_mcp/adapters/inbound/mcp_stdio.py:189-194`,
`src/kafka_mcp/adapters/inbound/cli.py:396-403`
**Issue:** The only test exercising `TransientError` is at the adapter layer
(`test_adapters.py:1313` — confirms the consumer *raises* it). The three new inbound
`except TransientError` branches that translate it to HTTP 503 / `ValueError` / `sys.exit(3)`
are **not exercised by any test** (grep for `503`/`TransientError` in `tests/` returns only
the adapter test). A regression that re-orders the `except DecodeError`/`except
TransientError` clauses, or remaps it to 404 (the exact bug WR-05 fixed), would not be
caught — the boundary side of WR-05 is unguarded.
**Fix:** Add a face-level test per inbound adapter with a mock client whose `get_message`
raises `TransientError`. Example (REST):
```python
def test_get_message_transient_returns_503():
    client = MagicMock()
    client.get_message.side_effect = TransientError("orders", 0, 5, "poll timed out")
    resp = TestClient(create_app(client)).post(
        "/tools/get_message", json={"topic": "orders", "partition": 0, "offset": 5})
    assert resp.status_code == 503
    assert resp.json()["detail"]["error"] == "TransientError"
```

### WR-04: `protoc` subprocess has no timeout — a hostile or pathological schema can wedge the worker thread

**File:** `src/kafka_mcp/adapters/outbound/schema_registry_http.py:289-304`
**Issue:** The schema source (`schema.schema_str`, fetched by a `schema_id` taken from
untrusted message wire bytes) is compiled via `subprocess.run([...], check=False)` with **no
`timeout=`**. Command injection is *not* possible — the argv is fixed and `schema_str` only
reaches `protoc` as file content, the temp dir is `tempfile.TemporaryDirectory()` (0700, no
race), and `--proto_path` is pinned to that dir. But a malicious/garbage schema that makes
`protoc` hang, or a very large schema, blocks the calling thread indefinitely — a
liveness/DoS exposure on the strict `get_message` path. (`--include_imports` with a
single-dir `--proto_path` means stray `import` lines just fail compilation → DecodeError,
which is acceptable.)
**Fix:** Bound the subprocess and map a timeout to DecodeError:
```python
try:
    completed = subprocess.run([...], capture_output=True, text=True,
                               check=False, timeout=10)
except subprocess.TimeoutExpired as exc:
    raise RuntimeError("protoc timed out compiling schema") from exc
```

### WR-05: `MessageToDict` default rendering diverges from the schema (camelCase + int64-as-string), making Protobuf `value:<path>` matching unreliable

**File:** `src/kafka_mcp/adapters/outbound/schema_registry_http.py:248-250`
**Issue:** `MessageToDict(message)` with defaults follows proto3 JSON mapping:
`int64`/`uint64`/`fixed64`/`bytes` fields render as **strings**, and field names are
**lowerCamelCase**, not the proto snake_case. The round-trip test only covers `int32`/`string`
(`test_decode_magic_byte_protobuf_roundtrip` asserts `age=42` as int — works only because
`age` is `int32`). For a realistic `order_id`/`customer_id` declared `int64`, the value comes
back as a string and the field name comes back camelCased. `_matches_key` compares
`str(current) == key`, so top-level int64-as-string still matches a string key, and
`_extract_evidence_keys` carries both casings for top-level aliases — but a **nested** lookup
like `value:payload.order_id` will only see `payload.orderId` and silently miss. This is a
correctness/consistency gap between the Avro path (snake_case, native ints) and the Protobuf
path. WARNING because top-level Evidence still resolves; nested dotted-path matching on
Protobuf does not.
**Fix:** Render with field names preserved so both decode paths agree with the registered
schema:
```python
return MessageToDict(message, preserving_proto_field_name=True)
```
Optionally document that proto3 int64 fields are JSON-mapped to strings, and add a Protobuf
test with an `int64` + nested-message field to lock the behavior in.

## Info

### IN-01: `_resolve_message_by_index` relies on dict-view insertion order for an index-based protocol

**File:** `src/kafka_mcp/adapters/outbound/schema_registry_http.py:419-447`
**Issue:** The top level uses `file_descriptor.message_types_by_name.values()` (a dict view);
the Confluent message-index array is an *index* into the file's declared `message_type`
order. Dict-view ordering matches declaration order in CPython 3.7+, so this is correct in
practice, but relying on `message_types_by_name` ordering for an index protocol is fragile.
The `0x00` shorthand (single top-level message, the common case) is unaffected.
**Fix:** Document the ordering assumption, or build the top-level list explicitly from the
descriptor's declared message order.

### IN-02: `KafkaClient(MockConsumer())` with no registry silently decodes to `value=None` on the strict path

**File:** `src/kafka_mcp/adapters/inbound/lib.py:44-66`
**Issue:** The `_NullSchemaRegistry` stub returns `None` from `decode` unconditionally
(documented, accepted T-02-04-E risk). A client constructed without a registry produces
`value=None` on `get_message`, which the strict path treats as "decoded to null" rather than
an error. Intended Phase-1-compat behavior; flagged for awareness only.
**Fix:** None required; behavior is documented and intentional.

### IN-03: `parse_args` docstring is stale (lists only list-topics/describe-topic)

**File:** `src/kafka_mcp/adapters/inbound/cli.py:165-180`
**Issue:** The docstring's "always carries" list enumerates only `list-topics`/
`describe-topic` and their fields, but the parser now also supports `search-messages` and
`get-message` with their own namespaces. Documentation drift, no functional impact.
**Fix:** Update the docstring to enumerate all four subcommands and their namespace fields.

---

_Reviewed: 2026-06-06_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
