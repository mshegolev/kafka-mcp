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
  warning: 2
  info: 4
  total: 6
status: issues_found
---

# Phase 2: Code Review Report

**Reviewed:** 2026-06-06T00:00:00Z
**Depth:** standard
**Files Reviewed:** 17
**Status:** issues_found

## Summary

Re-review of iteration 3 (final) for Phase 2 (Search + Decode). Evidence
gathered:

- `ruff check .` → **All checks passed!**
- `python3 -m pytest -q` → **185 passed in 0.99s**

**Iteration-2 fixes verified sound:**

- **WR-01 (grpc_tools.protoc):** Confirmed. The adapter compiles via the
  in-process `grpc_tools.protoc.main` (vendored wheel), not a system binary.
  No shell, no `subprocess`, no injection: `schema_str` only reaches the
  compiler as the *content* of a file inside a `tempfile.TemporaryDirectory()`
  (0700), and `argv` is fixed (`--proto_path` pinned to the temp dir). Protobuf
  decode works end-to-end for self-contained schemas (4 protobuf tests pass).
- **WR-04 (bounded compile):** Confirmed. Compile runs on a `daemon` worker
  thread joined with a 10s budget; timeout → `RuntimeError` → typed
  `DecodeError`. Daemon flag prevents interpreter-shutdown blocking. See the
  abandon-not-kill caveat in IN-01.
- **WR-05 (`preserving_proto_field_name=True`):** Confirmed and tested
  (`test_decode_protobuf_preserves_snake_case_and_nested_paths`). snake_case
  field names survive, nested `value:payload.order_id` resolves on the Protobuf
  path identically to Avro.
- **WR-02 (`TransientError` in `__all__`):** Confirmed (`__init__.py:34`).
- **WR-03 (REST/CLI/MCP `TransientError` faces):** Confirmed. REST → 503, CLI →
  exit 3, MCP → ValueError. Each has a face test in `test_inbound.py`.

**Core invariants confirmed:**

- **Hexagonal boundary:** Holds. No `confluent_kafka` / `fastavro` /
  `google.protobuf` imports in `domain/` or `ports/`; all decode libraries live
  only in `schema_registry_http.py`. Enforced by the boundary test.
- **Read-only:** Holds. `enable.auto.commit=False`, throwaway uuid4 `group.id`,
  `assign()`-only (no `subscribe` anywhere in the consumer source — guarded by
  `test_no_subscribe_in_source`).
- **Credentials unlogged:** Holds. `sasl_password` is extracted via
  `get_secret_value()` into a local conf dict and never stored as an attribute;
  `sr_pass` is passed into the SR client conf and not retained
  (`test_sr_credentials_not_logged`, `test_secret_str_not_exposed_in_repr`).

**No Critical defects found.** Two Warnings remain: a real functional gap in
the Protobuf decode path (well-known-type imports do not compile — WR-A), and a
stale CLI dispatch table in the wired entry point that makes the new Phase 2
`search-messages` / `get-message` subcommands unreachable via `kafka-mcp`
(WR-B). Neither is a security or data-loss issue; both degrade the
deliverable's real-world usability and should be fixed.

## Narrative Findings (AI reviewer)

## Warnings

### WR-A: Protobuf schemas importing well-known types fail to decode

**File:** `src/kafka_mcp/adapters/outbound/schema_registry_http.py:340-348`
**Issue:** `_run_protoc_bounded` invokes `grpc_tools.protoc.main` with an
`argv` whose only include path is the temp dir:

```python
argv = [
    "protoc",
    f"--proto_path={tmpdir}",
    f"--descriptor_set_out={out_path}",
    "--include_imports",
    proto_path,
]
```

`grpc_tools.protoc.main` does **not** auto-inject the bundled well-known-type
include path (that is done by grpc_tools' build helpers, not by `main`). So any
registered `.proto` that does `import "google/protobuf/timestamp.proto";`
(or wrappers / struct / any / duration — all extremely common in Confluent
Protobuf payloads, e.g. event timestamps) fails compilation and is surfaced as
a `DecodeError`. Reproduced directly against the adapter's exact argv:

```
adapter argv rc (nonzero=fails to decode): 1
with well-known include rc (0=works):     0
```

The grpc_tools wheel already vendors these protos at
`grpc_tools/_proto/google/protobuf/*.proto`, so the fix is to add that
directory as a second `--proto_path`. The existing tests only exercise
self-contained schemas (`Person`, `Order`), so this gap is invisible to the
suite — real-world schemas with a timestamp field will not decode in
`get_message` (hard `DecodeError` to the caller) and silently decode to
`value=None` in `search_messages` (degrading `value:<path>` matching and
Evidence extraction).

**Fix:**
```python
import grpc_tools
_WELL_KNOWN_INCLUDE = os.path.join(
    os.path.dirname(grpc_tools.__file__), "_proto"
)

argv = [
    "protoc",
    f"--proto_path={tmpdir}",
    f"--proto_path={_WELL_KNOWN_INCLUDE}",  # vendored google/protobuf/*.proto
    f"--descriptor_set_out={out_path}",
    "--include_imports",
    proto_path,
]
```
Add a regression test: a `.proto` importing `google/protobuf/timestamp.proto`
must decode to a dict (not raise `DecodeError`).

### WR-B: Wired `kafka-mcp` entry point cannot reach the new Phase 2 CLI subcommands

**File:** `src/kafka_mcp/server.py:62` (entry point declared in
`pyproject.toml:37` `kafka-mcp = "kafka_mcp.server:main"`); affects the
in-scope `src/kafka_mcp/adapters/inbound/cli.py` Phase 2 additions.
**Issue:** `cli.py` adds `search-messages` and `get-message` subcommands (the
Phase 2 search/decode CLI face), but the dispatcher in `server.py` — the actual
console-script entry point — still gates CLI mode on a stale set:

```python
_cli_subcommands = {"list-topics", "describe-topic"}
if args and args[0] in _cli_subcommands:
    from kafka_mcp.adapters.inbound.cli import main as cli_main
    cli_main(args)
    return
```

Running `kafka-mcp search-messages --key X` or `kafka-mcp get-message t 0 5`
does **not** match this set, falls through, and instead boots the
FastAPI/uvicorn HTTP server (the default branch). The new CLI face is therefore
unreachable through the documented `kafka-mcp ...` invocation even though
`cli.py` fully implements it and is tested in isolation
(`test_inbound.py`). The unit tests call `cli.run_*` / `cli.parse_args`
directly and never go through `server.main`, so they do not catch this.

**Fix:**
```python
_cli_subcommands = {
    "list-topics", "describe-topic", "search-messages", "get-message",
}
```
(Also update the now-stale docstring at `server.py:6,28-29` to list all four
subcommands.) Add a test that drives `server.main(["search-messages", ...])`
with a patched `KafkaClient.from_env` to lock the dispatch table to the CLI's
actual subparser set.

## Info

### IN-01: Bounded-protoc worker thread is abandoned, not cancelled, on timeout

**File:** `src/kafka_mcp/adapters/outbound/schema_registry_http.py:357-364`
**Issue:** On timeout the code raises `RuntimeError` but the daemon worker
running `grpc_tools.protoc.main` (a native call) keeps running until it
finishes — `join(timeout)` does not cancel it. For a pathological schema this
leaves a native compile churning in the background while the request already
returned a `DecodeError`. The `daemon=True` flag correctly prevents it from
blocking interpreter shutdown, so blast radius is bounded, but under repeated
hostile input background work could accumulate.
**Fix:** Document the abandon-not-kill semantics inline, or cache negative
results so a schema_str that previously timed out is not recompiled.

### IN-02: Sync REST handlers + single shared librdkafka Consumer = threadpool aliasing

**File:** `src/kafka_mcp/adapters/inbound/rest_api.py:158-253`
**Issue:** The route handlers are plain `def` (not `async def`), so FastAPI
dispatches them on its threadpool. Concurrent requests therefore share one
`ConfluentConsumerAdapter` and call `assign()` + `poll()` on a single
librdkafka `Consumer` from multiple threads, and mutate the adapter's
`_schema_cache` / `_proto_descriptor_cache` dicts concurrently. The cache
mutations are idempotent puts (benign), but concurrent `assign()`/seek on one
Consumer can interleave partition assignments across requests. Pre-existing
single-consumer architectural choice (Phase 1); concurrency is out of v1 scope.
Noted because Phase 2 search/get now drive real `poll()` traffic through it.
**Fix:** Out of scope for v1. Longer term: per-request consumer, a pool, or an
explicit lock around assign/poll.

### IN-03: proto3 int64/uint64/fixed64 decode to strings, mildly surprising for Evidence

**File:** `src/kafka_mcp/adapters/outbound/schema_registry_http.py:262`;
`src/kafka_mcp/domain/search_service.py:90-109`
**Issue:** `MessageToDict` renders proto3 64-bit integer fields as JSON strings
by spec. `_extract_evidence_keys` coerces with `str(raw_val)` and `_matches_key`
compares via `str(current)`, so matching still works — but a consumer reading
`value` directly will see `"order_id": "9000000001"` (string) for a Protobuf
int64 while the Avro path yields an int. Correctly called out in the WR-05
comment and consistent for matching; flagged only so downstream consumers
expecting numeric types are aware. No action required.

### IN-04: `parse_args` docstring is stale (lists only two subcommands)

**File:** `src/kafka_mcp/adapters/inbound/cli.py:165-180`
**Issue:** The `parse_args` docstring still says the Namespace carries
`subcommand: "list-topics" | "describe-topic"` and omits `search-messages` /
`get-message` and their fields. Documentation drift only; behaviour is correct.
**Fix:** Update the docstring to enumerate all four subcommands and their
fields.

---

_Reviewed: 2026-06-06T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
