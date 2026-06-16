---
phase: 04-extended-decode-transport
verified: 2026-06-16T12:00:00Z
status: passed
score: 4/4
overrides_applied: 0
---

# Phase 4: Extended Decode & Transport — Verification Report

**Phase Goal:** Users of all four faces can decode schema-encoded message keys, see schema_id on every KafkaMessage, and discover the FastAPI face as an MCP HTTP server via server.json
**Verified:** 2026-06-16T12:00:00Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | `KafkaClient.get_message()` and `search_messages()` return a decoded key dict when the key is schema-encoded, and fall back to raw/string key without error | ✓ VERIFIED | `_decode_key()` in search_service.py (L47-80) checks Confluent framing (magic byte 0x00, len≥5) before calling `registry.decode()`; DecodeError swallowed (returns None). Wired into both `search_messages()` (L391-397) and `get_message()` (L462-468) via `model_copy(update={key_decoded: ...})`. `_extract_schema_id()` (L25-44) guards with `len(raw) < 5 or raw[0] != 0x00`. Spot-check: `_decode_key(b'\x00\x00\x00\x00\x07payload', error_registry, ...)` returns None (resilient). `_decode_key(None, ...)` returns None. `_decode_key(b'plain', ...)` returns None. 72 domain tests pass. |
| 2 | `KafkaMessage` carries `schema_id` field (value+key) appearing identically in lib, MCP stdio, FastAPI `/tools/*`, CLI output | ✓ VERIFIED | `schema_id: dict[str, int \| None] \| None = None` at models.py L71. Computed as `{"value": _val_id, "key": _key_id}` when either side is framed, else None — identical logic in both `search_messages()` (L399-406) and `get_message()` (L470-477). 4-face symmetry confirmed: `_serialize_message()` in rest_api.py (L118-119), mcp_stdio.py (L59-60), cli.py (L271-272) all base64-encode `raw_key` and pass `key_decoded`/`schema_id` through `model_dump()`. Lib face returns `KafkaMessage` directly — `model_dump()` includes all three fields. Behavioral spot-check: identical `raw_key`/`key_decoded`/`schema_id` values across all three serializer faces confirmed. |
| 3 | `server.json` declares streamable-HTTP transport entry in `remotes` array matching FastAPI `/mcp` route | ✓ VERIFIED | server.json L67-134: `"remotes": [{"name": "kafka-mcp-http", "type": "streamable-http", "url": "http://localhost:8000/mcp", ...}]`. URL `/mcp` matches actual mount point in rest_api.py L292: `app.mount("/mcp", mcp_asgi_app)`. Stdio `packages` entry (L10-65) unchanged. glama.json L17-26: `serverConfigOptions` with `"transport": "streamable-http"` and `"url": "http://localhost:8000/mcp"`. stdio `serverConfig` (L12-16) unchanged. Programmatic assertion passed: `json.loads('server.json')['remotes'][0]['type'] == 'streamable-http'` ✓. HTTP mount test: 8 tests pass including `TestHttpMcpMount` (GET/POST /mcp/ return non-404, existing /tools/* routes unaffected). |
| 4 | Unit test suite passes with no regressions | ✓ VERIFIED | `pytest tests/ -x -q` → **270 passed**, 1 warning, 0 failures in 1.65s. Breakdown: test_domain.py 72 passed, test_adapters.py 71 passed, test_inbound.py 77 passed (includes 8 HTTP mount tests). Ruff check on all Python source files: all checks passed. |

**Score:** 4/4 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/kafka_mcp/domain/models.py` | KafkaMessage with raw_key, key_decoded, schema_id fields | ✓ VERIFIED | Lines 69-71: `raw_key: bytes \| None = None`, `key_decoded: dict[str, Any] \| None = None`, `schema_id: dict[str, int \| None] \| None = None`. Defaults are None. Existing fields unchanged. 82 lines total, substantive. |
| `src/kafka_mcp/domain/search_service.py` | `_extract_schema_id()` and `_decode_key()` helpers; wired into search_messages/get_message | ✓ VERIFIED | `_extract_schema_id()` at L25-44 (pure byte math, length guard). `_decode_key()` at L47-80 (framing check + resilient SR decode). Wired into `search_messages()` L390-414 and `get_message()` L461-485. 486 lines total, substantive. |
| `src/kafka_mcp/adapters/outbound/confluent_consumer.py` | raw_key threaded through both fetch paths | ✓ VERIFIED | `raw_key=raw_key` at L295 (fetch_messages) and L447 (fetch_message). `raw_key = msg.key()` extracted at L271 and L424. |
| `src/kafka_mcp/adapters/inbound/rest_api.py` | `_serialize_message` with raw_key base64, /mcp mount | ✓ VERIFIED | L117-119: raw_key base64 encoding. L131-236: `_create_http_mcp_server()` with 4 read-only tools. L262-292: FastMCP mount at /mcp with session manager lifecycle. 391 lines, substantive. |
| `src/kafka_mcp/adapters/inbound/mcp_stdio.py` | `_serialize_message` with raw_key base64 | ✓ VERIFIED | L58-60: raw_key base64 encoding in `_serialize_message()`. Pattern identical to rest_api. |
| `src/kafka_mcp/adapters/inbound/cli.py` | `_serialize_message_for_cli` with raw_key base64 | ✓ VERIFIED | L270-272: raw_key base64 encoding in `_serialize_message_for_cli()`. Pattern identical to rest_api/mcp_stdio. |
| `src/kafka_mcp/adapters/inbound/lib.py` | Lib face passes through new fields automatically | ✓ VERIFIED | Returns `KafkaMessage` domain objects directly (L202, L227). No serialization needed — `model_dump()` includes raw_key/key_decoded/schema_id. |
| `server.json` | remotes entry for streamable-HTTP at /mcp | ✓ VERIFIED | L67-134: `remotes` array with `type: "streamable-http"`, `url: "http://localhost:8000/mcp"`. Valid JSON. Packages (stdio) unchanged. |
| `glama.json` | serverConfigOptions with HTTP entry | ✓ VERIFIED | L17-26: `serverConfigOptions` with `transport: "streamable-http"`, `url: "http://localhost:8000/mcp"`. `serverConfig` stdio default intact. Valid JSON. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| confluent_consumer.py | models.py | `raw_key=raw_key` in KafkaMessage constructor | ✓ WIRED | L295 (fetch_messages) and L447 (fetch_message) both pass `raw_key=raw_key` |
| search_service.py `_decode_key()` | schema_registry port | `registry.decode(raw_key, ...)` | ✓ WIRED | L78: `registry.decode(raw_key, topic, partition, offset)` inside try/except DecodeError |
| search_service.py | models.py | `model_copy(update={key_decoded, schema_id})` | ✓ WIRED | L408-414 (search_messages) and L479-485 (get_message) — both pass `key_decoded` and `schema_id` |
| rest_api.py _serialize_message | models.py | `msg.raw_key` base64 encoding | ✓ WIRED | L118-119: `base64.b64encode(msg.raw_key)` |
| mcp_stdio.py _serialize_message | models.py | `msg.raw_key` base64 encoding | ✓ WIRED | L59-60: identical pattern |
| cli.py _serialize_message_for_cli | models.py | `msg.raw_key` base64 encoding | ✓ WIRED | L271-272: identical pattern |
| server.json remotes[0].url | rest_api.py HTTP mount path | `/mcp` URL matches mount point | ✓ WIRED | server.json L71: `"url": "http://localhost:8000/mcp"` matches rest_api.py L292: `app.mount("/mcp", ...)` |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|--------------|--------|-------------------|--------|
| search_service.py `key_decoded` | `_decode_key()` return | `registry.decode(raw_key, ...)` via SchemaRegistryPort | Yes — calls SR decode on framed keys; None for unframed | ✓ FLOWING |
| search_service.py `schema_id` | `_extract_schema_id()` return | Pure byte math on `raw_msg.raw` and `raw_msg.raw_key` | Yes — extracts int from bytes[1:5] | ✓ FLOWING |
| rest_api.py `_serialize_message` `raw_key` | `msg.raw_key` | Bytes from `confluent_consumer.fetch_messages()` via `msg.key()` | Yes — real wire bytes | ✓ FLOWING |
| models.py `raw_key` | `raw_key=raw_key` | `confluent_consumer.py` L271/L424: `raw_key = msg.key()` from librdkafka | Yes — real broker bytes | ✓ FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| KafkaMessage fields present with defaults | `model_dump()` check | raw_key=None, key_decoded=None, schema_id=None, backward compat OK | ✓ PASS |
| _extract_schema_id framing logic | Python smoke test | 7 extracted from `b'\x00\x00\x00\x00\x07x'`; None for bad magic/short/None | ✓ PASS |
| _decode_key resilient path | Python spot-check | DecodeError swallowed (returns None); None for None/unframed/short keys | ✓ PASS |
| 4-face serialization symmetry | Python smoke test | rest/mcp/cli all produce identical raw_key b64, key_decoded, schema_id | ✓ PASS |
| server.json remotes entry | JSON parse + assertion | type=streamable-http, url ends /mcp, packages stdio intact | ✓ PASS |
| glama.json serverConfigOptions | JSON parse + assertion | HTTP option present, stdio default intact | ✓ PASS |
| HTTP mount reachability | pytest -k HttpMcp | 8 tests pass (GET/POST /mcp/ return non-404, existing routes unaffected) | ✓ PASS |
| Full test suite | pytest tests/ -x -q | 270 passed, 0 failures | ✓ PASS |
| Ruff lint | ruff check (Python files) | All checks passed | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-----------|-------------|--------|----------|
| KEY-01 | 04-01, 04-02 | search_messages and get_message decode message key via SR, falling back to raw/string key without crash | ✓ SATISFIED | `_decode_key()` with framing guard + DecodeError swallow; wired in both methods; 23+ domain tests |
| KEY-02 | 04-01, 04-02 | KafkaMessage surfaces schema_id for decoded value and key, identically across all four faces | ✓ SATISFIED | `schema_id: dict[str, int\|None]\|None` on model; computed in search/get; base64 raw_key + passthrough in 3 serializers + lib model_dump; 13+ inbound tests including 4-face symmetry |
| HTTP-01 | 04-03 | server.json declares streamable-HTTP transport alongside stdio; endpoint matches FastAPI route | ✓ SATISFIED | server.json `remotes` array with `streamable-http` type, `url: .../mcp`; FastMCP mounted at `/mcp` in rest_api.py; 8 HTTP mount tests pass |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| search_service.py | 249, 260 | `TODO: AdminClient — wire real leader in Phase 2` | ℹ️ Info | Pre-existing from Phase 1; not introduced in Phase 4. Leader=0 placeholder documented. No impact on Phase 4 goal. |

No Phase-4-introduced anti-patterns, stubs, or placeholders found.

### Human Verification Required

No human verification items identified. All success criteria are verifiable programmatically and have been verified.

### Gaps Summary

No gaps found. All four ROADMAP success criteria are met:

1. **Key decode** — `_decode_key()` handles Confluent-framed keys via SR, falls back to None for unframed/plain keys without error. Wired in both `search_messages()` and `get_message()`.
2. **schema_id field** — Present on `KafkaMessage`, computed from raw bytes, flows through all four faces identically (lib via `model_dump()`, REST/MCP/CLI via serializer with base64 raw_key encoding).
3. **server.json HTTP transport** — `remotes` array declares `streamable-http` entry with URL `http://localhost:8000/mcp` matching the actual FastMCP mount point. glama.json documents HTTP option. Stdio entries unchanged.
4. **Test suite green** — 270 tests pass with 0 regressions. Phase 4 added 52 new tests across domain, adapter, and inbound test files.

---

_Verified: 2026-06-16T12:00:00Z_
_Verifier: OpenCode (gsd-verifier)_
