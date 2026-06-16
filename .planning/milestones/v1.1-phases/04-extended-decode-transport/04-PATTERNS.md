# Phase 4: Extended Decode & Transport - Pattern Map

**Mapped:** 2026-06-08
**Files analyzed:** 10
**Analogs found:** 10 / 10

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `src/kafka_mcp/domain/models.py` | model | data-structure | `src/kafka_mcp/domain/models.py` | self (additive field pattern) |
| `src/kafka_mcp/domain/search_service.py` | service | request-response | `src/kafka_mcp/domain/search_service.py` | self (evidence extraction pattern) |
| `src/kafka_mcp/adapters/outbound/confluent_consumer.py` | adapter | CRUD | `src/kafka_mcp/adapters/outbound/confluent_consumer.py` | self (key building pattern) |
| `src/kafka_mcp/adapters/outbound/schema_registry_http.py` | adapter | CRUD | `src/kafka_mcp/adapters/outbound/schema_registry_http.py` | self (decode reuse pattern) |
| `src/kafka_mcp/adapters/inbound/rest_api.py` | controller | request-response | `src/kafka_mcp/adapters/inbound/rest_api.py` | self (serialization pattern) |
| `src/kafka_mcp/adapters/inbound/mcp_stdio.py` | controller | request-response | `src/kafka_mcp/adapters/inbound/mcp_stdio.py` | self (serialization pattern) |
| `src/kafka_mcp/adapters/inbound/cli.py` | controller | request-response | `src/kafka_mcp/adapters/inbound/cli.py` | self (serialization pattern) |
| `src/kafka_mcp/adapters/inbound/lib.py` | service | request-response | `src/kafka_mcp/adapters/inbound/lib.py` | self (no changes needed — signature stable) |
| `src/kafka_mcp/server.py` | config | request-response | `src/kafka_mcp/server.py` | self (dispatch pattern — HTTP mount only) |
| `server.json` | config | data-structure | `server.json` | self (transport declaration) |
| `glama.json` | config | data-structure | `glama.json` | self (server config doc) |

---

## Pattern Assignments

### `src/kafka_mcp/domain/models.py` (model, data-structure)

**Analog:** Self — existing `KafkaMessage` model

**Pattern: Additive field (with default)** 

Existing Evidence fields (lines 70–79) show the pattern for adding optional fields:
```python
# --- Investigator Contract Evidence fields ---
source: str = "kafka"
event_type: str = "kafka_message"
keys: dict[str, str | None] = Field(
    default_factory=_default_evidence_keys,
    description=(
        "Extracted investigator identifiers.  "
        "Absent identifiers are None."
    ),
)
```

**Apply to new fields (lines 64–68, before Evidence block):**

Insert three new additive fields after `raw: bytes` (line 68):
```python
raw_key: bytes | None = None
key_decoded: dict[str, Any] | None = None
schema_id: dict[str, int | None] | None = None
```

**Rationale:** Same Field()-less pattern as `key`, `headers`, `value`. No default_factory needed; direct `None` default. These are optional wire/decode artifacts, not Evidence contract fields (do not add to evidence section).

---

### `src/kafka_mcp/domain/search_service.py` (service, request-response)

**Analog:** Self — existing evidence extraction pattern

**Pattern: Evidence extraction hook in message building** (lines 328–333, 377–379)

Existing pattern for attaching computed fields to messages:
```python
evidence_keys = _extract_evidence_keys(decoded, raw_msg.headers)
msg = raw_msg.model_copy(
    update={"value": decoded, "keys": evidence_keys}
)
```

**Apply to new fields:**

After evidence extraction, add key_decoded + schema_id extraction before `model_copy()` update:
1. In `search_messages()` (around line 333, before `model_copy`):
   - Call `decode_key()` helper (to be written in this file) for `raw_msg.raw_key`
   - Extract `schema_id` dict using the framing math from CONTEXT.md
   - Pass both to `model_copy(update={...})`

2. In `get_message()` (around line 379, same pattern):
   - Call `decode_key()` helper
   - Extract `schema_id` dict
   - Pass to `model_copy(update={...})`

**New helper function to add (lines ~114–150):**

```python
def _extract_schema_id(raw: bytes) -> int | None:
    """Extract schema_id from Confluent-framed bytes (if present).
    
    Returns int if raw has magic byte (0x00) at [0] and len >= 5,
    else None.
    """
    if raw and len(raw) >= 5 and raw[0] == 0x00:
        return int.from_bytes(raw[1:5], "big")
    return None


def _decode_key(
    raw_key: bytes | None,
    registry: SchemaRegistryPort,
    topic: str,
    partition: int,
    offset: int,
) -> dict | None:
    """Decode message key via SchemaRegistryPort (resilient).
    
    Attempts decode ONLY when raw_key carries Confluent framing
    (magic byte 0x00 at [0], len >= 5). Plain/string keys → None.
    DecodeError swallowed (resilient path, like search value-decode).
    
    Returns dict if decode succeeds, None otherwise.
    """
    if not raw_key or len(raw_key) < 5 or raw_key[0] != 0x00:
        return None
    
    try:
        return registry.decode(raw_key, topic, partition, offset)
    except DecodeError:
        return None
```

**Integration point (search_messages, line 333):**

```python
# After evidence extraction, before model_copy
key_decoded = _decode_key(
    raw_msg.raw_key, self._registry, topic, partition_id, raw_msg.offset
)
schema_id_value = _extract_schema_id(raw_msg.raw)
schema_id_key = _extract_schema_id(raw_msg.raw_key) if raw_msg.raw_key else None
schema_id = {
    "value": schema_id_value,
    "key": schema_id_key,
} if (schema_id_value is not None or schema_id_key is not None) else None

msg = raw_msg.model_copy(
    update={
        "value": decoded,
        "keys": evidence_keys,
        "key_decoded": key_decoded,
        "schema_id": schema_id,
    }
)
```

**Same pattern in `get_message()` (around line 379).**

---

### `src/kafka_mcp/adapters/outbound/confluent_consumer.py` (adapter, CRUD)

**Analog:** Self — existing key extraction pattern

**Pattern: Capture raw_key bytes (currently discarded)**

Lines 270–276 show the current key extraction that only keeps the UTF-8 string:

```python
# --- key (best-effort UTF-8, T-02-03-D) ---
raw_key = msg.key()
key_str: str | None = (
    raw_key.decode("utf-8", errors="replace")
    if raw_key is not None
    else None
)
```

**Apply to fetch_messages() (lines 289–299) and fetch_message() (lines 441–450):**

Change `KafkaMessage()` construction to include `raw_key`:

```python
result.append(
    KafkaMessage(
        topic=topic,
        partition=partition,
        offset=msg.offset(),
        key=key_str,
        raw_key=raw_key,  # ADD THIS LINE
        headers=headers_dict,
        value=None,
        timestamp_utc=ts_utc,
        raw=msg.value() or b"",
    )
)
```

**Rationale:** Only store the raw bytes; the service layer will handle UTF-8 fallback + key decode orchestration. No new logic in this adapter, just thread the bytes through.

---

### `src/kafka_mcp/adapters/outbound/schema_registry_http.py` (adapter, CRUD)

**Analog:** Self — existing `decode()` method (lines 113–152)

**Pattern: Reuse existing decode() for key bytes**

The current `decode()` method (lines 113–152) already handles:
- Magic byte detection (`raw[0] == 0x00`, `len >= 5`)
- Schema ID extraction (`int.from_bytes(raw[1:5], "big")`)
- SR lookup + Avro/Protobuf/JSON dispatch
- Typed `DecodeError` on failure

**Apply to key decoding (search_service.py context):**

The service layer's `_decode_key()` helper (defined in search_service.py) will call `registry.decode(raw_key, ...)` directly. No changes needed to `SchemaRegistryHttpAdapter` itself — the existing method works for both value and key bytes because the framing is identical.

**No changes to schema_registry_http.py.** The schema_id extraction (lines 168) is the canonical implementation:

```python
schema_id = int.from_bytes(raw[1:5], "big")
```

Service layer will replicate this math in the `_extract_schema_id()` helper for efficiency (avoid SR round-trip when only framing is needed).

---

### `src/kafka_mcp/adapters/inbound/rest_api.py` (controller, request-response)

**Analog:** Self — existing `_serialize_message()` function (lines 98–116)

**Pattern: Additive serialization (base64 + datetime + new fields)**

Existing serialization (lines 111–116):

```python
def _serialize_message(msg: KafkaMessage) -> dict:
    """Serialize a KafkaMessage to a JSON-safe dict."""
    data = msg.model_dump()
    data["raw"] = base64.b64encode(msg.raw).decode("ascii")
    if isinstance(data.get("timestamp_utc"), datetime):
        data["timestamp_utc"] = data["timestamp_utc"].isoformat()
    return data
```

**Apply to new fields:**

Add base64 encoding for `raw_key` (lines 112–113, after raw handling):

```python
def _serialize_message(msg: KafkaMessage) -> dict:
    """Serialize a KafkaMessage to a JSON-safe dict."""
    data = msg.model_dump()
    data["raw"] = base64.b64encode(msg.raw).decode("ascii")
    
    # ADD: base64-encode raw_key if present
    if msg.raw_key:
        data["raw_key"] = base64.b64encode(msg.raw_key).decode("ascii")
    
    if isinstance(data.get("timestamp_utc"), datetime):
        data["timestamp_utc"] = data["timestamp_utc"].isoformat()
    return data
```

**Rationale:** `model_dump()` already includes `key_decoded` and `schema_id` (both JSON-safe dicts/ints). Only `raw_key` needs encoding (same as `raw`). The datetime conversion already handles `timestamp_utc`.

---

### `src/kafka_mcp/adapters/inbound/mcp_stdio.py` (controller, request-response)

**Analog:** Self — existing `_serialize_message()` function (lines 43–61)

**Pattern: Identical to rest_api.py serialization**

The MCP stdio face has its own `_serialize_message()` (lines 43–61) that mirrors rest_api.py:

```python
def _serialize_message(msg: KafkaMessage) -> dict:
    """Serialize a KafkaMessage to a JSON-safe dict."""
    data = msg.model_dump()
    data["raw"] = base64.b64encode(msg.raw).decode("ascii")
    if isinstance(data.get("timestamp_utc"), datetime):
        data["timestamp_utc"] = data["timestamp_utc"].isoformat()
    return data
```

**Apply the same raw_key encoding:**

```python
def _serialize_message(msg: KafkaMessage) -> dict:
    """Serialize a KafkaMessage to a JSON-safe dict."""
    data = msg.model_dump()
    data["raw"] = base64.b64encode(msg.raw).decode("ascii")
    
    # ADD: base64-encode raw_key if present
    if msg.raw_key:
        data["raw_key"] = base64.b64encode(msg.raw_key).decode("ascii")
    
    if isinstance(data.get("timestamp_utc"), datetime):
        data["timestamp_utc"] = data["timestamp_utc"].isoformat()
    return data
```

**Rationale:** Maintains 4-face symmetry (lib, REST, MCP, CLI all use identical serialization).

---

### `src/kafka_mcp/adapters/inbound/cli.py` (controller, request-response)

**Analog:** Self — existing `_serialize_message()` helper pattern (search for usage)

**Search for CLI serialization pattern:**

CLI (lines 50–300+) does NOT have a dedicated `_serialize_message()` helper; instead, it uses a combination of `orjson_dumps()` and manual base64 encoding in the display path. However, CLI also needs to surface the new fields for `--json` output.

**Find the JSON serialization point in CLI (around lines 200–250):**

The CLI's `_handle_search_messages()` and `_handle_get_message()` likely call `orjson_dumps()` with message dicts. The easiest pattern is to add a `_serialize_message()` helper to cli.py matching the rest_api.py pattern, then use it before `orjson_dumps()`.

**Add to cli.py (before the command handlers, after imports):**

```python
def _serialize_message(msg: KafkaMessage) -> dict:
    """Serialize a KafkaMessage to a JSON-safe dict (mirror rest_api.py).
    
    Used by CLI --json output and table formatting.
    """
    data = msg.model_dump()
    data["raw"] = base64.b64encode(msg.raw).decode("ascii")
    
    # ADD: base64-encode raw_key if present
    if msg.raw_key:
        data["raw_key"] = base64.b64encode(msg.raw_key).decode("ascii")
    
    if isinstance(data.get("timestamp_utc"), datetime):
        data["timestamp_utc"] = data["timestamp_utc"].isoformat()
    return data
```

Then update CLI handlers to call `_serialize_message()` before orjson output.

---

### `src/kafka_mcp/adapters/inbound/lib.py` (service, request-response)

**Analog:** Self — existing `KafkaClient` wrapper (lines 69–110)

**Pattern: No changes needed**

`KafkaClient` (lib.py) is a thin wrapper around `TopicService` that delegates all operations. The `TopicService.search_messages()` and `TopicService.get_message()` already populate the new fields (via search_service.py changes above). `KafkaClient` methods return the same `list[KafkaMessage]` or `KafkaMessage` — the new fields flow through automatically via pydantic's `model_dump()` and model copying.

**No code changes to lib.py.**

---

### `src/kafka_mcp/server.py` (config, request-response)

**Analog:** Self — existing dispatch pattern (lines 38–96)

**Pattern: Add FastMCP streamable-http /mcp mount**

Current structure (lines 74–96): FastAPI app is created via `create_app(client)`, then passed to `uvicorn.run()`.

**Apply HTTP MCP mount:**

Modify `create_app()` in rest_api.py to register a FastMCP streamable-http app at `/mcp` (see rest_api.py section below). Then, in server.py, no dispatch changes needed — the mount is internal to the FastAPI app.

**Alternative approach (if mount is added to server.py):**

If FastMCP mount is done in server.py (lines 80–83), add:
```python
# After app creation, before uvicorn.run()
from mcp.server.fastmcp import FastMCP

# Create FastMCP server for /mcp endpoint
mcp_app = FastMCP("kafka-mcp-http")

# Register the same tools on mcp_app as on the stdio server
# (use helper function from mcp_stdio.py or create a shared module)

# Mount FastMCP at /mcp path on FastAPI app
# (use mcp_app.web_mount() or similar FastMCP API)
```

**Recommended:** Implement the mount in `rest_api.py::create_app()` (the factory), not in server.py (the dispatcher). This keeps concerns separate.

---

### Rest API HTTP MCP Mount (rest_api.py extension)

**Analog:** Self — existing FastAPI app structure (lines 140–150)

**Pattern: Mount FastMCP streamable-http at /mcp**

In `rest_api.py::create_app()`, after the lifespan context manager is defined (line 140–150), add FastMCP mount:

```python
@asynccontextmanager
async def _lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Release the librdkafka Consumer on server shutdown (WR-02)."""
    try:
        yield
    finally:
        client.close()

# CREATE FASTMCP APP FOR STREAMABLE-HTTP
from mcp.server.fastmcp import FastMCP

mcp_app = FastMCP("kafka-mcp-http")

# Register the four Kafka tools on the MCP app (reuse logic from mcp_stdio.py)
@mcp_app.tool(name="list_topics", ...)
def list_topics_tool(...): ...

# ... (register all four tools with same signatures as mcp_stdio.py)

# Mount on FastAPI app
app = FastAPI(lifespan=_lifespan)
app.mount("/mcp", mcp_app.web_mount())  # FastMCP web mount API

# Register existing REST routes (POST /tools/*)
# ... (existing routes continue below)
```

**Rationale:** Declares a real HTTP endpoint at `/mcp` that serves MCP. The declared endpoint in server.json must match this path.

---

### `server.json` (config, data-structure)

**Analog:** Self — existing `server.json` structure

**Pattern: Add HTTP transport entry alongside stdio**

Current structure (lines 10–66): Single `packages` array with a stdio transport.

**Add HTTP transport entry in `remotes`** (per MCP schema; see CONTEXT.md):

```json
{
  "$schema": "https://static.modelcontextprotocol.io/schemas/2025-12-11/server.schema.json",
  "name": "io.github.mshegolev/kafka-mcp",
  "description": "...",
  "repository": { "url": "...", "source": "github" },
  "version": "0.1.0",
  
  "packages": [
    {
      "registryType": "pypi",
      "identifier": "kafka-mcp",
      "version": "0.1.0",
      "transport": { "type": "stdio" },
      "environmentVariables": [ ... ]
    }
  ],
  
  "remotes": [
    {
      "name": "kafka-mcp-http",
      "type": "streamable-http",
      "url": "http://localhost:8000/mcp",
      "environmentVariables": [
        { "name": "KAFKA_BOOTSTRAP_SERVERS", "isRequired": true, ... },
        { "name": "KAFKA_MCP_HOST", "isRequired": false, "default": "127.0.0.1" },
        { "name": "KAFKA_MCP_PORT", "isRequired": false, "default": "8000" },
        ...
      ]
    }
  ]
}
```

**Rationale:** Truthful declaration of the actual HTTP endpoint running on `/mcp`. Clients can configure the HTTP transport with the declared URL.

---

### `glama.json` (config, data-structure)

**Analog:** Self — existing `glama.json` structure

**Pattern: Mirror HTTP option in serverConfig**

Current structure (lines 12–15): Single stdio serverConfig.

**Add HTTP serverConfig option:**

```json
{
  "$schema": "https://glama.ai/mcp/schemas/server.json",
  "name": "kafka-mcp",
  "displayName": "Kafka MCP",
  "description": "...",
  "vendor": "...",
  "sourceUrl": "...",
  "homepage": "...",
  "license": "MIT",
  "category": "developer-tools",
  "tags": ["kafka", "mcp", "messaging", "investigator", "read-only", "observability"],
  
  "serverConfig": {
    "command": "kafka-mcp",
    "args": ["--stdio"],
    "transport": "stdio"
  },
  
  "serverConfigOptions": [
    {
      "name": "HTTP",
      "command": "kafka-mcp",
      "args": [],
      "transport": "http",
      "url": "http://localhost:8000/mcp"
    }
  ],
  
  "tools": [ ... ]
}
```

**Rationale:** Documents both stdio (default) and HTTP run modes in the Glama registry.

---

## Shared Patterns

### 4-Face Serialization Symmetry
**Source:** `rest_api.py::_serialize_message()` (lines 98–116), `mcp_stdio.py::_serialize_message()` (lines 43–61)
**Apply to:** All four inbound faces (lib via model_dump, REST, MCP, CLI)

**Pattern:**
```python
def _serialize_message(msg: KafkaMessage) -> dict:
    data = msg.model_dump()
    data["raw"] = base64.b64encode(msg.raw).decode("ascii")
    if msg.raw_key:
        data["raw_key"] = base64.b64encode(msg.raw_key).decode("ascii")
    if isinstance(data.get("timestamp_utc"), datetime):
        data["timestamp_utc"] = data["timestamp_utc"].isoformat()
    return data
```

**Usage:** All inbound adapters call this (or equivalent) before returning/serializing a message. Ensures `key_decoded` and `schema_id` fields (JSON-safe) are automatically included in model_dump().

---

### Resilient Decode Pattern (Key-Specific)
**Source:** `search_service.py::_decode_key()` (new helper, inspired by lines 316–326 value decode)

**Pattern:**
```python
def _decode_key(...) -> dict | None:
    if not raw_key or len(raw_key) < 5 or raw_key[0] != 0x00:
        return None  # Plain key, not framed
    try:
        return registry.decode(raw_key, ...)
    except DecodeError:
        return None  # Swallow error, never crash
```

**Usage:** In `search_messages()` and `get_message()`, always attempt key decode; never raise. Matches the existing resilient value-decode pattern (line 317–326).

---

### Schema ID Extraction Pattern
**Source:** `schema_registry_http.py` line 168; copied to service layer

**Pattern:**
```python
def _extract_schema_id(raw: bytes) -> int | None:
    if raw and len(raw) >= 5 and raw[0] == 0x00:
        return int.from_bytes(raw[1:5], "big")
    return None
```

**Usage:** Called on both `raw` (message value) and `raw_key` (message key) to extract schema IDs. Result is a dict `{"value": <id>, "key": <id>}` or None (no framing detected on either).

---

## No Analog Found

**All files have close analogs** — this is a pure additive phase, modifying existing components to thread new fields through without breaking contracts. No new adapters, no new service types, no new transport mechanisms (only HTTP wiring of existing MCP server).

---

## Metadata

**Analog search scope:** 
- `src/kafka_mcp/domain/` (models, services)
- `src/kafka_mcp/adapters/outbound/` (consumer, schema registry)
- `src/kafka_mcp/adapters/inbound/` (REST, MCP, CLI, lib)
- `src/kafka_mcp/` (config, server dispatch)
- Root (server.json, glama.json)

**Files scanned:** 11 source files + 2 config files

**Pattern extraction date:** 2026-06-08

---

## Key Insights

1. **Models (domain/models.py):** Additive field pattern with `= None` default, no Field wrapper. Add `raw_key`, `key_decoded`, `schema_id` before Evidence block.

2. **Service layer (search_service.py):** Introduces two new helpers (`_extract_schema_id()`, `_decode_key()`) that mirror existing evidence extraction. Resilient decode (swallow DecodeError) matches value-decode pattern.

3. **Consumer adapter (confluent_consumer.py):** Simply capture `raw_key` bytes in both `fetch_messages()` and `fetch_message()` — no new logic, just thread through to KafkaMessage constructor.

4. **Schema registry (schema_registry_http.py):** No changes. Existing `decode()` method handles key bytes identically to value bytes (framing is identical).

5. **All four inbound faces:** Serialization symmetry. Each face has (or needs) a `_serialize_message()` that handles both `raw` and `raw_key` base64 encoding. `key_decoded` and `schema_id` flow through `model_dump()` automatically.

6. **FastAPI HTTP mount:** Add FastMCP app registered at `/mcp` inside `create_app()` factory in rest_api.py. Declare truthfully in server.json `remotes` array + glama.json `serverConfigOptions`.

7. **Read-only guarantee:** Phase 4 is additive-only — no consumer writes, no offset commits. The assign-based consumer + DecodeError swallowing ensure safety.
