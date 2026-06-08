---
phase: 04-extended-decode-transport
reviewed: 2026-06-08T15:30:00Z
depth: standard
files_reviewed: 8
files_reviewed_list:
  - src/kafka_mcp/domain/models.py
  - src/kafka_mcp/domain/search_service.py
  - src/kafka_mcp/adapters/outbound/confluent_consumer.py
  - src/kafka_mcp/adapters/inbound/rest_api.py
  - src/kafka_mcp/adapters/inbound/mcp_stdio.py
  - src/kafka_mcp/adapters/inbound/cli.py
  - server.json
  - glama.json
findings:
  critical: 1
  warning: 3
  info: 2
  total: 6
status: issues_found
---

# Phase 04: Code Review Report

**Reviewed:** 2026-06-08T15:30:00Z
**Depth:** standard
**Files Reviewed:** 8
**Status:** issues_found

## Summary

Phase 4 delivers three additive capabilities cleanly: `raw_key` byte threading through the
outbound consumer, key-decode + `schema_id` surfacing in the domain service, and a FastMCP
streamable-HTTP mount at `/mcp`. The hexagonal boundary is intact — no I/O or framework
imports in `domain/` or `ports/`. The length guard (`len >= 5` before any index access) is
present and correctly ordered in both `_extract_schema_id` and `_decode_key`. The
`is not None` (not truthiness) check for `raw_key` serialization is correctly applied in all
three inbound faces. The `model_copy(update={...})` pattern for key/schema_id surfacing is
correct and non-destructive to evidence fields. Session manager lifecycle integration in
`create_app()` is correct and confirmed working by the test suite.

One blocker stands out: the HTTP MCP `search_messages` tool parses `time_from`/`time_to`
strings via bare `datetime.fromisoformat()` without a timezone fallback. A caller who passes
a timezone-naive ISO-8601 string (no `Z` or offset) will trigger a `TypeError` at comparison
time inside the consumer adapter (`ts_utc >= time_to`, where `ts_utc` is always UTC-aware
and `time_to` is naive). The same bug exists in the MCP stdio face (pre-existing but not
fixed by Phase 4, and the Phase 4 scope explicitly added a second HTTP MCP path with the
same flaw). The CLI face correctly guards against this with an explicit `replace(tzinfo=utc)`
fallback.

---

## Critical Issues

### CR-01: Naive datetime comparison crash in HTTP MCP `search_messages` tool

**File:** `src/kafka_mcp/adapters/inbound/rest_api.py:195-196`

**Issue:** The `search_messages` function inside `_create_http_mcp_server` parses
`time_from` / `time_to` strings with bare `datetime.fromisoformat()`:

```python
tf = _dt.fromisoformat(time_from) if time_from is not None else None
tt = _dt.fromisoformat(time_to) if time_to is not None else None
```

If a caller passes a timezone-naive string (e.g. `"2026-01-01T00:00:00"` without a
trailing `Z` or UTC offset), `fromisoformat` returns a naive `datetime` (`.tzinfo is None`).
That naive datetime is forwarded through `KafkaClient.search_messages` →
`TopicService.search_messages` → `ConfluentConsumerAdapter.fetch_messages`, where line 267
of `confluent_consumer.py` executes:

```python
if time_to is not None and ts_utc >= time_to:
```

`ts_utc` is always UTC-aware (`datetime.fromtimestamp(..., tz=timezone.utc)`). Comparing an
aware datetime with a naive one raises `TypeError: can't compare offset-naive and
offset-aware datetimes`. This crashes the call with an unhandled exception — visible to the
caller as a 500/MCP error with no actionable message.

The same bug exists in the MCP stdio face (`mcp_stdio.py:150-151`), which also was not
fixed by Phase 4. The CLI face (`cli.py:306-314`) correctly handles this by applying
`replace(tzinfo=timezone.utc)` when `tzinfo is None`.

**Fix** — apply the same guard used in `cli.py` to both MCP faces:

```python
# In rest_api.py _create_http_mcp_server search_messages tool (lines 195-196):
from datetime import timezone as _tz
tf = _dt.fromisoformat(time_from) if time_from is not None else None
if tf is not None and tf.tzinfo is None:
    tf = tf.replace(tzinfo=_tz.utc)
tt = _dt.fromisoformat(time_to) if time_to is not None else None
if tt is not None and tt.tzinfo is None:
    tt = tt.replace(tzinfo=_tz.utc)
```

Apply the identical fix to `mcp_stdio.py:150-151`.

---

## Warnings

### WR-01: `limit` has no upper-bound guard on MCP stdio and HTTP MCP paths

**File:** `src/kafka_mcp/adapters/inbound/rest_api.py:191` (HTTP MCP tool),
`src/kafka_mcp/adapters/inbound/mcp_stdio.py:131` (stdio tool)

**Issue:** The `search_messages` tool registered in both `_create_http_mcp_server` (HTTP
MCP) and `create_mcp_server` (MCP stdio) declares `limit: int = 500` with no upper-bound
constraint. The REST face's Pydantic request model enforces `Field(ge=1, le=10000)` which
rejects values outside that range before they reach the service. The MCP faces pass the
caller-supplied integer directly to `KafkaClient.search_messages`.

`TopicService.search_messages` guards `limit <= 0` (returns `[]` immediately), but has no
upper cap. A caller who passes `limit=10_000_000` via the MCP transport will trigger a full
unbounded scan across all partitions (bounded only by `max_scan` per partition). This is a
DoS vector that the REST face correctly blocks but the MCP faces do not.

**Fix** — clamp `limit` inside each MCP tool before forwarding:

```python
# After receiving limit parameter, before calling client.search_messages:
limit = max(1, min(limit, 10_000))
```

Or validate it with a descriptive error:

```python
if limit <= 0 or limit > 10_000:
    raise ValueError(f"limit must be between 1 and 10000, got {limit!r}")
```

---

### WR-02: `server.json` remotes entry missing SASL/SSL environment variable declarations

**File:** `server.json:67-101`

**Issue:** The `remotes` entry for the HTTP transport omits five environment variables that
are documented in the `packages` (stdio) entry: `KAFKA_SECURITY_PROTOCOL`,
`KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, and
`KAFKA_SSL_VERIFY`. A user who selects the HTTP transport via an MCP host that reads
`server.json` will not be prompted for SASL/SSL credentials, causing silent connection
failures against secured brokers.

**Fix** — copy the missing variable declarations from the `packages` entry into the
`remotes[0].environmentVariables` array:

```json
{
  "name": "KAFKA_SECURITY_PROTOCOL",
  "description": "Security protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL",
  "isRequired": false,
  "format": "string",
  "default": "PLAINTEXT"
},
{
  "name": "KAFKA_SASL_MECHANISM",
  "description": "SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512",
  "isRequired": false,
  "format": "string"
},
{
  "name": "KAFKA_SASL_USERNAME",
  "description": "SASL username for broker authentication",
  "isRequired": false,
  "format": "string"
},
{
  "name": "KAFKA_SASL_PASSWORD",
  "description": "SASL password for broker authentication",
  "isRequired": false,
  "isSecret": true,
  "format": "string"
},
{
  "name": "KAFKA_SSL_VERIFY",
  "description": "Verify SSL certificates (true/false)",
  "isRequired": false,
  "format": "string",
  "default": "true"
}
```

---

### WR-03: Redundant local re-imports of already-imported names inside HTTP MCP tool closures

**File:** `src/kafka_mcp/adapters/inbound/rest_api.py:170-173`, `216-231`

**Issue:** The `describe_topic` and `get_message` tools inside `_create_http_mcp_server`
re-import exception classes that are already imported at the top of the module:

```python
# Already at module level (lines 40-45):
from kafka_mcp.domain.errors import (
    DecodeError,
    MessageNotFoundError,
    TopicNotFoundError,
    TransientError,
)

# Redundant inside describe_topic tool (line 170):
from kafka_mcp.domain.errors import TopicNotFoundError as _TopicNotFoundError

# Redundant inside get_message tool (lines 216-218):
from kafka_mcp.domain.errors import DecodeError as _DecodeError
from kafka_mcp.domain.errors import MessageNotFoundError as _MNF
from kafka_mcp.domain.errors import TransientError as _TransientError
```

These local imports introduce aliases that shadow the module-level names without necessity.
Every invocation of the tool re-executes the import statement. While Python's import
machinery caches modules in `sys.modules` (no performance penalty), the code is misleading:
a reader infers that the module-level names are not available inside the closure, which is
false.

**Fix** — remove the local re-imports and use the module-level names directly:

```python
def describe_topic(topic: str) -> dict:
    try:
        return client.describe_topic(topic).model_dump()
    except TopicNotFoundError as exc:          # use module-level name
        raise ValueError(f"Topic not found: {exc.topic!r}") from exc

def get_message(topic: str, partition: int, offset: int) -> dict:
    try:
        return _serialize_message(client.get_message(topic, partition, offset))
    except MessageNotFoundError as exc:        # module-level
        ...
    except TransientError as exc:             # module-level
        ...
    except DecodeError as exc:                # module-level
        ...
```

---

## Info

### IN-01: `glama.json` HTTP option uses `"transport": "http"` rather than `"streamable-http"`

**File:** `glama.json:21`

**Issue:** The `serverConfigOptions` entry for the HTTP mode declares
`"transport": "http"`, while `server.json` uses `"type": "streamable-http"` (the MCP
schema-correct term). Glama's schema (`$schema: https://glama.ai/mcp/schemas/server.json`)
may accept only the string values valid for the `serverConfig.transport` field (likely
`"stdio"` and `"http"` for Glama's own vocabulary). If Glama's schema validates this field
against the MCP spec's transport enum instead, the entry could fail schema validation.
The inconsistency between the two declaration files may confuse integrators.

**Fix** — verify what values Glama's schema accepts for `serverConfigOptions[].transport`.
If Glama accepts `"streamable-http"`, prefer consistency with `server.json`. If Glama only
accepts `"http"`, document the intentional divergence with a comment.

---

### IN-02: `_extract_schema_id` guard uses `not raw` (truthiness) rather than `raw is None`

**File:** `src/kafka_mcp/domain/search_service.py:42`

**Issue:**

```python
if not raw or len(raw) < 5 or raw[0] != 0x00:
    return None
```

`not raw` is `True` for both `None` and `b""` (zero-length bytes). This means an
empty-bytes value `b""` is treated as "no framing" and returns `None` rather than raising
or being treated as "too short." This is technically correct (a zero-length payload cannot
have Confluent framing), but the 04-CONTEXT.md spec says the guard is
`len(raw) >= 5 and raw[0] == 0x00` — a length-first check. The truthiness check adds a
`b"" → None` side-effect that is not documented and that differs from the canonical guard
shape.

The practical impact is zero: the `len(raw) < 5` branch would catch `b""` even without the
`not raw` pre-check (len(b"") = 0 < 5). The code is safe but the guard is slightly over-eager
in its first clause.

**Fix (optional style cleanup)** — either document the `b""` case explicitly, or replace
`not raw` with `raw is None` to match the spec's intent and use the `len < 5` check as the
actual short-circuit:

```python
if raw is None or len(raw) < 5 or raw[0] != 0x00:
    return None
```

Apply the same change to `_decode_key` line 75 for consistency.

---

_Reviewed: 2026-06-08T15:30:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
