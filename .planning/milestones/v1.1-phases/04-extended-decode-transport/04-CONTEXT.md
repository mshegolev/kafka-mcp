# Phase 4: Extended Decode & Transport - Context

**Gathered:** 2026-06-08
**Status:** Ready for planning

<domain>
## Phase Boundary

Extend the existing read-only decode + transport surface without breaking the
v1.0 contract. Three additive capabilities:
1. **KEY-01** — decode the message *key* via Schema Registry when it is
   schema-encoded; fall back to the raw/string key otherwise (no crash).
2. **KEY-02** — surface the Schema Registry `schema_id` (value, and key when
   key-decoded) on `KafkaMessage`, identically across all four faces.
3. **HTTP-01** — declare a streamable-HTTP transport in `server.json` that maps
   to a real FastAPI MCP endpoint.

In scope: `domain/models.py` (KafkaMessage fields), `domain/search_service.py`
(key-decode + schema_id extraction), outbound consumer adapter (stop discarding
raw key bytes), reuse of `SchemaRegistryHttpAdapter.decode()`, all four inbound
faces' serialization, `server.json` + `glama.json`, FastAPI HTTP mount.

Out of scope (deferred): searching BY decoded key (key_field stays as-is),
schema cache tuning (KEY-03, future), HTTP auth/TLS hardening (HTTP-02, future).
</domain>

<decisions>
## Implementation Decisions

### schema_id surfacing (KEY-02)
- Add a single field `schema_id: dict[str, int | None] | None` on `KafkaMessage`,
  shaped `{"value": <int|None>, "key": <int|None>}`. `None` (the whole field)
  when nothing was SR-decoded.
- Obtain the id by reading the Confluent wire framing directly:
  `int.from_bytes(raw[1:5], "big")` when `len(raw) >= 5 and raw[0] == 0x00`.
  No extra Schema Registry round-trip — the id is in the message bytes.
- For plain-JSON / unframed payloads the corresponding side is `None` (no `-1`
  sentinel).
- Surface identically in all four faces through the existing
  `_serialize_message` helper (and its MCP/CLI equivalents) — not lib-only.

### key decode + fallback (KEY-01)
- Do NOT break `key`. Keep `key: str | None` as the raw/UTF-8 string (v1.0
  contract). ADD a new additive field `key_decoded: dict | None`.
- Add `raw_key: bytes | None` to `KafkaMessage` (mirrors `raw` for the value);
  the consumer adapter must stop discarding raw key bytes and thread them through.
- Attempt key decode ONLY when the key carries Confluent framing
  (`raw_key and len(raw_key) >= 5 and raw_key[0] == 0x00`). Plain/string keys →
  `key_decoded = None`, never raise (resilient path like search value-decode).
- Reuse the existing `SchemaRegistryHttpAdapter.decode()` for the key bytes
  (no new `decode_key()` method) — it already handles framing + typed errors.

### HTTP transport in server.json (HTTP-01)
- Represent HTTP via a `remotes`-style streamable-HTTP transport entry in
  `server.json` alongside the existing stdio `packages` entry (MCP schema-correct
  shape), NOT a fake `transports` array.
- Make the declaration TRUTHFUL: mount a real FastMCP streamable-HTTP app at
  `/mcp` on the FastAPI server so the declared endpoint actually exists and
  serves MCP. The declared endpoint path must match this route.
- During planning, reconcile the roadmap's Phase-4 success-criterion #3 (which
  assumed a `d['transports']` array) to the real server.json shape — assert the
  HTTP transport entry exists in whatever key the MCP schema actually uses.
- Mirror the HTTP option in `glama.json` `serverConfig` (document the http run
  mode); keep stdio as the default.

### Claude's Discretion
- Exact FastMCP streamable-http mounting mechanics on the existing FastAPI app,
  field ordering, and test factoring are at Claude's discretion, provided the
  4-face symmetry and read-only guarantee hold.
</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `SchemaRegistryHttpAdapter.decode(raw, topic, partition, offset)` — already
  does magic-byte framing detection, schema_id extraction
  (`int.from_bytes(raw[1:5])`), Avro/Protobuf/JSON decode, typed `DecodeError`,
  and per-schema caching. Reuse it for key bytes.
- `domain/models.py::KafkaMessage` — pydantic v2 model; currently `key:str|None`,
  `value:dict|None`, `raw:bytes`, plus Investigator Evidence fields. No `raw_key`,
  no `schema_id` yet.
- `adapters/inbound/rest_api.py::_serialize_message` — the single serialization
  point for the FastAPI face; MCP/CLI faces have analogous serializers. New
  fields must flow through all of them.

### Established Patterns
- Hexagonal boundary enforced by a test: decode library imports live ONLY in
  `adapters/outbound/`; `domain/` and `ports/` stay import-free. schema_id
  extraction from raw bytes (pure byte math) is domain-safe.
- Resilient vs strict decode: `search_messages` swallows `DecodeError` (returns
  None), `get_message` may propagate. Key decode should follow the resilient
  pattern (never crash the message).
- Read-only guarantee: assign-only, `enable.auto.commit=false`, throwaway group.
  This phase adds no consumer writes.

### Integration Points
- `adapters/outbound/confluent_consumer.py` (lines ~270, ~422): builds `key_str`
  from `msg.key()` and currently DROPS `raw_key`. Must capture `raw_key` bytes
  into the model.
- `domain/search_service.py`: orchestrates decode; the natural place to attach
  `key_decoded` + `schema_id` after fetching messages.
- `adapters/inbound/rest_api.py::create_app` + `server.py` dispatch: the FastAPI
  app where the FastMCP streamable-http `/mcp` mount is added.
</code_context>

<specifics>
## Specific Ideas

- schema_id math: `int.from_bytes(raw[1:5], "big")` guarded by
  `len(raw) >= 5 and raw[0] == 0x00`. Same guard for `raw_key`.
- New `KafkaMessage` fields: `raw_key: bytes | None = None`,
  `key_decoded: dict[str, Any] | None = None`,
  `schema_id: dict[str, int | None] | None = None`.
- HTTP mount path: `/mcp` (FastMCP streamable-http), declared in server.json.
</specifics>

<deferred>
## Deferred Ideas

- Searching BY decoded key (extending `key_field` to traverse `key_decoded`) —
  not required by KEY-01; defer.
- Schema-lookup caching improvements (KEY-03) and HTTP auth/TLS (HTTP-02) —
  Future Requirements, separate milestone.
</deferred>
