---
phase: 01-foundation
reviewed: 2026-06-05T00:00:00Z
depth: standard
files_reviewed: 20
files_reviewed_list:
  - src/kafka_mcp/__init__.py
  - src/kafka_mcp/config.py
  - src/kafka_mcp/server.py
  - src/kafka_mcp/domain/models.py
  - src/kafka_mcp/domain/errors.py
  - src/kafka_mcp/domain/search_service.py
  - src/kafka_mcp/ports/consumer.py
  - src/kafka_mcp/ports/schema_registry.py
  - src/kafka_mcp/adapters/inbound/lib.py
  - src/kafka_mcp/adapters/inbound/mcp_stdio.py
  - src/kafka_mcp/adapters/inbound/rest_api.py
  - src/kafka_mcp/adapters/inbound/cli.py
  - src/kafka_mcp/adapters/outbound/confluent_consumer.py
  - src/kafka_mcp/adapters/outbound/schema_registry_http.py
  - src/kafka_mcp/adapters/outbound/json_orjson.py
  - tests/test_domain.py
  - tests/test_adapters.py
  - tests/test_lib.py
  - tests/test_inbound.py
  - pyproject.toml
findings:
  critical: 1
  warning: 4
  info: 4
  total: 9
status: issues_found
---

# Phase 1: Code Review Report

**Reviewed:** 2026-06-05
**Depth:** standard
**Files Reviewed:** 20
**Status:** issues_found

## Summary

Read-only Kafka MCP brick, hexagonal layout. The core architecture is sound and
several Phase-1 invariants verifiably hold:

- **Hexagonal boundary holds.** `grep` for `confluent_kafka|httpx|fastapi|mcp|uvicorn`
  imports in `src/kafka_mcp/domain/` and `src/kafka_mcp/ports/` returns nothing.
- **Read-only guarantee (KAFKA-06) holds structurally.** `confluent_consumer.py`
  sets `enable.auto.commit=False`, uses a uuid4 throwaway `group.id`
  (`kafka-mcp-ro-{uuid4}`), and contains no `subscribe()` call (test enforces this).
- **Credential hygiene holds.** `sasl_password`/`sr_pass` are `SecretStr`; the
  password is only materialised into a local conf dict in `__init__` and never
  stored on an attribute or logged. `.env.example` carries no real secrets.
- **Public-bind hardening** (commit 4479e08) is present in `server.py`.

However, the review surfaced one BLOCKER that breaks every TLS-secured production
deployment, plus contract and robustness defects. The unit tests do **not** catch
the BLOCKER because they exclusively use `PLAINTEXT` settings and mock the
`Consumer` constructor, so the real librdkafka config validation never runs.

These are narrative findings from direct code review. No `<structural_findings>`
block was supplied, so there is no fallow substrate section.

## Critical Issues

### CR-01: `SSL`/`SASL_SSL`-without-mechanism crashes at construction — `sasl.mechanism=None` rejected by librdkafka

**File:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py:59-69`
**Issue:**
The SASL branch is gated only by `if settings.security_protocol != "PLAINTEXT":`.
This wrongly assumes every non-`PLAINTEXT` protocol uses SASL. For the common,
valid TLS-only configuration `KAFKA_MCP_SECURITY_PROTOCOL=SSL` (no SASL), the
SASL fields are all unset, so the code injects `sasl.mechanism=None`,
`sasl.username=None`, `sasl.password=None` into the conf dict. librdkafka rejects
a `None` mechanism at `Consumer()` construction:

```
KafkaException: KafkaError{code=_INVALID_ARG,val=-186,
  str="Invalid value for configuration property \"sasl.mechanisms\": (null)"}
```

The same crash occurs for `SASL_SSL`/`SASL_PLAINTEXT` whenever
`KAFKA_MCP_SASL_MECHANISM` is not explicitly set. This means `KafkaClient.from_env()`
— the entry point used by the CLI, REST, and MCP faces — raises an unhandled
`KafkaException` (not a domain `ConfigError`) at startup for any TLS deployment.
Verified by constructing a real `Consumer` with these values; it fails.

The existing tests never catch this: every adapter test uses
`_make_settings()` → `PLAINTEXT`, and patches the `Consumer` constructor with a
`MagicMock`, so librdkafka's argument validation is never exercised.

**Fix:** Only add a SASL key when its value is present, and decide SASL vs
plain-TLS by mechanism, not by "not PLAINTEXT":

```python
if settings.security_protocol != "PLAINTEXT":
    conf["security.protocol"] = settings.security_protocol

# Only configure SASL when a mechanism is actually requested.
if settings.sasl_mechanism:
    conf["sasl.mechanism"] = settings.sasl_mechanism
    if settings.sasl_username is not None:
        conf["sasl.username"] = settings.sasl_username
    if settings.sasl_password is not None:
        conf["sasl.password"] = settings.sasl_password.get_secret_value()
```

Add a no-broker test that uses the real `Consumer` (or asserts the conf dict
omits `sasl.*` keys when `security_protocol="SSL"` and no mechanism is set), so
the regression is covered without mocking away librdkafka's validation.

## Warnings

### WR-01: Invalid `max_scan` / `poll_timeout` env vars leak raw `ValidationError`, breaking the D-04 single-exception contract

**File:** `src/kafka_mcp/config.py:38-70`
**Issue:**
`KafkaMcpSettings.__init__` is documented to convert pydantic field errors into
`ConfigError` "so callers have a single domain-typed exception to handle (D-04)".
The loop only re-raises for `err_type == "missing"` or `"value_error"`; for any
other pydantic error type (e.g. `int_parsing` for `KAFKA_MCP_MAX_SCAN=abc`,
`float_parsing` for `KAFKA_MCP_POLL_TIMEOUT=xyz`) it falls through and re-raises
the raw `ValidationError`. Verified:

```
$ KAFKA_MCP_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_MCP_MAX_SCAN=notanint ...
OTHER: ValidationError 1 validation error for KafkaMcpSettings
max_scan: Input should be a valid integer ...
```

Callers that only `except ConfigError` (as the module docstring and D-04 invite
them to) will not catch this, contradicting the stated contract.

**Fix:** Add a catch-all conversion after the typed branches, or convert every
error type:

```python
# After the typed checks, before re-raising:
first = exc.errors()[0]
loc = first.get("loc", ())
field = f"KAFKA_MCP_{str(loc[0]).upper()}" if loc else "config"
raise ConfigError(
    f"{field}: {first.get('msg', str(exc))}"
) from exc
```

### WR-02: `from_env()`-constructed consumers are never closed — librdkafka client leak

**File:** `src/kafka_mcp/adapters/inbound/lib.py:35-82` (and callers
`server.py:52,71`, `cli.py:195`)
**Issue:**
`ConfluentConsumerAdapter` is a context manager (`__enter__`/`__exit__` →
`Consumer.close()`), but `KafkaClient` neither exposes `close()` nor implements
the context-manager protocol, and it stores the adapter without ever entering it.
Every production path — `KafkaClient.from_env()` in CLI, REST, and MCP — builds a
real librdkafka `Consumer` (background threads, sockets, broker connection) that
is never closed. The CLI process exits quickly so the OS reclaims it, but the
long-lived REST and MCP servers accumulate a connected consumer per process with
no clean shutdown, and the documented `with ConfluentConsumerAdapter(...)` cleanup
contract is bypassed by the facade.

**Fix:** Give `KafkaClient` a `close()` plus `__enter__`/`__exit__` that delegate
to the underlying adapter, and call `Consumer.close()` on FastAPI/MCP shutdown
(e.g. FastAPI `lifespan`/`shutdown` event) and at the end of `cli.main`.

### WR-03: `describe_topic` uses `poll_timeout` as the broker watermark-request timeout

**File:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py:137-139`
**Issue:**
`get_watermark_offsets` passes `timeout=self._settings.poll_timeout` (default
**1.0s**) to a synchronous broker round-trip, while the sibling metadata calls
(`list_topics`, `get_partition_ids`) use a hardcoded `10.0s`. `poll_timeout` is
semantically the `consumer.poll()` timeout (per its docstring/`.env.example`),
not a metadata-fetch budget. Under broker latency or a topic with many
partitions, a 1.0s budget can spuriously raise `KafkaException` → which this
method then mistranslates into `TopicNotFoundError` (see WR-04), reporting an
existing topic as missing.

**Fix:** Use a dedicated, larger metadata timeout (or the same 10.0s used by the
other metadata calls) here, and reserve `poll_timeout` for actual `poll()` calls
introduced in Phase 2.

### WR-04: `get_watermark_offsets` collapses all `KafkaException`s into `TopicNotFoundError`

**File:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py:140-141`
**Issue:**
`except KafkaException` unconditionally re-raises as `TopicNotFoundError(topic)`.
`KafkaException` also covers transient/operational failures — request timeout
(see WR-03), broker unavailable, transport errors, authorization failures. Those
will be surfaced to the caller (and to the REST face as an HTTP **404**, per
`rest_api.py:99-103`) as "topic not found", which is incorrect and misleading
during an incident: a broker outage looks like a missing topic.

**Fix:** Inspect `exc.args[0].code()` and only map the genuine
`UNKNOWN_TOPIC_OR_PART` / unknown-partition codes to `TopicNotFoundError`;
re-raise other `KafkaException`s unchanged (or wrap in a distinct operational
error type).

## Info

### IN-01: Unused `httpx` import in the Phase-1 stub

**File:** `src/kafka_mcp/adapters/outbound/schema_registry_http.py:14`
**Issue:** `import httpx` is present but `httpx` is referenced only inside
docstrings and commented-out Phase-2 code. This is a live unused import
(ruff `F401`, which is in the configured lint `select`).
**Fix:** Remove the import until Phase 2 wires the real HTTP call, or guard it
with the commented block.

### IN-02: `orjson_dumps`/`orjson_loads` type hints disagree with actual usage

**File:** `src/kafka_mcp/adapters/outbound/json_orjson.py:21,36`
**Issue:** `orjson_dumps(obj: dict) -> bytes` is annotated to accept a `dict`,
but `cli.py:126` calls `orjson_dumps(topics)` with a `list[str]`. Likewise
`orjson_loads(...) -> dict` will return a `list`/scalar for non-object JSON.
Runtime is fine (orjson serialises lists), but the annotations are wrong and a
type checker would flag the `list` argument.
**Fix:** Widen to `obj: Any` / `-> Any` (or `dict | list`) to match real usage.

### IN-03: `_consumer` stored on both `KafkaClient` and `TopicService`

**File:** `src/kafka_mcp/adapters/inbound/lib.py:59-60`
**Issue:** `KafkaClient.__init__` stores `self._consumer = consumer` and then
also constructs `TopicService(consumer)`, which stores its own `self._consumer`.
`KafkaClient._consumer` is never read anywhere (the facade delegates exclusively
to `self._service`). Dead state that invites confusion about which reference
owns the consumer lifecycle (relevant to WR-02).
**Fix:** Drop `self._consumer` from `KafkaClient`, or keep only the one needed
to implement `close()` (WR-02).

### IN-04: Placeholder `leader=0` is indistinguishable from a real broker id 0

**File:** `src/kafka_mcp/domain/search_service.py:88`
**Issue:** `PartitionInfo.leader` is hardcoded to `0` as a Phase-2 placeholder,
but broker id `0` is a valid, common leader id. A consumer of `describe_topic`
output cannot tell "leader unknown" from "leader is broker 0". The CLI prints it
in the `Leader` column as if authoritative.
**Fix:** Make `leader` `int | None` and use `None` for the placeholder (or
`-1`), so downstream cannot mistake it for a real leader. Tracked TODO already
references Phase 2 AdminClient wiring.

---

_Reviewed: 2026-06-05_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
