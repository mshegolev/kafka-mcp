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
  critical: 0
  warning: 4
  info: 4
  total: 8
status: issues_found
---

# Phase 1: Code Review Report

**Reviewed:** 2026-06-05T00:00:00Z
**Depth:** standard
**Files Reviewed:** 20
**Status:** issues_found

## Summary

Iteration-2 re-review of the read-only Kafka MCP foundation. **All five prior
fixes are verified sound:**

- **CR-01 (SASL gating):** SASL keys are now gated on `settings.sasl_mechanism`
  truthiness, not on `!= PLAINTEXT`. TLS-only (`SSL`) configs no longer inject
  `sasl.mechanism=None`. Confirmed by `test_ssl_only_omits_sasl_keys` and the
  non-mocked `test_real_librdkafka_accepts_built_conf` regression test.
- **WR-01 (typed ConfigError):** `KafkaMcpSettings.__init__` wraps every pydantic
  `ValidationError` variant (`missing`, `value_error`, and the catch-all for
  `int_parsing`/`float_parsing`) into `ConfigError`. Traced live: bad `max_scan`,
  bad `poll_timeout`, whitespace and missing `bootstrap_servers` all surface as
  `ConfigError`. No raw `ValidationError` leaks.
- **WR-02 (close/ctx-mgr + lifespan):** `KafkaClient.close()` delegates safely
  (no-op when the consumer lacks `close()`), CLI and stdio wrap execution in
  `try/finally: client.close()`, and FastAPI uses an `asynccontextmanager`
  lifespan that closes on shutdown.
- **WR-03 (metadata timeout):** `get_watermark_offsets` now uses
  `_METADATA_TIMEOUT_SECONDS` (10s) instead of `poll_timeout`. Verified by
  `test_get_watermark_offsets_uses_metadata_timeout`.
- **WR-04 (code-based TopicNotFound mapping):** `get_watermark_offsets` maps only
  `UNKNOWN_TOPIC_OR_PART`, `_UNKNOWN_TOPIC`, `_UNKNOWN_PARTITION` to
  `TopicNotFoundError` and re-raises transient errors (`_TRANSPORT`, etc.). All
  three error constants confirmed present in the installed `confluent_kafka`.

**Invariants confirmed intact:**
- `domain/` and `ports/` contain zero broker/HTTP/framework imports (grep clean).
- Read-only guarantee holds: `enable.auto.commit=False`, throwaway uuid4
  `group.id`, no `subscribe(` anywhere in the adapter (assign-only).
- Credentials use `SecretStr`; `get_secret_value()` is read into a local-only
  conf dict and never stored on an attribute or logged.

The full suite (92 tests) passes. However, the WR-03 fix introduced an
import-ordering regression that **the project's own configured linter flags**,
and a parallel error-mapping inconsistency that WR-04 left unfixed in a sibling
method. No blockers. Four warnings and four info items below.

## Warnings

### WR-01: `get_partition_ids` mis-maps transient metadata errors to TopicNotFound

**File:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py:122-124`
**Issue:** WR-04 carefully fixed `get_watermark_offsets` to translate *only*
unknown-topic/partition error codes into `TopicNotFoundError`, re-raising
transient/operational failures. The sibling method `get_partition_ids` was left
with the broad-brush logic:

```python
topic_meta = metadata.topics.get(topic)
if topic_meta is None or topic_meta.error is not None:
    raise TopicNotFoundError(topic)
```

`TopicMetadata.error` is also set for transient, non-fatal conditions (e.g.
`LEADER_NOT_AVAILABLE` during a leader election/rebalance, or
`REPLICA_NOT_AVAILABLE`). For an existing topic in a transient state this raises
`TopicNotFoundError`, which the REST adapter then renders as HTTP 404 — reporting
a live topic as "not found" during a broker hiccup. This is the exact bug class
WR-04 fixed in the neighbouring method; the two metadata paths are now
inconsistent.
**Fix:** Mirror the WR-04 code-discrimination here — treat only genuine
unknown-topic codes as not-found:
```python
topic_meta = metadata.topics.get(topic)
if topic_meta is None:
    raise TopicNotFoundError(topic)
err = topic_meta.error
if err is not None:
    if err.code() in (
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError._UNKNOWN_TOPIC,
    ):
        raise TopicNotFoundError(topic)
    raise KafkaException(err)  # transient: surface unchanged
return sorted(topic_meta.partitions.keys())
```

### WR-02: WR-03 fix introduced an E402 lint regression (linter no longer clean)

**File:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py:24-31`
**Issue:** The WR-03 fix defined `_METADATA_TIMEOUT_SECONDS` (lines 24-27)
*between* the third-party import (`from confluent_kafka import ...`, line 22) and
the first-party imports (lines 29-31). This pushes those first-party imports
below a module-level statement, which the project's configured ruff ruleset
(`select = ["E", "F", "W", "I", "UP"]` in `pyproject.toml`) flags as three
`E402` violations. `ruff check src/` reports **14 errors total** — the package
no longer passes its own declared lint gate, which is a CI-blocking regression
in most pipelines.
**Fix:** Move the constant below all imports:
```python
from confluent_kafka import Consumer, KafkaError, KafkaException

from kafka_mcp.config import KafkaMcpSettings
from kafka_mcp.domain.errors import TopicNotFoundError

# Synchronous broker metadata round-trips use a generous fixed budget.
_METADATA_TIMEOUT_SECONDS = 10.0
```
(Then run `ruff check --fix src/` to clear the auto-fixable items below.)

### WR-03: Dead imports across multiple modules (F401)

**File:** `src/kafka_mcp/adapters/outbound/schema_registry_http.py:14,16`;
`src/kafka_mcp/domain/search_service.py:11`;
`src/kafka_mcp/adapters/inbound/lib.py:31`;
`src/kafka_mcp/adapters/outbound/confluent_consumer.py:31`
**Issue:** Ruff F401 flags genuinely unused imports:
- `schema_registry_http.py:14` — `import httpx` is referenced only inside a
  commented-out Phase-2 block, so it is dead today.
- `schema_registry_http.py:16` — `SchemaRegistryPort` imported but the class
  does not subclass or reference it (it satisfies the Protocol structurally).
- `search_service.py:11` — `TopicNotFoundError` imported but never referenced
  (it is raised by the injected consumer, not by this module).
- `lib.py:31` — `ConfigError` and `TopicNotFoundError` imported but unused in
  code (only mentioned in docstrings).
- `confluent_consumer.py:31` — `ConsumerPort` imported but unused (structural
  conformance, not referenced).

Dead imports obscure the real dependency graph and, for `httpx`, hide that the
stub has no runtime HTTP dependency. Some (e.g. `lib.py`) may be intentional
re-export-for-discoverability, but they are not in `__all__` and so serve no
purpose.
**Fix:** Remove the unused imports (or, where re-export is intended, add them to
an `__all__` and reference them). `ruff check --fix src/` removes the
unambiguous ones automatically.

### WR-04: SC-3 boundary test only catches `import confluent_kafka`, misses `from confluent_kafka import`

**File:** `tests/test_lib.py:295-311`
**Issue:** The hexagonal-boundary guard (a stated Phase 1 success criterion)
greps `domain/` for the literal string `import confluent_kafka`:

```python
"grep", "-r", "import confluent_kafka", "src/kafka_mcp/domain/",
```

This does not match `from confluent_kafka import Consumer`, which is the more
common import form (and exactly the form the outbound adapter uses on line 22).
A future regression that imports `from confluent_kafka import X` into a domain
module would pass this test silently, defeating the invariant the test exists to
protect. The test also hard-codes an absolute `cwd`
(`/opt/develop/aiqa/mcps/kafka-mcp`), making it non-portable across machines/CI.
**Fix:** Broaden the pattern and drop the absolute path:
```python
result = subprocess.run(
    ["grep", "-rE", r"(import confluent_kafka|from confluent_kafka)",
     "src/kafka_mcp/domain/", "src/kafka_mcp/ports/"],
    capture_output=True, text=True,
    cwd=str(pathlib.Path(__file__).resolve().parents[1]),
)
assert result.returncode != 0, f"BOUNDARY VIOLATION: {result.stdout}"
```

## Info

### IN-01: Magic-number `10.0` duplicated where the WR-03 constant should be used

**File:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py:98,120`
**Issue:** WR-03 introduced `_METADATA_TIMEOUT_SECONDS = 10.0` and applied it to
`get_watermark_offsets` (line 152), but `list_topics` (line 98) and
`get_partition_ids` (line 120) still use a hard-coded `timeout=10.0`. The intent
of the named constant — a single metadata-budget knob — is only partially
realised; the two literals will drift if the constant is ever tuned.
**Fix:** Replace both literals with `timeout=_METADATA_TIMEOUT_SECONDS`.

### IN-02: Quoted forward-reference type annotations now redundant (UP037)

**File:** `src/kafka_mcp/config.py:144`;
`src/kafka_mcp/adapters/inbound/lib.py:134`;
`src/kafka_mcp/adapters/outbound/confluent_consumer.py:184`
**Issue:** With `from __future__ import annotations` present in every module, the
string-quoted self-referential return types (`-> "KafkaMcpSettings"`,
`-> "KafkaClient"`, `-> "ConfluentConsumerAdapter"`) are unnecessary; ruff `UP037`
flags them. Purely stylistic, but contributes to the 14-error ruff count.
**Fix:** Drop the quotes (`-> KafkaMcpSettings`, etc.) — `ruff check --fix`
handles this.

### IN-03: Import blocks unsorted in CLI and REST adapters (I001)

**File:** `src/kafka_mcp/adapters/inbound/cli.py:26`;
`src/kafka_mcp/adapters/inbound/rest_api.py:24`
**Issue:** Ruff `I001` flags un-sorted/un-grouped import blocks. Cosmetic, but
part of the declared lint gate.
**Fix:** `ruff check --fix src/`.

### IN-04: `exc.errors()[0]` indexing in config catch-all is theoretically unguarded

**File:** `src/kafka_mcp/config.py:74`
**Issue:** The catch-all branch does `first = exc.errors()[0]` without checking
for an empty list. Pydantic always populates at least one error when raising
`ValidationError`, so this is unreachable in practice, but the bare index is a
latent `IndexError` if that assumption ever changes (e.g. a custom validator
re-raising an empty `ValidationError`).
**Fix:** Guard defensively:
```python
errors = exc.errors()
if not errors:
    raise ConfigError(str(exc)) from exc
first = errors[0]
```

---

_Reviewed: 2026-06-05T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
