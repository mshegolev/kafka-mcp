---
phase: 01-foundation
reviewed: 2026-06-05T22:10:00Z
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
  warning: 2
  info: 3
  total: 5
status: issues_found
---

# Phase 1: Code Review Report

**Reviewed:** 2026-06-05T22:10:00Z
**Depth:** standard
**Status:** issues_found

## Summary

Iteration 3 (final) adversarial re-review of the read-only Kafka MCP brick. The
iteration-2 fixes were verified directly against the source and the gates:

- **Lint gate (`ruff check src/`): GREEN** — "All checks passed!" confirmed.
- **Test suite: GREEN** — 92 passed, 0 failed (run in an isolated venv to
  bypass an unrelated, pre-existing `allure_pytest` plugin breakage in the
  global interpreter; that breakage is environmental, not a defect in this code).
- **WR-02 (consumer lifecycle):** All three entry points (`server.py` stdio,
  REST lifespan, CLI `finally`) call `client.close()`; `KafkaClient.close()` is
  a safe no-op for mock consumers. Sound.
- **WR-01 (`get_partition_ids` code-based TopicNotFound mapping):** Implemented
  with the same code-discrimination as WR-04 — only `UNKNOWN_TOPIC_OR_PART /
  _UNKNOWN_TOPIC / _UNKNOWN_PARTITION` map to `TopicNotFoundError`; everything
  else re-raises. All four `KafkaError` constants used were verified to exist
  (3, -188, -190, -195). Logic is correct.
- **WR-03 (metadata timeout):** Watermark fetch uses the dedicated 10s
  `_METADATA_TIMEOUT_SECONDS`, not `poll_timeout`. Covered by a test.
- **WR-04 (transient-vs-not-found boundary):** Hardened with a `_TRANSPORT`
  regression test asserting non-not-found errors surface unchanged. Sound.
- **Dead F401 imports (prior round, `src/`):** clean.

**Core invariants — all confirmed:**

- `domain/` and `ports/` contain **zero** broker/HTTP/web-framework imports
  (grep for `confluent_kafka|httpx|fastapi|uvicorn|requests|orjson|mcp` returns
  nothing). Hexagonal boundary holds.
- Read-only guarantee: `confluent_consumer.py` contains **no** `subscribe`,
  `.commit(`, or `store_offsets`; `enable.auto.commit=False` and a throwaway
  `kafka-mcp-ro-{uuid4}` group id are always set. Structurally read-only.
- Credentials (`sasl_password`, `sr_pass`) are `SecretStr`;
  `get_secret_value()` is called only inline into a local conf dict that is
  never stored or logged. `test_secret_str_not_exposed_in_repr` guards repr.

No Critical/Blocker issues remain. Two Warnings persist: a coverage gap on the
WR-01 fix path, and a project-wide lint failure scoped to `tests/`. Neither
blocks shipping the read-only brick, but both are real quality defects.

## Narrative Findings (AI reviewer)

### Warnings

#### WR-01: WR-01 fix path (`get_partition_ids`) has zero adapter-level test coverage

**File:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py:103-140`
**Issue:** The iteration-2 WR-01 fix added the most logic-heavy new code in the
phase — error-code discrimination on `TopicMetadata.error` that distinguishes
genuine unknown-topic codes (→ `TopicNotFoundError`) from transient codes
(→ re-raise `KafkaException`). This entire branch is **never exercised by any
test against the real adapter**. `grep get_partition_ids tests/` matches only
`test_lib.py` / `test_domain.py`, both of which use the in-memory `MockConsumer`
whose `get_partition_ids` is a trivial dict lookup — it never touches the
adapter's `topic_meta.error` logic. `test_adapters.py` has thorough coverage for
`get_watermark_offsets` (the parallel WR-04 path: not-found, unknown-partition,
transient `_TRANSPORT`) but **no equivalent for `get_partition_ids`**. The
`metadata.topics.get(topic) is None` branch, the three not-found codes, and the
`raise KafkaException(err)` transient branch can all silently regress.
**Fix:** Mirror the existing `TestConfluentConsumerAdapterWatermarks` tests for
`get_partition_ids`, driving a mocked `list_topics(topic=...)` return:
```python
def test_get_partition_ids_topic_missing_raises_not_found(self) -> None:
    from kafka_mcp.domain.errors import TopicNotFoundError
    meta = MagicMock(); meta.topics = {}        # topic absent -> .get() is None
    mock_consumer = MagicMock()
    mock_consumer.list_topics.return_value = meta
    adapter = self._make_adapter(mock_consumer)
    with pytest.raises(TopicNotFoundError):
        adapter.get_partition_ids("nope")

def test_get_partition_ids_transient_error_reraised(self) -> None:
    from confluent_kafka import KafkaError, KafkaException
    tmeta = MagicMock()
    tmeta.error = KafkaError(KafkaError.LEADER_NOT_AVAILABLE)
    meta = MagicMock(); meta.topics = {"t": tmeta}
    mock_consumer = MagicMock()
    mock_consumer.list_topics.return_value = meta
    adapter = self._make_adapter(mock_consumer)
    with pytest.raises(KafkaException):
        adapter.get_partition_ids("t")

def test_get_partition_ids_unknown_topic_code_raises_not_found(self) -> None:
    from confluent_kafka import KafkaError
    from kafka_mcp.domain.errors import TopicNotFoundError
    tmeta = MagicMock()
    tmeta.error = KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)
    meta = MagicMock(); meta.topics = {"t": tmeta}
    mock_consumer = MagicMock()
    mock_consumer.list_topics.return_value = meta
    adapter = self._make_adapter(mock_consumer)
    with pytest.raises(TopicNotFoundError):
        adapter.get_partition_ids("t")

def test_get_partition_ids_returns_sorted(self) -> None:
    tmeta = MagicMock(); tmeta.error = None
    tmeta.partitions = {2: MagicMock(), 0: MagicMock(), 1: MagicMock()}
    meta = MagicMock(); meta.topics = {"t": tmeta}
    mock_consumer = MagicMock()
    mock_consumer.list_topics.return_value = meta
    adapter = self._make_adapter(mock_consumer)
    assert adapter.get_partition_ids("t") == [0, 1, 2]
```

#### WR-02: `ruff check tests/` fails — project-wide lint gate is red

**File:** `tests/test_adapters.py:13,15`, `tests/test_domain.py:7`, `tests/test_inbound.py:8`, `tests/test_lib.py:12,16`
**Issue:** The fix loop only re-verified `ruff check src/`. The same ruff config
(`pyproject.toml` selects `E,F,W,I,B,UP` with no `tests/` exclusion) reports
**6 errors in `tests/`**: four `I001` (un-sorted import blocks) and two `F401`
(`import typing` unused in `test_adapters.py:15`; `import sys` unused in
`test_lib.py:16`). Tests are source code under this project's conventions, and
a CI step running `ruff check` (no path argument, or `ruff check .`) will fail.
The `F401`s in particular are the same class of dead-import defect that was
fixed in `src/` — leaving them in `tests/` is an inconsistent application of
that fix.
**Fix:** Run `ruff check tests/ --fix` (all 6 are auto-fixable), then re-run
`ruff check .` to confirm a clean tree. Verify the test suite still passes after
removing `typing` and `sys` (both are genuinely unreferenced — `test_lib.py`
imports `sys` but only uses `subprocess` and `pathlib`).

### Info

#### IN-01: `describe_topic` leader is a hardcoded placeholder

**File:** `src/kafka_mcp/domain/search_service.py:87`
**Issue:** `PartitionInfo.leader=0` is a documented Phase-2 TODO placeholder. The
field is exposed in REST/CLI/MCP output where a consumer could mistake `0` for a
real leader broker id (broker 0 is a valid id). This is acknowledged and scoped
out of Phase 1, so it is informational only.
**Fix:** Phase 2: wire the real leader via AdminClient, or make `leader`
`Optional[int]` defaulting to `None` so a placeholder is unambiguous.

#### IN-02: Duplicated not-found code-discrimination logic

**File:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py:131-139` and `174-183`
**Issue:** The three-code `UNKNOWN_TOPIC_OR_PART / _UNKNOWN_TOPIC /
_UNKNOWN_PARTITION` membership test is copy-pasted between `get_partition_ids`
and `get_watermark_offsets`. Acceptable for now, but the two copies can drift.
**Fix:** Extract a module-level `_NOT_FOUND_CODES = frozenset({...})` and an
`_is_not_found(err) -> bool` predicate used by both call sites.

#### IN-03: Hardcoded `timeout=10.0` magic number in `list_topics` / `get_partition_ids`

**File:** `src/kafka_mcp/adapters/outbound/confluent_consumer.py:97,119`
**Issue:** `list_topics` and `get_partition_ids` pass a literal `timeout=10.0`
while `get_watermark_offsets` correctly uses the named
`_METADATA_TIMEOUT_SECONDS = 10.0` constant. The literal and the constant are
the same value today but are not linked; a future tuning of the constant would
silently miss two of the three metadata round-trips.
**Fix:** Replace the two `timeout=10.0` literals with
`timeout=_METADATA_TIMEOUT_SECONDS`.

---

_Reviewed: 2026-06-05T22:10:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
