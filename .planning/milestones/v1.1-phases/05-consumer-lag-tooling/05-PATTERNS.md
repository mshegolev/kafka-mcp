# Phase 5: Consumer Lag Tooling - Pattern Map

**Mapped:** 2026-06-16
**Files analyzed:** 8 (7 source files + 1 test suite)
**Analogs found:** 8 / 8

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `src/kafka_mcp/domain/models.py` | model | data-contract | `KafkaMessage` in same file | exact |
| `src/kafka_mcp/ports/consumer.py` | port | request-response | `ConsumerPort.get_watermark_offsets` in same file | exact |
| `src/kafka_mcp/adapters/outbound/confluent_consumer.py` | adapter-outbound | request-response | `ConfluentConsumerAdapter.get_watermark_offsets` in same file | exact |
| `src/kafka_mcp/adapters/inbound/lib.py` | facade | request-response | `KafkaClient.describe_topic` in same file | exact |
| `src/kafka_mcp/adapters/inbound/mcp_stdio.py` | adapter-inbound | request-response | `describe_topic` tool in same file | exact |
| `src/kafka_mcp/adapters/inbound/rest_api.py` | adapter-inbound | request-response | `POST /tools/describe_topic` in same file | exact |
| `src/kafka_mcp/adapters/inbound/cli.py` | adapter-inbound | request-response | `describe-topic` subcommand in same file | exact |
| `tests/test_inbound.py` + `tests/test_adapters.py` | test | request-response | `MockKafkaClient` + adapter tests in same files | exact |

## Pattern Assignments

### `src/kafka_mcp/domain/models.py` — add `LagRecord` (model, data-contract)

**Analog:** `KafkaMessage` class in same file (lines 42–82)

**Module docstring pattern** (lines 1–5):
```python
"""Domain models — pure pydantic v2 data structures.

No I/O or framework imports. These are the canonical data contracts
used across all inbound and outbound adapters.
"""
```

**Imports pattern** (lines 7–13):
```python
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field
```

**Model definition pattern** — follow `KafkaMessage` shape (lines 42–82):
```python
class KafkaMessage(BaseModel):
    """A single Kafka message as returned by the search/get domain operations.

    Carries both wire metadata (topic, partition, offset, key, headers,
    timestamp) and the Investigator Contract Evidence surface (source,
    event_type, keys).
    """

    topic: str
    partition: int
    offset: int
    key: str | None = None
    # ... typed fields with defaults ...
    timestamp_utc: datetime

    # --- Investigator Contract Evidence fields ---
    source: str = "kafka"
    event_type: str = "kafka_message"
```

**New `LagRecord` should follow this exact shape:**
- Pydantic `BaseModel` subclass
- Typed fields: `group: str`, `topic: str`, `partition: int`, `current_offset: int`, `end_offset: int`, `lag: int`, `timestamp_utc: datetime`
- Evidence fields: `source: str = "kafka"`, `event_type: str = "consumer_lag"`
- Docstring explaining what the model represents

---

### `src/kafka_mcp/ports/consumer.py` — add `consumer_group_lag` method (port, request-response)

**Analog:** `get_watermark_offsets` method in same file (lines 38–50)

**Imports pattern** (lines 1–13):
```python
"""ConsumerPort — broker consumer protocol.

Pure Protocol definition: no broker library imports here.
Outbound adapters implement this protocol using the real librdkafka client.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from kafka_mcp.domain.errors import MessageNotFoundError  # noqa: F401
from kafka_mcp.domain.models import KafkaMessage
```

**Protocol method pattern** — follow `get_watermark_offsets` (lines 38–50):
```python
    def get_watermark_offsets(
        self, topic: str, partition: int
    ) -> tuple[int, int]:
        """Return (earliest, latest) offsets for the given partition.

        Args:
            topic: Topic name.
            partition: Partition index (0-based).

        Returns:
            Tuple of (earliest_offset, latest_offset).
        """
        ...
```

**New method should follow this pattern:**
- Type-annotated signature with `self`
- Full docstring with Args/Returns/Raises sections
- Body is `...` (protocol method stub)
- Import `LagRecord` from `kafka_mcp.domain.models`
- Signature: `def consumer_group_lag(self, group: str, topics: list[str] | None = None) -> list[LagRecord]: ...`

---

### `src/kafka_mcp/adapters/outbound/confluent_consumer.py` — implement `consumer_group_lag` (adapter-outbound, request-response)

**Analog:** `get_watermark_offsets` method in same file (lines 148–190)

**Imports pattern** (lines 17–31):
```python
from __future__ import annotations

from datetime import datetime, timezone
from types import TracebackType
from uuid import uuid4

from confluent_kafka import TIMESTAMP_CREATE_TIME, Consumer, KafkaError, KafkaException, TopicPartition

from kafka_mcp.config import KafkaMcpSettings
from kafka_mcp.domain.errors import (
    MessageNotFoundError,
    TopicNotFoundError,
    TransientError,
)
from kafka_mcp.domain.models import KafkaMessage
```

**Constructor pattern for AdminClient** — follow `__init__` config dict (lines 52–86):
```python
    def __init__(self, settings: KafkaMcpSettings) -> None:
        self._settings = settings

        conf: dict[str, object] = {
            "bootstrap.servers": settings.bootstrap_servers,
            "enable.auto.commit": False,
            "group.id": f"kafka-mcp-ro-{uuid4()}",
        }

        if settings.security_protocol != "PLAINTEXT":
            conf["security.protocol"] = settings.security_protocol

        if settings.sasl_mechanism:
            conf["sasl.mechanism"] = settings.sasl_mechanism
            if settings.sasl_username is not None:
                conf["sasl.username"] = settings.sasl_username
            if settings.sasl_password is not None:
                conf["sasl.password"] = settings.sasl_password.get_secret_value()

        self._consumer: Consumer = Consumer(conf)
```

**AdminClient should reuse the same config dict construction** (minus consumer-specific keys like `enable.auto.commit` and `group.id`). Create AdminClient inside `__init__` from the same broker/SASL config.

**Error handling pattern** — follow `get_watermark_offsets` (lines 167–190):
```python
    def get_watermark_offsets(
        self, topic: str, partition: int
    ) -> tuple[int, int]:
        try:
            low, high = self._consumer.get_watermark_offsets(
                topic, partition, timeout=_METADATA_TIMEOUT_SECONDS
            )
        except KafkaException as exc:
            code = None
            if exc.args and hasattr(exc.args[0], "code"):
                code = exc.args[0].code()
            if code in (
                KafkaError.UNKNOWN_TOPIC_OR_PART,
                KafkaError._UNKNOWN_TOPIC,
                KafkaError._UNKNOWN_PARTITION,
            ):
                raise TopicNotFoundError(topic) from exc
            raise
        return (low, high)
```

**Close pattern** — update `close()` (lines 458–465) to also close AdminClient:
```python
    def close(self) -> None:
        self._consumer.close()
```

**Key implementation detail:** `consumer_group_lag` should:
1. Call `AdminClient.list_consumer_group_offsets()` for committed offsets
2. Call `self.get_watermark_offsets()` per partition for end offsets
3. Compute `lag = end_offset - current_offset`
4. Return `list[LagRecord]` with `timestamp_utc = datetime.now(tz=timezone.utc)`
5. Handle no committed offset → `current_offset=0, lag=end_offset`
6. Handle empty group → return empty list

---

### `src/kafka_mcp/adapters/inbound/lib.py` — add `consumer_group_lag` to `KafkaClient` (facade, request-response)

**Analog:** `describe_topic` method in same file (lines 166–181)

**Imports pattern** (lines 29–41):
```python
from __future__ import annotations

from types import TracebackType
from typing import Any

from kafka_mcp.adapters.outbound.confluent_consumer import (
    ConfluentConsumerAdapter,
)
from kafka_mcp.config import KafkaMcpSettings
from kafka_mcp.domain.models import KafkaMessage, TopicInfo
from kafka_mcp.domain.search_service import TopicService
from kafka_mcp.ports.consumer import ConsumerPort
from kafka_mcp.ports.schema_registry import SchemaRegistryPort
```

**Facade method pattern** — follow `describe_topic` (lines 166–181):
```python
    def describe_topic(self, topic: str) -> TopicInfo:
        """Return detailed metadata for a single topic.

        Args:
            topic: Exact topic name to describe.

        Returns:
            :class:`kafka_mcp.domain.models.TopicInfo` with
            ``partition_count`` and a ``partitions`` list of
            :class:`kafka_mcp.domain.models.PartitionInfo` objects
            carrying ``earliest`` and ``latest`` offsets (Phase 1 SC-2).

        Raises:
            TopicNotFoundError: If the topic does not exist on the broker.
        """
        return self._service.describe_topic(topic)
```

**Key pattern:** For `consumer_group_lag`, the facade delegates directly to `self._consumer.consumer_group_lag(group, topics)` since this is a pass-through read-only query with no domain orchestration needed (no decode/search). No TopicService involvement needed — this is a direct port call, similar to how `list_topics` delegates to `self._service.list_topics()`.

**New method signature:**
```python
    def consumer_group_lag(
        self, group: str, topics: list[str] | None = None
    ) -> list[LagRecord]:
```

---

### `src/kafka_mcp/adapters/inbound/mcp_stdio.py` — register `consumer_group_lag` tool (adapter-inbound, request-response)

**Analog:** `describe_topic` tool registration in same file (lines 100–113)

**Tool registration pattern** (lines 100–113):
```python
    @app.tool(
        name="describe_topic",
        description=(
            "Return partition metadata and watermark offsets for a single Kafka topic. "
            "Returns earliest/latest offsets per partition."
        ),
        annotations=_READ_ONLY,
    )
    def describe_topic(topic: str) -> dict:  # noqa: D401
        """Describe a single topic by name."""
        try:
            return client.describe_topic(topic).model_dump()
        except TopicNotFoundError as exc:
            raise ValueError(f"Topic not found: {exc.topic!r}") from exc
```

**Serialization helper for LagRecord** — follow `_serialize_message` pattern (lines 43–64):
```python
def _serialize_message(msg: KafkaMessage) -> dict:
    data = msg.model_dump()
    data["raw"] = base64.b64encode(msg.raw).decode("ascii")
    if msg.raw_key is not None:
        data["raw_key"] = base64.b64encode(msg.raw_key).decode("ascii")
    if isinstance(data.get("timestamp_utc"), datetime):
        data["timestamp_utc"] = data["timestamp_utc"].isoformat()
    return data
```

**`LagRecord` serialization is simpler** — no raw bytes, just `model_dump()` with datetime→ISO conversion:
```python
def _serialize_lag_record(record: LagRecord) -> dict:
    data = record.model_dump()
    if isinstance(data.get("timestamp_utc"), datetime):
        data["timestamp_utc"] = data["timestamp_utc"].isoformat()
    return data
```

**New tool registration:**
```python
    @app.tool(
        name="consumer_group_lag",
        description=(
            "Report per-partition consumer lag (committed offset vs end offset) "
            "for a given consumer group. Read-only — no commits, no group joins."
        ),
        annotations=_READ_ONLY,
    )
    def consumer_group_lag(
        group: str, topics: list[str] | None = None
    ) -> list[dict]:
        return [_serialize_lag_record(r) for r in client.consumer_group_lag(group, topics)]
```

---

### `src/kafka_mcp/adapters/inbound/rest_api.py` — add `POST /tools/consumer_group_lag` (adapter-inbound, request-response)

**Analog:** `POST /tools/describe_topic` route in same file (lines 304–322)

**Request model pattern** — follow `DescribeTopicRequest` (lines 61–68):
```python
class DescribeTopicRequest(BaseModel):
    """Request body for POST /tools/describe_topic.

    ``topic`` must be a non-empty string — Pydantic enforces this by default
    (str fields reject None and non-string types automatically).
    """

    topic: str
```

**New request model:**
```python
class ConsumerGroupLagRequest(BaseModel):
    """Request body for POST /tools/consumer_group_lag."""

    group: str
    topics: list[str] | None = None
```

**Route handler pattern** — follow `_describe_topic` (lines 304–322):
```python
    @app.post("/tools/describe_topic")
    def _describe_topic(req: DescribeTopicRequest) -> dict:
        """Return partition metadata for a single topic."""
        try:
            info = client.describe_topic(req.topic)
            return {"result": info.model_dump()}
        except TopicNotFoundError as exc:
            raise HTTPException(
                status_code=404,
                detail={"error": "TopicNotFoundError", "topic": exc.topic},
            ) from exc
```

**New route:**
```python
    @app.post("/tools/consumer_group_lag")
    def _consumer_group_lag(req: ConsumerGroupLagRequest) -> dict:
        records = client.consumer_group_lag(req.group, req.topics)
        return {"result": [_serialize_lag_record(r) for r in records]}
```

**Also register in HTTP MCP tools** — follow the `_create_http_mcp_server` pattern (lines 131–236) which mirrors all tools from the stdio server.

**LagRecord serialization** — same `_serialize_lag_record` helper as in mcp_stdio (datetime→ISO, no bytes to base64):
```python
def _serialize_lag_record(record: LagRecord) -> dict:
    data = record.model_dump()
    if isinstance(data.get("timestamp_utc"), datetime):
        data["timestamp_utc"] = data["timestamp_utc"].isoformat()
    return data
```

---

### `src/kafka_mcp/adapters/inbound/cli.py` — add `consumer-group-lag` subcommand (adapter-inbound, request-response)

**Analog:** `describe-topic` subcommand in same file

**Parser pattern** — follow `describe-topic` parser registration (lines 82–96):
```python
    # describe-topic
    dt = subparsers.add_parser(
        "describe-topic",
        help="Show partition metadata and watermark offsets for a topic.",
    )
    dt.add_argument(
        "topic",
        help="Exact topic name to describe.",
    )
    dt.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output as JSON instead of table.",
    )
```

**New parser registration:**
```python
    # consumer-group-lag
    cgl = subparsers.add_parser(
        "consumer-group-lag",
        help="Report per-partition consumer lag for a consumer group.",
    )
    cgl.add_argument(
        "--group",
        required=True,
        help="Consumer group ID.",
    )
    cgl.add_argument(
        "--topics",
        default=None,
        help="Comma-separated list of topic names. Defaults to all committed topics.",
    )
    cgl.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output as JSON instead of table.",
    )
```

**Runner function pattern** — follow `run_describe_topic` (lines 219–251):
```python
def run_describe_topic(
    client: KafkaClient,
    topic: str,
    as_json: bool = False,
) -> None:
    try:
        info = client.describe_topic(topic)
    except TopicNotFoundError as exc:
        print(f"Error: topic '{exc.topic}' not found", file=sys.stderr)
        sys.exit(1)

    if as_json:
        print(orjson_dumps(info.model_dump()).decode())
        return

    # Human-readable partition table
    print(f"\nTopic: {info.name}  Partitions: {info.partition_count}\n")
    print(f"{'Partition':>10}  {'Leader':>8}  {'Earliest':>12}  {'Latest':>12}")
    print("-" * 50)
    for p in info.partitions:
        print(
            f"{p.id:>10}  {p.leader:>8}  {p.earliest:>12}  {p.latest:>12}"
        )
```

**Dispatch pattern** — follow `main()` dispatch (lines 459–488):
```python
        if ns.subcommand == "list-topics":
            run_list_topics(...)
        elif ns.subcommand == "describe-topic":
            run_describe_topic(client, ns.topic, as_json=ns.json)
        elif ns.subcommand == "search-messages":
            run_search_messages(...)
        elif ns.subcommand == "get-message":
            run_get_message(...)
        else:
            parser.print_help()
            sys.exit(1)
```

**Also update `server.py` dispatch** — add `"consumer-group-lag"` to the CLI subcommand set used in `server.main()`.

**CLI serialization** — follow `_serialize_message_for_cli` pattern (lines 254–276) but simpler since LagRecord has no raw bytes:
```python
def _serialize_lag_record_for_cli(record: LagRecord) -> dict:
    data = record.model_dump()
    if isinstance(data.get("timestamp_utc"), datetime):
        data["timestamp_utc"] = data["timestamp_utc"].isoformat()
    return data
```

**Tabular output** — lag records displayed as a table with columns: Group, Topic, Partition, Current, End, Lag.

---

### `tests/` — unit tests for all layers (test, request-response)

**Analog:** `tests/test_inbound.py` (MockKafkaClient) + `tests/test_adapters.py` (adapter mocks)

**MockKafkaClient pattern** — follow existing mock (test_inbound.py lines 51–105):
```python
class MockKafkaClient:
    """Minimal KafkaClient stand-in for adapter tests."""

    def close(self) -> None:
        """No-op close for test compatibility."""

    def list_topics(self, include_internal: bool = False) -> list[str]:
        topics = ["orders", "payments"]
        if include_internal:
            topics = ["__consumer_offsets"] + topics
        return topics

    def describe_topic(self, topic: str) -> TopicInfo:
        if topic == "payments":
            return TopicInfo(...)
        raise TopicNotFoundError(topic)

    def search_messages(self, key: str, **kwargs) -> list[KafkaMessage]:
        if key == "ORD-123":
            return [_SAMPLE_MSG]
        return []

    def get_message(self, topic: str, partition: int, offset: int) -> KafkaMessage:
        if topic == "orders" and partition == 0 and offset == 42:
            return _SAMPLE_MSG
        raise MessageNotFoundError(topic, partition, offset)
```

**Add to MockKafkaClient:**
```python
    def consumer_group_lag(
        self, group: str, topics: list[str] | None = None
    ) -> list[LagRecord]:
        if group == "my-group":
            return [_SAMPLE_LAG_RECORD]
        return []
```

**Adapter test pattern** — follow `TestConfluentConsumerAdapterWatermarks` (test_adapters.py lines 112–208):
```python
class TestConfluentConsumerAdapterWatermarks:
    def _make_adapter(self, mock_consumer: MagicMock) -> object:
        from kafka_mcp.adapters.outbound.confluent_consumer import ConfluentConsumerAdapter
        settings = _make_settings()
        with patch(
            "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
            return_value=mock_consumer,
        ):
            return ConfluentConsumerAdapter(settings)

    def test_get_watermark_offsets_returns_tuple(self) -> None:
        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 100)
        adapter = self._make_adapter(mock_consumer)
        low, high = adapter.get_watermark_offsets("payments", 0)
        assert low == 0
        assert high == 100
```

**FastAPI route test pattern** — follow existing POST route tests (test_inbound.py lines 113–176):
```python
def test_fastapi_describe_topic() -> None:
    from fastapi.testclient import TestClient
    from kafka_mcp.adapters.inbound.rest_api import create_app

    app = create_app(MockKafkaClient())
    client = TestClient(app)
    response = client.post("/tools/describe_topic", json={"topic": "payments"})
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["name"] == "payments"
```

**MCP tool test pattern** — follow existing readOnlyHint tests (test_inbound.py lines 319–346):
```python
def test_mcp_tools_have_read_only_hint() -> None:
    import asyncio
    from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

    server = create_mcp_server(MockKafkaClient())
    tools = asyncio.run(server.list_tools())
    tool_map = {t.name: t for t in tools}
    assert "describe_topic" in tool_map
    dt = tool_map["describe_topic"]
    assert dt.annotations is not None
    assert dt.annotations.readOnlyHint is True
```

**CLI test pattern** — follow existing CLI tests (test_inbound.py lines 212–274):
```python
def test_cli_describe_topic_table() -> None:
    from kafka_mcp.adapters.inbound.cli import run_describe_topic
    output = _capture_run(run_describe_topic, MockKafkaClient(), "payments", as_json=False)
    assert "Partition" in output
    assert "500" in output

def test_cli_describe_topic_json() -> None:
    import orjson
    from kafka_mcp.adapters.inbound.cli import run_describe_topic
    output = _capture_run(run_describe_topic, MockKafkaClient(), "payments", as_json=True)
    data = orjson.loads(output)
    assert data["name"] == "payments"
```

---

## Shared Patterns

### Read-Only Tool Annotations
**Source:** `src/kafka_mcp/adapters/inbound/mcp_stdio.py` line 40
**Apply to:** MCP stdio tool + HTTP MCP tool registration for `consumer_group_lag`
```python
_READ_ONLY = ToolAnnotations(readOnlyHint=True)
```

### Response Wrapper Convention
**Source:** `src/kafka_mcp/adapters/inbound/rest_api.py` lines 301–302
**Apply to:** FastAPI `POST /tools/consumer_group_lag` route
```python
return {"result": topics}   # always wrap in {"result": ...}
```

### LagRecord Serialization Helper
**Source:** Pattern derived from `_serialize_message` in all three inbound adapters
**Apply to:** All three inbound faces (MCP, REST, CLI)
```python
def _serialize_lag_record(record: LagRecord) -> dict:
    """Serialize a LagRecord to a JSON-safe dict."""
    data = record.model_dump()
    if isinstance(data.get("timestamp_utc"), datetime):
        data["timestamp_utc"] = data["timestamp_utc"].isoformat()
    return data
```
Note: LagRecord has no `bytes` fields, so no base64 encoding needed — simpler than `_serialize_message`.

### Error Handling — Domain Errors Mapped per Face
**Source:** Error handling in all inbound adapters
**Apply to:** `consumer_group_lag` handlers if needed (currently read-only query, likely no domain errors — empty group returns `[]`)

- **MCP:** Raise `ValueError` for domain errors
- **FastAPI:** Raise `HTTPException` with structured `detail` dict
- **CLI:** Print to stderr + `sys.exit(N)`

### Hexagonal Boundary — No Broker Imports in Domain/Ports
**Source:** Module docstrings in `domain/models.py`, `domain/errors.py`, `ports/consumer.py`
**Apply to:** `LagRecord` model and `ConsumerPort.consumer_group_lag` method

```python
# domain/models.py — no I/O or framework imports
# ports/consumer.py — no broker library imports
```

### 4-Face Symmetry
**Source:** Established across all existing capabilities (list_topics, describe_topic, search_messages, get_message)
**Apply to:** `consumer_group_lag` must be exposed identically across:
1. `KafkaClient.consumer_group_lag(group, topics)` → `list[LagRecord]`
2. MCP tool `consumer_group_lag` → `list[dict]`
3. `POST /tools/consumer_group_lag` → `{"result": [...]}`
4. `kafka-mcp consumer-group-lag --group GROUP [--topics T1,T2]` → table or JSON

### server.py CLI Dispatch
**Source:** `src/kafka_mcp/server.py` — routes CLI subcommands to `cli.main()`
**Apply to:** Must add `"consumer-group-lag"` to the subcommand set that routes to CLI

---

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| *(none)* | — | — | All files have exact analogs in the existing codebase |

Every file to be created/modified for Phase 5 has an exact role-and-data-flow analog in the existing codebase. The `consumer_group_lag` capability follows the identical 4-face pattern established by `describe_topic` (simple args → structured response, no streaming, read-only).

## Metadata

**Analog search scope:** `src/kafka_mcp/` (all modules), `tests/` (all test files)
**Files scanned:** 15 source files + 6 test files
**Pattern extraction date:** 2026-06-16
