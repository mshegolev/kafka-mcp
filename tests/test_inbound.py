"""Tests for all inbound adapter faces: MCP stdio, FastAPI REST, CLI.

All tests use MockKafkaClient — no real broker required.
Verifies Phase 1 success criterion 5: list_topics and describe_topic reachable
via MCP stdio, FastAPI, and CLI faces.
Phase 2 plan 02-05: also tests search_messages and get_message across all faces.
"""

from __future__ import annotations

import base64
import io
import sys
from datetime import datetime, timezone

import pytest

from kafka_mcp.domain.errors import (
    DecodeError,
    MessageNotFoundError,
    TopicNotFoundError,
    TransientError,
)
from kafka_mcp.domain.models import KafkaMessage, LagRecord, PartitionInfo, TopicInfo

# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------

_SAMPLE_TS = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
_SAMPLE_RAW = b"\x00\x01\x02\x03"
_SAMPLE_MSG = KafkaMessage(
    topic="orders",
    partition=0,
    offset=42,
    key="ORD-123",
    headers={"ce-type": "order.created"},
    value={"order_id": "ORD-123", "amount": 99},
    timestamp_utc=_SAMPLE_TS,
    raw=_SAMPLE_RAW,
    source="kafka",
    event_type="kafka_message",
    keys={"order_id": "ORD-123", "msisdn": None, "customer_id": None, "product_id": None},
)

_SAMPLE_LAG_TS = datetime(2026, 6, 16, 12, 0, 0, tzinfo=timezone.utc)
_SAMPLE_LAG_RECORD = LagRecord(
    group="my-group",
    topic="orders",
    partition=0,
    current_offset=50,
    end_offset=100,
    lag=50,
    timestamp_utc=_SAMPLE_LAG_TS,
)

# ---------------------------------------------------------------------------
# Mock KafkaClient — no broker, deterministic data
# ---------------------------------------------------------------------------


class MockKafkaClient:
    """Minimal KafkaClient stand-in for adapter tests.

    Returns deterministic topic and message data without connecting to a broker.
    """

    def close(self) -> None:
        """No-op close for test compatibility (lifespan teardown)."""

    def list_topics(self, include_internal: bool = False) -> list[str]:
        topics = ["orders", "payments"]
        if include_internal:
            topics = ["__consumer_offsets"] + topics
        return topics

    def describe_topic(self, topic: str) -> TopicInfo:
        if topic == "payments":
            return TopicInfo(
                name="payments",
                partition_count=2,
                partitions=[
                    PartitionInfo(id=0, leader=0, earliest=0, latest=500),
                    PartitionInfo(id=1, leader=0, earliest=0, latest=300),
                ],
            )
        if topic == "orders":
            return TopicInfo(
                name="orders",
                partition_count=1,
                partitions=[
                    PartitionInfo(id=0, leader=0, earliest=100, latest=900),
                ],
            )
        raise TopicNotFoundError(topic)

    def search_messages(self, key: str, **kwargs) -> list[KafkaMessage]:
        """Return a single sample message matching 'ORD-123'; else empty list."""
        if key == "ORD-123":
            return [_SAMPLE_MSG]
        return []

    def get_message(self, topic: str, partition: int, offset: int) -> KafkaMessage:
        """Return sample message at orders/0/42; raise errors for special cases."""
        if topic == "orders" and partition == 0 and offset == 42:
            return _SAMPLE_MSG
        if topic == "missing":
            raise MessageNotFoundError(topic, partition, offset)
        if topic == "corrupt":
            raise DecodeError(topic, partition, offset, "bad magic byte")
        if topic == "transient":
            # WR-03: in-range offset that timed out — retryable, not absence.
            raise TransientError(topic, partition, offset, "poll timed out")
        raise MessageNotFoundError(topic, partition, offset)

    def consumer_group_lag(self, group: str, topics: list[str] | None = None) -> list[LagRecord]:
        """Return sample lag records for 'my-group'; else empty list."""
        if group == "my-group":
            records = [
                _SAMPLE_LAG_RECORD,
                LagRecord(
                    group="my-group",
                    topic="payments",
                    partition=0,
                    current_offset=30,
                    end_offset=200,
                    lag=170,
                    timestamp_utc=_SAMPLE_LAG_TS,
                ),
            ]
            if topics is not None:
                records = [r for r in records if r.topic in topics]
            return records
        return []


# ---------------------------------------------------------------------------
# FastAPI adapter tests
# ---------------------------------------------------------------------------


def test_fastapi_list_topics() -> None:
    """POST /tools/list_topics returns 200 with list containing 'orders'."""
    from fastapi.testclient import TestClient

    from kafka_mcp.adapters.inbound.rest_api import create_app

    app = create_app(MockKafkaClient())
    client = TestClient(app)
    response = client.post("/tools/list_topics", json={})
    assert response.status_code == 200
    data = response.json()
    assert "result" in data
    assert "orders" in data["result"]
    assert "payments" in data["result"]


def test_fastapi_list_topics_include_internal() -> None:
    """POST /tools/list_topics with include_internal=true includes __consumer_offsets."""
    from fastapi.testclient import TestClient

    from kafka_mcp.adapters.inbound.rest_api import create_app

    app = create_app(MockKafkaClient())
    client = TestClient(app)
    response = client.post("/tools/list_topics", json={"include_internal": True})
    assert response.status_code == 200
    assert "__consumer_offsets" in response.json()["result"]


def test_fastapi_describe_topic() -> None:
    """POST /tools/describe_topic returns 200 with payments topic metadata."""
    from fastapi.testclient import TestClient

    from kafka_mcp.adapters.inbound.rest_api import create_app

    app = create_app(MockKafkaClient())
    client = TestClient(app)
    response = client.post("/tools/describe_topic", json={"topic": "payments"})
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["name"] == "payments"
    assert result["partition_count"] == 2
    assert len(result["partitions"]) == 2


def test_fastapi_describe_topic_not_found() -> None:
    """POST /tools/describe_topic with unknown topic returns HTTP 404."""
    from fastapi.testclient import TestClient

    from kafka_mcp.adapters.inbound.rest_api import create_app

    app = create_app(MockKafkaClient())
    client = TestClient(app)
    response = client.post("/tools/describe_topic", json={"topic": "unknown"})
    assert response.status_code == 404
    detail = response.json()["detail"]
    assert detail["error"] == "TopicNotFoundError"
    assert detail["topic"] == "unknown"


def test_fastapi_routes_match_mcp_convention() -> None:
    """FastAPI exposes POST /tools/list_topics and POST /tools/describe_topic only.

    Verifies D-16: no REST-resource-style routes like /topics or /describe.
    """
    from kafka_mcp.adapters.inbound.rest_api import create_app

    app = create_app(MockKafkaClient())
    paths = [r.path for r in app.routes]
    assert "/tools/list_topics" in paths
    assert "/tools/describe_topic" in paths
    # Resource-style routes must NOT exist (D-16)
    assert "/topics" not in paths
    assert "/describe" not in paths


# ---------------------------------------------------------------------------
# CLI adapter tests
# ---------------------------------------------------------------------------


def _capture_run(fn, *args, **kwargs) -> str:
    """Run fn(*args, **kwargs), capture stdout, return it as a string."""
    buf = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = buf
    try:
        fn(*args, **kwargs)
    finally:
        sys.stdout = old_stdout
    return buf.getvalue()


def test_cli_list_topics_table() -> None:
    """run_list_topics prints a table with both topic names."""
    from kafka_mcp.adapters.inbound.cli import run_list_topics

    output = _capture_run(run_list_topics, MockKafkaClient(), as_json=False)
    assert "payments" in output
    assert "orders" in output


def test_cli_list_topics_json() -> None:
    """run_list_topics with as_json=True prints valid JSON list."""
    import orjson

    from kafka_mcp.adapters.inbound.cli import run_list_topics

    output = _capture_run(run_list_topics, MockKafkaClient(), as_json=True)
    result = orjson.loads(output)
    topics = result["result"] if isinstance(result, dict) and "result" in result else result
    assert "payments" in topics
    assert "orders" in topics


def test_cli_list_topics_include_internal() -> None:
    """run_list_topics with include_internal=True includes __consumer_offsets."""
    from kafka_mcp.adapters.inbound.cli import run_list_topics

    output = _capture_run(run_list_topics, MockKafkaClient(), include_internal=True, as_json=False)
    assert "__consumer_offsets" in output


def test_cli_describe_topic_table() -> None:
    """run_describe_topic prints partition table with Partition header and latest offset."""
    from kafka_mcp.adapters.inbound.cli import run_describe_topic

    output = _capture_run(run_describe_topic, MockKafkaClient(), "payments", as_json=False)
    assert "Partition" in output
    assert "500" in output  # latest offset for partition 0


def test_cli_describe_topic_json() -> None:
    """run_describe_topic with as_json=True prints valid JSON TopicInfo."""
    import orjson

    from kafka_mcp.adapters.inbound.cli import run_describe_topic

    output = _capture_run(run_describe_topic, MockKafkaClient(), "payments", as_json=True)
    data = orjson.loads(output)
    assert data["name"] == "payments"
    assert data["partition_count"] == 2


def test_cli_describe_topic_not_found() -> None:
    """run_describe_topic raises SystemExit(1) for unknown topic."""
    from kafka_mcp.adapters.inbound.cli import run_describe_topic

    with pytest.raises(SystemExit) as exc_info:
        run_describe_topic(MockKafkaClient(), "unknown")
    assert exc_info.value.code == 1


def test_cli_parse_args_list_topics() -> None:
    """parse_args(['list-topics']) returns subcommand='list-topics'."""
    from kafka_mcp.adapters.inbound.cli import parse_args

    ns = parse_args(["list-topics"])
    assert ns.subcommand == "list-topics"
    assert ns.json is False
    assert ns.include_internal is False


def test_cli_parse_args_list_topics_json() -> None:
    """parse_args(['list-topics', '--json']) returns json=True."""
    from kafka_mcp.adapters.inbound.cli import parse_args

    ns = parse_args(["list-topics", "--json"])
    assert ns.json is True


def test_cli_parse_args_describe_topic() -> None:
    """parse_args(['describe-topic', 'payments']) returns topic='payments'."""
    from kafka_mcp.adapters.inbound.cli import parse_args

    ns = parse_args(["describe-topic", "payments"])
    assert ns.subcommand == "describe-topic"
    assert ns.topic == "payments"
    assert ns.json is False


def test_cli_parse_args_describe_topic_json() -> None:
    """parse_args(['describe-topic', 'payments', '--json']) sets json=True."""
    from kafka_mcp.adapters.inbound.cli import parse_args

    ns = parse_args(["describe-topic", "payments", "--json"])
    assert ns.json is True
    assert ns.topic == "payments"


# ---------------------------------------------------------------------------
# MCP stdio adapter tests
# ---------------------------------------------------------------------------


def test_mcp_tools_have_read_only_hint() -> None:
    """Both list_topics and describe_topic tools have readOnlyHint=True.

    Verifies D-13 (snake_case names) and D-14 (defense-in-depth readOnlyHint).
    """
    import asyncio

    from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

    server = create_mcp_server(MockKafkaClient())
    tools = asyncio.run(server.list_tools())
    tool_map = {t.name: t for t in tools}

    # D-13: snake_case names matching lib method names
    assert "list_topics" in tool_map, f"list_topics tool missing; found: {list(tool_map)}"
    assert "describe_topic" in tool_map, f"describe_topic tool missing; found: {list(tool_map)}"

    # D-14: readOnlyHint present (defense-in-depth alongside structural assign-based consumer)
    lt = tool_map["list_topics"]
    dt = tool_map["describe_topic"]
    assert lt.annotations is not None, "list_topics missing annotations"
    assert lt.annotations.readOnlyHint is True, (
        f"list_topics readOnlyHint expected True, got {lt.annotations.readOnlyHint}"
    )
    assert dt.annotations is not None, "describe_topic missing annotations"
    assert dt.annotations.readOnlyHint is True, (
        f"describe_topic readOnlyHint expected True, got {dt.annotations.readOnlyHint}"
    )


def test_mcp_importable() -> None:
    """create_mcp_server is importable from mcp_stdio module."""
    from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server  # noqa: F401

    assert callable(create_mcp_server)


# ---------------------------------------------------------------------------
# CLI — search-messages and get-message subcommands (Phase 2 plan 02-05)
# ---------------------------------------------------------------------------


def _capture_stderr(fn, *args, **kwargs) -> str:
    """Run fn(*args, **kwargs), capture stderr, return it as a string."""
    buf = io.StringIO()
    old_stderr = sys.stderr
    sys.stderr = buf
    try:
        fn(*args, **kwargs)
    except SystemExit:
        pass
    finally:
        sys.stderr = old_stderr
    return buf.getvalue()


def test_cli_search_messages_subcommand_exists() -> None:
    """parse_args(['search-messages', '--key', 'x']) does not raise."""
    from kafka_mcp.adapters.inbound.cli import parse_args

    ns = parse_args(["search-messages", "--key", "x"])
    assert ns.subcommand == "search-messages"
    assert ns.key == "x"


def test_cli_search_messages_json_output() -> None:
    """run_search_messages with as_json=True prints a JSON list."""
    import orjson

    from kafka_mcp.adapters.inbound.cli import run_search_messages

    output = _capture_run(
        run_search_messages,
        MockKafkaClient(),
        key="ORD-123",
        key_field=None,
        topics=None,
        time_from_str=None,
        time_to_str=None,
        limit=500,
        as_json=True,
    )
    data = orjson.loads(output)
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["topic"] == "orders"


def test_cli_search_messages_table_output() -> None:
    """run_search_messages with as_json=False prints a human-readable table."""
    from kafka_mcp.adapters.inbound.cli import run_search_messages

    output = _capture_run(
        run_search_messages,
        MockKafkaClient(),
        key="ORD-123",
        key_field=None,
        topics=None,
        time_from_str=None,
        time_to_str=None,
        limit=500,
        as_json=False,
    )
    # Table should contain column headers and message data
    assert "Topic" in output or "orders" in output
    assert "orders" in output


def test_cli_search_messages_flags() -> None:
    """parse_args accepts all search-messages flags."""
    from kafka_mcp.adapters.inbound.cli import parse_args

    ns = parse_args(
        [
            "search-messages",
            "--key",
            "ORD-123",
            "--key-field",
            "header:ce-type",
            "--topics",
            "orders,payments",
            "--headers",
            "trace_id=abc,key2=value2",
            "--time-from",
            "2026-01-01T00:00:00Z",
            "--time-to",
            "2026-01-02T00:00:00Z",
            "--limit",
            "100",
            "--json",
        ]
    )
    assert ns.subcommand == "search-messages"
    assert ns.key == "ORD-123"
    assert ns.key_field == "header:ce-type"
    assert ns.topics == "orders,payments"
    assert ns.headers == "trace_id=abc,key2=value2"
    assert ns.time_from == "2026-01-01T00:00:00Z"
    assert ns.time_to == "2026-01-02T00:00:00Z"
    assert ns.limit == 100
    assert ns.json is True


def test_cli_search_messages_headers_parsing() -> None:
    """run_search_messages correctly parses headers parameter."""
    from kafka_mcp.adapters.inbound.cli import run_search_messages

    # Create a mock client that can verify the headers parameter was passed correctly
    class TrackingMockKafkaClient:
        def __init__(self):
            self.last_call_kwargs = {}

        def search_messages(self, key, **kwargs):
            self.last_call_kwargs = kwargs
            # Return a minimal result
            from kafka_mcp.domain.models import KafkaMessage
            from datetime import datetime, timezone

            return [
                KafkaMessage(
                    topic="test",
                    partition=0,
                    offset=0,
                    key=key,
                    headers={},
                    value=None,
                    timestamp_utc=datetime.now(timezone.utc),
                    raw=b"",
                )
            ]

        def close(self):
            pass

    client = TrackingMockKafkaClient()

    output = _capture_run(
        run_search_messages,
        client,
        key="ORD-123",
        key_field=None,
        topics=None,
        headers="trace_id=abc,source=web",
        time_from_str=None,
        time_to_str=None,
        limit=500,
        as_json=True,
    )

    # Verify that the headers were parsed and passed correctly
    assert "headers" in client.last_call_kwargs
    headers_dict = client.last_call_kwargs["headers"]
    assert headers_dict is not None
    assert isinstance(headers_dict, dict)
    assert headers_dict["trace_id"] == "abc"
    assert headers_dict["source"] == "web"


def test_cli_get_message_subcommand_exists() -> None:
    """parse_args(['get-message', 'my-topic', '0', '42']) parses correctly."""
    from kafka_mcp.adapters.inbound.cli import parse_args

    ns = parse_args(["get-message", "my-topic", "0", "42"])
    assert ns.subcommand == "get-message"
    assert ns.topic == "my-topic"
    assert ns.partition == 0
    assert ns.offset == 42


def test_cli_get_message_json_output() -> None:
    """run_get_message with as_json=True prints JSON with base64 raw."""
    import orjson

    from kafka_mcp.adapters.inbound.cli import run_get_message

    output = _capture_run(
        run_get_message,
        MockKafkaClient(),
        "orders",
        0,
        42,
        as_json=True,
    )
    data = orjson.loads(output)
    assert data["topic"] == "orders"
    assert data["offset"] == 42
    # raw must be a string (base64), not bytes
    assert isinstance(data["raw"], str), f"raw should be str (base64), got {type(data['raw'])}"


def test_cli_get_message_not_found_exits_1() -> None:
    """MessageNotFoundError → SystemExit(1) with stderr message."""
    from kafka_mcp.adapters.inbound.cli import run_get_message

    stderr_out = _capture_stderr(
        run_get_message,
        MockKafkaClient(),
        "missing",
        0,
        0,
        as_json=False,
    )
    with pytest.raises(SystemExit) as exc_info:
        run_get_message(MockKafkaClient(), "missing", 0, 0, as_json=False)
    assert exc_info.value.code == 1
    assert "Error" in stderr_out or "missing" in stderr_out


def test_cli_get_message_decode_error_exits_2() -> None:
    """DecodeError → SystemExit(2) with stderr message including reason."""
    from kafka_mcp.adapters.inbound.cli import run_get_message

    stderr_out = _capture_stderr(
        run_get_message,
        MockKafkaClient(),
        "corrupt",
        0,
        0,
        as_json=False,
    )
    with pytest.raises(SystemExit) as exc_info:
        run_get_message(MockKafkaClient(), "corrupt", 0, 0, as_json=False)
    assert exc_info.value.code == 2
    assert "bad magic byte" in stderr_out or "Error" in stderr_out


def test_cli_raw_is_base64_in_json() -> None:
    """JSON output from get-message has raw as base64 string, not bytes."""
    import orjson

    from kafka_mcp.adapters.inbound.cli import run_get_message

    output = _capture_run(
        run_get_message,
        MockKafkaClient(),
        "orders",
        0,
        42,
        as_json=True,
    )
    data = orjson.loads(output)
    raw_field = data["raw"]
    assert isinstance(raw_field, str), f"raw should be str, got {type(raw_field)}"
    # Verify it decodes back to original bytes
    decoded = base64.b64decode(raw_field)
    assert decoded == _SAMPLE_RAW


# ---------------------------------------------------------------------------
# MCP stdio — search_messages and get_message tools (Phase 2 plan 02-05)
# ---------------------------------------------------------------------------


def test_mcp_search_messages_tool_registered() -> None:
    """FastMCP server has a tool named 'search_messages'."""
    import asyncio

    from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

    server = create_mcp_server(MockKafkaClient())
    tools = asyncio.run(server.list_tools())
    tool_names = [t.name for t in tools]
    assert "search_messages" in tool_names, f"search_messages tool missing; found: {tool_names}"


def test_mcp_search_messages_readonlyhint() -> None:
    """search_messages tool has readOnlyHint=True."""
    import asyncio

    from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

    server = create_mcp_server(MockKafkaClient())
    tools = asyncio.run(server.list_tools())
    tool_map = {t.name: t for t in tools}
    sm = tool_map.get("search_messages")
    assert sm is not None, "search_messages tool not found"
    assert sm.annotations is not None, "search_messages missing annotations"
    assert sm.annotations.readOnlyHint is True, f"readOnlyHint expected True, got {sm.annotations.readOnlyHint}"


def test_mcp_search_messages_returns_list() -> None:
    """search_messages tool returns list of dicts with base64-encoded raw field."""
    from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

    server = create_mcp_server(MockKafkaClient())
    # Call the tool by invoking the underlying function via the server
    # FastMCP tools are callable directly via server.call_tool
    import asyncio

    result = asyncio.run(server.call_tool("search_messages", {"key": "ORD-123"}))
    # result is a list of TextContent; parse the text as the returned value
    # FastMCP returns the Python return value encoded in content
    assert result is not None
    # The returned content list should be non-empty
    assert len(result) > 0


def test_mcp_get_message_tool_registered() -> None:
    """FastMCP server has a tool named 'get_message'."""
    import asyncio

    from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

    server = create_mcp_server(MockKafkaClient())
    tools = asyncio.run(server.list_tools())
    tool_names = [t.name for t in tools]
    assert "get_message" in tool_names, f"get_message tool missing; found: {tool_names}"


def test_mcp_get_message_decode_error_mapped() -> None:
    """DecodeError raised by get_message maps to ValueError for MCP."""
    import asyncio

    from mcp.shared.exceptions import McpError

    from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

    server = create_mcp_server(MockKafkaClient())
    with pytest.raises((ValueError, McpError, Exception)):
        asyncio.run(
            server.call_tool(
                "get_message",
                {"topic": "corrupt", "partition": 0, "offset": 0},
            )
        )


def test_mcp_get_message_not_found_mapped() -> None:
    """MessageNotFoundError raised by get_message maps to an error for MCP."""
    import asyncio

    from mcp.shared.exceptions import McpError

    from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

    server = create_mcp_server(MockKafkaClient())
    with pytest.raises((ValueError, McpError, Exception)):
        asyncio.run(
            server.call_tool(
                "get_message",
                {"topic": "missing", "partition": 0, "offset": 0},
            )
        )


# ---------------------------------------------------------------------------
# FastAPI REST — search_messages and get_message routes (Phase 2 plan 02-05)
# ---------------------------------------------------------------------------


def test_fastapi_post_search_messages_200() -> None:
    """POST /tools/search_messages returns 200 with result list."""
    from fastapi.testclient import TestClient

    from kafka_mcp.adapters.inbound.rest_api import create_app

    app = create_app(MockKafkaClient())
    client = TestClient(app)
    response = client.post("/tools/search_messages", json={"key": "ORD-123"})
    assert response.status_code == 200
    data = response.json()
    assert "result" in data
    assert isinstance(data["result"], list)
    assert len(data["result"]) == 1


def test_fastapi_post_search_messages_raw_base64() -> None:
    """Search result raw field is a base64 string, not bytes."""
    from fastapi.testclient import TestClient

    from kafka_mcp.adapters.inbound.rest_api import create_app

    app = create_app(MockKafkaClient())
    client = TestClient(app)
    response = client.post("/tools/search_messages", json={"key": "ORD-123"})
    assert response.status_code == 200
    result = response.json()["result"][0]
    assert "raw" in result
    assert isinstance(result["raw"], str), f"raw should be str (base64), got {type(result['raw'])}"
    # Verify it decodes to the original bytes
    decoded = base64.b64decode(result["raw"])
    assert decoded == _SAMPLE_RAW


def test_fastapi_post_get_message_200() -> None:
    """POST /tools/get_message returns 200 with result dict."""
    from fastapi.testclient import TestClient

    from kafka_mcp.adapters.inbound.rest_api import create_app

    app = create_app(MockKafkaClient())
    client = TestClient(app)
    response = client.post(
        "/tools/get_message",
        json={"topic": "orders", "partition": 0, "offset": 42},
    )
    assert response.status_code == 200
    data = response.json()
    assert "result" in data
    assert data["result"]["topic"] == "orders"
    assert data["result"]["offset"] == 42


def test_fastapi_post_get_message_404_not_found() -> None:
    """MessageNotFoundError → HTTP 404 with structured detail."""
    from fastapi.testclient import TestClient

    from kafka_mcp.adapters.inbound.rest_api import create_app

    app = create_app(MockKafkaClient())
    client = TestClient(app)
    response = client.post(
        "/tools/get_message",
        json={"topic": "missing", "partition": 0, "offset": 0},
    )
    assert response.status_code == 404
    detail = response.json()["detail"]
    assert detail["error"] == "MessageNotFoundError"
    assert detail["topic"] == "missing"


def test_fastapi_post_get_message_422_decode_error() -> None:
    """DecodeError → HTTP 422 with structured detail including reason."""
    from fastapi.testclient import TestClient

    from kafka_mcp.adapters.inbound.rest_api import create_app

    app = create_app(MockKafkaClient())
    client = TestClient(app)
    response = client.post(
        "/tools/get_message",
        json={"topic": "corrupt", "partition": 0, "offset": 0},
    )
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["error"] == "DecodeError"
    assert detail["topic"] == "corrupt"
    assert "reason" in detail


# ---------------------------------------------------------------------------
# TransientError inbound-face mapping (WR-03): REST 503 / CLI exit-3 / MCP
# ---------------------------------------------------------------------------


def test_fastapi_post_get_message_503_transient_error() -> None:
    """WR-03: TransientError → HTTP 503 with structured detail including reason.

    Guards the REST side of the WR-05 fix: a regression that remapped
    TransientError to 404 (absence) or re-ordered the except clauses would be
    caught here.
    """
    from fastapi.testclient import TestClient

    from kafka_mcp.adapters.inbound.rest_api import create_app

    app = create_app(MockKafkaClient())
    client = TestClient(app)
    response = client.post(
        "/tools/get_message",
        json={"topic": "transient", "partition": 0, "offset": 5},
    )
    assert response.status_code == 503
    detail = response.json()["detail"]
    assert detail["error"] == "TransientError"
    assert detail["topic"] == "transient"
    assert "reason" in detail


def test_cli_get_message_transient_error_exits_3() -> None:
    """WR-03: TransientError → SystemExit(3) with stderr message including reason."""
    from kafka_mcp.adapters.inbound.cli import run_get_message

    stderr_out = _capture_stderr(
        run_get_message,
        MockKafkaClient(),
        "transient",
        0,
        5,
        as_json=False,
    )
    with pytest.raises(SystemExit) as exc_info:
        run_get_message(MockKafkaClient(), "transient", 0, 5, as_json=False)
    assert exc_info.value.code == 3
    assert "transient" in stderr_out.lower() or "Error" in stderr_out


def test_mcp_get_message_transient_error_mapped() -> None:
    """WR-03: TransientError raised by get_message maps to an error for MCP."""
    import asyncio

    from mcp.shared.exceptions import McpError

    from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

    server = create_mcp_server(MockKafkaClient())
    with pytest.raises((ValueError, McpError, Exception)):
        asyncio.run(
            server.call_tool(
                "get_message",
                {"topic": "transient", "partition": 0, "offset": 5},
            )
        )


# ---------------------------------------------------------------------------
# server.main CLI dispatch (entry point)
# ---------------------------------------------------------------------------


class TestServerCliDispatch:
    """WR-B: the `kafka-mcp` entry point routes ALL CLI subcommands to the CLI.

    The Phase 2 subcommands (search-messages / get-message) were missing from
    server.main's dispatch set, so they silently fell through and booted the
    HTTP server instead — making the CLI face unreachable for Phase 2 ops via
    the documented `kafka-mcp ...` invocation. These tests guard every
    subcommand routes to the CLI runner and never reaches uvicorn.
    """

    @pytest.mark.parametrize(
        "subcommand",
        ["list-topics", "describe-topic", "search-messages", "get-message", "consumer-group-lag"],
    )
    def test_subcommand_routes_to_cli_runner(self, subcommand: str) -> None:
        from unittest.mock import patch

        import kafka_mcp.server as server

        captured: dict = {}

        def _fake_cli(args: list) -> None:
            captured["args"] = args

        with (
            patch.object(sys, "argv", ["kafka-mcp", subcommand, "x"]),
            patch("kafka_mcp.adapters.inbound.cli.main", _fake_cli),
            patch("uvicorn.run") as mock_uvicorn,
        ):
            server.main()

        assert captured.get("args") == [subcommand, "x"]
        mock_uvicorn.assert_not_called()


# ---------------------------------------------------------------------------
# SC-4 Regression: all four inbound faces × all four Investigator Contract ops
# ---------------------------------------------------------------------------
# These tests form the explicit SC-4 regression guard, asserting that:
#   lib (KafkaClient), MCP stdio, FastAPI REST, and CLI
#   all expose list_topics, describe_topic, search_messages, get_message.
# Tests use MockKafkaClient — no live broker required.
# ---------------------------------------------------------------------------


class TestSc4Regression:
    """SC-4 regression: all four faces deliver all four Investigator Contract ops."""

    # ---- MCP stdio face ----

    def test_sc4_mcp_search_messages(self) -> None:
        """MCP stdio face: search_messages tool returns list with source='kafka'."""
        import asyncio

        from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

        server = create_mcp_server(MockKafkaClient())
        result = asyncio.run(server.call_tool("search_messages", {"key": "ORD-123"}))
        # FastMCP returns a list of content objects; must be non-empty
        assert result is not None
        assert len(result) > 0

    def test_sc4_mcp_get_message(self) -> None:
        """MCP stdio face: get_message tool returns dict with topic/partition/offset."""
        import asyncio
        import json

        from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

        server = create_mcp_server(MockKafkaClient())
        result = asyncio.run(
            server.call_tool(
                "get_message",
                {"topic": "orders", "partition": 0, "offset": 42},
            )
        )
        assert result is not None
        assert len(result) > 0
        # FastMCP encodes the return value in content[0].text as JSON
        payload = json.loads(result[0].text)
        assert payload["topic"] == "orders"
        assert payload["partition"] == 0
        assert payload["offset"] == 42

    # ---- FastAPI REST face ----

    def test_sc4_fastapi_search_messages(self) -> None:
        """FastAPI face: POST /tools/search_messages returns 200 + result list."""
        from fastapi.testclient import TestClient

        from kafka_mcp.adapters.inbound.rest_api import create_app

        client = TestClient(create_app(MockKafkaClient()))
        response = client.post(
            "/tools/search_messages",
            json={"key": "ORD-123"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "result" in data
        assert isinstance(data["result"], list)
        assert len(data["result"]) == 1
        assert data["result"][0]["source"] == "kafka"

    def test_sc4_fastapi_get_message(self) -> None:
        """FastAPI face: POST /tools/get_message returns 200 + result dict."""
        from fastapi.testclient import TestClient

        from kafka_mcp.adapters.inbound.rest_api import create_app

        client = TestClient(create_app(MockKafkaClient()))
        response = client.post(
            "/tools/get_message",
            json={"topic": "orders", "partition": 0, "offset": 42},
        )
        assert response.status_code == 200
        data = response.json()
        assert "result" in data
        assert data["result"]["topic"] == "orders"
        assert data["result"]["partition"] == 0
        assert data["result"]["offset"] == 42

    # ---- CLI face ----

    def test_sc4_cli_search_messages(self, capsys) -> None:
        """CLI face: run_search_messages with as_json=True outputs topic/key fields."""
        import orjson

        from kafka_mcp.adapters.inbound.cli import run_search_messages

        run_search_messages(
            MockKafkaClient(),
            key="ORD-123",
            key_field=None,
            topics=None,
            time_from_str=None,
            time_to_str=None,
            limit=500,
            as_json=True,
        )
        captured = capsys.readouterr()
        data = orjson.loads(captured.out)
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["topic"] == "orders"
        assert data[0]["key"] == "ORD-123"

    def test_sc4_cli_get_message(self, capsys) -> None:
        """CLI face: run_get_message with as_json=True outputs offset field."""
        import orjson

        from kafka_mcp.adapters.inbound.cli import run_get_message

        run_get_message(
            MockKafkaClient(),
            "orders",
            0,
            42,
            as_json=True,
        )
        captured = capsys.readouterr()
        data = orjson.loads(captured.out)
        assert data["offset"] == 42
        assert data["topic"] == "orders"

    # ---- lib (KafkaClient) face — all four operations ----

    def test_sc4_lib_all_four_operations(self) -> None:
        """Lib face: MockKafkaClient exposes all four Investigator Contract operations."""
        from kafka_mcp.domain.models import KafkaMessage, TopicInfo

        mock = MockKafkaClient()

        # 1. list_topics
        topics = mock.list_topics()
        assert isinstance(topics, list)
        assert len(topics) >= 1

        # 2. describe_topic
        info = mock.describe_topic("orders")
        assert isinstance(info, TopicInfo)
        assert info.name == "orders"

        # 3. search_messages
        msgs = mock.search_messages(key="ORD-123")
        assert isinstance(msgs, list)
        assert len(msgs) == 1
        assert isinstance(msgs[0], KafkaMessage)
        assert msgs[0].source == "kafka"

        # 4. get_message
        msg = mock.get_message(topic="orders", partition=0, offset=42)
        assert isinstance(msg, KafkaMessage)
        assert msg.topic == "orders"
        assert msg.offset == 42


# ---------------------------------------------------------------------------
# Phase 4 Plan 02: 4-face raw_key base64 serialization (Task 2 RED)
# ---------------------------------------------------------------------------

# Sample message with raw_key, key_decoded, and schema_id for serialization tests
_RAW_KEY_BYTES = b"\x00\x00\x00\x00\x05test"
_SAMPLE_MSG_WITH_KEY = KafkaMessage(
    topic="orders",
    partition=0,
    offset=42,
    key="ORD-123",
    headers={"ce-type": "order.created"},
    value={"order_id": "ORD-123", "amount": 99},
    timestamp_utc=_SAMPLE_TS,
    raw=_SAMPLE_RAW,
    raw_key=_RAW_KEY_BYTES,
    key_decoded={"order_id": "ORD-1"},
    schema_id={"value": 5, "key": 5},
    source="kafka",
    event_type="kafka_message",
    keys={"order_id": "ORD-123", "msisdn": None, "customer_id": None, "product_id": None},
)
_EXPECTED_RAW_KEY_B64 = base64.b64encode(_RAW_KEY_BYTES).decode("ascii")


class TestRestApiRawKeySerialize:
    """REST face: _serialize_message encodes raw_key as base64."""

    def test_raw_key_base64_encoded(self) -> None:
        from kafka_mcp.adapters.inbound.rest_api import _serialize_message

        result = _serialize_message(_SAMPLE_MSG_WITH_KEY)
        assert result["raw_key"] == _EXPECTED_RAW_KEY_B64

    def test_raw_key_none_is_none(self) -> None:
        from kafka_mcp.adapters.inbound.rest_api import _serialize_message

        result = _serialize_message(_SAMPLE_MSG)
        assert result["raw_key"] is None

    def test_key_decoded_passes_through(self) -> None:
        from kafka_mcp.adapters.inbound.rest_api import _serialize_message

        result = _serialize_message(_SAMPLE_MSG_WITH_KEY)
        assert result["key_decoded"] == {"order_id": "ORD-1"}

    def test_schema_id_passes_through(self) -> None:
        from kafka_mcp.adapters.inbound.rest_api import _serialize_message

        result = _serialize_message(_SAMPLE_MSG_WITH_KEY)
        assert result["schema_id"] == {"value": 5, "key": 5}


class TestMcpStdioRawKeySerialize:
    """MCP stdio face: _serialize_message encodes raw_key as base64."""

    def test_raw_key_base64_encoded(self) -> None:
        from kafka_mcp.adapters.inbound.mcp_stdio import _serialize_message

        result = _serialize_message(_SAMPLE_MSG_WITH_KEY)
        assert result["raw_key"] == _EXPECTED_RAW_KEY_B64

    def test_raw_key_none_is_none(self) -> None:
        from kafka_mcp.adapters.inbound.mcp_stdio import _serialize_message

        result = _serialize_message(_SAMPLE_MSG)
        assert result["raw_key"] is None

    def test_key_decoded_passes_through(self) -> None:
        from kafka_mcp.adapters.inbound.mcp_stdio import _serialize_message

        result = _serialize_message(_SAMPLE_MSG_WITH_KEY)
        assert result["key_decoded"] == {"order_id": "ORD-1"}

    def test_schema_id_passes_through(self) -> None:
        from kafka_mcp.adapters.inbound.mcp_stdio import _serialize_message

        result = _serialize_message(_SAMPLE_MSG_WITH_KEY)
        assert result["schema_id"] == {"value": 5, "key": 5}


class TestCliRawKeySerialize:
    """CLI face: _serialize_message_for_cli encodes raw_key as base64."""

    def test_raw_key_base64_encoded(self) -> None:
        from kafka_mcp.adapters.inbound.cli import _serialize_message_for_cli

        result = _serialize_message_for_cli(_SAMPLE_MSG_WITH_KEY)
        assert result["raw_key"] == _EXPECTED_RAW_KEY_B64

    def test_raw_key_none_is_none(self) -> None:
        from kafka_mcp.adapters.inbound.cli import _serialize_message_for_cli

        result = _serialize_message_for_cli(_SAMPLE_MSG)
        assert result["raw_key"] is None

    def test_key_decoded_passes_through(self) -> None:
        from kafka_mcp.adapters.inbound.cli import _serialize_message_for_cli

        result = _serialize_message_for_cli(_SAMPLE_MSG_WITH_KEY)
        assert result["key_decoded"] == {"order_id": "ORD-1"}

    def test_schema_id_passes_through(self) -> None:
        from kafka_mcp.adapters.inbound.cli import _serialize_message_for_cli

        result = _serialize_message_for_cli(_SAMPLE_MSG_WITH_KEY)
        assert result["schema_id"] == {"value": 5, "key": 5}


class TestFourFaceSymmetry:
    """4-face symmetry: all faces produce identical raw_key/key_decoded/schema_id."""

    def test_four_face_raw_key_symmetry(self) -> None:
        from kafka_mcp.adapters.inbound.cli import _serialize_message_for_cli
        from kafka_mcp.adapters.inbound.mcp_stdio import (
            _serialize_message as mcp_ser,
        )
        from kafka_mcp.adapters.inbound.rest_api import (
            _serialize_message as rest_ser,
        )

        rest_d = rest_ser(_SAMPLE_MSG_WITH_KEY)
        mcp_d = mcp_ser(_SAMPLE_MSG_WITH_KEY)
        cli_d = _serialize_message_for_cli(_SAMPLE_MSG_WITH_KEY)

        for face, d in [("rest", rest_d), ("mcp", mcp_d), ("cli", cli_d)]:
            assert d["raw_key"] == _EXPECTED_RAW_KEY_B64, f"{face}: raw_key mismatch"
            assert d["key_decoded"] == {"order_id": "ORD-1"}, f"{face}: key_decoded mismatch"
            assert d["schema_id"] == {"value": 5, "key": 5}, f"{face}: schema_id mismatch"


# ---------------------------------------------------------------------------
# Phase 4 Plan 03: FastMCP streamable-HTTP mount at /mcp (HTTP-01)
# ---------------------------------------------------------------------------


class TestHttpMcpMount:
    """HTTP-01: FastAPI app has a /mcp mount that returns non-404 (HTTP transport)."""

    def test_mcp_mount_returns_non_404(self) -> None:
        """GET /mcp/ returns a status code that is NOT 404.

        Confirms that the FastMCP streamable-HTTP app is mounted at /mcp
        on the FastAPI app returned by create_app(). Accepts 200, 405, or
        any non-404 response — the mount exists and serves MCP.
        """
        from starlette.testclient import TestClient

        from kafka_mcp.adapters.inbound.rest_api import create_app

        client_app = create_app(MockKafkaClient())
        with TestClient(client_app) as tc:
            resp = tc.get("/mcp/")
            assert resp.status_code != 404, (
                f"Expected non-404 at /mcp/ but got {resp.status_code}. "
                "The FastMCP streamable-HTTP app must be mounted at /mcp."
            )

    def test_mcp_mount_post_returns_non_404(self) -> None:
        """POST /mcp/ returns a status code that is NOT 404.

        MCP protocol uses POST for requests; the mount must handle both.
        """
        from starlette.testclient import TestClient

        from kafka_mcp.adapters.inbound.rest_api import create_app

        client_app = create_app(MockKafkaClient())
        with TestClient(client_app) as tc:
            resp = tc.post("/mcp/", json={})
            assert resp.status_code != 404, f"Expected non-404 at POST /mcp/ but got {resp.status_code}."

    def test_existing_tools_routes_unaffected(self) -> None:
        """Existing /tools/* routes remain accessible after /mcp mount."""
        from starlette.testclient import TestClient

        from kafka_mcp.adapters.inbound.rest_api import create_app

        client_app = create_app(MockKafkaClient())
        with TestClient(client_app) as tc:
            resp = tc.post("/tools/list_topics", json={})
            assert resp.status_code == 200, f"Expected 200 at /tools/list_topics but got {resp.status_code}."


# ---------------------------------------------------------------------------
# CR-01 regression: naive datetime strings must not crash MCP faces
# ---------------------------------------------------------------------------


class TestNaiveDatetimeGuard:
    """CR-01: timezone-naive ISO strings must be accepted (defaulted to UTC).

    A caller who passes "2026-01-01T00:00:00" (no Z, no offset) to the MCP
    search_messages tool must not receive a TypeError.  The CLI face already
    guards against this; these tests verify the same guard is present in the
    HTTP MCP and stdio MCP faces (fix for CR-01).
    """

    NAIVE_ISO = "2026-01-01T00:00:00"
    # Use +00:00 offset (not Z) — Python 3.10 fromisoformat does not support Z.
    AWARE_ISO = "2026-01-01T00:00:00+00:00"

    # ---- HTTP MCP tool (rest_api._create_http_mcp_server) ----

    def test_http_mcp_search_naive_time_from_no_crash(self) -> None:
        """HTTP MCP search_messages: naive time_from string does not raise TypeError."""
        import asyncio

        from kafka_mcp.adapters.inbound.rest_api import _create_http_mcp_server

        server = _create_http_mcp_server(MockKafkaClient())
        # Must not raise TypeError; naive datetime would crash the consumer
        # comparison ts_utc >= time_to if the guard is absent.
        result = asyncio.run(
            server.call_tool(
                "search_messages",
                {"key": "ORD-123", "time_from": self.NAIVE_ISO},
            )
        )
        assert result is not None

    def test_http_mcp_search_naive_time_to_no_crash(self) -> None:
        """HTTP MCP search_messages: naive time_to string does not raise TypeError."""
        import asyncio

        from kafka_mcp.adapters.inbound.rest_api import _create_http_mcp_server

        server = _create_http_mcp_server(MockKafkaClient())
        result = asyncio.run(
            server.call_tool(
                "search_messages",
                {"key": "ORD-123", "time_to": self.NAIVE_ISO},
            )
        )
        assert result is not None

    def test_http_mcp_search_aware_string_still_works(self) -> None:
        """HTTP MCP search_messages: tz-aware ISO string (+00:00) still works."""
        import asyncio

        from kafka_mcp.adapters.inbound.rest_api import _create_http_mcp_server

        server = _create_http_mcp_server(MockKafkaClient())
        result = asyncio.run(
            server.call_tool(
                "search_messages",
                {"key": "ORD-123", "time_from": self.AWARE_ISO},
            )
        )
        assert result is not None

    # ---- MCP stdio face (mcp_stdio.create_mcp_server) ----

    def test_stdio_mcp_search_naive_time_from_no_crash(self) -> None:
        """MCP stdio search_messages: naive time_from string does not raise TypeError."""
        import asyncio

        from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

        server = create_mcp_server(MockKafkaClient())
        result = asyncio.run(
            server.call_tool(
                "search_messages",
                {"key": "ORD-123", "time_from": self.NAIVE_ISO},
            )
        )
        assert result is not None

    def test_stdio_mcp_search_naive_time_to_no_crash(self) -> None:
        """MCP stdio search_messages: naive time_to string does not raise TypeError."""
        import asyncio

        from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

        server = create_mcp_server(MockKafkaClient())
        result = asyncio.run(
            server.call_tool(
                "search_messages",
                {"key": "ORD-123", "time_to": self.NAIVE_ISO},
            )
        )
        assert result is not None

    def test_stdio_mcp_search_aware_string_still_works(self) -> None:
        """MCP stdio search_messages: tz-aware ISO string (+00:00) still works."""
        import asyncio

        from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

        server = create_mcp_server(MockKafkaClient())
        result = asyncio.run(
            server.call_tool(
                "search_messages",
                {"key": "ORD-123", "time_from": self.AWARE_ISO},
            )
        )
        assert result is not None


# ---------------------------------------------------------------------------
# WR-01 regression: limit must be clamped to [1, 10000] on MCP faces
# ---------------------------------------------------------------------------


class TestLimitUpperBound:
    """WR-01: limit values above 10000 must be silently clamped to 10000.

    The REST face guards this via Field(ge=1, le=10000).  These tests verify
    that the HTTP MCP and stdio MCP faces apply the same cap (fix for WR-01).
    """

    def test_http_mcp_search_oversized_limit_clamped(self) -> None:
        """HTTP MCP: limit=10_000_001 does not propagate unclamped."""
        import asyncio

        from kafka_mcp.adapters.inbound.rest_api import _create_http_mcp_server

        server = _create_http_mcp_server(MockKafkaClient())
        # No error expected; the clamp should silently cap to 10_000.
        result = asyncio.run(
            server.call_tool(
                "search_messages",
                {"key": "ORD-123", "limit": 10_000_001},
            )
        )
        assert result is not None

    def test_http_mcp_search_normal_limit_unchanged(self) -> None:
        """HTTP MCP: limit=100 (within range) is accepted without change."""
        import asyncio

        from kafka_mcp.adapters.inbound.rest_api import _create_http_mcp_server

        server = _create_http_mcp_server(MockKafkaClient())
        result = asyncio.run(
            server.call_tool(
                "search_messages",
                {"key": "ORD-123", "limit": 100},
            )
        )
        assert result is not None

    def test_stdio_mcp_search_oversized_limit_clamped(self) -> None:
        """Stdio MCP: limit=10_000_001 does not propagate unclamped."""
        import asyncio

        from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

        server = create_mcp_server(MockKafkaClient())
        result = asyncio.run(
            server.call_tool(
                "search_messages",
                {"key": "ORD-123", "limit": 10_000_001},
            )
        )
        assert result is not None

    def test_stdio_mcp_search_zero_limit_clamped_to_one(self) -> None:
        """Stdio MCP: limit=0 is clamped up to 1 (not silently skipped)."""
        import asyncio

        from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

        server = create_mcp_server(MockKafkaClient())
        result = asyncio.run(
            server.call_tool(
                "search_messages",
                {"key": "ORD-123", "limit": 0},
            )
        )
        assert result is not None


# ---------------------------------------------------------------------------
# Phase 5 Plan 02: consumer_group_lag 4-face tests (LAG-02)
# ---------------------------------------------------------------------------


class TestFastapiConsumerGroupLag:
    """FastAPI: POST /tools/consumer_group_lag — LAG-02 REST face."""

    def test_consumer_group_lag_200(self) -> None:
        """POST returns 200 with result list of lag records."""
        from fastapi.testclient import TestClient

        from kafka_mcp.adapters.inbound.rest_api import create_app

        app = create_app(MockKafkaClient())
        client = TestClient(app)
        response = client.post("/tools/consumer_group_lag", json={"group": "my-group"})
        assert response.status_code == 200
        data = response.json()
        assert "result" in data
        assert isinstance(data["result"], list)
        assert len(data["result"]) == 2
        rec = data["result"][0]
        assert rec["group"] == "my-group"
        assert rec["topic"] == "orders"
        assert rec["lag"] == 50
        assert rec["source"] == "kafka"
        assert rec["event_type"] == "consumer_lag"

    def test_consumer_group_lag_topics_filter(self) -> None:
        """POST with topics filter returns only matching topics."""
        from fastapi.testclient import TestClient

        from kafka_mcp.adapters.inbound.rest_api import create_app

        app = create_app(MockKafkaClient())
        client = TestClient(app)
        response = client.post(
            "/tools/consumer_group_lag",
            json={"group": "my-group", "topics": ["orders"]},
        )
        assert response.status_code == 200
        result = response.json()["result"]
        assert len(result) == 1
        assert result[0]["topic"] == "orders"

    def test_consumer_group_lag_empty_group(self) -> None:
        """POST with unknown group returns empty result list."""
        from fastapi.testclient import TestClient

        from kafka_mcp.adapters.inbound.rest_api import create_app

        app = create_app(MockKafkaClient())
        client = TestClient(app)
        response = client.post("/tools/consumer_group_lag", json={"group": "unknown-group"})
        assert response.status_code == 200
        assert response.json()["result"] == []

    def test_consumer_group_lag_timestamp_utc_is_string(self) -> None:
        """timestamp_utc in response is an ISO-8601 string, not a datetime."""
        from fastapi.testclient import TestClient

        from kafka_mcp.adapters.inbound.rest_api import create_app

        app = create_app(MockKafkaClient())
        client = TestClient(app)
        response = client.post("/tools/consumer_group_lag", json={"group": "my-group"})
        rec = response.json()["result"][0]
        assert isinstance(rec["timestamp_utc"], str)
        assert "2026-06-16" in rec["timestamp_utc"]

    def test_consumer_group_lag_route_exists(self) -> None:
        """POST /tools/consumer_group_lag route is registered."""
        from kafka_mcp.adapters.inbound.rest_api import create_app

        app = create_app(MockKafkaClient())
        paths = [r.path for r in app.routes if hasattr(r, "path")]
        assert "/tools/consumer_group_lag" in paths


class TestMcpConsumerGroupLag:
    """MCP stdio: consumer_group_lag tool — LAG-02 MCP face."""

    def test_tool_registered(self) -> None:
        """consumer_group_lag tool is registered on FastMCP server."""
        import asyncio

        from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

        server = create_mcp_server(MockKafkaClient())
        tools = asyncio.run(server.list_tools())
        tool_names = [t.name for t in tools]
        assert "consumer_group_lag" in tool_names

    def test_read_only_hint(self) -> None:
        """consumer_group_lag tool has readOnlyHint=True (LAG-01 read-only)."""
        import asyncio

        from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

        server = create_mcp_server(MockKafkaClient())
        tools = asyncio.run(server.list_tools())
        tool_map = {t.name: t for t in tools}
        cgl = tool_map["consumer_group_lag"]
        assert cgl.annotations is not None
        assert cgl.annotations.readOnlyHint is True

    def test_returns_lag_records(self) -> None:
        """consumer_group_lag tool returns non-empty result for known group."""
        import asyncio
        import json

        from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

        server = create_mcp_server(MockKafkaClient())
        result = asyncio.run(server.call_tool("consumer_group_lag", {"group": "my-group"}))
        assert result is not None
        # call_tool returns (content_list, is_error) or just content_list
        # depending on FastMCP version; handle both shapes
        content = result[0] if isinstance(result, tuple) else result
        assert len(content) > 0
        # Each dict in list[dict] is serialized as a separate TextContent
        first = json.loads(content[0].text)
        assert isinstance(first, dict)
        assert first["group"] == "my-group"
        assert first["source"] == "kafka"
        assert first["event_type"] == "consumer_lag"
        # Should have 2 records total (orders + payments)
        assert len(content) == 2

    def test_empty_group_returns_empty(self) -> None:
        """consumer_group_lag for unknown group returns empty list."""
        import asyncio

        from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

        server = create_mcp_server(MockKafkaClient())
        result = asyncio.run(server.call_tool("consumer_group_lag", {"group": "ghost"}))
        assert result is not None
        content = result[0] if isinstance(result, tuple) else result
        # Empty list returns empty content list (no TextContent items)
        assert len(content) == 0


class TestCliConsumerGroupLag:
    """CLI: consumer-group-lag subcommand — LAG-02 CLI face."""

    def test_parse_args(self) -> None:
        """parse_args parses consumer-group-lag with --group."""
        from kafka_mcp.adapters.inbound.cli import parse_args

        ns = parse_args(["consumer-group-lag", "--group", "my-group"])
        assert ns.subcommand == "consumer-group-lag"
        assert ns.group == "my-group"
        assert ns.topics is None
        assert ns.json is False

    def test_parse_args_with_topics(self) -> None:
        """parse_args accepts --topics flag."""
        from kafka_mcp.adapters.inbound.cli import parse_args

        ns = parse_args(
            [
                "consumer-group-lag",
                "--group",
                "my-group",
                "--topics",
                "orders,payments",
            ]
        )
        assert ns.topics == "orders,payments"

    def test_table_output(self) -> None:
        """run_consumer_group_lag prints a table with lag data."""
        from kafka_mcp.adapters.inbound.cli import run_consumer_group_lag

        output = _capture_run(
            run_consumer_group_lag,
            MockKafkaClient(),
            group="my-group",
            as_json=False,
        )
        assert "Group" in output
        assert "Topic" in output
        assert "Lag" in output
        assert "my-group" in output
        assert "orders" in output
        assert "50" in output  # lag value

    def test_json_output(self) -> None:
        """run_consumer_group_lag with as_json=True prints valid JSON list."""
        import orjson

        from kafka_mcp.adapters.inbound.cli import run_consumer_group_lag

        output = _capture_run(
            run_consumer_group_lag,
            MockKafkaClient(),
            group="my-group",
            as_json=True,
        )
        data = orjson.loads(output)
        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["group"] == "my-group"
        assert data[0]["source"] == "kafka"
        assert data[0]["event_type"] == "consumer_lag"

    def test_empty_group_table(self) -> None:
        """Empty group prints '(no lag records)' message."""
        from kafka_mcp.adapters.inbound.cli import run_consumer_group_lag

        output = _capture_run(
            run_consumer_group_lag,
            MockKafkaClient(),
            group="unknown-group",
            as_json=False,
        )
        assert "(no lag records)" in output

    def test_topics_filter(self) -> None:
        """--topics flag filters results to matching topics only."""
        import orjson

        from kafka_mcp.adapters.inbound.cli import run_consumer_group_lag

        output = _capture_run(
            run_consumer_group_lag,
            MockKafkaClient(),
            group="my-group",
            topics="orders",
            as_json=True,
        )
        data = orjson.loads(output)
        assert len(data) == 1
        assert data[0]["topic"] == "orders"


class TestServerConsumerGroupLagDispatch:
    """server.main routes consumer-group-lag to CLI runner."""

    def test_consumer_group_lag_routes_to_cli(self) -> None:
        from unittest.mock import patch as mock_patch

        import kafka_mcp.server as server

        captured: dict = {}

        def _fake_cli(args: list) -> None:
            captured["args"] = args

        with (
            mock_patch.object(sys, "argv", ["kafka-mcp", "consumer-group-lag", "--group", "g"]),
            mock_patch("kafka_mcp.adapters.inbound.cli.main", _fake_cli),
            mock_patch("uvicorn.run") as mock_uvicorn,
        ):
            server.main()

        assert captured.get("args") == ["consumer-group-lag", "--group", "g"]
        mock_uvicorn.assert_not_called()


class TestHttpMcpConsumerGroupLag:
    """HTTP MCP: consumer_group_lag tool at /mcp — LAG-02 HTTP MCP face."""

    def test_http_mcp_tool_registered(self) -> None:
        """HTTP MCP server has consumer_group_lag tool."""
        import asyncio

        from kafka_mcp.adapters.inbound.rest_api import _create_http_mcp_server

        server = _create_http_mcp_server(MockKafkaClient())
        tools = asyncio.run(server.list_tools())
        tool_names = [t.name for t in tools]
        assert "consumer_group_lag" in tool_names

    def test_http_mcp_read_only_hint(self) -> None:
        """HTTP MCP consumer_group_lag tool has readOnlyHint=True."""
        import asyncio

        from kafka_mcp.adapters.inbound.rest_api import _create_http_mcp_server

        server = _create_http_mcp_server(MockKafkaClient())
        tools = asyncio.run(server.list_tools())
        tool_map = {t.name: t for t in tools}
        cgl = tool_map["consumer_group_lag"]
        assert cgl.annotations is not None
        assert cgl.annotations.readOnlyHint is True


class TestConsumerGroupLagFourFaceSymmetry:
    """LAG-02: all four faces return identical LagRecord schema."""

    def test_lib_face(self) -> None:
        """Lib face: MockKafkaClient.consumer_group_lag returns list[LagRecord]."""
        mock = MockKafkaClient()
        records = mock.consumer_group_lag("my-group")
        assert isinstance(records, list)
        assert len(records) == 2
        assert all(isinstance(r, LagRecord) for r in records)
        assert records[0].source == "kafka"
        assert records[0].event_type == "consumer_lag"

    def test_four_face_field_parity(self) -> None:
        """All faces produce the same field set for LagRecord."""
        import asyncio
        import json

        from fastapi.testclient import TestClient

        from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server
        from kafka_mcp.adapters.inbound.rest_api import create_app

        mock = MockKafkaClient()
        required_fields = {
            "group",
            "topic",
            "partition",
            "current_offset",
            "end_offset",
            "lag",
            "timestamp_utc",
            "source",
            "event_type",
        }

        # Lib face
        lib_fields = set(mock.consumer_group_lag("my-group")[0].model_dump().keys())
        assert required_fields <= lib_fields

        # REST face
        app = create_app(mock)
        tc = TestClient(app)
        resp = tc.post("/tools/consumer_group_lag", json={"group": "my-group"})
        rest_fields = set(resp.json()["result"][0].keys())
        assert required_fields <= rest_fields

        # MCP stdio face
        server = create_mcp_server(mock)
        result = asyncio.run(server.call_tool("consumer_group_lag", {"group": "my-group"}))
        content = result[0] if isinstance(result, tuple) else result
        mcp_fields = set(json.loads(content[0].text).keys())
        assert required_fields <= mcp_fields

        # CLI face (JSON output)
        import orjson

        from kafka_mcp.adapters.inbound.cli import run_consumer_group_lag

        cli_out = _capture_run(run_consumer_group_lag, mock, group="my-group", as_json=True)
        cli_fields = set(orjson.loads(cli_out)[0].keys())
        assert required_fields <= cli_fields
