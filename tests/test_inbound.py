"""Tests for all inbound adapter faces: MCP stdio, FastAPI REST, CLI.

All tests use MockKafkaClient — no real broker required.
Verifies Phase 1 success criterion 5: list_topics and describe_topic reachable
via MCP stdio, FastAPI, and CLI faces.
"""

from __future__ import annotations

import io
import sys

import pytest

from kafka_mcp.domain.errors import TopicNotFoundError
from kafka_mcp.domain.models import PartitionInfo, TopicInfo


# ---------------------------------------------------------------------------
# Mock KafkaClient — no broker, deterministic data
# ---------------------------------------------------------------------------


class MockKafkaClient:
    """Minimal KafkaClient stand-in for adapter tests.

    Returns deterministic topic data without connecting to a broker.
    """

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
    response = client.post(
        "/tools/list_topics", json={"include_internal": True}
    )
    assert response.status_code == 200
    assert "__consumer_offsets" in response.json()["result"]


def test_fastapi_describe_topic() -> None:
    """POST /tools/describe_topic returns 200 with payments topic metadata."""
    from fastapi.testclient import TestClient

    from kafka_mcp.adapters.inbound.rest_api import create_app

    app = create_app(MockKafkaClient())
    client = TestClient(app)
    response = client.post(
        "/tools/describe_topic", json={"topic": "payments"}
    )
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
    response = client.post(
        "/tools/describe_topic", json={"topic": "unknown"}
    )
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
    topics = orjson.loads(output)
    assert "payments" in topics
    assert "orders" in topics


def test_cli_list_topics_include_internal() -> None:
    """run_list_topics with include_internal=True includes __consumer_offsets."""
    from kafka_mcp.adapters.inbound.cli import run_list_topics

    output = _capture_run(
        run_list_topics, MockKafkaClient(), include_internal=True, as_json=False
    )
    assert "__consumer_offsets" in output


def test_cli_describe_topic_table() -> None:
    """run_describe_topic prints partition table with Partition header and latest offset."""
    from kafka_mcp.adapters.inbound.cli import run_describe_topic

    output = _capture_run(
        run_describe_topic, MockKafkaClient(), "payments", as_json=False
    )
    assert "Partition" in output
    assert "500" in output  # latest offset for partition 0


def test_cli_describe_topic_json() -> None:
    """run_describe_topic with as_json=True prints valid JSON TopicInfo."""
    import orjson

    from kafka_mcp.adapters.inbound.cli import run_describe_topic

    output = _capture_run(
        run_describe_topic, MockKafkaClient(), "payments", as_json=True
    )
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
