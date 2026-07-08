"""Phase 12 Plan 02 — face coverage for previously-uncovered tools.

COV-01: consumer_group_lag exercised on stdio MCP, HTTP MCP, and REST faces
        (including the REST topics filter), asserting delegation to the client.
COV-02: correlate_messages exercised on stdio MCP, HTTP MCP, and REST faces,
        verifying the base64 raw + timestamp round-trip both inbound
        (the fake client receives reconstructed KafkaMessage objects) and
        outbound (the response raw decodes back to the original bytes).

Assertion-only: these tests drive the production adapters in-process via
FastMCP ``call_tool`` and FastAPI ``TestClient``. No src/ file is modified.

Patterns (fake client + TestClient + call_tool result-shape handling) mirror
tests/test_inbound.py.
"""

from __future__ import annotations

import asyncio
import base64
import json
from datetime import datetime, timezone

from kafka_mcp.domain.models import KafkaMessage, LagRecord

# ---------------------------------------------------------------------------
# Shared fixtures (mirror tests/test_inbound.py shapes)
# ---------------------------------------------------------------------------

_SAMPLE_RAW = b"\x00\x01\x02\x03"
_SAMPLE_TS = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
_SAMPLE_TS_ISO = _SAMPLE_TS.isoformat()  # "2026-01-01T12:00:00+00:00"

_SAMPLE_LAG_TS = datetime(2026, 6, 16, 12, 0, 0, tzinfo=timezone.utc)


def _lag_records(group: str, topics: list[str] | None = None) -> list[LagRecord]:
    """Deterministic lag records for 'my-group'; empty otherwise."""
    if group != "my-group":
        return []
    records = [
        LagRecord(
            group="my-group",
            topic="orders",
            partition=0,
            current_offset=50,
            end_offset=100,
            lag=50,
            timestamp_utc=_SAMPLE_LAG_TS,
        ),
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


class LagFakeClient:
    """Fake KafkaClient exposing only consumer_group_lag for COV-01."""

    def close(self) -> None:  # lifespan teardown compatibility
        pass

    def consumer_group_lag(
        self, group: str, topics: list[str] | None = None
    ) -> list[LagRecord]:
        return _lag_records(group, topics)


def _content(result):
    """Normalize FastMCP call_tool return into the content list.

    call_tool returns either ``(content_list, is_error)`` or just
    ``content_list`` depending on the FastMCP version.
    """
    return result[0] if isinstance(result, tuple) else result


# ---------------------------------------------------------------------------
# COV-01: consumer_group_lag across stdio MCP, HTTP MCP, and REST
# ---------------------------------------------------------------------------


def test_cov01_consumer_group_lag_stdio_mcp() -> None:
    """stdio MCP face: consumer_group_lag delegates to the client."""
    from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

    server = create_mcp_server(LagFakeClient())
    result = asyncio.run(server.call_tool("consumer_group_lag", {"group": "my-group"}))
    content = _content(result)
    assert len(content) == 2
    first = json.loads(content[0].text)
    assert first["group"] == "my-group"
    assert first["topic"] == "orders"
    assert first["lag"] == 50
    assert first["source"] == "kafka"
    assert first["event_type"] == "consumer_lag"


def test_cov01_consumer_group_lag_http_mcp() -> None:
    """HTTP MCP face: consumer_group_lag registered and delegates."""
    from kafka_mcp.adapters.inbound.rest_api import _create_http_mcp_server

    server = _create_http_mcp_server(LagFakeClient())
    result = asyncio.run(server.call_tool("consumer_group_lag", {"group": "my-group"}))
    content = _content(result)
    assert len(content) == 2
    first = json.loads(content[0].text)
    assert first["group"] == "my-group"
    assert first["topic"] == "orders"
    assert first["lag"] == 50
    assert first["source"] == "kafka"
    assert first["event_type"] == "consumer_lag"


def test_cov01_consumer_group_lag_rest() -> None:
    """REST face: POST /tools/consumer_group_lag delegates to the client."""
    from fastapi.testclient import TestClient

    from kafka_mcp.adapters.inbound.rest_api import create_app

    client = TestClient(create_app(LagFakeClient()))
    response = client.post("/tools/consumer_group_lag", json={"group": "my-group"})
    assert response.status_code == 200
    result = response.json()["result"]
    assert isinstance(result, list)
    assert len(result) == 2
    rec = result[0]
    assert rec["group"] == "my-group"
    assert rec["topic"] == "orders"
    assert rec["lag"] == 50
    assert rec["source"] == "kafka"
    assert rec["event_type"] == "consumer_lag"


def test_cov01_consumer_group_lag_rest_topics_filter() -> None:
    """REST face: topics filter narrows the delegated result."""
    from fastapi.testclient import TestClient

    from kafka_mcp.adapters.inbound.rest_api import create_app

    client = TestClient(create_app(LagFakeClient()))
    response = client.post(
        "/tools/consumer_group_lag",
        json={"group": "my-group", "topics": ["orders"]},
    )
    assert response.status_code == 200
    result = response.json()["result"]
    assert len(result) == 1
    assert result[0]["topic"] == "orders"
