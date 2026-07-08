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


# ---------------------------------------------------------------------------
# COV-02: correlate_messages across faces with base64 raw + timestamp round-trip
# ---------------------------------------------------------------------------


def _correlated_msg() -> KafkaMessage:
    """Deterministic correlated message returned by the fake client."""
    return KafkaMessage(
        topic="payments",
        partition=1,
        offset=7,
        key="ORD-123",
        headers={"ce-type": "payment.created"},
        value={"order_id": "ORD-123", "amount": 42},
        timestamp_utc=_SAMPLE_TS,
        raw=_SAMPLE_RAW,
        source="kafka",
        event_type="kafka_message",
    )


class CorrelateFakeClient:
    """Fake KafkaClient exposing correlate_messages for COV-02.

    Captures the received ``initial_results`` so the test can assert the
    base64 raw + timestamp round-trip reconstructed real KafkaMessage objects.
    """

    def __init__(self) -> None:
        self.received_initial_results: list[KafkaMessage] | None = None
        self.received_follow_topics: list[str] | None = None

    def close(self) -> None:  # lifespan teardown compatibility
        pass

    def correlate_messages(
        self,
        initial_results: list[KafkaMessage],
        follow_topics: list[str],
        **kwargs,
    ) -> list[KafkaMessage]:
        self.received_initial_results = initial_results
        self.received_follow_topics = follow_topics
        return [_correlated_msg()]


def _initial_payload_dict() -> dict:
    """A message dict as a real caller sends it over a face.

    ``raw`` is a base64 string and ``timestamp_utc`` is an ISO-8601 string —
    matching the inverse-of-_serialize_message decode the adapters perform.
    """
    return {
        "topic": "orders",
        "partition": 0,
        "offset": 42,
        "key": "ORD-123",
        "headers": {"ce-type": "order.created"},
        "value": {"order_id": "ORD-123", "amount": 99},
        "timestamp_utc": _SAMPLE_TS_ISO,
        "raw": base64.b64encode(_SAMPLE_RAW).decode("ascii"),
    }


def _assert_inbound_round_trip(client: CorrelateFakeClient) -> None:
    """The fake received a reconstructed KafkaMessage (raw bytes + tz-aware ts)."""
    assert client.received_follow_topics == ["payments"]
    assert client.received_initial_results is not None
    assert len(client.received_initial_results) == 1
    reconstructed = client.received_initial_results[0]
    assert isinstance(reconstructed, KafkaMessage)
    # base64 raw round-trip: bytes reconstructed exactly
    assert reconstructed.raw == _SAMPLE_RAW
    # timestamp round-trip: tz-aware datetime parsed from the ISO string
    assert reconstructed.timestamp_utc == _SAMPLE_TS
    assert reconstructed.timestamp_utc.tzinfo is not None


def test_cov02_correlate_messages_stdio_mcp() -> None:
    """stdio MCP face: correlate_messages delegates + inbound round-trip."""
    from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

    client = CorrelateFakeClient()
    server = create_mcp_server(client)
    result = asyncio.run(
        server.call_tool(
            "correlate_messages",
            {
                "initial_results_data": [_initial_payload_dict()],
                "follow_topics": ["payments"],
            },
        )
    )
    content = _content(result)
    assert len(content) == 1
    _assert_inbound_round_trip(client)


def test_cov02_correlate_messages_http_mcp() -> None:
    """HTTP MCP face: correlate_messages delegates + inbound round-trip."""
    from kafka_mcp.adapters.inbound.rest_api import _create_http_mcp_server

    client = CorrelateFakeClient()
    server = _create_http_mcp_server(client)
    result = asyncio.run(
        server.call_tool(
            "correlate_messages",
            {
                "initial_results_data": [_initial_payload_dict()],
                "follow_topics": ["payments"],
            },
        )
    )
    content = _content(result)
    assert len(content) == 1
    _assert_inbound_round_trip(client)


def test_cov02_correlate_messages_rest() -> None:
    """REST face: POST /tools/correlate_messages delegates + outbound round-trip.

    REST request field is ``initial_results`` (per CorrelateMessagesRequest),
    whereas the MCP tool param is ``initial_results_data``.
    """
    from fastapi.testclient import TestClient

    from kafka_mcp.adapters.inbound.rest_api import create_app

    client = CorrelateFakeClient()
    tc = TestClient(create_app(client))
    response = tc.post(
        "/tools/correlate_messages",
        json={
            "initial_results": [_initial_payload_dict()],
            "follow_topics": ["payments"],
        },
    )
    assert response.status_code == 200
    result = response.json()["result"]
    assert isinstance(result, list)
    assert len(result) == 1
    # Inbound round-trip: fake received a reconstructed KafkaMessage.
    _assert_inbound_round_trip(client)
    # Outbound round-trip: response raw is a base64 string decoding to the
    # original sample bytes (proving the serialize round-trip on the way out).
    out = result[0]
    assert isinstance(out["raw"], str)
    assert base64.b64decode(out["raw"]) == _SAMPLE_RAW
    assert out["topic"] == "payments"
