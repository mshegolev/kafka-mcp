"""Test-only hardening for the frozen read-only tool surface.

Phase 12 (v1.3) — HINT-01 + PARSE-01. This module is assertion-only: it does
NOT modify any src/ behavior. It locks two existing guarantees:

  HINT-01: every registered tool on the stdio MCP face
    (``mcp_stdio.create_mcp_server``) and the HTTP MCP face
    (``rest_api._create_http_mcp_server``) advertises ``readOnlyHint``,
    ``idempotentHint``, and ``openWorldHint`` all ``True``. Both shared
    module-level ``_READ_ONLY`` ToolAnnotations constants (mcp_stdio +
    rest_api) — the single annotation object mounted on every REST/HTTP-MCP
    tool at registration — are pinned to carry the three hints. The tool set
    is derived from ``list_tools()`` so a newly added tool cannot silently
    skip the check.

  PARSE-01: ``mcp_stdio._parse_iso_utc`` raises an actionable, param-named
    ``ValueError`` for bad ``time_from`` / ``time_to`` input, accepts a
    trailing-Z UTC timestamp as tz-aware UTC, and defaults a naive timestamp
    to UTC.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from kafka_mcp.domain.errors import TopicNotFoundError
from kafka_mcp.domain.models import KafkaMessage, LagRecord, PartitionInfo, TopicInfo

# The frozen read-only tool set (D-13/D-14). Used as a subset check to catch
# accidental tool removal — NOT as the source of truth for the hint loop.
_FROZEN_TOOLS = frozenset(
    {
        "list_topics",
        "describe_topic",
        "search_messages",
        "get_message",
        "consumer_group_lag",
        "correlate_messages",
    }
)

_SAMPLE_TS = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


class FakeKafkaClient:
    """Minimal in-module KafkaClient stand-in for annotation introspection.

    Mirrors the MockKafkaClient shape from tests/test_inbound.py. No broker
    contact; deterministic non-sensitive data only. correlate_messages is not
    exercised by the annotation checks, so it is intentionally omitted.
    """

    def close(self) -> None:
        """No-op close for lifespan teardown compatibility."""

    def list_topics(self, include_internal: bool = False) -> list[str]:
        topics = ["orders", "payments"]
        if include_internal:
            topics = ["__consumer_offsets"] + topics
        return topics

    def describe_topic(self, topic: str) -> TopicInfo:
        if topic in ("orders", "payments"):
            return TopicInfo(
                name=topic,
                partition_count=1,
                partitions=[PartitionInfo(id=0, leader=0, earliest=0, latest=100)],
            )
        raise TopicNotFoundError(topic)

    def search_messages(self, key: str, **kwargs) -> list[KafkaMessage]:
        return []

    def get_message(self, topic: str, partition: int, offset: int) -> KafkaMessage:
        return KafkaMessage(
            topic=topic,
            partition=partition,
            offset=offset,
            key="ORD-123",
            headers={},
            value=None,
            timestamp_utc=_SAMPLE_TS,
            raw=b"",
        )

    def consumer_group_lag(self, group: str, topics: list[str] | None = None) -> list[LagRecord]:
        return []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _list_tools(server) -> dict:
    """Return {name: Tool} by introspecting server.list_tools() (async)."""
    tools = asyncio.run(server.list_tools())
    return {t.name: t for t in tools}


def _assert_all_hints(tool, face: str) -> None:
    """Assert a single tool advertises the three read-only hints all True."""
    ann = tool.annotations
    assert ann is not None, f"{face}:{tool.name} is missing ToolAnnotations"
    assert ann.readOnlyHint is True, (
        f"{face}:{tool.name} readOnlyHint expected True, got {ann.readOnlyHint}"
    )
    assert ann.idempotentHint is True, (
        f"{face}:{tool.name} idempotentHint expected True, got {ann.idempotentHint}"
    )
    assert ann.openWorldHint is True, (
        f"{face}:{tool.name} openWorldHint expected True, got {ann.openWorldHint}"
    )


# ---------------------------------------------------------------------------
# HINT-01 — annotation coverage across stdio MCP, HTTP MCP, REST wiring
# ---------------------------------------------------------------------------


def _build_servers():
    """Return [(face_name, server)] for the two MCP faces."""
    from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server
    from kafka_mcp.adapters.inbound.rest_api import _create_http_mcp_server

    client = FakeKafkaClient()
    return [
        ("stdio", create_mcp_server(client)),
        ("http", _create_http_mcp_server(client)),
    ]


@pytest.mark.parametrize("face,server", _build_servers())
def test_every_tool_advertises_read_only_hints(face: str, server) -> None:
    """HINT-01: every registered tool on each MCP face has all three hints True.

    The tool set is derived from list_tools() (not a hardcoded list), so a
    newly added tool cannot silently skip the hint check.
    """
    tool_map = _list_tools(server)
    assert tool_map, f"{face}: no tools registered"
    for tool in tool_map.values():
        _assert_all_hints(tool, face)


@pytest.mark.parametrize("face,server", _build_servers())
def test_frozen_tool_set_is_subset(face: str, server) -> None:
    """HINT-01: the frozen tool set is a subset of discovered tools.

    Catches accidental removal of a tool from either MCP face.
    """
    discovered = set(_list_tools(server))
    missing = _FROZEN_TOOLS - discovered
    assert not missing, f"{face}: frozen tools missing from surface: {sorted(missing)}"


def test_stdio_read_only_annotation_constant_carries_all_hints() -> None:
    """HINT-01: the stdio module's shared _READ_ONLY constant carries all hints.

    This is the single annotation object attached to every stdio MCP tool at
    registration; pinning it proves the wiring advertises the hints.
    """
    from kafka_mcp.adapters.inbound import mcp_stdio

    ro = mcp_stdio._READ_ONLY
    assert ro.readOnlyHint is True
    assert ro.idempotentHint is True
    assert ro.openWorldHint is True


def test_rest_read_only_annotation_constant_carries_all_hints() -> None:
    """HINT-01: rest_api._READ_ONLY carries all three hints.

    This is the single annotation object attached to every REST-mounted HTTP
    MCP tool at registration; asserting it proves the REST face wiring
    advertises the hints.
    """
    from kafka_mcp.adapters.inbound import rest_api

    ro = rest_api._READ_ONLY
    assert ro.readOnlyHint is True
    assert ro.idempotentHint is True
    assert ro.openWorldHint is True


# ---------------------------------------------------------------------------
# PARSE-01 — _parse_iso_utc rejects bad input, accepts trailing-Z / naive UTC
# ---------------------------------------------------------------------------

_ISO_MARKER = "ISO-8601"
_TRAILING_Z_MARKER = "Z'"  # the "...Z" example the function emits in its message


def test_parse_iso_utc_bad_time_from_raises_actionable_error() -> None:
    """PARSE-01: bad time_from raises a ValueError naming the param + format."""
    from kafka_mcp.adapters.inbound.mcp_stdio import _parse_iso_utc

    with pytest.raises(ValueError) as exc:
        _parse_iso_utc("not-a-timestamp", "time_from")
    msg = str(exc.value)
    assert "time_from" in msg
    assert _ISO_MARKER in msg
    assert _TRAILING_Z_MARKER in msg


def test_parse_iso_utc_bad_time_to_raises_actionable_error() -> None:
    """PARSE-01: bad time_to raises a ValueError naming the param + format."""
    from kafka_mcp.adapters.inbound.mcp_stdio import _parse_iso_utc

    with pytest.raises(ValueError) as exc:
        _parse_iso_utc("32:99 nope", "time_to")
    msg = str(exc.value)
    assert "time_to" in msg
    assert _ISO_MARKER in msg
    assert _TRAILING_Z_MARKER in msg


def test_parse_iso_utc_trailing_z_parses_to_tz_aware_utc() -> None:
    """PARSE-01: a trailing-Z timestamp parses to tz-aware UTC."""
    from kafka_mcp.adapters.inbound.mcp_stdio import _parse_iso_utc

    result = _parse_iso_utc("2026-06-01T00:00:00Z", "time_from")
    assert result.tzinfo is not None
    assert result.utcoffset() == timedelta(0)
    assert result == datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_parse_iso_utc_naive_defaults_to_utc() -> None:
    """PARSE-01: a naive timestamp (no offset) is defaulted to UTC."""
    from kafka_mcp.adapters.inbound.mcp_stdio import _parse_iso_utc

    result = _parse_iso_utc("2026-06-01T00:00:00", "time_to")
    assert result.tzinfo is not None
    assert result.utcoffset() == timedelta(0)
    assert result == datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
