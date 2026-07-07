"""MCP stdio inbound adapter — registers read-only Kafka tools via FastMCP.

Exposes six tools that delegate to KafkaClient:
  - list_topics: returns sorted list of topic names
  - describe_topic: returns TopicInfo metadata for a single topic
  - search_messages: search messages by key within a time window
  - get_message: fetch and decode a single message by coordinates
  - consumer_group_lag: report per-partition consumer lag for a group
  - correlate_messages: follow extracted IDs across topics into a chain

All tools carry ``readOnlyHint=True`` on their ToolAnnotations (D-13, D-14),
plus ``idempotentHint=True`` (repeated reads have no side effects) and
``openWorldHint=True`` (they query an external Kafka broker).
Defense-in-depth: the structural assign-based consumer (no subscribe/commit)
is the primary read-only guarantee; the hints are advisory to the MCP host.

Usage::

    from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server
    from kafka_mcp.adapters.inbound.lib import KafkaClient

    client = KafkaClient.from_env()
    app = create_mcp_server(client)
    app.run("stdio")   # blocks, reads from stdin/writes to stdout
"""

from __future__ import annotations

import base64
from datetime import datetime, timezone

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from kafka_mcp.adapters.inbound.lib import KafkaClient
from kafka_mcp.domain.errors import (
    DecodeError,
    MessageNotFoundError,
    TopicNotFoundError,
    TransientError,
)
from kafka_mcp.domain.models import KafkaMessage, LagRecord

_READ_ONLY = ToolAnnotations(
    readOnlyHint=True,
    idempotentHint=True,
    openWorldHint=True,
)


def _parse_iso_utc(value: str, param: str) -> datetime:
    """Parse an ISO-8601 timestamp string, defaulting naive values to UTC.

    Accepts a trailing ``Z`` (UTC) which :func:`datetime.fromisoformat` rejects
    on Python < 3.11. Raises a :class:`ValueError` with an actionable message
    naming *param* and the expected format, instead of leaking the raw
    ``fromisoformat`` error to the MCP host.

    Args:
        value: The ISO-8601 timestamp string to parse.
        param: The tool parameter name (for the error message), e.g. ``time_from``.

    Returns:
        A timezone-aware :class:`datetime` (UTC if the input had no offset).

    Raises:
        ValueError: When *value* is not a valid ISO-8601 timestamp.
    """
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(
            f"Invalid {param}: {value!r} is not a valid ISO-8601 timestamp. "
            "Use e.g. '2026-06-01T00:00:00Z' or '2026-06-01T12:30:00+00:00'."
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _serialize_message(msg: KafkaMessage) -> dict:
    """Serialize a KafkaMessage to a JSON-safe dict.

    Converts ``raw`` bytes to a base64-encoded ASCII string so the dict
    can be safely serialized over MCP/REST/CLI boundaries.

    Args:
        msg: A :class:`~kafka_mcp.domain.models.KafkaMessage` domain object.

    Returns:
        Dict with all fields from ``model_dump()`` plus ``raw`` replaced
        by its base64-encoded ASCII representation.
    """
    data = msg.model_dump()
    data["raw"] = base64.b64encode(msg.raw).decode("ascii")
    # KEY-02: base64-encode raw_key bytes when present (T-04-06)
    if msg.raw_key is not None:
        data["raw_key"] = base64.b64encode(msg.raw_key).decode("ascii")
    # Serialize datetime to ISO-8601 string for JSON compatibility
    if isinstance(data.get("timestamp_utc"), datetime):
        data["timestamp_utc"] = data["timestamp_utc"].isoformat()
    return data


def _serialize_lag_record(record: LagRecord) -> dict:
    """Serialize a LagRecord to a JSON-safe dict.

    Converts ``timestamp_utc`` to an ISO-8601 string. LagRecord has no
    bytes fields, so no base64 encoding is needed.

    Args:
        record: A :class:`~kafka_mcp.domain.models.LagRecord` domain object.

    Returns:
        Dict with all fields from ``model_dump()`` plus ``timestamp_utc``
        as an ISO-8601 string.
    """
    data = record.model_dump()
    if isinstance(data.get("timestamp_utc"), datetime):
        data["timestamp_utc"] = data["timestamp_utc"].isoformat()
    return data


def create_mcp_server(client: KafkaClient) -> FastMCP:
    """Build and return a FastMCP server wired to *client*.

    Registers:
    - ``list_topics`` — returns list[str] of topic names
    - ``describe_topic`` — returns TopicInfo as dict
    - ``search_messages`` — returns list[dict] of KafkaMessage dicts
    - ``get_message`` — returns a single KafkaMessage dict
    - ``consumer_group_lag`` — returns list[dict] of LagRecord dicts
    - ``correlate_messages`` — returns list[dict] of correlated KafkaMessage dicts

    All tools have ``readOnlyHint=True`` per D-13/D-14, plus
    ``idempotentHint=True`` and ``openWorldHint=True``.

    Args:
        client: A :class:`~kafka_mcp.adapters.inbound.lib.KafkaClient`
            instance (real or mock).

    Returns:
        A configured :class:`~mcp.server.fastmcp.FastMCP` instance.
        Call ``server.run("stdio")`` to start the stdio transport.
    """
    app = FastMCP("kafka-mcp")

    @app.tool(
        name="list_topics",
        description=(
            "Return a sorted list of Kafka topic names available on the broker. "
            "Pass include_internal=true to include internal topics (e.g. __consumer_offsets)."
        ),
        annotations=_READ_ONLY,
    )
    def list_topics(include_internal: bool = False) -> list[str]:  # noqa: D401
        """List all topics on the broker."""
        return client.list_topics(include_internal=include_internal)

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

    @app.tool(
        name="search_messages",
        description=(
            "Search Kafka messages by key within an optional time window. "
            "Returns up to `limit` matching messages with decoded values and "
            "Investigator Contract evidence fields (source, event_type, keys). "
            "raw bytes are base64-encoded in the response."
        ),
        annotations=_READ_ONLY,
    )
    def search_messages(
        key: str,
        key_field: str | None = None,
        topics: list[str] | None = None,
        headers: dict[str, str] | None = None,
        time_from: str | None = None,
        time_to: str | None = None,
        limit: int = 500,
    ) -> list[dict]:  # noqa: D401
        """Search messages matching *key* across topics.

        Args:
            key: The value to match (against message key, header, or value field).
            key_field: Optional match field — None/"key" for message key,
                "header:<name>" for header, "value:<dotted.path>" for value field.
            topics: Optional list of topic names to scan. Defaults to all topics.
            headers: Optional dict of header key-value pairs to filter by.
            time_from: Optional ISO8601 datetime string for the start of the
                time window. Defaults to earliest available messages.
            time_to: Optional ISO8601 datetime string for the end of the
                time window. Defaults to now.
            limit: Maximum number of matching messages to return (1–10000).
                Defaults to 500.

        Returns:
            List of message dicts with base64-encoded ``raw`` field.
        """
        # WR-01: clamp limit to [1, 10000] to prevent unbounded scan via MCP.
        limit = max(1, min(limit, 10_000))
        tf = _parse_iso_utc(time_from, "time_from") if time_from is not None else None
        tt = _parse_iso_utc(time_to, "time_to") if time_to is not None else None
        results = client.search_messages(
            key,
            key_field=key_field,
            topics=topics,
            headers=headers,
            time_from=tf,
            time_to=tt,
            limit=limit,
        )
        return [_serialize_message(m) for m in results]

    @app.tool(
        name="get_message",
        description=(
            "Fetch and decode a single Kafka message by topic, partition, and offset. "
            "Returns the full Evidence shape including decoded value and raw (base64). "
            "Raises an error if the offset is out of range or the payload cannot be decoded."
        ),
        annotations=_READ_ONLY,
    )
    def get_message(topic: str, partition: int, offset: int) -> dict:  # noqa: D401
        """Fetch a single message by exact coordinates.

        Args:
            topic: Topic name.
            partition: Partition index (0-based).
            offset: Exact message offset.

        Returns:
            Message dict with base64-encoded ``raw`` field.

        Raises:
            ValueError: When the message is not found at the given coordinates.
            ValueError: When the payload cannot be decoded.
        """
        try:
            return _serialize_message(client.get_message(topic, partition, offset))
        except MessageNotFoundError as exc:
            raise ValueError(f"Message not found: {exc.topic}[{exc.partition}]@{exc.offset}") from exc
        except TransientError as exc:
            # WR-05: in-range offset that timed out — transient, not absence.
            raise ValueError(
                f"Transient read failure: {exc.topic}[{exc.partition}]@{exc.offset}: {exc.reason}"
            ) from exc
        except DecodeError as exc:
            raise ValueError(f"Decode failed: {exc.topic}[{exc.partition}]@{exc.offset}: {exc.reason}") from exc

    @app.tool(
        name="consumer_group_lag",
        description=(
            "Report per-partition consumer lag (committed offset vs end offset) "
            "for a given consumer group. Read-only — no commits, no group joins."
        ),
        annotations=_READ_ONLY,
    )
    def consumer_group_lag(group: str, topics: list[str] | None = None) -> list[dict]:  # noqa: D401
        """Report per-partition lag for a consumer group."""
        records = client.consumer_group_lag(group, topics)
        return [_serialize_lag_record(r) for r in records]

    @app.tool(
        name="correlate_messages",
        description=(
            "Correlate messages by following extracted IDs from initial results into additional topics. "
            "Returns correlated messages with correlation_chain populated."
        ),
        annotations=_READ_ONLY,
    )
    def correlate_messages(
        initial_results_data: list[dict],
        follow_topics: list[str],
        limit: int = 500,
        regex_patterns: list[str] | None = None,
        jsonpath_expressions: list[str] | None = None,
        max_depth: int | None = None,
        max_breadth: int | None = None,
        bidirectional: bool = False,
    ) -> list[dict]:  # noqa: D401
        """Correlate messages by following extracted IDs into additional topics."""
        # Convert dict data back to KafkaMessage objects (inverse of _serialize_message).
        initial_results: list[KafkaMessage] = []
        for msg_data in initial_results_data:
            # Handle base64 decoding of raw fields
            if "raw" in msg_data and isinstance(msg_data["raw"], str):
                msg_data["raw"] = base64.b64decode(msg_data["raw"])
            if "raw_key" in msg_data and isinstance(msg_data["raw_key"], str):
                msg_data["raw_key"] = base64.b64decode(msg_data["raw_key"])
            # Handle timestamp parsing
            if "timestamp_utc" in msg_data and isinstance(msg_data["timestamp_utc"], str):
                msg_data["timestamp_utc"] = datetime.fromisoformat(msg_data["timestamp_utc"].replace("Z", "+00:00"))
            initial_results.append(KafkaMessage(**msg_data))

        results = client.correlate_messages(
            initial_results=initial_results,
            follow_topics=follow_topics,
            limit=limit,
            regex_patterns=regex_patterns,
            jsonpath_expressions=jsonpath_expressions,
            max_depth=max_depth,
            max_breadth=max_breadth,
            bidirectional=bidirectional,
        )
        return [_serialize_message(m) for m in results]

    return app
