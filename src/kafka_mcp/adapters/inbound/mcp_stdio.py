"""MCP stdio inbound adapter — registers read-only Kafka tools via FastMCP.

Exposes four tools that delegate to KafkaClient:
  - list_topics: returns sorted list of topic names
  - describe_topic: returns TopicInfo metadata for a single topic
  - search_messages: search messages by key within a time window
  - get_message: fetch and decode a single message by coordinates

All tools carry ``readOnlyHint=True`` on their ToolAnnotations (D-13, D-14).
Defense-in-depth: the structural assign-based consumer (no subscribe/commit)
is the primary read-only guarantee; readOnlyHint is advisory to the MCP host.

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
from kafka_mcp.domain.models import KafkaMessage

_READ_ONLY = ToolAnnotations(readOnlyHint=True)


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


def create_mcp_server(client: KafkaClient) -> FastMCP:
    """Build and return a FastMCP server wired to *client*.

    Registers:
    - ``list_topics`` — returns list[str] of topic names
    - ``describe_topic`` — returns TopicInfo as dict
    - ``search_messages`` — returns list[dict] of KafkaMessage dicts
    - ``get_message`` — returns a single KafkaMessage dict

    All tools have ``readOnlyHint=True`` per D-13/D-14.

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
        tf = datetime.fromisoformat(time_from) if time_from is not None else None
        if tf is not None and tf.tzinfo is None:
            tf = tf.replace(tzinfo=timezone.utc)
        tt = datetime.fromisoformat(time_to) if time_to is not None else None
        if tt is not None and tt.tzinfo is None:
            tt = tt.replace(tzinfo=timezone.utc)
        results = client.search_messages(
            key,
            key_field=key_field,
            topics=topics,
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
            raise ValueError(
                f"Message not found: {exc.topic}[{exc.partition}]@{exc.offset}"
            ) from exc
        except TransientError as exc:
            # WR-05: in-range offset that timed out — transient, not absence.
            raise ValueError(
                f"Transient read failure: "
                f"{exc.topic}[{exc.partition}]@{exc.offset}: {exc.reason}"
            ) from exc
        except DecodeError as exc:
            raise ValueError(
                f"Decode failed: {exc.topic}[{exc.partition}]@{exc.offset}: {exc.reason}"
            ) from exc

    return app
