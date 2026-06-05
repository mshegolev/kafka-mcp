"""MCP stdio inbound adapter — registers read-only Kafka tools via FastMCP.

Exposes two tools that delegate to KafkaClient:
  - list_topics: returns sorted list of topic names
  - describe_topic: returns TopicInfo metadata for a single topic

Both tools carry ``readOnlyHint=True`` on their ToolAnnotations (D-13, D-14).
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

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from kafka_mcp.adapters.inbound.lib import KafkaClient
from kafka_mcp.domain.errors import TopicNotFoundError

_READ_ONLY = ToolAnnotations(readOnlyHint=True)


def create_mcp_server(client: KafkaClient) -> FastMCP:
    """Build and return a FastMCP server wired to *client*.

    Registers:
    - ``list_topics`` — returns list[str] of topic names
    - ``describe_topic`` — returns TopicInfo as dict

    Both tools have ``readOnlyHint=True`` per D-13/D-14.

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

    return app
