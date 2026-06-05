"""kafka-mcp — read-only Kafka MCP brick.

Primary programmatic entry point::

    from kafka_mcp import KafkaClient
    client = KafkaClient.from_env()
    topics = client.list_topics()
    info = client.describe_topic("payments")
    messages = client.search_messages("ORD-123")
    message = client.get_message("orders", 0, 42)
"""

from __future__ import annotations

from kafka_mcp.adapters.inbound.lib import KafkaClient
from kafka_mcp.domain.errors import (
    ConfigError,
    DecodeError,
    MessageNotFoundError,
    TopicNotFoundError,
    TransientError,
)
from kafka_mcp.domain.models import KafkaMessage, PartitionInfo, TopicInfo

__all__ = [
    "KafkaClient",
    "TopicInfo",
    "PartitionInfo",
    "KafkaMessage",
    "TopicNotFoundError",
    "ConfigError",
    "DecodeError",
    "MessageNotFoundError",
    "TransientError",
]
