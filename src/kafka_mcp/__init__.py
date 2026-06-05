"""kafka-mcp — read-only Kafka MCP brick.

Primary programmatic entry point::

    from kafka_mcp import KafkaClient
    client = KafkaClient.from_env()
    topics = client.list_topics()
    info = client.describe_topic("payments")
"""

from __future__ import annotations

from kafka_mcp.adapters.inbound.lib import KafkaClient
from kafka_mcp.domain.errors import ConfigError, TopicNotFoundError
from kafka_mcp.domain.models import PartitionInfo, TopicInfo

__all__ = [
    "KafkaClient",
    "TopicInfo",
    "PartitionInfo",
    "TopicNotFoundError",
    "ConfigError",
]
