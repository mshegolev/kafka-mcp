"""kafka-mcp — read-only Kafka MCP brick.

Primary programmatic entry point::

    from kafka_mcp import KafkaClient
    client = KafkaClient.from_env()
    topics = client.list_topics()

KafkaClient is defined in plan 01-03 (adapters/inbound/lib.py).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from kafka_mcp.domain.errors import ConfigError, TopicNotFoundError
from kafka_mcp.domain.models import PartitionInfo, TopicInfo

if TYPE_CHECKING:
    # KafkaClient will be created in plan 01-03; guard avoids circular
    # import until adapters/inbound/lib.py exists.
    from kafka_mcp.adapters.inbound.lib import KafkaClient

__all__ = [
    "KafkaClient",
    "TopicInfo",
    "PartitionInfo",
    "TopicNotFoundError",
    "ConfigError",
]
