"""Domain models — pure pydantic v2 data structures.

No I/O or framework imports. These are the canonical data contracts
used across all inbound and outbound adapters.
"""

from __future__ import annotations

from pydantic import BaseModel


class PartitionInfo(BaseModel):
    """Metadata for a single Kafka partition."""

    id: int
    leader: int
    earliest: int
    latest: int


class TopicInfo(BaseModel):
    """Metadata for a Kafka topic including per-partition offsets."""

    name: str
    partition_count: int
    partitions: list[PartitionInfo]
