"""Domain models — pure pydantic v2 data structures.

No I/O or framework imports. These are the canonical data contracts
used across all inbound and outbound adapters.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


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


def _default_evidence_keys() -> dict[str, str | None]:
    """Return an empty Evidence keys dict with all required identifiers set to None."""
    return {
        "order_id": None,
        "msisdn": None,
        "customer_id": None,
        "product_id": None,
    }


class KafkaMessage(BaseModel):
    """A single Kafka message as returned by the search/get domain operations.

    Carries both wire metadata (topic, partition, offset, key, headers,
    timestamp) and the Investigator Contract Evidence surface (source,
    event_type, keys).

    ``raw`` is kept as bytes in the domain object.  Inbound faces
    (REST/MCP/CLI) are responsible for base64-encoding before serialisation
    — never the model itself.

    ``timestamp_utc`` must be a UTC-aware datetime.  Adapters should
    convert broker CreateTime milliseconds via
    ``datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)``.
    When broker timestamp type is LogAppendTime or unset (-1) adapters
    should fall back gracefully (e.g. use ``datetime.now(timezone.utc)``
    and note the fallback).
    """

    topic: str
    partition: int
    offset: int
    key: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    value: dict[str, Any] | None = None
    timestamp_utc: datetime
    raw: bytes
    raw_key: bytes | None = None
    key_decoded: dict[str, Any] | None = None
    schema_id: dict[str, int | None] | None = None

    # --- Investigator Contract Evidence fields ---
    source: str = "kafka"
    event_type: str = "kafka_message"
    keys: dict[str, str | None] = Field(
        default_factory=_default_evidence_keys,
        description=("Extracted investigator identifiers.  Absent identifiers are None."),
    )


class LagRecord(BaseModel):
    """Per-partition consumer-group lag snapshot.

    Carries the committed-vs-end offset delta for a single partition,
    plus Investigator-Contract Evidence fields (source, event_type) so
    each row is usable as a timeline data point (LAG-03).

    ``timestamp_utc`` is the wall-clock UTC time at query execution —
    lag is a point-in-time snapshot, not a message timestamp.
    """

    group: str
    topic: str
    partition: int
    current_offset: int
    end_offset: int
    lag: int
    timestamp_utc: datetime

    # --- Investigator Contract Evidence fields ---
    source: str = "kafka"
    event_type: str = "consumer_lag"
