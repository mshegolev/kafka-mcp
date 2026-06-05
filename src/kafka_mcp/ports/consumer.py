"""ConsumerPort — broker consumer protocol.

Pure Protocol definition: no broker library imports here.
Outbound adapters implement this protocol using the real librdkafka client.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from kafka_mcp.domain.errors import MessageNotFoundError
from kafka_mcp.domain.models import KafkaMessage


@runtime_checkable
class ConsumerPort(Protocol):
    """Protocol for read-only Kafka consumer operations.

    Implementations must:
    - Never commit offsets (assign-based, not subscribe-based)
    - Use a throwaway group id (e.g. kafka-reader-ro-{uuid4})
    - Set enable.auto.commit=false
    """

    def list_topics(self, include_internal: bool = False) -> list[str]:
        """Return a list of topic names available on the broker.

        Args:
            include_internal: If False (default), exclude topics whose
                names start with '__' (e.g. __consumer_offsets).

        Returns:
            Sorted list of topic name strings.
        """
        ...

    def get_watermark_offsets(
        self, topic: str, partition: int
    ) -> tuple[int, int]:
        """Return (earliest, latest) offsets for the given partition.

        Args:
            topic: Topic name.
            partition: Partition index (0-based).

        Returns:
            Tuple of (earliest_offset, latest_offset).
        """
        ...

    def get_partition_ids(self, topic: str) -> list[int]:
        """Return a sorted list of partition IDs for the given topic.

        Args:
            topic: Topic name.

        Returns:
            Sorted list of partition integer IDs (e.g. [0, 1, 2]).

        Raises:
            TopicNotFoundError: When the topic does not exist on the broker.
        """
        ...

    def fetch_messages(
        self,
        topic: str,
        partition: int,
        start_offset: int,
        stop_offset: int,
        time_to: datetime | None,
        limit: int,
    ) -> list[KafkaMessage]:
        """Consume messages from [start_offset, stop_offset) bounded by time_to
        and limit.

        Returns KafkaMessage objects with raw bytes; decode is NOT performed
        here.  Never commits offsets (assign-based, KAFKA-06).

        Args:
            topic: Topic name.
            partition: Partition index (0-based).
            start_offset: Inclusive start offset for the scan.
            stop_offset: Exclusive stop offset for the scan.
            time_to: Optional upper bound on message timestamp (UTC-aware).
                Scan stops when the first message with
                ``timestamp_utc >= time_to`` is encountered.
            limit: Maximum number of messages to return from this call.

        Returns:
            List of KafkaMessage objects (may be empty).
        """
        ...

    def fetch_message(
        self,
        topic: str,
        partition: int,
        offset: int,
    ) -> KafkaMessage:
        """Fetch a single raw message by exact offset.

        Raises MessageNotFoundError when offset is beyond watermarks.
        Returns KafkaMessage with raw bytes; decode is NOT performed here.

        Args:
            topic: Topic name.
            partition: Partition index (0-based).
            offset: Exact offset to fetch.

        Returns:
            KafkaMessage at the given offset.

        Raises:
            MessageNotFoundError: When the offset is beyond the partition
                watermarks or no message exists at that offset.
        """
        ...

    def offsets_for_times(
        self,
        topic: str,
        partition: int,
        timestamp_ms: int,
    ) -> int:
        """Return the start offset for messages at or after timestamp_ms.

        Returns the earliest offset when timestamp_ms is before the first
        message in the partition (OFFSET_BEGINNING sentinel = -2).

        Args:
            topic: Topic name.
            partition: Partition index (0-based).
            timestamp_ms: POSIX millisecond timestamp to seek from.

        Returns:
            Offset of the first message at or after timestamp_ms, or -2
            (OFFSET_BEGINNING) if timestamp_ms is before the earliest message.
        """
        ...
