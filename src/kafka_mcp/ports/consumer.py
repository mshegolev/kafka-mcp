"""ConsumerPort — broker consumer protocol.

Pure Protocol definition: no broker library imports here.
Outbound adapters implement this protocol using the real librdkafka client.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


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
