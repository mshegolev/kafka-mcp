"""ConsumerPort — broker consumer protocol.

Pure Protocol definition: no confluent_kafka import here.
Outbound adapters (adapters/outbound/confluent_consumer.py) implement
this protocol using the real librdkafka client.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ConsumerPort(Protocol):
    """Protocol for read-only Kafka consumer operations.

    Implementations must:
    - Never commit offsets (assign-based, not subscribe-based)
    - Use a throwaway group id (kafka-mcp-ro-{uuid4})
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
