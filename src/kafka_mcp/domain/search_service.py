"""TopicService — domain orchestration for Kafka topic operations.

Pure domain service: imports only ports and domain types.
No broker library, HTTP library, or web framework imports are present
here — enforced by the hexagonal boundary assertion in
tests/test_lib.py (Phase 1 SC-3).
"""

from __future__ import annotations

from kafka_mcp.domain.models import PartitionInfo, TopicInfo
from kafka_mcp.ports.consumer import ConsumerPort


class TopicService:
    """Domain service that orchestrates topic queries via ConsumerPort.

    All Kafka I/O is delegated to the injected ConsumerPort — this
    class contains zero I/O code (hexagonal architecture, D-07).

    Example::

        from kafka_mcp.domain.search_service import TopicService
        from kafka_mcp.adapters.outbound import ConfluentConsumerAdapter
        svc = TopicService(ConfluentConsumerAdapter(settings))
        topics = svc.list_topics()
        info = svc.describe_topic("payments")
    """

    def __init__(self, consumer: ConsumerPort) -> None:
        """Initialise with a ConsumerPort implementation.

        Args:
            consumer: Any object that satisfies the ConsumerPort Protocol.
                Injected so tests can pass a MockConsumer with no real
                broker.
        """
        self._consumer = consumer

    def list_topics(self, include_internal: bool = False) -> list[str]:
        """Return a list of topic names available on the broker.

        Args:
            include_internal: When ``False`` (default), excludes topics
                whose names start with ``"__"`` (D-09).

        Returns:
            Sorted list of topic name strings.
        """
        return self._consumer.list_topics(
            include_internal=include_internal
        )

    def describe_topic(self, topic: str) -> TopicInfo:
        """Return detailed metadata for a single topic.

        Retrieves partition IDs via ``ConsumerPort.get_partition_ids``
        (raises ``TopicNotFoundError`` if the topic does not exist),
        then fetches watermark offsets per partition.

        Args:
            topic: Exact topic name to describe.

        Returns:
            ``TopicInfo`` with ``partition_count`` and per-partition
            ``PartitionInfo`` objects carrying earliest/latest offsets.

        Raises:
            TopicNotFoundError: If the topic does not exist on the broker.

        Note:
            ``PartitionInfo.leader`` is set to ``0`` as a placeholder.
            Leader info requires an AdminClient call — deferred to
            Phase 2 per CONTEXT.md (AdminClient-based metadata APIs
            are out of scope for Phase 1 v1).
            # TODO: AdminClient — wire real leader in Phase 2
        """
        # Raises TopicNotFoundError if topic absent (T-03-03 mitigation)
        partition_ids = self._consumer.get_partition_ids(topic)

        partitions: list[PartitionInfo] = []
        for pid in partition_ids:
            low, high = self._consumer.get_watermark_offsets(topic, pid)
            partitions.append(
                PartitionInfo(
                    id=pid,
                    leader=0,  # TODO: AdminClient — wire real leader (Phase 2)
                    earliest=low,
                    latest=high,
                )
            )

        return TopicInfo(
            name=topic,
            partition_count=len(partitions),
            partitions=partitions,
        )
