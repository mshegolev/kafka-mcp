"""ConfluentConsumerAdapter — read-only Kafka broker adapter.

Implements ConsumerPort using librdkafka (confluent-kafka).

Read-only structural guarantee (KAFKA-06, D-05):
- Uses Consumer.assign() to attach to specific partitions; the subscription-based
  method is intentionally absent from this file (assign-only guarantee).
- enable.auto.commit=False is always set in the librdkafka conf dict.
- group.id is a throwaway uuid4-based name (kafka-mcp-ro-{uuid4}) per
  instance, so the consumer group can never collide with production groups.

T-02-01 (STRIDE): sasl_password is extracted via SecretStr.get_secret_value()
immediately before passing to Consumer(); the conf dict is local to __init__
and never stored or logged.
"""

from __future__ import annotations

from types import TracebackType
from uuid import uuid4

from confluent_kafka import Consumer, KafkaException

from kafka_mcp.config import KafkaMcpSettings
from kafka_mcp.domain.errors import TopicNotFoundError
from kafka_mcp.ports.consumer import ConsumerPort


class ConfluentConsumerAdapter:
    """Read-only Kafka consumer adapter backed by librdkafka.

    Implements :class:`kafka_mcp.ports.consumer.ConsumerPort`.

    Example::

        settings = KafkaMcpSettings(bootstrap_servers="broker:9092")
        with ConfluentConsumerAdapter(settings) as adapter:
            topics = adapter.list_topics()
            low, high = adapter.get_watermark_offsets("payments", 0)
    """

    def __init__(self, settings: KafkaMcpSettings) -> None:
        """Build the librdkafka Consumer from settings.

        Args:
            settings: Validated application settings.  ``bootstrap_servers``
                is required; SASL fields are included only when
                ``security_protocol`` is not ``"PLAINTEXT"``.
        """
        self._settings = settings

        conf: dict[str, object] = {
            "bootstrap.servers": settings.bootstrap_servers,
            # Read-only structural guarantees (KAFKA-06)
            "enable.auto.commit": False,
            "group.id": f"kafka-mcp-ro-{uuid4()}",
        }

        if settings.security_protocol != "PLAINTEXT":
            conf["security.protocol"] = settings.security_protocol

        # Only configure SASL when a mechanism is actually requested.
        # Keying on sasl_mechanism (not "not PLAINTEXT") means TLS-only
        # deployments (SECURITY_PROTOCOL=SSL) and SASL_SSL-without-explicit-
        # mechanism deployments construct cleanly instead of injecting a
        # None mechanism that librdkafka rejects at Consumer() construction.
        if settings.sasl_mechanism:
            conf["sasl.mechanism"] = settings.sasl_mechanism
            if settings.sasl_username is not None:
                conf["sasl.username"] = settings.sasl_username
            # Extract SecretStr value immediately — never stored in an
            # attribute or logged (T-02-01).
            if settings.sasl_password is not None:
                conf["sasl.password"] = settings.sasl_password.get_secret_value()

        self._consumer: Consumer = Consumer(conf)

    # ------------------------------------------------------------------
    # ConsumerPort implementation
    # ------------------------------------------------------------------

    def list_topics(self, include_internal: bool = False) -> list[str]:
        """Return a sorted list of topic names available on the broker.

        Args:
            include_internal: When ``False`` (default), topics whose names
                start with ``"__"`` (e.g. ``__consumer_offsets``) are
                excluded (D-09).

        Returns:
            Sorted list of topic name strings.
        """
        metadata = self._consumer.list_topics(timeout=10.0)
        names = list(metadata.topics.keys())
        if not include_internal:
            names = [n for n in names if not n.startswith("__")]
        return sorted(names)

    def get_partition_ids(self, topic: str) -> list[int]:
        """Return a sorted list of partition IDs for the given topic.

        Uses ``Consumer.list_topics(topic=topic)`` to fetch per-topic
        cluster metadata from librdkafka.

        Args:
            topic: Topic name.

        Returns:
            Sorted list of partition integer IDs.

        Raises:
            TopicNotFoundError: When the topic does not exist on the broker.
        """
        metadata = self._consumer.list_topics(
            topic=topic, timeout=10.0
        )
        topic_meta = metadata.topics.get(topic)
        if topic_meta is None or topic_meta.error is not None:
            raise TopicNotFoundError(topic)
        return sorted(topic_meta.partitions.keys())

    def get_watermark_offsets(
        self, topic: str, partition: int
    ) -> tuple[int, int]:
        """Return (low, high) watermark offsets for the given partition.

        Delegates to ``Consumer.get_watermark_offsets()`` (librdkafka)
        which issues a describe-request to the broker (D-06).

        Args:
            topic: Topic name.
            partition: Partition index (0-based).

        Returns:
            ``(low, high)`` as a tuple of ints (earliest, latest offsets).

        Raises:
            TopicNotFoundError: When librdkafka raises a KafkaException
                for an unknown topic/partition.
        """
        try:
            low, high = self._consumer.get_watermark_offsets(
                topic, partition, timeout=self._settings.poll_timeout
            )
        except KafkaException as exc:
            raise TopicNotFoundError(topic) from exc
        return (low, high)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "ConfluentConsumerAdapter":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close the underlying Consumer regardless of exceptions."""
        self._consumer.close()
