"""KafkaClient — public lib facade for the kafka-mcp brick.

This is the primary Investigator Contract entry point (D-02 library-first).
All other inbound adapters (MCP stdio, FastAPI, CLI) delegate to the same
KafkaClient methods rather than calling TopicService directly.

Usage::

    from kafka_mcp import KafkaClient
    client = KafkaClient.from_env()
    topics = client.list_topics()
    info = client.describe_topic("payments")

For testing without a real broker, use dependency injection::

    client = KafkaClient(MockConsumer())
    topics = client.list_topics()

T-03-01 (STRIDE): ConfigError message names only the missing key, never
any value.  Callers must not log exc.args as it may contain partial config.
"""

from __future__ import annotations

from types import TracebackType

from kafka_mcp.adapters.outbound.confluent_consumer import (
    ConfluentConsumerAdapter,
)
from kafka_mcp.config import KafkaMcpSettings
from kafka_mcp.domain.models import TopicInfo
from kafka_mcp.domain.search_service import TopicService
from kafka_mcp.ports.consumer import ConsumerPort


class KafkaClient:
    """Public lib facade — entry point for programmatic Kafka access.

    Wraps :class:`kafka_mcp.domain.search_service.TopicService` with
    a clean public API.  Accepts a ``ConsumerPort`` for dependency
    injection, enabling mock-based tests with no real broker.

    Example::

        # Production use
        client = KafkaClient.from_env()

        # Test use (mock consumer, no broker required)
        client = KafkaClient(MockConsumer())
    """

    def __init__(self, consumer: ConsumerPort) -> None:
        """Initialise with a ConsumerPort implementation.

        Args:
            consumer: Any object satisfying the ConsumerPort Protocol.
                Pass a mock in tests; :meth:`from_env` wires the real
                adapter in production.
        """
        self._consumer = consumer
        self._service = TopicService(consumer)

    @classmethod
    def from_env(cls) -> KafkaClient:
        """Construct a KafkaClient from environment variables.

        Reads configuration via :class:`kafka_mcp.config.KafkaMcpSettings`
        (env prefix ``KAFKA_MCP_``).  Fails fast if required variables
        are missing (D-04).

        Returns:
            A fully wired :class:`KafkaClient` backed by a real
            :class:`kafka_mcp.adapters.outbound.confluent_consumer.\
ConfluentConsumerAdapter`.

        Raises:
            ConfigError: If ``KAFKA_MCP_BOOTSTRAP_SERVERS`` is not set
                or is empty.  The error message names the missing key
                only — never any credential value (T-03-01).
        """
        settings = KafkaMcpSettings()  # raises ConfigError if missing
        consumer = ConfluentConsumerAdapter(settings)
        return cls(consumer)

    def list_topics(self, include_internal: bool = False) -> list[str]:
        """Return a sorted list of topic names available on the broker.

        Args:
            include_internal: When ``False`` (default), excludes topics
                whose names start with ``"__"`` (e.g.
                ``__consumer_offsets``).  Set to ``True`` to include them.

        Returns:
            Sorted list of topic name strings.
        """
        return self._service.list_topics(
            include_internal=include_internal
        )

    def describe_topic(self, topic: str) -> TopicInfo:
        """Return detailed metadata for a single topic.

        Args:
            topic: Exact topic name to describe.

        Returns:
            :class:`kafka_mcp.domain.models.TopicInfo` with
            ``partition_count`` and a ``partitions`` list of
            :class:`kafka_mcp.domain.models.PartitionInfo` objects
            carrying ``earliest`` and ``latest`` offsets (Phase 1 SC-2).

        Raises:
            TopicNotFoundError: If the topic does not exist on the broker.
        """
        return self._service.describe_topic(topic)

    # ------------------------------------------------------------------
    # Lifecycle (WR-02): the underlying librdkafka Consumer holds
    # background threads/sockets that must be released on shutdown.
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying consumer, releasing broker resources.

        Delegates to the injected consumer's ``close()`` when available.
        Mock consumers used in tests may omit ``close()``; in that case
        this is a no-op so dependency-injected test doubles keep working.
        """
        close = getattr(self._consumer, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> "KafkaClient":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close the underlying consumer regardless of exceptions."""
        self.close()
