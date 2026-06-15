"""KafkaClient — public lib facade for the kafka-mcp brick.

This is the primary Investigator Contract entry point (D-02 library-first).
All other inbound adapters (MCP stdio, FastAPI, CLI) delegate to the same
KafkaClient methods rather than calling TopicService directly.

Usage::

    from kafka_mcp import KafkaClient
    client = KafkaClient.from_env()
    topics = client.list_topics()
    info = client.describe_topic("payments")
    messages = client.search_messages("ORD-123")
    message = client.get_message("orders", 0, 42)

For testing without a real broker, use dependency injection::

    client = KafkaClient(MockConsumer())
    topics = client.list_topics()

    # With a mock registry (Phase 2 ops):
    client = KafkaClient(MockConsumer(), MockSchemaRegistry())
    messages = client.search_messages("key")

T-03-01 (STRIDE): ConfigError message names only the missing key, never
any value.  Callers must not log exc.args as it may contain partial config.
"""

from __future__ import annotations

from types import TracebackType
from typing import Any

from kafka_mcp.adapters.outbound.confluent_consumer import (
    ConfluentConsumerAdapter,
)
from kafka_mcp.config import KafkaMcpSettings
from kafka_mcp.domain.models import KafkaMessage, LagRecord, TopicInfo
from kafka_mcp.domain.search_service import TopicService
from kafka_mcp.ports.consumer import ConsumerPort
from kafka_mcp.ports.schema_registry import SchemaRegistryPort


class _NullSchemaRegistry:
    """Minimal SchemaRegistryPort stub for backward-compatible construction.

    Returned when no registry is provided to KafkaClient(consumer).
    decode() returns None unconditionally — no credentials, no I/O,
    no errors (T-02-04-E: accepted risk).

    This enables Phase 1 test patterns (KafkaClient(MockConsumer())) to
    keep working without supplying a registry.  Decoded value will be None
    on all messages, which is acceptable for list_topics/describe_topic ops.
    """

    def get_schema(self, subject: str) -> dict | None:
        return None

    def decode(
        self,
        raw: bytes,
        topic: str = "",
        partition: int = 0,
        offset: int = 0,
    ) -> dict[str, Any] | None:
        return None


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

        # Test use with registry (Phase 2 search/decode operations)
        client = KafkaClient(MockConsumer(), MockSchemaRegistry())
    """

    def __init__(
        self,
        consumer: ConsumerPort,
        registry: SchemaRegistryPort | None = None,
    ) -> None:
        """Initialise with a ConsumerPort and an optional SchemaRegistryPort.

        Args:
            consumer: Any object satisfying the ConsumerPort Protocol.
                Pass a mock in tests; :meth:`from_env` wires the real
                adapter in production.
            registry: Any object satisfying the SchemaRegistryPort Protocol.
                When ``None`` (default), a :class:`_NullSchemaRegistry` is
                used, which returns ``None`` for all decode calls.  This
                keeps Phase 1 test patterns working without change.
                Pass a real or mock registry to enable Phase 2 operations.
        """
        self._consumer = consumer
        effective_registry: SchemaRegistryPort = registry if registry is not None else _NullSchemaRegistry()
        self._service = TopicService(consumer, effective_registry)

    @classmethod
    def from_env(cls) -> KafkaClient:
        """Construct a KafkaClient from environment variables.

        Reads configuration via :class:`kafka_mcp.config.KafkaMcpSettings`
        (env prefix ``KAFKA_MCP_``).  Fails fast if required variables
        are missing (D-04).

        Wires a real :class:`ConfluentConsumerAdapter` and a real
        :class:`SchemaRegistryHttpAdapter` from settings.

        Returns:
            A fully wired :class:`KafkaClient` backed by a real
            :class:`kafka_mcp.adapters.outbound.confluent_consumer.\
ConfluentConsumerAdapter` and a real
            :class:`kafka_mcp.adapters.outbound.schema_registry_http.\
SchemaRegistryHttpAdapter`.

        Raises:
            ConfigError: If ``KAFKA_MCP_BOOTSTRAP_SERVERS`` is not set
                or is empty.  The error message names the missing key
                only — never any credential value (T-03-01).
        """
        from kafka_mcp.adapters.outbound.schema_registry_http import (
            SchemaRegistryHttpAdapter,
        )

        settings = KafkaMcpSettings()  # raises ConfigError if missing
        consumer = ConfluentConsumerAdapter(settings)
        registry = SchemaRegistryHttpAdapter(
            url=settings.schema_registry_url,
            user=settings.sr_user,
            password=(settings.sr_pass.get_secret_value() if settings.sr_pass is not None else None),
        )
        return cls(consumer, registry)

    def list_topics(self, include_internal: bool = False) -> list[str]:
        """Return a sorted list of topic names available on the broker.

        Args:
            include_internal: When ``False`` (default), excludes topics
                whose names start with ``"__"`` (e.g.
                ``__consumer_offsets``).  Set to ``True`` to include them.

        Returns:
            Sorted list of topic name strings.
        """
        return self._service.list_topics(include_internal=include_internal)

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

    def search_messages(
        self,
        key: str,
        **kwargs: Any,
    ) -> list[KafkaMessage]:
        """Search for messages matching a key within a time window.

        Delegates to :meth:`TopicService.search_messages`.

        Args:
            key: The value to match.
            **kwargs: Forwarded to :meth:`TopicService.search_messages`.
                Supported keyword args: ``key_field``, ``topics``,
                ``time_from``, ``time_to``, ``limit``.

        Returns:
            List of :class:`KafkaMessage` objects with decoded values and
            evidence keys populated (Phase 2 SC-1).
        """
        return self._service.search_messages(key, **kwargs)

    def get_message(
        self,
        topic: str,
        partition: int,
        offset: int,
    ) -> KafkaMessage:
        """Fetch and decode a single message by exact coordinates.

        Delegates to :meth:`TopicService.get_message`.

        Args:
            topic: Topic name.
            partition: Partition index (0-based).
            offset: Exact message offset.

        Returns:
            :class:`KafkaMessage` with decoded value and evidence keys
            (Phase 2 SC-2).

        Raises:
            MessageNotFoundError: When the offset is out of watermark range.
            DecodeError: When the payload cannot be decoded.
        """
        return self._service.get_message(topic, partition, offset)

    def consumer_group_lag(self, group: str, topics: list[str] | None = None) -> list[LagRecord]:
        """Return per-partition consumer lag for a consumer group.

        Read-only query — delegates directly to the consumer port.
        No domain orchestration needed (no decode, no search logic).

        Args:
            group: Consumer group ID.
            topics: Optional list of topic names to filter. When None,
                reports lag for all topics with committed offsets.

        Returns:
            List of :class:`~kafka_mcp.domain.models.LagRecord` objects.
        """
        return self._consumer.consumer_group_lag(group, topics)

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

    def __enter__(self) -> KafkaClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close the underlying consumer regardless of exceptions."""
        self.close()
