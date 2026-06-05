"""Domain error types — typed exceptions for the Kafka domain layer.

No I/O or framework imports. All errors carry structured context so
callers can handle them without string parsing.
"""

from __future__ import annotations


class ConfigError(ValueError):
    """Raised when required configuration is missing or invalid.

    ConfigError is a ValueError subclass so it bubbles naturally through
    pydantic validators and standard exception hierarchies.

    Example::

        raise ConfigError("KAFKA_MCP_BOOTSTRAP_SERVERS is required but was not set")
    """


class TopicNotFoundError(Exception):
    """Raised when the requested Kafka topic does not exist on the broker.

    Attributes:
        topic: The topic name that was not found.

    Example::

        raise TopicNotFoundError("payments")
    """

    def __init__(self, topic: str) -> None:
        super().__init__(f"Topic not found: {topic!r}")
        self.topic = topic


class DecodeError(Exception):
    """Raised when a Kafka message payload cannot be decoded.

    Carries the message coordinates and a human-readable reason so
    callers can handle decode failures with full context.

    Attributes:
        topic: Topic name.
        partition: Partition index.
        offset: Message offset.
        reason: Human-readable description of the decode failure.

    Example::

        raise DecodeError("payments", 0, 42, "unknown magic byte 0x01")
    """

    def __init__(
        self, topic: str, partition: int, offset: int, reason: str
    ) -> None:
        super().__init__(
            f"Decode failed for {topic}[{partition}]@{offset}: {reason}"
        )
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.reason = reason


class TransientError(Exception):
    """Raised when a read operation fails for a transient/operational reason.

    Distinct from :class:`MessageNotFoundError`: the requested message may well
    exist (e.g. the offset is within the partition watermarks) but the broker
    did not return it within the poll budget. Callers should treat this as a
    retryable I/O condition, NOT as a definitive "not found" answer.

    Attributes:
        topic: Topic name.
        partition: Partition index.
        offset: Requested offset.
        reason: Human-readable description of the transient failure.

    Example::

        raise TransientError("orders", 0, 5, "poll timed out for in-range offset")
    """

    def __init__(
        self, topic: str, partition: int, offset: int, reason: str
    ) -> None:
        super().__init__(
            f"Transient read failure for {topic}[{partition}]@{offset}: {reason}"
        )
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.reason = reason


class MessageNotFoundError(Exception):
    """Raised when a message at the requested coordinates does not exist.

    Used by the single-message ``get_message`` path when the offset is
    beyond the partition watermarks or the partition is empty.

    Attributes:
        topic: Topic name.
        partition: Partition index.
        offset: Requested offset that was not found.

    Example::

        raise MessageNotFoundError("orders", 1, 9999)
    """

    def __init__(self, topic: str, partition: int, offset: int) -> None:
        super().__init__(f"No message at {topic}[{partition}]@{offset}")
        self.topic = topic
        self.partition = partition
        self.offset = offset
