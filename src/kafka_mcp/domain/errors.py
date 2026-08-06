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


class BrokerUnreachableError(Exception):
    """Raised when an operation completed only because the broker was unreachable.

    Distinct from every "not found"/"empty" outcome in this module, and that
    distinction is the entire point. librdkafka reports connection-level
    failures — rejected TLS handshake, all brokers down, refused authentication
    — through an *asynchronous* error callback, not by raising from ``poll()``.
    A search against a broker that is refusing the connection therefore finishes
    normally and returns an empty list, which reads downstream as *this data
    does not exist*.

    Observed on ep-stage 2026-08-06: the broker was rejecting the client
    certificate and an investigation recorded Kafka as ``empty`` — no exception,
    no log line, nothing to investigate. "There is no such message" and "we
    never reached the broker" call for opposite responses from an operator;
    conflating them costs far more time than an explicit error ever does.

    Attributes:
        detail: The connection error librdkafka reported, verbatim. It carries
            the broker address and reason, never credentials.

    Example::

        raise BrokerUnreachableError("_SSL: broker certificate rejected")
    """

    def __init__(self, detail: str) -> None:
        super().__init__(
            f"Broker unreachable: {detail}. The operation returned no data, but the "
            f"connection failed — an empty result is only meaningful over a working link."
        )
        self.detail = detail


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
