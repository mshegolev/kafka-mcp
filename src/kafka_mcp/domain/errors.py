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
