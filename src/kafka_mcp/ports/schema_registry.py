"""SchemaRegistryPort — schema registry protocol.

Pure Protocol definition: no HTTP library imports here.
Outbound adapters implement this using HTTP calls to the real registry.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from kafka_mcp.domain.errors import DecodeError


@runtime_checkable
class SchemaRegistryPort(Protocol):
    """Protocol for Schema Registry operations.

    Implementations fetch Avro/Protobuf/JSON schemas by subject name.
    Phase 1 ships a stub adapter; full decode is KAFKA-05 (Phase 2).
    """

    def get_schema(self, subject: str) -> dict | None:
        """Fetch the latest schema for the given subject.

        Args:
            subject: Schema subject name (e.g. "payments-value").

        Returns:
            Schema dict if found, None if subject does not exist.
        """
        ...

    def decode(
        self,
        raw: bytes,
        topic: str = "",
        partition: int = 0,
        offset: int = 0,
    ) -> dict[str, Any] | None:
        """Decode a raw wire-format message payload to a plain dict.

        Returns None when the payload cannot be decoded (resilient path
        for search_messages).  Raises DecodeError (for get_message path —
        callers choose whether to propagate).

        Confluent magic-byte framing (0x00 + 4-byte schema_id) is handled
        in the implementation; json.loads fallback when framing is absent.

        The optional ``topic``/``partition``/``offset`` parameters carry the
        message coordinates so a raised :class:`DecodeError` reports the real
        location instead of a placeholder ``[0]@0`` (WR-01/CR-02). Callers on
        the strict ``get_message`` path SHOULD pass them; the search path may
        omit them since it swallows DecodeError.

        Args:
            raw: Raw bytes from the Kafka message value field.
            topic: Topic name (for DecodeError context).
            partition: Partition index (for DecodeError context).
            offset: Message offset (for DecodeError context).

        Returns:
            Decoded dict, or None if decoding is not possible.

        Raises:
            DecodeError: When the caller wants a hard failure on decode
                errors (single-message get_message path).
        """
        ...
