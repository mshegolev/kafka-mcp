"""SchemaRegistryPort — schema registry protocol.

Pure Protocol definition: no httpx or requests import here.
Outbound adapters implement this using HTTP calls to the real registry.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


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
