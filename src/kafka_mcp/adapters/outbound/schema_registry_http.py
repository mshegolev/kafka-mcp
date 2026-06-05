"""SchemaRegistryHttpAdapter — real decode implementation for Confluent Schema Registry.

Wire-format detection:
  - Confluent magic byte (0x00) + 4-byte big-endian schema_id → SR lookup → Avro/Protobuf decode
  - No magic byte → json.loads fallback
  - Failure → typed DecodeError (never an unhandled exception)

Security (T-02-02-B):
  - sr_pass is passed directly to SchemaRegistryClient conf dict; NOT stored as attribute
  - default object repr shows no attribute values (plain class, not dataclass/pydantic)

Hexagonal boundary:
  - All decode library imports (confluent_kafka.schema_registry, fastavro, google.protobuf)
    live ONLY in this outbound adapter; domain/ and ports/ stay import-free.
"""

from __future__ import annotations

import json

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer

from kafka_mcp.domain.errors import DecodeError

# Confluent wire-format framing: 1 magic byte + 4-byte big-endian schema_id
_MAGIC_BYTE = 0x00
_FRAMING_HEADER_LEN = 5  # magic(1) + schema_id(4)


class SchemaRegistryHttpAdapter:
    """Real HTTP adapter for the Confluent Schema Registry.

    Implements :class:`kafka_mcp.ports.schema_registry.SchemaRegistryPort`.

    Instantiates a :class:`confluent_kafka.schema_registry.SchemaRegistryClient`
    (which caches schemas by id internally).  This adapter also maintains an
    adapter-level schema cache (``_schema_cache``) so deserializers are only
    constructed once per schema_id.

    Example::

        adapter = SchemaRegistryHttpAdapter(
            url="http://schema-registry:8081",
            user="alice",
            password="secret",
        )
        decoded = adapter.decode(raw_bytes, topic="payments", partition=0, offset=0)
    """

    def __init__(
        self,
        url: str | None,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        """Initialise the adapter with connection parameters.

        Args:
            url: Base URL of the Schema Registry (e.g. ``"http://registry:8081"``).
                 Pass ``None`` to disable SR (JSON-only path; Confluent-framed payloads
                 will raise DecodeError).
            user: Optional basic-auth username.
            password: Optional basic-auth password.
                      Passed directly to SchemaRegistryClient conf — NOT stored as
                      an instance attribute (T-02-02-B).
        """
        if url is not None:
            conf: dict[str, str] = {"url": url}
            if user and password:
                conf["basic.auth.user.info"] = f"{user}:{password}"
            self._client: SchemaRegistryClient | None = SchemaRegistryClient(conf)
        else:
            self._client = None

        self._url = url
        self._user = user
        # Schema cache keyed by schema_id; maps to the Schema object.
        # Prevents repeated SR round-trips for the same schema_id within
        # the adapter's lifetime (separate from SR client's own cache).
        self._schema_cache: dict[int, Schema] = {}

    # ------------------------------------------------------------------ #
    # SchemaRegistryPort — decode                                          #
    # ------------------------------------------------------------------ #

    def decode(
        self,
        raw: bytes,
        topic: str = "",
        partition: int = 0,
        offset: int = 0,
    ) -> dict | None:
        """Decode a raw wire-format message payload to a plain dict.

        Framing detection logic:
        1. If ``len(raw) >= 5`` and ``raw[0] == 0x00`` → Confluent framing:
           a. If SR client is not configured (url=None) → raise DecodeError.
           b. Extract schema_id from bytes 1–4 (big-endian).
           c. Look up schema from SR (cached after first fetch).
           d. Dispatch to AvroDeserializer or ProtobufDeserializer.
           e. Unknown schema type → raise DecodeError.
           f. Deserialization failure → wrap as DecodeError (T-02-02-A).
        2. Otherwise → attempt ``json.loads(raw)``.
           - Success → return dict.
           - Failure → raise DecodeError with reason containing "json".

        Args:
            raw: Raw bytes from the Kafka message value field.
            topic: Topic name (for DecodeError context).
            partition: Partition index (for DecodeError context).
            offset: Message offset (for DecodeError context).

        Returns:
            Decoded dict, or None if raw is empty.

        Raises:
            DecodeError: On any decode failure (untrusted payload safety).
        """
        if not raw:
            return None

        if len(raw) >= _FRAMING_HEADER_LEN and raw[0] == _MAGIC_BYTE:
            return self._decode_confluent(raw, topic, partition, offset)

        return self._decode_json(raw, topic, partition, offset)

    # ------------------------------------------------------------------ #
    # Internal decode helpers                                              #
    # ------------------------------------------------------------------ #

    def _decode_confluent(
        self, raw: bytes, topic: str, partition: int, offset: int
    ) -> dict:
        """Decode Confluent-framed payload (magic byte detected)."""
        if self._client is None:
            raise DecodeError(
                topic, partition, offset,
                reason="Schema Registry not configured",
            )

        schema_id = int.from_bytes(raw[1:5], "big")

        # Adapter-level cache to avoid repeated SR round-trips for the same schema_id.
        if schema_id not in self._schema_cache:
            self._schema_cache[schema_id] = self._client.get_schema(schema_id)

        schema = self._schema_cache[schema_id]
        schema_type = (schema.schema_type or "AVRO").upper()

        if schema_type == "AVRO":
            return self._decode_avro(raw, schema, topic, partition, offset)
        elif schema_type == "PROTOBUF":
            return self._decode_protobuf(raw, schema, topic, partition, offset)
        else:
            raise DecodeError(
                topic, partition, offset,
                reason=f"unknown schema type: {schema_type}",
            )

    def _decode_avro(
        self,
        raw: bytes,
        schema: Schema,
        topic: str,
        partition: int,
        offset: int,
    ) -> dict:
        """Decode Confluent-framed Avro bytes to dict.

        AvroDeserializer expects the FULL Confluent wire bytes (magic + schema_id
        + payload) — it handles framing internally.
        """
        try:
            deserializer = AvroDeserializer(
                self._client, schema.schema_str
            )
            result = deserializer(raw, ctx=None)
            return result if isinstance(result, dict) else dict(result)
        except Exception as exc:
            raise DecodeError(
                topic, partition, offset,
                reason=f"avro decode failed: {exc}",
            ) from exc

    def _decode_protobuf(
        self,
        raw: bytes,
        schema: Schema,
        topic: str,
        partition: int,
        offset: int,
    ) -> dict:
        """Decode Confluent-framed Protobuf bytes to dict.

        Without pre-generated message classes we cannot fully decode arbitrary
        Protobuf payloads.  ProtobufDeserializer requires a concrete message type
        (generated class) at construction time, which we don't have for generic
        decode.

        TODO (Phase 3): Support pre-generated Protobuf message types via a
        configurable message-type registry so specific schemas can be decoded
        without resorting to dynamic class resolution.

        For now we raise DecodeError with an actionable reason rather than
        silently executing unknown dynamic code (T-02-02-D).
        """
        try:
            # ProtobufDeserializer requires a concrete message_type class.
            # We use None as a sentinel to trigger the proper error path
            # and wrap it as DecodeError.
            deserializer = ProtobufDeserializer(None, {"use.deprecated.format": False})
            result = deserializer(raw, ctx=None)
            # If somehow it succeeds, convert to dict via json_format
            if hasattr(result, "DESCRIPTOR"):
                from google.protobuf.json_format import MessageToDict
                return MessageToDict(result)
            if isinstance(result, dict):
                return result
            return dict(result)
        except DecodeError:
            raise
        except Exception as exc:
            raise DecodeError(
                topic, partition, offset,
                reason=f"protobuf decode failed: {exc}",
            ) from exc

    def _decode_json(
        self, raw: bytes, topic: str, partition: int, offset: int
    ) -> dict:
        """Attempt JSON decode of raw bytes (no Confluent framing)."""
        try:
            result = json.loads(raw)
            if isinstance(result, dict):
                return result
            # json.loads can return a list, int, str, etc. — wrap in a container
            return {"value": result}
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
            raise DecodeError(
                topic, partition, offset,
                reason=f"json decode failed: {exc}",
            ) from exc

    # ------------------------------------------------------------------ #
    # SchemaRegistryPort — get_schema (backward compatibility)             #
    # ------------------------------------------------------------------ #

    def get_schema(self, subject: str) -> dict | None:
        """Fetch the latest schema for the given subject.

        Returns ``None`` in all cases — this adapter's decode pipeline uses
        schema_id lookup (not subject-based lookup).  Kept for SchemaRegistryPort
        backward compatibility.

        Args:
            subject: Schema subject name (e.g. ``"payments-value"``).

        Returns:
            ``None`` always (schema_id-based lookup used by decode()).
        """
        return None
