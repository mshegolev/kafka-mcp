"""SchemaRegistryHttpAdapter — real decode implementation for Confluent Schema Registry.

Wire-format detection:
  - Confluent magic byte (0x00) + 4-byte big-endian schema_id → SR lookup → Avro/Protobuf decode
  - No magic byte → json.loads fallback
  - Failure → typed DecodeError (never an unhandled exception)

Protobuf decode (generic, no pre-compiled classes):
  - The Confluent Protobuf framing is magic(1) + schema_id(4) + a message-index
    header (a varint count followed by that many varint indices; a single 0x00
    byte is the common "first message" shorthand) followed by the serialized
    message bytes.
  - The registered ``.proto`` source (``schema.schema_str``) is compiled to a
    ``FileDescriptorSet`` at decode time via the ``protoc`` binary, loaded into
    a private ``DescriptorPool``, and the concrete message type selected by the
    message-index path. The message is then parsed and rendered to a plain dict
    via ``MessageToDict``.
  - If ``protoc`` is unavailable, or the message-index path cannot be resolved,
    a typed ``DecodeError`` is raised (never an unhandled exception). See
    ``_decode_protobuf`` for the precise limitation notes.

Security (T-02-02-B):
  - sr_pass is passed directly to SchemaRegistryClient conf dict; NOT stored as attribute
  - default object repr shows no attribute values (plain class, not dataclass/pydantic)

Hexagonal boundary:
  - All decode library imports (confluent_kafka.schema_registry, fastavro, google.protobuf)
    live ONLY in this outbound adapter; domain/ and ports/ stay import-free.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

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
        # Compiled Protobuf FileDescriptor cache keyed by raw schema_str, so a
        # given .proto is compiled via protoc at most once per adapter lifetime.
        self._proto_descriptor_cache: dict[str, object] = {}

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
        """Decode Confluent-framed Protobuf bytes to a plain dict (generic).

        Generic decode without pre-compiled message classes:
          1. Skip the framing header (magic + schema_id), then read the
             Protobuf message-index header (varint count + that many varint
             indices; a single 0x00 byte is the "first message" shorthand).
          2. Compile ``schema.schema_str`` (the registered ``.proto`` source)
             to a ``FileDescriptorSet`` via the ``protoc`` binary, load it into
             a private ``DescriptorPool``, and resolve the concrete message
             type by walking the message-index path over the file's top-level
             message types.
          3. Build a dynamic message class via ``message_factory``, parse the
             remaining bytes, and render to a dict via ``MessageToDict``.

        Limitation: requires a ``protoc`` binary on PATH. When it is absent (or
        the schema references imports we cannot resolve), a typed DecodeError is
        raised — never an unhandled exception (T-02-02-D). Pre-compiled message
        types are NOT required.
        """
        try:
            index_path, payload = _strip_protobuf_index_header(
                raw[_FRAMING_HEADER_LEN:]
            )
            file_descriptor = self._compile_proto_descriptor(schema)
            message_descriptor = _resolve_message_by_index(
                file_descriptor, index_path
            )

            from google.protobuf import message_factory

            message = message_factory.GetMessageClass(message_descriptor)()
            message.ParseFromString(payload)

            from google.protobuf.json_format import MessageToDict

            return MessageToDict(message)
        except DecodeError:
            raise
        except Exception as exc:
            raise DecodeError(
                topic, partition, offset,
                reason=f"protobuf decode failed: {exc}",
            ) from exc

    def _compile_proto_descriptor(self, schema: Schema):
        """Compile a registered ``.proto`` schema to a FileDescriptor.

        Uses the ``protoc`` binary to emit a FileDescriptorSet, then loads it
        into this adapter's private DescriptorPool (cached per schema_str so a
        given schema is compiled at most once per adapter lifetime).

        Raises:
            DecodeError-friendly RuntimeError on failure (the caller wraps it).
        """
        schema_str = schema.schema_str or ""
        cached = self._proto_descriptor_cache.get(schema_str)
        if cached is not None:
            return cached

        protoc = shutil.which("protoc")
        if protoc is None:
            raise RuntimeError(
                "protoc binary not found on PATH; generic protobuf decode "
                "requires protoc to compile the registered .proto schema"
            )

        from google.protobuf import descriptor_pb2, descriptor_pool

        with tempfile.TemporaryDirectory() as tmpdir:
            proto_path = os.path.join(tmpdir, "schema.proto")
            out_path = os.path.join(tmpdir, "schema.desc")
            with open(proto_path, "w", encoding="utf-8") as fh:
                fh.write(schema_str)

            completed = subprocess.run(
                [
                    protoc,
                    f"--proto_path={tmpdir}",
                    f"--descriptor_set_out={out_path}",
                    "--include_imports",
                    proto_path,
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            if completed.returncode != 0:
                raise RuntimeError(
                    f"protoc failed: {completed.stderr.strip()}"
                )

            with open(out_path, "rb") as fh:
                fds = descriptor_pb2.FileDescriptorSet()
                fds.ParseFromString(fh.read())

        # Use a fresh pool per schema to avoid duplicate-symbol collisions
        # across different schemas registered under different ids.
        pool = descriptor_pool.DescriptorPool()
        file_descriptor = None
        for file_proto in fds.file:
            file_descriptor = pool.Add(file_proto)

        if file_descriptor is None:
            raise RuntimeError("protoc produced an empty descriptor set")

        self._proto_descriptor_cache[schema_str] = file_descriptor
        return file_descriptor

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


# ---------------------------------------------------------------------------- #
# Protobuf framing helpers (module-level, pure)                                 #
# ---------------------------------------------------------------------------- #


def _read_varint(buf: bytes, pos: int) -> tuple[int, int]:
    """Read a base-128 varint from ``buf`` starting at ``pos``.

    Returns ``(value, new_pos)``. Raises ValueError if the buffer ends mid
    varint (truncated/corrupt framing).
    """
    result = 0
    shift = 0
    while True:
        if pos >= len(buf):
            raise ValueError("truncated varint in protobuf message-index header")
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            break
        shift += 7
    return result, pos


def _strip_protobuf_index_header(buf: bytes) -> tuple[list[int], bytes]:
    """Strip the Confluent Protobuf message-index header.

    ``buf`` is the payload AFTER the magic+schema_id framing. The header is a
    varint ``count`` followed by ``count`` varint indices; a single ``0x00``
    byte is the common shorthand for "the first message type" (an empty index
    path == ``[0]``).

    Returns ``(index_path, remaining_payload)`` where ``index_path`` is the
    list of message indices to walk (defaulting to ``[0]`` for the shorthand).
    """
    if not buf:
        return [0], b""

    # Shorthand: a single 0x00 means count==0 → first message type.
    count, pos = _read_varint(buf, 0)
    if count == 0:
        return [0], buf[pos:]

    index_path: list[int] = []
    for _ in range(count):
        value, pos = _read_varint(buf, pos)
        index_path.append(value)
    return index_path, buf[pos:]


def _resolve_message_by_index(file_descriptor: object, index_path: list[int]):
    """Resolve a message Descriptor by walking the message-index path.

    The first index selects a top-level message type in the file; subsequent
    indices select nested message types within it.

    Raises ValueError if any index is out of range.
    """
    top_level = list(file_descriptor.message_types_by_name.values())  # type: ignore[attr-defined]
    if not index_path:
        index_path = [0]

    first = index_path[0]
    if first < 0 or first >= len(top_level):
        raise ValueError(
            f"protobuf message index {first} out of range "
            f"(file declares {len(top_level)} top-level message types)"
        )
    descriptor = top_level[first]

    for idx in index_path[1:]:
        nested = list(descriptor.nested_types)
        if idx < 0 or idx >= len(nested):
            raise ValueError(
                f"nested protobuf message index {idx} out of range"
            )
        descriptor = nested[idx]

    return descriptor
