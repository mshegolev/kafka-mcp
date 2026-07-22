"""TopicService — domain orchestration for Kafka topic operations.

Pure domain service: imports only ports and domain types.
No broker library, HTTP library, or web framework imports are present
here — enforced by the hexagonal boundary assertion in
tests/test_lib.py (Phase 1 SC-3).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from kafka_mcp.domain.errors import DecodeError
from kafka_mcp.domain.models import KafkaMessage, PartitionInfo, TopicInfo
from kafka_mcp.ports.consumer import ConsumerPort
from kafka_mcp.ports.schema_registry import SchemaRegistryPort

# OFFSET_BEGINNING sentinel value from librdkafka / confluent-kafka.
# Returned by offsets_for_times() when the requested timestamp is before
# the first message in the partition.
_OFFSET_BEGINNING = -2


def _extract_schema_id(raw: bytes | None) -> int | None:
    """Extract schema_id from Confluent wire-framed bytes (if present).

    The Confluent Schema Registry wire format starts with a magic byte (0x00)
    followed by a 4-byte big-endian schema ID.  Returns the schema ID integer
    when ``raw`` carries this framing; returns ``None`` for plain/unframed
    payloads, short bytes, or ``None`` input.

    Length guard (T-04-03 mitigation): ``len(raw) >= 5`` is checked BEFORE
    any index access so short/malformed byte sequences never panic.

    Args:
        raw: Raw message bytes (value or key).  May be ``None``.

    Returns:
        Integer schema ID when framed; ``None`` otherwise.
    """
    if not raw or len(raw) < 5 or raw[0] != 0x00:
        return None
    return int.from_bytes(raw[1:5], "big")


def _decode_key(
    raw_key: bytes | None,
    registry: SchemaRegistryPort,
    topic: str,
    partition: int,
    offset: int,
) -> dict | None:
    """Decode message key via SchemaRegistryPort (resilient path).

    Attempts decode ONLY when ``raw_key`` carries Confluent framing
    (magic byte 0x00 at index 0, length >= 5).  Plain/string keys return
    ``None`` without calling the registry.  ``DecodeError`` is caught and
    swallowed — the message is never dropped due to a key decode failure
    (mirrors the resilient value-decode pattern in ``search_messages``).

    Reuses the existing ``SchemaRegistryPort.decode()`` method — no new
    ``decode_key`` method is added to the port (D-KEY-01 decision).

    Args:
        raw_key: Raw key bytes from the Kafka message.  May be ``None``.
        registry: Injected :class:`~kafka_mcp.ports.schema_registry.SchemaRegistryPort`.
        topic: Topic name (forwarded to ``registry.decode``).
        partition: Partition index (forwarded to ``registry.decode``).
        offset: Message offset (forwarded to ``registry.decode``).

    Returns:
        Decoded key dict when decode succeeds; ``None`` otherwise.
    """
    if not raw_key or len(raw_key) < 5 or raw_key[0] != 0x00:
        return None
    try:
        return registry.decode(raw_key, topic, partition, offset)
    except DecodeError:
        return None


def _matches_key(msg: KafkaMessage, key: str, key_field: str | None) -> bool:
    """Return True when the message matches key under the given key_field.

    Key field semantics (per CONTEXT.md):
    - None / "key"          → compare msg.key == key
    - "header:<name>"       → compare msg.headers.get(name) == key
    - "value:<dotted.path>" → traverse decoded value dict, compare leaf == key

    Any traversal error (missing key, value=None) returns False silently —
    T-02-04-A: dotted path uses str-split + dict key access only; no eval/getattr.
    """
    if key_field is None or key_field == "key":
        return msg.key == key

    if key_field.startswith("header:"):
        header_name = key_field[7:]
        return msg.headers.get(header_name) == key

    if key_field.startswith("value:"):
        path = key_field[6:]
        if msg.value is None:
            return False
        try:
            current: Any = msg.value
            for segment in path.split("."):
                if not isinstance(current, dict):
                    return False
                current = current[segment]
            return str(current) == key
        except (KeyError, TypeError):
            return False

    return False


def _matches_headers(msg: KafkaMessage, headers: dict[str, str] | None) -> bool:
    """Return True when the message matches all specified header key-value pairs.

    Args:
        msg: The KafkaMessage to check
        headers: Dictionary of header key-value pairs to match, or None for no filtering

    Returns:
        True if all specified headers match exactly, False otherwise
    """
    if headers is None or len(headers) == 0:
        return True

    for header_name, header_value in headers.items():
        if msg.headers.get(header_name) != header_value:
            return False

    return True


def _extract_evidence_keys(
    value: dict[str, Any] | None,
    headers: dict[str, str],
) -> dict[str, str | None]:
    """Extract Investigator Contract Evidence identifiers from value + headers.

    Well-known aliases per identifier (checked in value first, then headers):
    - order_id:    "order_id", "orderId", "order-id"
    - msisdn:      "msisdn", "phone", "phoneNumber"
    - customer_id: "customer_id", "customerId"
    - product_id:  "product_id", "productId"

    Returns a dict with all four keys; absent identifiers are None.
    """
    _aliases: dict[str, list[str]] = {
        "order_id": ["order_id", "orderId", "order-id"],
        "msisdn": ["msisdn", "phone", "phoneNumber"],
        "customer_id": ["customer_id", "customerId"],
        "product_id": ["product_id", "productId"],
    }

    result: dict[str, str | None] = {
        "order_id": None,
        "msisdn": None,
        "customer_id": None,
        "product_id": None,
    }

    for evidence_key, aliases in _aliases.items():
        found: str | None = None

        # Check decoded value dict first
        if value is not None:
            for alias in aliases:
                raw_val = value.get(alias)
                if raw_val is not None:
                    found = str(raw_val)
                    break

        # Fall back to headers
        if found is None:
            for alias in aliases:
                header_val = headers.get(alias)
                if header_val is not None:
                    found = header_val
                    break

        result[evidence_key] = found

    return result


def _extract_correlation_ids(msg: KafkaMessage) -> set[str]:
    """Extract correlation IDs from message value and headers.

    Looks for common correlation field names in both message value and headers.
    Returns a set of unique correlation IDs found.
    """
    correlation_fields = {
        "trace_id",
        "traceId",
        "trace-id",
        "correlation_id",
        "correlationId",
        "correlation-id",
        "request_id",
        "requestId",
        "request-id",
        "transaction_id",
        "transactionId",
        "transaction-id",
        "span_id",
        "spanId",
        "span-id",
        "parent_id",
        "parentId",
        "parent-id",
        "order_id",
        "orderId",
        "order-id",
        "customer_id",
        "customerId",
        "customer-id",
        "session_id",
        "sessionId",
        "session-id",
    }

    ids: set[str] = set()

    # Extract from value
    if msg.value is not None and isinstance(msg.value, dict):
        for field in correlation_fields:
            val = msg.value.get(field)
            if val is not None:
                ids.add(str(val))

    # Extract from headers
    for field in correlation_fields:
        val = msg.headers.get(field)
        if val is not None:
            ids.add(val)

    # Also check evidence keys for correlation IDs
    for val in msg.keys.values():
        if val is not None:
            ids.add(val)

    return ids


class TopicService:
    """Domain service that orchestrates topic queries via ConsumerPort.

    All Kafka I/O is delegated to the injected ConsumerPort; all decode
    logic is delegated to the injected SchemaRegistryPort — this class
    contains zero I/O code (hexagonal architecture, D-07).

    Example::

        from kafka_mcp.domain.search_service import TopicService
        from kafka_mcp.adapters.outbound import ConfluentConsumerAdapter
        from kafka_mcp.adapters.outbound.schema_registry_http import (
            SchemaRegistryHttpAdapter,
        )
        svc = TopicService(
            ConfluentConsumerAdapter(settings),
            SchemaRegistryHttpAdapter(settings.schema_registry_url),
        )
        topics = svc.list_topics()
        info = svc.describe_topic("payments")
        messages = svc.search_messages("ORD-123")
    """

    def __init__(
        self,
        consumer: ConsumerPort,
        registry: SchemaRegistryPort,
    ) -> None:
        """Initialise with a ConsumerPort and a SchemaRegistryPort.

        Args:
            consumer: Any object that satisfies the ConsumerPort Protocol.
                Injected so tests can pass a MockConsumer with no real
                broker.
            registry: Any object that satisfies the SchemaRegistryPort
                Protocol.  Injected so tests can pass a MockSchemaRegistry
                with no real Schema Registry.
        """
        self._consumer = consumer
        self._registry = registry

    def list_topics(self, include_internal: bool = False) -> list[str]:
        """Return a list of topic names available on the broker.

        Args:
            include_internal: When ``False`` (default), excludes topics
                whose names start with ``"__"`` (D-09).

        Returns:
            Sorted list of topic name strings.
        """
        return self._consumer.list_topics(include_internal=include_internal)

    def describe_topic(self, topic: str) -> TopicInfo:
        """Return detailed metadata for a single topic.

        Retrieves partition IDs via ``ConsumerPort.get_partition_ids``
        (raises ``TopicNotFoundError`` if the topic does not exist),
        then fetches watermark offsets per partition.

        Args:
            topic: Exact topic name to describe.

        Returns:
            ``TopicInfo`` with ``partition_count`` and per-partition
            ``PartitionInfo`` objects carrying earliest/latest offsets.

        Raises:
            TopicNotFoundError: If the topic does not exist on the broker.

        Note:
            ``PartitionInfo.leader`` carries the real leader broker id when the
            consumer exposes ``get_partition_leaders`` (the confluent adapter
            reads it from the same cluster-metadata payload). For consumers that
            do not surface leader metadata it stays ``0`` (best-effort — leader
            is cosmetic, never an evidence field).
        """
        # Raises TopicNotFoundError if topic absent (T-03-03 mitigation)
        partition_ids = self._consumer.get_partition_ids(topic)

        # Best-effort leader enrichment: use it when the port provides it,
        # otherwise fall back to 0 without failing describe_topic.
        get_leaders = getattr(self._consumer, "get_partition_leaders", None)
        leaders: dict[int, int] = get_leaders(topic) if callable(get_leaders) else {}

        partitions: list[PartitionInfo] = []
        for pid in partition_ids:
            low, high = self._consumer.get_watermark_offsets(topic, pid)
            partitions.append(
                PartitionInfo(
                    id=pid,
                    leader=leaders.get(pid, 0),
                    earliest=low,
                    latest=high,
                )
            )

        return TopicInfo(
            name=topic,
            partition_count=len(partitions),
            partitions=partitions,
        )

    def search_messages(
        self,
        key: str,
        *,
        key_field: str | None = None,
        topics: list[str] | None = None,
        headers: dict[str, str] | None = None,
        time_from: datetime | None = None,
        time_to: datetime | None = None,
        limit: int = 500,
    ) -> list[KafkaMessage]:
        """Search for messages matching a key within a time window.

        Scans the requested topics/partitions using ConsumerPort, decodes
        each message via SchemaRegistryPort, and returns those matching
        the key according to key_field semantics and header filters.

        Algorithm:
        1. Resolve topics (None → list_topics()).
        2. Resolve time_to (None → datetime.now(UTC)).
        3. For each topic/partition: resolve start_offset from time_from
           (None → low watermark); resolve stop_offset from high watermark.
        4. Fetch, decode, match key + headers, extract Evidence; respect global limit.
        5. Sort results by timestamp_utc.

        Args:
            key: The value to match.
            key_field: Matching strategy: None/"key" (message key),
                "header:<name>" (header value), "value:<path>" (decoded
                field at dotted path).
            topics: Topic names to scan.  None → all non-internal topics.
            headers: Optional dictionary of header key-value pairs to filter by.
                Messages must match ALL specified header key-value pairs.
            time_from: Inclusive start of time window (UTC-aware).
                None → scan from earliest offset.
            time_to: Exclusive end of time window (UTC-aware).
                None → datetime.now(UTC).
            limit: Global maximum number of matching messages to return.
                0 or negative → returns [] immediately (T-02-04-D).

        Returns:
            List of KafkaMessage objects (decoded value, evidence keys),
            sorted by timestamp_utc.
        """
        # T-02-04-D: guard against non-positive limit
        if limit <= 0:
            return []

        # 1. Resolve topics
        resolved_topics: list[str]
        if topics is None:
            resolved_topics = self._consumer.list_topics(include_internal=False)
        else:
            resolved_topics = topics

        # 2. Resolve time_to
        resolved_time_to: datetime
        if time_to is None:
            resolved_time_to = datetime.now(tz=timezone.utc)
        else:
            resolved_time_to = time_to

        results: list[KafkaMessage] = []

        # 5. Scan topics × partitions
        for topic in resolved_topics:
            if len(results) >= limit:
                break

            partition_ids = self._consumer.get_partition_ids(topic)

            for partition_id in partition_ids:
                if len(results) >= limit:
                    break

                # Resolve start_offset
                low, stop_offset = self._consumer.get_watermark_offsets(topic, partition_id)

                if time_from is not None:
                    ts_ms = int(time_from.timestamp() * 1000)
                    start_offset = self._consumer.offsets_for_times(topic, partition_id, ts_ms)
                    # offsets_for_times returns -2 (OFFSET_BEGINNING) when
                    # timestamp is before earliest message → use low watermark
                    if start_offset == _OFFSET_BEGINNING or start_offset < 0:
                        start_offset = low
                else:
                    start_offset = low

                remaining = limit - len(results)
                if remaining <= 0:
                    break

                raw_msgs = self._consumer.fetch_messages(
                    topic,
                    partition_id,
                    start_offset,
                    stop_offset,
                    resolved_time_to,
                    remaining,
                )

                for raw_msg in raw_msgs:
                    # Resilient decode: DecodeError → value=None, keep message
                    try:
                        decoded = self._registry.decode(
                            raw_msg.raw,
                            raw_msg.topic,
                            raw_msg.partition,
                            raw_msg.offset,
                        )
                    except DecodeError:
                        decoded = None

                    evidence_keys = _extract_evidence_keys(decoded, raw_msg.headers)

                    # KEY-01: resilient key decode (DecodeError swallowed)
                    key_decoded = _decode_key(
                        raw_msg.raw_key,
                        self._registry,
                        raw_msg.topic,
                        raw_msg.partition,
                        raw_msg.offset,
                    )

                    # KEY-02: schema_id dict from Confluent wire framing
                    _val_id = _extract_schema_id(raw_msg.raw)
                    _key_id = _extract_schema_id(raw_msg.raw_key)
                    schema_id = (
                        {"value": _val_id, "key": _key_id} if _val_id is not None or _key_id is not None else None
                    )

                    msg = raw_msg.model_copy(
                        update={
                            "value": decoded,
                            "keys": evidence_keys,
                            "key_decoded": key_decoded,
                            "schema_id": schema_id,
                        }
                    )

                    # Apply key matching AND header filtering (AND semantics)
                    if _matches_key(msg, key, key_field) and _matches_headers(msg, headers):
                        results.append(msg)
                        if len(results) >= limit:
                            break

        # Sort results by timestamp_utc across all topics
        results.sort(key=lambda msg: msg.timestamp_utc)

        return results

    def get_message(
        self,
        topic: str,
        partition: int,
        offset: int,
    ) -> KafkaMessage:
        """Fetch and decode a single message by exact coordinates.

        Args:
            topic: Topic name.
            partition: Partition index (0-based).
            offset: Exact message offset.

        Returns:
            KafkaMessage with decoded value and evidence keys populated.

        Raises:
            MessageNotFoundError: When the offset is out of watermark range
                or no message is found at the offset.
            DecodeError: When the message payload cannot be decoded.
                Propagates to the caller (single-message strict path).
        """
        # Raises MessageNotFoundError if not found (propagate)
        raw_msg = self._consumer.fetch_message(topic, partition, offset)

        # Raises DecodeError if decode fails (propagate — strict single-message path).
        # Pass the real coordinates so the surfaced DecodeError reports the
        # actual topic/partition/offset, not a placeholder [0]@0 (CR-02).
        decoded = self._registry.decode(
            raw_msg.raw,
            raw_msg.topic,
            raw_msg.partition,
            raw_msg.offset,
        )

        evidence_keys = _extract_evidence_keys(decoded, raw_msg.headers)

        # KEY-01: resilient key decode (DecodeError swallowed — strict only for value)
        key_decoded = _decode_key(
            raw_msg.raw_key,
            self._registry,
            raw_msg.topic,
            raw_msg.partition,
            raw_msg.offset,
        )

        # KEY-02: schema_id dict from Confluent wire framing
        _val_id = _extract_schema_id(raw_msg.raw)
        _key_id = _extract_schema_id(raw_msg.raw_key)
        schema_id = {"value": _val_id, "key": _key_id} if _val_id is not None or _key_id is not None else None

        return raw_msg.model_copy(
            update={
                "value": decoded,
                "keys": evidence_keys,
                "key_decoded": key_decoded,
                "schema_id": schema_id,
            }
        )
