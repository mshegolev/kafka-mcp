"""ConfluentConsumerAdapter — read-only Kafka broker adapter.

Implements ConsumerPort using librdkafka (confluent-kafka).

Read-only structural guarantee (KAFKA-06, D-05):
- Uses Consumer.assign() to attach to specific partitions; the subscription-based
  method is intentionally absent from this file (assign-only guarantee).
- enable.auto.commit=False is always set in the librdkafka conf dict.
- group.id is a throwaway uuid4-based name (kafka-mcp-ro-{uuid4}) per
  instance, so the consumer group can never collide with production groups.

T-02-01 (STRIDE): sasl_password is extracted via SecretStr.get_secret_value()
immediately before passing to Consumer(); the conf dict is local to __init__
and never stored or logged.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import TracebackType
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka import TIMESTAMP_CREATE_TIME

from kafka_mcp.config import KafkaMcpSettings
from kafka_mcp.domain.errors import MessageNotFoundError, TopicNotFoundError
from kafka_mcp.domain.models import KafkaMessage

# Synchronous broker metadata round-trips (list_topics, watermark fetch)
# use a generous fixed budget. This is deliberately NOT poll_timeout, which
# is reserved for actual consumer.poll() calls (WR-03).
_METADATA_TIMEOUT_SECONDS = 10.0


class ConfluentConsumerAdapter:
    """Read-only Kafka consumer adapter backed by librdkafka.

    Implements :class:`kafka_mcp.ports.consumer.ConsumerPort`.

    Example::

        settings = KafkaMcpSettings(bootstrap_servers="broker:9092")
        with ConfluentConsumerAdapter(settings) as adapter:
            topics = adapter.list_topics()
            low, high = adapter.get_watermark_offsets("payments", 0)
    """

    def __init__(self, settings: KafkaMcpSettings) -> None:
        """Build the librdkafka Consumer from settings.

        Args:
            settings: Validated application settings.  ``bootstrap_servers``
                is required; SASL fields are included only when
                ``security_protocol`` is not ``"PLAINTEXT"``.
        """
        self._settings = settings

        conf: dict[str, object] = {
            "bootstrap.servers": settings.bootstrap_servers,
            # Read-only structural guarantees (KAFKA-06)
            "enable.auto.commit": False,
            "group.id": f"kafka-mcp-ro-{uuid4()}",
        }

        if settings.security_protocol != "PLAINTEXT":
            conf["security.protocol"] = settings.security_protocol

        # Only configure SASL when a mechanism is actually requested.
        # Keying on sasl_mechanism (not "not PLAINTEXT") means TLS-only
        # deployments (SECURITY_PROTOCOL=SSL) and SASL_SSL-without-explicit-
        # mechanism deployments construct cleanly instead of injecting a
        # None mechanism that librdkafka rejects at Consumer() construction.
        if settings.sasl_mechanism:
            conf["sasl.mechanism"] = settings.sasl_mechanism
            if settings.sasl_username is not None:
                conf["sasl.username"] = settings.sasl_username
            # Extract SecretStr value immediately — never stored in an
            # attribute or logged (T-02-01).
            if settings.sasl_password is not None:
                conf["sasl.password"] = settings.sasl_password.get_secret_value()

        self._consumer: Consumer = Consumer(conf)

    # ------------------------------------------------------------------
    # ConsumerPort implementation
    # ------------------------------------------------------------------

    def list_topics(self, include_internal: bool = False) -> list[str]:
        """Return a sorted list of topic names available on the broker.

        Args:
            include_internal: When ``False`` (default), topics whose names
                start with ``"__"`` (e.g. ``__consumer_offsets``) are
                excluded (D-09).

        Returns:
            Sorted list of topic name strings.
        """
        metadata = self._consumer.list_topics(timeout=10.0)
        names = list(metadata.topics.keys())
        if not include_internal:
            names = [n for n in names if not n.startswith("__")]
        return sorted(names)

    def get_partition_ids(self, topic: str) -> list[int]:
        """Return a sorted list of partition IDs for the given topic.

        Uses ``Consumer.list_topics(topic=topic)`` to fetch per-topic
        cluster metadata from librdkafka.

        Args:
            topic: Topic name.

        Returns:
            Sorted list of partition integer IDs.

        Raises:
            TopicNotFoundError: When the topic does not exist on the broker.
        """
        metadata = self._consumer.list_topics(
            topic=topic, timeout=10.0
        )
        topic_meta = metadata.topics.get(topic)
        if topic_meta is None:
            raise TopicNotFoundError(topic)
        # WR-01: mirror the WR-04 code-discrimination from
        # get_watermark_offsets. TopicMetadata.error is also set for
        # transient/operational conditions (LEADER_NOT_AVAILABLE,
        # REPLICA_NOT_AVAILABLE during a rebalance/election). Only genuine
        # unknown-topic/partition codes mean "not found"; everything else
        # must surface unchanged so a live topic in a transient state isn't
        # reported as missing (and 404'd by the REST adapter).
        err = topic_meta.error
        if err is not None:
            if err.code() in (
                KafkaError.UNKNOWN_TOPIC_OR_PART,
                KafkaError._UNKNOWN_TOPIC,
                KafkaError._UNKNOWN_PARTITION,
            ):
                raise TopicNotFoundError(topic)
            raise KafkaException(err)
        return sorted(topic_meta.partitions.keys())

    def get_watermark_offsets(
        self, topic: str, partition: int
    ) -> tuple[int, int]:
        """Return (low, high) watermark offsets for the given partition.

        Delegates to ``Consumer.get_watermark_offsets()`` (librdkafka)
        which issues a describe-request to the broker (D-06).

        Args:
            topic: Topic name.
            partition: Partition index (0-based).

        Returns:
            ``(low, high)`` as a tuple of ints (earliest, latest offsets).

        Raises:
            TopicNotFoundError: When librdkafka raises a KafkaException
                for an unknown topic/partition.
        """
        try:
            # WR-03: a watermark fetch is a synchronous broker metadata
            # round-trip, not a consumer.poll(); use the dedicated metadata
            # budget so broker latency / many partitions don't spuriously
            # raise (and then get mis-mapped to TopicNotFoundError).
            low, high = self._consumer.get_watermark_offsets(
                topic, partition, timeout=_METADATA_TIMEOUT_SECONDS
            )
        except KafkaException as exc:
            # WR-04: only genuine unknown-topic / unknown-partition errors
            # mean "not found". Transient/operational failures (timeout,
            # broker unavailable, transport, auth) must surface unchanged so
            # an outage isn't reported as a missing topic (and 404'd by REST).
            code = None
            if exc.args and hasattr(exc.args[0], "code"):
                code = exc.args[0].code()
            if code in (
                KafkaError.UNKNOWN_TOPIC_OR_PART,
                KafkaError._UNKNOWN_TOPIC,
                KafkaError._UNKNOWN_PARTITION,
            ):
                raise TopicNotFoundError(topic) from exc
            raise
        return (low, high)

    def fetch_messages(
        self,
        topic: str,
        partition: int,
        start_offset: int,
        stop_offset: int,
        time_to: datetime | None,
        limit: int,
    ) -> list[KafkaMessage]:
        """Consume messages from [start_offset, stop_offset) bounded by
        time_to and limit.

        Uses assign() to seek to start_offset; never uses the group-based
        subscription API and never commits offsets (assign-only, KAFKA-06).

        Scan terminates when any of the following is true:
        - Consumer.poll() returns None (end of partition or timeout)
        - Message.error() is truthy (transient error — return partial result)
        - msg.offset() >= stop_offset
        - Per-partition scan counter exceeds self._settings.max_scan
          (T-02-03-A DoS guard)
        - msg.timestamp_utc > time_to (when time_to is not None)
        - len(result) >= limit

        Args:
            topic: Topic name.
            partition: Partition index (0-based).
            start_offset: Inclusive start offset for the scan.
            stop_offset: Exclusive stop offset for the scan.
            time_to: Optional UTC-aware datetime upper bound on timestamps.
            limit: Maximum messages to return from this call.

        Returns:
            List of KafkaMessage objects (may be empty). value is always
            None — decode is performed by the SchemaRegistryHttpAdapter
            in the domain service (plan 02-04).
        """
        tp = TopicPartition(topic, partition, start_offset)
        self._consumer.assign([tp])

        result: list[KafkaMessage] = []
        scan_count = 0

        while True:
            msg = self._consumer.poll(
                timeout=self._settings.poll_timeout
            )
            if msg is None:
                break
            if msg.error():
                break
            if msg.offset() >= stop_offset:
                break

            scan_count += 1
            if scan_count > self._settings.max_scan:
                break

            # --- timestamp extraction (T-02-03-B, clock-skew note) ---
            ts_type, ts_ms = msg.timestamp()
            if ts_type == TIMESTAMP_CREATE_TIME and ts_ms > 0:
                ts_utc = datetime.fromtimestamp(
                    ts_ms / 1000.0, tz=timezone.utc
                )
            else:
                # LogAppendTime or TIMESTAMP_NOT_AVAILABLE — fallback to
                # wall-clock now; note: subject to clock-skew (D-context).
                ts_utc = datetime.now(tz=timezone.utc)

            if time_to is not None and ts_utc > time_to:
                break

            # --- key (best-effort UTF-8, T-02-03-D) ---
            raw_key = msg.key()
            key_str: str | None = (
                raw_key.decode("utf-8", errors="replace")
                if raw_key is not None
                else None
            )

            # --- headers (best-effort UTF-8, T-02-03-D) ---
            raw_headers = msg.headers()
            headers_dict: dict[str, str] = {}
            if raw_headers:
                for name, val in raw_headers:
                    headers_dict[name] = (
                        val.decode("utf-8", errors="replace")
                        if isinstance(val, bytes)
                        else str(val)
                    )

            result.append(
                KafkaMessage(
                    topic=topic,
                    partition=partition,
                    offset=msg.offset(),
                    key=key_str,
                    headers=headers_dict,
                    value=None,
                    timestamp_utc=ts_utc,
                    raw=msg.value() or b"",
                )
            )

            if len(result) >= limit:
                break

        return result

    def offsets_for_times(
        self,
        topic: str,
        partition: int,
        timestamp_ms: int,
    ) -> int:
        """Return the start offset for messages at or after timestamp_ms.

        Uses Consumer.offsets_for_times([TopicPartition(topic, partition,
        timestamp_ms)]) which returns a list of TopicPartition objects with
        .offset set to the result.  OFFSET_BEGINNING (-2) is returned when
        timestamp_ms is before the earliest message in the partition.

        Args:
            topic: Topic name.
            partition: Partition index (0-based).
            timestamp_ms: POSIX millisecond timestamp to seek from.

        Returns:
            Offset of the first message at or after timestamp_ms, or -2
            (OFFSET_BEGINNING) when before the earliest message.
        """
        tp = TopicPartition(topic, partition, timestamp_ms)
        result = self._consumer.offsets_for_times(
            [tp], timeout=_METADATA_TIMEOUT_SECONDS
        )
        # result is a list[TopicPartition]; the .offset is the resolved offset
        # or OFFSET_INVALID (-1001) / OFFSET_BEGINNING (-2) when not found.
        if result:
            returned_offset = result[0].offset
            # OFFSET_BEGINNING (-2) means "before first message" — return -2
            # so the service layer uses the low watermark.
            # OFFSET_INVALID (-1001) or negative (besides -2) means no messages
            # at or after the requested time — treat as earliest.
            if returned_offset == -2 or returned_offset < 0:
                return -2
            return returned_offset
        return -2

    def fetch_message(
        self,
        topic: str,
        partition: int,
        offset: int,
    ) -> KafkaMessage:
        """Fetch a single raw message by exact offset.

        Uses assign() to seek to the exact offset; never uses the
        group-based subscription API and never commits (KAFKA-06).

        Algorithm:
        1. Range-check via get_watermark_offsets; raise MessageNotFoundError
           if offset < low or offset >= high.
        2. Assign to exact offset and poll with 5× poll_timeout budget
           (longer single-message window for broker latency).
        3. None poll result or msg.error() → raise MessageNotFoundError.
        4. Extract timestamp, key, headers same as fetch_messages.
        5. Return KafkaMessage(value=None) — decode done by domain service.

        Args:
            topic: Topic name.
            partition: Partition index (0-based).
            offset: Exact offset to fetch.

        Returns:
            KafkaMessage at the given offset with raw bytes; value is None.

        Raises:
            MessageNotFoundError: When offset is out of watermark range or
                Consumer.poll() times out.
        """
        low, high = self.get_watermark_offsets(topic, partition)
        if offset < low or offset >= high:
            raise MessageNotFoundError(topic, partition, offset)

        tp = TopicPartition(topic, partition, offset)
        self._consumer.assign([tp])

        msg = self._consumer.poll(
            timeout=self._settings.poll_timeout * 5
        )
        if msg is None:
            raise MessageNotFoundError(topic, partition, offset)
        if msg.error():
            raise MessageNotFoundError(topic, partition, offset)

        # --- timestamp extraction (same as fetch_messages) ---
        ts_type, ts_ms = msg.timestamp()
        if ts_type == TIMESTAMP_CREATE_TIME and ts_ms > 0:
            ts_utc = datetime.fromtimestamp(
                ts_ms / 1000.0, tz=timezone.utc
            )
        else:
            ts_utc = datetime.now(tz=timezone.utc)

        # --- key (best-effort UTF-8, T-02-03-D) ---
        raw_key = msg.key()
        key_str: str | None = (
            raw_key.decode("utf-8", errors="replace")
            if raw_key is not None
            else None
        )

        # --- headers (best-effort UTF-8, T-02-03-D) ---
        raw_headers = msg.headers()
        headers_dict: dict[str, str] = {}
        if raw_headers:
            for name, val in raw_headers:
                headers_dict[name] = (
                    val.decode("utf-8", errors="replace")
                    if isinstance(val, bytes)
                    else str(val)
                )

        return KafkaMessage(
            topic=topic,
            partition=partition,
            offset=msg.offset(),
            key=key_str,
            headers=headers_dict,
            value=None,
            timestamp_utc=ts_utc,
            raw=msg.value() or b"",
        )

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying librdkafka Consumer.

        Releases background threads, sockets and the broker connection.
        Safe to call directly (e.g. from a facade ``close()``) as well as
        via the context-manager protocol.
        """
        self._consumer.close()

    def __enter__(self) -> ConfluentConsumerAdapter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close the underlying Consumer regardless of exceptions."""
        self.close()
