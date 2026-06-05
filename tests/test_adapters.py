"""No-broker adapter test suite.

All tests run without a running Kafka broker — confluent_kafka.Consumer
is patched with unittest.mock.MagicMock throughout.

Covers:
- ConfluentConsumerAdapter: internal-topic filtering, watermark offsets,
  context-manager cleanup, assign-only read-only guarantee
- SchemaRegistryHttpAdapter: stub returns None when url is None
- orjson helpers: encode/decode round-trip
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kafka_mcp.ports.consumer import ConsumerPort
from kafka_mcp.ports.schema_registry import SchemaRegistryPort

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(bootstrap_servers: str = "localhost:9092") -> object:
    """Return a minimal KafkaMcpSettings instance for adapter tests."""
    from kafka_mcp.config import KafkaMcpSettings

    return KafkaMcpSettings(bootstrap_servers=bootstrap_servers)


def _make_metadata_mock(topic_names: list[str]) -> MagicMock:
    """Build a metadata mock whose .topics dict mirrors confluent_kafka shape."""
    meta = MagicMock()
    # metadata.topics is dict[str, TopicMetadata-like]; we only need the keys
    meta.topics = {name: MagicMock(topic=name) for name in topic_names}
    return meta


# ---------------------------------------------------------------------------
# ConfluentConsumerAdapter — list_topics
# ---------------------------------------------------------------------------


class TestConfluentConsumerAdapterListTopics:
    """list_topics filtering and sorting behaviour."""

    def _make_adapter(self, mock_consumer: MagicMock) -> object:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        settings = _make_settings()
        with patch(
            "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
            return_value=mock_consumer,
        ):
            return ConfluentConsumerAdapter(settings)

    def test_list_topics_excludes_internal_by_default(self) -> None:
        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_metadata_mock(
            ["__consumer_offsets", "payments", "__transaction_state", "orders"]
        )
        adapter = self._make_adapter(mock_consumer)
        result = adapter.list_topics(include_internal=False)
        assert result == ["orders", "payments"]

    def test_list_topics_include_internal_true(self) -> None:
        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_metadata_mock(
            ["__consumer_offsets", "payments"]
        )
        adapter = self._make_adapter(mock_consumer)
        result = adapter.list_topics(include_internal=True)
        assert "__consumer_offsets" in result
        assert "payments" in result

    def test_list_topics_default_excludes_internal(self) -> None:
        """list_topics() with no args defaults to include_internal=False."""
        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_metadata_mock(
            ["__consumer_offsets", "payments"]
        )
        adapter = self._make_adapter(mock_consumer)
        result = adapter.list_topics()
        assert result == ["payments"]

    def test_list_topics_returns_sorted(self) -> None:
        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_metadata_mock(
            ["zebra", "apple", "mango"]
        )
        adapter = self._make_adapter(mock_consumer)
        result = adapter.list_topics()
        assert result == ["apple", "mango", "zebra"]

    def test_list_topics_empty_broker(self) -> None:
        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_metadata_mock([])
        adapter = self._make_adapter(mock_consumer)
        assert adapter.list_topics() == []


# ---------------------------------------------------------------------------
# ConfluentConsumerAdapter — get_watermark_offsets
# ---------------------------------------------------------------------------


class TestConfluentConsumerAdapterWatermarks:
    """get_watermark_offsets delegation and error handling."""

    def _make_adapter(self, mock_consumer: MagicMock) -> object:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        settings = _make_settings()
        with patch(
            "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
            return_value=mock_consumer,
        ):
            return ConfluentConsumerAdapter(settings)

    def test_get_watermark_offsets_returns_tuple(self) -> None:
        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 100)
        adapter = self._make_adapter(mock_consumer)
        low, high = adapter.get_watermark_offsets("payments", 0)
        assert low == 0
        assert high == 100

    def test_get_watermark_offsets_delegates_to_consumer(self) -> None:
        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (42, 999)
        adapter = self._make_adapter(mock_consumer)
        result = adapter.get_watermark_offsets("orders", 2)
        assert result == (42, 999)
        mock_consumer.get_watermark_offsets.assert_called_once()

    def test_get_watermark_offsets_raises_topic_not_found(self) -> None:
        from confluent_kafka import KafkaError, KafkaException

        from kafka_mcp.domain.errors import TopicNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.side_effect = KafkaException(
            KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)
        )
        adapter = self._make_adapter(mock_consumer)
        with pytest.raises(TopicNotFoundError) as exc_info:
            adapter.get_watermark_offsets("nonexistent", 0)
        assert exc_info.value.topic == "nonexistent"

    def test_get_watermark_offsets_unknown_partition_is_not_found(
        self,
    ) -> None:
        """Unknown-partition codes also map to TopicNotFoundError (WR-04)."""
        from confluent_kafka import KafkaError, KafkaException

        from kafka_mcp.domain.errors import TopicNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.side_effect = KafkaException(
            KafkaError(KafkaError._UNKNOWN_PARTITION)
        )
        adapter = self._make_adapter(mock_consumer)
        with pytest.raises(TopicNotFoundError):
            adapter.get_watermark_offsets("payments", 99)

    def test_get_watermark_offsets_transient_error_reraised(self) -> None:
        """Transient/operational KafkaExceptions must NOT become 404 (WR-04).

        A broker outage / transport error / timeout should surface as the
        original KafkaException, not as TopicNotFoundError.
        """
        from confluent_kafka import KafkaError, KafkaException

        from kafka_mcp.domain.errors import TopicNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.side_effect = KafkaException(
            KafkaError(KafkaError._TRANSPORT)
        )
        adapter = self._make_adapter(mock_consumer)
        with pytest.raises(KafkaException):
            adapter.get_watermark_offsets("payments", 0)
        # And specifically NOT mistranslated to "not found".
        mock_consumer.get_watermark_offsets.side_effect = KafkaException(
            KafkaError(KafkaError._TRANSPORT)
        )
        with pytest.raises(KafkaException):
            try:
                adapter.get_watermark_offsets("payments", 0)
            except TopicNotFoundError:  # pragma: no cover - regression guard
                pytest.fail("transient error mistranslated to TopicNotFound")

    def test_get_watermark_offsets_uses_metadata_timeout(self) -> None:
        """WR-03: watermark fetch uses the 10s metadata budget, not poll_timeout."""
        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 10)
        adapter = self._make_adapter(mock_consumer)
        adapter.get_watermark_offsets("payments", 0)
        _args, kwargs = mock_consumer.get_watermark_offsets.call_args
        # poll_timeout default is 1.0; the metadata budget is 10.0.
        assert kwargs.get("timeout") == 10.0


# ---------------------------------------------------------------------------
# ConfluentConsumerAdapter — get_partition_ids
# ---------------------------------------------------------------------------


def _make_topic_metadata(
    topic: str, *, partitions: list[int] | None = None, error: object = None
) -> MagicMock:
    """Build a list_topics(topic=...) result mock for a single topic.

    metadata.topics is dict[str, TopicMetadata-like]; each TopicMetadata has
    ``.error`` (None or a KafkaError) and ``.partitions`` (dict keyed by id).
    Pass ``partitions=None`` to omit the topic entirely (broker returned no
    metadata for it).
    """
    meta = MagicMock()
    if partitions is None and error is None:
        meta.topics = {}
        return meta
    topic_meta = MagicMock()
    topic_meta.error = error
    topic_meta.partitions = {pid: MagicMock(id=pid) for pid in (partitions or [])}
    meta.topics = {topic: topic_meta}
    return meta


class TestConfluentConsumerAdapterGetPartitionIds:
    """get_partition_ids: returns sorted ids; discriminates not-found vs transient (WR-01)."""

    def _make_adapter(self, mock_consumer: MagicMock) -> object:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        settings = _make_settings()
        with patch(
            "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
            return_value=mock_consumer,
        ):
            return ConfluentConsumerAdapter(settings)

    def test_returns_sorted_partition_ids(self) -> None:
        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_topic_metadata(
            "payments", partitions=[2, 0, 1]
        )
        adapter = self._make_adapter(mock_consumer)
        assert adapter.get_partition_ids("payments") == [0, 1, 2]

    def test_uses_metadata_timeout(self) -> None:
        """WR-03/WR-01: per-topic metadata fetch uses the 10s budget."""
        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_topic_metadata(
            "payments", partitions=[0]
        )
        adapter = self._make_adapter(mock_consumer)
        adapter.get_partition_ids("payments")
        _args, kwargs = mock_consumer.list_topics.call_args
        assert kwargs.get("timeout") == 10.0
        assert kwargs.get("topic") == "payments"

    def test_missing_topic_metadata_is_not_found(self) -> None:
        from kafka_mcp.domain.errors import TopicNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_topic_metadata("ghost")
        adapter = self._make_adapter(mock_consumer)
        with pytest.raises(TopicNotFoundError) as exc_info:
            adapter.get_partition_ids("ghost")
        assert exc_info.value.topic == "ghost"

    def test_unknown_topic_error_code_is_not_found(self) -> None:
        from confluent_kafka import KafkaError

        from kafka_mcp.domain.errors import TopicNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_topic_metadata(
            "ghost", error=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)
        )
        adapter = self._make_adapter(mock_consumer)
        with pytest.raises(TopicNotFoundError):
            adapter.get_partition_ids("ghost")

    def test_transient_error_code_reraised(self) -> None:
        """WR-01: a transient TopicMetadata.error must NOT become 404."""
        from confluent_kafka import KafkaError, KafkaException

        from kafka_mcp.domain.errors import TopicNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_topic_metadata(
            "payments", error=KafkaError(KafkaError.LEADER_NOT_AVAILABLE)
        )
        adapter = self._make_adapter(mock_consumer)
        with pytest.raises(KafkaException):
            try:
                adapter.get_partition_ids("payments")
            except TopicNotFoundError:  # pragma: no cover - regression guard
                pytest.fail("transient error mistranslated to TopicNotFound")


# ---------------------------------------------------------------------------
# ConfluentConsumerAdapter — context manager
# ---------------------------------------------------------------------------


class TestConfluentConsumerAdapterContextManager:
    """Context manager calls Consumer.close() exactly once on exit."""

    def test_context_manager_calls_close(self) -> None:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        mock_consumer = MagicMock()
        settings = _make_settings()
        with patch(
            "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
            return_value=mock_consumer,
        ):
            adapter = ConfluentConsumerAdapter(settings)

        with adapter:
            pass

        mock_consumer.close.assert_called_once()

    def test_context_manager_calls_close_on_exception(self) -> None:
        """close() is called even when an exception occurs inside the block."""
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        mock_consumer = MagicMock()
        settings = _make_settings()
        with patch(
            "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
            return_value=mock_consumer,
        ):
            adapter = ConfluentConsumerAdapter(settings)

        with pytest.raises(RuntimeError):
            with adapter:
                raise RuntimeError("test error")

        mock_consumer.close.assert_called_once()


# ---------------------------------------------------------------------------
# ConfluentConsumerAdapter — Protocol compliance
# ---------------------------------------------------------------------------


class TestConfluentConsumerAdapterProtocol:
    """Adapter satisfies ConsumerPort runtime_checkable Protocol."""

    def test_isinstance_consumer_port(self) -> None:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        mock_consumer = MagicMock()
        settings = _make_settings()
        with patch(
            "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
            return_value=mock_consumer,
        ):
            adapter = ConfluentConsumerAdapter(settings)

        assert isinstance(adapter, ConsumerPort)

    def test_no_subscribe_in_source(self) -> None:
        """subscribe() must not appear in the adapter source (KAFKA-06 / T-02-03)."""
        import inspect

        from kafka_mcp.adapters.outbound import confluent_consumer

        source = inspect.getsource(confluent_consumer)
        non_comment_lines = [
            line
            for line in source.splitlines()
            if "subscribe" in line and not line.strip().startswith("#")
        ]
        assert non_comment_lines == [], (
            f"subscribe() found in adapter source (KAFKA-06 violation): "
            f"{non_comment_lines}"
        )


# ---------------------------------------------------------------------------
# ConfluentConsumerAdapter — config dict
# ---------------------------------------------------------------------------


class TestConfluentConsumerAdapterConfig:
    """librdkafka config dict always has auto.commit=False and uuid4 group.id."""

    def test_config_contains_auto_commit_false(self) -> None:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        captured_conf: dict = {}

        def fake_consumer(conf: dict) -> MagicMock:
            captured_conf.update(conf)
            return MagicMock()

        settings = _make_settings()
        with patch(
            "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
            side_effect=fake_consumer,
        ):
            ConfluentConsumerAdapter(settings)

        assert captured_conf.get("enable.auto.commit") is False

    def test_config_group_id_starts_with_kafka_mcp_ro(self) -> None:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        captured_conf: dict = {}

        def fake_consumer(conf: dict) -> MagicMock:
            captured_conf.update(conf)
            return MagicMock()

        settings = _make_settings()
        with patch(
            "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
            side_effect=fake_consumer,
        ):
            ConfluentConsumerAdapter(settings)

        group_id = captured_conf.get("group.id", "")
        assert group_id.startswith("kafka-mcp-ro-"), (
            f"group.id '{group_id}' does not start with 'kafka-mcp-ro-'"
        )

    def test_group_id_is_unique_per_instance(self) -> None:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        group_ids: list[str] = []

        def fake_consumer(conf: dict) -> MagicMock:
            group_ids.append(conf.get("group.id", ""))
            return MagicMock()

        settings = _make_settings()
        with patch(
            "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
            side_effect=fake_consumer,
        ):
            ConfluentConsumerAdapter(settings)
            ConfluentConsumerAdapter(settings)

        assert group_ids[0] != group_ids[1], (
            "group.id must be unique per instantiation (uuid4)"
        )

    def test_ssl_only_omits_sasl_keys(self) -> None:
        """TLS-only (SSL, no mechanism) must not inject any sasl.* keys.

        Regression for CR-01: gating SASL on ``not PLAINTEXT`` injected
        ``sasl.mechanism=None`` which librdkafka rejects at construction.
        """
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )
        from kafka_mcp.config import KafkaMcpSettings

        captured_conf: dict = {}

        def fake_consumer(conf: dict) -> MagicMock:
            captured_conf.update(conf)
            return MagicMock()

        settings = KafkaMcpSettings(
            bootstrap_servers="localhost:9092", security_protocol="SSL"
        )
        with patch(
            "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
            side_effect=fake_consumer,
        ):
            ConfluentConsumerAdapter(settings)

        assert captured_conf.get("security.protocol") == "SSL"
        assert not any(k.startswith("sasl.") for k in captured_conf), (
            f"sasl.* keys must be omitted for TLS-only config: {captured_conf}"
        )

    def test_sasl_keys_added_only_when_mechanism_set(self) -> None:
        """SASL keys are configured only when a mechanism is requested."""
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )
        from kafka_mcp.config import KafkaMcpSettings

        captured_conf: dict = {}

        def fake_consumer(conf: dict) -> MagicMock:
            captured_conf.update(conf)
            return MagicMock()

        settings = KafkaMcpSettings(
            bootstrap_servers="localhost:9092",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="alice",
            sasl_password="secret",
        )
        with patch(
            "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
            side_effect=fake_consumer,
        ):
            ConfluentConsumerAdapter(settings)

        assert captured_conf.get("sasl.mechanism") == "PLAIN"
        assert captured_conf.get("sasl.username") == "alice"
        assert captured_conf.get("sasl.password") == "secret"

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"security_protocol": "PLAINTEXT"},
            # TLS-only: the exact deployment CR-01 broke. Before the fix this
            # injected sasl.mechanism=None and raised KafkaException here.
            {"security_protocol": "SSL"},
            # SASL over TLS with an explicit mechanism (the supported SASL path).
            {
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "PLAIN",
                "sasl_username": "alice",
                "sasl_password": "secret",
            },
        ],
    )
    def test_real_librdkafka_accepts_built_conf(self, kwargs: dict) -> None:
        """Construct a REAL confluent_kafka.Consumer with the built conf.

        Non-mocked regression test for CR-01: librdkafka validates the conf
        dict at ``Consumer()`` construction. Before the fix, a TLS-only
        config injected ``sasl.mechanism=None`` and raised ``KafkaException``
        here. We assert no exception is raised for valid TLS/SASL configs.

        (``SASL_SSL`` with NO mechanism is intentionally not tested: SASL
        inherently requires a mechanism and librdkafka rejects that config
        on its own merits, independent of this adapter.)
        """
        from confluent_kafka import KafkaException

        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )
        from kafka_mcp.config import KafkaMcpSettings

        settings = KafkaMcpSettings(
            bootstrap_servers="localhost:9092", **kwargs
        )

        try:
            # No mock: this exercises librdkafka's real config validation.
            ConfluentConsumerAdapter(settings)
        except KafkaException as exc:  # pragma: no cover - regression guard
            pytest.fail(
                f"librdkafka rejected a valid conf {kwargs}: {exc}"
            )


# ---------------------------------------------------------------------------
# SchemaRegistryHttpAdapter
# ---------------------------------------------------------------------------


class TestSchemaRegistryHttpAdapter:
    """Stub adapter returns None when url is None; no errors raised."""

    def test_get_schema_returns_none_when_url_none(self) -> None:
        from kafka_mcp.adapters.outbound.schema_registry_http import (
            SchemaRegistryHttpAdapter,
        )

        adapter = SchemaRegistryHttpAdapter(url=None)
        assert adapter.get_schema("payments-value") is None

    def test_get_schema_returns_none_for_any_subject(self) -> None:
        from kafka_mcp.adapters.outbound.schema_registry_http import (
            SchemaRegistryHttpAdapter,
        )

        adapter = SchemaRegistryHttpAdapter(url=None, user="u", password="p")
        for subject in ["foo", "bar-value", "baz-key"]:
            assert adapter.get_schema(subject) is None

    def test_no_exception_when_url_is_none(self) -> None:
        from kafka_mcp.adapters.outbound.schema_registry_http import (
            SchemaRegistryHttpAdapter,
        )

        # Must not raise at construction or at method call
        adapter = SchemaRegistryHttpAdapter(url=None)
        result = adapter.get_schema("any")
        assert result is None

    def test_isinstance_schema_registry_port(self) -> None:
        from kafka_mcp.adapters.outbound.schema_registry_http import (
            SchemaRegistryHttpAdapter,
        )

        adapter = SchemaRegistryHttpAdapter(url=None)
        assert isinstance(adapter, SchemaRegistryPort)


# ---------------------------------------------------------------------------
# orjson helpers
# ---------------------------------------------------------------------------


class TestOrjsonHelpers:
    """orjson_loads and orjson_dumps round-trip correctness."""

    def test_loads_bytes(self) -> None:
        from kafka_mcp.adapters.outbound.json_orjson import orjson_loads

        result = orjson_loads(b'{"key": "value"}')
        assert result == {"key": "value"}

    def test_loads_str(self) -> None:
        from kafka_mcp.adapters.outbound.json_orjson import orjson_loads

        result = orjson_loads('{"num": 42}')
        assert result == {"num": 42}

    def test_dumps_bytes(self) -> None:
        from kafka_mcp.adapters.outbound.json_orjson import orjson_dumps

        result = orjson_dumps({"key": "value"})
        assert isinstance(result, bytes)

    def test_dumps_loads_roundtrip(self) -> None:
        from kafka_mcp.adapters.outbound.json_orjson import orjson_dumps, orjson_loads

        original = {"topic": "payments", "partition": 0, "offset": 42}
        encoded = orjson_dumps(original)
        decoded = orjson_loads(encoded)
        assert decoded == original

    def test_dumps_compact_no_spaces(self) -> None:
        """orjson produces compact JSON (no extra spaces after colon/comma)."""
        from kafka_mcp.adapters.outbound.json_orjson import orjson_dumps

        result = orjson_dumps({"key": "value"})
        assert result == b'{"key":"value"}'


# ---------------------------------------------------------------------------
# SchemaRegistryHttpAdapter — decode pipeline (plan 02-02)
# ---------------------------------------------------------------------------
# Helpers shared across decode tests

_ADAPTER_MOD = "kafka_mcp.adapters.outbound.schema_registry_http"

_SCHEMA_ID = 42
_SCHEMA_ID_BYTES = _SCHEMA_ID.to_bytes(4, "big")
_MAGIC = bytes([0x00])
_CONFLUENT_PREFIX = _MAGIC + _SCHEMA_ID_BYTES  # 5 bytes total


def _make_sr_adapter(url: str | None = "http://sr:8081") -> object:
    from kafka_mcp.adapters.outbound.schema_registry_http import (
        SchemaRegistryHttpAdapter,
    )

    return SchemaRegistryHttpAdapter(url=url)


def _mock_schema(schema_type: str = "AVRO", schema_str: str = '{"type":"record","name":"T","fields":[]}') -> MagicMock:
    """Build a mock Schema object as returned by SchemaRegistryClient.get_schema()."""
    schema = MagicMock()
    schema.schema_type = schema_type
    schema.schema_str = schema_str
    return schema


class TestSchemaRegistryDecodeJson:
    """JSON fallback path: no magic byte in payload."""

    def test_decode_json_fallback(self) -> None:
        """Plain JSON bytes (no magic byte) → returns the decoded dict."""
        with patch(_ADAPTER_MOD + ".SchemaRegistryClient"):
            adapter = _make_sr_adapter()
        raw = b'{"key": "val"}'
        result = adapter.decode(raw)
        assert result == {"key": "val"}

    def test_decode_json_fallback_invalid(self) -> None:
        """Non-JSON, non-magic-byte payload → raises DecodeError with 'json' in reason."""
        from kafka_mcp.domain.errors import DecodeError

        with patch(_ADAPTER_MOD + ".SchemaRegistryClient"):
            adapter = _make_sr_adapter()
        raw = b"not-json"
        with pytest.raises(DecodeError) as exc_info:
            adapter.decode(raw, topic="t", partition=0, offset=0)
        assert "json" in exc_info.value.reason.lower()

    def test_decode_none_url_raises_decode_error(self) -> None:
        """With url=None (SR not configured), magic-byte payload raises DecodeError."""
        from kafka_mcp.domain.errors import DecodeError

        adapter = _make_sr_adapter(url=None)
        raw = _CONFLUENT_PREFIX + b"\x00" * 10
        with pytest.raises(DecodeError) as exc_info:
            adapter.decode(raw, topic="t", partition=0, offset=0)
        assert "not configured" in exc_info.value.reason.lower()


class TestSchemaRegistryDecodeAvro:
    """Confluent-framed Avro payloads decoded via mocked AvroDeserializer."""

    def test_decode_magic_byte_avro(self) -> None:
        """Confluent-framed payload → calls AvroDeserializer and returns dict."""
        mock_schema = _mock_schema("AVRO")
        mock_client = MagicMock()
        mock_client.get_schema.return_value = mock_schema
        mock_deserializer = MagicMock(return_value={"order_id": "123"})

        with (
            patch(_ADAPTER_MOD + ".SchemaRegistryClient", return_value=mock_client),
            patch(_ADAPTER_MOD + ".AvroDeserializer", return_value=mock_deserializer),
        ):
            adapter = _make_sr_adapter()
            raw = _CONFLUENT_PREFIX + b"\x01" * 10
            result = adapter.decode(raw)
        assert result == {"order_id": "123"}

    def test_decode_corrupt_avro_raises_decode_error(self) -> None:
        """Avro deserializer failure → wrapped as DecodeError (not propagated raw)."""
        from kafka_mcp.domain.errors import DecodeError

        mock_schema = _mock_schema("AVRO")
        mock_client = MagicMock()
        mock_client.get_schema.return_value = mock_schema
        mock_deserializer = MagicMock(side_effect=Exception("corrupt avro data"))

        with (
            patch(_ADAPTER_MOD + ".SchemaRegistryClient", return_value=mock_client),
            patch(_ADAPTER_MOD + ".AvroDeserializer", return_value=mock_deserializer),
        ):
            adapter = _make_sr_adapter()
            raw = _CONFLUENT_PREFIX + b"\xff" * 5
            with pytest.raises(DecodeError):
                adapter.decode(raw, topic="t", partition=0, offset=0)

    def test_schema_registry_client_cached(self) -> None:
        """Schema is fetched only once per schema_id (SR client caches internally)."""
        mock_schema = _mock_schema("AVRO")
        mock_client = MagicMock()
        mock_client.get_schema.return_value = mock_schema
        mock_deserializer = MagicMock(return_value={"k": "v"})

        with (
            patch(_ADAPTER_MOD + ".SchemaRegistryClient", return_value=mock_client),
            patch(_ADAPTER_MOD + ".AvroDeserializer", return_value=mock_deserializer),
        ):
            adapter = _make_sr_adapter()
            raw = _CONFLUENT_PREFIX + b"\x01" * 6
            adapter.decode(raw)
            adapter.decode(raw)

        # SchemaRegistryClient.get_schema was called once (second call uses cached result)
        assert mock_client.get_schema.call_count == 1


class TestSchemaRegistryDecodeProtobuf:
    """Confluent-framed Protobuf payloads decoded via mocked ProtobufDeserializer."""

    def test_decode_magic_byte_protobuf(self) -> None:
        """Confluent-framed PROTOBUF payload → calls ProtobufDeserializer and returns dict."""
        mock_schema = _mock_schema("PROTOBUF")
        mock_client = MagicMock()
        mock_client.get_schema.return_value = mock_schema
        mock_deserializer = MagicMock(return_value={"msisdn": "79001234567"})

        with (
            patch(_ADAPTER_MOD + ".SchemaRegistryClient", return_value=mock_client),
            patch(_ADAPTER_MOD + ".ProtobufDeserializer", return_value=mock_deserializer),
        ):
            adapter = _make_sr_adapter()
            raw = _CONFLUENT_PREFIX + b"\x02" * 10
            result = adapter.decode(raw)
        assert result == {"msisdn": "79001234567"}

    def test_decode_unknown_schema_type(self) -> None:
        """Schema type other than AVRO/PROTOBUF → DecodeError with 'unknown schema type'."""
        from kafka_mcp.domain.errors import DecodeError

        mock_schema = _mock_schema("JSON")  # not AVRO or PROTOBUF
        mock_client = MagicMock()
        mock_client.get_schema.return_value = mock_schema

        with (
            patch(_ADAPTER_MOD + ".SchemaRegistryClient", return_value=mock_client),
        ):
            adapter = _make_sr_adapter()
            raw = _CONFLUENT_PREFIX + b"\x03" * 5
            with pytest.raises(DecodeError) as exc_info:
                adapter.decode(raw, topic="t", partition=0, offset=0)
        assert "unknown schema type" in exc_info.value.reason.lower()


class TestSchemaRegistryAdapterSecurity:
    """Security invariants: credentials not exposed; Protocol membership."""

    def test_implements_schema_registry_port(self) -> None:
        """SchemaRegistryHttpAdapter(url=None) is an instance of SchemaRegistryPort."""
        adapter = _make_sr_adapter(url=None)
        assert isinstance(adapter, SchemaRegistryPort)

    def test_sr_credentials_not_logged(self) -> None:
        """repr() and str() of the adapter must not contain the sr_pass value."""
        from kafka_mcp.adapters.outbound.schema_registry_http import (
            SchemaRegistryHttpAdapter,
        )

        secret = "super-secret-password-xyz"
        with patch(_ADAPTER_MOD + ".SchemaRegistryClient"):
            adapter = SchemaRegistryHttpAdapter(
                url="http://sr:8081", user="alice", password=secret
            )
        assert secret not in repr(adapter)
        assert secret not in str(adapter)


# ---------------------------------------------------------------------------
# ConfluentConsumerAdapter — fetch_messages (plan 02-03)
# ---------------------------------------------------------------------------


def _make_consumer_adapter(mock_consumer: MagicMock) -> object:
    """Build a ConfluentConsumerAdapter with a mocked librdkafka Consumer."""
    from kafka_mcp.adapters.outbound.confluent_consumer import (
        ConfluentConsumerAdapter,
    )
    from kafka_mcp.config import KafkaMcpSettings

    settings = KafkaMcpSettings(bootstrap_servers="localhost:9092")
    with patch(
        "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
        return_value=mock_consumer,
    ):
        return ConfluentConsumerAdapter(settings)


def _make_msg_mock(
    offset: int,
    value: bytes = b'{"event":"test"}',
    key: bytes | None = b"order-key",
    headers: list | None = None,
    timestamp_type: int = 1,  # TIMESTAMP_CREATE_TIME
    timestamp_ms: int = 1_700_000_000_000,
    error: object = None,
) -> MagicMock:
    """Build a mock confluent_kafka.Message for adapter tests."""
    msg = MagicMock()
    msg.offset.return_value = offset
    msg.value.return_value = value
    msg.key.return_value = key
    msg.headers.return_value = headers
    msg.timestamp.return_value = (timestamp_type, timestamp_ms)
    msg.error.return_value = error
    return msg


class TestFetchMessages:
    """ConfluentConsumerAdapter.fetch_messages — seek-and-scan logic."""

    def test_fetch_messages_returns_kafka_message_list(self) -> None:
        """Polled messages are returned as a list[KafkaMessage]."""
        from kafka_mcp.domain.models import KafkaMessage

        mock_consumer = MagicMock()
        msg0 = _make_msg_mock(
            offset=10,
            value=b'{"event":"order_created"}',
            key=b"order-123",
            headers=[("x-trace", b"abc")],
        )
        mock_consumer.poll.side_effect = [msg0, None]
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_messages(
            topic="orders",
            partition=0,
            start_offset=10,
            stop_offset=100,
            time_to=None,
            limit=50,
        )

        assert isinstance(result, list)
        assert len(result) == 1
        msg = result[0]
        assert isinstance(msg, KafkaMessage)
        assert msg.topic == "orders"
        assert msg.partition == 0
        assert msg.offset == 10
        assert msg.key == "order-123"
        assert msg.headers == {"x-trace": "abc"}
        assert msg.raw == b'{"event":"order_created"}'
        assert msg.value is None

    def test_fetch_messages_stops_at_stop_offset(self) -> None:
        """Messages with offset >= stop_offset are not returned."""
        mock_consumer = MagicMock()
        msg0 = _make_msg_mock(offset=5)
        msg1 = _make_msg_mock(offset=10)  # exactly at stop_offset — excluded
        mock_consumer.poll.side_effect = [msg0, msg1, None]
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_messages(
            topic="orders",
            partition=0,
            start_offset=5,
            stop_offset=10,
            time_to=None,
            limit=100,
        )

        offsets = [m.offset for m in result]
        assert 10 not in offsets
        assert 5 in offsets

    def test_fetch_messages_stops_at_limit(self) -> None:
        """Returns at most `limit` messages."""
        mock_consumer = MagicMock()
        msgs = [_make_msg_mock(offset=i) for i in range(10)]
        mock_consumer.poll.side_effect = msgs + [None]
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_messages(
            topic="orders",
            partition=0,
            start_offset=0,
            stop_offset=1000,
            time_to=None,
            limit=3,
        )

        assert len(result) == 3

    def test_fetch_messages_stops_at_time_to(self) -> None:
        """Messages with timestamp_utc > time_to are excluded and scan stops."""
        from datetime import datetime, timezone

        mock_consumer = MagicMock()
        # msg0: at 1000s — within time_to of 1500s
        msg0 = _make_msg_mock(
            offset=0, timestamp_type=1, timestamp_ms=1_000_000
        )
        # msg1: at 2000s — exceeds time_to of 1.5s epoch → 1500000ms
        msg1 = _make_msg_mock(
            offset=1, timestamp_type=1, timestamp_ms=2_000_000
        )
        mock_consumer.poll.side_effect = [msg0, msg1, None]
        adapter = _make_consumer_adapter(mock_consumer)

        time_to = datetime.fromtimestamp(1500.0, tz=timezone.utc)
        result = adapter.fetch_messages(
            topic="orders",
            partition=0,
            start_offset=0,
            stop_offset=1000,
            time_to=time_to,
            limit=100,
        )

        # Only the first message (within time_to) should be returned
        assert len(result) == 1
        assert result[0].offset == 0

    def test_fetch_messages_timestamp_utc_from_create_time(self) -> None:
        """TIMESTAMP_CREATE_TIME → UTC-aware datetime derived from ms correctly."""
        from datetime import datetime, timezone

        mock_consumer = MagicMock()
        # 1_700_000_000_000 ms = epoch 1_700_000_000 s
        ts_ms = 1_700_000_000_000
        msg0 = _make_msg_mock(
            offset=0, timestamp_type=1, timestamp_ms=ts_ms
        )
        mock_consumer.poll.side_effect = [msg0, None]
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_messages(
            topic="payments",
            partition=0,
            start_offset=0,
            stop_offset=100,
            time_to=None,
            limit=10,
        )

        assert len(result) == 1
        ts = result[0].timestamp_utc
        expected = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        assert ts == expected
        assert ts.tzinfo is not None

    def test_fetch_messages_timestamp_fallback_log_append_time(self) -> None:
        """TIMESTAMP_LOG_APPEND_TIME → timestamp_utc still set (fallback path)."""
        mock_consumer = MagicMock()
        # timestamp_type=2 is TIMESTAMP_LOG_APPEND_TIME
        msg0 = _make_msg_mock(
            offset=0, timestamp_type=2, timestamp_ms=1_000_000_000
        )
        mock_consumer.poll.side_effect = [msg0, None]
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_messages(
            topic="events",
            partition=0,
            start_offset=0,
            stop_offset=100,
            time_to=None,
            limit=10,
        )

        assert len(result) == 1
        ts = result[0].timestamp_utc
        assert ts is not None
        assert ts.tzinfo is not None  # must be UTC-aware

    def test_fetch_messages_respects_max_scan(self) -> None:
        """Scan stops after max_scan messages regardless of limit."""
        from kafka_mcp.config import KafkaMcpSettings

        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        mock_consumer = MagicMock()
        # Produce 200 messages; max_scan is set to 5 via settings
        msgs = [_make_msg_mock(offset=i) for i in range(200)]
        mock_consumer.poll.side_effect = msgs + [None]

        # Use settings with max_scan=5
        settings = KafkaMcpSettings(
            bootstrap_servers="localhost:9092",
            max_scan=5,
        )
        with patch(
            "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
            return_value=mock_consumer,
        ):
            adapter = ConfluentConsumerAdapter(settings)

        result = adapter.fetch_messages(
            topic="events",
            partition=0,
            start_offset=0,
            stop_offset=1000,
            time_to=None,
            limit=100,
        )

        assert len(result) <= 5

    def test_fetch_messages_empty_partition(self) -> None:
        """Consumer.poll returns None immediately → returns empty list."""
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = None
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_messages(
            topic="orders",
            partition=0,
            start_offset=0,
            stop_offset=100,
            time_to=None,
            limit=50,
        )

        assert result == []

    def test_fetch_messages_no_subscribe_call(self) -> None:
        """Consumer.subscribe is never called during fetch_messages (KAFKA-06)."""
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = None
        adapter = _make_consumer_adapter(mock_consumer)

        adapter.fetch_messages(
            topic="orders",
            partition=0,
            start_offset=0,
            stop_offset=100,
            time_to=None,
            limit=50,
        )

        mock_consumer.subscribe.assert_not_called()


# ---------------------------------------------------------------------------
# ConfluentConsumerAdapter — fetch_message (plan 02-03)
# ---------------------------------------------------------------------------


class TestFetchMessage:
    """ConfluentConsumerAdapter.fetch_message — single-offset lookup."""

    def test_fetch_message_returns_single_kafka_message(self) -> None:
        """Valid offset within watermarks → returns one KafkaMessage."""
        from kafka_mcp.domain.models import KafkaMessage

        mock_consumer = MagicMock()
        # Watermarks: low=0, high=100; requesting offset=10 (in range)
        mock_consumer.get_watermark_offsets.return_value = (0, 100)
        msg = _make_msg_mock(
            offset=10,
            value=b'{"event":"payment"}',
            key=b"pay-456",
            headers=[("x-id", b"xyz")],
        )
        mock_consumer.poll.return_value = msg
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_message(topic="payments", partition=0, offset=10)

        assert isinstance(result, KafkaMessage)
        assert result.topic == "payments"
        assert result.partition == 0
        assert result.offset == 10
        assert result.key == "pay-456"
        assert result.headers == {"x-id": "xyz"}
        assert result.raw == b'{"event":"payment"}'
        assert result.value is None

    def test_fetch_message_out_of_range_raises_message_not_found(
        self,
    ) -> None:
        """offset >= latest watermark → raises MessageNotFoundError."""
        from kafka_mcp.domain.errors import MessageNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 50)
        adapter = _make_consumer_adapter(mock_consumer)

        with pytest.raises(MessageNotFoundError):
            adapter.fetch_message(topic="orders", partition=1, offset=50)

    def test_fetch_message_message_not_found_carries_coordinates(
        self,
    ) -> None:
        """MessageNotFoundError.topic, .partition, .offset match the request."""
        from kafka_mcp.domain.errors import MessageNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 30)
        adapter = _make_consumer_adapter(mock_consumer)

        with pytest.raises(MessageNotFoundError) as exc_info:
            adapter.fetch_message(
                topic="events", partition=2, offset=999
            )

        err = exc_info.value
        assert err.topic == "events"
        assert err.partition == 2
        assert err.offset == 999

    def test_fetch_message_timeout_returns_message_not_found(self) -> None:
        """Consumer.poll returns None (timeout) → raises MessageNotFoundError."""
        from kafka_mcp.domain.errors import MessageNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 100)
        mock_consumer.poll.return_value = None  # timeout
        adapter = _make_consumer_adapter(mock_consumer)

        with pytest.raises(MessageNotFoundError):
            adapter.fetch_message(topic="orders", partition=0, offset=5)

    def test_fetch_message_no_subscribe_in_fetch_message(self) -> None:
        """Consumer.subscribe is never called during fetch_message (KAFKA-06)."""
        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 100)
        msg = _make_msg_mock(offset=5)
        mock_consumer.poll.return_value = msg
        adapter = _make_consumer_adapter(mock_consumer)

        adapter.fetch_message(topic="orders", partition=0, offset=5)

        mock_consumer.subscribe.assert_not_called()
