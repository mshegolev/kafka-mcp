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

import typing
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
        from confluent_kafka import KafkaException

        from kafka_mcp.domain.errors import TopicNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.side_effect = KafkaException(
            "Unknown topic"
        )
        adapter = self._make_adapter(mock_consumer)
        with pytest.raises(TopicNotFoundError) as exc_info:
            adapter.get_watermark_offsets("nonexistent", 0)
        assert exc_info.value.topic == "nonexistent"


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
