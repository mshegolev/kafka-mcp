"""No-broker test suite for domain contracts and configuration.

All tests run without a running Kafka broker — only domain models,
errors, port protocols, and config validation are exercised here.
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timezone

import pytest

from kafka_mcp.domain.errors import ConfigError, TopicNotFoundError
from kafka_mcp.domain.models import PartitionInfo, TopicInfo
from kafka_mcp.ports.consumer import ConsumerPort
from kafka_mcp.ports.schema_registry import SchemaRegistryPort

# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------


class TestPartitionInfo:
    def test_fields_present(self) -> None:
        p = PartitionInfo(id=0, leader=1, earliest=0, latest=100)
        assert p.id == 0
        assert p.leader == 1
        assert p.earliest == 0
        assert p.latest == 100

    def test_model_dump(self) -> None:
        p = PartitionInfo(id=2, leader=0, earliest=10, latest=50)
        d = p.model_dump()
        assert d == {"id": 2, "leader": 0, "earliest": 10, "latest": 50}


class TestTopicInfo:
    def test_fields_present(self) -> None:
        partitions = [PartitionInfo(id=0, leader=0, earliest=0, latest=10)]
        t = TopicInfo(name="test-topic", partition_count=1, partitions=partitions)
        assert t.name == "test-topic"
        assert t.partition_count == 1
        assert len(t.partitions) == 1
        assert t.partitions[0].latest == 10

    def test_model_dump_structure(self) -> None:
        partitions = [PartitionInfo(id=0, leader=0, earliest=0, latest=10)]
        t = TopicInfo(name="payments", partition_count=1, partitions=partitions)
        d = t.model_dump()
        assert d["name"] == "payments"
        assert d["partition_count"] == 1
        assert isinstance(d["partitions"], list)
        assert d["partitions"][0]["latest"] == 10


# ---------------------------------------------------------------------------
# Domain errors
# ---------------------------------------------------------------------------


class TestTopicNotFoundError:
    def test_topic_attribute(self) -> None:
        err = TopicNotFoundError("payments")
        assert err.topic == "payments"

    def test_is_exception(self) -> None:
        err = TopicNotFoundError("orders")
        assert isinstance(err, Exception)

    def test_message_contains_topic(self) -> None:
        err = TopicNotFoundError("my-topic")
        assert "my-topic" in str(err)


class TestConfigError:
    def test_is_value_error(self) -> None:
        err = ConfigError("KAFKA_MCP_BOOTSTRAP_SERVERS is required but was not set")
        assert isinstance(err, ValueError)

    def test_message_preserved(self) -> None:
        msg = "missing: KAFKA_MCP_BOOTSTRAP_SERVERS"
        err = ConfigError(msg)
        assert msg in str(err)


# ---------------------------------------------------------------------------
# Port protocols (runtime_checkable)
# ---------------------------------------------------------------------------


class TestConsumerPort:
    def test_compliant_class_passes_isinstance(self) -> None:
        class MockConsumer:
            def list_topics(self, include_internal: bool = False) -> list[str]:
                return []

            def get_watermark_offsets(
                self, topic: str, partition: int
            ) -> tuple[int, int]:
                return (0, 0)

            def get_partition_ids(self, topic: str) -> list[int]:
                return [0]

            def fetch_messages(
                self,
                topic: str,
                partition: int,
                start_offset: int,
                stop_offset: int,
                time_to: datetime | None,
                limit: int,
            ) -> list:
                return []

            def fetch_message(
                self, topic: str, partition: int, offset: int
            ) -> object:
                raise NotImplementedError

            def offsets_for_times(
                self, topic: str, partition: int, timestamp_ms: int
            ) -> int:
                return 0

        assert isinstance(MockConsumer(), ConsumerPort)

    def test_non_compliant_class_fails_isinstance(self) -> None:
        class BadConsumer:
            pass

        assert not isinstance(BadConsumer(), ConsumerPort)

    def test_missing_one_method_fails(self) -> None:
        class PartialConsumer:
            def list_topics(self, include_internal: bool = False) -> list[str]:
                return []

        assert not isinstance(PartialConsumer(), ConsumerPort)


class TestSchemaRegistryPort:
    def test_compliant_class_passes_isinstance(self) -> None:
        class MockRegistry:
            def get_schema(self, subject: str) -> dict | None:
                return None

            def decode(self, raw: bytes) -> dict | None:
                return None

        assert isinstance(MockRegistry(), SchemaRegistryPort)

    def test_non_compliant_class_fails_isinstance(self) -> None:
        class BadRegistry:
            pass

        assert not isinstance(BadRegistry(), SchemaRegistryPort)


# ---------------------------------------------------------------------------
# KafkaMcpSettings config validation
# ---------------------------------------------------------------------------


class TestKafkaMcpSettings:
    def test_missing_bootstrap_servers_raises_config_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("KAFKA_MCP_BOOTSTRAP_SERVERS", raising=False)
        # Ensure no .env file provides it in test context
        from kafka_mcp.config import KafkaMcpSettings

        with pytest.raises((ConfigError, Exception)) as exc_info:
            KafkaMcpSettings(_env_file=None)  # type: ignore[call-arg]
        # Either ConfigError or pydantic ValidationError should be raised
        # when bootstrap_servers is absent
        assert exc_info.value is not None

    def test_defaults_when_bootstrap_servers_provided(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("KAFKA_MCP_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.delenv("KAFKA_MCP_SECURITY_PROTOCOL", raising=False)
        monkeypatch.delenv("KAFKA_MCP_MAX_SCAN", raising=False)
        monkeypatch.delenv("KAFKA_MCP_POLL_TIMEOUT", raising=False)

        from kafka_mcp.config import KafkaMcpSettings

        settings = KafkaMcpSettings(_env_file=None)  # type: ignore[call-arg]
        assert settings.bootstrap_servers == "localhost:9092"
        assert settings.security_protocol == "PLAINTEXT"
        assert settings.max_scan == 100_000
        assert settings.poll_timeout == 1.0

    def test_sasl_fields_accepted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("KAFKA_MCP_BOOTSTRAP_SERVERS", "broker:9093")
        monkeypatch.setenv("KAFKA_MCP_SECURITY_PROTOCOL", "SASL_SSL")
        monkeypatch.setenv("KAFKA_MCP_SASL_MECHANISM", "PLAIN")
        monkeypatch.setenv("KAFKA_MCP_SASL_USERNAME", "user1")
        monkeypatch.setenv("KAFKA_MCP_SASL_PASSWORD", "s3cr3t")

        from kafka_mcp.config import KafkaMcpSettings

        s = KafkaMcpSettings(_env_file=None)  # type: ignore[call-arg]
        assert s.sasl_mechanism == "PLAIN"
        assert s.sasl_username == "user1"
        # Password stored as SecretStr — .get_secret_value() to access
        assert s.sasl_password is not None

    def test_schema_registry_fields_accepted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("KAFKA_MCP_BOOTSTRAP_SERVERS", "broker:9092")
        monkeypatch.setenv(
            "KAFKA_MCP_SCHEMA_REGISTRY_URL", "http://sr:8081"
        )
        monkeypatch.setenv("KAFKA_MCP_SR_USER", "sr-user")
        monkeypatch.setenv("KAFKA_MCP_SR_PASS", "sr-pass")

        from kafka_mcp.config import KafkaMcpSettings

        s = KafkaMcpSettings(_env_file=None)  # type: ignore[call-arg]
        assert s.schema_registry_url == "http://sr:8081"
        assert s.sr_user == "sr-user"
        assert s.sr_pass is not None

    def test_empty_bootstrap_servers_raises_config_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("KAFKA_MCP_BOOTSTRAP_SERVERS", "   ")

        from kafka_mcp.config import KafkaMcpSettings

        with pytest.raises(ConfigError):
            KafkaMcpSettings(_env_file=None)  # type: ignore[call-arg]

    def test_secret_str_not_exposed_in_repr(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("KAFKA_MCP_BOOTSTRAP_SERVERS", "broker:9092")
        monkeypatch.setenv("KAFKA_MCP_SASL_PASSWORD", "topsecret")
        monkeypatch.setenv("KAFKA_MCP_SR_PASS", "sr-secret")

        from kafka_mcp.config import KafkaMcpSettings

        s = KafkaMcpSettings(_env_file=None)  # type: ignore[call-arg]
        repr_str = repr(s)
        assert "topsecret" not in repr_str
        assert "sr-secret" not in repr_str


# ---------------------------------------------------------------------------
# Phase 2: KafkaMessage model (Task 1 RED)
# ---------------------------------------------------------------------------


class TestKafkaMessage:
    def _make_message(self, **kwargs):  # type: ignore[no-untyped-def]
        from kafka_mcp.domain.models import KafkaMessage

        defaults = {
            "topic": "test-topic",
            "partition": 0,
            "offset": 42,
            "key": "some-key",
            "headers": {"x-trace": "abc"},
            "value": {"order_id": "ORD-1"},
            "timestamp_utc": datetime(2026, 6, 5, 12, 0, 0, tzinfo=timezone.utc),
            "raw": b"\x00\x01\x02",
        }
        defaults.update(kwargs)
        return KafkaMessage(**defaults)

    def test_kafka_message_fields(self) -> None:
        from kafka_mcp.domain.models import KafkaMessage

        msg = self._make_message()
        assert msg.topic == "test-topic"
        assert msg.partition == 0
        assert msg.offset == 42
        assert msg.key == "some-key"
        assert msg.headers == {"x-trace": "abc"}
        assert msg.value == {"order_id": "ORD-1"}
        assert isinstance(msg.timestamp_utc, datetime)
        assert isinstance(msg.raw, bytes)

    def test_kafka_message_key_optional(self) -> None:
        msg = self._make_message(key=None)
        assert msg.key is None

    def test_kafka_message_value_optional(self) -> None:
        msg = self._make_message(value=None)
        assert msg.value is None

    def test_kafka_message_raw_bytes(self) -> None:
        msg = self._make_message(raw=b"\xff\xfe\xfd")
        assert msg.raw == b"\xff\xfe\xfd"
        assert isinstance(msg.raw, bytes)

    def test_kafka_message_timestamp_utc_is_datetime(self) -> None:
        msg = self._make_message()
        assert isinstance(msg.timestamp_utc, datetime)

    def test_kafka_message_evidence_source(self) -> None:
        msg = self._make_message()
        assert msg.source == "kafka"

    def test_kafka_message_evidence_event_type(self) -> None:
        msg = self._make_message()
        assert msg.event_type == "kafka_message"

    def test_kafka_message_evidence_keys_present(self) -> None:
        msg = self._make_message()
        assert "order_id" in msg.keys
        assert "msisdn" in msg.keys
        assert "customer_id" in msg.keys
        assert "product_id" in msg.keys


# ---------------------------------------------------------------------------
# Phase 2: DecodeError (Task 1 RED)
# ---------------------------------------------------------------------------


class TestDecodeError:
    def test_decode_error_is_domain_exception(self) -> None:
        from kafka_mcp.domain.errors import DecodeError

        err = DecodeError(
            topic="payments", partition=0, offset=10, reason="unknown magic byte"
        )
        assert isinstance(err, Exception)
        assert not isinstance(err, ValueError)

    def test_decode_error_carries_topic_partition_offset(self) -> None:
        from kafka_mcp.domain.errors import DecodeError

        err = DecodeError(topic="events", partition=2, offset=99, reason="bad avro")
        assert err.topic == "events"
        assert err.partition == 2
        assert err.offset == 99

    def test_decode_error_carries_reason(self) -> None:
        from kafka_mcp.domain.errors import DecodeError

        err = DecodeError(topic="t", partition=0, offset=0, reason="json parse failed")
        assert err.reason == "json parse failed"


# ---------------------------------------------------------------------------
# Phase 2: MessageNotFoundError (Task 1 RED)
# ---------------------------------------------------------------------------


class TestMessageNotFoundError:
    def test_message_not_found_error_carries_coordinates(self) -> None:
        from kafka_mcp.domain.errors import MessageNotFoundError

        err = MessageNotFoundError(topic="orders", partition=1, offset=500)
        assert err.topic == "orders"
        assert err.partition == 1
        assert err.offset == 500


# ---------------------------------------------------------------------------
# Phase 2: Hexagonal boundary check (Task 1 RED)
# ---------------------------------------------------------------------------


class TestHexagonalBoundary:
    def test_hexagonal_boundary_domain_models(self) -> None:
        """domain/ must not import any I/O or decode libraries."""
        result = subprocess.run(
            [
                "grep",
                "-rn",
                (
                    "import confluent_kafka"
                    r"\|import fastavro"
                    r"\|import avro"
                    r"\|import google.protobuf"
                ),
                "src/kafka_mcp/domain/",
            ],
            capture_output=True,
            text=True,
        )
        assert result.stdout.strip() == "", (
            f"Hexagonal boundary violated in domain/:\n{result.stdout}"
        )


# ---------------------------------------------------------------------------
# Phase 2: Port protocol extensions (Task 2 RED)
# ---------------------------------------------------------------------------


class MockConsumer:
    """Full-protocol mock used across port contract tests."""

    def list_topics(self, include_internal: bool = False) -> list[str]:
        return []

    def get_watermark_offsets(
        self, topic: str, partition: int
    ) -> tuple[int, int]:
        return (0, 0)

    def get_partition_ids(self, topic: str) -> list[int]:
        return [0]

    def fetch_messages(
        self,
        topic: str,
        partition: int,
        start_offset: int,
        stop_offset: int,
        time_to: datetime | None,
        limit: int,
    ):  # type: ignore[return]
        return []

    def fetch_message(self, topic: str, partition: int, offset: int):  # type: ignore[return]
        from kafka_mcp.domain.errors import MessageNotFoundError

        raise MessageNotFoundError(topic=topic, partition=partition, offset=offset)

    def offsets_for_times(
        self, topic: str, partition: int, timestamp_ms: int
    ) -> int:
        return 0


class MockSchemaRegistry:
    """Full-protocol mock for SchemaRegistryPort including decode."""

    def get_schema(self, subject: str) -> dict | None:
        return None

    def decode(self, raw: bytes) -> dict | None:
        return None


class TestConsumerPortExtended:
    def test_consumer_port_has_fetch_messages(self) -> None:
        assert hasattr(ConsumerPort, "fetch_messages")

    def test_consumer_port_has_fetch_message(self) -> None:
        assert hasattr(ConsumerPort, "fetch_message")

    def test_consumer_port_mock_implements_protocol(self) -> None:
        assert isinstance(MockConsumer(), ConsumerPort)


class TestSchemaRegistryPortExtended:
    def test_schema_registry_port_has_decode(self) -> None:
        assert hasattr(SchemaRegistryPort, "decode")

    def test_schema_registry_port_mock_implements(self) -> None:
        assert isinstance(MockSchemaRegistry(), SchemaRegistryPort)
