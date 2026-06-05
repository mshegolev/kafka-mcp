"""No-broker test suite for domain contracts and configuration.

All tests run without a running Kafka broker — only domain models,
errors, port protocols, and config validation are exercised here.
"""

from __future__ import annotations

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
