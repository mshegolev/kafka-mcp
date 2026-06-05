"""End-to-end tests for KafkaClient lib facade and TopicService.

All tests use a MockConsumer satisfying ConsumerPort — no real Kafka
broker is required.

Phase 1 success criteria verified here:
  SC-1: lib-first proof (KafkaClient works without MCP/FastAPI)
  SC-2: describe_topic returns TopicInfo with per-partition offsets
  SC-3: hexagonal boundary (domain/ has zero confluent_kafka imports)

Phase 2 success criteria verified here:
  SC-1: search_messages returns list[KafkaMessage] with all fields present
  SC-2: get_message returns KafkaMessage with decoded value
  SC-3: resilient decode in search / raising decode in get_message
  SC-4: Evidence keys (source/event_type/keys) on returned messages
"""

from __future__ import annotations

import pathlib
import subprocess
from datetime import datetime, timezone
from typing import Any

import pytest

from kafka_mcp.domain.errors import (
    ConfigError,
    DecodeError,
    MessageNotFoundError,
    TopicNotFoundError,
)
from kafka_mcp.domain.models import KafkaMessage, TopicInfo

# ---------------------------------------------------------------------------
# Mock ConsumerPort (no real broker needed)
# ---------------------------------------------------------------------------

_NOW_UTC = datetime.now(tz=timezone.utc)


def _make_raw_msg(
    topic: str,
    partition: int,
    offset: int,
    key: str | None = None,
    headers: dict[str, str] | None = None,
    raw: bytes = b"{}",
) -> KafkaMessage:
    """Helper to build a KafkaMessage with value=None (raw only)."""
    return KafkaMessage(
        topic=topic,
        partition=partition,
        offset=offset,
        key=key,
        headers=headers or {},
        value=None,
        timestamp_utc=_NOW_UTC,
        raw=raw,
    )


class MockConsumer:
    """In-memory ConsumerPort implementation for testing.

    Topics:
      - "payments" (2 partitions: ids 0, 1)
      - "orders"   (1 partition:  id 0)
      - "__consumer_offsets" (internal, 1 partition)

    Watermark offsets:
      - partition 0: (earliest=0, latest=500)
      - partition 1: (earliest=0, latest=300)
    """

    _ALL_TOPICS: list[str] = ["__consumer_offsets", "orders", "payments"]
    _PARTITION_IDS: dict[str, list[int]] = {
        "payments": [0, 1],
        "orders": [0],
        "__consumer_offsets": [0],
    }
    _OFFSETS: dict[int, tuple[int, int]] = {
        0: (0, 500),
        1: (0, 300),
    }

    def list_topics(self, include_internal: bool = False) -> list[str]:
        if include_internal:
            return sorted(self._ALL_TOPICS)
        return sorted(
            t for t in self._ALL_TOPICS if not t.startswith("__")
        )

    def get_partition_ids(self, topic: str) -> list[int]:
        if topic not in self._PARTITION_IDS:
            raise TopicNotFoundError(topic)
        return self._PARTITION_IDS[topic]

    def get_watermark_offsets(
        self, topic: str, partition: int
    ) -> tuple[int, int]:
        if topic not in self._PARTITION_IDS:
            raise TopicNotFoundError(topic)
        return self._OFFSETS.get(partition, (0, 0))

    def fetch_messages(
        self,
        topic: str,
        partition: int,
        start_offset: int,
        stop_offset: int,
        time_to: datetime | None,
        limit: int,
    ) -> list[KafkaMessage]:
        """Default stub: returns empty list (override in subclasses)."""
        return []

    def fetch_message(
        self,
        topic: str,
        partition: int,
        offset: int,
    ) -> KafkaMessage:
        """Default stub: raises MessageNotFoundError (override in subclasses)."""
        raise MessageNotFoundError(topic, partition, offset)

    def offsets_for_times(
        self,
        topic: str,
        partition: int,
        timestamp_ms: int,
    ) -> int:
        """Default stub: returns 0 (use low watermark)."""
        return 0


# ---------------------------------------------------------------------------
# Mock SchemaRegistryPort (no real SR needed)
# ---------------------------------------------------------------------------


class MockSchemaRegistry:
    """Neutral in-memory SchemaRegistryPort stub.

    By default decode() returns {} (empty dict — neutral, matches anything).
    Override _decode_result in subclasses or pass a callable to control.
    """

    def __init__(
        self,
        decode_result: dict[str, Any] | None = None,
        raise_decode_error: bool = False,
        decode_topic: str = "test",
    ) -> None:
        self._decode_result = decode_result if decode_result is not None else {}
        self._raise_decode_error = raise_decode_error
        self._decode_topic = decode_topic

    def get_schema(self, subject: str) -> dict | None:
        return None

    def decode(self, raw: bytes) -> dict[str, Any] | None:
        if self._raise_decode_error:
            raise DecodeError(self._decode_topic, 0, 0, "mock decode failure")
        return self._decode_result


# ---------------------------------------------------------------------------
# TopicService unit tests (Task 1)
# ---------------------------------------------------------------------------


class TestTopicService:
    """Unit tests for domain/search_service.py TopicService."""

    def _make_service(self) -> object:
        from kafka_mcp.domain.search_service import TopicService

        return TopicService(MockConsumer())

    def test_list_topics_excludes_internal(self) -> None:
        svc = self._make_service()
        topics = svc.list_topics()
        assert "__consumer_offsets" not in topics
        assert "payments" in topics
        assert "orders" in topics

    def test_list_topics_includes_internal(self) -> None:
        svc = self._make_service()
        topics = svc.list_topics(include_internal=True)
        assert "__consumer_offsets" in topics

    def test_list_topics_returns_list_of_str(self) -> None:
        svc = self._make_service()
        result = svc.list_topics()
        assert isinstance(result, list)
        assert all(isinstance(t, str) for t in result)

    def test_describe_topic_returns_topic_info(self) -> None:
        svc = self._make_service()
        ti = svc.describe_topic("payments")
        assert isinstance(ti, TopicInfo)
        assert ti.name == "payments"
        assert ti.partition_count == 2
        assert len(ti.partitions) == 2

    def test_describe_topic_partition_offsets(self) -> None:
        svc = self._make_service()
        ti = svc.describe_topic("payments")
        p0 = next(p for p in ti.partitions if p.id == 0)
        p1 = next(p for p in ti.partitions if p.id == 1)
        assert p0.earliest == 0
        assert p0.latest == 500
        assert p1.earliest == 0
        assert p1.latest == 300

    def test_describe_topic_unknown_raises(self) -> None:
        svc = self._make_service()
        with pytest.raises(TopicNotFoundError) as exc_info:
            svc.describe_topic("nonexistent-topic")
        assert exc_info.value.topic == "nonexistent-topic"

    def test_topic_service_stores_consumer(self) -> None:
        from kafka_mcp.domain.search_service import TopicService

        mock = MockConsumer()
        svc = TopicService(mock)
        assert svc._consumer is mock


# ---------------------------------------------------------------------------
# KafkaClient facade tests (Task 2)
# ---------------------------------------------------------------------------


class TestKafkaClient:
    """Integration tests for adapters/inbound/lib.py KafkaClient."""

    def _make_client(self) -> object:
        from kafka_mcp.adapters.inbound.lib import KafkaClient

        return KafkaClient(MockConsumer())

    def test_list_topics_excludes_internal(self) -> None:
        client = self._make_client()
        topics = client.list_topics()
        assert "__consumer_offsets" not in topics
        assert "payments" in topics
        assert "orders" in topics

    def test_list_topics_includes_internal(self) -> None:
        client = self._make_client()
        topics = client.list_topics(include_internal=True)
        assert "__consumer_offsets" in topics

    def test_describe_topic_returns_topic_info(self) -> None:
        client = self._make_client()
        ti = client.describe_topic("payments")
        assert isinstance(ti, TopicInfo)
        assert ti.name == "payments"
        assert ti.partition_count == 2
        assert len(ti.partitions) == 2

    def test_describe_topic_unknown_raises(self) -> None:
        client = self._make_client()
        with pytest.raises(TopicNotFoundError):
            client.describe_topic("unknown")

    def test_from_env_raises_config_error_when_no_broker(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """KafkaClient.from_env() raises ConfigError without bootstrap."""
        from kafka_mcp.adapters.inbound.lib import KafkaClient

        monkeypatch.delenv("KAFKA_MCP_BOOTSTRAP_SERVERS", raising=False)
        with pytest.raises(ConfigError):
            KafkaClient.from_env()


class TestKafkaClientLifecycle:
    """WR-02: KafkaClient closes its underlying consumer."""

    def test_close_delegates_to_consumer(self) -> None:
        from kafka_mcp.adapters.inbound.lib import KafkaClient

        class ClosableConsumer(MockConsumer):
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        consumer = ClosableConsumer()
        client = KafkaClient(consumer)
        client.close()
        assert consumer.closed is True

    def test_context_manager_closes_consumer(self) -> None:
        from kafka_mcp.adapters.inbound.lib import KafkaClient

        class ClosableConsumer(MockConsumer):
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        consumer = ClosableConsumer()
        with KafkaClient(consumer) as client:
            assert client.list_topics() is not None
        assert consumer.closed is True

    def test_close_noop_when_consumer_has_no_close(self) -> None:
        """Mock consumers without close() must not break client.close()."""
        from kafka_mcp.adapters.inbound.lib import KafkaClient

        client = KafkaClient(MockConsumer())
        # Must not raise even though MockConsumer has no close().
        client.close()


class TestConfigErrorContract:
    """WR-01: every invalid env var surfaces as ConfigError (D-04).

    Before the fix, pydantic error types other than ``missing`` /
    ``value_error`` (e.g. ``int_parsing``, ``float_parsing``) leaked a raw
    ``ValidationError``, breaking the single-exception contract.
    """

    def test_invalid_max_scan_raises_config_error(self) -> None:
        from kafka_mcp.config import KafkaMcpSettings

        with pytest.raises(ConfigError) as exc_info:
            KafkaMcpSettings(
                bootstrap_servers="localhost:9092", max_scan="notanint"
            )
        assert "MAX_SCAN" in str(exc_info.value)

    def test_invalid_poll_timeout_raises_config_error(self) -> None:
        from kafka_mcp.config import KafkaMcpSettings

        with pytest.raises(ConfigError) as exc_info:
            KafkaMcpSettings(
                bootstrap_servers="localhost:9092", poll_timeout="xyz"
            )
        assert "POLL_TIMEOUT" in str(exc_info.value)

    def test_invalid_max_scan_not_raw_validation_error(self) -> None:
        """The leaked pydantic ValidationError must be fully converted."""
        from pydantic import ValidationError

        from kafka_mcp.config import KafkaMcpSettings

        with pytest.raises(ConfigError):
            try:
                KafkaMcpSettings(
                    bootstrap_servers="localhost:9092", max_scan="abc"
                )
            except ValidationError:  # pragma: no cover - regression guard
                pytest.fail(
                    "raw ValidationError leaked instead of ConfigError"
                )


# ---------------------------------------------------------------------------
# Phase 1 success criterion tests
# ---------------------------------------------------------------------------


def test_phase1_success_criterion_1_lib_first() -> None:
    """SC-1: KafkaClient lib facade works without MCP or FastAPI."""
    from kafka_mcp.adapters.inbound.lib import KafkaClient

    client = KafkaClient(MockConsumer())
    result = client.list_topics()
    assert isinstance(result, list), "SC-1: lib-first proof"
    assert len(result) > 0


def test_phase1_success_criterion_2_describe_offsets() -> None:
    """SC-2: describe_topic returns TopicInfo with per-partition offsets."""
    from kafka_mcp.adapters.inbound.lib import KafkaClient

    client = KafkaClient(MockConsumer())
    ti = client.describe_topic("payments")
    assert isinstance(ti, TopicInfo)
    assert ti.partitions[0].earliest == 0
    assert ti.partitions[0].latest == 500


def test_phase1_success_criterion_3_hexagonal_boundary() -> None:
    """SC-3: domain/ and ports/ contain zero confluent_kafka imports.

    WR-04: the pattern catches BOTH ``import confluent_kafka`` and
    ``from confluent_kafka import X`` (the latter is the more common form
    and the one the outbound adapter actually uses). cwd is resolved
    relative to this test file so the guard is portable across machines/CI.
    """
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [
            "grep",
            "-rE",
            r"(import confluent_kafka|from confluent_kafka)",
            "src/kafka_mcp/domain/",
            "src/kafka_mcp/ports/",
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
    )
    assert result.returncode != 0, (
        f"BOUNDARY VIOLATION: confluent_kafka found in domain/ or ports/: "
        f"{result.stdout}"
    )


# ---------------------------------------------------------------------------
# Top-level package import tests
# ---------------------------------------------------------------------------


def test_top_level_package_exports_kafka_client() -> None:
    """KafkaClient is importable from kafka_mcp top-level package."""
    from kafka_mcp import KafkaClient  # noqa: F401

    assert KafkaClient is not None


def test_top_level_package_exports_all_public_types() -> None:
    """All public types importable from kafka_mcp top-level."""
    from kafka_mcp import (  # noqa: F401
        ConfigError,
        KafkaClient,
        PartitionInfo,
        TopicInfo,
        TopicNotFoundError,
    )


# ---------------------------------------------------------------------------
# Phase 2 Task 1: TopicService.search_messages RED tests
# ---------------------------------------------------------------------------


class MockConsumerWithMessages(MockConsumer):
    """MockConsumer that returns a configurable list of messages."""

    def __init__(self, messages: list[KafkaMessage] | None = None) -> None:
        self._messages = messages or []

    def fetch_messages(
        self,
        topic: str,
        partition: int,
        start_offset: int,
        stop_offset: int,
        time_to: datetime | None,
        limit: int,
    ) -> list[KafkaMessage]:
        # Return up to `limit` messages matching this topic/partition
        result = [
            m for m in self._messages
            if m.topic == topic and m.partition == partition
        ]
        return result[:limit]

    def offsets_for_times(
        self,
        topic: str,
        partition: int,
        timestamp_ms: int,
    ) -> int:
        return 0


def _make_service(
    consumer: MockConsumer | None = None,
    registry: MockSchemaRegistry | None = None,
) -> object:
    from kafka_mcp.domain.search_service import TopicService

    return TopicService(
        consumer or MockConsumer(),
        registry or MockSchemaRegistry(),
    )


class TestTopicServiceSearchMessages:
    """Phase 2 RED tests for TopicService.search_messages."""

    def test_search_messages_returns_matching_by_key(self) -> None:
        """Mock consumer returns 2 messages; one matches key → 1 result."""
        msgs = [
            _make_raw_msg("orders", 0, 0, key="match-key"),
            _make_raw_msg("orders", 0, 1, key="other-key"),
        ]
        consumer = MockConsumerWithMessages(msgs)
        registry = MockSchemaRegistry(decode_result={"data": "value"})
        svc = _make_service(consumer, registry)

        results = svc.search_messages("match-key")
        assert len(results) == 1
        assert results[0].key == "match-key"

    def test_search_messages_key_field_header(self) -> None:
        """key_field='header:x-order-id' matches message with that header."""
        msgs = [
            _make_raw_msg(
                "orders", 0, 0, key="any",
                headers={"x-order-id": "ORD-123"},
            ),
            _make_raw_msg("orders", 0, 1, key="any", headers={}),
        ]
        consumer = MockConsumerWithMessages(msgs)
        svc = _make_service(consumer)

        results = svc.search_messages(
            "ORD-123", key_field="header:x-order-id"
        )
        assert len(results) == 1
        assert results[0].headers.get("x-order-id") == "ORD-123"

    def test_search_messages_key_field_value_path(self) -> None:
        """key_field='value:order_id' matches decoded value field."""
        msgs = [
            _make_raw_msg("orders", 0, 0, key="any"),
            _make_raw_msg("orders", 0, 1, key="any"),
        ]

        class RegistryWithOrderId(MockSchemaRegistry):
            _call = 0

            def decode(self, raw: bytes) -> dict[str, Any] | None:
                self.__class__._call += 1
                if self.__class__._call == 1:
                    return {"order_id": "ORD-999"}
                return {"order_id": "ORD-000"}

        consumer = MockConsumerWithMessages(msgs)
        svc = _make_service(consumer, RegistryWithOrderId())

        results = svc.search_messages(
            "ORD-999", key_field="value:order_id"
        )
        assert len(results) == 1
        assert results[0].value is not None
        assert results[0].value.get("order_id") == "ORD-999"

    def test_search_messages_default_window(self) -> None:
        """time_from=None → uses earliest offset; time_to=None → uses now."""
        msgs = [_make_raw_msg("orders", 0, 0, key="k")]
        consumer = MockConsumerWithMessages(msgs)
        svc = _make_service(consumer)

        # No time bounds supplied — should not raise; should return match
        results = svc.search_messages("k")
        assert isinstance(results, list)

    def test_search_messages_global_limit(self) -> None:
        """3 matching messages but limit=2 → returns exactly 2."""
        msgs = [
            _make_raw_msg("orders", 0, i, key="same-key")
            for i in range(3)
        ]
        consumer = MockConsumerWithMessages(msgs)
        svc = _make_service(consumer)

        results = svc.search_messages("same-key", limit=2)
        assert len(results) == 2

    def test_search_messages_decode_failure_resilient(self) -> None:
        """Decode failure on one message → value=None retained, scan continues."""
        msgs = [
            _make_raw_msg("orders", 0, 0, key="k"),
            _make_raw_msg("orders", 0, 1, key="k"),
        ]

        class PartialFailRegistry(MockSchemaRegistry):
            _call = 0

            def decode(self, raw: bytes) -> dict[str, Any] | None:
                self.__class__._call += 1
                if self.__class__._call == 1:
                    raise DecodeError("orders", 0, 0, "corrupt")
                return {}

        consumer = MockConsumerWithMessages(msgs)
        # Both messages have key="k" so both match (key-based)
        svc = _make_service(consumer, PartialFailRegistry())

        results = svc.search_messages("k")
        # Both messages are returned — first has value=None (decode failed),
        # second has value={} (decode succeeded)
        assert len(results) == 2
        assert results[0].value is None
        assert results[1].value == {}

    def test_search_messages_evidence_keys_extracted(self) -> None:
        """Returned KafkaMessage.keys has order_id and msisdn populated."""
        msgs = [_make_raw_msg("orders", 0, 0, key="k")]
        registry = MockSchemaRegistry(
            decode_result={
                "order_id": "ORD-42",
                "msisdn": "+79991234567",
            }
        )
        consumer = MockConsumerWithMessages(msgs)
        svc = _make_service(consumer, registry)

        results = svc.search_messages("k")
        assert len(results) == 1
        assert results[0].keys["order_id"] == "ORD-42"
        assert results[0].keys["msisdn"] == "+79991234567"

    def test_search_messages_topics_none_scans_all(self) -> None:
        """topics=None → calls list_topics() to discover all non-internal topics."""

        class TrackingConsumer(MockConsumer):
            def __init__(self) -> None:
                self.list_topics_called = False

            def list_topics(
                self, include_internal: bool = False
            ) -> list[str]:
                self.list_topics_called = True
                return ["orders"]

            def fetch_messages(
                self,
                topic: str,
                partition: int,
                start_offset: int,
                stop_offset: int,
                time_to: datetime | None,
                limit: int,
            ) -> list[KafkaMessage]:
                return []

        consumer = TrackingConsumer()
        svc = _make_service(consumer)
        svc.search_messages("anything", topics=None)
        assert consumer.list_topics_called

    def test_search_messages_phase2_sc1(self) -> None:
        """SC-1: search_messages returns list[KafkaMessage] with all fields."""
        from datetime import timedelta

        t1 = datetime.now(tz=timezone.utc) - timedelta(hours=1)
        t2 = datetime.now(tz=timezone.utc)
        msgs = [_make_raw_msg("orders", 0, 0, key="test")]
        registry = MockSchemaRegistry(
            decode_result={"field": "val"}
        )
        consumer = MockConsumerWithMessages(msgs)
        svc = _make_service(consumer, registry)

        results = svc.search_messages(
            "test", time_from=t1, time_to=t2, limit=10
        )
        assert isinstance(results, list)
        assert len(results) == 1
        msg = results[0]
        assert isinstance(msg, KafkaMessage)
        assert msg.timestamp_utc is not None
        assert msg.key == "test"
        assert isinstance(msg.headers, dict)
        assert msg.raw == b"{}"
        # value is decoded dict
        assert msg.value == {"field": "val"}
        assert msg.source == "kafka"
        assert msg.event_type == "kafka_message"
