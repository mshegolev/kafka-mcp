"""Search/get_message real-wire round-trip integration tests.

Validates that search_messages and get_message work against a real Kafka
broker with seeded JSON messages. All assertions verify decoded values,
evidence fields, and error handling against real wire protocols.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from kafka_mcp.domain.errors import MessageNotFoundError

pytestmark = pytest.mark.integration


class TestSearchMessages:
    """search_messages against a real broker with seeded JSON messages."""

    def test_search_messages_finds_seeded_json(self, kafka_client, seed_json_topic):
        """Search by key finds the seeded JSON message with decoded value."""
        results = kafka_client.search_messages("key-0", topics=[seed_json_topic])
        assert len(results) >= 1
        msg = results[0]
        assert msg.key == "key-0"
        assert msg.value is not None
        assert msg.value["order_id"] == "ORD-0"
        assert msg.value["amount"] == 100
        assert msg.source == "kafka"
        assert msg.event_type == "kafka_message"

    def test_search_messages_no_match(self, kafka_client, seed_json_topic):
        """Search for a nonexistent key returns an empty list."""
        results = kafka_client.search_messages("nonexistent-key-xyz", topics=[seed_json_topic])
        assert results == []

    def test_search_messages_respects_limit(self, kafka_client, seed_json_topic):
        """Search with limit=1 returns at most 1 message."""
        results = kafka_client.search_messages("key-0", topics=[seed_json_topic], limit=1)
        assert len(results) <= 1


class TestGetMessage:
    """get_message against a real broker with seeded JSON messages."""

    def test_get_message_returns_real_message(self, kafka_client, seed_json_topic):
        """get_message fetches a real message by exact coordinates."""
        info = kafka_client.describe_topic(seed_json_topic)
        p0 = info.partitions[0]
        assert p0.latest > p0.earliest, "seeded topic should have messages"
        msg = kafka_client.get_message(seed_json_topic, p0.id, p0.earliest)
        assert msg.topic == seed_json_topic
        assert msg.partition == p0.id
        assert msg.offset == p0.earliest
        assert msg.value is not None  # JSON decoded
        assert isinstance(msg.timestamp_utc, datetime)
        assert msg.raw is not None and len(msg.raw) > 0

    def test_get_message_evidence_fields(self, kafka_client, seed_json_topic):
        """get_message populates evidence keys from decoded JSON value."""
        info = kafka_client.describe_topic(seed_json_topic)
        p0 = info.partitions[0]
        msg = kafka_client.get_message(seed_json_topic, p0.id, p0.earliest)
        assert msg.source == "kafka"
        assert msg.event_type == "kafka_message"
        assert isinstance(msg.keys, dict)
        # Seeded JSON messages have order_id and customer_id
        assert "order_id" in msg.keys
        assert "customer_id" in msg.keys

    def test_get_message_not_found_raises(self, kafka_client, seed_json_topic):
        """get_message raises MessageNotFoundError for an out-of-range offset."""
        info = kafka_client.describe_topic(seed_json_topic)
        p0 = info.partitions[0]
        with pytest.raises(MessageNotFoundError):
            kafka_client.get_message(seed_json_topic, p0.id, p0.latest + 1000)
