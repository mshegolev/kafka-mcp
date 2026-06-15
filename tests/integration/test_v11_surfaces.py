"""v1.1 surface integration tests — key decode, schema_id, consumer_group_lag.

Validates the v1.1 additions:
  - KEY-01/KEY-02: Schema-encoded key decode (key_decoded, schema_id.key)
  - LAG-01/LAG-03: consumer_group_lag with real committed offsets + Evidence fields

All tests run against real testcontainers Kafka + Schema Registry.
"""

from __future__ import annotations

import pytest

from kafka_mcp.domain.models import LagRecord

pytestmark = pytest.mark.integration


class TestKeyDecode:
    """KEY-01/KEY-02: Schema-encoded key decode against real SR."""

    def test_key_decode_avro_key(self, kafka_client, seed_avro_key_topic):
        """Avro-encoded key is decoded to key_decoded dict (KEY-01)."""
        results = kafka_client.search_messages(
            "KEY-ORD-0",
            topics=[seed_avro_key_topic],
            key_field="value:order_id",
        )
        assert len(results) >= 1
        msg = results[0]
        # key_decoded should be populated (Avro-encoded key)
        assert msg.key_decoded is not None, "Schema-encoded key should be decoded"
        assert msg.key_decoded["order_id"] == "KEY-ORD-0"
        assert msg.key_decoded["region"] == "EU"

    def test_key_decode_schema_id_includes_key(self, kafka_client, seed_avro_key_topic):
        """schema_id dict has both key and value schema IDs (KEY-02)."""
        info = kafka_client.describe_topic(seed_avro_key_topic)
        p0 = info.partitions[0]
        msg = kafka_client.get_message(seed_avro_key_topic, p0.id, p0.earliest)
        assert msg.schema_id is not None
        assert "key" in msg.schema_id
        assert "value" in msg.schema_id
        assert msg.schema_id["key"] is not None and msg.schema_id["key"] > 0
        assert msg.schema_id["value"] is not None and msg.schema_id["value"] > 0

    def test_plain_key_no_crash(self, kafka_client, seed_json_topic):
        """Plain string key → key_decoded is None, no crash (KEY-01 fallback)."""
        info = kafka_client.describe_topic(seed_json_topic)
        p0 = info.partitions[0]
        msg = kafka_client.get_message(seed_json_topic, p0.id, p0.earliest)
        # Plain string key → not schema-encoded → key_decoded is None
        assert msg.key_decoded is None
        # key should still be the plain string
        assert msg.key is not None


class TestConsumerGroupLag:
    """LAG-01/LAG-03: consumer_group_lag against real broker."""

    def test_consumer_group_lag_real_group(self, kafka_client, seed_lag_consumer, seed_json_topic):
        """consumer_group_lag returns LagRecord with Evidence fields (LAG-01/LAG-03)."""
        records = kafka_client.consumer_group_lag("test-lag-group", topics=[seed_json_topic])
        assert len(records) >= 1, "Should have lag records for test-lag-group"
        rec = records[0]
        assert isinstance(rec, LagRecord)
        assert rec.group == "test-lag-group"
        assert rec.topic == seed_json_topic
        assert rec.partition >= 0
        assert rec.current_offset >= 0
        assert rec.end_offset > 0
        assert rec.lag >= 0
        # LAG-03: Evidence fields
        assert rec.source == "kafka"
        assert rec.event_type == "consumer_lag"
        assert rec.timestamp_utc is not None

    def test_consumer_group_lag_has_positive_lag(self, kafka_client, seed_lag_consumer, seed_json_topic):
        """Partially consumed group should have positive total lag (LAG-01)."""
        records = kafka_client.consumer_group_lag("test-lag-group", topics=[seed_json_topic])
        total_lag = sum(r.lag for r in records)
        # Consumed 2 of 5, so lag should be approximately 3 (may vary by partition)
        assert total_lag > 0, "Should have positive lag (consumed fewer than produced)"

    def test_consumer_group_lag_unknown_group(self, kafka_client):
        """Unknown consumer group returns empty list."""
        records = kafka_client.consumer_group_lag("nonexistent-group-xyz-99")
        assert records == [], "Unknown group should return empty list"
