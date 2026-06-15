"""Decode round-trip integration tests — Avro, Protobuf, JSON.

Validates that messages produced with real Confluent wire framing (Avro,
Protobuf) and plain JSON are correctly decoded by the KafkaClient's
search_messages and get_message paths against a live Schema Registry.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


class TestAvroDecode:
    """Avro decode against real Schema Registry."""

    def test_avro_decode_round_trip(self, kafka_client, seed_avro_topic):
        """Search finds Avro-encoded message and decodes values correctly."""
        results = kafka_client.search_messages("avro-key-0", topics=[seed_avro_topic])
        assert len(results) >= 1
        msg = results[0]
        assert msg.value is not None, "Avro message should be decoded"
        assert msg.value["order_id"] == "AVRO-ORD-0"
        assert msg.value["amount"] == 200
        assert msg.value["customer_id"] == "CUST-0"

    def test_avro_get_message_decode(self, kafka_client, seed_avro_topic):
        """get_message decodes Avro-encoded value correctly."""
        info = kafka_client.describe_topic(seed_avro_topic)
        p0 = info.partitions[0]
        msg = kafka_client.get_message(seed_avro_topic, p0.id, p0.earliest)
        assert msg.value is not None, "Avro get_message should decode"
        assert "order_id" in msg.value
        assert msg.value["order_id"].startswith("AVRO-ORD-")

    def test_avro_schema_id_populated(self, kafka_client, seed_avro_topic):
        """Avro-encoded message has schema_id dict with value schema ID."""
        info = kafka_client.describe_topic(seed_avro_topic)
        p0 = info.partitions[0]
        msg = kafka_client.get_message(seed_avro_topic, p0.id, p0.earliest)
        assert msg.schema_id is not None
        assert "value" in msg.schema_id
        assert msg.schema_id["value"] is not None
        assert isinstance(msg.schema_id["value"], int)
        assert msg.schema_id["value"] > 0


class TestProtobufDecode:
    """Protobuf decode against real Schema Registry."""

    def test_protobuf_decode_round_trip(self, kafka_client, seed_protobuf_topic):
        """Search finds Protobuf-framed message and decodes values correctly."""
        results = kafka_client.search_messages("proto-key-0", topics=[seed_protobuf_topic])
        assert len(results) >= 1
        msg = results[0]
        assert msg.value is not None, "Protobuf message should be decoded"
        # Protobuf decode uses preserving_proto_field_name=True (snake_case)
        assert msg.value["event_id"] == "EVT-0"
        assert msg.value["priority"] == 10
        assert msg.value["description"] == "Test event 0"

    def test_protobuf_get_message_decode(self, kafka_client, seed_protobuf_topic):
        """get_message decodes Protobuf-framed value correctly."""
        info = kafka_client.describe_topic(seed_protobuf_topic)
        p0 = info.partitions[0]
        msg = kafka_client.get_message(seed_protobuf_topic, p0.id, p0.earliest)
        assert msg.value is not None, "Protobuf get_message should decode"
        assert "event_id" in msg.value
        assert msg.value["event_id"].startswith("EVT-")


class TestJsonDecode:
    """JSON decode (no Confluent framing) against real broker."""

    def test_json_decode_round_trip(self, kafka_client, seed_json_topic):
        """Search finds plain JSON message and decodes values correctly."""
        results = kafka_client.search_messages("key-1", topics=[seed_json_topic])
        assert len(results) >= 1
        msg = results[0]
        assert msg.value is not None, "JSON message should be decoded"
        assert msg.value["order_id"] == "ORD-1"
        assert msg.value["amount"] == 101

    def test_json_get_message_decode(self, kafka_client, seed_json_topic):
        """get_message decodes plain JSON value; schema_id is None."""
        info = kafka_client.describe_topic(seed_json_topic)
        p0 = info.partitions[0]
        msg = kafka_client.get_message(seed_json_topic, p0.id, p0.earliest)
        assert msg.value is not None, "JSON get_message should decode"
        assert isinstance(msg.value, dict)
        # JSON messages have no Confluent framing → schema_id is None
        assert msg.schema_id is None


class TestThreeFormatsAllDecode:
    """Cross-format summary assertion."""

    def test_three_formats_all_decode(self, kafka_client, seed_json_topic, seed_avro_topic, seed_protobuf_topic):
        """At least one message from each format decodes successfully."""
        for topic_name, key_prefix in [
            (seed_json_topic, "key-"),
            (seed_avro_topic, "avro-key-"),
            (seed_protobuf_topic, "proto-key-"),
        ]:
            results = kafka_client.search_messages(f"{key_prefix}0", topics=[topic_name])
            assert len(results) >= 1, f"No messages found in {topic_name}"
            assert results[0].value is not None, f"Decode failed for {topic_name}"
