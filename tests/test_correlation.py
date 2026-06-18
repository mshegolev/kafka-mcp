"""Test suite for correlation service functionality."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from kafka_mcp.domain.models import KafkaMessage
from kafka_mcp.domain.correlation_service import CorrelationService


class MockConsumer:
    """Mock consumer that returns predefined messages for search queries."""

    def __init__(self, search_results=None):
        self.search_results = search_results or {}
        self.calls = []
        # Pre-populate topics from search results
        self._topics = set()
        for msg_list in self.search_results.values():
            for msg in msg_list:
                self._topics.add(msg.topic)

    def list_topics(self, include_internal=False):
        return list(self._topics)

    def get_partition_ids(self, topic):
        return [0]

    def get_watermark_offsets(self, topic, partition):
        return (0, 100)

    def offsets_for_times(self, topic, partition, timestamp_ms):
        return 0

    def fetch_messages(self, topic, partition, start_offset, stop_offset, time_to, limit):
        self.calls.append(("fetch_messages", topic, partition, start_offset, stop_offset, time_to, limit))

        # Find matching messages in our search results
        key = (topic, partition)
        if key in self.search_results:
            # Apply offset-based slicing (simplified)
            msgs = self.search_results[key]
            # In a real implementation, we'd filter by offset range
            # For testing, we'll just return up to the limit
            return msgs[:limit]
        return []

    def fetch_message(self, topic, partition, offset):
        raise NotImplementedError

    def consumer_group_lag(self, group, topics=None):
        return []


class MockSchemaRegistry:
    """Mock schema registry that returns predefined decode results."""

    def __init__(self, decode_results=None):
        self.decode_results = decode_results or {}

    def get_schema(self, subject):
        return None

    def decode(self, raw, topic="", partition=0, offset=0):
        key = (topic, partition, offset)
        return self.decode_results.get(key)


class TestCorrelationService:
    """Test the CorrelationService class."""

    _BASE_TS = datetime(2026, 6, 8, 0, 0, 0, tzinfo=timezone.utc)

    def _make_message(self, topic="test-topic", partition=0, offset=0, key=None, headers=None, value=None, **kwargs):
        """Create a test KafkaMessage with default values."""
        return KafkaMessage(
            topic=topic,
            partition=partition,
            offset=offset,
            key=key,
            headers=headers or {},
            value=value,
            timestamp_utc=self._BASE_TS,
            raw=b"test-payload",
            **kwargs,
        )

    def test_extract_correlation_ids_from_headers(self):
        """Test that correlation IDs are extracted from message headers."""
        from kafka_mcp.domain.search_service import _extract_correlation_ids

        msg = self._make_message(headers={"trace_id": "trace-123", "correlation_id": "corr-456"})

        ids = _extract_correlation_ids(msg)
        assert "trace-123" in ids
        assert "corr-456" in ids

    def test_extract_correlation_ids_from_value(self):
        """Test that correlation IDs are extracted from message values."""
        from kafka_mcp.domain.search_service import _extract_correlation_ids

        msg = self._make_message(value={"trace_id": "trace-123", "request_id": "req-789"})

        ids = _extract_correlation_ids(msg)
        assert "trace-123" in ids
        assert "req-789" in ids

    def test_extract_correlation_ids_from_evidence_keys(self):
        """Test that correlation IDs are extracted from evidence keys."""
        from kafka_mcp.domain.search_service import _extract_correlation_ids

        msg = self._make_message(keys={"order_id": "ORD-123", "customer_id": "CUST-456"})

        ids = _extract_correlation_ids(msg)
        assert "ORD-123" in ids
        assert "CUST-456" in ids

    def test_correlate_messages_empty_input(self):
        """Test that correlate_messages handles empty inputs correctly."""
        consumer = MockConsumer()
        registry = MockSchemaRegistry()
        service = CorrelationService(consumer, registry)

        # Empty initial results
        result = service.correlate_messages([], ["topic-a"], 100)
        assert result == []

        # No follow topics
        msg = self._make_message()
        result = service.correlate_messages([msg], [], 100)
        assert len(result) == 1
        assert result[0].correlation_chain == []

    def test_correlate_messages_no_correlation_ids(self):
        """Test that messages without correlation IDs are returned with empty chains."""
        consumer = MockConsumer()
        registry = MockSchemaRegistry()
        service = CorrelationService(consumer, registry)

        msg = self._make_message(value={"unrelated_field": "value"})
        result = service.correlate_messages([msg], ["topic-a"], 100)

        assert len(result) == 1
        assert result[0].correlation_chain == []

    def test_correlate_messages_with_correlation(self):
        """Test the full correlation flow with matching messages."""
        # Initial message with correlation ID
        initial_msg = self._make_message(
            topic="orders",
            offset=1,
            key="order-123",
            headers={"trace_id": "trace-abc"},
            value={"order_id": "order-123"},
        )

        # Correlated message that should be found
        correlated_msg = self._make_message(
            topic="payments",
            offset=2,
            key="payment-456",
            headers={"trace_id": "trace-abc"},
            value={"payment_id": "payment-456"},
        )

        # Set up mock consumer to return the correlated message when searching for trace-abc
        consumer = MockConsumer({("payments", 0): [correlated_msg]})
        registry = MockSchemaRegistry()
        service = CorrelationService(consumer, registry)

        result = service.correlate_messages([initial_msg], ["payments"], 100)

        # Should have both initial and correlated messages
        assert len(result) == 2

        # Find which is which
        initial_result = next(m for m in result if m.topic == "orders")
        correlated_result = next(m for m in result if m.topic == "payments")

        # Initial message should have empty correlation chain
        assert initial_result.correlation_chain == []

        # Correlated message should have the correlation ID in its chain
        assert "trace-abc" in correlated_result.correlation_chain

    def test_correlate_messages_limit_enforcement(self):
        """Test that the limit parameter is properly enforced."""
        # Create initial message
        initial_msg = self._make_message(headers={"trace_id": "trace-1"})

        # Create multiple correlated messages
        correlated_messages = [
            self._make_message(topic="logs", offset=i, headers={"trace_id": "trace-1"}, value={"log_id": f"log-{i}"})
            for i in range(10)
        ]

        consumer = MockConsumer({("logs", 0): correlated_messages})
        registry = MockSchemaRegistry()
        service = CorrelationService(consumer, registry)

        # Limit to 5 total messages
        result = service.correlate_messages([initial_msg], ["logs"], 5)

        # Should have exactly 5 messages (1 initial + 4 correlated)
        assert len(result) == 5
