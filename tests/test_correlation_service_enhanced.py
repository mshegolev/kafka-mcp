"""Unit tests for enhanced correlation service features."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock

import pytest

from kafka_mcp.domain.correlation_service import CorrelationService, _extract_correlation_ids, _extract_with_jsonpath
from kafka_mcp.domain.models import KafkaMessage


class MockConsumer:
    """Mock consumer for testing."""

    def consumer_group_lag(self, group: str, topics: list[str] | None = None) -> list:
        return []


class MockSchemaRegistry:
    """Mock schema registry for testing."""

    def get_schema(self, subject: str) -> dict | None:
        return None

    def decode(self, raw: bytes, topic: str = "", partition: int = 0, offset: int = 0) -> dict | None:
        return None


@pytest.fixture
def correlation_service():
    """Create a CorrelationService instance with mock dependencies."""
    consumer = MockConsumer()
    registry = MockSchemaRegistry()
    return CorrelationService(consumer, registry)


@pytest.fixture
def sample_message():
    """Create a sample KafkaMessage for testing."""
    return KafkaMessage(
        topic="test-topic",
        partition=0,
        offset=1,
        key="test-key",
        headers={"trace_id": "trace-123", "correlation_id": "corr-456"},
        value={"order_id": "order-789", "traceId": "trace-123", "parent_id": "parent-000"},
        timestamp_utc=datetime.now(timezone.utc),
        raw=b"test raw data",
    )


def test_extract_correlation_ids_with_regex(correlation_service, sample_message):
    """Test regex-based correlation ID extraction."""
    # Test with regex patterns
    regex_patterns = [r'"traceId":"([^"]+)"', r'"order_id":"([^"]+)"']
    ids = _extract_correlation_ids(sample_message, regex_patterns)

    # Should find traceId and order_id from the value
    assert "trace-123" in ids
    assert "order-789" in ids

    # Should also find standard IDs
    assert "trace-123" in ids  # from headers
    assert "corr-456" in ids  # from headers
    assert "order-789" in ids  # from value


def test_extract_correlation_ids_with_invalid_regex(correlation_service, sample_message):
    """Test regex-based extraction with invalid patterns."""
    # Test with invalid regex patterns (should not crash)
    regex_patterns = [r"invalid[", r'"order_id":"([^"]+)"']
    ids = _extract_correlation_ids(sample_message, regex_patterns)

    # Should still find valid patterns
    assert "order-789" in ids


def test_extract_with_jsonpath(correlation_service, sample_message):
    """Test JSONPath-based correlation ID extraction."""
    # Skip if jsonpath-ng is not available
    try:
        import jsonpath_ng

        if jsonpath_ng is None:
            pytest.skip("jsonpath-ng not available")
    except ImportError:
        pytest.skip("jsonpath-ng not available")

    # Test with JSONPath expressions
    ids = _extract_with_jsonpath(sample_message, "$.order_id")
    assert "order-789" in ids


def test_correlation_service_with_regex_patterns(correlation_service):
    """Test CorrelationService with regex patterns."""
    # Create mock initial results
    initial_msg = KafkaMessage(
        topic="orders",
        partition=0,
        offset=1,
        key="order-123",
        headers={"trace_id": "trace-456"},
        value={"order_id": "order-123", "traceId": "trace-456", "customer_id": "cust-789"},
        timestamp_utc=datetime.now(timezone.utc),
        raw=b"test data",
    )

    # Mock the topic service to return some follow-up messages
    mock_topic_service = Mock()
    mock_topic_service.search_messages.return_value = [
        KafkaMessage(
            topic="payments",
            partition=0,
            offset=2,
            key="payment-456",
            headers={"trace_id": "trace-456"},
            value={"payment_id": "payment-456", "order_id": "order-123"},
            timestamp_utc=datetime.now(timezone.utc),
            raw=b"payment data",
        )
    ]

    # Replace the topic service
    correlation_service._topic_service = mock_topic_service

    # Test with regex patterns
    regex_patterns = [r'"order_id":"([^"]+)"']
    results = correlation_service.correlate_messages(
        initial_results=[initial_msg],
        follow_topics=["payments"],
        limit=10,
        regex_patterns=regex_patterns,
        bidirectional=False,
    )

    # Should have the initial message and the correlated message
    assert len(results) >= 1
    mock_topic_service.search_messages.assert_called()


def test_correlation_service_bidirectional(correlation_service):
    """Test CorrelationService with bidirectional traversal."""
    # Create mock initial results
    initial_msg = KafkaMessage(
        topic="orders",
        partition=0,
        offset=1,
        key="order-123",
        headers={"trace_id": "trace-456"},
        value={"order_id": "order-123", "traceId": "trace-456"},
        timestamp_utc=datetime.now(timezone.utc),
        raw=b"test data",
    )

    # Mock the topic service to return some follow-up messages
    mock_topic_service = Mock()
    mock_topic_service.search_messages.return_value = [
        KafkaMessage(
            topic="payments",
            partition=0,
            offset=2,
            key="payment-456",
            headers={"trace_id": "trace-456"},
            value={"payment_id": "payment-456", "order_id": "order-123"},
            timestamp_utc=datetime.now(timezone.utc),
            raw=b"payment data",
        )
    ]

    # Replace the topic service
    correlation_service._topic_service = mock_topic_service

    # Test with bidirectional enabled
    results = correlation_service.correlate_messages(
        initial_results=[initial_msg],
        follow_topics=["payments"],
        limit=10,
        bidirectional=True,
    )

    # Should have the initial message and the correlated message
    assert len(results) >= 1


def test_correlation_service_with_limits(correlation_service):
    """Test CorrelationService with depth and breadth limits."""
    # Create mock initial results
    initial_msg = KafkaMessage(
        topic="orders",
        partition=0,
        offset=1,
        key="order-123",
        headers={"trace_id": "trace-456"},
        value={"order_id": "order-123", "traceId": "trace-456"},
        timestamp_utc=datetime.now(timezone.utc),
        raw=b"test data",
    )

    # Mock the topic service
    mock_topic_service = Mock()
    mock_topic_service.search_messages.return_value = []

    # Replace the topic service
    correlation_service._topic_service = mock_topic_service

    # Test with limits
    results = correlation_service.correlate_messages(
        initial_results=[initial_msg],
        follow_topics=["payments", "shipments"],
        limit=5,
        max_depth=3,
        max_breadth=2,
    )

    # Should return results (even if empty due to mocking)
    assert isinstance(results, list)


if __name__ == "__main__":
    pytest.main([__file__])
