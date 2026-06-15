"""Basic connectivity integration tests — list_topics and describe_topic.

Validates real-wire round-trips against a testcontainers Kafka broker.
All tests use session-scoped fixtures from conftest.py (containers start
once per session).
"""

from __future__ import annotations

import pytest

from kafka_mcp.domain.errors import TopicNotFoundError
from kafka_mcp.domain.models import TopicInfo

pytestmark = pytest.mark.integration


class TestListTopics:
    """list_topics against a real broker with a seeded topic."""

    def test_list_topics_returns_seeded_topic(self, kafka_client, seed_json_topic):
        """Seeded 'test-json' topic appears in the topic list."""
        topics = kafka_client.list_topics()
        assert isinstance(topics, list)
        assert seed_json_topic in topics

    def test_list_topics_excludes_internal(self, kafka_client, seed_json_topic):
        """Internal topics (prefixed with __) are excluded by default."""
        topics = kafka_client.list_topics(include_internal=False)
        assert all(not t.startswith("__") for t in topics)

    def test_list_topics_includes_internal_when_requested(self, kafka_client, seed_json_topic):
        """include_internal=True exposes __consumer_offsets."""
        topics = kafka_client.list_topics(include_internal=True)
        # CP-Kafka creates __consumer_offsets by default
        assert any(t.startswith("__") for t in topics)


class TestDescribeTopic:
    """describe_topic against a real broker."""

    def test_describe_topic_returns_real_partition_info(self, kafka_client, seed_json_topic):
        """describe_topic returns TopicInfo with real partition metadata."""
        info = kafka_client.describe_topic(seed_json_topic)
        assert isinstance(info, TopicInfo)
        assert info.name == seed_json_topic
        assert info.partition_count >= 1
        assert len(info.partitions) >= 1
        # Seeded topic has messages → latest offset > 0
        assert info.partitions[0].latest > 0

    def test_describe_topic_unknown_raises(self, kafka_client):
        """describe_topic raises TopicNotFoundError for a nonexistent topic."""
        with pytest.raises(TopicNotFoundError):
            kafka_client.describe_topic("nonexistent-topic-xyz-99")
