"""Integration tests for correlation functionality."""

from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock

from kafka_mcp.adapters.inbound.cli import run_correlate_messages
from kafka_mcp.adapters.inbound.lib import KafkaClient
from kafka_mcp.domain.models import KafkaMessage


class TestCorrelationIntegration:
    """Test the integration of correlation functionality."""

    def test_run_correlate_messages_with_json_output(self):
        """Test that run_correlate_messages produces JSON output correctly."""
        from datetime import datetime, timezone

        # Create mock messages
        initial_msg = KafkaMessage(
            topic="orders",
            partition=0,
            offset=1,
            key="order-123",
            headers={"trace_id": "trace-abc"},
            value={"order_id": "order-123"},
            timestamp_utc=datetime(2026, 6, 8, 0, 0, 0, tzinfo=timezone.utc),
            raw=b"test-payload-1",
        )

        correlated_msg = KafkaMessage(
            topic="payments",
            partition=0,
            offset=2,
            key="payment-456",
            headers={"trace_id": "trace-abc"},
            value={"payment_id": "payment-456"},
            timestamp_utc=datetime(2026, 6, 8, 0, 1, 0, tzinfo=timezone.utc),
            raw=b"test-payload-2",
        )

        # Mock the KafkaClient methods
        with patch("kafka_mcp.adapters.inbound.cli.KafkaClient.from_env") as mock_from_env:
            mock_client = MagicMock()
            mock_from_env.return_value = mock_client

            # Mock search_messages to return our initial message
            mock_client.search_messages.return_value = [initial_msg]

            # Mock correlate_messages to return both messages
            mock_client.correlate_messages.return_value = [initial_msg, correlated_msg]

            # Capture print output
            with patch("builtins.print") as mock_print:
                run_correlate_messages(client=mock_client, key="order-123", follow_topics="payments", as_json=True)

                # Verify the methods were called
                mock_client.search_messages.assert_called_once()
                mock_client.correlate_messages.assert_called_once()

                # Verify print was called with JSON output
                mock_print.assert_called_once()
                printed_output = mock_print.call_args[0][0]
                assert "trace-abc" in printed_output
                assert "order-123" in printed_output
                assert "payment-456" in printed_output
