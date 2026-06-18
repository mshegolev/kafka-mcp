"""CorrelationService — domain service for correlating Kafka messages across topics.

Implements the correlation engine functionality as specified in Phase 9 CONTEXT.md:
- COR-01: Extract correlated IDs from search results
- COR-02: Follow extracted IDs into additional topics
- COR-03: Output conforms to Investigator-Contract Evidence shape with correlation_chain

Pure domain service: imports only ports and domain types.
No broker library, HTTP library, or web framework imports are present.
"""

from __future__ import annotations

from typing import Any

from kafka_mcp.domain.models import KafkaMessage
from kafka_mcp.domain.search_service import TopicService
from kafka_mcp.ports.consumer import ConsumerPort
from kafka_mcp.ports.schema_registry import SchemaRegistryPort


def _extract_correlation_ids(msg: KafkaMessage) -> set[str]:
    """Extract correlation IDs from message value and headers.

    Looks for common correlation field names in both message value and headers.
    Returns a set of unique correlation IDs found.
    """
    correlation_fields = {
        "trace_id",
        "traceId",
        "trace-id",
        "correlation_id",
        "correlationId",
        "correlation-id",
        "request_id",
        "requestId",
        "request-id",
        "transaction_id",
        "transactionId",
        "transaction-id",
        "span_id",
        "spanId",
        "span-id",
        "parent_id",
        "parentId",
        "parent-id",
        "order_id",
        "orderId",
        "order-id",
        "customer_id",
        "customerId",
        "customer-id",
        "session_id",
        "sessionId",
        "session-id",
    }

    ids: set[str] = set()

    # Extract from value
    if msg.value is not None and isinstance(msg.value, dict):
        for field in correlation_fields:
            val = msg.value.get(field)
            if val is not None:
                ids.add(str(val))

    # Extract from headers
    for field in correlation_fields:
        val = msg.headers.get(field)
        if val is not None:
            ids.add(val)

    # Also check evidence keys for correlation IDs
    for key, val in msg.keys.items():
        if val is not None:
            ids.add(val)

    return ids


class CorrelationService:
    """Domain service that orchestrates message correlation across Kafka topics.

    Builds on the existing TopicService to implement cross-topic correlation
    by extracting IDs from initial search results and following them into
    additional topics.

    All Kafka I/O is delegated to the injected ConsumerPort; all decode
    logic is delegated to the injected SchemaRegistryPort — this class
    contains zero I/O code (hexagonal architecture).
    """

    def __init__(
        self,
        consumer: ConsumerPort,
        registry: SchemaRegistryPort,
    ) -> None:
        """Initialise with a ConsumerPort and a SchemaRegistryPort.

        Args:
            consumer: Any object that satisfies the ConsumerPort Protocol.
                Injected so tests can pass a MockConsumer with no real
                broker.
            registry: Any object that satisfies the SchemaRegistryPort
                Protocol.  Injected so tests can pass a MockSchemaRegistry
                with no real Schema Registry.
        """
        self._topic_service = TopicService(consumer, registry)
        # Define correlation field names for searching
        self._correlation_fields = [
            "trace_id",
            "traceId",
            "trace-id",
            "correlation_id",
            "correlationId",
            "correlation-id",
            "request_id",
            "requestId",
            "request-id",
            "transaction_id",
            "transactionId",
            "transaction-id",
            "span_id",
            "spanId",
            "span-id",
            "parent_id",
            "parentId",
            "parent-id",
            "order_id",
            "orderId",
            "order-id",
            "customer_id",
            "customerId",
            "customer-id",
            "session_id",
            "sessionId",
            "session-id",
        ]

    def correlate_messages(
        self,
        initial_results: list[KafkaMessage],
        follow_topics: list[str],
        limit: int = 500,
    ) -> list[KafkaMessage]:
        """Correlate messages by following extracted IDs into additional topics.

        Implements the core correlation algorithm:
        1. Extract correlation IDs from initial search results
        2. For each ID, search follow_topics for matching messages
        3. Build correlation_chain linking each message to the path that discovered it
        4. Return combined results sorted by timestamp_utc

        Args:
            initial_results: Initial search results to extract correlation IDs from.
            follow_topics: List of topic names to search for correlated messages.
            limit: Maximum number of total correlated messages to return.

        Returns:
            List of KafkaMessage objects with correlation_chain populated,
            sorted by timestamp_utc.
        """
        if not initial_results or limit <= 0:
            return []

        # If no follow topics, return initial results with empty chains
        if not follow_topics:
            for msg in initial_results:
                msg.correlation_chain = []
            return initial_results[:limit]

        # Step 1: Extract correlation IDs from initial results
        correlation_ids = self._extract_all_correlation_ids(initial_results)

        # Step 2: Follow IDs into additional topics
        correlated_messages: list[KafkaMessage] = []

        # Add initial results with empty correlation chains
        for msg in initial_results:
            msg.correlation_chain = []
            correlated_messages.append(msg)

        # Track how many messages we've collected
        collected_count = len(correlated_messages)

        # If no correlation IDs found, return initial results
        if not correlation_ids:
            correlated_messages.sort(key=lambda msg: msg.timestamp_utc)
            return correlated_messages[:limit]

        # For each correlation ID, search in follow_topics using various field names
        for corr_id in correlation_ids:
            if collected_count >= limit:
                break

            # Search for this ID in follow_topics using different correlation field names
            id_matches: list[KafkaMessage] = []

            # Search in message keys
            key_matches = self._topic_service.search_messages(
                key=corr_id,
                key_field="key",
                topics=follow_topics,
                limit=limit - collected_count,
            )
            id_matches.extend(key_matches)

            if len(id_matches) < (limit - collected_count):
                # Search in headers using each correlation field name
                for field_name in self._correlation_fields:
                    if len(id_matches) >= (limit - collected_count):
                        break

                    header_matches = self._topic_service.search_messages(
                        key=corr_id,
                        key_field=f"header:{field_name}",
                        topics=follow_topics,
                        limit=limit - collected_count - len(id_matches),
                    )
                    id_matches.extend(header_matches)

            if len(id_matches) < (limit - collected_count):
                # Search in values using each correlation field name
                for field_name in self._correlation_fields:
                    if len(id_matches) >= (limit - collected_count):
                        break

                    value_matches = self._topic_service.search_messages(
                        key=corr_id,
                        key_field=f"value:{field_name}",
                        topics=follow_topics,
                        limit=limit - collected_count - len(id_matches),
                    )
                    id_matches.extend(value_matches)

            # Add correlation chain information to each matched message
            # Remove duplicates while preserving order
            seen_offsets = set((msg.topic, msg.partition, msg.offset) for msg in correlated_messages)
            unique_matches = []
            for msg in id_matches:
                msg_offset = (msg.topic, msg.partition, msg.offset)
                if msg_offset not in seen_offsets:
                    unique_matches.append(msg)
                    seen_offsets.add(msg_offset)

            for msg in unique_matches:
                msg.correlation_chain = [corr_id]
                correlated_messages.append(msg)

            collected_count += len(unique_matches)

        # Step 3: Sort all results by timestamp_utc
        correlated_messages.sort(key=lambda msg: msg.timestamp_utc)

        # Step 4: Apply limit
        return correlated_messages[:limit]

    def _extract_all_correlation_ids(self, messages: list[KafkaMessage]) -> set[str]:
        """Extract all unique correlation IDs from a list of messages.

        Args:
            messages: List of KafkaMessage objects to extract IDs from.

        Returns:
            Set of unique correlation IDs found across all messages.
        """
        all_ids: set[str] = set()

        for msg in messages:
            msg_ids = _extract_correlation_ids(msg)
            all_ids.update(msg_ids)

        return all_ids
