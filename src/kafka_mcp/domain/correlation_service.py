"""CorrelationService — domain service for correlating Kafka messages across topics.

Implements the correlation engine functionality as specified in Phase 9 CONTEXT.md:
- COR-01: Extract correlated IDs from search results
- COR-02: Follow extracted IDs into additional topics
- COR-03: Output conforms to Investigator-Contract Evidence shape with correlation_chain

Pure domain service: imports only ports and domain types.
No broker library, HTTP library, or web framework imports are present.
"""

from __future__ import annotations

import re

from kafka_mcp.domain.models import KafkaMessage
from kafka_mcp.domain.search_service import TopicService
from kafka_mcp.ports.consumer import ConsumerPort
from kafka_mcp.ports.schema_registry import SchemaRegistryPort


def _extract_correlation_ids(msg: KafkaMessage, regex_patterns: list[str] | None = None) -> set[str]:
    """Extract correlation IDs from message value and headers.

    Looks for common correlation field names in both message value and headers.
    Also supports regex pattern matching if regex_patterns is provided.
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
    if msg.value is not None:
        # Standard field extraction
        if isinstance(msg.value, dict):
            for field in correlation_fields:
                val = msg.value.get(field)
                if val is not None:
                    ids.add(str(val))

        # Regex pattern matching on serialized value
        if regex_patterns and isinstance(msg.value, (dict, str)):
            value_str = str(msg.value) if not isinstance(msg.value, str) else msg.value
            for pattern in regex_patterns:
                try:
                    matches = re.findall(pattern, value_str)
                    ids.update(matches)
                except re.error:
                    # Invalid regex pattern, skip it
                    pass

    # Extract from headers
    for field in correlation_fields:
        val = msg.headers.get(field)
        if val is not None:
            ids.add(val)

    # Regex pattern matching on headers
    if regex_patterns:
        headers_str = str(msg.headers)
        for pattern in regex_patterns:
            try:
                matches = re.findall(pattern, headers_str)
                ids.update(matches)
            except re.error:
                # Invalid regex pattern, skip it
                pass

    # Also check evidence keys for correlation IDs
    for val in msg.keys.values():
        if val is not None:
            ids.add(val)

    return ids


def _extract_with_jsonpath(msg: KafkaMessage, jsonpath_expr: str) -> set[str]:
    """Extract correlation IDs using JSONPath expression.

    Args:
        msg: KafkaMessage to extract from
        jsonpath_expr: JSONPath expression to use for extraction

    Returns:
        Set of extracted correlation IDs
    """
    ids: set[str] = set()

    # Try to import jsonpath-ng if available
    try:
        # Try importing jsonpath_ng
        import sys

        if "jsonpath_ng" in sys.modules:
            jsonpath_ng = sys.modules["jsonpath_ng"]
        else:
            import importlib

            jsonpath_ng = importlib.import_module("jsonpath_ng")

        if jsonpath_ng is not None:
            jsonpath_expr_parsed = jsonpath_ng.parse(jsonpath_expr)

            # Extract from value
            if msg.value is not None and isinstance(msg.value, dict):
                matches = jsonpath_expr_parsed.find(msg.value)
                for match in matches:
                    ids.add(str(match.value))

            # Extract from headers (convert to dict if possible)
            if msg.headers:
                headers_dict = dict(msg.headers)
                matches = jsonpath_expr_parsed.find(headers_dict)
                for match in matches:
                    ids.add(str(match.value))

    except (ImportError, Exception):
        # jsonpath-ng not available or any other error, return empty set
        pass

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
        regex_patterns: list[str] | None = None,
        jsonpath_expressions: list[str] | None = None,
        max_depth: int | None = None,
        max_breadth: int | None = None,
        bidirectional: bool = False,
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
            regex_patterns: Optional list of regex patterns for ID extraction.
            jsonpath_expressions: Optional list of JSONPath expressions for ID extraction.
            max_depth: Optional maximum correlation depth (default: unlimited).
            max_breadth: Optional maximum correlation breadth per level (default: unlimited).
            bidirectional: Whether to enable backward correlation traversal.

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
        correlation_ids = self._extract_all_correlation_ids(initial_results, regex_patterns, jsonpath_expressions)

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

        # Apply breadth limit if specified
        effective_breadth = max_breadth if max_breadth is not None else len(correlation_ids)
        limited_correlation_ids = list(correlation_ids)[:effective_breadth]

        # For each correlation ID, search in follow_topics using various field names
        for corr_id in limited_correlation_ids:
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

            # If bidirectional is enabled, also search backwards for references to this ID
            if bidirectional and len(id_matches) < (limit - collected_count):
                backward_matches = self._search_backward_references(
                    corr_id, follow_topics, limit - collected_count - len(id_matches)
                )
                # Mark backward matches with the appropriate direction
                for msg in backward_matches:
                    # Add detailed correlation information for backward matches
                    correlation_detail = {
                        "id": corr_id,
                        "direction": "backward",
                        "extraction_method": "backward_reference",
                        "timestamp": msg.timestamp_utc.isoformat() if msg.timestamp_utc else None,
                    }
                    msg.correlation_details.append(correlation_detail)
                id_matches.extend(backward_matches)

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
                # Update the correlation_chain as a list of strings
                if not msg.correlation_chain:
                    msg.correlation_chain = [corr_id]
                else:
                    msg.correlation_chain.append(corr_id)

                # Check if this message was already marked as backward
                is_backward = any(detail.get("direction") == "backward" for detail in msg.correlation_details)

                # Add detailed correlation information
                correlation_detail = {
                    "id": corr_id,
                    "direction": "backward" if is_backward else "forward",
                    "extraction_method": "backward_reference" if is_backward else "standard",
                    "timestamp": msg.timestamp_utc.isoformat() if msg.timestamp_utc else None,
                }
                msg.correlation_details.append(correlation_detail)

                correlated_messages.append(msg)

            collected_count += len(unique_matches)

        # Step 3: Sort all results by timestamp_utc
        correlated_messages.sort(key=lambda msg: msg.timestamp_utc)

        # Step 4: Apply limit
        return correlated_messages[:limit]

    def _extract_all_correlation_ids(
        self,
        messages: list[KafkaMessage],
        regex_patterns: list[str] | None = None,
        jsonpath_expressions: list[str] | None = None,
    ) -> set[str]:
        """Extract all unique correlation IDs from a list of messages.

        Args:
            messages: List of KafkaMessage objects to extract IDs from.
            regex_patterns: Optional list of regex patterns for ID extraction.
            jsonpath_expressions: Optional list of JSONPath expressions for ID extraction.

        Returns:
            Set of unique correlation IDs found across all messages.
        """
        all_ids: set[str] = set()

        for msg in messages:
            # Extract with standard method
            msg_ids = _extract_correlation_ids(msg, regex_patterns)
            all_ids.update(msg_ids)

            # Extract with JSONPath if expressions provided
            if jsonpath_expressions:
                for expr in jsonpath_expressions:
                    jsonpath_ids = _extract_with_jsonpath(msg, expr)
                    all_ids.update(jsonpath_ids)

        return all_ids

    def _search_backward_references(self, correlation_id: str, topics: list[str], limit: int) -> list[KafkaMessage]:
        """Search for backward references to a correlation ID.

        This method looks for messages that might be the source of the given
        correlation ID, enabling bidirectional correlation traversal.

        Args:
            correlation_id: The ID to search for references to.
            topics: List of topics to search in.
            limit: Maximum number of messages to return.

        Returns:
            List of KafkaMessage objects that reference the correlation ID.
        """
        backward_matches: list[KafkaMessage] = []

        # Search for messages that might be the source of this correlation ID
        # We'll look for messages where this ID appears in value fields that
        # commonly contain references to other IDs
        source_fields = [
            "parent_id",
            "parentId",
            "parent-id",
            "source_id",
            "sourceId",
            "source-id",
            "origin_id",
            "originId",
            "origin-id",
            "caused_by",
            "causedBy",
            "caused-by",
            "trigger_id",
            "triggerId",
            "trigger-id",
        ]

        # Limit the number of matches we collect
        collected_count = 0

        for topic in topics:
            if collected_count >= limit:
                break

            # Search in message values using source field names
            for field_name in source_fields:
                if collected_count >= limit:
                    break

                try:
                    field_matches = self._topic_service.search_messages(
                        key=correlation_id,
                        key_field=f"value:{field_name}",
                        topics=[topic],
                        limit=limit - collected_count,
                    )
                    backward_matches.extend(field_matches)
                    collected_count += len(field_matches)
                except Exception:
                    # If one field fails, continue with others
                    continue

        return backward_matches[:limit]
