# Phase 9 Summary: Correlation Engine

## Overview
Successfully implemented the correlation engine that extracts correlated entity IDs from search results and follows those IDs into additional topics to build cross-service event chains. This enables investigators to trace entities across service boundaries and construct comprehensive event timelines.

## Features Delivered

### 1. Core Correlation Functionality
- **ID Extraction**: Extracts correlation IDs from message values, headers, and evidence keys using intelligent field matching
- **Cross-Topic Following**: Follows extracted IDs into specified follow topics using multi-faceted search strategies
- **Evidence-Shaped Output**: Produces output conforming to Investigator-Contract Evidence shape with correlation_chain field

### 2. New Components
- **CorrelationService**: Domain service implementing the core correlation logic with pure business logic
- **Extended KafkaMessage Model**: Added correlation_chain field to track ID discovery paths
- **CLI Command**: New correlate-messages subcommand for command-line usage
- **REST API Endpoint**: New /tools/correlate_messages endpoint for HTTP access
- **MCP Tool**: New correlate_messages tool for MCP clients with streamable-HTTP transport

### 3. Integration Points
- **Built on Existing Infrastructure**: Reuses TopicService and existing search mechanisms for efficiency
- **Multi-Topic Support**: Works seamlessly with multi-topic search capabilities from Phase 8
- **Header Filtering Compatibility**: Integrates with existing header filtering capabilities
- **Limit Enforcement**: Respects global message limits to prevent unbounded result sets

## Implementation Details

### Domain Layer (src/kafka_mcp/domain/correlation_service.py)
- Created CorrelationService domain service with pure business logic
- Implemented _extract_correlation_ids helper function for intelligent ID extraction
- Added correlate_messages method that orchestrates the correlation workflow
- Reused existing TopicService for multi-topic search operations
- Implemented correlation_chain building for tracking ID discovery paths
- Added efficient duplicate detection using message coordinates
- Ensured results are sorted by timestamp_utc across all topics

### Application Layer
- **Library Adapter**: Added correlate_messages method in src/kafka_mcp/adapters/inbound/lib.py
- **CLI**: Added correlate-messages subcommand with comprehensive options in src/kafka_mcp/adapters/inbound/cli.py
- **REST API**: Added /tools/correlate_messages endpoint in src/kafka_mcp/adapters/inbound/rest_api.py
- **MCP**: Registered correlate_messages tool in src/kafka_mcp/server.py with streamable-HTTP transport

### Search Strategy
The correlation engine uses a multi-faceted search approach for comprehensive discovery:
1. Searches in message keys (key_field="key")
2. Searches in common correlation header names (key_field="header:trace_id", etc.)
3. Searches in common correlation value fields (key_field="value:trace_id", etc.)

This ensures comprehensive discovery of correlated messages across different data structures.

## Testing
Added comprehensive test coverage:

### Unit Tests (tests/test_correlation.py)
- Test ID extraction from various message formats and field naming conventions
- Test correlation workflow with mocked data scenarios
- Test edge cases (empty results, no correlation IDs, etc.)
- Test correlation_chain building and sorting
- Test duplicate detection and limit enforcement

### Integration Tests (tests/test_correlation_integration.py)
- Test CLI correlate-messages command with various options
- Test REST API /tools/correlate_messages endpoint
- Test library-level correlate_messages method
- Verify Evidence-shaped output format compliance
- Confirm backward compatibility with existing functionality

## Success Criteria Verification

✅ **COR-01**: KafkaClient.correlate_messages extracts correlated IDs from search results by scanning message values and headers for configurable ID field patterns, then searches for those IDs in follow_topics

✅ **COR-02**: The correlation output is a list of messages sorted by timestamp_utc across all topics (initial + follow), where each message carries source="kafka", event_type="correlated_message", timestamp_utc, and keys fields conforming to the Investigator-Contract Evidence shape

✅ **COR-03**: Each message in the correlation output includes a correlation_chain field that records the ID path that discovered it, so an investigator can reconstruct why each message was included

✅ **Reuse**: correlate_messages reuses the Phase 8 multi-topic search + header filtering internally (no duplicated scan logic); the correlation layer is additive on top of the search domain

## Usage Examples

### CLI Usage
```bash
# Correlate messages starting with an order ID
kafka-mcp correlate-messages --key "ORD-123" --follow-topics "payments,shipments" --json

# Correlate with time window and limit
kafka-mcp correlate-messages --key "REQ-456" --initial-topics "orders" --follow-topics "payments,inventory" --time-from "2026-01-01T00:00:00Z" --limit 100
```

### REST API Usage
```bash
curl -X POST http://localhost:8000/tools/correlate_messages \
  -H "Content-Type: application/json" \
  -d '{
    "initial_results": [...],
    "follow_topics": ["payments", "shipments"],
    "limit": 100
  }'
```

### Programmatic Usage
```python
from kafka_mcp import KafkaClient

client = KafkaClient.from_env()
initial_results = client.search_messages("ORD-123")
correlated_results = client.correlate_messages(
    initial_results=initial_results,
    follow_topics=["payments", "shipments"],
    limit=100
)
```

## Performance Characteristics
- Efficient duplicate detection using message coordinates prevents redundant processing
- Limit enforcement (default 500 messages) prevents unbounded result sets
- Single-hop correlation as specified prevents complex recursive searches
- Reuse of existing optimized search paths leverages Phase 8 performance improvements

## Impact
This implementation enables powerful cross-topic investigation workflows where investigators can:
1. Trace entities across service boundaries using correlation IDs
2. Automatically discover related events in other topics
3. Understand the causal relationships between events through correlation_chain
4. Get chronologically ordered results from multiple topics in a single call
5. Maintain full backward compatibility with existing scripts and tools

## Next Steps
- Phase 10: 4-Face Symmetry & Integration Tests (finalizing v1.2 milestone)
- Potential future enhancements for recursive correlation depth control (COR-04)
- Possible correlation caching mechanisms for repeated queries (COR-05)