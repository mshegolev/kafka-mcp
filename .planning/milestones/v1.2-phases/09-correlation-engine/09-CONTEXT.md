# Phase 9 Context: Correlation Engine

## Objective
Implement a correlation engine that extracts correlated entity IDs from search results and follows those IDs into additional topics to build cross-service event chains.

## Key Decisions

### 1. Correlation Depth Control
**Decision**: Implement single-hop correlation only for v1.2
**Rationale**: 
- COR-04 (Recursive correlation depth control) is deferred to a future milestone
- Out-of-scope section explicitly states recursive multi-hop correlation is deferred
- V1.2 will support single-hop follow from initial results only
- This prevents unbounded fan-out while delivering core correlation functionality

### 2. ID Extraction Approach
**Decision**: Extract IDs from both message values and headers
**Rationale**:
- COR-01 requires scanning message values and headers for configurable ID field patterns
- Need to support common correlation patterns like `trace_id`, `order_id`, `parent_id`
- Will implement configurable field extraction to support various ID formats

### 3. Topic Specification
**Decision**: Require explicit topic lists, no auto-discovery
**Rationale**:
- Out-of-scope section excludes auto-discovery of all topics for correlation
- Prevents unbounded fan-out and maintains predictable behavior
- Follow_topics parameter will specify which topics to search in for correlations

### 4. Correlation Chain Tracking
**Decision**: Include correlation_chain field in output
**Rationale**:
- COR-03 requires correlation_chain field linking each message to the ID path that discovered it
- Enables investigators to reconstruct why each message was included
- Maintains Investigator-Contract Evidence shape compliance

### 5. Integration with Existing Functionality
**Decision**: Build on Phase 8 multi-topic search + header filtering
**Rationale**:
- COR-02 states correlation output should reuse multi-topic search internally
- No duplicated scan logic - correlation layer is additive on top of search domain
- Leverages existing header filtering and multi-topic capabilities

## Implementation Approach

### Core Components
1. **CorrelationService**: New domain service implementing correlation logic
2. **IDExtractor**: Component to extract correlated IDs from messages
3. **CorrelationEngine**: Orchestrates the correlation workflow
4. **EvidenceMapper**: Maps correlated messages to Investigator-Contract Evidence format

### Key Methods
1. `correlate_messages(initial_results, follow_topics)` - Main entry point
2. `extract_correlation_ids(message)` - Extract IDs from message value/headers
3. `build_correlation_chain(original_id, followed_id, field_name)` - Track correlation paths
4. `map_to_evidence(messages, correlation_chains)` - Convert to Evidence format

## Technical Constraints
1. Maintain backward compatibility with existing search functionality
2. Reuse existing multi-topic search and header filtering internally
3. Return results sorted by timestamp_utc across all topics
4. Include source="kafka", event_type="correlated_message" in output
5. Handle edge cases gracefully (missing IDs, circular references, etc.)

## Success Criteria Mapping
- COR-01: Extract correlated IDs from search results by scanning message values and headers
- COR-02: Follow extracted IDs into additional topics to build cross-service event chains
- COR-03: Output conforms to Investigator-Contract Evidence shape with correlation_chain field

## Next Steps
1. Plan the implementation details
2. Design the domain service and supporting components
3. Implement the correlation engine functionality
4. Add comprehensive test coverage
5. Integrate with existing MCP/FastAPI/CLI faces