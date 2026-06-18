## Context

The current correlation engine in kafka-mcp provides basic cross-topic correlation capabilities but lacks advanced features that investigators need for complex troubleshooting scenarios. The existing implementation only supports simple ID extraction and forward-only traversal, which limits its effectiveness for deep root cause analysis.

This design enhances the correlation engine to support more sophisticated correlation patterns, bidirectional traversal, and configurable limits to prevent resource exhaustion.

## Goals / Non-Goals

**Goals:**
- Support regex-based and structured data pattern matching for correlation ID extraction
- Enable bidirectional correlation traversal (both forward and backward in time)
- Add configurable limits for correlation depth and breadth to prevent resource exhaustion
- Maintain backward compatibility with existing correlate_messages API where possible
- Enhance correlation chain information to include traversal direction and pattern details

**Non-Goals:**
- Completely redesign the correlation engine architecture
- Add visualization capabilities to the core library (this remains a presentation layer concern)
- Support for external correlation sources outside of Kafka topics
- Real-time correlation streaming (batch processing only)

## Decisions

### Decision: Pattern Matching Approach
**Chosen Approach**: Extend the existing ID extraction mechanism to support regex patterns and structured data paths (JSONPath, etc.)

**Rationale**: This approach builds on the existing implementation and provides flexibility for users to define custom extraction patterns without requiring major architectural changes.

**Alternatives Considered**:
1. External pattern engine - Would require additional dependencies and complexity
2. Simple wildcard matching - Would be insufficient for complex extraction scenarios

### Decision: Bidirectional Traversal Implementation
**Chosen Approach**: Modify the correlation algorithm to support backward traversal by searching for references to the initial correlation IDs in message fields

**Rationale**: This approach leverages existing search capabilities and maintains consistency with the forward traversal implementation.

**Alternatives Considered**:
1. Separate backward correlation service - Would increase complexity and duplication
2. Graph-based traversal engine - Would require significant architectural changes

### Decision: Resource Limit Controls
**Chosen Approach**: Add depth and breadth limits as parameters to the correlation API with sensible defaults

**Rationale**: Parameter-based controls provide flexibility for users while preventing runaway correlation operations that could impact system performance.

**Alternatives Considered**:
1. Global configuration only - Would be less flexible for different use cases
2. Time-based limits only - Would be harder to predict and control

## Risks / Trade-offs

**[Performance Risk]** → Implementing pattern matching could slow down correlation operations
**Mitigation**: Use compiled regex patterns and efficient parsing libraries; add performance benchmarks

**[Complexity Risk]** → Bidirectional traversal increases algorithmic complexity
**Mitigation**: Thorough testing with edge cases; clear documentation of behavior

**[Backward Compatibility Risk]** → API changes may break existing integrations
**Mitigation**: Maintain backward compatible defaults; provide clear migration documentation