# Phase 10 Context: 4-Face Symmetry & Integration Tests

## Objective
Ensure all v1.2 capabilities (multi-topic search, header filtering, correlate_messages) are accessible identically through MCP stdio, FastAPI REST, and CLI faces — and the integration test suite covers cross-topic scenarios against a real broker.

## Key Decisions

### 1. Face Symmetry Requirements
**Decision**: All v1.2 capabilities must be exposed identically across all four faces
**Rationale**: 
- SYM-01 requires all new capabilities to be exposed identically across lib KafkaClient, MCP stdio tool, FastAPI /tools/* POST endpoint, and kafka-mcp CLI subcommand
- All faces must return the same JSON schema for consistency
- This maintains the library-first architectural principle

### 2. Integration Test Coverage
**Decision**: Update integration test suite for cross-topic scenarios against real broker
**Rationale**:
- Need to verify functionality works end-to-end with actual Kafka brokers
- Cross-topic search and correlation scenarios must be tested with real data
- Testcontainers-based integration tests provide reliable environment

### 3. Backward Compatibility
**Decision**: Maintain full backward compatibility with existing functionality
**Rationale**:
- Existing 323 tests must continue to pass without modification
- New parameters should have appropriate defaults
- No breaking changes to existing APIs

## Implementation Approach

### Core Components to Verify
1. **MCP stdio face**: Expose multi-topic search (topics param), header filtering (headers param) on search_messages tool, and new correlate_messages tool
2. **FastAPI REST face**: Expose /tools/search_messages accepting topics + headers body fields, and new /tools/correlate_messages endpoint
3. **CLI face**: kafka-mcp search-messages --topics ... --headers ... and kafka-mcp correlate-messages ... commands
4. **Integration Tests**: Cross-topic search and correlation scenarios against testcontainers broker

### Key Verification Points
1. All faces return identical JSON schemas for equivalent inputs
2. New parameters work consistently across all faces
3. Existing functionality remains unaffected
4. Integration tests cover realistic usage scenarios

## Technical Constraints
1. Maintain consistency with existing codebase architecture and patterns
2. Reuse existing MCP, REST, and CLI frameworks
3. Ensure proper error handling and validation
4. Follow established testing patterns and conventions

## Success Criteria Mapping
- SYM-01: All new and extended capabilities exposed identically across all four faces with same schema

## Next Steps
1. Verify MCP face exposes all v1.2 capabilities correctly
2. Verify FastAPI REST face exposes all v1.2 capabilities correctly
3. Verify CLI face exposes all v1.2 capabilities correctly
4. Update integration test suite with cross-topic scenarios
5. Run full test suite to ensure no regressions