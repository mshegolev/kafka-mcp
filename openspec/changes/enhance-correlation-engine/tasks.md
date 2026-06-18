## 1. Core Correlation Service Enhancements

- [x] 1.1 Extend CorrelationService to support regex-based pattern matching for ID extraction
- [x] 1.2 Add structured data parsing support (JSONPath) for ID extraction
- [x] 1.3 Implement bidirectional correlation traversal (backward search capability)
- [x] 1.4 Add correlation depth and breadth limit parameters to CorrelationService
- [x] 1.5 Update correlation chain data structure to include traversal direction and pattern information

## 2. API Interface Updates

- [x] 2.1 Update correlate_messages method signature in lib.py to accept new parameters
- [x] 2.2 Update correlate_messages method signature in mcp_stdio.py to accept new parameters
- [x] 2.3 Update correlate_messages method signature in rest_api.py to accept new parameters
- [x] 2.4 Update correlate_messages method signature in cli.py to accept new parameters
- [x] 2.5 Maintain backward compatibility with existing parameter sets

## 3. Domain Model Updates

- [x] 3.1 Extend KafkaMessage model to support enhanced correlation chain information
- [x] 3.2 Add new correlation pattern matching configuration models
- [x] 3.3 Update correlation chain data structure to include direction and pattern metadata

## 4. Testing

- [x] 4.1 Add unit tests for regex-based pattern matching in CorrelationService
- [x] 4.2 Add unit tests for structured data parsing in CorrelationService
- [x] 4.3 Add unit tests for bidirectional correlation traversal
- [x] 4.4 Add unit tests for correlation depth and breadth limits
- [x] 4.5 Add integration tests for new correlate_messages parameters
- [x] 4.6 Verify backward compatibility with existing correlation functionality

## 5. Documentation

- [x] 5.1 Update API documentation for correlate_messages with new parameters
- [x] 5.2 Add examples for advanced correlation patterns
- [x] 5.3 Document bidirectional correlation usage
- [x] 5.4 Document correlation limit configuration