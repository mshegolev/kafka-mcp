## Why

The current correlation engine in kafka-mcp allows investigators to trace entities across multiple Kafka topics, but it has limitations in terms of flexibility and depth of analysis. Investigators often need more sophisticated correlation patterns, better control over correlation traversal, and enhanced visualization of correlation chains. Enhancing the correlation engine will make cross-topic investigations more powerful and insightful.

## What Changes

- **Enhanced Correlation Patterns**: Support for more sophisticated correlation patterns beyond simple ID matching, including regex-based extractions and structured data parsing
- **Bidirectional Correlation**: Ability to follow correlations both forward and backward in time/service chains
- **Correlation Depth Control**: Configurable limits on correlation depth to prevent infinite loops and excessive resource consumption
- **Correlation Visualization**: Enhanced output format that better represents correlation chains and relationships
- **Performance Optimizations**: Improved caching and parallel processing for correlation operations
- **BREAKING**: Updated `correlate_messages` API to support new parameters and return formats

## Capabilities

### New Capabilities
- `advanced-correlation`: Support for sophisticated correlation patterns including regex extraction and structured data parsing
- `bidirectional-traversal`: Ability to follow correlations in both forward and backward directions
- `correlation-controls`: Configuration options for correlation depth, breadth, and resource limits

### Modified Capabilities
- `correlate-messages`: Enhanced API with new parameters for pattern matching, traversal direction, and depth controls

## Impact

- Changes to the `KafkaClient.correlate_messages` method signature and behavior
- Updates to the correlation engine implementation in the domain layer
- Potential performance improvements for correlation operations
- Enhanced output format for correlation results
- Possible additions to MCP, REST, and CLI interfaces to expose new parameters