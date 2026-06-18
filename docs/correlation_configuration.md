# Correlation Engine Configuration

This document describes the configuration options available for the enhanced correlation engine.

## Correlation Pattern Configuration

The `CorrelationPatternConfig` class defines patterns used for extracting correlation IDs from messages.

### Regex Patterns

Regular expressions can be used to extract IDs from message content:

```python
patterns = CorrelationPatternConfig(
    regex_patterns=[
        r'"traceId":"([^"]+)"',           # Extract traceId from JSON
        r'"correlation_id":"([^"]+)"',    # Extract correlation_id from JSON
        r'order_id=(\w+)',                # Extract order_id from key=value format
    ]
)
```

### JSONPath Expressions

JSONPath expressions can extract IDs from structured JSON data (requires jsonpath-ng):

```python
patterns = CorrelationPatternConfig(
    jsonpath_expressions=[
        "$.headers.trace_id",             # Extract from headers
        "$.payload.order_id",             # Extract from nested payload
        "$.metadata.correlation_id",      # Extract from metadata
    ]
)
```

## Correlation Limits Configuration

The `CorrelationLimitsConfig` class defines limits to prevent excessive resource consumption.

### Max Depth

Limit the number of correlation hops:

```python
limits = CorrelationLimitsConfig(
    max_depth=5,          # Maximum 5 hops
    timeout_seconds=30    # 30 second timeout
)
```

### Max Breadth

Limit the number of branches at each correlation level:

```python
limits = CorrelationLimitsConfig(
    max_breadth=10,       # Maximum 10 branches per level
    timeout_seconds=60    # 60 second timeout
)
```

## Correlation Traversal Configuration

The `CorrelationTraversalConfig` class defines how correlation traversal behaves.

### Bidirectional Traversal

Enable backward correlation to find root causes:

```python
traversal = CorrelationTraversalConfig(
    bidirectional=True,        # Enable backward traversal
    follow_causality=True,     # Follow causal relationships
    exclude_internal_topics=True  # Exclude internal Kafka topics
)
```

## Complete Configuration Example

A complete correlation configuration combining all options:

```python
from kafka_mcp.domain.correlation_config import (
    CorrelationConfig,
    CorrelationPatternConfig,
    CorrelationLimitsConfig,
    CorrelationTraversalConfig
)

config = CorrelationConfig(
    patterns=CorrelationPatternConfig(
        regex_patterns=[
            r'"traceId":"([^"]+)"',
            r'"correlation_id":"([^"]+)"'
        ],
        jsonpath_expressions=[
            "$.headers.trace_id",
            "$.payload.order_id"
        ]
    ),
    limits=CorrelationLimitsConfig(
        max_depth=5,
        max_breadth=10,
        timeout_seconds=60
    ),
    traversal=CorrelationTraversalConfig(
        bidirectional=True,
        follow_causality=True,
        exclude_internal_topics=True
    )
)
```

## CLI Configuration Options

The CLI supports all correlation configuration options as command-line arguments:

```bash
# Pattern matching options
--regex-patterns '"traceId":"([^"]+)"',"correlation_id":"([^"]+)"'
--jsonpath-expressions '$.headers.trace_id,$.payload.order_id'

# Limit options
--max-depth 5
--max-breadth 10

# Traversal options
--bidirectional
```

## Environment Variables

Some correlation behaviors can be configured via environment variables:

```bash
# Default correlation depth limit
export KAFKA_MCP_DEFAULT_MAX_DEPTH=3

# Default correlation breadth limit
export KAFKA_MCP_DEFAULT_MAX_BREADTH=5

# Default correlation timeout
export KAFKA_MCP_CORRELATION_TIMEOUT_SECONDS=30
```