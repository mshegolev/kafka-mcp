# Correlation Engine Examples

This document provides examples of how to use the enhanced correlation engine features.

## 1. Basic Correlation

Basic correlation follows IDs from initial search results into additional topics:

```python
from kafka_mcp import KafkaClient

client = KafkaClient.from_env()

# Initial search
initial_results = client.search_messages("order-123", topics=["orders"])

# Correlate with follow topics
correlated = client.correlate_messages(
    initial_results=initial_results,
    follow_topics=["payments", "shipments"]
)
```

## 2. Regex Pattern Matching

Extract correlation IDs using regular expressions:

```python
# Extract trace IDs using regex patterns
correlated = client.correlate_messages(
    initial_results=initial_results,
    follow_topics=["payments", "shipments"],
    regex_patterns=[
        r'"traceId":"([^"]+)"',      # Extract traceId from JSON
        r'"correlation_id":"([^"]+)"' # Extract correlation_id from JSON
    ]
)
```

## 3. Bidirectional Correlation

Enable backward traversal to find root causes:

```python
# Enable bidirectional correlation to find causes and effects
correlated = client.correlate_messages(
    initial_results=initial_results,
    follow_topics=["orders", "payments", "shipments"],
    bidirectional=True
)

# Check correlation direction in results
for msg in correlated:
    for detail in msg.correlation_details:
        print(f"Direction: {detail['direction']}")
        print(f"ID: {detail['id']}")
```

## 4. Correlation Limits

Control resource consumption with depth and breadth limits:

```python
# Limit correlation depth and breadth
correlated = client.correlate_messages(
    initial_results=initial_results,
    follow_topics=["orders", "payments", "shipments", "notifications"],
    max_depth=3,      # Maximum 3 hops
    max_breadth=5     # Maximum 5 branches per level
)
```

## 5. JSONPath Extraction

Extract IDs using JSONPath expressions (requires jsonpath-ng):

```python
# Extract IDs using JSONPath
correlated = client.correlate_messages(
    initial_results=initial_results,
    follow_topics=["payments", "shipments"],
    jsonpath_expressions=[
        "$.headers.trace_id",    # Extract trace_id from headers
        "$.payload.order_id",    # Extract order_id from nested payload
        "$.metadata.parent_id"   # Extract parent_id from metadata
    ]
)
```

## 6. CLI Usage Examples

Use the CLI with enhanced correlation options:

```bash
# Basic correlation
kafka-mcp correlate-messages --key order-123 \
  --follow-topics payments,shipments

# With regex patterns
kafka-mcp correlate-messages --key error-456 \
  --follow-topics orders,payments,shipments \
  --regex-patterns '"traceId":"([^"]+)"'

# Bidirectional correlation
kafka-mcp correlate-messages --key order-123 \
  --follow-topics orders,payments,shipments \
  --bidirectional

# With limits
kafka-mcp correlate-messages --key order-123 \
  --follow-topics orders,payments,shipments \
  --max-depth 5 --max-breadth 10
```

## 7. Configuration Objects

Use configuration objects for complex setups:

```python
from kafka_mcp.domain.correlation_config import (
    CorrelationConfig, 
    CorrelationPatternConfig,
    CorrelationLimitsConfig,
    CorrelationTraversalConfig
)

# Create a comprehensive correlation configuration
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
        follow_causality=True
    )
)

# Use configuration in correlation (custom implementation would be needed)
```

## 8. Working with Correlation Results

Process correlation results with enhanced metadata:

```python
# Process correlation results
for msg in correlated:
    print(f"Message: {msg.topic}[{msg.partition}]@{msg.offset}")
    print(f"Timestamp: {msg.timestamp_utc}")
    
    # Show correlation chain
    print(f"Chain: {' -> '.join(msg.correlation_chain)}")
    
    # Show detailed correlation information
    for detail in msg.correlation_details:
        print(f"  ID: {detail['id']}")
        print(f"  Direction: {detail['direction']}")
        print(f"  Method: {detail['extraction_method']}")
        print(f"  Timestamp: {detail['timestamp']}")
```