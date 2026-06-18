# Specifications: Kafka MCP OpenAPI Specification

## Overview
This document defines the OpenAPI 3.0 specification for the Kafka MCP REST API. The specification includes all endpoints, request/response schemas, and data models used by the service.

## Specification File
The complete OpenAPI specification is available in `openspec/specs/kafka-mcp-api.json`.

## Endpoints

### 1. List Topics
- **Path**: `/tools/list_topics`
- **Method**: POST
- **Description**: Return a sorted list of Kafka topic names available on the broker
- **Request**: Optional `include_internal` boolean parameter
- **Response**: Array of topic names

### 2. Describe Topic
- **Path**: `/tools/describe_topic`
- **Method**: POST
- **Description**: Return partition metadata and watermark offsets for a single Kafka topic
- **Request**: Required `topic` string parameter
- **Response**: Topic information including partition metadata

### 3. Search Messages
- **Path**: `/tools/search_messages`
- **Method**: POST
- **Description**: Search Kafka messages by key within an optional time window
- **Request**: Required `key`, optional `key_field`, `topics`, `headers`, `time_from`, `time_to`, `limit`
- **Response**: Array of Kafka messages with decoded values

### 4. Get Message
- **Path**: `/tools/get_message`
- **Method**: POST
- **Description**: Fetch and decode a single Kafka message by topic, partition, and offset
- **Request**: Required `topic`, `partition`, `offset` parameters
- **Response**: Single Kafka message with decoded value

### 5. Consumer Group Lag
- **Path**: `/tools/consumer_group_lag`
- **Method**: POST
- **Description**: Report per-partition consumer lag for a given consumer group
- **Request**: Required `group`, optional `topics` parameter
- **Response**: Array of lag records per partition

### 6. Correlate Messages
- **Path**: `/tools/correlate_messages`
- **Method**: POST
- **Description**: Correlate messages by following extracted IDs into additional topics
- **Request**: Required `initial_results`, `follow_topics`, optional `limit`
- **Response**: Array of correlated messages with correlation chains

## Data Models

### KafkaMessage
Represents a decoded Kafka message with evidence fields:
- `topic`: string - Topic name
- `partition`: integer - Partition index
- `offset`: integer - Message offset
- `key`: string|null - Message key
- `value`: object - Decoded message value
- `headers`: object - Message headers
- `timestamp_utc`: string - ISO8601 timestamp
- `raw`: string - Base64-encoded raw message bytes
- `raw_key`: string|null - Base64-encoded raw key bytes
- `schema_id`: integer|null - Schema Registry ID
- `source`: string - Source identifier ("kafka")
- `event_type`: string - Event type ("message")
- `keys`: object - Evidence keys
- `correlation_chain`: array - Correlation ID chain (for correlated messages)

### TopicInfo
Metadata about a Kafka topic:
- `name`: string - Topic name
- `partition_count`: integer - Number of partitions
- `partitions`: array - Partition information

### PartitionInfo
Information about a topic partition:
- `id`: integer - Partition ID
- `leader`: integer - Leader broker ID
- `replicas`: array - Replica broker IDs
- `isrs`: array - In-sync replica broker IDs
- `earliest_offset`: integer - Earliest message offset
- `latest_offset`: integer - Latest message offset

### LagRecord
Consumer group lag information:
- `group`: string - Consumer group name
- `topic`: string - Topic name
- `partition`: integer - Partition ID
- `current_offset`: integer - Current committed offset
- `end_offset`: integer - Latest message offset
- `lag`: integer - Offset difference
- `timestamp_utc`: string - ISO8601 timestamp

## Validation
The specification has been validated against the OpenAPI 3.0 schema and tested with the actual implementation.