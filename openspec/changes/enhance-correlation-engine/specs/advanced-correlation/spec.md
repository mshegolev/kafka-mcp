## ADDED Requirements

### Requirement: System supports regex-based correlation pattern matching
The system SHALL allow users to define correlation patterns using regular expressions to extract identifiers from message payloads and headers.

#### Scenario: Regex pattern extracts trace ID from JSON payload
- **WHEN** a message contains a JSON payload with a "traceId" field and the correlation pattern is set to extract values matching `"traceId":"([^"]+)"`
- **THEN** the system extracts the trace ID value and uses it for correlation traversal

### Requirement: System supports structured data parsing for correlation
The system SHALL allow users to define correlation patterns using structured data paths (e.g., JSONPath, XPath) to extract identifiers from message payloads.

#### Scenario: JSONPath extracts user ID from nested JSON structure
- **WHEN** a message contains a JSON payload with nested structure {"user": {"id": "12345"}} and the correlation pattern is set to extract values using JSONPath $.user.id
- **THEN** the system extracts the user ID value "12345" and uses it for correlation traversal