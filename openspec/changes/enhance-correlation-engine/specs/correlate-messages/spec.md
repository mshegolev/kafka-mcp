## MODIFIED Requirements

### Requirement: Correlate messages across topics with enhanced parameters
The system SHALL allow users to correlate messages across multiple Kafka topics with enhanced parameters including pattern matching, traversal direction, and depth controls.

#### Scenario: Correlate messages with regex pattern matching
- **WHEN** a user calls correlate_messages with a regex pattern for ID extraction
- **THEN** the system uses the regex pattern to extract correlation IDs from message payloads and headers

#### Scenario: Correlate messages with bidirectional traversal
- **WHEN** a user calls correlate_messages with backward traversal enabled
- **THEN** the system follows correlation IDs both forward and backward in time to build a complete event chain

#### Scenario: Correlate messages with depth limits
- **WHEN** a user calls correlate_messages with a maximum depth parameter set to 3
- **THEN** the system stops correlation traversal after 3 hops and returns the correlation chain up to that depth

### Requirement: Correlate messages output includes enhanced correlation chain information
The system SHALL include enhanced correlation chain information in the output that indicates traversal direction and pattern matching details.

#### Scenario: Correlation output includes traversal direction
- **WHEN** a correlation operation involves both forward and backward traversal
- **THEN** each entry in the correlation_chain field includes a direction indicator (forward/backward) along with the field name and value

#### Scenario: Correlation output includes pattern information
- **WHEN** a correlation operation uses regex or structured data patterns for ID extraction
- **THEN** each entry in the correlation_chain field includes information about which pattern was used to extract the ID