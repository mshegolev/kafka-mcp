## ADDED Requirements

### Requirement: System supports backward correlation traversal
The system SHALL allow users to traverse correlation chains in both forward and backward directions, enabling investigation of both causes and effects of events.

#### Scenario: Backward traversal finds root cause of error
- **WHEN** an error event is found in a topic and backward traversal is enabled
- **THEN** the system follows correlation IDs to previous topics to identify the root cause event

### Requirement: System marks traversal direction in correlation chain
The system SHALL indicate the direction of traversal (forward/backward) for each hop in the correlation chain to provide context to investigators.

#### Scenario: Correlation chain shows traversal direction
- **WHEN** a correlation operation involves both forward and backward traversal
- **THEN** each entry in the correlation_chain field includes a direction indicator (forward/backward) along with the field name and value