# correlation-controls Specification

## Purpose
TBD - created by archiving change enhance-correlation-engine. Update Purpose after archive.
## Requirements
### Requirement: System supports configurable correlation depth limits
The system SHALL allow users to configure maximum correlation depth to prevent infinite loops and excessive resource consumption during correlation operations.

#### Scenario: Depth limit prevents excessive correlation traversal
- **WHEN** a correlation operation is configured with a maximum depth of 3 and the correlation chain would exceed this depth
- **THEN** the system stops traversal and returns the correlation chain up to the maximum depth

### Requirement: System supports configurable correlation breadth limits
The system SHALL allow users to configure maximum number of correlation branches to follow at each level to control resource consumption.

#### Scenario: Breadth limit controls fan-out during correlation
- **WHEN** a correlation operation encounters a message that correlates to 10 other messages but breadth is limited to 5
- **THEN** the system follows only the first 5 correlation paths and ignores the rest

