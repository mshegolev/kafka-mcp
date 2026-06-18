# Proposal: Add OpenAPI Specification for Kafka MCP

## Why
The Kafka MCP project currently lacks a formal API specification that documents the REST endpoints, request/response formats, and data models. This makes it difficult for developers to understand the API contract and for tools to generate client code or documentation.

Without a formal specification:
- Developers must reverse-engineer the API from code or trial-and-error
- Client code generation tools cannot automatically create SDKs
- API contract changes are harder to track and validate
- Documentation becomes outdated as the API evolves

## What Changes
Add an OpenAPI (Swagger) specification that describes all the REST endpoints exposed by the Kafka MCP service, including:
- `/tools/list_topics` - List Kafka topics
- `/tools/describe_topic` - Describe a Kafka topic
- `/tools/search_messages` - Search Kafka messages
- `/tools/get_message` - Get a specific Kafka message
- `/tools/consumer_group_lag` - Get consumer group lag information
- `/tools/correlate_messages` - Correlate messages across topics

## Benefits
1. **Developer Experience**: Clear API documentation for consumers
2. **Tool Integration**: Enable code generation, testing, and validation tools
3. **Contract Clarity**: Explicitly define API contracts and data models
4. **Maintenance**: Easier to track API changes and ensure backward compatibility

## Implementation Plan
1. Create OpenAPI 3.0 specification file describing all endpoints
2. Document request/response schemas for each endpoint
3. Define reusable components for common data models
4. Validate the specification against the actual implementation
5. Integrate specification validation into the CI/CD pipeline

## Success Criteria
- [ ] OpenAPI specification file created and validated
- [ ] All REST endpoints documented with accurate schemas
- [ ] Specification integrated into development workflow
- [ ] CI/CD pipeline validates specification consistency