# Phase 6: Real-Broker E2E Contour - Context

**Gathered:** 2026-06-16
**Status:** Ready for planning
**Mode:** Auto-generated (infrastructure phase — discuss skipped)

<domain>
## Phase Boundary

An automated integration suite running against a real Kafka broker and Schema Registry
(testcontainers) verifies all tools — including the v1.1 surfaces (key decode, schema_id,
lag) — with real-wire round-trips and real schema-encoded decode.

In scope: testcontainers fixtures (Kafka + Schema Registry), test-only producer for seeding,
pytest -m integration marker, real-wire round-trip tests for list_topics, describe_topic,
search_messages, get_message, consumer_group_lag; real decode tests for Avro, Protobuf,
JSON via live Schema Registry; key decode and schema_id tests against live broker.

Out of scope: performance testing, load testing, production deployment testing, auth/TLS
testing against live brokers.
</domain>

<decisions>
## Implementation Decisions

### OpenCode's Discretion
All implementation choices are at OpenCode's discretion — pure infrastructure phase.
Use ROADMAP phase goal, success criteria, and codebase conventions to guide decisions.

Key constraints from success criteria:
- pytest -m integration boots real Kafka + Schema Registry via testcontainers
- pytest (no -m integration) skips all integration tests (unit suite stays hermetic)
- Real-wire round-trips for all existing tools + v1.1 surfaces
- At least one Avro, one Protobuf, one JSON schema-encoded round-trip
- Read-only guarantee maintained (assign-only, no offset commits)
</decisions>

<code_context>
## Existing Code Insights

Codebase context will be gathered during plan-phase research.
</code_context>

<specifics>
## Specific Ideas

- Use testcontainers-python for Kafka + Schema Registry containers
- Avro/Protobuf/JSON schemas registered via confluent_kafka.schema_registry
- Test producer seeds messages with known payloads for assertion
- conftest.py fixtures shared across integration test modules
- Mark all integration tests with @pytest.mark.integration
</specifics>

<deferred>
## Deferred Ideas

None — discuss phase skipped.
</deferred>
