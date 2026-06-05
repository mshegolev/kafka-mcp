# Walking Skeleton — kafka-mcp

**Phase:** 1
**Generated:** 2026-06-05

## Capability Proven End-to-End

An investigator calls `KafkaClient.from_env().list_topics()` and
`KafkaClient.from_env().describe_topic(name)` in a pytest without MCP or
FastAPI; the same operations are reachable via MCP stdio, `POST /tools/list_topics`
FastAPI, and the `kafka-mcp list-topics` CLI.

## Architectural Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Framework (inbound HTTP) | FastAPI + uvicorn | Umbrella D5: async-capable, matches sibling bricks |
| Broker client | confluent-kafka >= 2.14 (librdkafka) | Umbrella D5: NOT kafka-python; librdkafka is production-grade, supports SASL |
| MCP protocol | mcp >= 1.27 (stdio transport) | MCP Investigation Suite standard; stdio is the only supported transport in v1 |
| Data validation | pydantic v2 + BaseSettings | Umbrella D5: pydantic v2 models in domain; BaseSettings for env-driven config with KAFKA_MCP_ prefix |
| Serialization | orjson | Umbrella D5: fast JSON; used in outbound HTTP responses and CLI --json flag |
| Hexagonal boundary | domain/ imports zero I/O or framework libs | Umbrella D7: ConsumerPort + SchemaRegistryPort protocols in ports/ isolate domain from adapters |
| Read-only enforcement | assign-based Consumer (never subscribe()), enable.auto.commit=false, throwaway group kafka-mcp-ro-{uuid4} | KAFKA-06: structural guarantee — assign cannot commit offsets to production groups |
| Config | pydantic BaseSettings, env prefix KAFKA_MCP_, fail-fast ConfigError on missing keys | D-01/D-04 from 01-CONTEXT.md |
| Directory layout | src/kafka_mcp/{domain,ports,adapters/{inbound,outbound},config.py,server.py} | PROJECT.md hexagonal v2 layout |
| Build backend | hatchling >= 1.24 | Consistent with sibling bricks (jaeger-mcp, kibana-mcp) |
| Python target | 3.10 – 3.12 | Umbrella D5 |
| CLI entry point | kafka-mcp = "kafka_mcp.server:main" (argparse subcommands) | D-15 from 01-CONTEXT.md: table output + --json flag |
| FastAPI route convention | POST /tools/{tool_name} (MCP-mirrored, not REST resource routes) | D-16 from 01-CONTEXT.md |
| Rust extension | Deferred — Phase 3, benchmark-gated | Umbrella D9: premature Rust is an anti-pattern |
| Schema Registry | SchemaRegistryPort protocol wired but stubbed in Phase 1 | Full Avro/Protobuf/JSON decode is KAFKA-05 (Phase 2) |

## Stack Touched in Phase 1

- [x] Project scaffold (pyproject.toml, hatchling, ruff, pytest, src layout)
- [x] Routing — FastAPI POST /tools/list_topics and POST /tools/describe_topic
- [x] Broker read — confluent_consumer.py calls list_topics() and get_watermark_offsets() against real broker
- [x] MCP stdio — mcp_stdio.py registers list_topics and describe_topic tools with readOnlyHint:true
- [x] CLI — kafka-mcp list-topics / describe-topic subcommands, table + --json output
- [x] Lib facade — KafkaClient.from_env() as the primary programmatic entry point
- [x] Deployment — documented local-run via pytest (lib-only) and uvicorn (REST); broker via env vars

## Out of Scope (Deferred to Later Slices)

- search_messages, get_message (Phase 2 — KAFKA-02, KAFKA-03)
- Message-body decode: Avro, Protobuf, JSON via Schema Registry (Phase 2 — KAFKA-05)
- Rust/pyo3 partition scanner (Phase 3 — KAFKA-07, benchmark-gated)
- CI wheels via cibuildwheel (Phase 3)
- Glama publish, glama.json, server.json (Phase 3)
- mTLS for broker or Schema Registry (deferred per CONTEXT.md)
- AdminClient-based metadata APIs (watermark offsets suffice for v1)

## Subsequent Slice Plan

- Phase 2: Investigator can search events by key in a time window and retrieve
  decoded message bodies (Avro/Protobuf/JSON via Schema Registry).
- Phase 3: Benchmark-gated Rust scanner; CI multi-platform wheel publish;
  Glama submission.
