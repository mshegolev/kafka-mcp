# Roadmap: kafka-mcp

## Overview

Three phases deliver a read-only Kafka MCP brick that copies the hexagonal v2
skeleton from graphql-mcp, adds confluent-kafka and Schema Registry adapters,
implements search + decode, then benchmarks and optionally ships a Rust scanner
before wiring all inbound faces and publishing to Glama. Every requirement maps
to exactly one phase; phases build strictly on top of each other.

---

## Phases

- [x] **Phase 1: Foundation** - Hexagonal skeleton + broker/SR adapters + topic inspection tools (completed 2026-06-05)
- [ ] **Phase 2: Search + Decode** - search_messages, get_message, Avro/Protobuf/JSON decode
- [ ] **Phase 3: Native + Ship** - Benchmark-gated Rust scanner, all inbound faces, CI wheels, publish

---

## Phase Details

### Phase 1: Foundation

**Goal**: The hexagonal v2 skeleton is in place with read-only broker and Schema
Registry adapters; `list_topics` and `describe_topic` work via the lib facade,
MCP stdio, FastAPI REST, and CLI; read-only guarantee is structurally enforced.
**Mode:** mvp
**Depends on**: Nothing (first phase)
**Requirements**: KAFKA-01, KAFKA-04, KAFKA-06
**Success Criteria** (what must be TRUE):

  1. `from kafka_mcp import KafkaClient; c.list_topics()` returns topic names
     in a pytest without touching MCP or FastAPI (lib-first proof).

  2. `c.describe_topic(name)` returns partition count and earliest/latest
     offsets for each partition.

  3. The `domain/` layer imports zero I/O libraries or frameworks; all broker
     I/O goes through the `ConsumerPort` protocol (hexagonal boundary holds).

  4. A temporary consumer-group is created for every scan and no offsets are
     committed to production groups; read-only annotation is present on all
     MCP tool definitions.

  5. `list_topics` and `describe_topic` are reachable via MCP stdio, FastAPI
     REST (`/tools/list_topics`), and `kafka-mcp list-topics` CLI.
**Plans**: 4 plans
Plans:
**Wave 1**

- [x] 01-01-PLAN.md — Scaffold + domain contracts (models, errors, ports, config)

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 01-02-PLAN.md — Outbound adapters (ConfluentConsumerAdapter, SchemaRegistryHttpAdapter stub)

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 01-03-PLAN.md — TopicService domain service + KafkaClient lib facade + end-to-end lib pytest

**Wave 4** *(blocked on Wave 3 completion)*

- [x] 01-04-PLAN.md — MCP stdio + FastAPI REST + CLI inbound adapters + server.py entry point

### Phase 2: Search + Decode

**Goal**: Investigators can find events by key within a time window and retrieve
decoded message bodies; all wire formats (Avro, Protobuf, JSON) are decoded via
Schema Registry before the payload is returned.
**Mode:** mvp
**Depends on**: Phase 1
**Requirements**: KAFKA-02, KAFKA-03, KAFKA-05
**Success Criteria** (what must be TRUE):

  1. `c.search_messages(key="some-msisdn", time_from=..., time_to=..., limit=100)`
     returns a list of `KafkaMessage` objects each containing `timestamp_utc`,
     `key`, `headers`, `value` (decoded dict), and `raw`.

  2. `c.get_message(topic, partition, offset)` returns a single `KafkaMessage`
     with a decoded `value` for all three wire formats (Avro, Protobuf, JSON).

  3. When Schema Registry returns an Avro or Protobuf schema, the message value
     is decoded to a plain dict; plain JSON falls back to `json.loads`; an
     unknown format raises a typed domain error (not an unhandled exception).

  4. Evidence fields required by the Investigator Contract are present on every
     returned message: `source="kafka"`, `event_type="kafka_message"`,
     `timestamp_utc`, `keys{order_id, msisdn, customer_id, product_id}`.
**Plans**: 5 plans
Plans:
**Wave 1**

- [ ] 02-01-PLAN.md — Domain contracts: KafkaMessage model, DecodeError, MessageNotFoundError, extended ConsumerPort + SchemaRegistryPort

**Wave 2** *(blocked on Wave 1 completion — runs in parallel)*

- [ ] 02-02-PLAN.md — Decode adapter: real SchemaRegistryHttpAdapter with Confluent framing + Avro/Protobuf/JSON decode
- [ ] 02-03-PLAN.md — Consumer scan adapter: ConfluentConsumerAdapter.fetch_messages + fetch_message + offsets_for_times

**Wave 3** *(blocked on Wave 2 completion)*

- [ ] 02-04-PLAN.md — Domain service + KafkaClient: search_messages, get_message, Evidence extraction, lib-layer vertical slice

**Wave 4** *(blocked on Wave 3 completion)*

- [ ] 02-05-PLAN.md — Inbound faces: wire search_messages + get_message into MCP stdio, FastAPI REST, CLI

### Phase 3: Native + Ship

**Goal**: A pure-Python partition scanner is benchmarked; Rust/pyo3 scanner is
added only if the benchmark proves CPU-bound speedup; all four inbound faces
(lib, stdio, FastAPI, CLI) are complete; CI publishes multi-platform wheels;
the brick is published to Glama.
**Mode:** mvp
**Depends on**: Phase 2
**Requirements**: KAFKA-07
**Success Criteria** (what must be TRUE):

  1. `pytest-benchmark` run on the pure-Python scanner produces a documented
     baseline in `EVALUATION.md`; Rust scanner is present and active only if
     benchmark output shows CPU-bound speedup (decision recorded in PROJECT.md).

  2. When Rust wheel is absent, the pure-Python fallback is selected
     automatically and all tests pass without a Rust toolchain.

  3. CI matrix (cibuildwheel) produces wheels for Linux manylinux x86_64/
     aarch64, macOS arm64/x86_64, and Windows AMD64 for Python 3.10–3.12;
     sdist is also published.

  4. All four inbound faces deliver the same Investigator Contract operations:
     `KafkaClient` lib import, `kafka-mcp` stdio, FastAPI `/tools/*`, and
     `kafka-mcp` CLI subcommands.

  5. `glama.json`, `server.json`, `EVALUATION.md`, `CHANGELOG.md`, `LICENSE`
     (MIT) are present and pass Glama validation; `server.json` declares the
     stdio PyPI package.
**Plans**: TBD

---

## Progress

| Phase | Plans Complete | Status      | Completed |
|-------|----------------|-------------|-----------|
| 1. Foundation | 4/4 | Complete    | 2026-06-05 |
| 2. Search + Decode | 0/5 | Not started | -    |
| 3. Native + Ship | 0/? | Not started | -      |
