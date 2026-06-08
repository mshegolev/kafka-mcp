# Retrospective: kafka-mcp

A living retrospective across milestones. Newest milestone sections are added
above the Cross-Milestone Trends section.

## Milestone: v1.0 — MVP (Read-only Kafka MCP brick)

**Shipped:** 2026-06-08
**Phases:** 3 | **Plans:** 12 | **Tests:** 205 passing | **LOC:** ~3.4k src / ~4.1k tests

### What Was Built

A read-only Kafka MCP brick on a hexagonal architecture. `list_topics`,
`describe_topic`, `search_messages` (by business key within a time window), and
`get_message` are exposed identically across four inbound faces (lib
`KafkaClient`, MCP stdio, FastAPI `/tools/*`, `kafka-mcp` CLI). Message bodies are
decoded from Avro / Protobuf / JSON via Schema Registry, and every returned
message carries the Investigator-Contract Evidence fields. Read-only is enforced
structurally (assign-only, `enable.auto.commit=false`, throwaway consumer group).
KAFKA-07 (native scanner) was closed by a benchmark proving the scan is
I/O-bound, so Rust was correctly not added; a try-import seam keeps the option
open. Distribution artifacts (glama.json, server.json, MIT LICENSE, CHANGELOG,
GitHub Actions CI with tag-gated publish) are prepared but not live-published.

### What Worked

- **Smart-discuss grey-area tables** front-loaded the consequential decisions
  (config shape, read-only mechanism, decode behavior, Rust gate, publish
  posture) so executors had unambiguous, locked context.
- **Strict hexagonal boundary, enforced by a test** (`grep` for I/O imports in
  domain/ports) kept the layering honest across 12 plans and three phases.
- **The adversarial code-review → fix → re-review loop earned its cost.** It
  caught real, test-invisible defects: a Protobuf decoder that crashed at
  construction, wrong DecodeError coordinates, a CLI face that booted the HTTP
  server instead of dispatching subcommands, and a system-`protoc` portability
  trap (fixed by switching to pip-installable `grpc_tools.protoc`).
- **Benchmark-gating an optimization** turned "should we add Rust?" into a
  measured, auditable decision (EVALUATION.md) instead of a guess.

### What Was Inefficient

- **Dependency drift surfaced late.** New decode deps (authlib, fastavro,
  grpcio-tools) were added to pyproject mid-phase but not installed in the test
  interpreter, causing a collection error; a protobuf constraint (`>=7.35.0`) was
  outright unsatisfiable with grpcio-tools and only caught by a pre-commit lock
  hook. Declaring + installing deps in the same step would have avoided rework.
- **SUMMARY `requirements_completed` frontmatter went unpopulated**, degrading the
  milestone audit's 3-source cross-reference to a 2-source check (VERIFICATION +
  traceability still gave full coverage).
- **A transient API 500 mid-executor** required manual disk/git spot-checking to
  confirm the work had actually committed before resuming.

### Patterns Established

- Single-plan-per-wave phases run sequentially on the main tree (worktree
  isolation adds merge overhead with no parallelism benefit there).
- End-of-phase: code-review → `--fix --auto` loop (cap 3), then close any
  residual high-value warnings by hand before verification.
- "Prepare-don't-live-publish" posture for outward-facing release steps in an
  autonomous run: author the artifacts + CI, gate the actual publish on a human
  tagged release.

### Key Lessons

- Tests passing ≠ correct: mock-based suites hid the Protobuf-construction crash
  and the CLI dispatch gap. Pair unit tests with at least one real, non-mock path
  per risky integration (the real-wire Protobuf round-trip test was decisive).
- Declare a dependency and install/lock it in the same change, or CI will be the
  first place the conflict is found.
- Gate optimizations on measurement; "premature Rust" was avoided by a 30-minute
  benchmark.

### Cost Observations

- Model mix: orchestration on Opus; planners/executors/fixers on Sonnet;
  verifiers/checkers/integration on Haiku.
- Sessions: 1 autonomous run spanning 2026-06-05 → 2026-06-08.
- Notable: the review/fix loops were the largest token sink and also the highest
  defect-yield activity of the milestone.

## Cross-Milestone Trends

| Milestone | Phases | Plans | Tests | Notable |
|-----------|--------|-------|-------|---------|
| v1.0 MVP | 3 | 12 | 205 | First ship; review loop caught 3 critical defects |
