# Phase 3: Native + Ship - Context

**Gathered:** 2026-06-08
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 3 closes out v1: it benchmarks the pure-Python partition scanner, makes a
benchmark-gated decision about a Rust/pyo3 accelerator (KAFKA-07), confirms all
four inbound faces are complete, wires multi-platform CI wheel builds, and
prepares (but does NOT live-publish) the Glama / PyPI distribution artifacts.

In scope:
- A `pytest-benchmark` baseline of the pure-Python scan/decode hot loop, written
  up in `EVALUATION.md`.
- A benchmark-gated decision on Rust: add a `native/` pyo3 scanner ONLY if the
  benchmark proves the hot path is CPU-bound AND a prototype gives ≥2× speedup;
  otherwise pure-Python remains the permanent path. Decision recorded in
  PROJECT.md and EVALUATION.md.
- A scanner seam with automatic pure-Python fallback (`try import _native`).
- Packaging + CI: a `py3-none-any` wheel + sdist for the pure-Python case, with
  a cibuildwheel matrix wired but dormant unless a native extension exists;
  publish gated on a release tag / manual dispatch.
- Distribution artifacts: `glama.json`, `server.json` (declares the stdio PyPI
  package), `EVALUATION.md`, `CHANGELOG.md`, `LICENSE` (MIT) — present and
  schema-valid against Glama.

Out of scope (explicit): performing a LIVE PyPI upload or LIVE Glama submission
during this autonomous run (outward-facing, credentialed, requires explicit
go-ahead — CI does the real publish on a tagged release). Produce / consumer-
group mgmt / schema writes remain out of scope for all of v1.

Requirements: KAFKA-07 (benchmark-gated Rust scanner).

</domain>

<decisions>
## Implementation Decisions

### Benchmark Methodology & the Rust Gate (KAFKA-07)
- Benchmark the pure-Python partition-scan + decode-dispatch hot loop with
  `pytest-benchmark` over a synthetic in-memory message batch (no live broker);
  record the baseline in `EVALUATION.md`.
- Rust is added ONLY if the benchmark shows the hot path is CPU-bound AND a Rust
  prototype yields a meaningful speedup (≥2× on the hot path). Otherwise the
  pure-Python scanner stays. The decision (either way) is recorded in PROJECT.md
  and EVALUATION.md.
- Anticipated outcome: the scan is network/IO-bound, so Rust is NOT added (this
  is the "premature Rust" pitfall from the brief). The benchmark + EVALUATION.md
  must still be produced to PROVE the decision rather than assert it.
- Introduce a `scanner` seam so a Rust implementation could drop in later without
  an API change, while shipping the pure-Python implementation active.

### Native/Rust Scaffolding (conditional on the gate)
- Do NOT add a `native/` Rust crate unless the benchmark justifies it (honors
  KAFKA-07: "Rust ONLY after profiling proves CPU-bound"). Keep only the
  fallback-capable seam.
- Fallback selection: `try: import kafka_mcp._native except ImportError: <pure
  Python>` — the whole test suite passes with no Rust toolchain (SC-2).
- If Rust is ever added, the build path is maturin + pyo3 with abi3 wheels —
  documented in EVALUATION.md.
- The benchmark-gated decision is recorded in BOTH PROJECT.md (Decisions) and
  EVALUATION.md.

### CI & Multi-Platform Wheels (SC-3)
- CI platform is GitHub Actions (cibuildwheel and Glama are GitHub-oriented).
- For the pure-Python outcome: build a single `py3-none-any` wheel + sdist. The
  full cibuildwheel matrix (Linux manylinux x86_64/aarch64, macOS arm64/x86_64,
  Windows AMD64, Python 3.10–3.12) is wired in the workflow but only engaged if a
  native extension is present.
- Publishing is gated on a release tag / manual `workflow_dispatch` — never on
  every push to main.
- Supported Python floor is 3.10–3.12 (matches the fixed stack).

### Glama / PyPI Artifacts & Publish Posture
- POSTURE: prepare artifacts + CI only. Do NOT perform a live PyPI upload or
  Glama submission in this autonomous run — those are outward-facing, need
  credentials, and require explicit human go-ahead. CI performs the real publish
  on a tagged release.
- Required files (SC-5): `glama.json`, `server.json` (declares the stdio PyPI
  package `kafka-mcp`), `EVALUATION.md`, `CHANGELOG.md`, `LICENSE` (MIT) — all
  present and valid against the Glama schema (validate locally).
- `server.json` declares the stdio transport plus the PyPI package name.
- LICENSE is MIT, authored in this phase (per SC-5).

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- Phases 1–2 delivered the full hexagonal brick: domain (models incl.
  KafkaMessage, errors, TopicService with list_topics/describe_topic/
  search_messages/get_message), ports, outbound adapters (assign-based read-only
  ConfluentConsumerAdapter, real SchemaRegistryHttpAdapter with Avro/Protobuf/
  JSON decode via in-process grpc_tools), and all four inbound faces (lib, MCP
  stdio, FastAPI POST /tools/*, CLI subcommands). 190 tests pass, ruff clean.
- pyproject.toml uses the hatchling build backend with full runtime deps already
  declared (mcp, confluent-kafka, fastapi, uvicorn, pydantic, orjson, authlib,
  cachetools, fastavro, protobuf, googleapis-common-protos, grpcio-tools).
- The `kafka-mcp` console-script entry point (server.main) already dispatches
  --stdio / CLI subcommands / HTTP. SC-4 (all four faces) is essentially already
  satisfied by Phase 1–2 — Phase 3 confirms/regression-checks it.

### Established Patterns
- Hexagonal boundary (tested), read-only guarantee (assign-only), typed domain
  errors, pydantic v2 models, mock-based tests (no live broker), per-wave
  sequential execution on the main tree, end-of-phase code-review + fix loop.
- The scanner hot loop currently lives in ConfluentConsumerAdapter.fetch_messages
  (the candidate for the benchmark + the Rust seam).

### Integration Points
- The benchmark targets the pure-Python scan/decode loop. The scanner seam wraps
  whatever fetch_messages does today behind a swappable implementation.
- Packaging: hatchling for pure-Python; if Rust is ever added, maturin takes over
  the build (documented, not switched now).

</code_context>

<specifics>
## Specific Ideas

- Umbrella decision D9: Rust via pyo3/maturin with pure-Python(orjson) fallback;
  Rust ONLY after benchmark proves a CPU-bound win — KAFKA-07 is explicitly gated
  on the benchmark.
- The brief's build order ends: pure-Python scan → bench → (maybe) Rust → CI
  wheels → Glama. Honor that order.
- EVALUATION.md is the artifact that makes the Rust decision auditable.

</specifics>

<deferred>
## Deferred Ideas

- Live PyPI upload and live Glama submission — deferred to a human-triggered
  tagged release; out of scope for this autonomous run.
- An actual Rust crate — deferred unless/until a benchmark proves a CPU-bound
  speedup (expected: not in v1).
- HTTP transport declaration in server.json — stdio only for v1.

</deferred>
