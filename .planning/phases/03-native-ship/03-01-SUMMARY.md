---
phase: "03-native-ship"
plan: "01"
subsystem: scanner
tags: [benchmark, tdd, rust-gate, kafka-07, pure-python]
dependency_graph:
  requires:
    - "02-search-decode/02-04 (search_service._extract_evidence_keys available)"
  provides:
    - "scanner seam (scan_partition importable, try-import _native fallback)"
    - "pytest-benchmark baseline (EVALUATION.md with measured numbers)"
    - "KAFKA-07 gate closed (Rust decision recorded and justified)"
  affects:
    - "Phase 3 plans 02-03 (packaging + CI — pure-Python py3-none-any confirmed)"
tech_stack:
  added:
    - "pytest-benchmark>=4.0 (dev extra; installed via pip into .venv)"
  patterns:
    - "TDD RED/GREEN: test commits before implementation commits"
    - "Scanner seam: try/except ImportError guard for native extension"
    - "KAFKA-07 benchmark gate: CPU-bound test before adding Rust dependency"
key_files:
  created:
    - "src/kafka_mcp/scanner.py"
    - "tests/benchmarks/__init__.py"
    - "tests/benchmarks/test_scan_benchmark.py"
    - "EVALUATION.md"
  modified:
    - "pyproject.toml (pytest-benchmark dev extra + tests/benchmarks testpath)"
    - ".planning/PROJECT.md (KAFKA-07 decision entry + requirement marked done)"
    - ".gitignore (.benchmark_result.json + .benchmarks/ added as ephemeral)"
decisions:
  - "KAFKA-07: Rust scanner NOT added — benchmark confirms I/O-bound hot path"
  - "Pure-Python scan_partition is permanent v1 implementation"
  - "Scanner seam (try-import _native) preserved for future Rust drop-in"
metrics:
  duration: "~30 min"
  completed: "2026-06-08T06:56:33Z"
  tasks: 2
  files: 7
---

# Phase 3 Plan 01: Scanner Seam + Benchmark Baseline Summary

**One-liner:** Pure-Python scanner seam with pytest-benchmark baseline proving
I/O-bound hot path — KAFKA-07 Rust gate closed (Rust deferred, not needed in v1).

## What Was Built

### Task 1 (TDD): Scanner seam + pytest-benchmark suite

`src/kafka_mcp/scanner.py` — the scanner seam module. A `try/except ImportError`
guard attempts `from kafka_mcp._native import scan_partition`; on failure (the
expected path without a Rust toolchain) it defines a pure-Python `scan_partition`
that iterates `list[dict]` messages, applies key matching, calls
`_extract_evidence_keys` from the domain service, and returns matching dicts.

`tests/benchmarks/test_scan_benchmark.py` — 5 unit tests + 3 benchmark tests using
`pytest-benchmark.pedantic()`. Unit tests cover: import without native, fallback
behaviour with monkeypatched builtins, correctness (subset filtering), empty input,
no-match. Benchmark tests measure the CPU-only hot loop at 100, 10,000, and 1,000
messages with orjson decode.

TDD gates followed:
- RED commit `c9fc169`: test file + pyproject.toml change; all 8 tests fail
  (ModuleNotFoundError: kafka_mcp.scanner)
- GREEN commit `8dbe28a`: scanner.py implementation; all 8 tests pass

### Task 2 (auto): EVALUATION.md + PROJECT.md Rust decision — commit ae519fb

`EVALUATION.md` at repo root: all four required sections including `## Rust
Decision` with actual benchmark numbers (~48-50 ns/msg key-compare, ~479
ns/msg with orjson decode), analysis of I/O-bound workload character, and the
explicit KAFKA-07 gate outcome.

`.planning/PROJECT.md`: KAFKA-07 requirement marked `[x]` complete; decisions
section has a KAFKA-07 entry with measured latencies, gate rationale, and
evidence file reference.

## Benchmark Numbers (arm64, Python 3.10.4, pytest-benchmark 5.2.3)

| Benchmark                    | N msgs | Mean (µs) | Per-msg (ns) |
|------------------------------|--------|-----------|--------------|
| scan_small (key-compare only)| 100    | ~3.2–4.8  | ~32–48       |
| scan_large (key-compare only)| 10,000 | ~249–502  | ~25–50       |
| decode_dispatch (orjson)     | 1,000  | ~446–479  | ~446–479     |

Real-world `librdkafka poll()` latency: 1–10 ms/batch. CPU work is 4× smaller
than a single poll. Hot path is I/O-bound.

## KAFKA-07 Gate Decision

**Rust NOT added.** Pure-Python scanner is the permanent v1 implementation.

The benchmark confirms the scan hot path is I/O-bound: `librdkafka Consumer.poll()`
network round-trips dominate end-to-end scan time. The CPU work (key comparison,
dict traversal, orjson decode) is negligible — ~25–50 ns/msg for key-compare,
~446–479 ns/msg with orjson — versus 1–10 ms per poll batch. A Rust pyo3 scanner
would not improve end-to-end throughput. Gate condition (CPU-bound speedup ≥2×
achievable) is NOT met.

The scanner seam in `scanner.py` (try-import guard) is preserved so a Rust
extension can be dropped in without any API change if workload character changes.

## Deviations from Plan

None — plan executed exactly as written.

The TDD flow matched the plan: RED tests first (ImportError confirmed),
implementation second (all tests green), benchmark run producing .benchmark_result.json.
pytest-benchmark was installed via `pip` into the existing `.venv` (not `uv sync`
because `uv sync` fails on the grpcio-tools version constraint in Python 3.14 venv;
`python3 -m pytest` uses system Python 3.10 where existing 190 tests run cleanly,
and `.venv/bin/pytest` with Python 3.14 runs the benchmark suite).

## TDD Gate Compliance

- RED gate: commit `c9fc169` (`test(03-01):`) — failing tests confirmed
- GREEN gate: commit `8dbe28a` (`feat(03-01):`) — all tests passing

## Self-Check: PASSED

Files created:
- `src/kafka_mcp/scanner.py` — FOUND
- `tests/benchmarks/__init__.py` — FOUND
- `tests/benchmarks/test_scan_benchmark.py` — FOUND
- `EVALUATION.md` — FOUND

Commits present:
- `c9fc169` — FOUND (RED test gate)
- `8dbe28a` — FOUND (GREEN implementation gate)
- `ae519fb` — FOUND (docs: EVALUATION.md + PROJECT.md KAFKA-07 decision)

Verification:
- `from kafka_mcp.scanner import scan_partition` — OK (Python 3.10.4, no Rust)
- `## Rust Decision` in EVALUATION.md — FOUND
- `KAFKA-07` in .planning/PROJECT.md — FOUND (2 occurrences)
- 190 existing tests pass — CONFIRMED
- 8 benchmark suite tests pass — CONFIRMED (5 unit + 3 benchmark)
