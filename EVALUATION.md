# EVALUATION.md — Partition Scanner Benchmark Baseline

## Overview

This document records the pytest-benchmark baseline for the pure-Python
partition scan/decode hot loop in `kafka_mcp.scanner.scan_partition`.
The benchmark was run on **2026-06-08** on **arm64 (Apple Silicon)** with
**Python 3.10.4** using `pytest-benchmark 5.2.3` in pedantic mode (controls
setup/teardown isolation per call). The benchmark subject is a purely
in-memory function: no live Kafka broker, no network I/O, no disk access.
The goal is to measure the CPU work (key comparison, dict traversal, orjson
decode) to determine whether a Rust/pyo3 scanner would provide a meaningful
CPU-bound speedup (KAFKA-07 gate criterion: ≥2× speedup required).

## Benchmark Results

| Benchmark Name                 | N msgs | Match rate | Mean (µs) | StdDev (µs) | Ops/sec   | Rounds |
|-------------------------------|--------|------------|-----------|-------------|-----------|--------|
| test_benchmark_scan_small     | 100    | 1/100      | 3.24      | 0.071       | 308,486   | 50     |
| test_benchmark_scan_large     | 10,000 | 10/10,000  | 249.14    | 4.730       | 4,014     | 20     |
| test_benchmark_decode_dispatch| 1,000  | 10/1,000   | 445.68    | 11.628      | 2,244     | 30     |

**Per-message cost (pure-Python CPU work only):**

- `scan_small`: 3.24 µs / 100 msgs = **~32 ns per message** (key compare + evidence extract)
- `scan_large`: 249.14 µs / 10,000 msgs = **~25 ns per message** (same loop, amortised)
- `decode_dispatch`: 445.68 µs / 1,000 msgs = **~446 ns per message** (orjson.loads + key compare)

Hardware: Apple M-series arm64. Python 3.10.4 (CPython). pytest-benchmark 5.2.3.

## Analysis

The scan hot loop (key comparison + dict traversal + `_extract_evidence_keys`)
costs **~25–32 nanoseconds per message** for the pure key-compare path and
**~446 nanoseconds per message** when `orjson.loads` is included. In a
real Kafka scan, the dominant cost is `librdkafka Consumer.poll()` — a
synchronous network round-trip to the broker, which on even a local broker
takes **1–10 milliseconds per batch**. Even at a generous 1 ms poll latency,
the CPU work (≤0.45 µs/msg × 500 msg/batch = ~225 µs) is **at least 4× smaller**
than a single poll(). The bottleneck is the broker network, not the CPU.

A Rust scanner replacing the pure-Python byte-processing would shave the
already-negligible CPU term but would have no effect on the dominant I/O term.
End-to-end throughput would be unchanged. Therefore the KAFKA-07 gate condition
(CPU-bound speedup ≥2×) is **not met** — adding a Rust pyo3 scanner in v1 would
be premature optimisation (the "premature Rust" pitfall noted in the project brief).

## Rust Decision

**KAFKA-07 gate result: pure-Python.**

The benchmark shows **~25–32 ns per message** (key compare path) and
**~446 ns per message** (with orjson decode). The hot path is I/O-bound:
`librdkafka poll()` network round-trips dominate the real-world scan;
the CPU work measured here is negligible relative to broker latency.
No CPU-bound speedup ≥2× is achievable via a Rust pyo3 scanner in this
workload. **The Rust native/ extension is NOT added in v1.**

The pure-Python scanner seam (`src/kafka_mcp/scanner.py`, try-import guard)
remains the **permanent active implementation**. The seam is already prepared
for a Rust drop-in without any API change.

Re-evaluation trigger: if the workload character changes (large in-memory
batch decode without broker I/O, e.g. a replay/analytics use case), re-run
this benchmark to determine whether the CPU term becomes dominant.

## If Rust Were Needed

If a future benchmark proves a CPU-bound hot path, the planned build path is:

- **Extension:** `native/` Rust crate using **pyo3 0.22+** with `abi3` wheels
  (Python 3.10+ compatible, no per-minor-version rebuild needed).
- **Build backend:** Replace `hatchling` with **maturin 0.15+** in
  `pyproject.toml`; maturin handles compilation + PEP 517 wheel packaging.
- **CI wheels:** **cibuildwheel 2.21+** builds the full matrix in GitHub
  Actions: Linux manylinux/musllinux (x86_64 + aarch64), macOS arm64/x86_64,
  Windows AMD64, Python 3.10–3.12; plus an sdist fallback for other platforms.
- **Activation:** The seam in `scanner.py` already imports the extension via
  `from kafka_mcp._native import scan_partition`; installing the wheel is
  sufficient to activate the fast path with zero code changes.
- **Pure-Python fallback:** `orjson` remains the `except ImportError` fallback
  so `pip install kafka-mcp` works without a Rust toolchain on any platform.
