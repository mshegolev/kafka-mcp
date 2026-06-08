"""pytest-benchmark suite for the pure-Python partition scan/decode hot loop.

Benchmark tests (require pytest-benchmark; skipped when absent):
  - test_benchmark_scan_small    — 100 msgs, 1 match
  - test_benchmark_scan_large    — 10_000 msgs, 10 matches
  - test_benchmark_decode_dispatch — 1_000 msgs with orjson decode

All benchmarks use pedantic() so the harness controls setup/teardown isolation.
Benchmarks never assert a latency threshold — they measure only.

The scanner *unit* tests (correctness + native-fallback seam) live in
``tests/test_scanner.py`` so they run in the default suite and CI; this module
is module-level ``importorskip``-gated on pytest-benchmark and CI runs it with
``--ignore=tests/benchmarks`` (WR-01).
"""

from __future__ import annotations

from typing import Any

import orjson
import pytest

# ---------------------------------------------------------------------------
# Skip guard: if pytest-benchmark is not installed, skip the whole module
# (this module contains only benchmark-fixture tests)
# ---------------------------------------------------------------------------
pytest.importorskip(
    "pytest_benchmark",
    reason="pytest-benchmark not installed; skipping benchmarks",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_messages(
    n: int,
    target_key: str,
    match_every: int | None = None,
) -> list[dict[str, Any]]:
    """Produce a synthetic in-memory list of message dicts.

    Args:
        n: Total number of messages to produce.
        target_key: The key value that constitutes a "match".
        match_every: If given, every match_every-th message (0-indexed) will
            have ``key == target_key``; the rest have unique keys.  If None,
            no messages match.

    Returns:
        A list of dicts with keys: "key", "value", "headers", "raw".
    """
    msgs: list[dict[str, Any]] = []
    for i in range(n):
        if match_every is not None and i % match_every == 0:
            key = target_key
        else:
            key = f"key-{i}"
        value = {
            "order_id": f"ORD-{i}",
            "msisdn": f"7900{i:07d}",
            "customerId": f"CUST-{i}",
            "productId": f"PROD-{i % 50}",
        }
        msgs.append(
            {
                "key": key,
                "value": value,
                "headers": {"trace_id": f"trace-{i}"},
                "raw": orjson.dumps(value),
            }
        )
    return msgs


# ---------------------------------------------------------------------------
# Benchmark fixtures
# ---------------------------------------------------------------------------

# Pre-built message sets (module-level so they are not re-created per call)
_MSGS_100 = _make_messages(100, "target-key", match_every=100)   # 1 match
_MSGS_10K = _make_messages(10_000, "target-key", match_every=1_000)  # 10 matches
_MSGS_1K_JSON = _make_messages(1_000, "target", match_every=100)  # 10 matches


def _scan_with_decode(
    messages: list[dict[str, Any]], key: str
) -> list[dict[str, Any]]:
    """Scan + orjson.loads on each message's raw bytes."""
    from kafka_mcp.scanner import scan_partition

    # Decode raw bytes on every message before scanning
    decoded_msgs = []
    for msg in messages:
        decoded_value = orjson.loads(msg["raw"])
        decoded_msgs.append({**msg, "value": decoded_value})
    return scan_partition(decoded_msgs, key)


# ---------------------------------------------------------------------------
# Benchmark tests (always-pass — they measure, never threshold-assert)
# ---------------------------------------------------------------------------


def test_benchmark_scan_small(benchmark: Any) -> None:
    """Benchmark: 100 messages, 1 match — pure-Python key comparison loop."""
    from kafka_mcp.scanner import scan_partition

    benchmark.pedantic(
        scan_partition,
        args=[_MSGS_100, "target-key"],
        rounds=50,
        warmup_rounds=5,
    )


def test_benchmark_scan_large(benchmark: Any) -> None:
    """Benchmark: 10_000 messages, 10 matches — measures loop throughput."""
    from kafka_mcp.scanner import scan_partition

    benchmark.pedantic(
        scan_partition,
        args=[_MSGS_10K, "target-key"],
        rounds=20,
        warmup_rounds=3,
    )


def test_benchmark_decode_dispatch(benchmark: Any) -> None:
    """Benchmark: 1_000 messages with orjson.loads on raw bytes."""
    benchmark.pedantic(
        _scan_with_decode,
        args=[_MSGS_1K_JSON, "target"],
        rounds=30,
        warmup_rounds=5,
    )
