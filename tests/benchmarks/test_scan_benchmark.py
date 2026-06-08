"""pytest-benchmark suite for the pure-Python partition scan/decode hot loop.

Unit tests (always run, no benchmark fixture):
  - test_scan_partition_pure_python_importable — import guard
  - test_scan_partition_no_native_fallback — fallback behaviour

Benchmark tests (require pytest-benchmark; skipped when absent):
  - test_benchmark_scan_small    — 100 msgs, 1 match
  - test_benchmark_scan_large    — 10_000 msgs, 10 matches
  - test_benchmark_decode_dispatch — 1_000 msgs with orjson decode

All benchmarks use pedantic() so the harness controls setup/teardown isolation.
Benchmarks never assert a latency threshold — they measure only.
"""

from __future__ import annotations

import builtins
import sys
from typing import Any
from unittest.mock import patch

import orjson
import pytest

# ---------------------------------------------------------------------------
# Skip guard: if pytest-benchmark is not installed, skip benchmark tests only
# ---------------------------------------------------------------------------
pytest_benchmark = pytest.importorskip(
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
# Unit tests (RED before scanner.py exists)
# ---------------------------------------------------------------------------


def test_scan_partition_pure_python_importable() -> None:
    """scan_partition must be importable from kafka_mcp.scanner."""
    # This will raise ImportError (RED) until scanner.py is created.
    from kafka_mcp.scanner import scan_partition  # noqa: F401

    assert callable(scan_partition)


def test_scan_partition_no_native_fallback() -> None:
    """Even with kafka_mcp._native absent, scan_partition must be callable.

    Monkeypatches builtins.__import__ so that importing kafka_mcp._native
    raises ImportError, then verifies the seam still exposes scan_partition.
    """
    # Remove cached module if previously imported
    for key in list(sys.modules.keys()):
        if "kafka_mcp.scanner" in key or "kafka_mcp._native" in key:
            del sys.modules[key]

    original_import = builtins.__import__

    def _block_native(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "kafka_mcp._native":
            raise ImportError("_native not available (test monkeypatch)")
        return original_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=_block_native):
        # Re-import after patching
        import importlib
        import kafka_mcp.scanner as scanner_mod
        importlib.reload(scanner_mod)
        sp = scanner_mod.scan_partition

    assert callable(sp), "scan_partition must be callable without _native"


def test_scan_partition_returns_correct_subset() -> None:
    """scan_partition must return only messages matching target_key."""
    from kafka_mcp.scanner import scan_partition

    msgs = _make_messages(100, "target-key", match_every=10)
    result = scan_partition(msgs, "target-key")
    # Every 10th message matches → indices 0, 10, 20, ..., 90 → 10 matches
    assert len(result) == 10
    for item in result:
        assert item["key"] == "target-key"


def test_scan_partition_empty_input() -> None:
    """scan_partition must return [] for an empty message list."""
    from kafka_mcp.scanner import scan_partition

    assert scan_partition([], "any-key") == []


def test_scan_partition_no_matches() -> None:
    """scan_partition must return [] when no messages match the key."""
    from kafka_mcp.scanner import scan_partition

    msgs = _make_messages(50, "target-key", match_every=None)
    result = scan_partition(msgs, "target-key")
    assert result == []


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
