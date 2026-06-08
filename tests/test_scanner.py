"""Unit tests for the pure-Python partition scanner seam (kafka_mcp.scanner).

These tests are always collected by the default ``pytest`` invocation — they
have no dependency on pytest-benchmark and exercise the scanner's correctness
and its native-fallback import seam. They were relocated out of
``tests/benchmarks/test_scan_benchmark.py`` (which is module-level
``importorskip``-gated on pytest-benchmark and ``--ignore``-d in CI) so the
seam's correctness is actually exercised in the standard suite + CI (WR-01).
"""

from __future__ import annotations

import builtins
import importlib
import sys
from typing import Any
from unittest.mock import patch

import orjson

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
# Seam / correctness unit tests
# ---------------------------------------------------------------------------


def test_scan_partition_pure_python_importable() -> None:
    """scan_partition must be importable from kafka_mcp.scanner."""
    from kafka_mcp.scanner import scan_partition  # noqa: F401

    assert callable(scan_partition)


def test_scan_partition_no_native_fallback() -> None:
    """Even with kafka_mcp._native absent, scan_partition must be callable.

    Monkeypatches builtins.__import__ so that importing kafka_mcp._native
    raises ImportError, then verifies the seam still exposes scan_partition.

    The reload mutates the cached ``kafka_mcp.scanner`` module object in place,
    so it is restored in a ``finally`` block to avoid leaking the pure-Python
    fallback state into subsequent tests in the same process (WR-04).
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

    import kafka_mcp.scanner as scanner_mod

    try:
        with patch("builtins.__import__", side_effect=_block_native):
            importlib.reload(scanner_mod)
            sp = scanner_mod.scan_partition
        assert callable(sp), "scan_partition must be callable without _native"
    finally:
        # Restore the real (possibly-native) seam for subsequent tests.
        importlib.reload(scanner_mod)


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
