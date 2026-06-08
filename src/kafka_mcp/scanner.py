"""Scanner seam — partition scan/decode hot loop.

This module exposes the ``scan_partition`` callable.

If ``kafka_mcp._native`` is present (a compiled pyo3 extension), its
``scan_partition`` is used; otherwise the pure-Python implementation below is
active. The fallback is selected automatically via a ``try/except ImportError``
guard, so the entire test suite and all four inbound faces work with NO Rust
toolchain present (KAFKA-07, SC-2).

Decision (KAFKA-07):
    The pytest-benchmark baseline (see EVALUATION.md) confirmed that the scan
    hot path is I/O-bound: librdkafka's poll() network round-trips dominate;
    the CPU work here (key comparison, dict traversal, orjson decode) is
    negligible. A Rust pyo3 scanner would NOT improve end-to-end throughput.
    The pure-Python implementation is therefore the permanent active path for v1.
    If the workload character changes (large in-memory batch decode without
    broker I/O), re-run the benchmark to re-evaluate.

    The try-import seam is preserved so a Rust extension could be dropped in
    later without any API change.

STRIDE mitigations (T-03-01-A):
    - Only ``ImportError`` is caught in the try-import block; no bare ``except``.
    - The native extension is absent by default in CI without a Rust toolchain.
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Seam: try to import the compiled Rust extension; fall back to pure Python.
# ---------------------------------------------------------------------------
try:
    from kafka_mcp._native import scan_partition  # type: ignore[import]
except ImportError:
    # Pure-Python fallback — active whenever the Rust extension is absent.
    # This is the expected path for v1 (benchmark confirmed I/O-bound hot path).

    from kafka_mcp.domain.search_service import _extract_evidence_keys  # noqa: E402

    def scan_partition(  # type: ignore[misc]
        messages: list[dict[str, Any]],
        key: str,
    ) -> list[dict[str, Any]]:
        """Scan a list of in-memory message dicts and return those matching key.

        This function mirrors the key-matching and evidence-extraction logic
        from :func:`kafka_mcp.domain.search_service.TopicService.search_messages`
        but operates on plain dicts rather than
        :class:`kafka_mcp.domain.models.KafkaMessage` objects.  It is the
        benchmark subject for KAFKA-07 (pure-Python hot-path baseline).

        The dict schema expected per message:
            ``{"key": str, "value": dict, "headers": dict, "raw": bytes}``

        Args:
            messages: List of message dicts to scan.
            key: The key string to match against ``msg["key"]``.

        Returns:
            A new list containing only the dicts whose ``"key"`` equals *key*.
            Evidence identifiers are extracted and merged into each match.
        """
        result: list[dict[str, Any]] = []
        for msg in messages:
            if msg.get("key") == key:
                # Mirror evidence-key extraction from the domain service
                evidence = _extract_evidence_keys(
                    msg.get("value"),
                    msg.get("headers", {}),
                )
                # Return a shallow copy with evidence merged in
                result.append({**msg, "keys": evidence})
        return result


__all__ = ["scan_partition"]
