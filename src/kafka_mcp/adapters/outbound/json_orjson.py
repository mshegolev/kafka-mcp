"""orjson-backed JSON codec helpers.

Thin wrappers around ``orjson`` for consistent serialization across all
inbound adapters.  Using ``orjson`` gives ~3-5x throughput over the stdlib
``json`` module for message-body encode/decode (relevant when scanning
large topics in Phase 2).

Example::

    from kafka_mcp.adapters.outbound.json_orjson import orjson_loads, orjson_dumps

    data = orjson_loads(b'{"key": "value"}')   # -> {"key": "value"}
    raw  = orjson_dumps({"key": "value"})       # -> b'{"key":"value"}'
"""

from __future__ import annotations

import orjson


def orjson_loads(data: bytes | str) -> dict:
    """Deserialise JSON bytes or string to a Python dict.

    Args:
        data: Raw JSON payload as ``bytes`` or ``str``.

    Returns:
        Parsed Python dict.

    Raises:
        orjson.JSONDecodeError: When ``data`` is not valid JSON.
    """
    return orjson.loads(data)


def orjson_dumps(obj: dict) -> bytes:
    """Serialise a Python dict to compact JSON bytes.

    orjson produces compact output (no spaces after ``:`` or ``,``), which
    is the canonical wire format used across adapters.

    Args:
        obj: Python dict to serialise.

    Returns:
        UTF-8 encoded JSON bytes.

    Raises:
        TypeError: When ``obj`` contains values that orjson cannot encode.
    """
    return orjson.dumps(obj)
