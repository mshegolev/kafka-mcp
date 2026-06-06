"""kafka-mcp server entry point.

Dispatches to one of three modes based on sys.argv:

1. ``--stdio`` flag  → MCP stdio server (reads from stdin, writes to stdout)
2. CLI subcommand    → CLI runner (list-topics, describe-topic)
3. Default / "serve" → FastAPI/uvicorn HTTP server

Environment variables (T-04-06):
  KAFKA_MCP_HOST: uvicorn bind host (default "127.0.0.1", loopback-only).
    Set explicitly to "0.0.0.0" to expose the HTTP face on all interfaces —
    the brick has NO built-in authentication, so a public bind MUST be fronted
    by an authenticating reverse proxy (API key / mTLS / auth header). A startup
    warning is emitted when a non-loopback host is configured.
  KAFKA_MCP_PORT: uvicorn bind port (default "8000")
  KAFKA_MCP_BOOTSTRAP_SERVERS: required — fails fast with ConfigError if absent

Usage::

    # Start HTTP server (default mode)
    kafka-mcp
    kafka-mcp serve

    # Start MCP stdio server
    kafka-mcp --stdio

    # Use CLI
    kafka-mcp list-topics
    kafka-mcp describe-topic payments --json
"""

from __future__ import annotations

import os
import sys


def main() -> None:
    """Entry point for the ``kafka-mcp`` command.

    Inspects ``sys.argv`` to select execution mode, then delegates.
    ConfigError is raised before any server starts if required env vars
    are absent (D-04 fail-fast; T-04-06 bounds server lifetime to valid config).
    """
    args = sys.argv[1:]

    # MCP stdio mode: kafka-mcp --stdio
    if "--stdio" in args:
        from kafka_mcp.adapters.inbound.lib import KafkaClient
        from kafka_mcp.adapters.inbound.mcp_stdio import create_mcp_server

        client = KafkaClient.from_env()
        server = create_mcp_server(client)
        # WR-02: close the underlying Consumer when the stdio server exits.
        try:
            server.run("stdio")
        finally:
            client.close()
        return

    # CLI mode: kafka-mcp list-topics | describe-topic | search-messages | get-message ...
    _cli_subcommands = {
        "list-topics",
        "describe-topic",
        "search-messages",
        "get-message",
    }
    if args and args[0] in _cli_subcommands:
        from kafka_mcp.adapters.inbound.cli import main as cli_main

        cli_main(args)
        return

    # Default: HTTP server (kafka-mcp or kafka-mcp serve)
    import uvicorn

    from kafka_mcp.adapters.inbound.lib import KafkaClient
    from kafka_mcp.adapters.inbound.rest_api import create_app

    client = KafkaClient.from_env()
    # create_app registers a FastAPI shutdown hook that calls client.close()
    # so the librdkafka Consumer is released on server shutdown (WR-02).
    app = create_app(client)
    # Secure default: bind to loopback. Exposing the unauthenticated HTTP face on
    # all interfaces requires an explicit KAFKA_MCP_HOST=0.0.0.0 opt-in.
    host = os.environ.get("KAFKA_MCP_HOST", "127.0.0.1")
    port = int(os.environ.get("KAFKA_MCP_PORT", "8000"))
    if host not in ("127.0.0.1", "localhost", "::1"):
        print(
            f"WARNING: kafka-mcp HTTP face binding to {host!r} (non-loopback). "
            "This brick has no built-in authentication — front it with an "
            "authenticating reverse proxy (API key / mTLS / auth header) before "
            "exposing it on an untrusted network.",
            file=sys.stderr,
        )
    uvicorn.run(app, host=host, port=port)
