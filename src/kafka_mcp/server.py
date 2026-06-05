"""kafka-mcp server entry point.

Dispatches to one of three modes based on sys.argv:

1. ``--stdio`` flag  → MCP stdio server (reads from stdin, writes to stdout)
2. CLI subcommand    → CLI runner (list-topics, describe-topic)
3. Default / "serve" → FastAPI/uvicorn HTTP server

Environment variables (T-04-06):
  KAFKA_MCP_HOST: uvicorn bind host (default "0.0.0.0")
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
        server.run("stdio")
        return

    # CLI mode: kafka-mcp list-topics | describe-topic ...
    _cli_subcommands = {"list-topics", "describe-topic"}
    if args and args[0] in _cli_subcommands:
        from kafka_mcp.adapters.inbound.cli import main as cli_main

        cli_main(args)
        return

    # Default: HTTP server (kafka-mcp or kafka-mcp serve)
    import uvicorn

    from kafka_mcp.adapters.inbound.lib import KafkaClient
    from kafka_mcp.adapters.inbound.rest_api import create_app

    client = KafkaClient.from_env()
    app = create_app(client)
    host = os.environ.get("KAFKA_MCP_HOST", "0.0.0.0")
    port = int(os.environ.get("KAFKA_MCP_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)
