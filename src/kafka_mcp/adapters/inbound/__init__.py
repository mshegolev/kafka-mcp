"""Inbound adapters — lib facade, MCP stdio, REST API, CLI."""

from .lib import KafkaClient
from .mcp_stdio import create_mcp_server
from .rest_api import create_app

__all__ = ["KafkaClient", "create_mcp_server", "create_app"]
