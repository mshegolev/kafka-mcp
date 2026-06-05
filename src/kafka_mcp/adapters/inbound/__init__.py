"""Inbound adapters — lib facade, MCP stdio, REST API, CLI."""

from .lib import KafkaClient

__all__ = ["KafkaClient"]
