"""Outbound adapters — confluent consumer, schema registry HTTP."""

from .confluent_consumer import ConfluentConsumerAdapter
from .json_orjson import orjson_dumps, orjson_loads
from .schema_registry_http import SchemaRegistryHttpAdapter

__all__ = [
    "ConfluentConsumerAdapter",
    "SchemaRegistryHttpAdapter",
    "orjson_loads",
    "orjson_dumps",
]
