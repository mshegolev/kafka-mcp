"""FastAPI REST inbound adapter — MCP-mirror POST /tools/* convention.

Exposes:
  POST /tools/list_topics       — body: {include_internal?: bool}
  POST /tools/describe_topic    — body: {topic: str}
  POST /tools/search_messages   — body: SearchMessagesRequest
  POST /tools/get_message       — body: GetMessageRequest

Route names follow the MCP tool-call convention (D-16): POST /tools/{tool_name}
taking a JSON body.  No REST-resource routes like /topics or /describe.

T-04-01 (Tampering): Pydantic request models validate all inputs before they
reach KafkaClient.  Unknown topics return HTTP 404 rather than leaking metadata.
T-02-05-A (Tampering): limit field constrained ge=1, le=10000 via Field.

Usage::

    from kafka_mcp.adapters.inbound.rest_api import create_app
    from kafka_mcp.adapters.inbound.lib import KafkaClient
    import uvicorn

    client = KafkaClient.from_env()
    app = create_app(client)
    uvicorn.run(app, host="0.0.0.0", port=8000)
"""

from __future__ import annotations

import base64
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from kafka_mcp.adapters.inbound.lib import KafkaClient
from kafka_mcp.domain.errors import (
    DecodeError,
    MessageNotFoundError,
    TopicNotFoundError,
    TransientError,
)
from kafka_mcp.domain.models import KafkaMessage

# ---------------------------------------------------------------------------
# Request models (T-04-01: Pydantic validates inputs at the boundary)
# ---------------------------------------------------------------------------


class ListTopicsRequest(BaseModel):
    """Request body for POST /tools/list_topics."""

    include_internal: bool = False


class DescribeTopicRequest(BaseModel):
    """Request body for POST /tools/describe_topic.

    ``topic`` must be a non-empty string — Pydantic enforces this by default
    (str fields reject None and non-string types automatically).
    """

    topic: str


class SearchMessagesRequest(BaseModel):
    """Request body for POST /tools/search_messages.

    T-02-05-A: ``limit`` is constrained to 1–10000 to prevent DoS via
    excessive scan depth.
    T-02-05-D: ``time_from``/``time_to`` are pydantic datetime fields;
    FastAPI/pydantic parse ISO8601 automatically and reject malformed input
    with a 422 before it reaches KafkaClient.
    """

    key: str
    key_field: str | None = None
    topics: list[str] | None = None
    time_from: datetime | None = None
    time_to: datetime | None = None
    limit: int = Field(default=500, ge=1, le=10000)


class GetMessageRequest(BaseModel):
    """Request body for POST /tools/get_message."""

    topic: str
    partition: int
    offset: int


# ---------------------------------------------------------------------------
# Serialization helper
# ---------------------------------------------------------------------------


def _serialize_message(msg: KafkaMessage) -> dict:
    """Serialize a KafkaMessage to a JSON-safe dict.

    Converts ``raw`` bytes to a base64-encoded ASCII string so the dict
    can be safely returned over HTTP (bytes are not JSON-serializable).

    Args:
        msg: A :class:`~kafka_mcp.domain.models.KafkaMessage` domain object.

    Returns:
        Dict with all fields from ``model_dump()`` plus ``raw`` replaced
        by its base64-encoded ASCII representation.
    """
    data = msg.model_dump()
    data["raw"] = base64.b64encode(msg.raw).decode("ascii")
    # KEY-02: base64-encode raw_key bytes when present (T-04-06)
    if msg.raw_key is not None:
        data["raw_key"] = base64.b64encode(msg.raw_key).decode("ascii")
    # Serialize datetime to ISO-8601 string (model_dump may return datetime obj)
    if isinstance(data.get("timestamp_utc"), datetime):
        data["timestamp_utc"] = data["timestamp_utc"].isoformat()
    return data


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_app(client: KafkaClient) -> FastAPI:
    """Build and return a FastAPI app wired to *client*.

    Registers:
    - ``POST /tools/list_topics``
    - ``POST /tools/describe_topic``
    - ``POST /tools/search_messages``
    - ``POST /tools/get_message``

    Args:
        client: A :class:`~kafka_mcp.adapters.inbound.lib.KafkaClient`
            instance (real or mock).

    Returns:
        A configured :class:`~fastapi.FastAPI` application instance.
    """
    @asynccontextmanager
    async def _lifespan(_app: FastAPI) -> AsyncIterator[None]:
        """Release the librdkafka Consumer on server shutdown (WR-02).

        The long-lived REST server otherwise leaks a connected consumer
        (background threads/sockets) for the process lifetime.
        """
        try:
            yield
        finally:
            client.close()

    app = FastAPI(
        title="kafka-mcp",
        description="Read-only Kafka MCP REST adapter",
        lifespan=_lifespan,
    )

    @app.post("/tools/list_topics")
    def _list_topics(req: ListTopicsRequest) -> dict:
        """Return a sorted list of topic names.

        Returns:
            ``{"result": ["topic-a", "topic-b", ...]}``
        """
        topics = client.list_topics(include_internal=req.include_internal)
        return {"result": topics}

    @app.post("/tools/describe_topic")
    def _describe_topic(req: DescribeTopicRequest) -> dict:
        """Return partition metadata for a single topic.

        Returns:
            ``{"result": {name, partition_count, partitions: [...]}}``

        Raises:
            HTTPException 404: When the topic does not exist on the broker.
                Body: ``{"detail": {"error": "TopicNotFoundError", "topic": "..."}}``
        """
        try:
            info = client.describe_topic(req.topic)
            return {"result": info.model_dump()}
        except TopicNotFoundError as exc:
            raise HTTPException(
                status_code=404,
                detail={"error": "TopicNotFoundError", "topic": exc.topic},
            ) from exc

    @app.post("/tools/search_messages")
    def _search_messages(req: SearchMessagesRequest) -> dict:
        """Search Kafka messages by key within an optional time window.

        Returns:
            ``{"result": [...]}`` — list of message dicts with base64 raw.
        """
        results = client.search_messages(
            req.key,
            key_field=req.key_field,
            topics=req.topics,
            time_from=req.time_from,
            time_to=req.time_to,
            limit=req.limit,
        )
        return {"result": [_serialize_message(m) for m in results]}

    @app.post("/tools/get_message")
    def _get_message(req: GetMessageRequest) -> dict:
        """Fetch and decode a single message by exact coordinates.

        Returns:
            ``{"result": {...}}`` — message dict with base64 raw.

        Raises:
            HTTPException 404: When no message exists at the given coordinates.
                Body: ``{"detail": {"error": "MessageNotFoundError", ...}}``
            HTTPException 422: When the payload cannot be decoded.
                Body: ``{"detail": {"error": "DecodeError", ..., "reason": "..."}}``
        """
        try:
            msg = client.get_message(req.topic, req.partition, req.offset)
            return {"result": _serialize_message(msg)}
        except MessageNotFoundError as exc:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "MessageNotFoundError",
                    "topic": exc.topic,
                    "partition": exc.partition,
                    "offset": exc.offset,
                },
            ) from exc
        except TransientError as exc:
            # WR-05: in-range offset that timed out — transient, not absence.
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "TransientError",
                    "topic": exc.topic,
                    "partition": exc.partition,
                    "offset": exc.offset,
                    "reason": exc.reason,
                },
            ) from exc
        except DecodeError as exc:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "DecodeError",
                    "topic": exc.topic,
                    "partition": exc.partition,
                    "offset": exc.offset,
                    "reason": exc.reason,
                },
            ) from exc

    return app
