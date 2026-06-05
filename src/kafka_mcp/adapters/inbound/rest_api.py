"""FastAPI REST inbound adapter — MCP-mirror POST /tools/* convention.

Exposes:
  POST /tools/list_topics   — body: {include_internal?: bool}
  POST /tools/describe_topic — body: {topic: str}

Route names follow the MCP tool-call convention (D-16): POST /tools/{tool_name}
taking a JSON body.  No REST-resource routes like /topics or /describe.

T-04-01 (Tampering): Pydantic request models validate all inputs before they
reach KafkaClient.  Unknown topics return HTTP 404 rather than leaking metadata.

Usage::

    from kafka_mcp.adapters.inbound.rest_api import create_app
    from kafka_mcp.adapters.inbound.lib import KafkaClient
    import uvicorn

    client = KafkaClient.from_env()
    app = create_app(client)
    uvicorn.run(app, host="0.0.0.0", port=8000)
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from kafka_mcp.adapters.inbound.lib import KafkaClient
from kafka_mcp.domain.errors import TopicNotFoundError


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


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_app(client: KafkaClient) -> FastAPI:
    """Build and return a FastAPI app wired to *client*.

    Registers:
    - ``POST /tools/list_topics``
    - ``POST /tools/describe_topic``

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

    return app
