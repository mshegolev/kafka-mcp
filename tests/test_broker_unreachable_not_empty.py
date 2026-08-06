"""An empty search result must not be reported when the broker was unreachable.

Why this file exists
--------------------
librdkafka reports connection-level failures — rejected TLS handshake, all brokers down,
refused authentication — through an *asynchronous* error callback, not by raising from
``poll()``. Until this brick installed an ``error_cb``, those errors were discarded by the
client library: ``poll()`` returned ``None`` until the caller's budget ran out and the search
finished normally with an empty list.

Observed on ep-stage 2026-08-06: the broker was rejecting the client certificate (the pod
carried ``CN=escart`` where working clients carry ``CN=eord``) and the consuming service
recorded Kafka as ``status=empty`` — no exception, no log line, nothing to investigate. The
search sat in librdkafka's retry loop for 352 seconds and still reported "no data".

"There is no such message" and "we never reached the broker" require opposite responses from
an operator. These tests pin that they stay distinguishable.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from kafka_mcp.adapters.inbound.lib import KafkaClient
from kafka_mcp.adapters.outbound.confluent_consumer import ConnectionErrorRecorder
from kafka_mcp.domain.errors import BrokerUnreachableError


class _Consumer:
    """Minimal ConsumerPort double; `last_connection_error` is opt-in, as on the real adapter."""

    def __init__(self, connection_error: str | None = None, present: bool = True) -> None:
        if present:
            self.last_connection_error = connection_error


def _client(consumer: object, messages: list) -> KafkaClient:
    client = KafkaClient(consumer)  # type: ignore[arg-type]
    client._service = MagicMock()
    client._service.search_messages = MagicMock(return_value=messages)
    return client


def test_empty_result_over_a_failed_connection_raises() -> None:
    client = _client(_Consumer("_SSL: broker certificate rejected"), messages=[])

    with pytest.raises(BrokerUnreachableError) as excinfo:
        client.search_messages("order-42")

    assert "_SSL" in str(excinfo.value)
    assert excinfo.value.detail == "_SSL: broker certificate rejected"


def test_empty_result_over_a_healthy_connection_stays_empty() -> None:
    """The ordinary "genuinely no matches" answer must be untouched."""
    client = _client(_Consumer(None), messages=[])

    assert client.search_messages("order-42") == []


def test_messages_found_despite_a_past_error_are_returned() -> None:
    """A recovered blip must not invalidate data the broker actually returned.

    Connection errors can be transient — one refused broker in a cluster, a reconnect. If
    messages came back, the link worked; raising here would turn a successful search into a
    false alarm, which is how a useful check gets switched off.
    """
    client = _client(_Consumer("_TRANSPORT: broker connection failure"), messages=[object()])

    assert len(client.search_messages("order-42")) == 1


def test_consumer_without_the_attribute_is_unaffected() -> None:
    """Existing test doubles predate `last_connection_error` and must keep working."""
    client = _client(_Consumer(present=False), messages=[])

    assert client.search_messages("order-42") == []


class TestConnectionErrorRecorder:
    def test_records_and_exposes_the_last_error(self) -> None:
        recorder = ConnectionErrorRecorder("consumer")
        err = MagicMock()
        err.name.return_value = "_SSL"
        err.str.return_value = "broker certificate rejected"

        recorder(err)

        assert recorder.last_error == "_SSL: broker certificate rejected"
        assert recorder.error_count == 1

    def test_clean_link_reports_nothing(self) -> None:
        assert ConnectionErrorRecorder("consumer").last_error is None
