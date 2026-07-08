"""MTLS-02 — real-broker mTLS end-to-end integration test.

Connects the brick to a REAL SSL/mTLS Kafka broker using client-certificate
material and runs a read-only operation (list_topics, optionally
describe_topic) over the TLS handshake. This proves the ssl.* wiring
(plan 11-01) works on the wire, not merely in the librdkafka conf dict.

The test is env-gated (mirroring the v1.1 Phase 6 real-broker contour):
it is marked ``@pytest.mark.integration`` so the default
``pytest -m 'not integration'`` run never collects it, and it SKIPS
cleanly (never fails/errors) when the mTLS broker + client-cert env vars
are absent — so CI without a broker stays green.

A working staging mTLS contour was proven this session
(broker 10.35.158.66:9094, stage certs, SECURITY_PROTOCOL=SSL, via
``kafka-mcp list-topics`` / ``describe-topic eord``). The broker address
and cert paths are read from the environment; NO cert material is
committed to the repo (T-11-03).

Required env to run (all must be present, else SKIP):
    KAFKA_MCP_MTLS_TEST_BOOTSTRAP  broker SSL-listener address
                                   (falls back to KAFKA_MCP_BOOTSTRAP_SERVERS)
    KAFKA_MCP_SSL_CERTIFICATE_LOCATION  client cert PEM path
    KAFKA_MCP_SSL_KEY_LOCATION          client key PEM path
    KAFKA_MCP_SSL_CA_LOCATION           CA cert PEM path
Optional:
    KAFKA_MCP_SSL_KEY_PASSWORD  client key passphrase (omit if unencrypted)
    KAFKA_MCP_MTLS_TEST_TOPIC   topic to exercise describe_topic against
"""

from __future__ import annotations

import os

import pytest

from kafka_mcp.domain.models import TopicInfo

# --------------------------------------------------------------------------- #
# Env-gated skip guard                                                         #
# --------------------------------------------------------------------------- #

_BOOTSTRAP = os.environ.get("KAFKA_MCP_MTLS_TEST_BOOTSTRAP") or os.environ.get(
    "KAFKA_MCP_BOOTSTRAP_SERVERS"
)
_CERT = os.environ.get("KAFKA_MCP_SSL_CERTIFICATE_LOCATION")
_KEY = os.environ.get("KAFKA_MCP_SSL_KEY_LOCATION")
_CA = os.environ.get("KAFKA_MCP_SSL_CA_LOCATION")
_KEY_PASSWORD = os.environ.get("KAFKA_MCP_SSL_KEY_PASSWORD")  # optional
_DESCRIBE_TOPIC = os.environ.get("KAFKA_MCP_MTLS_TEST_TOPIC")  # optional

# All four connection essentials (broker + cert + key + CA) must be present.
# When any is absent the module-level skipif fires → SKIP, never FAIL/ERROR.
_MTLS_ENV_READY = bool(_BOOTSTRAP and _CERT and _KEY and _CA)
_SKIP_REASON = (
    "mTLS broker + client cert env not provided — skipping MTLS-02 e2e "
    "(set KAFKA_MCP_MTLS_TEST_BOOTSTRAP + KAFKA_MCP_SSL_CERTIFICATE_LOCATION "
    "+ KAFKA_MCP_SSL_KEY_LOCATION + KAFKA_MCP_SSL_CA_LOCATION to run)"
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _MTLS_ENV_READY, reason=_SKIP_REASON),
]


# --------------------------------------------------------------------------- #
# Fixtures                                                                     #
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def mtls_settings():
    """Build KafkaMcpSettings from the env-provided mTLS contour.

    Values are passed explicitly to KafkaMcpSettings(...) rather than relying
    on ambient env so the test is self-contained and unambiguous about which
    fields exercise the SSL/mTLS path.
    """
    from kafka_mcp.config import KafkaMcpSettings

    kwargs: dict[str, object] = {
        "bootstrap_servers": _BOOTSTRAP,
        "security_protocol": "SSL",
        "ssl_certificate_location": _CERT,
        "ssl_key_location": _KEY,
        "ssl_ca_location": _CA,
    }
    if _KEY_PASSWORD:
        kwargs["ssl_key_password"] = _KEY_PASSWORD

    return KafkaMcpSettings(**kwargs)


@pytest.fixture(scope="module")
def mtls_client(mtls_settings):
    """KafkaClient wired to the real mTLS broker via ConfluentConsumerAdapter.

    A null/None registry is used — decode is not needed for the read-only
    ops (list_topics / describe_topic). Used as a context manager so the
    underlying consumer/admin client is closed after the module's tests.
    """
    from kafka_mcp.adapters.inbound.lib import KafkaClient
    from kafka_mcp.adapters.outbound.confluent_consumer import (
        ConfluentConsumerAdapter,
    )

    consumer = ConfluentConsumerAdapter(mtls_settings)
    with KafkaClient(consumer) as client:
        yield client


# --------------------------------------------------------------------------- #
# Tests                                                                        #
# --------------------------------------------------------------------------- #


class TestMtlsEndToEnd:
    """Read-only ops over a real client-certificate TLS handshake (MTLS-02)."""

    def test_list_topics_over_mtls(self, mtls_client):
        """list_topics() succeeds over the mTLS handshake and returns a list.

        A successful return proves the SSL/mTLS handshake completed: the
        AdminClient fetched broker metadata over the client-cert TLS channel.
        """
        topics = mtls_client.list_topics()
        assert isinstance(topics, list)

    def test_describe_topic_over_mtls(self, mtls_client):
        """describe_topic() over mTLS returns valid TopicInfo (opt-in via env).

        Skipped unless KAFKA_MCP_MTLS_TEST_TOPIC names a topic on the broker.
        """
        if not _DESCRIBE_TOPIC:
            pytest.skip(
                "KAFKA_MCP_MTLS_TEST_TOPIC not set — skipping describe_topic "
                "leg of MTLS-02"
            )
        info = mtls_client.describe_topic(_DESCRIBE_TOPIC)
        assert isinstance(info, TopicInfo)
        assert info.name == _DESCRIBE_TOPIC
        assert info.partition_count >= 1
