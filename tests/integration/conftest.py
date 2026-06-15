"""Testcontainers fixtures for real-broker integration tests.

Session-scoped: containers start once per test session, shared across all
integration tests. Tests skip gracefully when Docker is unavailable.

Fixtures:
    kafka_container   — running KafkaContainer (session)
    sr_container      — running Schema Registry DockerContainer (session)
    bootstrap_servers — broker address string
    schema_registry_url — SR HTTP URL string
    kafka_settings    — KafkaMcpSettings wired to real containers
    kafka_client      — KafkaClient wired to real broker + real SR
    seed_json_topic   — seeds a "test-json" topic with 5 plain JSON messages
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.request

import pytest

pytestmark = pytest.mark.integration


def _ensure_docker_host() -> None:
    """Ensure DOCKER_HOST is set so the docker Python SDK can connect.

    When using Colima or other non-default Docker contexts the Unix
    socket lives outside ``/var/run/docker.sock``.  If DOCKER_HOST is
    already set we trust it; otherwise we ask ``docker context inspect``
    for the current endpoint and export it.
    """
    if os.environ.get("DOCKER_HOST"):
        return

    try:
        result = subprocess.run(
            ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        endpoint = result.stdout.strip()
        if endpoint and endpoint != "unix:///var/run/docker.sock":
            os.environ["DOCKER_HOST"] = endpoint
    except Exception:
        pass  # best-effort; will fail later with a clear Docker error


# ---------------------------------------------------------------------------
# Kafka container
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kafka_container():
    """Start a Kafka container (session-scoped).

    Skips gracefully when Docker is unavailable.
    Uses pinned image tag for reproducibility (T-06-01).
    """
    _ensure_docker_host()

    try:
        from testcontainers.kafka import KafkaContainer
    except ImportError:
        pytest.skip("testcontainers[kafka] not installed")

    try:
        container = KafkaContainer("confluentinc/cp-kafka:7.6.0")
        container.start(timeout=120)
    except Exception as exc:
        pytest.skip(f"Docker not available — skipping integration tests: {exc}")

    yield container
    container.stop()


# ---------------------------------------------------------------------------
# Schema Registry container
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def sr_container(kafka_container):
    """Start a Schema Registry container connected to the Kafka container.

    Uses the Kafka container's internal network IP for inter-container
    communication. Waits up to 30 s for SR to become healthy (T-06-02).
    """
    import docker as docker_sdk
    from testcontainers.core.container import DockerContainer

    # Get Kafka container's internal IP for container-to-container communication.
    # CP-Kafka internal PLAINTEXT listener is on port 9092 by default.
    docker_client = docker_sdk.from_env()
    container_info = docker_client.containers.get(kafka_container._container.id)
    net_settings = container_info.attrs.get("NetworkSettings", {})
    kafka_network_ip = net_settings.get("IPAddress") or ""
    if not kafka_network_ip:
        # Colima / non-default Docker contexts put the IP inside Networks.bridge
        networks = net_settings.get("Networks", {})
        for net_info in networks.values():
            ip = net_info.get("IPAddress", "")
            if ip:
                kafka_network_ip = ip
                break
    if not kafka_network_ip:
        pytest.fail("Could not determine Kafka container internal IP")

    sr_kafka_bootstrap = f"PLAINTEXT://{kafka_network_ip}:9092"

    sr = (
        DockerContainer("confluentinc/cp-schema-registry:7.6.0")
        .with_env("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .with_env("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", sr_kafka_bootstrap)
        .with_env("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
        .with_exposed_ports(8081)
    )
    sr.start()

    # Wait for SR to become healthy
    sr_host = sr.get_container_host_ip()
    sr_port = sr.get_exposed_port(8081)
    sr_url = f"http://{sr_host}:{sr_port}"

    for _attempt in range(120):
        try:
            urllib.request.urlopen(f"{sr_url}/subjects", timeout=2)  # noqa: S310
            break
        except Exception:
            time.sleep(1)
    else:
        # Capture SR logs for diagnostics before stopping
        try:
            logs = sr.get_logs()
            log_tail = (logs[0] or b"")[-500:].decode("utf-8", errors="replace")
        except Exception:
            log_tail = "(could not retrieve logs)"
        sr.stop()
        pytest.fail(f"Schema Registry did not become healthy in 120s. Logs: {log_tail}")

    yield sr
    sr.stop()


# ---------------------------------------------------------------------------
# Convenience address fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def bootstrap_servers(kafka_container) -> str:
    """Return the external bootstrap server address for the Kafka container."""
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="session")
def schema_registry_url(sr_container) -> str:
    """Return the HTTP URL for the Schema Registry container."""
    sr_host = sr_container.get_container_host_ip()
    sr_port = sr_container.get_exposed_port(8081)
    return f"http://{sr_host}:{sr_port}"


# ---------------------------------------------------------------------------
# KafkaMcpSettings & KafkaClient
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kafka_settings(bootstrap_servers, schema_registry_url):
    """Build KafkaMcpSettings wired to the real testcontainer endpoints."""
    from kafka_mcp.config import KafkaMcpSettings

    return KafkaMcpSettings(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=schema_registry_url,
        poll_timeout=5.0,  # containers are slower; generous timeout
    )


@pytest.fixture(scope="session")
def kafka_client(kafka_settings):
    """Build a KafkaClient wired to real broker + real Schema Registry."""
    from kafka_mcp.adapters.inbound.lib import KafkaClient
    from kafka_mcp.adapters.outbound.confluent_consumer import ConfluentConsumerAdapter
    from kafka_mcp.adapters.outbound.schema_registry_http import SchemaRegistryHttpAdapter

    consumer = ConfluentConsumerAdapter(kafka_settings)
    registry = SchemaRegistryHttpAdapter(
        url=kafka_settings.schema_registry_url,
    )
    client = KafkaClient(consumer, registry)
    yield client
    client.close()


# ---------------------------------------------------------------------------
# Test data seeding
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def seed_json_topic(bootstrap_servers) -> str:
    """Seed a 'test-json' topic with 5 plain JSON messages.

    Uses confluent_kafka.Producer directly. All test data is synthetic
    (T-06-03: no PII, ephemeral containers destroyed after session).
    """
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap_servers})

    topic = "test-json"
    for i in range(5):
        value = json.dumps({"order_id": f"ORD-{i}", "amount": 100 + i, "customer_id": f"CUST-{i}"}).encode()
        producer.produce(
            topic=topic,
            key=f"key-{i}".encode(),
            value=value,
        )

    remaining = producer.flush(timeout=10)
    assert remaining == 0, f"Producer flush timed out with {remaining} messages pending"

    # Allow broker metadata propagation
    time.sleep(2)

    return topic
