"""Testcontainers fixtures for real-broker integration tests.

Session-scoped: containers start once per test session, shared across all
integration tests. Tests skip gracefully when Docker is unavailable.

Fixtures:
    kafka_container     — running KafkaContainer (session)
    sr_container        — running Schema Registry DockerContainer (session)
    bootstrap_servers   — broker address string
    schema_registry_url — SR HTTP URL string
    kafka_settings      — KafkaMcpSettings wired to real containers
    kafka_client        — KafkaClient wired to real broker + real SR
    seed_json_topic     — seeds a "test-json" topic with 5 plain JSON messages
    sr_client           — SchemaRegistryClient for schema registration
    seed_avro_topic     — seeds a "test-avro" topic with 3 Avro-encoded messages
    seed_protobuf_topic — seeds a "test-proto" topic with 3 Protobuf-framed messages
    seed_avro_key_topic — seeds a "test-avro-key" topic with 2 Avro-encoded key+value messages
    seed_lag_consumer   — creates a consumer group with partial offset commits for lag tests
"""

from __future__ import annotations

import json
import os
import struct
import subprocess
import tempfile
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


# ---------------------------------------------------------------------------
# Schema Registry client fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def sr_client(schema_registry_url):
    """Return a SchemaRegistryClient for schema registration in seed fixtures."""
    from confluent_kafka.schema_registry import SchemaRegistryClient

    return SchemaRegistryClient({"url": schema_registry_url})


# ---------------------------------------------------------------------------
# Avro-encoded topic seed
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def seed_avro_topic(bootstrap_servers, sr_client):
    """Seed a 'test-avro' topic with 3 Avro-encoded messages.

    Uses AvroSerializer to produce messages with real Confluent wire framing
    (magic byte + schema_id + Avro payload) registered against the live SR.
    """
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    avro_schema_str = json.dumps(
        {
            "type": "record",
            "name": "TestOrder",
            "namespace": "com.test",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "amount", "type": "int"},
                {"name": "customer_id", "type": "string"},
            ],
        }
    )

    avro_serializer = AvroSerializer(sr_client, avro_schema_str)
    producer = Producer({"bootstrap.servers": bootstrap_servers})

    topic = "test-avro"
    for i in range(3):
        value = {"order_id": f"AVRO-ORD-{i}", "amount": 200 + i, "customer_id": f"CUST-{i}"}
        serialized = avro_serializer(value, SerializationContext(topic, MessageField.VALUE))
        producer.produce(topic, key=f"avro-key-{i}".encode(), value=serialized)

    remaining = producer.flush(timeout=10)
    assert remaining == 0, f"Avro producer flush timed out with {remaining} messages pending"
    time.sleep(2)

    return topic


# ---------------------------------------------------------------------------
# Protobuf-framed topic seed
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def seed_protobuf_topic(bootstrap_servers, sr_client):
    """Seed a 'test-proto' topic with 3 Protobuf-framed messages.

    Manually constructs Confluent Protobuf wire format:
    magic(0x00) + schema_id(4B BE) + message_index(0x00) + proto payload.

    Uses grpc_tools.protoc to compile the schema and google.protobuf to
    serialize, mirroring the decode path in SchemaRegistryHttpAdapter.
    """
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry import Schema
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
    from grpc_tools import protoc as _protoc

    proto_schema_str = (
        'syntax = "proto3";\n'
        "package com.test;\n"
        "message TestEvent {\n"
        "    string event_id = 1;\n"
        "    int32 priority = 2;\n"
        "    string description = 3;\n"
        "}\n"
    )

    # Register schema in SR
    schema = Schema(proto_schema_str, schema_type="PROTOBUF")
    schema_id = sr_client.register_schema("test-proto-value", schema)

    # Compile .proto to FileDescriptorSet using in-process protoc
    with tempfile.TemporaryDirectory() as tmpdir:
        proto_path = os.path.join(tmpdir, "schema.proto")
        out_path = os.path.join(tmpdir, "schema.desc")
        _wkt_proto_path = os.path.join(os.path.dirname(_protoc.__file__), "_proto")
        with open(proto_path, "w") as f:
            f.write(proto_schema_str)
        rc = _protoc.main(
            [
                "protoc",
                f"--proto_path={tmpdir}",
                f"--proto_path={_wkt_proto_path}",
                f"--descriptor_set_out={out_path}",
                "--include_imports",
                proto_path,
            ]
        )
        assert rc == 0, f"protoc failed with rc={rc}"
        with open(out_path, "rb") as f:
            fds = descriptor_pb2.FileDescriptorSet()
            fds.ParseFromString(f.read())

    pool = descriptor_pool.DescriptorPool()
    file_desc = None
    for file_proto in fds.file:
        file_desc = pool.Add(file_proto)
    assert file_desc is not None, "protoc produced empty descriptor set"
    msg_desc = list(file_desc.message_types_by_name.values())[0]
    MsgClass = message_factory.GetMessageClass(msg_desc)

    # Produce 3 messages with Confluent Protobuf wire framing
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    topic = "test-proto"
    for i in range(3):
        msg = MsgClass()
        msg.event_id = f"EVT-{i}"
        msg.priority = 10 + i
        msg.description = f"Test event {i}"
        payload = msg.SerializeToString()
        # Confluent Protobuf wire: magic(0x00) + schema_id(4B BE) + msg_index(0x00) + payload
        framed = struct.pack(">bI", 0, schema_id) + b"\x00" + payload
        producer.produce(topic, key=f"proto-key-{i}".encode(), value=framed)

    remaining = producer.flush(timeout=10)
    assert remaining == 0, f"Proto producer flush timed out with {remaining} messages pending"
    time.sleep(2)

    return topic


# ---------------------------------------------------------------------------
# Avro-encoded key topic seed (KEY-01/KEY-02)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def seed_avro_key_topic(bootstrap_servers, sr_client):
    """Seed a 'test-avro-key' topic with 2 messages having Avro-encoded keys.

    Both key and value are Avro-serialized with Confluent wire framing.
    Tests KEY-01 (key_decoded populated) and KEY-02 (schema_id.key populated).
    """
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    key_schema_str = json.dumps(
        {
            "type": "record",
            "name": "OrderKey",
            "namespace": "com.test",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "region", "type": "string"},
            ],
        }
    )

    value_schema_str = json.dumps(
        {
            "type": "record",
            "name": "TestOrder",
            "namespace": "com.test.keyed",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "amount", "type": "int"},
            ],
        }
    )

    key_serializer = AvroSerializer(sr_client, key_schema_str)
    value_serializer = AvroSerializer(sr_client, value_schema_str)
    producer = Producer({"bootstrap.servers": bootstrap_servers})

    topic = "test-avro-key"
    for i in range(2):
        key_val = {"order_id": f"KEY-ORD-{i}", "region": "EU"}
        value_val = {"order_id": f"KEY-ORD-{i}", "amount": 500 + i}
        ser_key = key_serializer(key_val, SerializationContext(topic, MessageField.KEY))
        ser_value = value_serializer(value_val, SerializationContext(topic, MessageField.VALUE))
        producer.produce(topic, key=ser_key, value=ser_value)

    remaining = producer.flush(timeout=10)
    assert remaining == 0, f"Avro-key producer flush timed out with {remaining} messages pending"
    time.sleep(2)

    return topic


# ---------------------------------------------------------------------------
# Lag consumer seed (LAG-01/LAG-03)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def seed_lag_consumer(bootstrap_servers, seed_json_topic):
    """Create a consumer group with partial offset commits for lag tests.

    Consumes 2 of 5 messages from seed_json_topic, commits, then closes.
    This creates a 'test-lag-group' with committed offsets and lag > 0.
    """
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "test-lag-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([seed_json_topic])

    # Consume 2 of 5 messages, then commit
    consumed = 0
    for _ in range(20):  # poll up to 20 times (generous for slow containers)
        msg = consumer.poll(timeout=5.0)
        if msg is None:
            continue
        if msg.error():
            continue
        consumed += 1
        if consumed >= 2:
            break

    assert consumed >= 2, f"Only consumed {consumed} messages, expected at least 2"
    consumer.commit(asynchronous=False)
    consumer.close()
    time.sleep(1)

    return "test-lag-group"
