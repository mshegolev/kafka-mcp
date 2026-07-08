# kafka-mcp

<!-- mcp-name: io.github.mshegolev/kafka-mcp -->

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Read-only Kafka MCP brick — investigate events by key within a time window.**

Give Claude (or any MCP-capable agent), a GitLab CI job, or a pytest suite read
access to Kafka topics — list topics, describe partitions/offsets, fetch a single
message, and search messages by business key (msisdn / customerId / productId /
orderId) within a time window, with Schema Registry decode (Avro / Protobuf /
JSON) — without ever producing or mutating anything.

Part of the [Investigation MCP Suite](https://github.com/mshegolev) — a set of
reusable hexagonal MCP bricks consumed identically from an AI agent (stdio/MCP),
an HTTP service (FastAPI), a library import (`KafkaClient`), and a CLI.

## Why

Cross-system incident investigation ("a trace errored — what happened on the
bus?") usually means hand-rolling a consumer. This brick makes it deterministic
and safe:

- **Read-only by guarantee.** No produce call, no persistent consumer-group side
  effect. The scan uses a temporary group and is bounded by configured limits.
  All four tools carry `readOnlyHint=true`.
- **Key + time-window search.** Find messages by exact business key inside a
  bounded time range — the dominant investigation pattern.
- **Schema Registry decode.** Avro, Protobuf, and JSON are auto-detected and
  decoded via the Schema Registry; raw bytes never leak to the caller.
- **Four faces, one core.** Hexagonal (`domain/` + `ports/` +
  `adapters/{inbound,outbound}`): the same operations from lib, stdio, FastAPI,
  and CLI.
- **Pure-Python, no toolchain required.** `pip install` and `pytest` work with no
  Rust toolchain. (A native partition scanner is gated behind a benchmark — see
  [`EVALUATION.md`](EVALUATION.md); the current decision is pure-Python.)

## Tools

| Tool | Description |
|------|-------------|
| `list_topics` | List broker topics (optionally including internal topics). |
| `describe_topic` | Partition count and current low/high offsets for a topic. |
| `get_message` | Retrieve a single message by `topic` / `partition` / `offset`. |
| `search_messages` | Find messages by exact key within a time window; decoded via Schema Registry. |
| `consumer_group_lag` | Report per-partition consumer lag (committed vs end offset) for a consumer group. |
| `correlate_messages` | Correlate messages across topics by following extracted IDs with advanced pattern matching. |

All tools are read-only (`readOnlyHint=true`, plus `idempotentHint=true` and
`openWorldHint=true`).

## Install

```bash
pip install kafka-events-mcp
# or, for local development:
uv sync --extra dev
```

## Configure

All settings come from environment variables prefixed `KAFKA_MCP_`:

```bash
export KAFKA_MCP_BOOTSTRAP_SERVERS=broker1:9092,broker2:9092
# Optional auth (omit for PLAINTEXT):
export KAFKA_MCP_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_MCP_SASL_MECHANISM=PLAIN
export KAFKA_MCP_SASL_USERNAME=alice
export KAFKA_MCP_SASL_PASSWORD=secret
# Optional Schema Registry:
export KAFKA_MCP_SCHEMA_REGISTRY_URL=https://sr.internal:8081
```

`KAFKA_MCP_BOOTSTRAP_SERVERS` is required; a missing/empty value raises an
actionable configuration error naming the variable.

### mTLS (client-certificate TLS)

For brokers that require mutual TLS, authenticate with a client certificate
instead of (or alongside) SASL. Set `KAFKA_MCP_SECURITY_PROTOCOL=SSL` and point
the four `KAFKA_MCP_SSL_*` variables at your certificate material:

```bash
# mTLS: client-certificate TLS to the broker.
export KAFKA_MCP_BOOTSTRAP_SERVERS=broker1:9093,broker2:9093
export KAFKA_MCP_SECURITY_PROTOCOL=SSL
# Client certificate (PEM) presented to the broker:
export KAFKA_MCP_SSL_CERTIFICATE_LOCATION=/run/secrets/kafka/client.crt.pem
# Client private key (PEM) matching the certificate above:
export KAFKA_MCP_SSL_KEY_LOCATION=/run/secrets/kafka/client.key.pem
# CA certificate (PEM) used to verify the broker's server certificate:
export KAFKA_MCP_SSL_CA_LOCATION=/run/secrets/kafka/ca.pem
# Passphrase protecting the private key (omit if the key is unencrypted):
export KAFKA_MCP_SSL_KEY_PASSWORD=changeit
```

`KAFKA_MCP_SECURITY_PROTOCOL=SSL` is what activates mTLS — the four
`KAFKA_MCP_SSL_*` values are wired into librdkafka's `ssl.*` conf on **both** the
consumer path (topic/message reads) and the admin/lag path
(`consumer_group_lag`), so every broker connection presents the same client
certificate. `KAFKA_MCP_SSL_KEY_PASSWORD` is held as a Pydantic `SecretStr`,
unwrapped only into the local client conf and never logged or echoed. mTLS and
SASL are independent: use `SECURITY_PROTOCOL=SSL` for certificate-only auth, or
`SASL_SSL` plus the `KAFKA_MCP_SASL_*` variables for SASL over TLS.

> **Security caution.** Never commit real certificates, private keys, or the key
> passphrase to source control. Point `KAFKA_MCP_SSL_CERTIFICATE_LOCATION`,
> `KAFKA_MCP_SSL_KEY_LOCATION`, and `KAFKA_MCP_SSL_CA_LOCATION` at mounted secret
> paths (e.g. Kubernetes secret mounts or Docker secrets), and inject
> `KAFKA_MCP_SSL_KEY_PASSWORD` from your secret store at runtime.

## Run

```bash
# stdio (Claude Desktop / Claude Code / Cursor / Glama):
kafka-mcp

# FastAPI HTTP service:
kafka-mcp serve         # see --help for host/port

# CLI subcommands:
kafka-mcp list-topics
kafka-mcp search-messages --key 79991234567 --since 2026-06-01T00:00:00Z
kafka-mcp correlate-messages --key order-123 --follow-topics payments,shipments --bidirectional
```

### Advanced Correlation Features

The `correlate_messages` tool supports advanced correlation patterns:

```bash
# Regex pattern matching for ID extraction
kafka-mcp correlate-messages --key order-123 \
  --follow-topics payments,shipments \
  --regex-patterns '"traceId":"([^"]+)"'

# Bidirectional correlation traversal
kafka-mcp correlate-messages --key error-456 \
  --follow-topics orders,payments,shipments \
  --bidirectional

# Limit correlation depth and breadth
kafka-mcp correlate-messages --key order-123 \
  --follow-topics orders,payments,shipments \
  --max-depth 5 --max-breadth 10
```

### Library import

```python
from kafka_mcp.adapters.inbound.lib import KafkaClient

client = KafkaClient.from_env()
topics = client.list_topics()
info = client.describe_topic("orders.events")
```

## Development

```bash
uv sync --extra dev
uv run pytest                                   # full suite
uv run pytest tests/benchmarks/ --benchmark-only  # scanner benchmark
uv run ruff check src/
```

## License

[MIT](LICENSE)
