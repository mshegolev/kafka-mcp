# kafka-mcp — brick project

> Part of **Investigation MCP Suite** (umbrella: `/opt/develop/aiqa/investigate-suite/`).
> Full design spec: `/opt/develop/aiqa/docs/superpowers/specs/2026-06-05-investigation-mcp-suite-design.md` (§4.1).
> Copies the **v2 hexagonal skeleton** from `graphql-mcp`.

## What This Is

Генерик read-only Kafka MCP-кирпич: поиск событий по ключу (msisdn/customerId/productId/
orderId) во временном окне + вытягивание тел сообщений. Library-first:
`from kafka_mcp import KafkaClient` работает в pytest без MCP/FastAPI.

## Architecture (hexagonal v2 — копия graphql-mcp)

```
src/kafka_mcp/
  domain/   models.py errors.py search_service.py            # чистое
  ports/    consumer.py(broker) schema_registry.py json_codec.py
  adapters/
    inbound/  rest_api.py(FastAPI) mcp_stdio.py lib.py(KafkaClient) cli.py
    outbound/ confluent_consumer.py schema_registry_http.py json_native.py json_orjson.py
  config.py server.py
native/     Rust pyo3 — СКАН ПАРТИЦИЙ (реальный CPU-hotspot, включать после бенча)
```

Стек: `mcp>=1.27`, `confluent-kafka>=2.14` (librdkafka; **НЕ kafka-python**), Schema Registry
(Avro/Protobuf/JSON), FastAPI+uvicorn, orjson, pydantic v2; build maturin+pyo3; CI cibuildwheel.

## Requirements (v1) — KAFKA-01..07

- [x] **KAFKA-01**: `list_topics` — validated in Phase 1 (Foundation)
- [x] **KAFKA-02**: `search_messages` по ключу + временное окно — validated in Phase 2
- [x] **KAFKA-03**: `get_message` по topic/partition/offset — validated in Phase 2
- [x] **KAFKA-04**: `describe_topic` (партиции, offset'ы) — validated in Phase 1
- [x] **KAFKA-05**: декод Avro/Protobuf/JSON через Schema Registry — validated in Phase 2
- [x] **KAFKA-06**: read-only — временный consumer-group, ограниченный скан, без produce — validated in Phase 1
- [x] **KAFKA-07**: Rust-сканер включается ТОЛЬКО после профайлинга (CPU-bound доказан)

Anti: produce, consumer-group mgmt, exactly-once, broker config.

## ⭐ Investigator Contract (lib-фасад)

```python
from kafka_mcp import KafkaClient
c = KafkaClient.from_env()

list[str] = c.list_topics()
list[KafkaMessage] = c.search_messages(
    key: str, *, key_field: str | None = None,        # key_field: где искать (key/header/value path)
    topics: list[str] | None = None,
    time_from: datetime | None = None, time_to: datetime | None = None,
    limit: int = 500)
KafkaMessage = c.get_message(topic: str, partition: int, offset: int)
TopicInfo = c.describe_topic(topic: str)

# KafkaMessage{ topic, partition, offset, key, headers: dict,
#               value: dict,            # декодированное тело
#               timestamp_utc: datetime, raw }
```

**Зачем investigator'у:** `search_messages(key=orderId|msisdn|customerId|productId)` в окне
инцидента — каждое сообщение становится событием на timeline (что мы отправили / что
инициировало действия сервисов).

**Обязательно surface для Evidence:**
- `timestamp_utc` сообщения (UTC) — критично для timeline (см. pitfall clock-skew).
- `key` + идентификаторы из `headers`/`value` (order_id, msisdn, customer_id, product_id, trace_id если есть).
- декодированный `value` (payload) + `raw` для drill-down.
- read-only гарантия: временный consumer-group, без коммита offset'ов в прод-группы.

`Evidence{ source="kafka", event_type="kafka_message", timestamp_utc, keys{...}, payload=value, raw }`

## Build order
Скопировать скелет graphql-mcp → consumer/schema-registry адаптеры → domain search →
**сначала pure-Python скан, бенч, потом Rust** (pitfall: преждевременный Rust) → lib→mcp→FastAPI→cli
→ tests (read-only, temp group, format decode) → CI-wheels → Glama.

## Decisions: наследуются от зонтика (D1/D2/D5/D7/D8/D9).

### KAFKA-07 [Phase 3, Plan 01]: Rust scanner NOT added — I/O-bound benchmark result

**Date:** 2026-06-08
**Gate:** pytest-benchmark pedantic mode on pure-Python `scan_partition` hot loop.
**Measured baseline (arm64, Python 3.10.4, pytest-benchmark 5.2.3):**
- 100-msg scan: ~4.78 µs total (~48 ns/msg) — key compare + evidence extract
- 10,000-msg scan: ~502 µs total (~50 ns/msg)
- 1,000-msg scan + orjson decode: ~479 µs total (~479 ns/msg)

**Decision:** Rust scanner NOT added. Benchmark confirms the hot path is
I/O-bound: `librdkafka poll()` network round-trips (1–10 ms/batch) dominate;
CPU work (48–479 ns/msg) is negligible. Gate condition (≥2× CPU-bound speedup
achievable via Rust pyo3) is NOT met. Pure-Python scanner (`scanner.py`) is the
permanent v1 implementation. Scanner seam (try-import `kafka_mcp._native`) keeps
the option open for a future Rust drop-in without API change.

**Evidence:** `EVALUATION.md` at repo root; `.benchmark_result.json` (ephemeral).

---

## Current State (shipped v1.0 — 2026-06-08)

v1.0 MVP is complete and verified. All 7 requirements (KAFKA-01..07) validated;
3 phases, 12 plans, 205 passing tests, ruff clean. The brick exposes
`list_topics`, `describe_topic`, `search_messages`, and `get_message` identically
across four faces (lib `KafkaClient`, MCP stdio, FastAPI `/tools/*`, `kafka-mcp`
CLI), with structural read-only enforcement, Avro/Protobuf/JSON decode via Schema
Registry, and the Investigator-Contract Evidence fields. ~3,400 LOC src /
~4,100 LOC tests (Python 3.10–3.12, hatchling). Distribution artifacts
(glama.json, server.json, MIT LICENSE, CHANGELOG, GitHub Actions CI) are prepared;
the live PyPI/Glama publish is gated on a human-triggered tagged release.

**Validated requirements:** KAFKA-01 (list_topics), KAFKA-04 (describe_topic),
KAFKA-06 (read-only) — v1.0 Phase 1; KAFKA-02 (search_messages), KAFKA-03
(get_message), KAFKA-05 (decode) — v1.0 Phase 2; KAFKA-07 (benchmark-gated Rust,
resolved to pure-Python) — v1.0 Phase 3.

## Next Milestone Goals (candidates for v1.1+)

- Live PyPI publish + Glama submission via the tagged-release CI workflow.
- Real-broker integration/E2E test contour (current suite is mock-based by design).
- Optional: decode message keys via Schema Registry; expose `schema_id`; HTTP
  transport in server.json. Rust scanner remains gated on a future CPU-bound
  benchmark result (not anticipated).

---
*Brick brief — запускай агента здесь и реализуй методы из Investigator Contract.*

*Last updated: 2026-06-08 after v1.0 milestone*
