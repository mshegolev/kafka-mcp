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
- [ ] **KAFKA-02**: `search_messages` по ключу + временное окно
- [ ] **KAFKA-03**: `get_message` по topic/partition/offset
- [x] **KAFKA-04**: `describe_topic` (партиции, offset'ы) — validated in Phase 1
- [ ] **KAFKA-05**: декод Avro/Protobuf/JSON через Schema Registry
- [x] **KAFKA-06**: read-only — временный consumer-group, ограниченный скан, без produce — validated in Phase 1
- [ ] **KAFKA-07**: Rust-сканер включается ТОЛЬКО после профайлинга (CPU-bound доказан)

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

---
*Brick brief — запускай агента здесь и реализуй методы из Investigator Contract.*
