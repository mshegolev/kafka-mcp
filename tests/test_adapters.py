"""No-broker adapter test suite.

All tests run without a running Kafka broker — confluent_kafka.Consumer
is patched with unittest.mock.MagicMock throughout.

Covers:
- ConfluentConsumerAdapter: internal-topic filtering, watermark offsets,
  context-manager cleanup, assign-only read-only guarantee
- SchemaRegistryHttpAdapter: stub returns None when url is None
- orjson helpers: encode/decode round-trip
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kafka_mcp.ports.consumer import ConsumerPort
from kafka_mcp.ports.schema_registry import SchemaRegistryPort

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(bootstrap_servers: str = "localhost:9092") -> object:
    """Return a minimal KafkaMcpSettings instance for adapter tests."""
    from kafka_mcp.config import KafkaMcpSettings

    return KafkaMcpSettings(bootstrap_servers=bootstrap_servers)


def _make_metadata_mock(topic_names: list[str]) -> MagicMock:
    """Build a metadata mock whose .topics dict mirrors confluent_kafka shape."""
    meta = MagicMock()
    # metadata.topics is dict[str, TopicMetadata-like]; we only need the keys
    meta.topics = {name: MagicMock(topic=name) for name in topic_names}
    return meta


# ---------------------------------------------------------------------------
# ConfluentConsumerAdapter — list_topics
# ---------------------------------------------------------------------------


class TestConfluentConsumerAdapterListTopics:
    """list_topics filtering and sorting behaviour."""

    def _make_adapter(self, mock_consumer: MagicMock) -> object:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        settings = _make_settings()
        with (
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
                return_value=mock_consumer,
            ),
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
                return_value=MagicMock(),
            ),
        ):
            return ConfluentConsumerAdapter(settings)

    def test_list_topics_excludes_internal_by_default(self) -> None:
        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_metadata_mock(
            ["__consumer_offsets", "payments", "__transaction_state", "orders"]
        )
        adapter = self._make_adapter(mock_consumer)
        result = adapter.list_topics(include_internal=False)
        assert result == ["orders", "payments"]

    def test_list_topics_include_internal_true(self) -> None:
        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_metadata_mock(["__consumer_offsets", "payments"])
        adapter = self._make_adapter(mock_consumer)
        result = adapter.list_topics(include_internal=True)
        assert "__consumer_offsets" in result
        assert "payments" in result

    def test_list_topics_default_excludes_internal(self) -> None:
        """list_topics() with no args defaults to include_internal=False."""
        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_metadata_mock(["__consumer_offsets", "payments"])
        adapter = self._make_adapter(mock_consumer)
        result = adapter.list_topics()
        assert result == ["payments"]

    def test_list_topics_returns_sorted(self) -> None:
        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_metadata_mock(["zebra", "apple", "mango"])
        adapter = self._make_adapter(mock_consumer)
        result = adapter.list_topics()
        assert result == ["apple", "mango", "zebra"]

    def test_list_topics_empty_broker(self) -> None:
        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_metadata_mock([])
        adapter = self._make_adapter(mock_consumer)
        assert adapter.list_topics() == []


# ---------------------------------------------------------------------------
# ConfluentConsumerAdapter — get_watermark_offsets
# ---------------------------------------------------------------------------


class TestConfluentConsumerAdapterWatermarks:
    """get_watermark_offsets delegation and error handling."""

    def _make_adapter(self, mock_consumer: MagicMock) -> object:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        settings = _make_settings()
        with (
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
                return_value=mock_consumer,
            ),
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
                return_value=MagicMock(),
            ),
        ):
            return ConfluentConsumerAdapter(settings)

    def test_get_watermark_offsets_returns_tuple(self) -> None:
        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 100)
        adapter = self._make_adapter(mock_consumer)
        low, high = adapter.get_watermark_offsets("payments", 0)
        assert low == 0
        assert high == 100

    def test_get_watermark_offsets_delegates_to_consumer(self) -> None:
        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (42, 999)
        adapter = self._make_adapter(mock_consumer)
        result = adapter.get_watermark_offsets("orders", 2)
        assert result == (42, 999)
        mock_consumer.get_watermark_offsets.assert_called_once()

    def test_get_watermark_offsets_raises_topic_not_found(self) -> None:
        from confluent_kafka import KafkaError, KafkaException

        from kafka_mcp.domain.errors import TopicNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.side_effect = KafkaException(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))
        adapter = self._make_adapter(mock_consumer)
        with pytest.raises(TopicNotFoundError) as exc_info:
            adapter.get_watermark_offsets("nonexistent", 0)
        assert exc_info.value.topic == "nonexistent"

    def test_get_watermark_offsets_unknown_partition_is_not_found(
        self,
    ) -> None:
        """Unknown-partition codes also map to TopicNotFoundError (WR-04)."""
        from confluent_kafka import KafkaError, KafkaException

        from kafka_mcp.domain.errors import TopicNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.side_effect = KafkaException(KafkaError(KafkaError._UNKNOWN_PARTITION))
        adapter = self._make_adapter(mock_consumer)
        with pytest.raises(TopicNotFoundError):
            adapter.get_watermark_offsets("payments", 99)

    def test_get_watermark_offsets_transient_error_reraised(self) -> None:
        """Transient/operational KafkaExceptions must NOT become 404 (WR-04).

        A broker outage / transport error / timeout should surface as the
        original KafkaException, not as TopicNotFoundError.
        """
        from confluent_kafka import KafkaError, KafkaException

        from kafka_mcp.domain.errors import TopicNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.side_effect = KafkaException(KafkaError(KafkaError._TRANSPORT))
        adapter = self._make_adapter(mock_consumer)
        with pytest.raises(KafkaException):
            adapter.get_watermark_offsets("payments", 0)
        # And specifically NOT mistranslated to "not found".
        mock_consumer.get_watermark_offsets.side_effect = KafkaException(KafkaError(KafkaError._TRANSPORT))
        with pytest.raises(KafkaException):
            try:
                adapter.get_watermark_offsets("payments", 0)
            except TopicNotFoundError:  # pragma: no cover - regression guard
                pytest.fail("transient error mistranslated to TopicNotFound")

    def test_get_watermark_offsets_uses_metadata_timeout(self) -> None:
        """WR-03: watermark fetch uses the 10s metadata budget, not poll_timeout."""
        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 10)
        adapter = self._make_adapter(mock_consumer)
        adapter.get_watermark_offsets("payments", 0)
        _args, kwargs = mock_consumer.get_watermark_offsets.call_args
        # poll_timeout default is 1.0; the metadata budget is 10.0.
        assert kwargs.get("timeout") == 10.0


# ---------------------------------------------------------------------------
# ConfluentConsumerAdapter — get_partition_ids
# ---------------------------------------------------------------------------


def _make_topic_metadata(topic: str, *, partitions: list[int] | None = None, error: object = None) -> MagicMock:
    """Build a list_topics(topic=...) result mock for a single topic.

    metadata.topics is dict[str, TopicMetadata-like]; each TopicMetadata has
    ``.error`` (None or a KafkaError) and ``.partitions`` (dict keyed by id).
    Pass ``partitions=None`` to omit the topic entirely (broker returned no
    metadata for it).
    """
    meta = MagicMock()
    if partitions is None and error is None:
        meta.topics = {}
        return meta
    topic_meta = MagicMock()
    topic_meta.error = error
    topic_meta.partitions = {pid: MagicMock(id=pid) for pid in (partitions or [])}
    meta.topics = {topic: topic_meta}
    return meta


class TestConfluentConsumerAdapterGetPartitionIds:
    """get_partition_ids: returns sorted ids; discriminates not-found vs transient (WR-01)."""

    def _make_adapter(self, mock_consumer: MagicMock) -> object:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        settings = _make_settings()
        with (
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
                return_value=mock_consumer,
            ),
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
                return_value=MagicMock(),
            ),
        ):
            return ConfluentConsumerAdapter(settings)

    def test_returns_sorted_partition_ids(self) -> None:
        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_topic_metadata("payments", partitions=[2, 0, 1])
        adapter = self._make_adapter(mock_consumer)
        assert adapter.get_partition_ids("payments") == [0, 1, 2]

    def test_uses_metadata_timeout(self) -> None:
        """WR-03/WR-01: per-topic metadata fetch uses the 10s budget."""
        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_topic_metadata("payments", partitions=[0])
        adapter = self._make_adapter(mock_consumer)
        adapter.get_partition_ids("payments")
        _args, kwargs = mock_consumer.list_topics.call_args
        assert kwargs.get("timeout") == 10.0
        assert kwargs.get("topic") == "payments"

    def test_missing_topic_metadata_is_not_found(self) -> None:
        from kafka_mcp.domain.errors import TopicNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_topic_metadata("ghost")
        adapter = self._make_adapter(mock_consumer)
        with pytest.raises(TopicNotFoundError) as exc_info:
            adapter.get_partition_ids("ghost")
        assert exc_info.value.topic == "ghost"

    def test_unknown_topic_error_code_is_not_found(self) -> None:
        from confluent_kafka import KafkaError

        from kafka_mcp.domain.errors import TopicNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_topic_metadata(
            "ghost", error=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)
        )
        adapter = self._make_adapter(mock_consumer)
        with pytest.raises(TopicNotFoundError):
            adapter.get_partition_ids("ghost")

    def test_transient_error_code_reraised(self) -> None:
        """WR-01: a transient TopicMetadata.error must NOT become 404."""
        from confluent_kafka import KafkaError, KafkaException

        from kafka_mcp.domain.errors import TopicNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_topic_metadata(
            "payments", error=KafkaError(KafkaError.LEADER_NOT_AVAILABLE)
        )
        adapter = self._make_adapter(mock_consumer)
        with pytest.raises(KafkaException):
            try:
                adapter.get_partition_ids("payments")
            except TopicNotFoundError:  # pragma: no cover - regression guard
                pytest.fail("transient error mistranslated to TopicNotFound")


def _make_topic_metadata_with_leaders(topic: str, leaders: dict[int, int]) -> MagicMock:
    """list_topics(topic=...) result where each partition carries a real leader."""
    meta = MagicMock()
    topic_meta = MagicMock()
    topic_meta.error = None
    topic_meta.partitions = {
        pid: MagicMock(id=pid, leader=leader) for pid, leader in leaders.items()
    }
    meta.topics = {topic: topic_meta}
    return meta


class TestConfluentConsumerAdapterGetPartitionLeaders:
    """get_partition_leaders: real leader broker ids from cluster metadata (P3)."""

    def _make_adapter(self, mock_consumer: MagicMock) -> object:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        settings = _make_settings()
        with (
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
                return_value=mock_consumer,
            ),
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
                return_value=MagicMock(),
            ),
        ):
            return ConfluentConsumerAdapter(settings)

    def test_returns_partition_to_leader_map(self) -> None:
        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_topic_metadata_with_leaders(
            "payments", {0: 7, 1: 3, 2: 7}
        )
        adapter = self._make_adapter(mock_consumer)
        assert adapter.get_partition_leaders("payments") == {0: 7, 1: 3, 2: 7}

    def test_missing_topic_returns_empty_map(self) -> None:
        """Best-effort: absent topic yields {} (get_partition_ids owns 404)."""
        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_topic_metadata("ghost")
        adapter = self._make_adapter(mock_consumer)
        assert adapter.get_partition_leaders("ghost") == {}

    def test_transient_error_returns_empty_map(self) -> None:
        """A transient metadata error must not fail describe_topic over leader."""
        from confluent_kafka import KafkaError

        mock_consumer = MagicMock()
        mock_consumer.list_topics.return_value = _make_topic_metadata(
            "payments", error=KafkaError(KafkaError.LEADER_NOT_AVAILABLE)
        )
        adapter = self._make_adapter(mock_consumer)
        assert adapter.get_partition_leaders("payments") == {}


# ---------------------------------------------------------------------------
# ConfluentConsumerAdapter — context manager
# ---------------------------------------------------------------------------


class TestConfluentConsumerAdapterContextManager:
    """Context manager calls Consumer.close() exactly once on exit."""

    def test_context_manager_calls_close(self) -> None:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        mock_consumer = MagicMock()
        settings = _make_settings()
        with (
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
                return_value=mock_consumer,
            ),
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
                return_value=MagicMock(),
            ),
        ):
            adapter = ConfluentConsumerAdapter(settings)

        with adapter:
            pass

        mock_consumer.close.assert_called_once()

    def test_context_manager_calls_close_on_exception(self) -> None:
        """close() is called even when an exception occurs inside the block."""
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        mock_consumer = MagicMock()
        settings = _make_settings()
        with (
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
                return_value=mock_consumer,
            ),
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
                return_value=MagicMock(),
            ),
        ):
            adapter = ConfluentConsumerAdapter(settings)

        with pytest.raises(RuntimeError):
            with adapter:
                raise RuntimeError("test error")

        mock_consumer.close.assert_called_once()


# ---------------------------------------------------------------------------
# ConfluentConsumerAdapter — Protocol compliance
# ---------------------------------------------------------------------------


class TestConfluentConsumerAdapterProtocol:
    """Adapter satisfies ConsumerPort runtime_checkable Protocol."""

    def test_isinstance_consumer_port(self) -> None:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        mock_consumer = MagicMock()
        settings = _make_settings()
        with (
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
                return_value=mock_consumer,
            ),
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
                return_value=MagicMock(),
            ),
        ):
            adapter = ConfluentConsumerAdapter(settings)

        assert isinstance(adapter, ConsumerPort)

    def test_no_subscribe_in_source(self) -> None:
        """subscribe() must not appear in the adapter source (KAFKA-06 / T-02-03)."""
        import inspect

        from kafka_mcp.adapters.outbound import confluent_consumer

        source = inspect.getsource(confluent_consumer)
        non_comment_lines = [
            line for line in source.splitlines() if "subscribe" in line and not line.strip().startswith("#")
        ]
        assert non_comment_lines == [], f"subscribe() found in adapter source (KAFKA-06 violation): {non_comment_lines}"


# ---------------------------------------------------------------------------
# ConfluentConsumerAdapter — config dict
# ---------------------------------------------------------------------------


class TestConfluentConsumerAdapterConfig:
    """librdkafka config dict always has auto.commit=False and uuid4 group.id."""

    def test_config_contains_auto_commit_false(self) -> None:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        captured_conf: dict = {}

        def fake_consumer(conf: dict) -> MagicMock:
            captured_conf.update(conf)
            return MagicMock()

        settings = _make_settings()
        with (
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
                side_effect=fake_consumer,
            ),
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
                return_value=MagicMock(),
            ),
        ):
            ConfluentConsumerAdapter(settings)

        assert captured_conf.get("enable.auto.commit") is False

    def test_config_group_id_starts_with_kafka_mcp_ro(self) -> None:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        captured_conf: dict = {}

        def fake_consumer(conf: dict) -> MagicMock:
            captured_conf.update(conf)
            return MagicMock()

        settings = _make_settings()
        with (
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
                side_effect=fake_consumer,
            ),
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
                return_value=MagicMock(),
            ),
        ):
            ConfluentConsumerAdapter(settings)

        group_id = captured_conf.get("group.id", "")
        assert group_id.startswith("kafka-mcp-ro-"), f"group.id '{group_id}' does not start with 'kafka-mcp-ro-'"

    def test_group_id_is_unique_per_instance(self) -> None:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        group_ids: list[str] = []

        def fake_consumer(conf: dict) -> MagicMock:
            group_ids.append(conf.get("group.id", ""))
            return MagicMock()

        settings = _make_settings()
        with (
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
                side_effect=fake_consumer,
            ),
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
                return_value=MagicMock(),
            ),
        ):
            ConfluentConsumerAdapter(settings)
            ConfluentConsumerAdapter(settings)

        assert group_ids[0] != group_ids[1], "group.id must be unique per instantiation (uuid4)"

    def test_ssl_only_omits_sasl_keys(self) -> None:
        """TLS-only (SSL, no mechanism) must not inject any sasl.* keys.

        Regression for CR-01: gating SASL on ``not PLAINTEXT`` injected
        ``sasl.mechanism=None`` which librdkafka rejects at construction.
        """
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )
        from kafka_mcp.config import KafkaMcpSettings

        captured_conf: dict = {}

        def fake_consumer(conf: dict) -> MagicMock:
            captured_conf.update(conf)
            return MagicMock()

        settings = KafkaMcpSettings(bootstrap_servers="localhost:9092", security_protocol="SSL")
        with (
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
                side_effect=fake_consumer,
            ),
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
                return_value=MagicMock(),
            ),
        ):
            ConfluentConsumerAdapter(settings)

        assert captured_conf.get("security.protocol") == "SSL"
        assert not any(k.startswith("sasl.") for k in captured_conf), (
            f"sasl.* keys must be omitted for TLS-only config: {captured_conf}"
        )

    def test_ssl_cert_keys_added_when_fields_set(self) -> None:
        """mTLS: ssl.* keys present (key.password unwrapped) and no sasl.* keys."""
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )
        from kafka_mcp.config import KafkaMcpSettings

        captured_conf: dict = {}

        def fake_consumer(conf: dict) -> MagicMock:
            captured_conf.update(conf)
            return MagicMock()

        settings = KafkaMcpSettings(
            bootstrap_servers="localhost:9092",
            security_protocol="SSL",
            ssl_certificate_location="/c.pem",
            ssl_key_location="/k.pem",
            ssl_ca_location="/ca.pem",
            ssl_key_password="pw",
        )
        with (
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
                side_effect=fake_consumer,
            ),
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
                return_value=MagicMock(),
            ),
        ):
            ConfluentConsumerAdapter(settings)

        assert captured_conf.get("security.protocol") == "SSL"
        assert captured_conf.get("ssl.certificate.location") == "/c.pem"
        assert captured_conf.get("ssl.key.location") == "/k.pem"
        assert captured_conf.get("ssl.ca.location") == "/ca.pem"
        # key password is unwrapped to the plaintext string in the conf dict
        assert captured_conf.get("ssl.key.password") == "pw"
        assert not any(k.startswith("sasl.") for k in captured_conf), (
            f"sasl.* keys must be omitted for mTLS config: {captured_conf}"
        )

    def test_ssl_cert_keys_omitted_when_fields_unset(self, monkeypatch) -> None:
        """Backward-compat: no ssl.* cert keys when SSL fields are unset (T-NRG-02).

        Uses ``_env_file=None`` + ambient-env clearing so a developer ``.env``
        with real SSL cert paths cannot leak ssl.* keys into this PLAINTEXT
        baseline (the settings object must genuinely be "unset" here).
        """
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )
        from kafka_mcp.config import KafkaMcpSettings

        captured_conf: dict = {}

        def fake_consumer(conf: dict) -> MagicMock:
            captured_conf.update(conf)
            return MagicMock()

        self._clear_ambient_ssl_env(monkeypatch)
        settings = KafkaMcpSettings(bootstrap_servers="localhost:9092", _env_file=None)
        with (
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
                side_effect=fake_consumer,
            ),
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
                return_value=MagicMock(),
            ),
        ):
            ConfluentConsumerAdapter(settings)

        for key in (
            "ssl.certificate.location",
            "ssl.key.location",
            "ssl.ca.location",
            "ssl.key.password",
        ):
            assert key not in captured_conf, f"{key} must be omitted when unset"

    # ------------------------------------------------------------------
    # MTLS-01: assert ssl.* wiring on BOTH the consumer AND the admin conf.
    #
    # The three tests above (test_ssl_cert_keys_added_when_fields_set,
    # test_ssl_cert_keys_omitted_when_fields_unset, test_ssl_only_omits_sasl_keys)
    # capture ONLY the Consumer conf — they patch AdminClient with a bare
    # return_value=MagicMock(), so the ssl.* wiring on the AdminClient conf is
    # unasserted. The parametrized tests below patch BOTH Consumer and AdminClient
    # with conf-capturing side_effects and run each assertion body against the
    # "consumer" and "admin" conf builders, closing MTLS-01's "wired into BOTH the
    # consumer and the admin" clause and its "emitted only when set / no
    # PLAINTEXT-SASL regression" clause. src/ is NOT modified — the wiring already
    # exists; these tests only assert it.
    # ------------------------------------------------------------------

    @staticmethod
    def _clear_ambient_ssl_env(monkeypatch) -> None:
        """Strip any ambient KAFKA_MCP_* SSL/security env so an "unset" settings
        baseline is hermetic.

        KafkaMcpSettings auto-loads a project ``.env`` (env_file=".env" in its
        model_config) and OS env vars under the ``KAFKA_MCP_`` prefix. On a
        developer machine the local ``.env`` may point the SSL cert/key/CA at
        real paths, which would leak ssl.* keys into a settings object we intend
        to be a plain PLAINTEXT baseline — falsely failing the "omitted when
        unset" assertions. Callers combine this with ``_env_file=None`` so
        neither the file nor the OS environment contaminates the baseline.
        """
        for var in (
            "KAFKA_MCP_SECURITY_PROTOCOL",
            "KAFKA_MCP_SSL_CERTIFICATE_LOCATION",
            "KAFKA_MCP_SSL_KEY_LOCATION",
            "KAFKA_MCP_SSL_CA_LOCATION",
            "KAFKA_MCP_SSL_KEY_PASSWORD",
        ):
            monkeypatch.delenv(var, raising=False)

    @staticmethod
    def _capture_confs(settings) -> dict[str, dict]:
        """Instantiate the adapter, capturing both the consumer and admin confs.

        Returns ``{"consumer": <conf>, "admin": <conf>}`` — the two librdkafka
        conf dicts the adapter builds in ``__init__``. Both AdminClient and
        Consumer are patched with a side_effect that records the conf argument,
        mirroring the existing ``fake_consumer(conf)`` capture idiom.
        """
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        captured_consumer_conf: dict = {}
        captured_admin_conf: dict = {}

        def fake_consumer(conf: dict) -> MagicMock:
            captured_consumer_conf.update(conf)
            return MagicMock()

        def fake_admin(conf: dict) -> MagicMock:
            captured_admin_conf.update(conf)
            return MagicMock()

        with (
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
                side_effect=fake_consumer,
            ),
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
                side_effect=fake_admin,
            ),
        ):
            ConfluentConsumerAdapter(settings)

        return {"consumer": captured_consumer_conf, "admin": captured_admin_conf}

    @pytest.mark.parametrize("conf_builder", ["consumer", "admin"])
    def test_mtls_ssl_keys_added_on_both_confs_when_fields_set(
        self, conf_builder: str
    ) -> None:
        """mTLS: ssl.* keys land on the consumer AND admin conf (MTLS-01).

        The AdminClient path is the previously-unasserted gap — the existing
        ``test_ssl_cert_keys_added_when_fields_set`` proves only the Consumer conf.
        """
        from kafka_mcp.config import KafkaMcpSettings

        settings = KafkaMcpSettings(
            bootstrap_servers="localhost:9092",
            security_protocol="SSL",
            ssl_certificate_location="/c.pem",
            ssl_key_location="/k.pem",
            ssl_ca_location="/ca.pem",
            ssl_key_password="pw",
        )
        conf = self._capture_confs(settings)[conf_builder]

        assert conf.get("security.protocol") == "SSL"
        assert conf.get("ssl.certificate.location") == "/c.pem"
        assert conf.get("ssl.key.location") == "/k.pem"
        assert conf.get("ssl.ca.location") == "/ca.pem"
        # key password is unwrapped to plaintext (SecretStr.get_secret_value())
        assert conf.get("ssl.key.password") == "pw"
        # SSL-only must never inject sasl.* keys (no SASL regression).
        assert not any(k.startswith("sasl.") for k in conf), (
            f"sasl.* keys must be omitted from the {conf_builder} conf under "
            f"mTLS: {conf}"
        )

    @pytest.mark.parametrize("conf_builder", ["consumer", "admin"])
    def test_mtls_ssl_keys_omitted_on_both_confs_when_unset(
        self, conf_builder: str, monkeypatch
    ) -> None:
        """No ssl.* keys on the consumer OR admin conf when SSL fields unset.

        Proves the PLAINTEXT path stays byte-for-byte free of ssl.* wiring on
        both conf builders (T-NRG-02 / no PLAINTEXT regression, MTLS-01).
        """
        from kafka_mcp.config import KafkaMcpSettings

        self._clear_ambient_ssl_env(monkeypatch)
        settings = KafkaMcpSettings(bootstrap_servers="localhost:9092", _env_file=None)
        conf = self._capture_confs(settings)[conf_builder]

        for key in (
            "ssl.certificate.location",
            "ssl.key.location",
            "ssl.ca.location",
            "ssl.key.password",
        ):
            assert key not in conf, (
                f"{key} must be omitted from the {conf_builder} conf when unset"
            )

    def test_sasl_keys_added_only_when_mechanism_set(self) -> None:
        """SASL keys are configured only when a mechanism is requested."""
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )
        from kafka_mcp.config import KafkaMcpSettings

        captured_conf: dict = {}

        def fake_consumer(conf: dict) -> MagicMock:
            captured_conf.update(conf)
            return MagicMock()

        settings = KafkaMcpSettings(
            bootstrap_servers="localhost:9092",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="alice",
            sasl_password="secret",
        )
        with (
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
                side_effect=fake_consumer,
            ),
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
                return_value=MagicMock(),
            ),
        ):
            ConfluentConsumerAdapter(settings)

        assert captured_conf.get("sasl.mechanism") == "PLAIN"
        assert captured_conf.get("sasl.username") == "alice"
        assert captured_conf.get("sasl.password") == "secret"

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"security_protocol": "PLAINTEXT"},
            # TLS-only: the exact deployment CR-01 broke. Before the fix this
            # injected sasl.mechanism=None and raised KafkaException here.
            {"security_protocol": "SSL"},
            # SASL over TLS with an explicit mechanism (the supported SASL path).
            {
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "PLAIN",
                "sasl_username": "alice",
                "sasl_password": "secret",
            },
        ],
    )
    def test_real_librdkafka_accepts_built_conf(
        self, kwargs: dict, monkeypatch
    ) -> None:
        """Construct a REAL confluent_kafka.Consumer with the built conf.

        Non-mocked regression test for CR-01: librdkafka validates the conf
        dict at ``Consumer()`` construction. Before the fix, a TLS-only
        config injected ``sasl.mechanism=None`` and raised ``KafkaException``
        here. We assert no exception is raised for valid TLS/SASL configs.

        (``SASL_SSL`` with NO mechanism is intentionally not tested: SASL
        inherently requires a mechanism and librdkafka rejects that config
        on its own merits, independent of this adapter.)

        Hermetic: ``_env_file=None`` + ambient-env clearing keeps a developer
        ``.env`` (which may point ssl.ca.location at real, machine-local cert
        paths) from being injected into the REAL Consumer — librdkafka would
        otherwise fail to ``fopen`` the missing CA file and this protocol-variant
        regression guard would fail for reasons unrelated to CR-01.
        """
        from confluent_kafka import KafkaException

        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )
        from kafka_mcp.config import KafkaMcpSettings

        self._clear_ambient_ssl_env(monkeypatch)
        settings = KafkaMcpSettings(
            bootstrap_servers="localhost:9092", _env_file=None, **kwargs
        )

        try:
            # No mock: this exercises librdkafka's real config validation.
            ConfluentConsumerAdapter(settings)
        except KafkaException as exc:  # pragma: no cover - regression guard
            pytest.fail(f"librdkafka rejected a valid conf {kwargs}: {exc}")


# ---------------------------------------------------------------------------
# SchemaRegistryHttpAdapter
# ---------------------------------------------------------------------------


class TestSchemaRegistryHttpAdapter:
    """Stub adapter returns None when url is None; no errors raised."""

    def test_get_schema_returns_none_when_url_none(self) -> None:
        from kafka_mcp.adapters.outbound.schema_registry_http import (
            SchemaRegistryHttpAdapter,
        )

        adapter = SchemaRegistryHttpAdapter(url=None)
        assert adapter.get_schema("payments-value") is None

    def test_get_schema_returns_none_for_any_subject(self) -> None:
        from kafka_mcp.adapters.outbound.schema_registry_http import (
            SchemaRegistryHttpAdapter,
        )

        adapter = SchemaRegistryHttpAdapter(url=None, user="u", password="p")
        for subject in ["foo", "bar-value", "baz-key"]:
            assert adapter.get_schema(subject) is None

    def test_no_exception_when_url_is_none(self) -> None:
        from kafka_mcp.adapters.outbound.schema_registry_http import (
            SchemaRegistryHttpAdapter,
        )

        # Must not raise at construction or at method call
        adapter = SchemaRegistryHttpAdapter(url=None)
        result = adapter.get_schema("any")
        assert result is None

    def test_isinstance_schema_registry_port(self) -> None:
        from kafka_mcp.adapters.outbound.schema_registry_http import (
            SchemaRegistryHttpAdapter,
        )

        adapter = SchemaRegistryHttpAdapter(url=None)
        assert isinstance(adapter, SchemaRegistryPort)


# ---------------------------------------------------------------------------
# orjson helpers
# ---------------------------------------------------------------------------


class TestOrjsonHelpers:
    """orjson_loads and orjson_dumps round-trip correctness."""

    def test_loads_bytes(self) -> None:
        from kafka_mcp.adapters.outbound.json_orjson import orjson_loads

        result = orjson_loads(b'{"key": "value"}')
        assert result == {"key": "value"}

    def test_loads_str(self) -> None:
        from kafka_mcp.adapters.outbound.json_orjson import orjson_loads

        result = orjson_loads('{"num": 42}')
        assert result == {"num": 42}

    def test_dumps_bytes(self) -> None:
        from kafka_mcp.adapters.outbound.json_orjson import orjson_dumps

        result = orjson_dumps({"key": "value"})
        assert isinstance(result, bytes)

    def test_dumps_loads_roundtrip(self) -> None:
        from kafka_mcp.adapters.outbound.json_orjson import orjson_dumps, orjson_loads

        original = {"topic": "payments", "partition": 0, "offset": 42}
        encoded = orjson_dumps(original)
        decoded = orjson_loads(encoded)
        assert decoded == original

    def test_dumps_compact_no_spaces(self) -> None:
        """orjson produces compact JSON (no extra spaces after colon/comma)."""
        from kafka_mcp.adapters.outbound.json_orjson import orjson_dumps

        result = orjson_dumps({"key": "value"})
        assert result == b'{"key":"value"}'


# ---------------------------------------------------------------------------
# SchemaRegistryHttpAdapter — decode pipeline (plan 02-02)
# ---------------------------------------------------------------------------
# Helpers shared across decode tests

_ADAPTER_MOD = "kafka_mcp.adapters.outbound.schema_registry_http"

_SCHEMA_ID = 42
_SCHEMA_ID_BYTES = _SCHEMA_ID.to_bytes(4, "big")
_MAGIC = bytes([0x00])
_CONFLUENT_PREFIX = _MAGIC + _SCHEMA_ID_BYTES  # 5 bytes total


def _make_sr_adapter(url: str | None = "http://sr:8081") -> object:
    from kafka_mcp.adapters.outbound.schema_registry_http import (
        SchemaRegistryHttpAdapter,
    )

    return SchemaRegistryHttpAdapter(url=url)


def _mock_schema(schema_type: str = "AVRO", schema_str: str = '{"type":"record","name":"T","fields":[]}') -> MagicMock:
    """Build a mock Schema object as returned by SchemaRegistryClient.get_schema()."""
    schema = MagicMock()
    schema.schema_type = schema_type
    schema.schema_str = schema_str
    return schema


class TestSchemaRegistryDecodeJson:
    """JSON fallback path: no magic byte in payload."""

    def test_decode_json_fallback(self) -> None:
        """Plain JSON bytes (no magic byte) → returns the decoded dict."""
        with patch(_ADAPTER_MOD + ".SchemaRegistryClient"):
            adapter = _make_sr_adapter()
        raw = b'{"key": "val"}'
        result = adapter.decode(raw)
        assert result == {"key": "val"}

    def test_decode_json_fallback_non_object_returns_none(self) -> None:
        """WR-04: non-object JSON (list/scalar) → None, not a {"value": ...} wrapper."""
        with patch(_ADAPTER_MOD + ".SchemaRegistryClient"):
            adapter = _make_sr_adapter()
        # A JSON array payload has no object body to decode.
        assert adapter.decode(b"[1, 2, 3]") is None
        # A bare JSON scalar likewise yields no decoded object.
        assert adapter.decode(b"42") is None
        assert adapter.decode(b'"hello"') is None

    def test_decode_json_fallback_invalid(self) -> None:
        """Non-JSON, non-magic-byte payload → raises DecodeError with 'json' in reason."""
        from kafka_mcp.domain.errors import DecodeError

        with patch(_ADAPTER_MOD + ".SchemaRegistryClient"):
            adapter = _make_sr_adapter()
        raw = b"not-json"
        with pytest.raises(DecodeError) as exc_info:
            adapter.decode(raw, topic="t", partition=0, offset=0)
        assert "json" in exc_info.value.reason.lower()

    def test_decode_none_url_raises_decode_error(self) -> None:
        """With url=None (SR not configured), magic-byte payload raises DecodeError."""
        from kafka_mcp.domain.errors import DecodeError

        adapter = _make_sr_adapter(url=None)
        raw = _CONFLUENT_PREFIX + b"\x00" * 10
        with pytest.raises(DecodeError) as exc_info:
            adapter.decode(raw, topic="t", partition=0, offset=0)
        assert "not configured" in exc_info.value.reason.lower()


class TestSchemaRegistryDecodeAvro:
    """Confluent-framed Avro payloads decoded via mocked AvroDeserializer."""

    def test_decode_magic_byte_avro(self) -> None:
        """Confluent-framed payload → calls AvroDeserializer and returns dict."""
        mock_schema = _mock_schema("AVRO")
        mock_client = MagicMock()
        mock_client.get_schema.return_value = mock_schema
        mock_deserializer = MagicMock(return_value={"order_id": "123"})

        with (
            patch(_ADAPTER_MOD + ".SchemaRegistryClient", return_value=mock_client),
            patch(_ADAPTER_MOD + ".AvroDeserializer", return_value=mock_deserializer),
        ):
            adapter = _make_sr_adapter()
            raw = _CONFLUENT_PREFIX + b"\x01" * 10
            result = adapter.decode(raw)
        assert result == {"order_id": "123"}

    def test_decode_corrupt_avro_raises_decode_error(self) -> None:
        """Avro deserializer failure → wrapped as DecodeError (not propagated raw)."""
        from kafka_mcp.domain.errors import DecodeError

        mock_schema = _mock_schema("AVRO")
        mock_client = MagicMock()
        mock_client.get_schema.return_value = mock_schema
        mock_deserializer = MagicMock(side_effect=Exception("corrupt avro data"))

        with (
            patch(_ADAPTER_MOD + ".SchemaRegistryClient", return_value=mock_client),
            patch(_ADAPTER_MOD + ".AvroDeserializer", return_value=mock_deserializer),
        ):
            adapter = _make_sr_adapter()
            raw = _CONFLUENT_PREFIX + b"\xff" * 5
            with pytest.raises(DecodeError):
                adapter.decode(raw, topic="t", partition=0, offset=0)

    def test_schema_registry_client_cached(self) -> None:
        """Schema is fetched only once per schema_id (SR client caches internally)."""
        mock_schema = _mock_schema("AVRO")
        mock_client = MagicMock()
        mock_client.get_schema.return_value = mock_schema
        mock_deserializer = MagicMock(return_value={"k": "v"})

        with (
            patch(_ADAPTER_MOD + ".SchemaRegistryClient", return_value=mock_client),
            patch(_ADAPTER_MOD + ".AvroDeserializer", return_value=mock_deserializer),
        ):
            adapter = _make_sr_adapter()
            raw = _CONFLUENT_PREFIX + b"\x01" * 6
            adapter.decode(raw)
            adapter.decode(raw)

        # SchemaRegistryClient.get_schema was called once (second call uses cached result)
        assert mock_client.get_schema.call_count == 1


def _build_protobuf_payload(proto_src: str, message_name: str, fields: dict, schema_id: int = 123) -> bytes:
    """Compile ``proto_src`` with protoc, serialize a message, and wrap it in
    Confluent Protobuf framing (magic + schema_id + 0x00 index + payload).

    This produces a REAL wire payload (no mocks) so the generic decode path is
    exercised end-to-end (CR-01).
    """
    import os
    import tempfile

    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
    from grpc_tools import protoc as _protoc

    with tempfile.TemporaryDirectory() as td:
        pp = os.path.join(td, "s.proto")
        op = os.path.join(td, "s.desc")
        with open(pp, "w", encoding="utf-8") as fh:
            fh.write(proto_src)
        # WR-01: compile via the in-process grpc_tools.protoc (vendored
        # compiler), matching the adapter — no system protoc binary required.
        rc = _protoc.main(
            [
                "protoc",
                f"--proto_path={td}",
                f"--descriptor_set_out={op}",
                "--include_imports",
                pp,
            ]
        )
        assert rc == 0, f"grpc_tools.protoc failed (rc={rc})"
        fds = descriptor_pb2.FileDescriptorSet()
        with open(op, "rb") as fh:
            fds.ParseFromString(fh.read())
    pool = descriptor_pool.DescriptorPool()
    fd = None
    for f in fds.file:
        fd = pool.Add(f)
    desc = fd.message_types_by_name[message_name]
    msg_cls = message_factory.GetMessageClass(desc)
    msg = msg_cls()
    for k, v in fields.items():
        setattr(msg, k, v)
    payload = msg.SerializeToString()
    # magic(1) + schema_id(4, big-endian) + index header 0x00 + payload
    return b"\x00" + schema_id.to_bytes(4, "big") + b"\x00" + payload


def _grpc_tools_available() -> bool:
    """True when the in-process grpc_tools.protoc compiler is importable.

    WR-01: the adapter now compiles via grpc_tools (a declared pip dependency)
    rather than a system ``protoc`` binary, so the decode path is exercisable on
    any host with the wheel installed — no PATH binary required.
    """
    try:
        import grpc_tools.protoc  # noqa: F401
    except ImportError:
        return False
    return True


@pytest.mark.skipif(
    not _grpc_tools_available(),
    reason="grpcio-tools required for generic protobuf decode",
)
class TestSchemaRegistryDecodeProtobuf:
    """Confluent-framed Protobuf payloads decoded via in-process grpc_tools."""

    _PROTO_SRC = 'syntax = "proto3";\nmessage Person { string msisdn = 1; int32 age = 2; }\n'

    # WR-05: a schema with snake_case fields, an int64, and a nested message,
    # to lock in preserving_proto_field_name=True (snake_case + nested paths).
    _PROTO_SRC_NESTED = (
        'syntax = "proto3";\n'
        "message Order {\n"
        "  message Payload { int64 order_id = 1; string product_name = 2; }\n"
        "  int64 customer_id = 1;\n"
        "  Payload payload = 2;\n"
        "}\n"
    )

    def test_protobuf_construction_does_not_raise_attributeerror(self) -> None:
        """CR-01: the decode path must not raise AttributeError at construction.

        The old code constructed ``ProtobufDeserializer(None, ...)`` which
        dereferenced ``None.DESCRIPTOR`` and raised AttributeError for EVERY
        protobuf payload. This asserts that path is gone: a real payload now
        decodes without any AttributeError.
        """
        raw = _build_protobuf_payload(self._PROTO_SRC, "Person", {"msisdn": "79001234567", "age": 7})
        schema = _mock_schema("PROTOBUF", schema_str=self._PROTO_SRC)
        mock_client = MagicMock()
        mock_client.get_schema.return_value = schema

        with patch(_ADAPTER_MOD + ".SchemaRegistryClient", return_value=mock_client):
            adapter = _make_sr_adapter()
            # Must NOT raise AttributeError (the CR-01 defect).
            result = adapter.decode(raw, topic="people", partition=0, offset=0)
        assert isinstance(result, dict)

    def test_decode_magic_byte_protobuf_roundtrip(self) -> None:
        """Real Confluent-framed PROTOBUF payload → generic decode to dict."""
        raw = _build_protobuf_payload(self._PROTO_SRC, "Person", {"msisdn": "79001234567", "age": 42})
        schema = _mock_schema("PROTOBUF", schema_str=self._PROTO_SRC)
        mock_client = MagicMock()
        mock_client.get_schema.return_value = schema

        with patch(_ADAPTER_MOD + ".SchemaRegistryClient", return_value=mock_client):
            adapter = _make_sr_adapter()
            result = adapter.decode(raw)
        assert result == {"msisdn": "79001234567", "age": 42}

    def test_decode_protobuf_preserves_snake_case_and_nested_paths(self) -> None:
        """WR-05: snake_case field names are preserved and nested paths resolve.

        With ``preserving_proto_field_name=True`` the decoded dict mirrors the
        registered ``.proto`` (snake_case), so a nested dotted lookup such as
        ``value:payload.order_id`` resolves — proving the Protobuf path now
        agrees with the Avro path. (proto3 int64 JSON-maps to a string by spec;
        ``_matches_key`` compares via ``str(...)`` so a string key still matches.)
        """
        import os
        import tempfile
        from datetime import datetime, timezone

        from google.protobuf import (
            descriptor_pb2,
            descriptor_pool,
            message_factory,
        )
        from grpc_tools import protoc as _protoc

        from kafka_mcp.domain.models import KafkaMessage
        from kafka_mcp.domain.search_service import _matches_key

        # Build a nested-message wire payload by hand (the flat _build helper's
        # setattr cannot populate a nested message field).
        with tempfile.TemporaryDirectory() as td:
            pp = os.path.join(td, "s.proto")
            op = os.path.join(td, "s.desc")
            with open(pp, "w", encoding="utf-8") as fh:
                fh.write(self._PROTO_SRC_NESTED)
            rc = _protoc.main(
                [
                    "protoc",
                    f"--proto_path={td}",
                    f"--descriptor_set_out={op}",
                    "--include_imports",
                    pp,
                ]
            )
            assert rc == 0, f"grpc_tools.protoc failed (rc={rc})"
            fds = descriptor_pb2.FileDescriptorSet()
            with open(op, "rb") as fh:
                fds.ParseFromString(fh.read())
        pool = descriptor_pool.DescriptorPool()
        fd = None
        for f in fds.file:
            fd = pool.Add(f)
        order_cls = message_factory.GetMessageClass(fd.message_types_by_name["Order"])
        order = order_cls()
        order.customer_id = 7000000000  # int64 > 2**31, exercises int64 mapping
        order.payload.order_id = 9000000001
        order.payload.product_name = "widget"
        payload = order.SerializeToString()
        raw = b"\x00" + (123).to_bytes(4, "big") + b"\x00" + payload

        schema = _mock_schema("PROTOBUF", schema_str=self._PROTO_SRC_NESTED)
        mock_client = MagicMock()
        mock_client.get_schema.return_value = schema

        with patch(_ADAPTER_MOD + ".SchemaRegistryClient", return_value=mock_client):
            adapter = _make_sr_adapter()
            result = adapter.decode(raw)

        # Snake_case field names preserved (NOT camelCased customerId/orderId).
        assert "customer_id" in result
        assert "payload" in result
        assert "order_id" in result["payload"]
        assert "product_name" in result["payload"]

        # Nested dotted-path matching now resolves on the Protobuf path. Wrap the
        # decoded value in a KafkaMessage and match via the value:<path> form —
        # exactly how the search service queries it (proves WR-05 end-to-end).
        msg = KafkaMessage(
            topic="orders",
            partition=0,
            offset=0,
            key=None,
            headers={},
            value=result,
            timestamp_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
            raw=raw,
        )
        assert _matches_key(msg, "9000000001", "value:payload.order_id")
        assert _matches_key(msg, "7000000000", "value:customer_id")

    def test_decode_protobuf_schema_importing_well_known_type(self) -> None:
        """WR-A: a schema importing google/protobuf/timestamp.proto must compile.

        The grpc_tools well-known-type include path is required on the protoc
        argv; without it, any schema importing google/protobuf/* fails to
        compile (rc=1) and every such payload becomes a DecodeError. This
        compiles such a schema end-to-end through the adapter's own path.
        """
        proto_src = (
            'syntax = "proto3";\n'
            'import "google/protobuf/timestamp.proto";\n'
            "message Event {\n"
            "  string id = 1;\n"
            "  google.protobuf.Timestamp created_at = 2;\n"
            "}\n"
        )
        schema = _mock_schema("PROTOBUF", schema_str=proto_src)
        with patch(_ADAPTER_MOD + ".SchemaRegistryClient"):
            adapter = _make_sr_adapter()
            # Must NOT raise — the WKT include path lets protoc resolve the
            # google/protobuf/timestamp.proto import.
            descriptor = adapter._compile_proto_descriptor(schema)
        assert "Event" in descriptor.message_types_by_name

    def test_decode_unknown_schema_type(self) -> None:
        """Schema type other than AVRO/PROTOBUF → DecodeError with 'unknown schema type'."""
        from kafka_mcp.domain.errors import DecodeError

        mock_schema = _mock_schema("JSON")  # not AVRO or PROTOBUF
        mock_client = MagicMock()
        mock_client.get_schema.return_value = mock_schema

        with (
            patch(_ADAPTER_MOD + ".SchemaRegistryClient", return_value=mock_client),
        ):
            adapter = _make_sr_adapter()
            raw = _CONFLUENT_PREFIX + b"\x03" * 5
            with pytest.raises(DecodeError) as exc_info:
                adapter.decode(raw, topic="t", partition=0, offset=0)
        assert "unknown schema type" in exc_info.value.reason.lower()


class TestSchemaRegistryAdapterSecurity:
    """Security invariants: credentials not exposed; Protocol membership."""

    def test_implements_schema_registry_port(self) -> None:
        """SchemaRegistryHttpAdapter(url=None) is an instance of SchemaRegistryPort."""
        adapter = _make_sr_adapter(url=None)
        assert isinstance(adapter, SchemaRegistryPort)

    def test_sr_credentials_not_logged(self) -> None:
        """repr() and str() of the adapter must not contain the sr_pass value."""
        from kafka_mcp.adapters.outbound.schema_registry_http import (
            SchemaRegistryHttpAdapter,
        )

        secret = "super-secret-password-xyz"
        with patch(_ADAPTER_MOD + ".SchemaRegistryClient"):
            adapter = SchemaRegistryHttpAdapter(url="http://sr:8081", user="alice", password=secret)
        assert secret not in repr(adapter)
        assert secret not in str(adapter)


# ---------------------------------------------------------------------------
# ConfluentConsumerAdapter — fetch_messages (plan 02-03)
# ---------------------------------------------------------------------------


def _make_consumer_adapter(mock_consumer: MagicMock) -> object:
    """Build a ConfluentConsumerAdapter with a mocked librdkafka Consumer."""
    from kafka_mcp.adapters.outbound.confluent_consumer import (
        ConfluentConsumerAdapter,
    )
    from kafka_mcp.config import KafkaMcpSettings

    settings = KafkaMcpSettings(bootstrap_servers="localhost:9092")
    with (
        patch(
            "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
            return_value=mock_consumer,
        ),
        patch(
            "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
            return_value=MagicMock(),
        ),
    ):
        return ConfluentConsumerAdapter(settings)


def _make_msg_mock(
    offset: int,
    value: bytes = b'{"event":"test"}',
    key: bytes | None = b"order-key",
    headers: list | None = None,
    timestamp_type: int = 1,  # TIMESTAMP_CREATE_TIME
    timestamp_ms: int = 1_700_000_000_000,
    error: object = None,
) -> MagicMock:
    """Build a mock confluent_kafka.Message for adapter tests."""
    msg = MagicMock()
    msg.offset.return_value = offset
    msg.value.return_value = value
    msg.key.return_value = key
    msg.headers.return_value = headers
    msg.timestamp.return_value = (timestamp_type, timestamp_ms)
    msg.error.return_value = error
    return msg


class TestFetchMessages:
    """ConfluentConsumerAdapter.fetch_messages — seek-and-scan logic."""

    def test_fetch_messages_returns_kafka_message_list(self) -> None:
        """Polled messages are returned as a list[KafkaMessage]."""
        from kafka_mcp.domain.models import KafkaMessage

        mock_consumer = MagicMock()
        msg0 = _make_msg_mock(
            offset=10,
            value=b'{"event":"order_created"}',
            key=b"order-123",
            headers=[("x-trace", b"abc")],
        )
        mock_consumer.poll.side_effect = [msg0, None]
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_messages(
            topic="orders",
            partition=0,
            start_offset=10,
            stop_offset=100,
            time_to=None,
            limit=50,
        )

        assert isinstance(result, list)
        assert len(result) == 1
        msg = result[0]
        assert isinstance(msg, KafkaMessage)
        assert msg.topic == "orders"
        assert msg.partition == 0
        assert msg.offset == 10
        assert msg.key == "order-123"
        assert msg.headers == {"x-trace": "abc"}
        assert msg.raw == b'{"event":"order_created"}'
        assert msg.value is None

    def test_fetch_messages_stops_at_stop_offset(self) -> None:
        """Messages with offset >= stop_offset are not returned."""
        mock_consumer = MagicMock()
        msg0 = _make_msg_mock(offset=5)
        msg1 = _make_msg_mock(offset=10)  # exactly at stop_offset — excluded
        mock_consumer.poll.side_effect = [msg0, msg1, None]
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_messages(
            topic="orders",
            partition=0,
            start_offset=5,
            stop_offset=10,
            time_to=None,
            limit=100,
        )

        offsets = [m.offset for m in result]
        assert 10 not in offsets
        assert 5 in offsets

    def test_fetch_messages_stops_at_limit(self) -> None:
        """Returns at most `limit` messages."""
        mock_consumer = MagicMock()
        msgs = [_make_msg_mock(offset=i) for i in range(10)]
        mock_consumer.poll.side_effect = msgs + [None]
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_messages(
            topic="orders",
            partition=0,
            start_offset=0,
            stop_offset=1000,
            time_to=None,
            limit=3,
        )

        assert len(result) == 3

    def test_fetch_messages_excludes_messages_at_or_after_time_to(self) -> None:
        """WR-03: time_to is exclusive — a message at exactly time_to is dropped."""
        from datetime import datetime, timezone

        mock_consumer = MagicMock()
        # msg0: at 1000s — strictly within time_to of 1500s
        msg0 = _make_msg_mock(offset=0, timestamp_type=1, timestamp_ms=1_000_000)
        # msg1: at exactly 1500s — equals time_to → excluded (exclusive bound)
        msg1 = _make_msg_mock(offset=1, timestamp_type=1, timestamp_ms=1_500_000)
        mock_consumer.poll.side_effect = [msg0, msg1, None]
        adapter = _make_consumer_adapter(mock_consumer)

        time_to = datetime.fromtimestamp(1500.0, tz=timezone.utc)
        result = adapter.fetch_messages(
            topic="orders",
            partition=0,
            start_offset=0,
            stop_offset=1000,
            time_to=time_to,
            limit=100,
        )

        # Only the first message (strictly before time_to) should be returned
        assert len(result) == 1
        assert result[0].offset == 0

    def test_fetch_messages_out_of_order_timestamp_does_not_truncate(self) -> None:
        """WR-02: an out-of-window message must NOT stop the scan.

        Offsets are not strictly timestamp-ordered. A future-dated message at
        offset 1 must be skipped while a later in-window message at offset 2 is
        still returned (the old `break` would drop offset 2).
        """
        from datetime import datetime, timezone

        mock_consumer = MagicMock()
        msg0 = _make_msg_mock(offset=0, timestamp_type=1, timestamp_ms=1_000_000)
        # offset 1 is out of window (future-dated) — must be skipped, not break
        msg1 = _make_msg_mock(offset=1, timestamp_type=1, timestamp_ms=9_000_000)
        # offset 2 is back in window — must still be returned
        msg2 = _make_msg_mock(offset=2, timestamp_type=1, timestamp_ms=1_200_000)
        mock_consumer.poll.side_effect = [msg0, msg1, msg2, None]
        adapter = _make_consumer_adapter(mock_consumer)

        time_to = datetime.fromtimestamp(1500.0, tz=timezone.utc)
        result = adapter.fetch_messages(
            topic="orders",
            partition=0,
            start_offset=0,
            stop_offset=1000,
            time_to=time_to,
            limit=100,
        )

        offsets = [m.offset for m in result]
        assert offsets == [0, 2]

    def test_fetch_messages_timestamp_utc_from_create_time(self) -> None:
        """TIMESTAMP_CREATE_TIME → UTC-aware datetime derived from ms correctly."""
        from datetime import datetime, timezone

        mock_consumer = MagicMock()
        # 1_700_000_000_000 ms = epoch 1_700_000_000 s
        ts_ms = 1_700_000_000_000
        msg0 = _make_msg_mock(offset=0, timestamp_type=1, timestamp_ms=ts_ms)
        mock_consumer.poll.side_effect = [msg0, None]
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_messages(
            topic="payments",
            partition=0,
            start_offset=0,
            stop_offset=100,
            time_to=None,
            limit=10,
        )

        assert len(result) == 1
        ts = result[0].timestamp_utc
        expected = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        assert ts == expected
        assert ts.tzinfo is not None

    def test_fetch_messages_timestamp_fallback_log_append_time(self) -> None:
        """TIMESTAMP_LOG_APPEND_TIME → timestamp_utc still set (fallback path)."""
        mock_consumer = MagicMock()
        # timestamp_type=2 is TIMESTAMP_LOG_APPEND_TIME
        msg0 = _make_msg_mock(offset=0, timestamp_type=2, timestamp_ms=1_000_000_000)
        mock_consumer.poll.side_effect = [msg0, None]
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_messages(
            topic="events",
            partition=0,
            start_offset=0,
            stop_offset=100,
            time_to=None,
            limit=10,
        )

        assert len(result) == 1
        ts = result[0].timestamp_utc
        assert ts is not None
        assert ts.tzinfo is not None  # must be UTC-aware

    def test_fetch_messages_respects_max_scan(self) -> None:
        """Scan stops after max_scan messages regardless of limit."""
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )
        from kafka_mcp.config import KafkaMcpSettings

        mock_consumer = MagicMock()
        # Produce 200 messages; max_scan is set to 5 via settings
        msgs = [_make_msg_mock(offset=i) for i in range(200)]
        mock_consumer.poll.side_effect = msgs + [None]

        # Use settings with max_scan=5
        settings = KafkaMcpSettings(
            bootstrap_servers="localhost:9092",
            max_scan=5,
        )
        with (
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
                return_value=mock_consumer,
            ),
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
                return_value=MagicMock(),
            ),
        ):
            adapter = ConfluentConsumerAdapter(settings)

        result = adapter.fetch_messages(
            topic="events",
            partition=0,
            start_offset=0,
            stop_offset=1000,
            time_to=None,
            limit=100,
        )

        assert len(result) <= 5

    def test_fetch_messages_empty_partition(self) -> None:
        """Consumer.poll returns None immediately → returns empty list."""
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = None
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_messages(
            topic="orders",
            partition=0,
            start_offset=0,
            stop_offset=100,
            time_to=None,
            limit=50,
        )

        assert result == []

    def test_fetch_messages_no_subscribe_call(self) -> None:
        """Consumer.subscribe is never called during fetch_messages (KAFKA-06)."""
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = None
        adapter = _make_consumer_adapter(mock_consumer)

        adapter.fetch_messages(
            topic="orders",
            partition=0,
            start_offset=0,
            stop_offset=100,
            time_to=None,
            limit=50,
        )

        mock_consumer.subscribe.assert_not_called()


# ---------------------------------------------------------------------------
# ConfluentConsumerAdapter — fetch_message (plan 02-03)
# ---------------------------------------------------------------------------


class TestFetchMessage:
    """ConfluentConsumerAdapter.fetch_message — single-offset lookup."""

    def test_fetch_message_returns_single_kafka_message(self) -> None:
        """Valid offset within watermarks → returns one KafkaMessage."""
        from kafka_mcp.domain.models import KafkaMessage

        mock_consumer = MagicMock()
        # Watermarks: low=0, high=100; requesting offset=10 (in range)
        mock_consumer.get_watermark_offsets.return_value = (0, 100)
        msg = _make_msg_mock(
            offset=10,
            value=b'{"event":"payment"}',
            key=b"pay-456",
            headers=[("x-id", b"xyz")],
        )
        mock_consumer.poll.return_value = msg
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_message(topic="payments", partition=0, offset=10)

        assert isinstance(result, KafkaMessage)
        assert result.topic == "payments"
        assert result.partition == 0
        assert result.offset == 10
        assert result.key == "pay-456"
        assert result.headers == {"x-id": "xyz"}
        assert result.raw == b'{"event":"payment"}'
        assert result.value is None

    def test_fetch_message_out_of_range_raises_message_not_found(
        self,
    ) -> None:
        """offset >= latest watermark → raises MessageNotFoundError."""
        from kafka_mcp.domain.errors import MessageNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 50)
        adapter = _make_consumer_adapter(mock_consumer)

        with pytest.raises(MessageNotFoundError):
            adapter.fetch_message(topic="orders", partition=1, offset=50)

    def test_fetch_message_message_not_found_carries_coordinates(
        self,
    ) -> None:
        """MessageNotFoundError.topic, .partition, .offset match the request."""
        from kafka_mcp.domain.errors import MessageNotFoundError

        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 30)
        adapter = _make_consumer_adapter(mock_consumer)

        with pytest.raises(MessageNotFoundError) as exc_info:
            adapter.fetch_message(topic="events", partition=2, offset=999)

        err = exc_info.value
        assert err.topic == "events"
        assert err.partition == 2
        assert err.offset == 999

    def test_fetch_message_timeout_for_in_range_offset_is_transient(self) -> None:
        """WR-05: poll timeout for an IN-RANGE offset → TransientError, not NotFound.

        The offset 5 is within watermarks (0, 100). A None poll therefore means
        the broker did not deliver within the budget — a transient/operational
        condition, not a definitive absence.
        """
        from kafka_mcp.domain.errors import (
            MessageNotFoundError,
            TransientError,
        )

        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 100)
        mock_consumer.poll.return_value = None  # timeout
        adapter = _make_consumer_adapter(mock_consumer)

        with pytest.raises(TransientError) as exc_info:
            adapter.fetch_message(topic="orders", partition=0, offset=5)
        # Must NOT be conflated with a real not-found.
        assert not isinstance(exc_info.value, MessageNotFoundError)
        assert exc_info.value.topic == "orders"
        assert exc_info.value.partition == 0
        assert exc_info.value.offset == 5

    def test_fetch_message_no_subscribe_in_fetch_message(self) -> None:
        """Consumer.subscribe is never called during fetch_message (KAFKA-06)."""
        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 100)
        msg = _make_msg_mock(offset=5)
        mock_consumer.poll.return_value = msg
        adapter = _make_consumer_adapter(mock_consumer)

        adapter.fetch_message(topic="orders", partition=0, offset=5)

        mock_consumer.subscribe.assert_not_called()


# ---------------------------------------------------------------------------
# Phase 4 Plan 01: raw_key threading (Task 2 RED)
# ---------------------------------------------------------------------------


class TestFetchMessagesRawKey:
    """fetch_messages() threads raw_key bytes into KafkaMessage (D-KEY-01)."""

    def test_fetch_messages_raw_key_bytes_threaded(self) -> None:
        """When msg.key() returns bytes, KafkaMessage.raw_key equals those bytes."""
        raw_key_bytes = b"\x00\x00\x00\x00\x07hello"
        mock_consumer = MagicMock()
        msg0 = _make_msg_mock(
            offset=5,
            value=b'{"event":"test"}',
            key=raw_key_bytes,
        )
        mock_consumer.poll.side_effect = [msg0, None]
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_messages(
            topic="orders",
            partition=0,
            start_offset=5,
            stop_offset=100,
            time_to=None,
            limit=10,
        )

        assert len(result) == 1
        assert result[0].raw_key == raw_key_bytes

    def test_fetch_messages_raw_key_none_when_key_is_none(self) -> None:
        """When msg.key() returns None, KafkaMessage.raw_key is None."""
        mock_consumer = MagicMock()
        msg0 = _make_msg_mock(
            offset=5,
            value=b'{"event":"test"}',
            key=None,
        )
        mock_consumer.poll.side_effect = [msg0, None]
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_messages(
            topic="orders",
            partition=0,
            start_offset=5,
            stop_offset=100,
            time_to=None,
            limit=10,
        )

        assert len(result) == 1
        assert result[0].raw_key is None

    def test_fetch_messages_key_str_still_populated(self) -> None:
        """key: str field is still populated via UTF-8 decode alongside raw_key."""
        raw_key_bytes = b"\x00\x00\x00\x00\x07hello"
        mock_consumer = MagicMock()
        msg0 = _make_msg_mock(
            offset=5,
            value=b'{"event":"test"}',
            key=raw_key_bytes,
        )
        mock_consumer.poll.side_effect = [msg0, None]
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_messages(
            topic="orders",
            partition=0,
            start_offset=5,
            stop_offset=100,
            time_to=None,
            limit=10,
        )

        assert len(result) == 1
        # key should be the UTF-8 replacement-decoded string (bytes contain
        # non-UTF8 prefix but errors="replace" produces a string, not None)
        assert result[0].key is not None


class TestFetchMessageRawKey:
    """fetch_message() threads raw_key bytes into KafkaMessage (D-KEY-01)."""

    def test_fetch_message_raw_key_bytes_threaded(self) -> None:
        """When msg.key() returns bytes, KafkaMessage.raw_key equals those bytes."""
        raw_key_bytes = b"\x00\x00\x00\x00\x07hello"
        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 100)
        msg = _make_msg_mock(
            offset=10,
            value=b'{"event":"payment"}',
            key=raw_key_bytes,
        )
        mock_consumer.poll.return_value = msg
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_message(topic="payments", partition=0, offset=10)

        assert result.raw_key == raw_key_bytes

    def test_fetch_message_raw_key_none_when_key_is_none(self) -> None:
        """When msg.key() returns None, KafkaMessage.raw_key is None."""
        mock_consumer = MagicMock()
        mock_consumer.get_watermark_offsets.return_value = (0, 100)
        msg = _make_msg_mock(
            offset=10,
            value=b'{"event":"payment"}',
            key=None,
        )
        mock_consumer.poll.return_value = msg
        adapter = _make_consumer_adapter(mock_consumer)

        result = adapter.fetch_message(topic="payments", partition=0, offset=10)

        assert result.raw_key is None


# ---------------------------------------------------------------------------
# ConfluentConsumerAdapter — consumer_group_lag (Phase 5 Plan 01)
# ---------------------------------------------------------------------------


def _make_tp_mock(topic: str, partition: int, offset: int, error: object = None) -> MagicMock:
    """Build a mock TopicPartition for AdminClient offset results."""
    tp = MagicMock()
    tp.topic = topic
    tp.partition = partition
    tp.offset = offset
    tp.error = error
    return tp


class TestConfluentConsumerAdapterLag:
    """consumer_group_lag: reads committed offsets via AdminClient, computes lag."""

    def _make_adapter(self, mock_consumer: MagicMock, mock_admin: MagicMock) -> object:
        from kafka_mcp.adapters.outbound.confluent_consumer import (
            ConfluentConsumerAdapter,
        )

        settings = _make_settings()
        with (
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.Consumer",
                return_value=mock_consumer,
            ),
            patch(
                "kafka_mcp.adapters.outbound.confluent_consumer.AdminClient",
                return_value=mock_admin,
            ),
        ):
            return ConfluentConsumerAdapter(settings)

    def _mock_admin_offsets(self, mock_admin: MagicMock, group: str, tps: list[MagicMock]) -> None:
        """Configure mock_admin.list_consumer_group_offsets to return tps."""
        group_result = MagicMock()
        group_result.topic_partitions = tps
        future = MagicMock()
        future.result.return_value = group_result
        mock_admin.list_consumer_group_offsets.return_value = {group: future}

    def test_consumer_group_lag_returns_lag_records(self) -> None:
        """Two partitions with committed offsets → 2 LagRecords with correct lag."""
        from kafka_mcp.domain.models import LagRecord

        mock_consumer = MagicMock()
        mock_admin = MagicMock()
        adapter = self._make_adapter(mock_consumer, mock_admin)

        tps = [
            _make_tp_mock("orders", 0, 50),
            _make_tp_mock("orders", 1, 30),
        ]
        self._mock_admin_offsets(mock_admin, "my-group", tps)

        # Mock get_watermark_offsets on the consumer (adapter delegates to it)
        mock_consumer.get_watermark_offsets.return_value = (0, 100)

        result = adapter.consumer_group_lag("my-group")

        assert len(result) == 2
        assert all(isinstance(r, LagRecord) for r in result)

        # Sort by partition for deterministic assertion
        result.sort(key=lambda r: r.partition)
        assert result[0].partition == 0
        assert result[0].current_offset == 50
        assert result[0].end_offset == 100
        assert result[0].lag == 50
        assert result[1].partition == 1
        assert result[1].current_offset == 30
        assert result[1].end_offset == 100
        assert result[1].lag == 70

    def test_consumer_group_lag_no_committed_offset(self) -> None:
        """Partition with offset=-1001 (OFFSET_INVALID) → current_offset=0, lag=end_offset."""
        mock_consumer = MagicMock()
        mock_admin = MagicMock()
        adapter = self._make_adapter(mock_consumer, mock_admin)

        tps = [_make_tp_mock("orders", 0, -1001)]
        self._mock_admin_offsets(mock_admin, "my-group", tps)
        mock_consumer.get_watermark_offsets.return_value = (0, 200)

        result = adapter.consumer_group_lag("my-group")

        assert len(result) == 1
        assert result[0].current_offset == 0
        assert result[0].end_offset == 200
        assert result[0].lag == 200

    def test_consumer_group_lag_empty_group(self) -> None:
        """AdminClient raises KafkaException → empty list returned."""
        from confluent_kafka import KafkaException

        mock_consumer = MagicMock()
        mock_admin = MagicMock()
        adapter = self._make_adapter(mock_consumer, mock_admin)

        future = MagicMock()
        future.result.side_effect = KafkaException(MagicMock(code=MagicMock(return_value=-1)))
        mock_admin.list_consumer_group_offsets.return_value = {"nonexistent": future}

        result = adapter.consumer_group_lag("nonexistent")
        assert result == []

    def test_consumer_group_lag_topics_filter(self) -> None:
        """topics=["orders"] → only "orders" partitions returned, "payments" excluded."""
        mock_consumer = MagicMock()
        mock_admin = MagicMock()
        adapter = self._make_adapter(mock_consumer, mock_admin)

        tps = [
            _make_tp_mock("orders", 0, 50),
            _make_tp_mock("payments", 0, 10),
        ]
        self._mock_admin_offsets(mock_admin, "my-group", tps)
        mock_consumer.get_watermark_offsets.return_value = (0, 100)

        result = adapter.consumer_group_lag("my-group", topics=["orders"])

        assert len(result) == 1
        assert result[0].topic == "orders"

    def test_consumer_group_lag_topics_none_returns_all(self) -> None:
        """topics=None → all committed topics returned."""
        mock_consumer = MagicMock()
        mock_admin = MagicMock()
        adapter = self._make_adapter(mock_consumer, mock_admin)

        tps = [
            _make_tp_mock("orders", 0, 50),
            _make_tp_mock("payments", 0, 10),
        ]
        self._mock_admin_offsets(mock_admin, "my-group", tps)
        mock_consumer.get_watermark_offsets.return_value = (0, 100)

        result = adapter.consumer_group_lag("my-group", topics=None)

        topics_returned = {r.topic for r in result}
        assert topics_returned == {"orders", "payments"}

    def test_consumer_group_lag_evidence_fields(self) -> None:
        """Each LagRecord has source='kafka' and event_type='consumer_lag'."""
        mock_consumer = MagicMock()
        mock_admin = MagicMock()
        adapter = self._make_adapter(mock_consumer, mock_admin)

        tps = [_make_tp_mock("orders", 0, 50)]
        self._mock_admin_offsets(mock_admin, "my-group", tps)
        mock_consumer.get_watermark_offsets.return_value = (0, 100)

        result = adapter.consumer_group_lag("my-group")

        assert len(result) == 1
        assert result[0].source == "kafka"
        assert result[0].event_type == "consumer_lag"

    def test_consumer_group_lag_timestamp_utc_is_utc_aware(self) -> None:
        """timestamp_utc has tzinfo set to UTC."""
        from datetime import timezone

        mock_consumer = MagicMock()
        mock_admin = MagicMock()
        adapter = self._make_adapter(mock_consumer, mock_admin)

        tps = [_make_tp_mock("orders", 0, 50)]
        self._mock_admin_offsets(mock_admin, "my-group", tps)
        mock_consumer.get_watermark_offsets.return_value = (0, 100)

        result = adapter.consumer_group_lag("my-group")

        assert len(result) == 1
        ts = result[0].timestamp_utc
        assert ts.tzinfo is not None
        assert ts.tzinfo == timezone.utc
