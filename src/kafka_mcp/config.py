"""KafkaMcpSettings — pydantic-settings configuration for kafka-mcp.

Reads all settings from environment variables prefixed with KAFKA_MCP_.
Raises ConfigError (not a generic pydantic ValidationError) when
required fields are absent or invalid, so callers can handle config
problems with a single except clause.

T-01-01 (STRIDE): sasl_password and sr_pass are stored as SecretStr so
that __repr__ / __str__ never expose credential values in logs.
"""

from __future__ import annotations

from pydantic import SecretStr, ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from kafka_mcp.domain.errors import ConfigError


class KafkaMcpSettings(BaseSettings):
    """Environment-driven settings for the kafka-mcp brick.

    All variables are read from the environment (or a .env file) with the
    KAFKA_MCP_ prefix.  Missing / empty ``bootstrap_servers`` raises
    :class:`kafka_mcp.domain.errors.ConfigError` immediately (fail-fast,
    D-04).

    Example::

        # .env
        KAFKA_MCP_BOOTSTRAP_SERVERS=broker1:9092,broker2:9092
        KAFKA_MCP_SECURITY_PROTOCOL=SASL_SSL
        KAFKA_MCP_SASL_MECHANISM=PLAIN
        KAFKA_MCP_SASL_USERNAME=alice
        KAFKA_MCP_SASL_PASSWORD=secret
    """

    def __init__(self, **data: object) -> None:
        """Wrap pydantic ValidationError in ConfigError for cleaner caller UX.

        Pydantic catches ValueError subclasses (including ConfigError) raised
        by @model_validator and re-wraps them in ValidationError.  We intercept
        here and re-raise as ConfigError so callers have a single domain-typed
        exception to handle (D-04 fail-fast contract).
        """
        try:
            super().__init__(**data)
        except ValidationError as exc:
            # Convert any field-level error into a ConfigError so callers
            # have a single domain-typed exception to handle (D-04).
            for error in exc.errors():
                err_type = error.get("type", "")
                loc = error.get("loc", ())
                field = loc[0] if loc else "unknown"

                if err_type == "missing":
                    # Required env var not set — name the missing key
                    env_key = f"KAFKA_MCP_{str(field).upper()}"
                    raise ConfigError(
                        f"{env_key} is required but was not set"
                    ) from exc

                if err_type == "value_error":
                    msg = error.get("msg", str(exc))
                    # pydantic prefixes with "Value error, " — strip if present
                    msg = msg.removeprefix("Value error, ")
                    raise ConfigError(msg) from exc

            # No recognised error type — re-raise ValidationError as-is
            raise

    model_config = SettingsConfigDict(
        env_prefix="KAFKA_MCP_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        # Never include secrets in the model's string representations
        # (covers T-01-01 STRIDE threat).
        hide_input_in_errors=True,
    )

    # ------------------------------------------------------------------ #
    # Connection                                                          #
    # ------------------------------------------------------------------ #

    bootstrap_servers: str
    """Comma-separated list of Kafka broker addresses (required)."""

    security_protocol: str = "PLAINTEXT"
    """librdkafka security.protocol (PLAINTEXT, SASL_PLAINTEXT, SASL_SSL)."""

    # ------------------------------------------------------------------ #
    # SASL passthrough (D-02)                                             #
    # ------------------------------------------------------------------ #

    sasl_mechanism: str | None = None
    """SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI."""

    sasl_username: str | None = None
    """SASL username (plain text; not a secret)."""

    sasl_password: SecretStr | None = None
    """SASL password — stored as SecretStr, never logged (T-01-01)."""

    # ------------------------------------------------------------------ #
    # Schema Registry (D-03)                                              #
    # ------------------------------------------------------------------ #

    schema_registry_url: str | None = None
    """Base URL of the Schema Registry (e.g. http://sr:8081)."""

    sr_user: str | None = None
    """Schema Registry basic-auth username."""

    sr_pass: SecretStr | None = None
    """Schema Registry basic-auth password — SecretStr (T-01-01)."""

    # ------------------------------------------------------------------ #
    # Scan limits (D-07, D-08)                                            #
    # ------------------------------------------------------------------ #

    max_scan: int = 100_000
    """Maximum messages to scan per operation (prevents unbounded scans)."""

    poll_timeout: float = 1.0
    """consumer.poll() timeout in seconds."""

    # ------------------------------------------------------------------ #
    # Validation                                                          #
    # ------------------------------------------------------------------ #

    @model_validator(mode="after")
    def _require_bootstrap_servers(self) -> "KafkaMcpSettings":
        """Fail fast if bootstrap_servers is absent or whitespace-only.

        Raises:
            ConfigError: Named error listing the missing/invalid key.
        """
        if not self.bootstrap_servers or not self.bootstrap_servers.strip():
            raise ConfigError(
                "KAFKA_MCP_BOOTSTRAP_SERVERS is required but was not set"
            )
        return self
