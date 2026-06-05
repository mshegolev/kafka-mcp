"""SchemaRegistryHttpAdapter — HTTP adapter for Confluent Schema Registry.

Phase 1 stub: wired to SchemaRegistryPort but returns None for all
get_schema() calls.  The full Avro/Protobuf/JSON decode (KAFKA-05) is
deferred to Phase 2.

T-02-02 (STRIDE): sr_pass is stored as an attribute but never logged or
printed.  Phase 2 must use httpx BasicAuth and must not log the
Authorization header.
"""

from __future__ import annotations


class SchemaRegistryHttpAdapter:
    """Stub HTTP adapter for the Confluent Schema Registry.

    Implements :class:`kafka_mcp.ports.schema_registry.SchemaRegistryPort`.

    In Phase 1 this adapter is wired but returns ``None`` for all calls so
    downstream code can rely on the port contract without triggering real
    HTTP requests.  Phase 2 will replace the stub body with a real
    ``httpx`` GET request.

    Example::

        adapter = SchemaRegistryHttpAdapter(
            url="http://schema-registry:8081",
            user="alice",
            password="secret",
        )
        schema = adapter.get_schema("payments-value")  # returns None in Phase 1
    """

    def __init__(
        self,
        url: str | None,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        """Initialise the adapter with connection parameters.

        Args:
            url: Base URL of the Schema Registry
                (e.g. ``"http://registry:8081"``).  Pass ``None`` to run
                in stub mode (no HTTP calls will be made).
            user: Optional basic-auth username (D-03).
            password: Optional basic-auth password — not logged (T-02-02).
        """
        self._url = url
        self._user = user
        self._password = password

    def decode(self, raw: bytes) -> dict | None:
        """Decode a raw wire-format message payload to a plain dict.

        Phase 2 stub — full implementation (Confluent magic-byte framing +
        Avro/Protobuf/JSON fallback) delivered in plan 02-03.

        Returns:
            ``None`` in Phase 1 stub.
        """
        # Phase 2 implementation replaces this stub in plan 02-03.
        return None

    def get_schema(self, subject: str) -> dict | None:
        """Fetch the latest schema for the given subject.

        Phase 1 stub: always returns ``None``.

        Phase 2 will implement::

            GET {url}/subjects/{subject}/versions/latest

        using ``httpx`` with optional BasicAuth.  The response body is a
        dict with keys such as ``id``, ``schema``, ``schemaType``.

        Args:
            subject: Schema subject name (e.g. ``"payments-value"``).

        Returns:
            ``None`` in Phase 1 (stub).  Phase 2 returns the schema dict
            or ``None`` when the subject does not exist.
        """
        # Phase 1 stub — url not yet wired to a real HTTP call.
        # Phase 2: uncomment and implement below.
        #
        # if self._url is None:
        #     return None
        # auth = (
        #     httpx.BasicAuth(self._user, self._password)
        #     if self._user and self._password
        #     else None
        # )
        # with httpx.Client(auth=auth) as client:
        #     response = client.get(
        #         f"{self._url}/subjects/{subject}/versions/latest"
        #     )
        #     if response.status_code == 404:
        #         return None
        #     response.raise_for_status()
        #     return response.json()
        return None
