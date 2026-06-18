"""CLI inbound adapter — argparse-based command-line interface for kafka-mcp.

Subcommands:
  kafka-mcp list-topics [--json] [--include-internal]
  kafka-mcp describe-topic <topic> [--json]
  kafka-mcp search-messages --key <key> [options] [--json]
  kafka-mcp get-message <topic> <partition> <offset> [--json]
  kafka-mcp consumer-group-lag --group <group> [--topics T1,T2] [--json]

By default, output is formatted as a human-readable table.
Pass ``--json`` for machine-readable orjson output.

Wires to KafkaClient (D-15, D-02 library-first): all data retrieval is
delegated to :class:`~kafka_mcp.adapters.inbound.lib.KafkaClient`.

Usage::

    # Programmatic
    from kafka_mcp.adapters.inbound.cli import main
    main()   # reads sys.argv

    # Entry point
    kafka-mcp list-topics
    kafka-mcp list-topics --json
    kafka-mcp describe-topic payments
    kafka-mcp describe-topic payments --json
    kafka-mcp search-messages --key ORD-123
    kafka-mcp search-messages --key ORD-123 --json
    kafka-mcp get-message orders 0 42
    kafka-mcp get-message orders 0 42 --json
"""

from __future__ import annotations

import argparse
import base64
import sys
from argparse import Namespace
from datetime import datetime, timezone

from kafka_mcp.adapters.inbound.lib import KafkaClient
from kafka_mcp.adapters.outbound.json_orjson import orjson_dumps
from kafka_mcp.domain.errors import (
    DecodeError,
    MessageNotFoundError,
    TopicNotFoundError,
    TransientError,
)
from kafka_mcp.domain.models import KafkaMessage, LagRecord

# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    """Construct and return the top-level argument parser."""
    parser = argparse.ArgumentParser(
        prog="kafka-mcp",
        description="Read-only Kafka CLI — inspect topics and messages.",
    )
    subparsers = parser.add_subparsers(dest="subcommand")

    # list-topics
    lt = subparsers.add_parser(
        "list-topics",
        help="List all topic names on the broker.",
    )
    lt.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output as JSON array instead of table.",
    )
    lt.add_argument(
        "--include-internal",
        dest="include_internal",
        action="store_true",
        default=False,
        help="Include internal topics (e.g. __consumer_offsets).",
    )

    # describe-topic
    dt = subparsers.add_parser(
        "describe-topic",
        help="Show partition metadata and watermark offsets for a topic.",
    )
    dt.add_argument(
        "topic",
        help="Exact topic name to describe.",
    )
    dt.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output as JSON instead of table.",
    )

    # search-messages
    sm = subparsers.add_parser(
        "search-messages",
        help="Search Kafka messages by key within an optional time window.",
    )
    sm.add_argument(
        "--key",
        required=True,
        help="The value to match (message key, header value, or value field).",
    )
    sm.add_argument(
        "--key-field",
        dest="key_field",
        default=None,
        help=(
            "Match field: None/'key' for message key, "
            "'header:<name>' for a header, 'value:<dotted.path>' for value field."
        ),
    )
    sm.add_argument(
        "--topics",
        default=None,
        help="Comma-separated list of topic names to scan. Defaults to all topics.",
    )
    sm.add_argument(
        "--headers",
        default=None,
        help="Comma-separated list of header key-value pairs to filter by (e.g., 'trace_id=abc,key2=value2').",
    )
    sm.add_argument(
        "--time-from",
        dest="time_from",
        default=None,
        help="Start of time window as ISO8601 datetime (e.g. 2026-01-01T00:00:00Z).",
    )
    sm.add_argument(
        "--time-to",
        dest="time_to",
        default=None,
        help="End of time window as ISO8601 datetime. Defaults to now.",
    )
    sm.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Maximum number of matching messages to return (default 500).",
    )
    sm.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output as JSON instead of table.",
    )

    # get-message
    gm = subparsers.add_parser(
        "get-message",
        help="Fetch and decode a single Kafka message by exact coordinates.",
    )
    gm.add_argument("topic", help="Topic name.")
    gm.add_argument("partition", type=int, help="Partition index (0-based).")
    gm.add_argument("offset", type=int, help="Exact message offset.")
    gm.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output as JSON instead of human-readable format.",
    )

    # consumer-group-lag
    cgl = subparsers.add_parser(
        "consumer-group-lag",
        help="Report per-partition consumer lag for a consumer group.",
    )
    cgl.add_argument(
        "--group",
        required=True,
        help="Consumer group ID.",
    )
    cgl.add_argument(
        "--topics",
        default=None,
        help="Comma-separated list of topic names. Defaults to all committed topics.",
    )
    cgl.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output as JSON instead of table.",
    )

    # correlate-messages
    cm = subparsers.add_parser(
        "correlate-messages",
        help="Correlate messages by following extracted IDs into additional topics.",
    )
    cm.add_argument(
        "--key",
        required=True,
        help="The value to match for initial search.",
    )
    cm.add_argument(
        "--key-field",
        dest="key_field",
        default=None,
        help=(
            "Match field for initial search: None/'key' for message key, "
            "'header:<name>' for a header, 'value:<dotted.path>' for value field."
        ),
    )
    cm.add_argument(
        "--initial-topics",
        dest="initial_topics",
        default=None,
        help="Comma-separated list of topic names for initial search. Defaults to all topics.",
    )
    cm.add_argument(
        "--follow-topics",
        dest="follow_topics",
        required=True,
        help="Comma-separated list of topic names to follow correlations into.",
    )
    cm.add_argument(
        "--time-from",
        dest="time_from",
        default=None,
        help="Start of time window as ISO8601 datetime (e.g. 2026-01-01T00:00:00Z).",
    )
    cm.add_argument(
        "--time-to",
        dest="time_to",
        default=None,
        help="End of time window as ISO8601 datetime. Defaults to now.",
    )
    cm.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Maximum number of correlated messages to return (default 500).",
    )
    cm.add_argument(
        "--regex-patterns",
        dest="regex_patterns",
        default=None,
        help="Comma-separated list of regex patterns for ID extraction.",
    )
    cm.add_argument(
        "--jsonpath-expressions",
        dest="jsonpath_expressions",
        default=None,
        help="Comma-separated list of JSONPath expressions for ID extraction.",
    )
    cm.add_argument(
        "--max-depth",
        dest="max_depth",
        type=int,
        default=None,
        help="Maximum correlation depth.",
    )
    cm.add_argument(
        "--max-breadth",
        dest="max_breadth",
        type=int,
        default=None,
        help="Maximum correlation breadth per level.",
    )
    cm.add_argument(
        "--bidirectional",
        dest="bidirectional",
        action="store_true",
        default=False,
        help="Enable backward correlation traversal.",
    )
    cm.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output as JSON instead of table.",
    )

    return parser


def parse_args(argv: list[str] | None = None) -> Namespace:
    """Parse *argv* (or sys.argv[1:] when None) and return a Namespace.

    The returned Namespace always carries:
    - ``subcommand``: "list-topics" | "describe-topic"
    - ``json``: bool
    - ``include_internal``: bool  (list-topics only)
    - ``topic``: str              (describe-topic only)

    Args:
        argv: Argument list to parse.  Defaults to ``sys.argv[1:]``.

    Returns:
        Parsed :class:`argparse.Namespace`.
    """
    return _build_parser().parse_args(argv)


# ---------------------------------------------------------------------------
# Runner functions
# ---------------------------------------------------------------------------


def run_list_topics(
    client: KafkaClient,
    include_internal: bool = False,
    as_json: bool = False,
) -> None:
    """Fetch and print topic names.

    Args:
        client: KafkaClient to query.
        include_internal: Include ``__``-prefixed internal topics.
        as_json: When True, print a JSON array; otherwise print a table.
    """
    topics = client.list_topics(include_internal=include_internal)

    if as_json:
        print(orjson_dumps({"result": topics}).decode())
        return

    if not topics:
        print("(no topics)")
        return

    # Human-readable table: header + separator + rows
    max_len = max(len(t) for t in topics)
    col = max(max_len, len("Topic"))
    print(f"{'Topic':<{col}}")
    print("-" * col)
    for topic in topics:
        print(f"{topic:<{col}}")


def run_describe_topic(
    client: KafkaClient,
    topic: str,
    as_json: bool = False,
) -> None:
    """Fetch and print partition metadata for *topic*.

    Args:
        client: KafkaClient to query.
        topic: Exact topic name to describe.
        as_json: When True, print JSON; otherwise print a partition table.

    Raises:
        SystemExit(1): When *topic* does not exist on the broker.
    """
    try:
        info = client.describe_topic(topic)
    except TopicNotFoundError as exc:
        print(f"Error: topic '{exc.topic}' not found", file=sys.stderr)
        sys.exit(1)

    if as_json:
        print(orjson_dumps(info.model_dump()).decode())
        return

    # Human-readable partition table
    print(f"\nTopic: {info.name}  Partitions: {info.partition_count}\n")
    print(f"{'Partition':>10}  {'Leader':>8}  {'Earliest':>12}  {'Latest':>12}")
    print("-" * 50)
    for p in info.partitions:
        print(f"{p.id:>10}  {p.leader:>8}  {p.earliest:>12}  {p.latest:>12}")


def _serialize_message_for_cli(msg: KafkaMessage) -> dict:
    """Serialize a KafkaMessage to a JSON-safe dict for CLI output.

    Converts ``raw`` bytes to a base64-encoded ASCII string.  orjson would
    natively serialize bytes as base64, but explicit conversion ensures the
    same shape is used in both --json and table modes.

    Args:
        msg: A :class:`~kafka_mcp.domain.models.KafkaMessage` domain object.

    Returns:
        Dict with all fields from ``model_dump()`` plus ``raw`` replaced
        by its base64-encoded ASCII representation.
    """
    data = msg.model_dump()
    data["raw"] = base64.b64encode(msg.raw).decode("ascii")
    # KEY-02: base64-encode raw_key bytes when present (T-04-06)
    if msg.raw_key is not None:
        data["raw_key"] = base64.b64encode(msg.raw_key).decode("ascii")
    # Convert datetime to ISO-8601 string for JSON compatibility
    if isinstance(data.get("timestamp_utc"), datetime):
        data["timestamp_utc"] = data["timestamp_utc"].isoformat()
    return data


def _serialize_lag_record_for_cli(record: LagRecord) -> dict:
    """Serialize a LagRecord to a JSON-safe dict for CLI output.

    Converts ``timestamp_utc`` to an ISO-8601 string. LagRecord has no
    bytes fields, so no base64 encoding is needed.

    Args:
        record: A :class:`~kafka_mcp.domain.models.LagRecord` domain object.

    Returns:
        Dict with all fields from ``model_dump()`` plus ``timestamp_utc``
        as an ISO-8601 string.
    """
    data = record.model_dump()
    if isinstance(data.get("timestamp_utc"), datetime):
        data["timestamp_utc"] = data["timestamp_utc"].isoformat()
    return data


def run_search_messages(
    client: KafkaClient,
    key: str,
    key_field: str | None = None,
    topics: str | None = None,
    headers: str | None = None,
    time_from_str: str | None = None,
    time_to_str: str | None = None,
    limit: int = 500,
    as_json: bool = False,
) -> None:
    """Search and print messages matching *key*.

    Args:
        client: KafkaClient to query.
        key: The value to match.
        key_field: Optional match field (None/"key", "header:<name>",
            "value:<dotted.path>").
        topics: Optional comma-separated topic names string.  Split on
            comma and stripped.  Defaults to None (all topics).
        headers: Optional comma-separated header key-value pairs string.
            Split on comma and parsed as key=value pairs.
        time_from_str: Optional ISO8601 datetime string for start of window.
        time_to_str: Optional ISO8601 datetime string for end of window.
        limit: Maximum number of matching messages (default 500).
        as_json: When True, print JSON; otherwise print a table.
    """
    # Parse datetime strings (T-02-05-D: fromisoformat is safe stdlib)
    tf: datetime | None = None
    if time_from_str is not None:
        tf = datetime.fromisoformat(time_from_str)
        if tf.tzinfo is None:
            tf = tf.replace(tzinfo=timezone.utc)

    tt: datetime | None = None
    if time_to_str is not None:
        tt = datetime.fromisoformat(time_to_str)
        if tt.tzinfo is None:
            tt = tt.replace(tzinfo=timezone.utc)

    # Parse comma-separated topics
    topics_list: list[str] | None = None
    if topics is not None:
        topics_list = [t.strip() for t in topics.split(",") if t.strip()]

    # Parse comma-separated header key-value pairs
    headers_dict: dict[str, str] | None = None
    if headers is not None:
        headers_dict = {}
        for pair in headers.split(","):
            pair = pair.strip()
            if "=" in pair:
                k, v = pair.split("=", 1)
                headers_dict[k.strip()] = v.strip()

    results = client.search_messages(
        key,
        key_field=key_field,
        topics=topics_list,
        headers=headers_dict,
        time_from=tf,
        time_to=tt,
        limit=limit,
    )

    if as_json:
        serialized = [_serialize_message_for_cli(m) for m in results]
        print(orjson_dumps(serialized).decode())
        return

    # Human-readable table: Timestamp | Topic | Partition | Offset | Key | Value
    if not results:
        print("(no matching messages)")
        return

    col_ts = 26
    col_topic = max(max(len(m.topic) for m in results), len("Topic"))
    col_part = max(len("Partition"), 9)
    col_off = max(len("Offset"), 6)
    col_key = max(max(len(str(m.key or "")) for m in results), len("Key"), 10)

    header = (
        f"{'Timestamp':<{col_ts}}  "
        f"{'Topic':<{col_topic}}  "
        f"{'Partition':>{col_part}}  "
        f"{'Offset':>{col_off}}  "
        f"{'Key':<{col_key}}  "
        f"Value"
    )
    print(header)
    print("-" * (len(header) + 10))
    for msg in results:
        ts_str = msg.timestamp_utc.isoformat()[:col_ts]
        val_str = str(msg.value or "")[:80]
        print(
            f"{ts_str:<{col_ts}}  "
            f"{msg.topic:<{col_topic}}  "
            f"{msg.partition:>{col_part}}  "
            f"{msg.offset:>{col_off}}  "
            f"{str(msg.key or ''):<{col_key}}  "
            f"{val_str}"
        )


def run_get_message(
    client: KafkaClient,
    topic: str,
    partition: int,
    offset: int,
    as_json: bool = False,
) -> None:
    """Fetch and print a single message.

    Args:
        client: KafkaClient to query.
        topic: Topic name.
        partition: Partition index (0-based).
        offset: Exact message offset.
        as_json: When True, print JSON; otherwise print human-readable output.

    Raises:
        SystemExit(1): When no message exists at the given coordinates.
        SystemExit(2): When the payload cannot be decoded.
    """
    try:
        msg = client.get_message(topic, partition, offset)
    except MessageNotFoundError as exc:
        print(
            f"Error: no message at {exc.topic}[{exc.partition}]@{exc.offset}",
            file=sys.stderr,
        )
        sys.exit(1)
    except TransientError as exc:
        # WR-05: in-range offset that timed out — transient, not a real absence.
        print(
            f"Error: transient read failure for {exc.topic}[{exc.partition}]@{exc.offset}: {exc.reason}",
            file=sys.stderr,
        )
        sys.exit(3)
    except DecodeError as exc:
        print(
            f"Error: decode failed for {exc.topic}[{exc.partition}]@{exc.offset}: {exc.reason}",
            file=sys.stderr,
        )
        sys.exit(2)

    if as_json:
        print(orjson_dumps(_serialize_message_for_cli(msg)).decode())
        return

    # Human-readable output
    print(f"\nMessage: {msg.topic}[{msg.partition}]@{msg.offset}")
    print(f"  Key           : {msg.key}")
    print(f"  Timestamp UTC : {msg.timestamp_utc.isoformat()}")
    print(f"  Source        : {msg.source}")
    print(f"  Event type    : {msg.event_type}")
    if msg.headers:
        print("  Headers:")
        for k, v in msg.headers.items():
            print(f"    {k}: {v}")
    print("  Evidence keys:")
    for k, v in msg.keys.items():
        print(f"    {k}: {v}")
    val_str = str(msg.value)[:512] if msg.value is not None else "(none)"
    print(f"  Value         : {val_str}")


def run_consumer_group_lag(
    client: KafkaClient,
    group: str,
    topics: str | None = None,
    as_json: bool = False,
) -> None:
    """Fetch and print per-partition consumer lag.

    Args:
        client: KafkaClient to query.
        group: Consumer group ID.
        topics: Optional comma-separated topic names string.
        as_json: When True, print JSON; otherwise print a table.
    """
    # Parse comma-separated topics
    topics_list: list[str] | None = None
    if topics is not None:
        topics_list = [t.strip() for t in topics.split(",") if t.strip()]

    records = client.consumer_group_lag(group, topics_list)

    if as_json:
        serialized = [_serialize_lag_record_for_cli(r) for r in records]
        print(orjson_dumps(serialized).decode())
        return

    if not records:
        print("(no lag records)")
        return

    # Human-readable table: Group | Topic | Partition | Current | End | Lag
    col_group = max(max(len(r.group) for r in records), len("Group"))
    col_topic = max(max(len(r.topic) for r in records), len("Topic"))

    header = (
        f"{'Group':<{col_group}}  {'Topic':<{col_topic}}  {'Partition':>9}  {'Current':>10}  {'End':>10}  {'Lag':>10}"
    )
    print(header)
    print("-" * len(header))
    for r in records:
        print(
            f"{r.group:<{col_group}}  "
            f"{r.topic:<{col_topic}}  "
            f"{r.partition:>9}  "
            f"{r.current_offset:>10}  "
            f"{r.end_offset:>10}  "
            f"{r.lag:>10}"
        )


def run_correlate_messages(
    client: KafkaClient,
    key: str,
    follow_topics: str,
    key_field: str | None = None,
    initial_topics: str | None = None,
    time_from_str: str | None = None,
    time_to_str: str | None = None,
    limit: int = 500,
    regex_patterns: str | None = None,
    jsonpath_expressions: str | None = None,
    max_depth: int | None = None,
    max_breadth: int | None = None,
    bidirectional: bool = False,
    as_json: bool = False,
) -> None:
    """Correlate messages by following extracted IDs into additional topics.

    Args:
        client: KafkaClient to query.
        key: The value to match for initial search.
        key_field: Optional match field for initial search.
        initial_topics: Optional comma-separated initial topic names string.
        follow_topics: Comma-separated follow topic names string.
        time_from_str: Optional ISO8601 datetime string for start of window.
        time_to_str: Optional ISO8601 datetime string for end of window.
        limit: Maximum number of correlated messages (default 500).
        regex_patterns: Optional comma-separated regex patterns for ID extraction.
        jsonpath_expressions: Optional comma-separated JSONPath expressions for ID extraction.
        max_depth: Optional maximum correlation depth.
        max_breadth: Optional maximum correlation breadth per level.
        bidirectional: Enable backward correlation traversal.
        as_json: When True, print JSON; otherwise print a table.
    """
    # Parse datetime strings
    tf: datetime | None = None
    if time_from_str is not None:
        tf = datetime.fromisoformat(time_from_str)
        if tf.tzinfo is None:
            tf = tf.replace(tzinfo=timezone.utc)

    tt: datetime | None = None
    if time_to_str is not None:
        tt = datetime.fromisoformat(time_to_str)
        if tt.tzinfo is None:
            tt = tt.replace(tzinfo=timezone.utc)

    # Parse comma-separated initial topics
    initial_topics_list: list[str] | None = None
    if initial_topics is not None:
        initial_topics_list = [t.strip() for t in initial_topics.split(",") if t.strip()]

    # Parse comma-separated follow topics (required)
    follow_topics_list = [t.strip() for t in follow_topics.split(",") if t.strip()]
    if not follow_topics_list:
        print("Error: --follow-topics is required", file=sys.stderr)
        sys.exit(1)

    # Parse comma-separated regex patterns
    regex_patterns_list: list[str] | None = None
    if regex_patterns is not None:
        regex_patterns_list = [p.strip() for p in regex_patterns.split(",") if p.strip()]

    # Parse comma-separated JSONPath expressions
    jsonpath_expressions_list: list[str] | None = None
    if jsonpath_expressions is not None:
        jsonpath_expressions_list = [e.strip() for e in jsonpath_expressions.split(",") if e.strip()]

    # Perform initial search
    initial_results = client.search_messages(
        key,
        key_field=key_field,
        topics=initial_topics_list,
        time_from=tf,
        time_to=tt,
        limit=limit,
    )

    # Perform correlation
    correlated_results = client.correlate_messages(
        initial_results=initial_results,
        follow_topics=follow_topics_list,
        limit=limit,
        regex_patterns=regex_patterns_list,
        jsonpath_expressions=jsonpath_expressions_list,
        max_depth=max_depth,
        max_breadth=max_breadth,
        bidirectional=bidirectional,
    )

    if as_json:
        serialized = [_serialize_message_for_cli(m) for m in correlated_results]
        print(orjson_dumps({"result": serialized}).decode())
        return

    # Human-readable table
    if not correlated_results:
        print("(no correlated messages)")
        return

    col_ts = 26
    col_topic = max(max(len(m.topic) for m in correlated_results), len("Topic"))
    col_part = max(len("Partition"), 9)
    col_off = max(len("Offset"), 6)
    col_key = max(max(len(str(m.key or "")) for m in correlated_results), len("Key"), 10)
    col_chain = max(max(len("->".join(m.correlation_chain)) for m in correlated_results), len("Correlation Chain"), 20)

    header = (
        f"{'Timestamp':<{col_ts}}  "
        f"{'Topic':<{col_topic}}  "
        f"{'Partition':>{col_part}}  "
        f"{'Offset':>{col_off}}  "
        f"{'Key':<{col_key}}  "
        f"{'Correlation Chain':<{col_chain}}  "
        f"Value"
    )
    print(header)
    print("-" * (len(header) + 20))
    for msg in correlated_results:
        ts_str = msg.timestamp_utc.isoformat()[:col_ts]
        val_str = str(msg.value or "")[:60]
        chain_str = "->".join(msg.correlation_chain)[:col_chain]
        print(
            f"{ts_str:<{col_ts}}  "
            f"{msg.topic:<{col_topic}}  "
            f"{msg.partition:>{col_part}}  "
            f"{msg.offset:>{col_off}}  "
            f"{str(msg.key or ''):<{col_key}}  "
            f"{chain_str:<{col_chain}}  "
            f"{val_str}"
        )


# ---------------------------------------------------------------------------
# main entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    """Parse arguments and dispatch to the appropriate runner.

    Args:
        argv: Argument list.  Defaults to ``sys.argv[1:]``.
    """
    parser = _build_parser()
    ns = parser.parse_args(argv)

    if ns.subcommand is None:
        parser.print_help()
        sys.exit(0)

    client = KafkaClient.from_env()

    # WR-02: ensure the underlying librdkafka Consumer is closed even though
    # the process is short-lived, so the documented cleanup contract holds.
    try:
        if ns.subcommand == "list-topics":
            run_list_topics(
                client,
                include_internal=ns.include_internal,
                as_json=ns.json,
            )
        elif ns.subcommand == "describe-topic":
            run_describe_topic(client, ns.topic, as_json=ns.json)
        elif ns.subcommand == "search-messages":
            run_search_messages(
                client,
                key=ns.key,
                key_field=ns.key_field,
                topics=ns.topics,
                headers=ns.headers,
                time_from_str=ns.time_from,
                time_to_str=ns.time_to,
                limit=ns.limit,
                as_json=ns.json,
            )
        elif ns.subcommand == "get-message":
            run_get_message(
                client,
                topic=ns.topic,
                partition=ns.partition,
                offset=ns.offset,
                as_json=ns.json,
            )
        elif ns.subcommand == "consumer-group-lag":
            run_consumer_group_lag(
                client,
                group=ns.group,
                topics=ns.topics,
                as_json=ns.json,
            )
        elif ns.subcommand == "correlate-messages":
            run_correlate_messages(
                client,
                key=ns.key,
                follow_topics=ns.follow_topics,
                key_field=ns.key_field,
                initial_topics=ns.initial_topics,
                time_from_str=ns.time_from,
                time_to_str=ns.time_to,
                limit=ns.limit,
                regex_patterns=ns.regex_patterns,
                jsonpath_expressions=ns.jsonpath_expressions,
                max_depth=ns.max_depth,
                max_breadth=ns.max_breadth,
                bidirectional=ns.bidirectional,
                as_json=ns.json,
            )
        else:
            parser.print_help()
            sys.exit(1)
    finally:
        client.close()
