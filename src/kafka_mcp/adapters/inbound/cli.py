"""CLI inbound adapter — argparse-based command-line interface for kafka-mcp.

Subcommands:
  kafka-mcp list-topics [--json] [--include-internal]
  kafka-mcp describe-topic <topic> [--json]

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
"""

from __future__ import annotations

import argparse
import sys
from argparse import Namespace

from kafka_mcp.adapters.inbound.lib import KafkaClient
from kafka_mcp.adapters.outbound.json_orjson import orjson_dumps
from kafka_mcp.domain.errors import TopicNotFoundError


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
        print(orjson_dumps(topics).decode())
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
        print(
            f"{p.id:>10}  {p.leader:>8}  {p.earliest:>12}  {p.latest:>12}"
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
        else:
            parser.print_help()
            sys.exit(1)
    finally:
        client.close()
