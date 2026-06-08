---
phase: "01"
plan: "01-04"
subsystem: "inbound-adapters"
tags: [inbound, mcp-stdio, fastapi, cli, server, tdd, hexagonal, SC-5, KAFKA-01, KAFKA-04, KAFKA-06]
dependency_graph:
  requires:
    - kafka_mcp.adapters.inbound.lib (KafkaClient)
    - kafka_mcp.domain.models (TopicInfo, PartitionInfo)
    - kafka_mcp.domain.errors (TopicNotFoundError)
    - kafka_mcp.adapters.outbound.json_orjson (orjson_dumps)
  provides:
    - kafka_mcp.adapters.inbound.mcp_stdio (create_mcp_server)
    - kafka_mcp.adapters.inbound.rest_api (create_app)
    - kafka_mcp.adapters.inbound.cli (main, run_list_topics, run_describe_topic)
    - kafka_mcp.server (main entry point)
  affects:
    - Phase 1 complete: all four inbound faces reachable (SC-5)
tech_stack:
  added:
    - FastMCP (mcp.server.fastmcp) with ToolAnnotations(readOnlyHint=True)
    - FastAPI + pydantic request models at HTTP boundary (T-04-01)
    - argparse CLI with table + --json output modes
    - uvicorn dispatch in server.py with KAFKA_MCP_HOST/PORT env vars
  patterns:
    - POST /tools/{tool_name} MCP-mirror convention (D-16)
    - ToolAnnotations readOnlyHint defense-in-depth alongside structural assign-based consumer (D-13, D-14)
    - sys.argv dispatch in server.py (--stdio / CLI subcommand / HTTP default)
    - orjson_dumps for machine-readable CLI output
key_files:
  created:
    - src/kafka_mcp/adapters/inbound/mcp_stdio.py
    - src/kafka_mcp/adapters/inbound/rest_api.py
    - src/kafka_mcp/adapters/inbound/cli.py
    - src/kafka_mcp/server.py
    - tests/test_inbound.py
  modified:
    - src/kafka_mcp/adapters/inbound/__init__.py
decisions:
  - "FastMCP (mcp.server.fastmcp.FastMCP) used over low-level mcp.server.Server — simpler decorator API, same readOnlyHint support via annotations=ToolAnnotations(readOnlyHint=True)"
  - "server.py uses sys.argv dispatch: --stdio flag for MCP stdio, known subcommand for CLI, default for uvicorn HTTP (aligns with D-02 library-first multi-face design)"
  - "FastAPI request models (ListTopicsRequest, DescribeTopicRequest) at HTTP boundary validate all inputs (T-04-01 mitigated)"
metrics:
  completed_date: "2026-06-05T12:35:00Z"
  duration_minutes: 15
  tasks_completed: 2
  files_created: 5
  files_modified: 1
  tests_added: 17
  tests_passing: 78
---

# Phase 01 Plan 01-04: MCP stdio + FastAPI + CLI Inbound Adapters Summary

**One-liner:** Three thin inbound adapter faces (MCP stdio via FastMCP, FastAPI POST /tools/*, argparse CLI) wired to KafkaClient, plus server.py dispatch entry point — completing Phase 1 SC-5 (all four faces reachable).

## What Was Built

### Task 1 — MCP stdio + FastAPI adapters (TDD RED then GREEN)

RED gate: wrote 17 tests in `tests/test_inbound.py` covering all three faces — all 17 failed (modules not yet created).

GREEN: created `adapters/inbound/mcp_stdio.py`:
- `create_mcp_server(client: KafkaClient) -> FastMCP` using `mcp.server.fastmcp.FastMCP`
- `list_topics` tool: `ToolAnnotations(readOnlyHint=True)`, accepts `include_internal: bool = False`
- `describe_topic` tool: `ToolAnnotations(readOnlyHint=True)`, accepts `topic: str`, maps `TopicNotFoundError` to `ValueError` for MCP error response
- Both tools delegate to KafkaClient methods (D-13 snake_case, D-14 defense-in-depth)

Created `adapters/inbound/rest_api.py`:
- `create_app(client: KafkaClient) -> FastAPI`
- `POST /tools/list_topics` — body: `{include_internal?: bool}`, response: `{result: list[str]}`
- `POST /tools/describe_topic` — body: `{topic: str}`, response: `{result: TopicInfo.model_dump()}`, 404 on `TopicNotFoundError`
- `ListTopicsRequest` + `DescribeTopicRequest` pydantic models at HTTP boundary (T-04-01)
- No `/topics` or `/describe` resource-style routes (D-16 MCP-mirror convention)

Updated `adapters/inbound/__init__.py` to export `create_mcp_server`, `create_app`.

7 FastAPI + MCP tests passing after Task 1.

### Task 2 — CLI adapter + server.py + full test suite (TDD GREEN)

Created `adapters/inbound/cli.py`:
- `parse_args(argv)`: argparse with `list-topics` and `describe-topic` subparsers; `--json` flag on both; `--include-internal` on list-topics
- `run_list_topics(client, include_internal, as_json)`: human table (left-aligned) or orjson JSON list
- `run_describe_topic(client, topic, as_json)`: partition table with Partition/Leader/Earliest/Latest columns, or orjson JSON; `SystemExit(1)` + stderr on `TopicNotFoundError`
- `main()`: wires argparse + `KafkaClient.from_env()` + dispatch

Created `src/kafka_mcp/server.py`:
- `main()` dispatches on `sys.argv`: `--stdio` → MCP stdio (`server.run("stdio")`), known CLI subcommand → `cli.main(args)`, default → `uvicorn.run(create_app(client))`
- `KAFKA_MCP_HOST` / `KAFKA_MCP_PORT` env vars for uvicorn bind (T-04-06)
- ConfigError raised before server starts if `KAFKA_MCP_BOOTSTRAP_SERVERS` absent (D-04 fail-fast)

Updated `adapters/inbound/__init__.py` to also export `main` from cli.

## Verification

```
pytest tests/ -v           →  78 passed in 0.45s
pytest tests/test_inbound.py -v -k "fastapi or cli"  →  15/15 passed
grep readOnlyHint mcp_stdio.py    →  present on both tools
grep -rn "\.subscribe(" src/      →  no subscribe — read-only OK
FastAPI routes: /tools/list_topics, /tools/describe_topic  →  D-16 confirmed
```

## Phase 1 Success Criteria

| SC | Description | Status |
|----|-------------|--------|
| SC-1 | list_topics reachable via lib facade | ✓ plan 01-03 |
| SC-2 | describe_topic returns PartitionInfo with offsets | ✓ plan 01-03 |
| SC-3 | domain/ has zero I/O imports (hexagonal boundary) | ✓ plan 01-03 |
| SC-4 | assign-based consumer, no subscribe(), no commits | ✓ plan 01-02 |
| SC-5 | list_topics + describe_topic reachable via MCP stdio, FastAPI, CLI | ✓ this plan |

## Deviations from Plan

None — plan executed exactly as written. FastMCP API confirmed as `annotations=ToolAnnotations(readOnlyHint=True)` (not `read_only_hint` keyword arg) via inspection prior to implementation.

## TDD Gate Compliance

- RED commit (`test(01-04): add failing tests...`): `545268c` — all 17 fail
- GREEN Task 1 commit (`feat(01-04): implement MCP stdio + FastAPI...`): `6786f79` — 7 FastAPI/MCP tests pass
- GREEN Task 2 commit (`feat(01-04): implement CLI adapter + server.py...`): `eab4215` — 78/78 all tests pass
- REFACTOR: not needed (code is clean)

## Known Stubs

None — all four inbound faces wire to real KafkaClient (or MockKafkaClient in tests). No placeholder data.

## Threat Flags

No new threat surface beyond the plan's `<threat_model>`:
- T-04-01 mitigated: `DescribeTopicRequest(BaseModel)` validates `topic: str` at HTTP boundary; unknown topics return 404 with structured `{error, topic}` detail only.
- T-04-05 mitigated: `readOnlyHint=True` on both MCP tools via `ToolAnnotations` AND structural assign-based consumer (plan 01-02); defense-in-depth per D-14.
- T-04-06 mitigated: `KafkaClient.from_env()` raises `ConfigError` before uvicorn starts when `KAFKA_MCP_BOOTSTRAP_SERVERS` is absent.

## Self-Check: PASSED

Files confirmed present:
- src/kafka_mcp/adapters/inbound/mcp_stdio.py ✓
- src/kafka_mcp/adapters/inbound/rest_api.py ✓
- src/kafka_mcp/adapters/inbound/cli.py ✓
- src/kafka_mcp/server.py ✓
- tests/test_inbound.py ✓

Commits confirmed:
- 545268c (RED) ✓
- 6786f79 (GREEN Task 1) ✓
- eab4215 (GREEN Task 2) ✓
