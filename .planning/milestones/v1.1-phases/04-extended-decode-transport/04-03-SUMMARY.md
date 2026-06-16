---
phase: "04-extended-decode-transport"
plan: "03"
subsystem: "inbound-http-transport"
tags: ["http", "mcp", "fastmcp", "transport", "streamable-http"]
dependency_graph:
  requires:
    - "04-01"
    - "04-02"
  provides:
    - "HTTP-01: FastMCP streamable-HTTP MCP transport at /mcp"
    - "server.json remotes entry for HTTP transport discovery"
    - "glama.json serverConfigOptions for HTTP mode documentation"
  affects:
    - "src/kafka_mcp/adapters/inbound/rest_api.py"
    - "server.json"
    - "glama.json"
    - "tests/test_inbound.py"
tech_stack:
  added:
    - "FastMCP streamable-HTTP ASGI app mounted on FastAPI via app.mount('/mcp')"
    - "StreamableHTTPSessionManager lifecycle integrated into FastAPI lifespan"
  patterns:
    - "TDD RED/GREEN cycle for HTTP mount test"
    - "streamable_http_path='/' + FastAPI prefix-stripping for correct routing"
    - "Session manager started in FastAPI lifespan to initialize async task group"
key_files:
  created: []
  modified:
    - "src/kafka_mcp/adapters/inbound/rest_api.py"
    - "server.json"
    - "glama.json"
    - "tests/test_inbound.py"
decisions:
  - "streamable_http_path='/' — FastAPI strips /mcp prefix before routing to sub-app; sub-app must handle '/' not '/mcp'"
  - "Session manager started in FastAPI lifespan — Starlette Mount doesn't propagate sub-app lifespans; must start explicitly"
  - "TestClient returns 421 (MCP transport security rejecting 'testserver' Host) — not 404; confirms mount exists"
  - "server.json uses 'remotes' array per MCP schema (not fabricated 'transports' key)"
metrics:
  duration_minutes: 28
  completed_date: "2026-06-08T14:28:11Z"
  tasks_completed: 2
  files_modified: 4
---

# Phase 04 Plan 03: HTTP Transport Mount Summary

FastMCP streamable-HTTP MCP endpoint mounted at /mcp on the FastAPI server; HTTP
transport declared truthfully in server.json remotes array and glama.json serverConfigOptions.

## What Was Built

**Task 1 (TDD): Mount FastMCP streamable-HTTP at /mcp in create_app()**

Added `_create_http_mcp_server()` helper to `rest_api.py` that registers all four
read-only tools on a `FastMCP("kafka-mcp-http", streamable_http_path="/")` instance.
`create_app()` now:
1. Creates the HTTP MCP server and calls `streamable_http_app()` (lazy init)
2. Accesses `session_manager` to prepare lifespan integration
3. Starts the StreamableHTTPSessionManager inside the FastAPI lifespan context
4. Mounts the ASGI app at `/mcp` via `app.mount("/mcp", mcp_asgi_app)`

**Task 2: Declare HTTP transport in server.json and glama.json**

- `server.json`: Added top-level `"remotes"` array with one entry:
  `{"name": "kafka-mcp-http", "type": "streamable-http", "url": "http://localhost:8000/mcp"}`
- `glama.json`: Added `"serverConfigOptions"` array documenting the HTTP run mode;
  existing `"serverConfig"` (stdio default) is unchanged.

## Key Technical Discovery

**FastMCP route prefix mismatch:** `streamable_http_app()` creates a Starlette app
with an internal route at `self.settings.streamable_http_path` (default `/mcp`). When
FastAPI mounts this at `/mcp`, it strips the prefix before routing to the sub-app, so
the sub-app receives requests at `/` — but the app only has a route at `/mcp`.

**Fix:** Configure `FastMCP("kafka-mcp-http", streamable_http_path="/")` so the
sub-app's internal route is `/`, which the prefix-stripped requests match.

**FastMCP lifespan not propagated:** Starlette's `Mount` class does not propagate
sub-app lifespans. The FastMCP session manager (`StreamableHTTPSessionManager`) must be
explicitly started via `async with _mcp_session_manager.run()` inside the parent
FastAPI lifespan context. Without this, requests fail with:
`RuntimeError: Task group is not initialized. Make sure to use run()`

**TestClient Host header:** MCP transport security returns HTTP 421 ("Misdirected
Request") for requests with `Host: testserver` (the default starlette TestClient host).
This is the expected response — **not** a 404 — confirming the mount exists. Tests
assert `status_code != 404`.

## Tests Added

- `TestHttpMcpMount.test_mcp_mount_returns_non_404`: GET /mcp/ returns 421 (not 404)
- `TestHttpMcpMount.test_mcp_mount_post_returns_non_404`: POST /mcp/ returns 421 (not 404)
- `TestHttpMcpMount.test_existing_tools_routes_unaffected`: POST /tools/list_topics still 200
- Added `close()` no-op to `MockKafkaClient` (needed when using `with TestClient(...)` which triggers lifespan teardown)

**TDD cycle:** RED committed `8bc03c9` → GREEN committed `6872cf8`

## Verification Results

```
pytest tests/test_inbound.py -k "http_mcp or HttpMcp" → 3 passed
pytest tests/ → 260 passed (no regressions)

server.json assertion:
  OK: server.json remotes entry valid
  OK: server.json shape reconciled (remotes, not transports)

glama.json assertion:
  OK: glama.json serverConfigOptions entry valid
  OK: glama.json stdio default intact
```

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] MockKafkaClient missing close() method**
- **Found during:** Task 1, RED phase — `with TestClient(client_app)` triggers lifespan shutdown
- **Issue:** `MockKafkaClient` had no `close()` method; FastAPI lifespan calls `client.close()` on shutdown
- **Fix:** Added `def close(self) -> None: pass` to `MockKafkaClient` in tests/test_inbound.py
- **Files modified:** tests/test_inbound.py
- **Commit:** 8bc03c9

**2. [Rule 1 - Bug] FastMCP prefix-route mismatch with FastAPI mount**
- **Found during:** Task 1, GREEN phase — tests returned 404 after initial mount
- **Issue:** `streamable_http_app()` defaults to `streamable_http_path='/mcp'`; FastAPI prefix-strips `/mcp` before routing to sub-app, leaving requests at `/` with no matching route
- **Fix:** Configure `FastMCP("kafka-mcp-http", streamable_http_path="/")` so sub-app handles `/` correctly
- **Files modified:** src/kafka_mcp/adapters/inbound/rest_api.py

**3. [Rule 1 - Bug] FastMCP session manager not initialized in TestClient context**
- **Found during:** Task 1, GREEN phase — after fix #2, tests returned 500 RuntimeError
- **Issue:** Starlette `Mount` does not propagate sub-app lifespans; `StreamableHTTPSessionManager` never initialized → `RuntimeError: Task group is not initialized`
- **Fix:** Integrated `async with _mcp_session_manager.run()` inside FastAPI lifespan context
- **Files modified:** src/kafka_mcp/adapters/inbound/rest_api.py

## Threat Flags

No new security-relevant surface beyond what is documented in the plan's threat model:
- T-04-07: /mcp endpoint has no auth (accepted, localhost default, HTTP-02 deferred)
- T-04-08: read-only guarantee enforced via assign-only consumer + readOnlyHint
- T-04-09: declared URL matches actual mount path

## TDD Gate Compliance

- RED gate commit: `8bc03c9` (`test(04-03): add failing test for FastMCP /mcp HTTP mount (RED)`)
- GREEN gate commit: `6872cf8` (`feat(04-03): mount FastMCP streamable-HTTP at /mcp in create_app (GREEN)`)
- REFACTOR: not needed (code is clear)

Both gates present and in correct order.

## Self-Check: PASSED

Files verified:
- src/kafka_mcp/adapters/inbound/rest_api.py: exists, contains `/mcp` mount and session manager
- server.json: exists, contains `"remotes"` array
- glama.json: exists, contains `"serverConfigOptions"` array

Commits verified:
- 8bc03c9: test RED commit (present)
- 6872cf8: feat GREEN commit (present)
- 9797aa3: feat server.json/glama.json commit (present)
