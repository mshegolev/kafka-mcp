---
phase: "03-native-ship"
plan: "02"
subsystem: distribution
tags: [packaging, glama, pypi, changelog, license, metadata]
dependency_graph:
  requires:
    - "03-01 (scanner seam + EVALUATION.md present)"
  provides:
    - "glama.json (Glama registry entry, schema-valid)"
    - "server.json (MCP server declaration: stdio + kafka-mcp PyPI package)"
    - "CHANGELOG.md (v0.1.0 release notes, Keep a Changelog format)"
    - "LICENSE (MIT, 2024-present)"
    - "pyproject.toml complete publish metadata (classifiers, license, authors, urls, readme)"
  affects:
    - "CI publish workflow (can now build and publish a tagged release)"
tech_stack:
  added: []
  patterns:
    - "Keep a Changelog (https://keepachangelog.com/en/1.0.0/)"
    - "SemVer (https://semver.org/spec/v2.0.0.html)"
    - "MCP server schema (static.modelcontextprotocol.io/schemas/2025-12-11/server.schema.json)"
    - "Glama server schema (glama.ai/mcp/schemas/server.json)"
key_files:
  created:
    - "glama.json"
    - "server.json"
    - "CHANGELOG.md"
    - "LICENSE"
  modified:
    - "pyproject.toml (classifiers, license, authors, keywords, readme, urls)"
decisions:
  - "server.json uses MCP 2025-12-11 schema shape (mirrors jaeger-mcp published pattern)"
  - "glama.json maintainers field set to mshegolev (mirrors jaeger-mcp glama.json)"
  - "pyproject.toml authors set to 'kafka-mcp contributors' (generic, CI sets real maintainer)"
  - "OWNER placeholder used in all URLs (CI/release process substitutes real GitHub org)"
metrics:
  duration: "~10 min"
  completed: "2026-06-08T07:15:00Z"
  tasks: 2
  files: 5
---

# Phase 3 Plan 02: Distribution Artifacts Summary

**One-liner:** SC-5 distribution artifacts complete — glama.json, server.json (stdio/PyPI),
CHANGELOG.md (v0.1.0), MIT LICENSE, and full pyproject.toml publish metadata for kafka-mcp.

## What Was Built

### Task 1: glama.json + server.json + pyproject.toml metadata — commit 7c82257

**server.json** at repo root — follows the MCP server registry schema
(`static.modelcontextprotocol.io/schemas/2025-12-11/server.schema.json`), matching the
jaeger-mcp published pattern. Declares stdio transport, PyPI package `kafka-mcp` v0.1.0,
and seven environment variable declarations (KAFKA_BOOTSTRAP_SERVERS, security protocol,
SASL credentials, Schema Registry URL, SSL verify).

**glama.json** at repo root — Glama registry entry with `$schema` ref, name, displayName,
description, vendor, sourceUrl, homepage, license=MIT, category, tags, serverConfig
(command=kafka-mcp --stdio), all four tools with readOnlyHint, and maintainers.

**pyproject.toml** updated: added `readme`, `license`, `authors`, `keywords`, `classifiers`
(Development Status Beta, Intended Audience Developers, License MIT, OS Independent,
Python 3.10/3.11/3.12, Topic Software Dev Libraries + System Monitoring), and
`[project.urls]` (Homepage, Repository, Issues) — all using `OWNER/kafka-mcp` placeholder.

### Task 2: LICENSE (MIT) + CHANGELOG.md — commit e5c0d9a

**LICENSE** — canonical MIT License text (OSI-standard), "2024-present kafka-mcp contributors",
complete grant + warranty disclaimer clauses.

**CHANGELOG.md** — Keep a Changelog format, v0.1.0 dated 2026-06-08. Added section covers:
KafkaClient lib facade (all four operations), hexagonal architecture, ConfluentConsumerAdapter
(assign-based read-only, five-condition bounded loop), SchemaRegistryHttpAdapter (Avro/Protobuf/
JSON Schema), all four inbound faces, evidence extraction (Investigator Contract fields),
scanner seam (try-import _native guard), EVALUATION.md + KAFKA-07 benchmark gate, MIT license.

## Test Result

190 tests pass (`python3 -m pytest tests/ -x -q --ignore=tests/benchmarks`).
No regressions introduced — all files are static data/metadata, not Python source.

## Final Verifications

| Check | Result |
|-------|--------|
| glama.json valid JSON, name=kafka-mcp | PASS |
| glama.json license=MIT, all 4 tools present | PASS |
| server.json transport=stdio, identifier=kafka-mcp | PASS |
| LICENSE grep "Permission is hereby granted" count=1 | PASS |
| CHANGELOG.md grep "[0.1.0]" count=2 (header + link) | PASS |
| pyproject.toml grep "classifiers" count=1 | PASS |
| uv run pytest 190 tests pass | PASS |

## Deviations from Plan

**1. [Rule 1 - Adaptation] server.json structure follows MCP 2025-12-11 schema**

- **Found during:** Task 1 — reading jaeger-mcp's published server.json
- **Issue:** The plan's example server.json shape (`transport`, `package.registry`,
  `package.name`, `package.installCommand`) is an older draft format. The published
  jaeger-mcp uses the newer MCP server schema with `packages[].registryType`,
  `packages[].identifier`, `packages[].transport.type`.
- **Fix:** Used the newer schema format matching jaeger-mcp and the
  `$schema` pointer from `static.modelcontextprotocol.io/schemas/2025-12-11/server.schema.json`.
  This ensures compatibility with Claude Desktop / MCP registry tooling.
- **Impact:** server.json structure differs from plan example but satisfies the done
  criteria (transport=stdio, package name=kafka-mcp) using the canonical field paths.
- **Files modified:** server.json

## Threat Surface Scan

No new network endpoints, auth paths, file access patterns, or schema changes introduced.
All created files are static data (JSON, Markdown, text). No secrets or credentials embedded.

## Self-Check: PASSED

Files created:
- `/opt/develop/aiqa/mcps/kafka-mcp/glama.json` — FOUND
- `/opt/develop/aiqa/mcps/kafka-mcp/server.json` — FOUND
- `/opt/develop/aiqa/mcps/kafka-mcp/CHANGELOG.md` — FOUND
- `/opt/develop/aiqa/mcps/kafka-mcp/LICENSE` — FOUND

Files modified:
- `/opt/develop/aiqa/mcps/kafka-mcp/pyproject.toml` — FOUND (classifiers present)

Commits present:
- `7c82257` — FOUND (feat(03-02): glama.json + server.json + pyproject.toml)
- `e5c0d9a` — FOUND (feat(03-02): LICENSE + CHANGELOG.md)
