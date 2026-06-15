---
phase: 07-release-pipeline
plan: 02
subsystem: metadata
tags: [release, glama, server-json, v1.1, metadata]
dependency_graph:
  requires: [05-consumer-lag, 04-http-transport]
  provides: [v1.1-metadata]
  affects: [glama-submission, mcp-client-discovery]
tech_stack:
  added: []
  patterns: [json-metadata-registry]
key_files:
  created: []
  modified:
    - glama.json
    - server.json
decisions:
  - "No version field in glama.json — Glama reads version from PyPI/GitHub"
  - "pyproject.toml version NOT updated here — reserved for release-time action per RELEASE.md"
  - "consumer_group_lag description matches mcp_stdio.py registration exactly"
metrics:
  duration: "66s"
  completed: "2026-06-15T19:41:45Z"
  tasks_completed: 2
  tasks_total: 2
  files_modified: 2
---

# Phase 7 Plan 2: Update glama.json + server.json for v1.1 Summary

**One-liner:** Added consumer_group_lag as 5th tool in glama.json and bumped server.json to v1.1.0, preserving HTTP transport metadata from Phase 4.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Update glama.json for v1.1 | 446d7cf | glama.json |
| 2 | Update server.json version to 1.1.0 | 2c85225 | server.json |

## Changes Made

### Task 1: Update glama.json for v1.1
- Appended `consumer_group_lag` tool entry to the `tools` array (5th tool)
- Tool description: "Report per-partition consumer lag (committed offset vs end offset) for a given consumer group. Read-only — no commits, no group joins."
- Set `readOnlyHint: true` matching existing tool pattern
- Preserved `serverConfigOptions` with HTTP streamable-http transport (Phase 4)
- Preserved all existing tools: list_topics, describe_topic, search_messages, get_message

### Task 2: Update server.json version to 1.1.0
- Bumped top-level `version` from `"0.1.0"` to `"1.1.0"`
- Bumped `packages[0].version` from `"0.1.0"` to `"1.1.0"`
- Preserved `remotes` array with streamable-http entry (Phase 4)
- Preserved all environment variable definitions for both stdio and HTTP transports

## Deviations from Plan

None — plan executed exactly as written.

## Verification Results

| Check | Result |
|-------|--------|
| glama.json valid JSON | PASS |
| server.json valid JSON | PASS |
| glama.json has 5 tools in correct order | PASS |
| All tools have readOnlyHint: true | PASS |
| glama.json has serverConfigOptions (HTTP) | PASS |
| server.json top-level version = 1.1.0 | PASS |
| server.json packages[0].version = 1.1.0 | PASS |
| server.json has streamable-http remote | PASS |

## Known Stubs

None — both files contain production-ready metadata.

## Threat Flags

No new security-relevant surface introduced. Changes are metadata-only (JSON registry files).

## Self-Check: PASSED

- glama.json: FOUND
- server.json: FOUND
- 07-02-SUMMARY.md: FOUND
- Commit 446d7cf (Task 1): FOUND
- Commit 2c85225 (Task 2): FOUND
