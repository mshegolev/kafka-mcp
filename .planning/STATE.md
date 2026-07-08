---
gsd_state_version: 1.0
milestone: v1.3
milestone_name: mTLS & Packaging Hardening
current_phase: 3
status: Awaiting next milestone
stopped_at: Completed 12-01-PLAN.md (HINT-01 + PARSE-01 test-only hardening)
last_updated: "2026-07-08T19:41:10.145Z"
last_activity: 2026-07-08
last_activity_desc: Milestone v1.3 completed and archived
progress:
  total_phases: 6
  completed_phases: 3
  total_plans: 5
  completed_plans: 5
  percent: 50
current_phase_name: Tool Surface Robustness Coverage
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-07-08)

**Core value:** Read-only Kafka MCP brick — find events by key in a time window,
decode Avro/Protobuf/JSON via Schema Registry, correlate across topics, surface
evidence for incident timelines. Library-first: works in pytest without MCP or
FastAPI.
**Current focus:** Milestone v1.3 — mTLS & Packaging Hardening

## Current Position

Phase: Milestone v1.3 complete
Plan: —
Status: Awaiting next milestone
Last activity: 2026-07-08 — Milestone v1.3 completed and archived

## Performance Metrics

**Velocity (baseline through v1.2):**

- Total plans completed: 25 (v1.0: 12, v1.1: 9, v1.2: 3 — 1 plan/phase)
- Average duration: — min
- Total execution time: 0 hours

**By Phase (v1.0 baseline):**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 01 | 4 | - | - |
| 02 | 5 | - | - |
| 03 | 3 | - | - |

*Updated after each plan completion*
| Phase 12 P01 | 6min | 2 tasks | 1 files |
| Phase 11 P02 | 12min | 1 tasks | 1 files |
| Phase 11 P01 | 28min | 2 tasks | 2 files |
| Phase 12 P02 | 6min | 2 tasks | 1 files |

## Accumulated Context

### Decisions

- [v1.0] Structurally read-only: assign-only, enable.auto.commit=false, throwaway consumer group
- [v1.0] Library-first (D2): MCP/REST/CLI are thin inbound adapters
- [v1.0] Hexagonal boundary enforced by a test (grep for I/O imports in domain/ports)
- [v1.0] FastMCP + ToolAnnotations(readOnlyHint=True) for MCP stdio adapter
- [v1.0] Rust scanner NOT added — I/O-bound benchmark result (KAFKA-07)
- [v1.0] "Prepare-don't-live-publish" posture: REL phases deliver verified pipeline + runbook; live push is human-gated
- [v1.1] Key lesson carried in: declare + install deps in the same change to avoid CI-only conflicts
- [v1.2] Phase 8 extends search_messages (additive params); Phase 9 adds correlate_messages (new domain service); Phase 10 wires 4-face symmetry
- [v1.3] Hardening milestone: much code already landed during development (mTLS wiring, hint annotations, _parse_iso_utc, kafka-events-mcp rename, OIDC release workflow) — phases verify end-to-end + add tests/docs, not greenfield
- [v1.3] Tool surface frozen and brick stays read-only for the milestone
- [Phase ?]: MTLS-02: env-gated real-broker mTLS e2e test (skipif on broker+cert+key+CA env) — reuses staging contour via env, no cert material committed (11-02)
- [Phase 11]: MTLS-01: assert ssl.* on both consumer + admin conf via one parametrized test capturing both builders (11-01)
- [Phase 11]: Made test_adapters.py config tests hermetic (_env_file=None + ambient KAFKA_MCP_* clearing) so a developer .env cannot leak real ssl.* cert paths (11-01)
- [Phase 12]: HINT-01 — derive tool set from list_tools() (not hardcoded) so new tools can't skip the readOnly/idempotent/openWorld hint check; pin both _READ_ONLY constants (mcp_stdio + rest_api) (12-01)
- [Phase 12]: PARSE-01 — lock _parse_iso_utc actionable param-named ValueError + trailing-Z/naive→UTC handling as test-only regression coverage (12-01)
- [Phase ?]: COV-01/COV-02 face coverage: dedicated CorrelateFakeClient captures initial_results to prove base64 raw + timestamp round-trip; MockKafkaClient lacks correlate_messages

### Pending Todos

None yet.

### Blockers/Concerns

None. v1.3 roadmap created; 3 phases (11–13), 9 requirements mapped. Ready for plan-phase.

## Session Continuity

Last session: 2026-07-08T18:47:47.287Z
Stopped at: Completed 12-01-PLAN.md (HINT-01 + PARSE-01 test-only hardening)
Resume file: None
Next action: `/gsd-execute-phase 12` (plan 12-02: COV-01/COV-02)
</content>

## Operator Next Steps

- Start the next milestone with /gsd-new-milestone
