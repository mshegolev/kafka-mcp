# Phase 7: Release Pipeline - Context

**Gathered:** 2026-06-16
**Status:** Ready for planning
**Mode:** Auto-generated (infrastructure phase — discuss skipped)

<domain>
## Phase Boundary

A maintainer can publish a versioned release to PyPI and submit to Glama by pushing a
git tag; the CI pipeline is verified end-to-end against TestPyPI, and a RELEASE.md
runbook documents every human-gated step.

In scope: GitHub Actions release workflow (tag-triggered), TestPyPI dry-run upload,
twine check, hatch build verification, RELEASE.md runbook, glama.json/server.json
updates for v1.1 (consumer_group_lag tool + HTTP transport).

Out of scope: actual live PyPI publish (human-gated), actual Glama account submission
(human-gated), PyPI credentials management.
</domain>

<decisions>
## Implementation Decisions

### OpenCode's Discretion
All implementation choices are at OpenCode's discretion — pure infrastructure phase.
Use ROADMAP phase goal, success criteria, and codebase conventions to guide decisions.

Key constraints from success criteria:
- v* tag push triggers CI job that builds sdist + wheels + dry-run TestPyPI upload
- RELEASE.md documents tagging, secrets setup, TestPyPI verification, Glama submission
- glama.json and server.json reflect v1.1 (consumer_group_lag, HTTP transport)
- twine check dist/* passes locally after hatch build
- Live PyPI push and Glama submission remain human actions (out of scope)
</decisions>

<code_context>
## Existing Code Insights

Codebase context will be gathered during plan-phase research.
</code_context>

<specifics>
## Specific Ideas

- GitHub Actions workflow with v* tag trigger
- hatch build for sdist + wheels
- twine check for distribution validation
- TestPyPI upload as dry-run verification
- RELEASE.md with step-by-step runbook
</specifics>

<deferred>
## Deferred Ideas

None — discuss phase skipped.
</deferred>
