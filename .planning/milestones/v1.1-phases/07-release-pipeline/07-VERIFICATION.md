---
phase: 07-release-pipeline
verified: 2026-06-15T19:47:16Z
status: human_needed
score: 4/4 must-haves verified
overrides_applied: 0
human_verification:
  - test: "Push a v* tag (e.g. v0.1.0-test) to a fork or test branch and verify the release.yml workflow triggers in GitHub Actions"
    expected: "Build job succeeds (hatch build + twine check), TestPyPI upload job runs (may fail without TEST_PYPI_TOKEN secret configured — that is expected)"
    why_human: "CI workflow execution requires a real GitHub Actions runner triggered by a tag push; cannot simulate in local verification"
  - test: "Verify the pypi-publish job does NOT run on workflow_dispatch"
    expected: "When manually triggering the workflow via Actions → Run workflow, only build + testpypi-upload jobs run; pypi-publish is skipped"
    why_human: "Requires real GitHub Actions execution to confirm the if-condition gates the job correctly"
---

# Phase 7: Release Pipeline — Verification Report

**Phase Goal:** A maintainer can publish a versioned release to PyPI and submit to Glama by pushing a git tag; CI pipeline verified against TestPyPI; RELEASE.md runbook.
**Verified:** 2026-06-15T19:47:16Z
**Status:** human_needed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Pushing v* tag triggers CI job: builds sdist+wheels + dry-run TestPyPI upload (twine check passes) | ✓ VERIFIED | `.github/workflows/release.yml` line 8: `tags: ["v*"]`; build job runs `hatch build` (line 32) + `python -m twine check dist/*` (line 35); `testpypi-upload` job runs `twine upload --repository testpypi` (line 65); valid YAML confirmed |
| 2 | RELEASE.md documents: tagging convention, secrets setup, TestPyPI verification, Glama submission steps | ✓ VERIFIED | RELEASE.md (120 lines): "Tagging Convention" section (line 13), "Secrets Setup" section (line 32) with TEST_PYPI_TOKEN + PYPI_TOKEN, "Local Verification" section (line 53) with hatch build + twine check, "CI Pipeline" section (line 74) with TestPyPI flow, "Glama Submission" section (line 93) with glama.ai URL |
| 3 | glama.json and server.json reflect v1.1 (consumer_group_lag tool, HTTP transport) | ✓ VERIFIED | glama.json: 5 tools in correct order [list_topics, describe_topic, search_messages, get_message, consumer_group_lag], all with readOnlyHint:true, serverConfigOptions has streamable-http. server.json: version=1.1.0 (top-level and packages[0]), remotes has streamable-http entry |
| 4 | python -m twine check dist/* passes after hatch build | ✓ VERIFIED | Ran locally: `hatch build` produced `kafka_mcp-0.1.0.tar.gz` + `kafka_mcp-0.1.0-py3-none-any.whl`; `python3 -m twine check dist/*` returned PASSED for both artifacts |

**Score:** 4/4 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `.github/workflows/release.yml` | Tag-triggered release CI: build + TestPyPI dry-run + optional live PyPI publish | ✓ VERIFIED | 93 lines, valid YAML. Has v* tag trigger, workflow_dispatch, 3 jobs (build, testpypi-upload, pypi-publish). pypi-publish gated with `if: github.event_name == 'push' && startsWith(github.ref, 'refs/tags/v')` + `environment: pypi`. id-token:write only on pypi-publish job. No hardcoded secrets. |
| `RELEASE.md` | Step-by-step release runbook for maintainers (min 40 lines) | ✓ VERIFIED | 120 lines. Covers: Prerequisites, Tagging Convention (semver), Secrets Setup (TEST_PYPI_TOKEN, PYPI_TOKEN), Local Verification (hatch build + twine check), CI Pipeline (tag-triggered flow), Glama Submission (glama.ai steps), Troubleshooting, Version History. |
| `glama.json` | Glama MCP server registry metadata for v1.1, contains consumer_group_lag | ✓ VERIFIED | Valid JSON. 5 tools: [list_topics, describe_topic, search_messages, get_message, consumer_group_lag]. All tools have readOnlyHint:true. Has serverConfigOptions with streamable-http transport. Tool names exactly match MCP registrations in `mcp_stdio.py`. |
| `server.json` | MCP server.json discovery metadata for v1.1, contains version 1.1.0 | ✓ VERIFIED | Valid JSON. Top-level version: "1.1.0". packages[0].version: "1.1.0". remotes array has streamable-http entry with correct URL. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `.github/workflows/release.yml` | `pyproject.toml` | `hatch build` reads project metadata | ✓ WIRED | Line 32: `run: hatch build` — hatch reads pyproject.toml for package metadata and version |
| `.github/workflows/release.yml` | TestPyPI | `twine upload --repository testpypi` | ✓ WIRED | Line 65: `python -m twine upload --repository testpypi --skip-existing --verbose dist/*` with env vars TWINE_USERNAME=__token__ and TWINE_PASSWORD from secrets.TEST_PYPI_TOKEN |
| `RELEASE.md` | `.github/workflows/release.yml` | Documents the workflow trigger and verification steps | ✓ WIRED | RELEASE.md "CI Pipeline" section (line 74) documents tag-triggered flow, "Manual dry-run via workflow_dispatch" (line 87), references `release.yml` by name |
| `glama.json` tools | `mcp_stdio.py` registered tools | Tool names must match registered MCP tools | ✓ WIRED | All 5 tool names in glama.json exactly match `name=` registrations in `src/kafka_mcp/adapters/inbound/mcp_stdio.py`: list_topics, describe_topic, search_messages, get_message, consumer_group_lag |
| `server.json` version | `pyproject.toml` version | Version should be consistent | ⚠️ NOTED | server.json=1.1.0, pyproject.toml=0.1.0. **Intentional:** Plan 07-02 explicitly states "pyproject.toml version NOT updated here — reserved for release-time action per RELEASE.md". RELEASE.md documents the version bump process at tag time. This is a pre-release state, not a bug. |
| Secrets naming | `release.yml` ↔ `RELEASE.md` | Secret names must match | ✓ WIRED | Both reference `TEST_PYPI_TOKEN` and `PYPI_TOKEN` consistently |

### Data-Flow Trace (Level 4)

Not applicable — this phase produces CI configuration, documentation, and metadata files (no dynamic data rendering).

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| hatch build produces sdist + wheel | `hatch build` | `kafka_mcp-0.1.0.tar.gz` + `kafka_mcp-0.1.0-py3-none-any.whl` | ✓ PASS |
| twine check validates distribution | `python3 -m twine check dist/*` | PASSED for both .tar.gz and .whl | ✓ PASS |
| release.yml is valid YAML | `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/release.yml'))"` | No errors | ✓ PASS |
| glama.json is valid JSON | `python3 -c "import json; json.load(open('glama.json'))"` | No errors | ✓ PASS |
| server.json is valid JSON | `python3 -c "import json; json.load(open('server.json'))"` | No errors | ✓ PASS |
| glama.json has exactly 5 tools | `python3 -c "..."` (programmatic check) | Tools: [list_topics, describe_topic, search_messages, get_message, consumer_group_lag] — Match: True | ✓ PASS |
| server.json version is 1.1.0 | `python3 -c "..."` (programmatic check) | Top-level: 1.1.0, Package: 1.1.0 | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| REL-01 | 07-01 | Pushing a v* git tag triggers a CI job that builds sdist+wheels and publishes to PyPI; verified E2E against TestPyPI | ✓ SATISFIED | release.yml has v* tag trigger → build (hatch build + twine check) → testpypi-upload → gated pypi-publish. Local hatch build + twine check PASSED. CI workflow structure verified. |
| REL-02 | 07-01, 07-02 | A maintainer can register the server on Glama from in-repo metadata (glama.json/server.json) following RELEASE.md runbook | ✓ SATISFIED | RELEASE.md Glama Submission section documents steps. glama.json has all 5 tools + HTTP transport. server.json at v1.1.0 with HTTP remote. |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| (none) | — | — | — | All four key files are clean: no TODO/FIXME/PLACEHOLDER/stub patterns found |

### Human Verification Required

Two items need human testing because they require actual GitHub Actions execution:

### 1. CI Workflow Trigger on Tag Push

**Test:** Push a `v*` tag (e.g., `v0.1.0-rc1`) to the repository and check the Actions tab
**Expected:** The "Release" workflow triggers. Build job succeeds (hatch build + twine check). TestPyPI upload job runs (may fail without `TEST_PYPI_TOKEN` secret configured — expected for first-time setup).
**Why human:** CI workflow execution requires a real GitHub Actions runner; cannot simulate locally

### 2. PyPI Publish Job Gating on workflow_dispatch

**Test:** Go to Actions → "Release" → "Run workflow" → select a branch → Run
**Expected:** Only `build` and `testpypi-upload` jobs execute. The `pypi-publish` job should be skipped (shown as gray/skipped in the Actions UI) because the `if:` condition restricts it to tag push events only.
**Why human:** Requires real GitHub Actions execution to confirm the if-condition properly gates the job

### Gaps Summary

**No automated gaps found.** All four success criteria from the ROADMAP are satisfied by codebase evidence:

1. **release.yml** correctly implements the tag-triggered CI pipeline with build → TestPyPI upload → gated PyPI publish flow
2. **RELEASE.md** (120 lines) is a comprehensive runbook covering all required topics
3. **glama.json** and **server.json** accurately reflect v1.1 capabilities
4. **hatch build + twine check** passes locally, confirming well-formed distribution

**Note on ROADMAP status:** The ROADMAP shows 07-01-PLAN as `[ ]` (unchecked) and no 07-01-SUMMARY.md exists, but the actual work artifacts (`release.yml`, `RELEASE.md`) ARE committed (commit `6727264`) and fully implemented. This appears to be a bookkeeping gap — the execution completed but the SUMMARY wasn't written and the ROADMAP checkbox wasn't updated. The code is done; only the tracking metadata is stale.

**Version note:** `pyproject.toml` remains at `0.1.0` while `server.json` is at `1.1.0`. This is intentional per the plan — `pyproject.toml` version bump is a release-time action documented in RELEASE.md. Not a gap.

---

_Verified: 2026-06-15T19:47:16Z_
_Verifier: OpenCode (gsd-verifier)_
