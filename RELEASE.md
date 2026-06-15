# Release Runbook — kafka-mcp

This document covers how to publish a new `kafka-mcp` release to PyPI and
submit the server listing to Glama.

## Prerequisites

- Python 3.10+ with `hatch` and `twine` installed (`pip install hatch twine`)
- Push access to the GitHub repository
- GitHub Secrets configured (see [Secrets Setup](#secrets-setup) below)

## Tagging Convention

Tags follow [semantic versioning](https://semver.org): `v{MAJOR}.{MINOR}.{PATCH}`
(e.g., `v1.1.0`).

The tag **must** match the `version` field in `pyproject.toml`.

```bash
# 1. Update version in pyproject.toml and server.json
#    e.g., version = "1.1.0"

# 2. Commit the version bump
git add pyproject.toml server.json
git commit -m "chore: bump version to 1.1.0"

# 3. Create and push the tag
git tag v1.1.0
git push origin main --tags
```

## Secrets Setup

The release workflow requires two GitHub repository secrets:

| Secret | Source | Scope |
|--------|--------|-------|
| `TEST_PYPI_TOKEN` | <https://test.pypi.org/manage/account/token/> | Project `kafka-mcp` (or entire account for first upload) |
| `PYPI_TOKEN` | <https://pypi.org/manage/account/token/> | Project `kafka-mcp` |

**How to add secrets:**

1. Go to **GitHub repo → Settings → Secrets and variables → Actions**
2. Click **New repository secret**
3. Enter the name (`TEST_PYPI_TOKEN` or `PYPI_TOKEN`) and paste the token value

**Optional — environment approval gate:**

Configure a `pypi` environment with manual approval in
**GitHub Settings → Environments → pypi**. This adds a human approval step
before the live PyPI publish job runs.

## Local Verification

Run these steps locally before pushing a tag to verify the distribution
is well-formed:

```bash
# Clean previous builds
rm -rf dist/

# Build sdist + wheel
hatch build

# Validate distribution metadata
python -m twine check dist/*
# Expected: PASSED for both .tar.gz and .whl

# (Optional) Test install from built wheel
pip install dist/kafka_mcp-*.whl --force-reinstall
python -c "from kafka_mcp.client import KafkaClient; print('OK')"
```

## CI Pipeline (Automated)

What happens when you push a `v*` tag:

1. **`release.yml`** triggers on the `v*` tag push
2. **Build job:** `hatch build` + `twine check dist/*` — validates the distribution
3. **TestPyPI upload:** dry-run publish to <https://test.pypi.org/project/kafka-mcp/>
4. **PyPI publish:** (requires `pypi` environment approval) publishes to live PyPI
5. Verify on TestPyPI:
   ```bash
   pip install -i https://test.pypi.org/simple/ kafka-mcp=={VERSION}
   ```

**Manual dry-run via workflow_dispatch** (no tag needed):

1. Go to **Actions → "Release" → "Run workflow"**
2. Select branch → click **Run**
3. This runs build + TestPyPI upload only (no live PyPI publish)

## Glama Submission

After the PyPI package is live:

1. Ensure `glama.json` is up to date (tool list, transport config)
2. Go to <https://glama.ai/mcp/servers> and click **Submit Server**
3. Enter the GitHub repository URL: `https://github.com/mshegolev/kafka-mcp`
4. Glama reads `glama.json` from the repo root to populate server metadata
5. Verify the listing shows all tools (`list_topics`, `describe_topic`,
   `search_messages`, `get_message`, `consumer_group_lag`) and both transport
   options (stdio, streamable-http)

## Troubleshooting

| Problem | Cause & Fix |
|---------|-------------|
| `twine check` fails | Check `pyproject.toml` metadata — description, URLs, classifiers |
| TestPyPI 400 "file already exists" | Version already uploaded; bump version or use `--skip-existing` |
| PyPI publish fails | Verify `PYPI_TOKEN` secret is set and scoped to the project |
| Glama submission rejected | Verify `glama.json` schema matches <https://glama.ai/mcp/schemas/server.json> |
| Build produces no wheel | Ensure `[tool.hatch.build.targets.wheel]` is configured in `pyproject.toml` |

## Version History

| Version | Date | Notes |
|---------|------|-------|
| v1.1.0 | TBD | consumer_group_lag tool, HTTP transport, real-broker E2E |
| v0.1.0 | 2026-06-08 | Initial v1.0 MVP release |
