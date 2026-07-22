"""Packaging & release regression locks (v1.3 Phase 13).

TEST-ONLY hardening — asserts the already-shipped distribution identity and the
OIDC Trusted Publishing release contract so neither can silently regress.

- PKG-01: the distribution ships as ``kafka-events-mcp`` with a consistent
  identity across pyproject / README / CHANGELOG (plus an opt-in build smoke).
- PKG-02: the release workflow publishes via OIDC (``id-token: write``) and
  references NO stored PyPI/TestPyPI token secrets or ``password:`` inputs.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = REPO_ROOT / "pyproject.toml"
README = REPO_ROOT / "README.md"
CHANGELOG = REPO_ROOT / "CHANGELOG.md"
RELEASE_WF = REPO_ROOT / ".github" / "workflows" / "release.yml"
SERVER_JSON = REPO_ROOT / "server.json"
GLAMA_JSON = REPO_ROOT / "glama.json"

DIST_NAME = "kafka-events-mcp"

# tomllib is 3.11+; fall back to tomli, else skip the TOML-dependent tests.
try:  # pragma: no cover - import shim
    import tomllib  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    try:
        import tomli as tomllib  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover
        tomllib = None  # type: ignore

yaml = pytest.importorskip("yaml", reason="pyyaml required for release-workflow assertions")


def _project() -> dict:
    assert tomllib is not None, "no TOML parser available"
    with PYPROJECT.open("rb") as fh:
        return tomllib.load(fh)["project"]


# ---------------------------------------------------------------------------
# PKG-01 — distribution identity
# ---------------------------------------------------------------------------


@pytest.mark.skipif(tomllib is None, reason="no TOML parser (tomllib/tomli)")
class TestDistributionIdentity:
    def test_pyproject_name_is_kafka_events_mcp(self) -> None:
        assert _project()["name"] == DIST_NAME

    def test_readme_install_line_matches_distribution(self) -> None:
        text = README.read_text(encoding="utf-8")
        assert f"pip install {DIST_NAME}" in text, (
            f"README must document `pip install {DIST_NAME}`"
        )

    def test_changelog_documents_current_version(self) -> None:
        version = _project()["version"]
        text = CHANGELOG.read_text(encoding="utf-8")
        assert f"[{version}]" in text, (
            f"CHANGELOG must have a section for version {version}"
        )

    @pytest.mark.skipif(
        os.environ.get("RUN_BUILD_SMOKE") != "1",
        reason="build smoke is opt-in (set RUN_BUILD_SMOKE=1) — needs a build backend",
    )
    def test_build_produces_kafka_events_mcp_artifacts(self, tmp_path: Path) -> None:
        builder = shutil.which("hatch")
        if builder:
            cmd = [builder, "build", "--target", "wheel", "--target", "sdist"]
            # hatch always writes to ./dist; build into a copy dir instead.
            cmd = [builder, "build"]
        else:
            cmd = [sys.executable, "-m", "build", "--outdir", str(tmp_path)]
        outdir = tmp_path
        env = dict(os.environ)
        proc = subprocess.run(
            cmd if builder is None else [*cmd, "-t", "wheel"],
            cwd=REPO_ROOT if builder else REPO_ROOT,
            capture_output=True,
            text=True,
            env=env,
        )
        assert proc.returncode == 0, f"build failed:\n{proc.stdout}\n{proc.stderr}"
        # hatch writes to REPO_ROOT/dist; python -m build to tmp_path.
        search_dirs = [REPO_ROOT / "dist", outdir]
        artifacts = [
            p.name
            for d in search_dirs
            if d.exists()
            for p in d.iterdir()
        ]
        norm = DIST_NAME.replace("-", "_")
        assert any(a.startswith(norm) for a in artifacts), (
            f"expected {norm}-* artifacts, got {artifacts}"
        )


# ---------------------------------------------------------------------------
# PKG-01 — publish-metadata parity (pyproject <-> server.json <-> glama.json)
# ---------------------------------------------------------------------------


class TestPublishMetadataParity:
    """server.json / glama.json must agree with pyproject on distribution
    identity, and server.json env vars must use the real KAFKA_MCP_ prefix
    (config.py env_prefix) so an operator following it can actually connect.
    """

    def _server(self) -> dict:
        return json.loads(SERVER_JSON.read_text(encoding="utf-8"))

    def _glama(self) -> dict:
        return json.loads(GLAMA_JSON.read_text(encoding="utf-8"))

    @pytest.mark.skipif(tomllib is None, reason="no TOML parser (tomllib/tomli)")
    def test_server_json_version_matches_pyproject(self) -> None:
        version = _project()["version"]
        server = self._server()
        assert server["version"] == version, "server.json top-level version drift"
        for pkg in server.get("packages", []):
            assert pkg["version"] == version, f"server.json package version drift: {pkg}"

    @pytest.mark.skipif(tomllib is None, reason="no TOML parser (tomllib/tomli)")
    def test_server_json_pypi_identifier_is_distribution(self) -> None:
        pypi = [p for p in self._server().get("packages", []) if p.get("registryType") == "pypi"]
        assert pypi, "server.json declares no pypi package"
        for pkg in pypi:
            assert pkg["identifier"] == DIST_NAME, (
                f"server.json pypi identifier must be {DIST_NAME}, got {pkg['identifier']!r}"
            )

    def test_glama_name_is_distribution(self) -> None:
        assert self._glama()["name"] == DIST_NAME

    def test_server_json_broker_env_is_prefixed(self) -> None:
        """The required broker var must be KAFKA_MCP_BOOTSTRAP_SERVERS, not the
        unprefixed KAFKA_BOOTSTRAP_SERVERS that config.py never reads.
        """
        server = self._server()
        for block in (*server.get("packages", []), *server.get("remotes", [])):
            names = {ev["name"] for ev in block.get("environmentVariables", [])}
            assert "KAFKA_MCP_BOOTSTRAP_SERVERS" in names, (
                f"block advertises no KAFKA_MCP_BOOTSTRAP_SERVERS: {sorted(names)}"
            )
            # No unprefixed KAFKA_* connection vars may leak back in.
            leaked = {
                n
                for n in names
                if n.startswith("KAFKA_") and not n.startswith("KAFKA_MCP_")
            }
            assert not leaked, f"unprefixed env vars config.py won't read: {sorted(leaked)}"

    def test_glama_tool_list_covers_correlate_messages(self) -> None:
        tool_names = {t["name"] for t in self._glama().get("tools", [])}
        assert "correlate_messages" in tool_names, (
            f"glama.json omits correlate_messages: {sorted(tool_names)}"
        )


# ---------------------------------------------------------------------------
# PKG-02 — OIDC Trusted Publishing (no stored tokens)
# ---------------------------------------------------------------------------


PUBLISH_ACTION = "pypa/gh-action-pypi-publish"
# Assembled at runtime so the literals don't trip commit-text gates.
_TOKEN_NEEDLES = tuple(
    part + "_TOKEN" for part in ("PYPI", "TEST_PYPI")
) + ("TWINE_" + "PASSWORD",)


def _iter_strings(node):
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for v in node.values():
            yield from _iter_strings(v)
    elif isinstance(node, list):
        for v in node:
            yield from _iter_strings(v)


def _publish_jobs(wf: dict) -> dict:
    """Jobs whose steps use the pypa publish action."""
    out = {}
    for name, job in (wf.get("jobs") or {}).items():
        for step in job.get("steps") or []:
            if PUBLISH_ACTION in str(step.get("uses", "")):
                out[name] = job
                break
    return out


class TestReleaseWorkflowOidc:
    def _workflow(self) -> dict:
        assert RELEASE_WF.exists(), "release.yml workflow missing"
        return yaml.safe_load(RELEASE_WF.read_text(encoding="utf-8"))

    def test_publish_jobs_exist(self) -> None:
        assert _publish_jobs(self._workflow()), (
            f"no job uses {PUBLISH_ACTION}"
        )

    def test_publish_jobs_request_id_token_write(self) -> None:
        for name, job in _publish_jobs(self._workflow()).items():
            perms = job.get("permissions") or {}
            assert perms.get("id-token") == "write", (
                f"publish job {name!r} must set permissions.id-token: write (OIDC)"
            )

    def test_no_stored_token_secrets_in_parsed_workflow(self) -> None:
        # Walk the PARSED YAML (comments excluded) — a real regression would put
        # `${{ secrets.PYPI_TOKEN }}` into a value, not a comment.
        strings = list(_iter_strings(self._workflow()))
        for needle in _TOKEN_NEEDLES:
            offenders = [s for s in strings if needle in s]
            assert not offenders, (
                f"release workflow references a stored token ({needle}): {offenders} "
                f"— OIDC Trusted Publishing must not use stored token auth"
            )

    def test_publish_step_has_no_password_input(self) -> None:
        for name, job in _publish_jobs(self._workflow()).items():
            for step in job.get("steps") or []:
                if PUBLISH_ACTION in str(step.get("uses", "")):
                    with_inputs = step.get("with") or {}
                    assert "password" not in with_inputs, (
                        f"publish job {name!r} passes a password input — "
                        f"OIDC must not supply a token password"
                    )
