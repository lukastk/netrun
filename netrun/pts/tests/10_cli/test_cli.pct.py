# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp cli.test_cli

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
import json
import os
from pathlib import Path

import pytest
from typer.testing import CliRunner

from netrun.cli._app import app

# %%
#|export
runner = CliRunner()

SAMPLE_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent / "sample_projects"
BASIC_CONFIG = str(SAMPLE_DIR / "00_basic_net_project" / "main.netrun.json")
POOLS_CONFIG = str(SAMPLE_DIR / "01_thread_and_process_pools" / "main.netrun.json")

# %% [markdown]
# ## Test validate

# %%
#|export
def test_validate_basic():
    result = runner.invoke(app, ["validate", "-c", BASIC_CONFIG])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["valid"] is True
    assert data["nodes"] == 4
    assert data["edges"] == 2


def test_validate_pools():
    result = runner.invoke(app, ["validate", "-c", POOLS_CONFIG])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["valid"] is True
    assert data["nodes"] == 6


def test_validate_not_found():
    result = runner.invoke(app, ["validate", "-c", "/nonexistent/file.netrun.json"])
    assert result.exit_code == 1


def test_validate_pretty():
    result = runner.invoke(app, ["validate", "-c", BASIC_CONFIG, "--pretty"])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["valid"] is True

# %% [markdown]
# ## Test structure

# %%
#|export
def test_structure_basic():
    result = runner.invoke(app, ["structure", "-c", BASIC_CONFIG])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert len(data["nodes"]) == 4
    assert len(data["edges"]) == 2
    # Check edge format
    assert data["edges"][0]["source"] == "double.out"
    assert data["edges"][0]["target"] == "add.a"


def test_structure_node_has_factory():
    result = runner.invoke(app, ["structure", "-c", BASIC_CONFIG])
    data = json.loads(result.stdout)
    node = data["nodes"][0]
    assert node["name"] == "double"
    assert node["factory"] == "netrun.node_factories.from_function"
    assert node["factory_args"]["func"] == "nodes.double"

# %% [markdown]
# ## Test convert

# %%
#|export
def test_convert_json_to_toml():
    result = runner.invoke(app, ["convert", BASIC_CONFIG])
    assert result.exit_code == 0
    # Should contain TOML syntax
    assert "[extra]" in result.stdout or "[[graph.nodes]]" in result.stdout


def test_convert_not_found():
    result = runner.invoke(app, ["convert", "/nonexistent.netrun.json"])
    assert result.exit_code == 1


def test_convert_bad_extension():
    result = runner.invoke(app, ["convert", "/some/file.txt"])
    assert result.exit_code == 1

# %% [markdown]
# ## Test factory-info

# %%
#|export
def test_factory_info():
    result = runner.invoke(app, ["factory-info", "netrun.node_factories.from_function", "--pretty"])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["factory"] == "netrun.node_factories.from_function"
    assert data["type"] == "node"
    assert "description" in data
    assert len(data["params"]) >= 1
    assert data["params"][0]["name"] == "func"
    assert data["params"][0]["required"] is True


def test_factory_info_bad_module():
    result = runner.invoke(app, ["factory-info", "nonexistent.module"])
    assert result.exit_code == 1

# %% [markdown]
# ## Test info

# %%
#|export
def test_info_basic():
    result = runner.invoke(app, ["info", "-c", BASIC_CONFIG, "--pretty"])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["nodes"] == 4
    assert data["edges"] == 2
    assert data["recipes"] == 0


def test_info_pools():
    result = runner.invoke(app, ["info", "-c", POOLS_CONFIG, "--pretty"])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["nodes"] == 6
    assert "pools" in data
    assert data["pools"]["threads"] == "thread"
    assert data["pools"]["processes"] == "multiprocess"
    assert data["recipes"] == 1

# %% [markdown]
# ## Test nodes

# %%
#|export
def test_nodes_basic():
    result = runner.invoke(app, ["nodes", "-c", BASIC_CONFIG])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert len(data) == 4
    names = [n["name"] for n in data]
    assert "double" in names
    assert "add" in names
    assert "format_result" in names
    assert "analyze" in names

# %% [markdown]
# ## Test node

# %%
#|export
def test_node_detail():
    result = runner.invoke(app, ["node", "double", "-c", BASIC_CONFIG, "--pretty"])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["name"] == "double"
    assert data["factory"] == "netrun.node_factories.from_function"
    assert data["factory_args"]["func"] == "nodes.double"


def test_node_not_found():
    result = runner.invoke(app, ["node", "nonexistent", "-c", BASIC_CONFIG])
    assert result.exit_code == 1

# %% [markdown]
# ## Test actions

# %%
#|export
def test_actions_list_basic():
    result = runner.invoke(app, ["actions", "list", "-c", BASIC_CONFIG])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert len(data) == 2
    ids = [a["id"] for a in data]
    assert "action-show-info" in ids
    assert "action-run-project" in ids


def test_actions_run_not_found():
    result = runner.invoke(app, ["actions", "run", "nonexistent", "double", "-c", BASIC_CONFIG])
    assert result.exit_code == 1


def test_actions_run_requires_node_or_global():
    result = runner.invoke(app, ["actions", "run", "some_action", "-c", BASIC_CONFIG])
    assert result.exit_code == 2


def test_actions_run_global_not_found():
    result = runner.invoke(app, ["actions", "run", "nonexistent", "--global", "-c", BASIC_CONFIG])
    assert result.exit_code == 1


def test_actions_run_global_and_node_conflict():
    result = runner.invoke(app, ["actions", "run", "some_action", "double", "--global", "-c", BASIC_CONFIG])
    assert result.exit_code == 2

# %% [markdown]
# ## Test recipes

# %%
#|export
def test_recipes_list_empty():
    result = runner.invoke(app, ["recipes", "list", "-c", BASIC_CONFIG])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data == {}


def test_recipes_list_pools():
    result = runner.invoke(app, ["recipes", "list", "-c", POOLS_CONFIG, "--pretty"])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert "add_node" in data
    assert data["add_node"]["path"] == "./recipes/add_node.py"


def test_recipes_run_not_found():
    result = runner.invoke(app, ["recipes", "run", "nonexistent", "-c", BASIC_CONFIG])
    assert result.exit_code == 1

# %% [markdown]
# ## Test download-agents

# %%
#|export
from unittest.mock import patch, MagicMock
import urllib.error


def _make_mock_urlopen(tree_json: dict, file_contents: dict[str, bytes] | None = None):
    """Create a mock for urllib.request.urlopen that serves tree API and raw file downloads."""
    if file_contents is None:
        file_contents = {}

    def mock_urlopen(req_or_url, **kwargs):
        url = req_or_url.full_url if hasattr(req_or_url, "full_url") else req_or_url
        ctx = MagicMock()
        if "api.github.com" in url:
            ctx.__enter__ = lambda s: s
            ctx.__exit__ = MagicMock(return_value=False)
            ctx.status = 200
            ctx.read.return_value = json.dumps(tree_json).encode()
        elif "raw.githubusercontent.com" in url:
            # Find matching file content by checking which path the URL ends with
            content = b"default content"
            for path, data in file_contents.items():
                if url.endswith(path):
                    content = data
                    break
            ctx.__enter__ = lambda s: s
            ctx.__exit__ = MagicMock(return_value=False)
            ctx.read.return_value = content
        else:
            raise urllib.error.URLError(f"unexpected URL: {url}")
        return ctx

    return mock_urlopen


def test_download_agents_success(tmp_path):
    tree = {
        "tree": [
            {"path": "agents/README.md", "type": "blob"},
            {"path": "agents/skills/foo/SKILL.md", "type": "blob"},
            {"path": "src/main.py", "type": "blob"},  # should be ignored
            {"path": "agents/subdir", "type": "tree"},  # should be ignored (not a blob)
        ]
    }
    file_contents = {
        "agents/README.md": b"# Agents README",
        "agents/skills/foo/SKILL.md": b"# Foo Skill",
    }
    mock_fn = _make_mock_urlopen(tree, file_contents)
    out_dir = str(tmp_path / "agents")

    with patch("netrun.cli._download_agents.urllib.request.urlopen", side_effect=mock_fn):
        result = runner.invoke(app, ["download-agents", out_dir])

    assert result.exit_code == 0
    assert "Found 2 file(s)" in result.output
    assert "Downloaded 2/2" in result.output

    # Verify files written
    assert (tmp_path / "agents" / "README.md").read_bytes() == b"# Agents README"
    assert (tmp_path / "agents" / "skills" / "foo" / "SKILL.md").read_bytes() == b"# Foo Skill"


def test_download_agents_no_files(tmp_path):
    tree = {"tree": [{"path": "src/main.py", "type": "blob"}]}
    mock_fn = _make_mock_urlopen(tree)
    out_dir = str(tmp_path / "agents")

    with patch("netrun.cli._download_agents.urllib.request.urlopen", side_effect=mock_fn):
        result = runner.invoke(app, ["download-agents", out_dir])

    assert result.exit_code == 1
    assert "No files found" in result.output


def test_download_agents_network_error(tmp_path):
    out_dir = str(tmp_path / "agents")

    with patch(
        "netrun.cli._download_agents.urllib.request.urlopen",
        side_effect=urllib.error.URLError("connection refused"),
    ):
        result = runner.invoke(app, ["download-agents", out_dir])

    assert result.exit_code == 1
    assert "failed to fetch tree" in result.output


def test_download_agents_branch_option(tmp_path):
    tree = {"tree": [{"path": "agents/INSTRUCTIONS.md", "type": "blob"}]}
    mock_fn = _make_mock_urlopen(tree, {"agents/INSTRUCTIONS.md": b"instructions"})
    out_dir = str(tmp_path / "agents")

    with patch("netrun.cli._download_agents.urllib.request.urlopen", side_effect=mock_fn) as _:
        result = runner.invoke(app, ["download-agents", out_dir, "-b", "dev"])

    assert result.exit_code == 0
    assert "branch: dev" in result.output
    assert (tmp_path / "agents" / "INSTRUCTIONS.md").read_bytes() == b"instructions"


def test_download_agents_partial_failure(tmp_path):
    tree = {
        "tree": [
            {"path": "agents/good.md", "type": "blob"},
            {"path": "agents/bad.md", "type": "blob"},
        ]
    }

    call_count = 0

    def mock_urlopen(req_or_url, **kwargs):
        nonlocal call_count
        url = req_or_url.full_url if hasattr(req_or_url, "full_url") else req_or_url
        if "api.github.com" in url:
            ctx = MagicMock()
            ctx.__enter__ = lambda s: s
            ctx.__exit__ = MagicMock(return_value=False)
            ctx.status = 200
            ctx.read.return_value = json.dumps(tree).encode()
            return ctx
        # First raw download succeeds, second fails
        call_count += 1
        if call_count == 1:
            ctx = MagicMock()
            ctx.__enter__ = lambda s: s
            ctx.__exit__ = MagicMock(return_value=False)
            ctx.read.return_value = b"good content"
            return ctx
        raise urllib.error.URLError("not found")

    out_dir = str(tmp_path / "agents")

    with patch("netrun.cli._download_agents.urllib.request.urlopen", side_effect=mock_urlopen):
        result = runner.invoke(app, ["download-agents", out_dir])

    assert result.exit_code == 0
    assert "Downloaded 1/2" in result.output
    assert "FAILED" in result.output
    assert (tmp_path / "agents" / "good.md").read_bytes() == b"good content"

# %% [markdown]
# ## Test help

# %%
#|export
def test_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "validate" in result.stdout
    assert "structure" in result.stdout
    assert "convert" in result.stdout
    assert "factory-info" in result.stdout
    assert "info" in result.stdout
    assert "nodes" in result.stdout
    assert "node" in result.stdout
    assert "actions" in result.stdout
    assert "recipes" in result.stdout
    assert "download-agents" in result.stdout

# %% [markdown]
# ## Regression: validate reports resolution failures

# %%
#|export
import tempfile

def test_validate_reports_resolution_failure():
    """Test that validate reports resolution errors instead of silently swallowing them.

    Regression test: netrun validate suppresses resolution failures by catching
    all exceptions and setting resolved=None, reporting valid=True.
    """
    config_data = {
        "graph": {
            "nodes": [
                {
                    "name": "TestNode",
                    "factory": "nonexistent_module_that_does_not_exist.factory",
                    "factory_args": {},
                    "execution_config": {
                        "pools": ["main"],
                    },
                }
            ],
            "edges": [],
        },
        "pools": {
            "main": {"spec": {"type": "main"}},
        },
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".netrun.json", delete=False) as f:
        json.dump(config_data, f)
        f.flush()
        result = runner.invoke(app, ["validate", "-c", f.name])

    # Resolution failures are reported as warnings (not errors, since resolution
    # depends on Python path at runtime). The key requirement: the failure must
    # NOT be silently swallowed — it must appear in the output.
    data = json.loads(result.stdout)
    warnings = data.get("warnings", [])
    errors = data.get("errors", [])
    all_messages = warnings + errors
    assert any(
        "resolution" in str(m).lower() or "module" in str(m).lower()
        for m in all_messages
    ), f"validate silently swallowed resolution failure: {data}"
