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
    assert data["nodes"] == 3
    assert data["edges"] == 2


def test_validate_pools():
    result = runner.invoke(app, ["validate", "-c", POOLS_CONFIG])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["valid"] is True
    assert data["nodes"] == 5


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
    assert len(data["nodes"]) == 3
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
    assert data["has_get_node_funcs"] is True
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
    assert data["nodes"] == 3
    assert data["edges"] == 2
    assert data["recipes"] == 0


def test_info_pools():
    result = runner.invoke(app, ["info", "-c", POOLS_CONFIG, "--pretty"])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["nodes"] == 5
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
    assert len(data) == 3
    names = [n["name"] for n in data]
    assert "double" in names
    assert "add" in names
    assert "format_result" in names

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
def test_actions_list_empty():
    result = runner.invoke(app, ["actions", "list", "-c", BASIC_CONFIG])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data == []


def test_actions_run_not_found():
    result = runner.invoke(app, ["actions", "run", "nonexistent", "-c", BASIC_CONFIG])
    assert result.exit_code == 1

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
