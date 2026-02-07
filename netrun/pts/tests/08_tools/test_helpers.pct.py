# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp tools.test_helpers

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
import pytest
from netrun.tools._helpers import (
    get_project_actions,
    get_node_actions,
    get_available_actions,
    get_action_settings,
    get_node_env,
    build_action_context,
    get_recipes,
)

# %%
#|export
def test_get_project_actions_empty():
    assert get_project_actions({}) == []
    assert get_project_actions({"ui": {}}) == []


def test_get_project_actions():
    extra = {
        "ui": {
            "actions": [
                {"id": "run", "label": "Run", "command": "echo run"},
            ]
        }
    }
    actions = get_project_actions(extra)
    assert len(actions) == 1
    assert actions[0].id == "run"
    assert actions[0].command == "echo run"


def test_get_node_actions():
    extra = {
        "ui": {
            "actions": [
                {"id": "debug", "label": "Debug", "command": "echo debug"},
            ]
        }
    }
    actions = get_node_actions(extra)
    assert len(actions) == 1
    assert actions[0].id == "debug"


def test_get_available_actions_merge():
    graph_extra = {
        "ui": {
            "actions": [
                {"id": "run", "label": "Run", "command": "echo run"},
                {"id": "test", "label": "Test", "command": "pytest"},
            ]
        }
    }
    node_extra = {
        "ui": {
            "actions": [
                {"id": "run", "label": "Run (node)", "command": "echo node_run"},
            ]
        }
    }
    actions = get_available_actions(graph_extra, node_extra)
    action_map = {a.id: a for a in actions}
    # Node's "run" should override project's "run"
    assert action_map["run"].label == "Run (node)"
    assert "test" in action_map


def test_get_action_settings():
    extra = {
        "ui": {
            "defaultCmd": "python",
            "env": {"FOO": "bar"},
            "actions": [{"id": "x", "label": "X", "command": "y"}],
        }
    }
    settings = get_action_settings(extra, project_root="/opt")
    assert settings["projectRoot"] == "/opt"
    assert settings["defaultCmd"] == "python"
    assert settings["env"] == {"FOO": "bar"}


def test_get_node_env():
    assert get_node_env({}) is None
    assert get_node_env({"ui": {}}) is None
    assert get_node_env({"ui": {"env": {}}}) is None
    assert get_node_env({"ui": {"env": {"A": "B"}}}) == {"A": "B"}


def test_build_action_context():
    graph_extra = {
        "ui": {
            "defaultCmd": "python",
            "env": {"PROJECT_VAR": "val"},
        }
    }
    node_extra = {
        "ui": {
            "env": {"NODE_VAR": "nval"},
        }
    }
    ctx = build_action_context(
        graph_extra,
        node_name="MyNode",
        node_id="n1",
        node_extra=node_extra,
        net_file_path="/tmp/test.json",
        project_root="/tmp",
        node_config='{"name": "MyNode"}',
    )
    assert ctx.node_name == "MyNode"
    assert ctx.default_cmd == "python"
    assert ctx.env == {"PROJECT_VAR": "val"}
    assert ctx.node_env == {"NODE_VAR": "nval"}
    assert ctx.project_root == "/tmp"
    assert ctx.node_config == '{"name": "MyNode"}'


def test_get_recipes_empty():
    assert get_recipes({}) == {}


def test_get_recipes():
    data = {
        "recipes": {
            "add_node": {
                "path": "./recipes/add_node.py",
                "description": "Add a node",
            }
        }
    }
    recipes = get_recipes(data)
    assert "add_node" in recipes
    assert recipes["add_node"].path == "./recipes/add_node.py"
    assert recipes["add_node"].description == "Add a node"
