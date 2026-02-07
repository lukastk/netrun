# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp tools.test_models

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
import pytest
from netrun.tools._models import (
    ActionConfig,
    ActionContext,
    ActionResult,
    RecipeConfig,
    RecipePrompt,
)

# %%
#|export
def test_action_config_basic():
    action = ActionConfig(id="test", label="Test", command="echo hello")
    assert action.id == "test"
    assert action.label == "Test"
    assert action.command == "echo hello"
    assert action.icon is None


def test_action_config_with_icon():
    action = ActionConfig(id="a1", label="Run", command="./run.sh", icon="play")
    assert action.icon == "play"


def test_action_context_defaults():
    ctx = ActionContext()
    assert ctx.node_name is None
    assert ctx.env is None
    assert ctx.node_env is None
    assert ctx.working_directory is None


def test_action_context_full():
    ctx = ActionContext(
        node_name="MyNode",
        node_id="node-1",
        net_file_path="/tmp/test.netrun.json",
        project_root="/tmp",
        default_cmd="python",
        env={"FOO": "bar"},
        node_env={"BAZ": "qux"},
        working_directory="/tmp/work",
    )
    assert ctx.node_name == "MyNode"
    assert ctx.env == {"FOO": "bar"}
    assert ctx.node_env == {"BAZ": "qux"}


def test_action_result():
    result = ActionResult(
        success=True,
        exit_code=0,
        stdout="hello\n",
        stderr="",
        resolved_command="echo hello",
    )
    assert result.success
    assert result.exit_code == 0
    assert result.timed_out is False


def test_action_result_timed_out():
    result = ActionResult(
        success=False,
        exit_code=-1,
        stdout="",
        stderr="timeout",
        resolved_command="sleep 100",
        timed_out=True,
    )
    assert result.timed_out


def test_recipe_config():
    recipe = RecipeConfig(path="./recipes/add_node.py", description="Add a node")
    assert recipe.path == "./recipes/add_node.py"
    assert recipe.description == "Add a node"


def test_recipe_config_no_description():
    recipe = RecipeConfig(path="./r.py")
    assert recipe.description is None


def test_recipe_prompt_defaults():
    prompt = RecipePrompt(name="count", label="How many?")
    assert prompt.type == "text"
    assert prompt.default is None
    assert prompt.options is None


def test_recipe_prompt_select():
    prompt = RecipePrompt(
        name="pool",
        label="Pool",
        type="select",
        default="main",
        options=["main", "thread", "process"],
    )
    assert prompt.type == "select"
    assert prompt.options == ["main", "thread", "process"]
