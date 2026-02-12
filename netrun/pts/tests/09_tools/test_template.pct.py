# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp tools.test_template

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
import pytest
from netrun.tools._models import ActionContext
from netrun.tools._template import (
    resolve_project_root,
    build_template_variables,
    resolve_template,
    resolve_working_directory,
)

# %%
#|export
# --- resolve_project_root ---

def test_resolve_project_root_none():
    assert resolve_project_root(None, None) is None


def test_resolve_project_root_none_with_net_file():
    assert resolve_project_root(None, "/tmp/test.netrun.json") == "/tmp"


def test_resolve_project_root_absolute():
    assert resolve_project_root("/opt/project", "/tmp/test.json") == "/opt/project"


def test_resolve_project_root_relative():
    result = resolve_project_root("sub/dir", "/home/user/test.json")
    assert result.endswith("sub/dir")
    assert "/home/user/" in result


# --- build_template_variables ---

def test_build_template_variables_empty():
    ctx = ActionContext()
    vars = build_template_variables(ctx)
    assert vars["NODE_NAME"] == ""
    assert vars["PROJECT_ROOT"] == ""


def test_build_template_variables_with_values():
    ctx = ActionContext(
        node_name="MyNode",
        net_file_path="/tmp/test.netrun.json",
        project_root="/tmp",
        default_cmd="python",
        node_config='{"name": "MyNode"}',
    )
    vars = build_template_variables(ctx)
    assert vars["NODE_NAME"] == "MyNode"
    assert vars["NET_FILE_PATH"] == "/tmp/test.netrun.json"
    assert vars["NET_FILE_DIR"] == "/tmp"
    assert vars["PROJECT_ROOT"] == "/tmp"
    assert vars["DEFAULT_CMD"] == "python"
    assert vars["NODE_CONFIG"] == '{"name": "MyNode"}'


def test_build_template_variables_node_config_default():
    """NODE_CONFIG defaults to '{}' when not provided."""
    ctx = ActionContext()
    vars = build_template_variables(ctx)
    assert vars["NODE_CONFIG"] == "{}"


def test_build_template_variables_precedence():
    ctx = ActionContext(
        node_name="Node",
        env={"NODE_NAME": "project_override"},
        node_env={"NODE_NAME": "node_override"},
    )
    vars = build_template_variables(ctx)
    # node_env should win
    assert vars["NODE_NAME"] == "node_override"


def test_build_template_variables_custom_env():
    ctx = ActionContext(
        env={"MY_VAR": "hello"},
        node_env={"MY_OTHER": "world"},
    )
    vars = build_template_variables(ctx)
    assert vars["MY_VAR"] == "hello"
    assert vars["MY_OTHER"] == "world"


# --- resolve_template ---

def test_resolve_template_dollar_syntax():
    ctx = ActionContext(node_name="TestNode")
    result = resolve_template("echo $NODE_NAME", ctx)
    assert result == "echo TestNode"


def test_resolve_template_brace_syntax():
    ctx = ActionContext(node_name="TestNode")
    result = resolve_template("echo ${NODE_NAME}", ctx)
    assert result == "echo TestNode"


def test_resolve_template_multiple():
    ctx = ActionContext(
        node_name="MyNode",
        net_file_path="/tmp/test.json",
    )
    result = resolve_template("$NODE_NAME in $NET_FILE_DIR", ctx)
    assert result == "MyNode in /tmp"


def test_resolve_template_custom_vars():
    ctx = ActionContext(env={"GREETING": "hello"})
    result = resolve_template("$GREETING world", ctx)
    assert result == "hello world"


def test_resolve_template_no_match():
    ctx = ActionContext()
    result = resolve_template("echo hello", ctx)
    assert result == "echo hello"


def test_resolve_template_node_config():
    ctx = ActionContext(node_config='{"name": "N"}')
    result = resolve_template("echo $NODE_CONFIG", ctx)
    assert result == 'echo {"name": "N"}'


def test_resolve_template_regex_safe_value():
    r"""Values with regex special chars (like backslashes in paths) are safe."""
    ctx = ActionContext(env={"PATH_VAR": r"C:\Users\test"})
    result = resolve_template("echo $PATH_VAR", ctx)
    assert result == r"echo C:\Users\test"


def test_resolve_template_single_quotes_preserved():
    """Variables inside single quotes are not substituted (shell behaviour)."""
    ctx = ActionContext(node_name="TestNode")
    result = resolve_template("echo '$NODE_NAME'", ctx)
    assert result == "echo '$NODE_NAME'"


def test_resolve_template_single_quotes_mixed():
    """Variables outside single quotes are substituted, inside are not."""
    ctx = ActionContext(node_name="TestNode", env={"GREETING": "hello"})
    result = resolve_template("echo $GREETING '$NODE_NAME' $NODE_NAME", ctx)
    assert result == "echo hello '$NODE_NAME' TestNode"


def test_resolve_template_single_quotes_brace_syntax():
    """Brace syntax inside single quotes is also preserved."""
    ctx = ActionContext(node_name="TestNode")
    result = resolve_template("echo '${NODE_NAME}'", ctx)
    assert result == "echo '${NODE_NAME}'"


# --- resolve_working_directory ---

def test_resolve_working_directory_explicit():
    ctx = ActionContext(working_directory="/opt/work")
    assert resolve_working_directory(ctx) == "/opt/work"


def test_resolve_working_directory_project_root():
    ctx = ActionContext(project_root="/opt/project")
    assert resolve_working_directory(ctx) == "/opt/project"


def test_resolve_working_directory_net_file():
    ctx = ActionContext(net_file_path="/tmp/test.json")
    assert resolve_working_directory(ctx) == "/tmp"


def test_resolve_working_directory_none():
    ctx = ActionContext()
    assert resolve_working_directory(ctx) is None
