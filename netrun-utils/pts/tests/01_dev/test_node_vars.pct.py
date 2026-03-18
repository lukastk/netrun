# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for show_node_vars

# %%
#|default_exp dev.test_node_vars

# %%
#|export
import json
import pytest
from pathlib import Path

from netrun_utils.dev.node_vars import show_node_vars

# %% [markdown]
# ## Helpers

# %%
#|export
def _write_config(tmp_path: Path) -> Path:
    config = {
        "pools": {"main": {"spec": {"type": "main"}}},
        "graph": {
            "nodes": [
                {
                    "name": "mynode",
                    "in_ports": {"data": {}},
                    "execution_config": {
                        "pools": ["main"],
                        "node_vars": {
                            "model": {"inherit": True},
                            "custom_param": {"value": "hello", "type": "str"},
                        },
                    },
                },
            ],
        },
        "node_vars": {
            "model": {"value": "gpt-4", "type": "str"},
            "temperature": {"value": 0.7, "type": "float"},
        },
    }
    path = tmp_path / "test.netrun.json"
    path.write_text(json.dumps(config))
    return path

# %% [markdown]
# ## Tests

# %%
#|export
def test_show_node_vars(tmp_path, capsys):
    path = _write_config(tmp_path)
    show_node_vars("mynode", path)
    output = capsys.readouterr().out
    assert "mynode" in output
    assert "model" in output
    assert "gpt-4" in output
    assert "temperature" in output
    assert "custom_param" in output
    assert "hello" in output


def test_show_node_vars_filtered(tmp_path, capsys):
    path = _write_config(tmp_path)
    show_node_vars("mynode", path, "model")
    output = capsys.readouterr().out
    assert "model" in output
    assert "temperature" not in output


def test_show_node_vars_with_overrides(tmp_path, capsys):
    path = _write_config(tmp_path)
    show_node_vars("mynode", path, global_node_vars={"model": "claude-3"})
    output = capsys.readouterr().out
    assert "claude-3" in output
