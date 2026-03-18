# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Dev Helpers

# %%
#|default_exp dev.test_helpers

# %%
#|export
import json
import pytest
import tempfile
from pathlib import Path

from netrun.net.config import (
    NetConfig,
    GraphConfig,
    NodeConfig,
    PortConfig,
    EdgeConfig,
    PoolConfig,
    MainPoolConfig,
    NodeExecutionConfig,
    NodeVariable,
)

from netrun_utils.dev._helpers import (
    _load_config,
    _resolve_node_name,
    _get_merged_node_vars,
)

# %% [markdown]
# ## Config for tests

# %%
#|export
def _write_test_config(tmp_path: Path, extra: dict | None = None) -> Path:
    """Write a minimal netrun config file and return its path."""
    config = {
        "pools": {"main": {"spec": {"type": "main"}}},
        "graph": {
            "nodes": [
                {
                    "name": "source",
                    "out_ports": {"out": {}},
                    "factory": "netrun.node_factories.from_function",
                    "factory_args": {"func": "netrun_utils.dev._helpers._load_config"},
                },
                {
                    "name": "processor",
                    "in_ports": {"data": {}},
                    "out_ports": {"result": {}},
                    "factory": "netrun.node_factories.from_function",
                    "factory_args": {"func": "netrun_utils.dev._helpers._load_config"},
                },
            ],
            "edges": [
                {"source_node": "source", "source_port": "out", "target_node": "processor", "target_port": "data"},
            ],
        },
    }
    if extra:
        config.update(extra)
    path = tmp_path / "test.netrun.json"
    path.write_text(json.dumps(config))
    return path

# %% [markdown]
# ## _resolve_node_name Tests

# %%
#|export
def test_resolve_exact_match():
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(nodes=[
            NodeConfig(name="alpha"),
            NodeConfig(name="beta"),
        ]),
    )
    assert _resolve_node_name(config, "alpha") == "alpha"
    assert _resolve_node_name(config, "beta") == "beta"


def test_resolve_suffix_match():
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(nodes=[
            NodeConfig(name="subgraph.inner_node"),
            NodeConfig(name="other"),
        ]),
    )
    assert _resolve_node_name(config, "inner_node") == "subgraph.inner_node"


def test_resolve_ambiguous():
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(nodes=[
            NodeConfig(name="a.foo"),
            NodeConfig(name="b.foo"),
        ]),
    )
    with pytest.raises(ValueError, match="Ambiguous"):
        _resolve_node_name(config, "foo")


def test_resolve_not_found():
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(nodes=[
            NodeConfig(name="alpha"),
        ]),
    )
    with pytest.raises(ValueError, match="not found"):
        _resolve_node_name(config, "missing")

# %% [markdown]
# ## _get_merged_node_vars Tests

# %%
#|export
def test_merged_vars_global_only():
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(nodes=[
            NodeConfig(name="n", execution_config=NodeExecutionConfig(pools=["main"])),
        ]),
        node_vars={"run_name": NodeVariable(value="test", type="str")},
    )
    merged = _get_merged_node_vars(config, "n")
    assert "run_name" in merged
    var, source = merged["run_name"]
    assert var.value == "test"
    assert source == "global"


def test_merged_vars_inherited():
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(nodes=[
            NodeConfig(
                name="n",
                execution_config=NodeExecutionConfig(
                    pools=["main"],
                    node_vars={"model": NodeVariable(inherit=True)},
                ),
            ),
        ]),
        node_vars={"model": NodeVariable(value="gpt-4", type="str")},
    )
    merged = _get_merged_node_vars(config, "n")
    var, source = merged["model"]
    assert var.value == "gpt-4"
    assert source == "inherited"


def test_merged_vars_inherited_overridden():
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(nodes=[
            NodeConfig(
                name="n",
                execution_config=NodeExecutionConfig(
                    pools=["main"],
                    node_vars={"model": NodeVariable(value="gpt-3.5", inherit=True)},
                ),
            ),
        ]),
        node_vars={"model": NodeVariable(value="gpt-4", type="str", options=["gpt-4", "gpt-3.5"])},
    )
    merged = _get_merged_node_vars(config, "n")
    var, source = merged["model"]
    assert var.value == "gpt-3.5"
    assert var.type == "str"
    assert var.options == ["gpt-4", "gpt-3.5"]
    assert source == "inherited (overridden)"


def test_merged_vars_node_level():
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(nodes=[
            NodeConfig(
                name="n",
                execution_config=NodeExecutionConfig(
                    pools=["main"],
                    node_vars={"custom": NodeVariable(value="hello", type="str")},
                ),
            ),
        ]),
    )
    merged = _get_merged_node_vars(config, "n")
    var, source = merged["custom"]
    assert var.value == "hello"
    assert source == "node-level"


def test_merged_vars_inherit_missing_global():
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(nodes=[
            NodeConfig(
                name="n",
                execution_config=NodeExecutionConfig(
                    pools=["main"],
                    node_vars={"missing": NodeVariable(inherit=True)},
                ),
            ),
        ]),
    )
    with pytest.raises(ValueError, match="no net-level variable"):
        _get_merged_node_vars(config, "n")

# %% [markdown]
# ## _load_config Tests

# %%
#|export
def test_load_config(tmp_path):
    path = _write_test_config(tmp_path)
    config = _load_config(path)
    assert len(config.graph.nodes) == 2


def test_load_config_with_vars(tmp_path):
    config_data = {
        "pools": {"main": {"spec": {"type": "main"}}},
        "graph": {
            "nodes": [NodeConfig(name="n").model_dump()],
        },
        "node_vars": {"x": {"type": "str"}},
    }
    path = tmp_path / "test.netrun.json"
    path.write_text(json.dumps(config_data))

    config = _load_config(path, global_node_vars={"x": "hello"})
    assert config.node_vars["x"].value == "hello"
