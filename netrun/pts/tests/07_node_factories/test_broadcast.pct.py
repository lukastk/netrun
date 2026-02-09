# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Broadcast Node Factory

# %%
#|default_exp node_factories.test_broadcast

# %%
#|export
import pytest
from netrun.node_factories.broadcast import (
    get_node_config,
    get_node_funcs,
    CopyMode,
)
from netrun.net.config import (
    SalvoConditionTermPortConfig,
    PortStateNonEmptyConfig,
    PacketCountNConfig,
    PacketCountAllConfig,
)

# %% [markdown]
# ## Config generation tests

# %%
#|export
def test_default_config():
    """Test default config: 1 input, N outputs."""
    config = get_node_config(num_outputs=3)
    assert config.name == "broadcast"
    assert list(config.in_ports.keys()) == ["in_0"]
    assert list(config.out_ports.keys()) == ["out_0", "out_1", "out_2"]
    assert config.execution_config is None  # factory protocol

# %%
#|export
def test_multi_input_config():
    """Test config with multiple inputs."""
    config = get_node_config(num_inputs=2, num_outputs=4)
    assert list(config.in_ports.keys()) == ["in_0", "in_1"]
    assert list(config.out_ports.keys()) == ["out_0", "out_1", "out_2", "out_3"]

# %%
#|export
def test_single_output():
    """Test M=1 is a valid passthrough/copy use case."""
    config = get_node_config(num_outputs=1)
    assert list(config.out_ports.keys()) == ["out_0"]

# %% [markdown]
# ## Salvo condition tests

# %%
#|export
def test_single_input_salvo_naming():
    """Test N=1 uses 'trigger' name."""
    config = get_node_config(num_inputs=1, num_outputs=2)
    assert "trigger" in config.in_salvo_conditions
    assert len(config.in_salvo_conditions) == 1

# %%
#|export
def test_multi_input_salvo_naming():
    """Test N>1 uses 'trigger_in_{i}' names."""
    config = get_node_config(num_inputs=3, num_outputs=2)
    assert "trigger_in_0" in config.in_salvo_conditions
    assert "trigger_in_1" in config.in_salvo_conditions
    assert "trigger_in_2" in config.in_salvo_conditions
    assert "trigger" not in config.in_salvo_conditions
    assert len(config.in_salvo_conditions) == 3

# %%
#|export
def test_input_salvo_condition_structure():
    """Test each input salvo condition has correct term, ports, max_salvos."""
    config = get_node_config(num_inputs=2, num_outputs=2)

    cond0 = config.in_salvo_conditions["trigger_in_0"]
    assert isinstance(cond0.term, SalvoConditionTermPortConfig)
    assert cond0.term.port_name == "in_0"
    assert isinstance(cond0.term.state, PortStateNonEmptyConfig)
    assert cond0.ports == {"in_0": PacketCountNConfig(count=1)}
    assert cond0.max_salvos.max == 1

    cond1 = config.in_salvo_conditions["trigger_in_1"]
    assert cond1.term.port_name == "in_1"
    assert cond1.ports == {"in_1": PacketCountNConfig(count=1)}

# %%
#|export
def test_output_salvo_condition():
    """Test output salvo is always-true, sends all from all ports."""
    config = get_node_config(num_outputs=3)
    assert "send" in config.out_salvo_conditions
    send = config.out_salvo_conditions["send"]
    assert len(send.ports) == 3
    for i in range(3):
        assert f"out_{i}" in send.ports
        assert isinstance(send.ports[f"out_{i}"], PacketCountAllConfig)

# %% [markdown]
# ## Validation tests

# %%
#|export
def test_invalid_num_inputs():
    """Test num_inputs < 1 raises ValueError."""
    with pytest.raises(ValueError, match="num_inputs"):
        get_node_config(num_inputs=0, num_outputs=2)
    with pytest.raises(ValueError, match="num_inputs"):
        get_node_config(num_inputs=-1, num_outputs=2)

# %%
#|export
def test_invalid_num_outputs():
    """Test num_outputs < 1 raises ValueError."""
    with pytest.raises(ValueError, match="num_outputs"):
        get_node_config(num_inputs=1, num_outputs=0)
    with pytest.raises(ValueError, match="num_outputs"):
        get_node_config(num_outputs=-5)

# %%
#|export
def test_invalid_copy_mode():
    """Test invalid copy_mode string raises ValueError."""
    with pytest.raises(ValueError, match="copy_mode"):
        get_node_config(num_outputs=2, copy_mode="invalid")

# %% [markdown]
# ## CopyMode tests

# %%
#|export
def test_copy_mode_string_coercion():
    """Test copy_mode accepts both enum and string values."""
    # Enum value
    config1 = get_node_config(num_outputs=2, copy_mode=CopyMode.deep)
    assert config1 is not None

    # String value
    config2 = get_node_config(num_outputs=2, copy_mode="shallow")
    assert config2 is not None

# %%
#|export
def test_copy_mode_enum_values():
    """Test CopyMode enum has expected values."""
    assert CopyMode.none == "none"
    assert CopyMode.shallow == "shallow"
    assert CopyMode.deep == "deep"

# %% [markdown]
# ## get_node_funcs tests

# %%
#|export
def test_get_node_funcs_returns_tuple():
    """Test get_node_funcs returns (exec_func, None, None, None)."""
    funcs = get_node_funcs(num_outputs=3)
    assert len(funcs) == 4
    assert callable(funcs[0])
    assert funcs[1] is None
    assert funcs[2] is None
    assert funcs[3] is None

# %%
#|export
def test_get_node_funcs_string_copy_mode():
    """Test get_node_funcs accepts string copy_mode."""
    funcs = get_node_funcs(num_outputs=2, copy_mode="deep")
    assert callable(funcs[0])
