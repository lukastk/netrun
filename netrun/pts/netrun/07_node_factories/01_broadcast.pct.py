# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp node_factories.broadcast

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();

# %% [markdown]
# # Broadcast Node Factory
#
# A node factory that creates a **broadcast** node: it consumes incoming
# packets and replicates each value to M output ports, optionally copying
# the value.
#
# This solves the fan-out problem (the graph validator forbids multiple
# edges from the same output port) by providing a dedicated node that
# explicitly replicates data.

# %%
#|export
from enum import Enum
import copy as _copy_module

from netrun.net.config import (
    NodeConfig,
    PortConfig,
    SalvoConditionConfig,
    SalvoConditionTermTrueConfig,
    MaxSalvosFiniteConfig,
    PacketCountAllConfig,
    PacketCountNConfig,
    PortStateNonEmptyConfig,
    SalvoConditionTermPortConfig,
)

# %%
#|export
class CopyMode(str, Enum):
    """How to copy values when broadcasting to multiple output ports."""
    none = "none"        # Same reference (no copy)
    shallow = "shallow"  # copy.copy()
    deep = "deep"        # copy.deepcopy()

# %% [markdown]
# ## Factory Functions

# %%
#|export
def get_node_config(_net_config=None, *, num_inputs: int = 1, num_outputs: int = 2, copy_mode: CopyMode | str = CopyMode.none) -> NodeConfig:
    """Create a NodeConfig for a broadcast node.

    Args:
        _net_config: NetConfig instance injected by the system (not user-facing).
        num_inputs: Number of input ports (N). Must be >= 1.
        num_outputs: Number of output ports (M). Must be >= 1.
        copy_mode: How to copy values for each output port.

    Returns:
        NodeConfig without execution_config (per factory protocol).

    Raises:
        ValueError: If num_inputs < 1, num_outputs < 1, or invalid copy_mode.
    """
    # Validate
    if num_inputs < 1:
        raise ValueError(f"num_inputs must be >= 1, got {num_inputs}")
    if num_outputs < 1:
        raise ValueError(f"num_outputs must be >= 1, got {num_outputs}")

    # Coerce copy_mode string to enum (validates it)
    if isinstance(copy_mode, str):
        try:
            copy_mode = CopyMode(copy_mode)
        except ValueError:
            raise ValueError(f"Invalid copy_mode: {copy_mode!r}. Must be one of: {', '.join(m.value for m in CopyMode)}")

    # Build ports
    in_ports = {f"in_{i}": PortConfig() for i in range(num_inputs)}
    out_ports = {f"out_{i}": PortConfig() for i in range(num_outputs)}

    # Build input salvo conditions: one per input port, each triggers independently
    in_salvo_conditions = {}
    for i in range(num_inputs):
        port_name = f"in_{i}"
        cond_name = "trigger" if num_inputs == 1 else f"trigger_in_{i}"
        in_salvo_conditions[cond_name] = SalvoConditionConfig(
            max_salvos=MaxSalvosFiniteConfig(max=1),
            ports={port_name: PacketCountNConfig(count=1)},
            term=SalvoConditionTermPortConfig(port_name=port_name, state=PortStateNonEmptyConfig()),
        )

    # Build output salvo condition: single "send", always-true, all ports
    out_salvo_conditions = {
        "send": SalvoConditionConfig(
            max_salvos=MaxSalvosFiniteConfig(max=1),
            ports={f"out_{i}": PacketCountAllConfig() for i in range(num_outputs)},
            term=SalvoConditionTermTrueConfig(),
        )
    }

    return NodeConfig(
        name="broadcast",
        in_ports=in_ports,
        out_ports=out_ports,
        in_salvo_conditions=in_salvo_conditions,
        out_salvo_conditions=out_salvo_conditions,
        extra={
            "ui": {
                "shape": "triangle-left",
                "hideLabel": True,
                "hideDescription": True,
                "hidePortNames": True,
            }
        },
    )


def get_node_funcs(_net_config=None, *, num_inputs: int = 1, num_outputs: int = 2, copy_mode: CopyMode | str = CopyMode.none) -> tuple:
    """Create execution functions for a broadcast node.

    Args:
        _net_config: NetConfig instance injected by the system (not user-facing).
        num_inputs: Number of input ports (N). Must be >= 1.
        num_outputs: Number of output ports (M). Must be >= 1.
        copy_mode: How to copy values for each output port.

    Returns:
        Tuple of (exec_func, None, None, None).
    """
    # Coerce copy_mode string to enum
    if isinstance(copy_mode, str):
        copy_mode = CopyMode(copy_mode)

    out_port_names = [f"out_{i}" for i in range(num_outputs)]

    def exec_func(ctx, packets):
        # Find the one input port that has packets (salvo gives us exactly 1 packet from 1 port)
        packet_id = None
        for port_name, pids in packets.items():
            if pids:
                packet_id = pids[0]
                break

        if packet_id is None:
            raise RuntimeError("Broadcast node received no packets")

        # Consume the packet to get its value
        value = ctx.consume_packet(packet_id)

        # Replicate to each output port
        for i, out_port in enumerate(out_port_names):
            if copy_mode == CopyMode.none:
                out_value = value
            elif copy_mode == CopyMode.shallow:
                out_value = _copy_module.copy(value)
            else:  # deep
                out_value = _copy_module.deepcopy(value)

            pid = ctx.create_packet(out_value)
            ctx.load_output_port(out_port, pid)

        ctx.send_output_salvo("send")

    return (exec_func, None, None, None)

# %%
#|export
_factory_desc = """\
Creates a broadcast node that replicates each incoming packet to M output ports.

**Factory args:**
- ``num_inputs`` (int, default 1): Number of input ports (N).
- ``num_outputs`` (int, required): Number of output ports (M).
- ``copy_mode`` (str, default "none"): How to copy values — ``"none"``
  (same reference), ``"shallow"`` (``copy.copy``), or ``"deep"``
  (``copy.deepcopy``).

**Input ports:** ``in_0``, ``in_1``, ..., ``in_{N-1}``
Each port triggers independently when non-empty, consuming 1 packet per epoch.

**Output ports:** ``out_0``, ``out_1``, ..., ``out_{M-1}``
All outputs receive a copy (or reference) of the consumed input value.

**Salvo conditions:**
- Input: One condition per port. For N=1, named ``"trigger"``;
  for N>1, named ``"trigger_in_0"``, ``"trigger_in_1"``, etc.
- Output: Single ``"send"`` condition (always-true, all ports).
"""

# %% [markdown]
# ## Tests

# %%
# Smoke test: default config with 1 input, 2 outputs
config = get_node_config(num_outputs=2)
assert config.name == "broadcast"
assert list(config.in_ports.keys()) == ["in_0"]
assert list(config.out_ports.keys()) == ["out_0", "out_1"]
assert "trigger" in config.in_salvo_conditions
assert "send" in config.out_salvo_conditions
assert config.execution_config is None
print("Smoke test passed: 1-in 2-out config")

# %%
# Test multi-input salvo naming
config3 = get_node_config(num_inputs=3, num_outputs=2)
assert "trigger_in_0" in config3.in_salvo_conditions
assert "trigger_in_1" in config3.in_salvo_conditions
assert "trigger_in_2" in config3.in_salvo_conditions
assert "trigger" not in config3.in_salvo_conditions
print("Multi-input salvo naming test passed")

# %%
# Test validation errors
import traceback

try:
    get_node_config(num_inputs=0, num_outputs=2)
    assert False, "Should have raised ValueError"
except ValueError as e:
    assert "num_inputs" in str(e)
    print(f"Validation test passed: {e}")

try:
    get_node_config(num_outputs=0)
    assert False, "Should have raised ValueError"
except ValueError as e:
    assert "num_outputs" in str(e)
    print(f"Validation test passed: {e}")

try:
    get_node_config(num_outputs=2, copy_mode="invalid")
    assert False, "Should have raised ValueError"
except ValueError as e:
    assert "copy_mode" in str(e)
    print(f"Validation test passed: {e}")

# %%
# Test CopyMode string coercion
config_str = get_node_config(num_outputs=2, copy_mode="deep")
assert config_str.execution_config is None  # factory protocol
print("String coercion test passed")

# %%
# Test get_node_funcs returns valid tuple
funcs = get_node_funcs(num_outputs=3)
assert len(funcs) == 4
assert callable(funcs[0])
assert funcs[1] is None
assert funcs[2] is None
assert funcs[3] is None
print("get_node_funcs test passed")
