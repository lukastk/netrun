# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp node_factories.join

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();

# %% [markdown]
# # Join Node Factory
#
# A node factory that creates a **join** node: it waits for packets to arrive
# on all N input ports, consumes them, and outputs a single dict
# ``{port_name: value_or_list}`` on its ``out`` port.
#
# This solves the barrier/synchronisation problem — wait for N upstream nodes
# to all produce output before continuing.

# %%
#|export
from typing import Any

from netrun.net.config import (
    NodeConfig,
    PortConfig,
    SalvoConditionConfig,
    SalvoConditionTermAndConfig,
    SalvoConditionTermPortConfig,
    SalvoConditionTermTrueConfig,
    MaxSalvosFiniteConfig,
    PacketCountAllConfig,
    PacketCountNConfig,
    PortStateNonEmptyConfig,
    PortStateEqualsOrGreaterThanConfig,
)

# %%
#|export
def _normalise_ports(ports: list[str] | dict[str, int]) -> dict[str, int]:
    """Normalise ports arg to ``{name: count}`` dict.

    Raises:
        ValueError: If ports is empty, contains invalid names, or counts < 1.
    """
    if isinstance(ports, list):
        if not ports:
            raise ValueError("ports must not be empty")
        if len(ports) != len(set(ports)):
            raise ValueError("ports must not contain duplicate names")
        return {name: 1 for name in ports}

    if isinstance(ports, dict):
        if not ports:
            raise ValueError("ports must not be empty")
        for name, count in ports.items():
            if not isinstance(count, int) or count < 1:
                raise ValueError(f"port '{name}' count must be an integer >= 1, got {count!r}")
        return dict(ports)

    raise TypeError(f"ports must be a list[str] or dict[str, int], got {type(ports).__name__}")

# %% [markdown]
# ## Factory Functions

# %%
#|export
def get_node_config(
    _net_config: Any = None,
    *,
    ports: list[str] | dict[str, int] = ("a", "b"),
) -> NodeConfig:
    """Create a NodeConfig for a join node.

    Args:
        _net_config: NetConfig instance injected by the system (not user-facing).
        ports: Input port specification.  Either a list of port names (each
            collects 1 packet) or a dict mapping port names to packet counts.

    Returns:
        NodeConfig without execution_config (per factory protocol).
    """
    port_counts = _normalise_ports(list(ports) if isinstance(ports, tuple) else ports)

    # Build input ports
    in_ports = {name: PortConfig() for name in port_counts}

    # Build output port
    out_ports = {"out": PortConfig()}

    # Build input salvo condition: single "trigger" that fires when ALL ports
    # have enough packets.
    port_terms = []
    salvo_ports: dict[str, PacketCountNConfig] = {}
    for name, count in port_counts.items():
        if count == 1:
            state = PortStateNonEmptyConfig()
        else:
            state = PortStateEqualsOrGreaterThanConfig(value=count)
        port_terms.append(SalvoConditionTermPortConfig(port_name=name, state=state))
        salvo_ports[name] = PacketCountNConfig(count=count)

    in_salvo_conditions = {
        "trigger": SalvoConditionConfig(
            max_salvos=MaxSalvosFiniteConfig(max=1),
            ports=salvo_ports,
            term=SalvoConditionTermAndConfig(terms=port_terms),
        ),
    }

    # Build output salvo condition
    out_salvo_conditions = {
        "send": SalvoConditionConfig(
            max_salvos=MaxSalvosFiniteConfig(max=1),
            ports={"out": PacketCountAllConfig()},
            term=SalvoConditionTermTrueConfig(),
        ),
    }

    return NodeConfig(
        name="join",
        in_ports=in_ports,
        out_ports=out_ports,
        in_salvo_conditions=in_salvo_conditions,
        out_salvo_conditions=out_salvo_conditions,
    )


def get_node_funcs(
    _net_config: Any = None,
    *,
    ports: list[str] | dict[str, int] = ("a", "b"),
) -> tuple:
    """Create execution functions for a join node.

    Args:
        _net_config: NetConfig instance injected by the system (not user-facing).
        ports: Input port specification (same as get_node_config).

    Returns:
        Tuple of (exec_func, None, None, None).
    """
    port_counts = _normalise_ports(list(ports) if isinstance(ports, tuple) else ports)

    def exec_func(ctx, packets):
        result: dict[str, Any] = {}
        for port_name, count in port_counts.items():
            packet_ids = packets.get(port_name, [])
            if count == 1:
                # Scalar: unwrap single value
                result[port_name] = ctx.consume_packet(packet_ids[0])
            else:
                # List: collect all consumed values
                result[port_name] = [ctx.consume_packet(pid) for pid in packet_ids]

        pid = ctx.create_packet(result)
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    return (exec_func, None, None, None)

# %%
#|export
_factory_desc = """\
Creates a join node that waits for packets on all input ports before firing.

**Factory args:**
- ``ports`` (list or dict): Input port specification.  A list of port names
  (each collects 1 packet) or a dict ``{name: count}`` where *count* is the
  number of packets to collect per port.

**Input ports:** One per entry in ``ports``.
The single salvo condition ``"trigger"`` fires when **all** ports have enough
packets (≥ count).  For ports with count=1 the value is unwrapped to a scalar;
for count>1 it is collected into a list.

**Output port:** ``out``
Emits a dict ``{port_name: value_or_list}`` containing the consumed input
values.

**Salvo conditions:**
- Input: ``"trigger"`` — AND of per-port ≥ count checks.
- Output: ``"send"`` — always-true, all ports.
"""
