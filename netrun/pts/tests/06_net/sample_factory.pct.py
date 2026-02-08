# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Sample Factory Module for Testing

# %%
#|default_exp net.sample_factory

# %%
#|export
from netrun.net.config import (
    NodeConfig,
    PortConfig,
    SalvoConditionConfig,
    SalvoConditionTermPortConfig,
    SalvoConditionTermTrueConfig,
    MaxSalvosFiniteConfig,
    PacketCountAllConfig,
    PortStateNonEmptyConfig,
)

# %%
#|export
def get_node_config(_net_config=None, *, name: str, threshold: float = 0.5) -> NodeConfig:
    """Returns graph structure: name, ports, salvo conditions.

    Must NOT set execution_config - that comes from get_node_funcs().
    """
    return NodeConfig(
        name=name,
        in_ports={"task": PortConfig()},
        out_ports={"result": PortConfig()},
        in_salvo_conditions={
            "trigger": SalvoConditionConfig(
                max_salvos=MaxSalvosFiniteConfig(max=1),
                ports={"task": PacketCountAllConfig()},
                term=SalvoConditionTermPortConfig(
                    port_name="task",
                    state=PortStateNonEmptyConfig(),
                ),
            ),
        },
        out_salvo_conditions={
            "send": SalvoConditionConfig(
                max_salvos=MaxSalvosFiniteConfig(max=1),
                ports={"result": PacketCountAllConfig()},
                term=SalvoConditionTermTrueConfig(),
            ),
        },
        # execution_config is NOT set here
    )

# %%
#|export
def get_node_funcs(_net_config=None, *, name: str, threshold: float = 0.5) -> tuple:
    """Returns execution functions.

    Arguments can be captured in closures for use in the functions.
    """

    def exec_func(ctx, packets):
        ctx.print(f"Processing with threshold={threshold}")
        for packet_id in packets.get("task", []):
            value = ctx.consume_packet(packet_id)
            if value.get("score", 0) > threshold:
                out_id = ctx.create_packet({"passed": True, **value})
                ctx.load_output_port("result", out_id)
        ctx.send_output_salvo("send")

    return (exec_func, None, None, None)
