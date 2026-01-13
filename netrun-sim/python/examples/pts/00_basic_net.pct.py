# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();

# %%
from netrun_sim import (
    # Graph types
    Graph, Node, Edge, Port, PortRef, PortType, PortSlotSpec,
    PortState, SalvoCondition, SalvoConditionTerm, MaxSalvos, Salvo,
    # NetSim types
    NetSim, NetAction, NetActionResponseData, PacketLocation,
)

# %% [markdown]
# Define the graph

# %%
node_a = Node(
    name="A",
    out_ports={
        "out1": Port(PortSlotSpec.infinite()),
        "out2": Port(PortSlotSpec.finite(1)),
    },
    in_salvo_conditions={
        "manual": SalvoCondition(
            max_salvos=MaxSalvos.finite(1),
            ports=[],
            term=SalvoConditionTerm.true_(),
        ),
    },
    out_salvo_conditions={
        "1": SalvoCondition(
            max_salvos=MaxSalvos.finite(1),
            ports=["out1"],
            term=SalvoConditionTerm.port("out1", PortState.non_empty()),
        ),
        "2": SalvoCondition(
            max_salvos=MaxSalvos.finite(1),
            ports=["out2"],
            term=SalvoConditionTerm.port("out2", PortState.non_empty()),
        ),
    },
)

# %%
node_b1 = Node(
    name="B1",
    in_ports={"in": Port(PortSlotSpec.infinite())},
    out_ports={"out": Port(PortSlotSpec.infinite())},
    in_salvo_conditions={
        "default": SalvoCondition(
            max_salvos=MaxSalvos.finite(1),
            ports=["in"],
            term=SalvoConditionTerm.port("in", PortState.non_empty()),
        ),
    },
    out_salvo_conditions={
        "default": SalvoCondition(
            max_salvos=MaxSalvos.Infinite,
            ports=["out"],
            term=SalvoConditionTerm.port("out", PortState.non_empty()),
        ),
    },
)

# %%
node_b2 = Node(
    name="B2",
    in_ports={"in": Port(PortSlotSpec.infinite())},
    out_ports={"out": Port(PortSlotSpec.infinite())},
    in_salvo_conditions={
        "default": SalvoCondition(
            max_salvos=MaxSalvos.finite(1),
            ports=["in"],
            term=SalvoConditionTerm.port("in", PortState.non_empty()),
        ),
    },
    out_salvo_conditions={
        "default": SalvoCondition(
            max_salvos=MaxSalvos.Infinite,
            ports=["out"],
            term=SalvoConditionTerm.port("out", PortState.non_empty()),
        ),
    },
)

# %%
node_c = Node(
    name="C",
    in_ports={"in1": Port(PortSlotSpec.infinite()), "in2": Port(PortSlotSpec.infinite())},
    out_ports={"out": Port(PortSlotSpec.infinite())},
)

# %%
edges = [
    Edge(
        PortRef("A", PortType.Output, "out1"),
        PortRef("B1", PortType.Input, "in"),
    ),
    Edge(
        PortRef("A", PortType.Output, "out2"),
        PortRef("B2", PortType.Input, "in"),
    ),
    Edge(
        PortRef("B1", PortType.Output, "out"),
        PortRef("C", PortType.Input, "in1"),
    ),
    Edge(
        PortRef("B2", PortType.Output, "out"),
        PortRef("C", PortType.Input, "in2"),
    ),
]

# %% [markdown]
# Create the graph and the net objects

# %%
graph = Graph([node_a, node_b1, node_b2, node_c], edges)
assert graph.validate() == []

net = NetSim(graph)

# %% [markdown]
# Try running the net until blocked. Should do nothing.

# %%
response_data, events = net.do_action(
    NetAction.run_net_until_blocked()
)
assert events == []

# %% [markdown]
# Create an epoch in node `A` manually

# %%
response_data, events = net.do_action(NetAction.create_epoch(
    "A",
    Salvo(
        "manual",
        []
    )
))

# %% [markdown]
# Start the epoch inside node `A`

# %%
epoch_id = response_data.epoch.id
response_data, events = net.do_action(NetAction.start_epoch(
    epoch_id
))

# %% [markdown]
# Create packets inside the node `A` epoch

# %%
response_data, events = net.do_action(NetAction.create_packet(
    epoch_id,
))
packetA1_id = response_data.packet_id

response_data, events = net.do_action(NetAction.create_packet(
    epoch_id,
))
packetA2_id = response_data.packet_id

# %% [markdown]
# Load packets into output ports in node `A`

# %%
response_data, events = net.do_action(NetAction.load_packet_into_output_port(
    packetA1_id,
    "out1"
))

response_data, events = net.do_action(NetAction.send_output_salvo(
    epoch_id,
    "1"
))

response_data, events = net.do_action(NetAction.load_packet_into_output_port(
    packetA2_id,
    "out2"
))

response_data, events = net.do_action(NetAction.send_output_salvo(
    epoch_id,
    "2"
))

# %% [markdown]
# Finish the epoch

# %%
response_data, events = net.do_action(NetAction.finish_epoch(epoch_id))

# %% [markdown]
# Run the net until blocked

# %%
response_data, events = net.do_action(
    NetAction.run_net_until_blocked()
)

print("Events in the net:")
for event in events:
    print("  ", event)

# %% [markdown]
# Should now have two startable epochs for `B1` and `B2`

# %%
startable_epochs = net.get_startable_epochs()
assert len(net.get_startable_epochs()) == 2
assert set([net.get_epoch(epoch_id).node_name for epoch_id in startable_epochs]) == {"B1", "B2"}

# %%
