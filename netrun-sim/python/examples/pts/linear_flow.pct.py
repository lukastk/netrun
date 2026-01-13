# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Linear Flow Example
#
# This notebook demonstrates a simple linear packet flow through a network: **A → B → C**
#
# We'll create a graph with three nodes connected in sequence, then simulate a packet flowing through the network.

# %% [markdown]
# ## Setup
#
# First, let's import all the types we need from `netrun_sim`.

# %%
from netrun_sim import (
    # Graph types
    Graph, Node, Edge, Port, PortRef, PortType, PortSlotSpec,
    PortState, SalvoCondition, SalvoConditionTerm, MaxSalvos,
    # NetSim types
    NetSim, NetAction, NetActionResponseData, PacketLocation,
)

# %% [markdown]
# ## Helper Functions
#
# Let's define some helper functions to reduce boilerplate when creating edges and nodes.

# %%
def create_edge(src_node: str, src_port: str, tgt_node: str, tgt_port: str) -> Edge:
    """Create an edge between two ports."""
    return Edge(
        PortRef(src_node, PortType.Output, src_port),
        PortRef(tgt_node, PortType.Input, tgt_port),
    )


def edge_location(src_node: str, src_port: str, tgt_node: str, tgt_port: str) -> PacketLocation:
    """Create a PacketLocation for an edge."""
    return PacketLocation.edge(
        Edge(
            PortRef(src_node, PortType.Output, src_port),
            PortRef(tgt_node, PortType.Input, tgt_port),
        )
    )

# %% [markdown]
# ## Creating the Graph
#
# Now let's create our three nodes:
#
# - **Node A**: Source node with one output port (no input ports)
# - **Node B**: Middle node with one input and one output port
# - **Node C**: Sink node with one input port (no output ports)
#
# Each node (except A) has an **input salvo condition** that triggers an epoch when packets arrive at its input port.

# %%
# Node A: Source node - just an output port, no inputs
node_a = Node(
    name="A",
    out_ports={"out": Port(PortSlotSpec.infinite())},
)

print(f"Created node A with output port 'out'")

# %%
# Node B: Middle node - one input, one output
# Has a salvo condition that triggers when input is non-empty
node_b = Node(
    name="B",
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

print(f"Created node B with input 'in' and output 'out'")

# %%
# Node C: Sink node - just an input port, no outputs
node_c = Node(
    name="C",
    in_ports={"in": Port(PortSlotSpec.infinite())},
    in_salvo_conditions={
        "default": SalvoCondition(
            max_salvos=MaxSalvos.finite(1),
            ports=["in"],
            term=SalvoConditionTerm.port("in", PortState.non_empty()),
        ),
    },
)

print(f"Created node C with input 'in'")

# %% [markdown]
# ## Creating Edges
#
# Now let's connect the nodes: A → B → C

# %%
edges = [
    create_edge("A", "out", "B", "in"),
    create_edge("B", "out", "C", "in"),
]

print("Created edges: A.out → B.in, B.out → C.in")

# %% [markdown]
# ## Building and Validating the Graph
#
# Let's create the graph and validate it to ensure there are no errors.

# %%
graph = Graph([node_a, node_b, node_c], edges)

errors = graph.validate()
if errors:
    print(f"Validation errors: {errors}")
else:
    print("Graph is valid!")
    print(f"Nodes: {list(graph.nodes().keys())}")
    print(f"Edges: {len(graph.edges())}")

# %% [markdown]
# ## Creating the Network
#
# Now we create a `NetSim` instance from the graph. This represents the runtime state of the network.

# %%
net = NetSim(graph)

print("Network created!")
print(f"Startable epochs: {len(net.get_startable_epochs())}")

# %% [markdown]
# ## Creating and Placing a Packet
#
# Let's create a packet and place it on the edge from A to B. This simulates node A producing an output.

# %%
# Create a packet (outside any epoch)
response, events = net.do_action(NetAction.create_packet())
assert isinstance(response, NetActionResponseData.Packet)
packet_id = response.packet_id

print(f"Created packet: {packet_id}")

# %%
# Place the packet on the edge A → B
net.do_action(
    NetAction.transport_packet_to_location(
        packet_id, 
        edge_location("A", "out", "B", "in")
    )
)

print(f"Placed packet on edge A → B")

# %% [markdown]
# ## Running the Network
#
# Now let's run the network until it's blocked. This will:
# 1. Move the packet from the edge to B's input port
# 2. Check salvo conditions - B's condition is satisfied
# 3. Create a startable epoch at node B

# %%
response, events = net.do_action(NetAction.run_net_until_blocked())

print(f"Network ran until blocked")
print(f"Events: {len(events)}")
for event in events:
    print(f"  - {event.kind}")

# %%
# Check for startable epochs
startable = net.get_startable_epochs()
print(f"Startable epochs: {len(startable)}")

if startable:
    epoch_id = startable[0]
    epoch = net.get_epoch(epoch_id)
    print(f"Epoch at node: {epoch.node_name}")

# %% [markdown]
# ## Processing Node B
#
# Now we'll simulate processing at node B:
# 1. Start the epoch
# 2. Consume the input packet
# 3. Create an output packet
# 4. Send it via the output salvo
# 5. Finish the epoch

# %%
# Start the epoch
response, events = net.do_action(NetAction.start_epoch(epoch_id))
assert isinstance(response, NetActionResponseData.StartedEpoch)
epoch = response.epoch

print(f"Started epoch at node {epoch.node_name}")
print(f"Input salvo has {len(epoch.in_salvo.packets)} packet(s)")

# %%
# Consume the input packet
input_packet_id = epoch.in_salvo.packets[0][1]  # (port_name, packet_id)
net.do_action(NetAction.consume_packet(input_packet_id))

print(f"Consumed input packet: {input_packet_id}")

# %%
# Create an output packet
response, _ = net.do_action(NetAction.create_packet(epoch.id))
assert isinstance(response, NetActionResponseData.Packet)
output_packet_id = response.packet_id

print(f"Created output packet: {output_packet_id}")

# %%
# Load into output port and send
net.do_action(NetAction.load_packet_into_output_port(output_packet_id, "out"))
net.do_action(NetAction.send_output_salvo(epoch.id, "default"))

print("Sent output salvo - packet is now on edge B → C")

# %%
# Finish the epoch
net.do_action(NetAction.finish_epoch(epoch.id))

print("Finished epoch at node B")

# %% [markdown]
# ## Running to Node C
#
# Now let's run the network again to move the packet to node C.

# %%
# Run network again
net.do_action(NetAction.run_net_until_blocked())

# Check for startable epochs at C
startable = net.get_startable_epochs()
print(f"Startable epochs: {len(startable)}")

if startable:
    epoch = net.get_epoch(startable[0])
    print(f"Epoch ready at node: {epoch.node_name}")
    print(f"Packets in input salvo: {len(epoch.in_salvo.packets)}")

# %% [markdown]
# ## Summary
#
# We successfully simulated a packet flowing through a linear network:
#
# 1. Created a graph with nodes A → B → C
# 2. Placed a packet on the edge from A to B
# 3. Ran the network - packet moved to B's input, creating a startable epoch
# 4. Processed node B - consumed input, created output, sent it downstream
# 5. Ran again - packet moved to C's input, creating another startable epoch
#
# The packet successfully traversed the entire network!
