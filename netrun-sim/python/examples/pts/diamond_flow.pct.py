# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Diamond Flow Example
#
# This notebook demonstrates a branching and merging flow pattern:
#
# ```
#        A
#       / \
#      B   C
#       \ /
#        D
# ```
#
# Key concepts demonstrated:
# - **Branching**: Node A sends packets to both B and C
# - **Merging**: Node D waits for packets from both B and C
# - **Synchronization**: D's epoch only triggers when **both** inputs are present

# %% [markdown]
# ## Setup

# %%
from netrun_sim import (
    # Graph types
    Graph,
    Node,
    Edge,
    Port,
    PortRef,
    PortType,
    PortSlotSpec,
    PortState,
    SalvoCondition,
    SalvoConditionTerm,
    MaxSalvos,
    # NetSim types
    NetSim,
    NetAction,
    NetActionResponseData,
    PacketLocation,
)

# %% [markdown]
# ## Helper Functions


# %%
def create_edge(src_node: str, src_port: str, tgt_node: str, tgt_port: str) -> Edge:
    """Create an edge between two ports."""
    return Edge(
        PortRef(src_node, PortType.Output, src_port),
        PortRef(tgt_node, PortType.Input, tgt_port),
    )


def edge_location(
    src_node: str, src_port: str, tgt_node: str, tgt_port: str
) -> PacketLocation:
    """Create a PacketLocation for an edge."""
    return PacketLocation.edge(
        Edge(
            PortRef(src_node, PortType.Output, src_port),
            PortRef(tgt_node, PortType.Input, tgt_port),
        )
    )


# %% [markdown]
# ## Creating the Nodes
#
# ### Node A: Source with Two Outputs
#
# Node A is the source - it has two output ports (`out1` and `out2`) that will send packets to B and C respectively.

# %%
node_a = Node(
    name="A",
    out_ports={
        "out1": Port(PortSlotSpec.infinite()),
        "out2": Port(PortSlotSpec.infinite()),
    },
)

print("Node A: source with outputs 'out1' (→ B) and 'out2' (→ C)")

# %% [markdown]
# ### Nodes B and C: Middle Nodes
#
# B and C are simple pass-through nodes. Each has one input and one output.


# %%
def create_simple_node(name: str) -> Node:
    """Create a simple node with one input and one output."""
    return Node(
        name=name,
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


node_b = create_simple_node("B")
node_c = create_simple_node("C")

print("Node B: middle node (receives from A.out1, sends to D.in1)")
print("Node C: middle node (receives from A.out2, sends to D.in2)")

# %% [markdown]
# ### Node D: Synchronizing Merge Node
#
# Node D has **two input ports** (`in1` from B, `in2` from C). Its salvo condition uses an **AND** term - it requires packets on **both** inputs before triggering an epoch.
#
# This demonstrates synchronization: D waits for both branches to complete before processing.

# %%
node_d = Node(
    name="D",
    in_ports={
        "in1": Port(PortSlotSpec.infinite()),
        "in2": Port(PortSlotSpec.infinite()),
    },
    in_salvo_conditions={
        "default": SalvoCondition(
            max_salvos=MaxSalvos.finite(1),
            ports=["in1", "in2"],
            # Require BOTH inputs to be non-empty (synchronization!)
            term=SalvoConditionTerm.and_(
                [
                    SalvoConditionTerm.port("in1", PortState.non_empty()),
                    SalvoConditionTerm.port("in2", PortState.non_empty()),
                ]
            ),
        ),
    },
)

print("Node D: merge node with TWO inputs")
print("  - in1: receives from B")
print("  - in2: receives from C")
print("  - Triggers only when BOTH inputs have packets (AND condition)")

# %% [markdown]
# ## Creating the Graph
#
# Now let's connect everything:
# - A.out1 → B.in
# - A.out2 → C.in
# - B.out → D.in1
# - C.out → D.in2

# %%
edges = [
    create_edge("A", "out1", "B", "in"),
    create_edge("A", "out2", "C", "in"),
    create_edge("B", "out", "D", "in1"),
    create_edge("C", "out", "D", "in2"),
]

graph = Graph([node_a, node_b, node_c, node_d], edges)

errors = graph.validate()
assert len(errors) == 0, f"Validation errors: {errors}"

print("Diamond graph created and validated!")
print(f"Nodes: {list(graph.nodes().keys())}")
print(f"Edges: {len(graph.edges())}")

# %% [markdown]
# ## Creating the Network and Packets
#
# We'll create two packets - one for each branch of the diamond.

# %%
net = NetSim(graph)

# Create two packets
response1, _ = net.do_action(NetAction.create_packet())
response2, _ = net.do_action(NetAction.create_packet())

assert isinstance(response1, NetActionResponseData.Packet)
assert isinstance(response2, NetActionResponseData.Packet)

packet1 = response1.packet_id
packet2 = response2.packet_id

print(f"Created packet 1: {packet1}")
print(f"Created packet 2: {packet2}")

# %% [markdown]
# ## Placing Packets on Edges from A
#
# Place each packet on a different branch:
# - Packet 1 → edge A.out1 → B
# - Packet 2 → edge A.out2 → C

# %%
# Place packet1 on edge A → B
net.do_action(
    NetAction.transport_packet_to_location(
        packet1, edge_location("A", "out1", "B", "in")
    )
)
print("Placed packet 1 on edge A → B")

# Place packet2 on edge A → C
net.do_action(
    NetAction.transport_packet_to_location(
        packet2, edge_location("A", "out2", "C", "in")
    )
)
print("Placed packet 2 on edge A → C")

# %% [markdown]
# ## First Run: Packets Move to B and C
#
# Running the network will move packets from edges to input ports, triggering epochs at both B and C.

# %%
net.run_until_blocked()

startable = net.get_startable_epochs()
print(f"Startable epochs: {len(startable)}")

for epoch_id in startable:
    epoch = net.get_epoch(epoch_id)
    print(f"  - Epoch at node {epoch.node_name}")

# %% [markdown]
# ## Processing B and C in Parallel
#
# Now we process both B and C. Each will:
# 1. Start its epoch
# 2. Consume the input packet
# 3. Create an output packet
# 4. Send it to D


# %%
def process_node(net: NetSim, epoch_id) -> None:
    """Process a simple pass-through node."""
    # Start the epoch
    response, _ = net.do_action(NetAction.start_epoch(epoch_id))
    assert isinstance(response, NetActionResponseData.StartedEpoch)
    epoch = response.epoch
    print(f"\nProcessing node {epoch.node_name}:")

    # Consume input packet
    input_packet = epoch.in_salvo.packets[0][1]
    net.do_action(NetAction.consume_packet(input_packet))
    print("  Consumed input packet")

    # Create output packet
    response, _ = net.do_action(NetAction.create_packet(epoch.id))
    assert isinstance(response, NetActionResponseData.Packet)
    output_packet = response.packet_id
    print("  Created output packet")

    # Load and send
    net.do_action(NetAction.load_packet_into_output_port(output_packet, "out"))
    net.do_action(NetAction.send_output_salvo(epoch.id, "default"))
    print("  Sent output to downstream edge")

    # Finish
    net.do_action(NetAction.finish_epoch(epoch.id))
    print("  Finished epoch")


# %%
# Process both B and C
for epoch_id in startable:
    process_node(net, epoch_id)

# %% [markdown]
# ## Second Run: Packets Move to D
#
# Now let's run the network again. The packets from B and C will move to D's input ports.

# %%
net.run_until_blocked()

# Check D's input ports
d_in1 = PacketLocation.input_port("D", "in1")
d_in2 = PacketLocation.input_port("D", "in2")

print("D's input ports after second run:")
print(f"  in1 (from B): {net.packet_count_at(d_in1)} packet(s)")
print(f"  in2 (from C): {net.packet_count_at(d_in2)} packet(s)")

# %% [markdown]
# ## Synchronization: D's Epoch is Ready
#
# Since D requires **both** inputs to be non-empty (AND condition), it should now have a startable epoch.

# %%
startable_d = net.get_startable_epochs()
print(f"Startable epochs at D: {len(startable_d)}")

if startable_d:
    d_epoch = net.get_epoch(startable_d[0])
    print("\nEpoch details:")
    print(f"  Node: {d_epoch.node_name}")
    print(f"  Packets in input salvo: {len(d_epoch.in_salvo.packets)}")

    for port_name, packet_id in d_epoch.in_salvo.packets:
        print(f"    - Port '{port_name}': packet {packet_id[:12]}...")

# %% [markdown]
# ## Summary
#
# We successfully demonstrated a diamond flow pattern:
#
# 1. **Branching**: Two packets were sent from A on different paths (A→B and A→C)
# 2. **Parallel Processing**: B and C processed their packets independently
# 3. **Synchronization**: D waited until **both** branches completed
# 4. **Merging**: D's epoch received packets from both B and C
#
# The key insight is D's salvo condition using `SalvoConditionTerm.and_()` to require both inputs. This is a fundamental pattern for coordinating parallel work in flow-based systems.

# %%
print("Diamond flow example complete!")
