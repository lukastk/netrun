"""Example: Diamond flow pattern with branching and merging

Graph structure:
       A
      / \\
     B   C
      \\ /
       D

This example demonstrates:
- Branching: A sends packets to both B and C
- Merging: D waits for packets from both B and C
- Synchronization: D's epoch only triggers when both inputs are present
"""

from netrun_sim import (
    Edge,
    EdgeRef,
    Graph,
    Net,
    NetAction,
    NetActionResponseData,
    Node,
    PacketLocation,
    Port,
    PortRef,
    PortSlotSpec,
    PortState,
    PortType,
    SalvoCondition,
    SalvoConditionTerm,
)


def create_edge(src_node: str, src_port: str, tgt_node: str, tgt_port: str) -> tuple[EdgeRef, Edge]:
    """Create an edge between two ports."""
    return (
        EdgeRef(
            PortRef(src_node, PortType.Output, src_port),
            PortRef(tgt_node, PortType.Input, tgt_port),
        ),
        Edge(),
    )


def edge_location(src_node: str, src_port: str, tgt_node: str, tgt_port: str) -> PacketLocation:
    """Create a PacketLocation for an edge."""
    return PacketLocation.edge(
        EdgeRef(
            PortRef(src_node, PortType.Output, src_port),
            PortRef(tgt_node, PortType.Input, tgt_port),
        )
    )


def create_simple_node(name: str) -> Node:
    """Create a simple node with one input and one output."""
    return Node(
        name=name,
        in_ports={"in": Port(PortSlotSpec.infinite())},
        out_ports={"out": Port(PortSlotSpec.infinite())},
        in_salvo_conditions={
            "default": SalvoCondition(
                max_salvos=1,
                ports=["in"],
                term=SalvoConditionTerm.port("in", PortState.non_empty()),
            ),
        },
        out_salvo_conditions={
            "default": SalvoCondition(
                max_salvos=0,
                ports=["out"],
                term=SalvoConditionTerm.port("out", PortState.non_empty()),
            ),
        },
    )


def create_diamond_graph() -> Graph:
    """Create the diamond graph A -> B,C -> D."""
    # Node A: source with two outputs
    node_a = Node(
        name="A",
        in_ports={},
        out_ports={
            "out1": Port(PortSlotSpec.infinite()),
            "out2": Port(PortSlotSpec.infinite()),
        },
        in_salvo_conditions={},
        out_salvo_conditions={},
    )

    # Node B: one input, one output
    node_b = create_simple_node("B")

    # Node C: one input, one output
    node_c = create_simple_node("C")

    # Node D: TWO inputs (requires both), no outputs
    node_d = Node(
        name="D",
        in_ports={
            "in1": Port(PortSlotSpec.infinite()),
            "in2": Port(PortSlotSpec.infinite()),
        },
        out_ports={},
        in_salvo_conditions={
            "default": SalvoCondition(
                max_salvos=1,
                ports=["in1", "in2"],
                # Require BOTH inputs to be non-empty
                term=SalvoConditionTerm.and_([
                    SalvoConditionTerm.port("in1", PortState.non_empty()),
                    SalvoConditionTerm.port("in2", PortState.non_empty()),
                ]),
            ),
        },
        out_salvo_conditions={},
    )

    edges = [
        create_edge("A", "out1", "B", "in"),
        create_edge("A", "out2", "C", "in"),
        create_edge("B", "out", "D", "in1"),
        create_edge("C", "out", "D", "in2"),
    ]

    graph = Graph([node_a, node_b, node_c, node_d], edges)
    errors = graph.validate()
    assert len(errors) == 0, f"Graph validation failed: {errors}"
    return graph


def main():
    # Create a diamond graph
    graph = create_diamond_graph()
    print("Created diamond graph: A -> B,C -> D")
    print("D requires inputs from BOTH B and C\n")

    net = Net(graph)

    # Create two packets and place them on edges from A
    response1, _ = net.do_action(NetAction.create_packet())
    response2, _ = net.do_action(NetAction.create_packet())
    assert isinstance(response1, NetActionResponseData.Packet)
    assert isinstance(response2, NetActionResponseData.Packet)
    packet1 = response1.packet_id
    packet2 = response2.packet_id
    print(f"Created packets: {packet1} and {packet2}")

    # Place packet1 on edge A -> B
    net.do_action(
        NetAction.transport_packet_to_location(packet1, edge_location("A", "out1", "B", "in"))
    )
    print("Placed packet1 on edge A -> B")

    # Place packet2 on edge A -> C
    net.do_action(
        NetAction.transport_packet_to_location(packet2, edge_location("A", "out2", "C", "in"))
    )
    print("Placed packet2 on edge A -> C")

    # Run network - packets move to B and C, triggering epochs
    net.do_action(NetAction.run_net_until_blocked())

    startable = net.get_startable_epochs()
    print(f"\nAfter first run: {len(startable)} startable epochs (B and C)")

    # Process B and C, sending outputs to D
    for epoch_id in startable:
        epoch = net.get_epoch(epoch_id)
        assert epoch is not None
        node_name = epoch.node_name
        print(f"\nProcessing node {node_name}")

        # Start the epoch
        response, _ = net.do_action(NetAction.start_epoch(epoch_id))
        assert isinstance(response, NetActionResponseData.StartedEpoch)
        started = response.epoch

        # Find and consume the input packet
        input_packet = started.in_salvo.packets[0][1]
        net.do_action(NetAction.consume_packet(input_packet))

        # Create output packet
        response, _ = net.do_action(NetAction.create_packet(started.id))
        assert isinstance(response, NetActionResponseData.Packet)
        output = response.packet_id

        # Load into output port and send
        net.do_action(NetAction.load_packet_into_output_port(output, "out"))
        net.do_action(NetAction.send_output_salvo(started.id, "default"))

        # Finish epoch
        net.do_action(NetAction.finish_epoch(started.id))
        print(f"  Finished {node_name} - sent packet to D")

    # Run network - packets move from B->D and C->D edges to D's input ports
    net.do_action(NetAction.run_net_until_blocked())

    # Check D's input ports
    d_in1 = PacketLocation.input_port("D", "in1")
    d_in2 = PacketLocation.input_port("D", "in2")
    print("\nD's input ports:")
    print(f"  in1 (from B): {net.packet_count_at(d_in1)} packets")
    print(f"  in2 (from C): {net.packet_count_at(d_in2)} packets")

    # D should now have a startable epoch (both inputs present)
    startable_d = net.get_startable_epochs()
    print(f"\nStartable epochs at D: {len(startable_d)}")

    if startable_d:
        d_epoch_id = startable_d[0]
        d_epoch = net.get_epoch(d_epoch_id)
        assert d_epoch is not None
        print(f"D's epoch received {len(d_epoch.in_salvo.packets)} packets from both branches!")

    print("\nDiamond flow example complete!")


if __name__ == "__main__":
    main()
