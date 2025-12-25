"""Example: Linear packet flow through A -> B -> C

This example demonstrates:
- Creating a simple linear graph
- Injecting a packet
- Running the network until blocked
- Starting epochs and processing packets
- Sending output salvos to continue flow
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


def create_node(name: str, in_ports: list[str], out_ports: list[str]) -> Node:
    """Create a simple node with default salvo conditions."""
    in_ports_dict = {p: Port(PortSlotSpec.infinite()) for p in in_ports}
    out_ports_dict = {p: Port(PortSlotSpec.infinite()) for p in out_ports}

    # Default input salvo condition: trigger when first input port is non-empty
    in_salvo_conditions = {}
    if in_ports:
        in_salvo_conditions["default"] = SalvoCondition(
            max_salvos=1,
            ports=in_ports,
            term=SalvoConditionTerm.port(in_ports[0], PortState.non_empty()),
        )

    # Default output salvo condition: can always send when port is non-empty
    out_salvo_conditions = {}
    if out_ports:
        out_salvo_conditions["default"] = SalvoCondition(
            max_salvos=0,  # unlimited
            ports=out_ports,
            term=SalvoConditionTerm.port(out_ports[0], PortState.non_empty()),
        )

    return Node(
        name=name,
        in_ports=in_ports_dict,
        out_ports=out_ports_dict,
        in_salvo_conditions=in_salvo_conditions,
        out_salvo_conditions=out_salvo_conditions,
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


def main():
    # Create a linear graph: A -> B -> C
    nodes = [
        create_node("A", [], ["out"]),
        create_node("B", ["in"], ["out"]),
        create_node("C", ["in"], []),
    ]
    edges = [
        create_edge("A", "out", "B", "in"),
        create_edge("B", "out", "C", "in"),
    ]
    graph = Graph(nodes, edges)
    print(f"Created graph with {len(graph.nodes())} nodes")

    # Validate the graph
    errors = graph.validate()
    assert len(errors) == 0, f"Graph validation failed: {errors}"

    # Create a network from the graph
    net = Net(graph)

    # Create a packet outside the network
    response_data, events = net.do_action(NetAction.create_packet())
    assert isinstance(response_data, NetActionResponseData.Packet)
    packet_id = response_data.packet_id
    print(f"Created packet: {packet_id}")

    # Transport packet to the edge A -> B
    edge_a_b = PacketLocation.edge(
        EdgeRef(
            PortRef("A", PortType.Output, "out"),
            PortRef("B", PortType.Input, "in"),
        )
    )
    net.do_action(NetAction.transport_packet_to_location(packet_id, edge_a_b))
    print("Placed packet on edge A -> B")

    # Run the network - packet moves to B's input port and triggers an epoch
    net.do_action(NetAction.run_net_until_blocked())
    print("Ran network until blocked")

    # Check for startable epochs
    startable = net.get_startable_epochs()
    print(f"Startable epochs: {len(startable)}")

    if startable:
        epoch_id = startable[0]

        # Start the epoch
        response_data, events = net.do_action(NetAction.start_epoch(epoch_id))
        assert isinstance(response_data, NetActionResponseData.StartedEpoch)
        epoch = response_data.epoch
        print(f"Started epoch {epoch.id} on node {epoch.node_name}")

        # In a real scenario, external code would process the packet here
        # For this example, we'll just consume it and create an output

        # Consume the input packet
        net.do_action(NetAction.consume_packet(packet_id))
        print("Consumed input packet")

        # Create an output packet
        response_data, events = net.do_action(NetAction.create_packet(epoch.id))
        assert isinstance(response_data, NetActionResponseData.Packet)
        output_packet = response_data.packet_id
        print(f"Created output packet: {output_packet}")

        # Load it into the output port
        net.do_action(NetAction.load_packet_into_output_port(output_packet, "out"))
        print("Loaded packet into output port")

        # Send the output salvo
        net.do_action(NetAction.send_output_salvo(epoch.id, "default"))
        print("Sent output salvo - packet is now on edge B -> C")

        # Finish the epoch
        net.do_action(NetAction.finish_epoch(epoch.id))
        print("Finished epoch")

        # Run the network again - packet moves to C
        net.do_action(NetAction.run_net_until_blocked())
        print("Ran network until blocked again")

        # Check for new startable epochs at C
        startable_c = net.get_startable_epochs()
        print(f"New startable epochs (should be at C): {len(startable_c)}")

    print("\nLinear flow example complete!")


if __name__ == "__main__":
    main()
