//! Example: Linear packet flow through A -> B -> C
//!
//! This example demonstrates:
//! - Creating a simple linear graph
//! - Injecting a packet
//! - Running the network until blocked
//! - Starting epochs and processing packets
//! - Sending output salvos to continue flow

use netrun_sim::graph::{
    Edge, EdgeRef, Graph, Node, Port, PortRef, PortSlotSpec, PortState, PortType,
    SalvoCondition, SalvoConditionTerm,
};
use netrun_sim::net::{
    Net, NetAction, NetActionResponse, NetActionResponseData, PacketLocation,
};
use std::collections::HashMap;

fn main() {
    // Create a linear graph: A -> B -> C
    let graph = create_linear_graph();
    println!("Created graph with {} nodes", graph.nodes().len());

    // Create a network from the graph
    let mut net = Net::new(graph);

    // Create a packet outside the network
    let packet_id = match net.do_action(&NetAction::CreatePacket(None)) {
        NetActionResponse::Success(NetActionResponseData::Packet(id), _) => {
            println!("Created packet: {}", id);
            id
        }
        _ => panic!("Failed to create packet"),
    };

    // Transport packet to the edge A -> B
    let edge_a_b = PacketLocation::Edge(EdgeRef {
        source: PortRef {
            node_name: "A".to_string(),
            port_type: PortType::Output,
            port_name: "out".to_string(),
        },
        target: PortRef {
            node_name: "B".to_string(),
            port_type: PortType::Input,
            port_name: "in".to_string(),
        },
    });
    net.do_action(&NetAction::TransportPacketToLocation(packet_id.clone(), edge_a_b));
    println!("Placed packet on edge A -> B");

    // Run the network - packet moves to B's input port and triggers an epoch
    net.do_action(&NetAction::RunNetUntilBlocked);
    println!("Ran network until blocked");

    // Check for startable epochs
    let startable = net.get_startable_epochs();
    println!("Startable epochs: {}", startable.len());

    if let Some(epoch_id) = startable.first() {
        // Start the epoch
        match net.do_action(&NetAction::StartEpoch(epoch_id.clone())) {
            NetActionResponse::Success(NetActionResponseData::StartedEpoch(epoch), _) => {
                println!("Started epoch {} on node {}", epoch.id, epoch.node_name);

                // In a real scenario, external code would process the packet here
                // For this example, we'll just consume it and create an output

                // Consume the input packet
                net.do_action(&NetAction::ConsumePacket(packet_id));
                println!("Consumed input packet");

                // Create an output packet
                let output_packet = match net.do_action(&NetAction::CreatePacket(Some(epoch.id.clone()))) {
                    NetActionResponse::Success(NetActionResponseData::Packet(id), _) => id,
                    _ => panic!("Failed to create output packet"),
                };
                println!("Created output packet: {}", output_packet);

                // Load it into the output port
                net.do_action(&NetAction::LoadPacketIntoOutputPort(output_packet.clone(), "out".to_string()));
                println!("Loaded packet into output port");

                // Send the output salvo
                net.do_action(&NetAction::SendOutputSalvo(epoch.id.clone(), "default".to_string()));
                println!("Sent output salvo - packet is now on edge B -> C");

                // Finish the epoch
                net.do_action(&NetAction::FinishEpoch(epoch.id));
                println!("Finished epoch");

                // Run the network again - packet moves to C
                net.do_action(&NetAction::RunNetUntilBlocked);
                println!("Ran network until blocked again");

                // Check for new startable epochs at C
                let startable_c = net.get_startable_epochs();
                println!("New startable epochs (should be at C): {}", startable_c.len());
            }
            _ => panic!("Failed to start epoch"),
        }
    }

    println!("\nLinear flow example complete!");
}

/// Creates a linear graph: A -> B -> C
fn create_linear_graph() -> Graph {
    let nodes = vec![
        create_node("A", vec![], vec!["out"]),
        create_node("B", vec!["in"], vec!["out"]),
        create_node("C", vec!["in"], vec![]),
    ];

    let edges = vec![
        create_edge("A", "out", "B", "in"),
        create_edge("B", "out", "C", "in"),
    ];

    let graph = Graph::new(nodes, edges);
    assert!(graph.validate().is_empty(), "Graph validation failed");
    graph
}

fn create_node(name: &str, in_ports: Vec<&str>, out_ports: Vec<&str>) -> Node {
    let in_ports_map: HashMap<String, Port> = in_ports
        .iter()
        .map(|p| (p.to_string(), Port { slots_spec: PortSlotSpec::Infinite }))
        .collect();

    let out_ports_map: HashMap<String, Port> = out_ports
        .iter()
        .map(|p| (p.to_string(), Port { slots_spec: PortSlotSpec::Infinite }))
        .collect();

    // Default input salvo condition: trigger when any input port is non-empty
    let mut in_salvo_conditions = HashMap::new();
    if !in_ports.is_empty() {
        in_salvo_conditions.insert(
            "default".to_string(),
            SalvoCondition {
                max_salvos: 1,
                ports: in_ports.iter().map(|s| s.to_string()).collect(),
                term: SalvoConditionTerm::Port {
                    port_name: in_ports[0].to_string(),
                    state: PortState::NonEmpty,
                },
            },
        );
    }

    // Default output salvo condition: can always send when port is non-empty
    let mut out_salvo_conditions = HashMap::new();
    if !out_ports.is_empty() {
        out_salvo_conditions.insert(
            "default".to_string(),
            SalvoCondition {
                max_salvos: 0, // unlimited
                ports: out_ports.iter().map(|s| s.to_string()).collect(),
                term: SalvoConditionTerm::Port {
                    port_name: out_ports[0].to_string(),
                    state: PortState::NonEmpty,
                },
            },
        );
    }

    Node {
        name: name.to_string(),
        in_ports: in_ports_map,
        out_ports: out_ports_map,
        in_salvo_conditions,
        out_salvo_conditions,
    }
}

fn create_edge(src_node: &str, src_port: &str, tgt_node: &str, tgt_port: &str) -> (EdgeRef, Edge) {
    (
        EdgeRef {
            source: PortRef {
                node_name: src_node.to_string(),
                port_type: PortType::Output,
                port_name: src_port.to_string(),
            },
            target: PortRef {
                node_name: tgt_node.to_string(),
                port_type: PortType::Input,
                port_name: tgt_port.to_string(),
            },
        },
        Edge {},
    )
}
