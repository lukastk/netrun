//! Example: Diamond flow pattern with branching and merging
//!
//! Graph structure:
//!        A
//!       / \
//!      B   C
//!       \ /
//!        D
//!
//! This example demonstrates:
//! - Branching: A sends packets to both B and C
//! - Merging: D waits for packets from both B and C
//! - Synchronization: D's epoch only triggers when both inputs are present

use netrun_sim::graph::{
    Edge, EdgeRef, Graph, Node, Port, PortRef, PortSlotSpec, PortState, PortType,
    SalvoCondition, SalvoConditionTerm,
};
use netrun_sim::net::{
    Net, NetAction, NetActionResponse, NetActionResponseData, PacketLocation,
};
use std::collections::HashMap;

fn main() {
    // Create a diamond graph
    let graph = create_diamond_graph();
    println!("Created diamond graph: A -> B,C -> D");
    println!("D requires inputs from BOTH B and C\n");

    let mut net = Net::new(graph);

    // Create two packets and place them on edges from A
    let packet1 = create_packet(&mut net);
    let packet2 = create_packet(&mut net);
    println!("Created packets: {} and {}", packet1, packet2);

    // Place packet1 on edge A -> B
    let edge_a_b = edge_location("A", "out1", "B", "in");
    net.do_action(&NetAction::TransportPacketToLocation(packet1.clone(), edge_a_b));
    println!("Placed packet1 on edge A -> B");

    // Place packet2 on edge A -> C
    let edge_a_c = edge_location("A", "out2", "C", "in");
    net.do_action(&NetAction::TransportPacketToLocation(packet2.clone(), edge_a_c));
    println!("Placed packet2 on edge A -> C");

    // Run network - packets move to B and C, triggering epochs
    net.do_action(&NetAction::RunNetUntilBlocked);

    let startable = net.get_startable_epochs();
    println!("\nAfter first run: {} startable epochs (B and C)", startable.len());

    // Process B and C, sending outputs to D
    for epoch_id in startable {
        let epoch = net.get_epoch(&epoch_id).unwrap();
        let node_name = epoch.node_name.clone();
        println!("\nProcessing node {}", node_name);

        // Start the epoch
        let started = match net.do_action(&NetAction::StartEpoch(epoch_id.clone())) {
            NetActionResponse::Success(NetActionResponseData::StartedEpoch(e), _) => e,
            _ => panic!("Failed to start epoch"),
        };

        // Find and consume the input packet
        let input_packet = started.in_salvo.packets[0].1.clone();
        net.do_action(&NetAction::ConsumePacket(input_packet));

        // Create output packet
        let output = create_packet_in_epoch(&mut net, &started.id);

        // Load into output port and send
        net.do_action(&NetAction::LoadPacketIntoOutputPort(output, "out".to_string()));
        net.do_action(&NetAction::SendOutputSalvo(started.id.clone(), "default".to_string()));

        // Finish epoch
        net.do_action(&NetAction::FinishEpoch(started.id));
        println!("  Finished {} - sent packet to D", node_name);
    }

    // Run network - packets move from B->D and C->D edges to D's input ports
    net.do_action(&NetAction::RunNetUntilBlocked);

    // Check D's input ports
    let d_in1 = PacketLocation::InputPort("D".to_string(), "in1".to_string());
    let d_in2 = PacketLocation::InputPort("D".to_string(), "in2".to_string());
    println!("\nD's input ports:");
    println!("  in1 (from B): {} packets", net.packet_count_at(&d_in1));
    println!("  in2 (from C): {} packets", net.packet_count_at(&d_in2));

    // D should now have a startable epoch (both inputs present)
    let startable_d = net.get_startable_epochs();
    println!("\nStartable epochs at D: {}", startable_d.len());

    if let Some(d_epoch_id) = startable_d.first() {
        let d_epoch = net.get_epoch(d_epoch_id).unwrap();
        println!("D's epoch received {} packets from both branches!", d_epoch.in_salvo.packets.len());
    }

    println!("\nDiamond flow example complete!");
}

fn create_diamond_graph() -> Graph {
    // Node A: source with two outputs
    let node_a = Node {
        name: "A".to_string(),
        in_ports: HashMap::new(),
        out_ports: [
            ("out1".to_string(), Port { slots_spec: PortSlotSpec::Infinite }),
            ("out2".to_string(), Port { slots_spec: PortSlotSpec::Infinite }),
        ].into(),
        in_salvo_conditions: HashMap::new(),
        out_salvo_conditions: HashMap::new(),
    };

    // Node B: one input, one output
    let node_b = create_simple_node("B");

    // Node C: one input, one output
    let node_c = create_simple_node("C");

    // Node D: TWO inputs (requires both), no outputs
    let node_d = Node {
        name: "D".to_string(),
        in_ports: [
            ("in1".to_string(), Port { slots_spec: PortSlotSpec::Infinite }),
            ("in2".to_string(), Port { slots_spec: PortSlotSpec::Infinite }),
        ].into(),
        out_ports: HashMap::new(),
        in_salvo_conditions: [(
            "default".to_string(),
            SalvoCondition {
                max_salvos: 1,
                ports: vec!["in1".to_string(), "in2".to_string()],
                // Require BOTH inputs to be non-empty
                term: SalvoConditionTerm::And(vec![
                    SalvoConditionTerm::Port {
                        port_name: "in1".to_string(),
                        state: PortState::NonEmpty,
                    },
                    SalvoConditionTerm::Port {
                        port_name: "in2".to_string(),
                        state: PortState::NonEmpty,
                    },
                ]),
            },
        )].into(),
        out_salvo_conditions: HashMap::new(),
    };

    let edges = vec![
        create_edge("A", "out1", "B", "in"),
        create_edge("A", "out2", "C", "in"),
        create_edge("B", "out", "D", "in1"),
        create_edge("C", "out", "D", "in2"),
    ];

    let graph = Graph::new(vec![node_a, node_b, node_c, node_d], edges);
    assert!(graph.validate().is_empty(), "Graph validation failed");
    graph
}

fn create_simple_node(name: &str) -> Node {
    Node {
        name: name.to_string(),
        in_ports: [("in".to_string(), Port { slots_spec: PortSlotSpec::Infinite })].into(),
        out_ports: [("out".to_string(), Port { slots_spec: PortSlotSpec::Infinite })].into(),
        in_salvo_conditions: [(
            "default".to_string(),
            SalvoCondition {
                max_salvos: 1,
                ports: vec!["in".to_string()],
                term: SalvoConditionTerm::Port {
                    port_name: "in".to_string(),
                    state: PortState::NonEmpty,
                },
            },
        )].into(),
        out_salvo_conditions: [(
            "default".to_string(),
            SalvoCondition {
                max_salvos: 0,
                ports: vec!["out".to_string()],
                term: SalvoConditionTerm::Port {
                    port_name: "out".to_string(),
                    state: PortState::NonEmpty,
                },
            },
        )].into(),
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

fn edge_location(src_node: &str, src_port: &str, tgt_node: &str, tgt_port: &str) -> PacketLocation {
    PacketLocation::Edge(EdgeRef {
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
    })
}

fn create_packet(net: &mut Net) -> ulid::Ulid {
    match net.do_action(&NetAction::CreatePacket(None)) {
        NetActionResponse::Success(NetActionResponseData::Packet(id), _) => id,
        _ => panic!("Failed to create packet"),
    }
}

fn create_packet_in_epoch(net: &mut Net, epoch_id: &ulid::Ulid) -> ulid::Ulid {
    match net.do_action(&NetAction::CreatePacket(Some(epoch_id.clone()))) {
        NetActionResponse::Success(NetActionResponseData::Packet(id), _) => id,
        _ => panic!("Failed to create packet in epoch"),
    }
}
