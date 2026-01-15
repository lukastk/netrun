use super::*;
use crate::test_fixtures::*;
use std::collections::HashMap;

// ========== State Snapshot Helpers for Exact Undo Verification ==========

/// A complete snapshot of the NetSim state for comparison.
#[derive(Debug, Clone, PartialEq)]
struct NetSimSnapshot {
    /// All packets with their IDs and locations
    packets: HashMap<PacketID, PacketLocation>,
    /// Packets at each location, in order (for IndexSet ordering verification)
    packets_by_location: HashMap<PacketLocation, Vec<PacketID>>,
    /// All epochs with their full state
    epochs: HashMap<EpochID, EpochSnapshot>,
    /// Set of startable epoch IDs
    startable_epochs: Vec<EpochID>,
    /// Node to epochs mapping
    node_to_epochs: HashMap<String, Vec<EpochID>>,
}

#[derive(Debug, Clone, PartialEq)]
struct EpochSnapshot {
    id: EpochID,
    node_name: String,
    state: EpochState,
    in_salvo_condition: String,
    in_salvo_packets: Vec<(String, PacketID)>,
    out_salvos_count: usize,
}

impl NetSimSnapshot {
    /// Capture the complete state of a NetSim.
    fn capture(net: &NetSim) -> Self {
        // Capture packets
        let packets: HashMap<PacketID, PacketLocation> = net
            ._packets
            .iter()
            .map(|(id, p)| (*id, p.location.clone()))
            .collect();

        // Capture packets_by_location with ordering
        let packets_by_location: HashMap<PacketLocation, Vec<PacketID>> = net
            ._packets_by_location
            .iter()
            .map(|(loc, ids)| (loc.clone(), ids.iter().cloned().collect()))
            .collect();

        // Capture epochs
        let epochs: HashMap<EpochID, EpochSnapshot> = net
            ._epochs
            .iter()
            .map(|(id, e)| {
                (
                    *id,
                    EpochSnapshot {
                        id: e.id,
                        node_name: e.node_name.clone(),
                        state: e.state.clone(),
                        in_salvo_condition: e.in_salvo.salvo_condition.clone(),
                        in_salvo_packets: e.in_salvo.packets.clone(),
                        out_salvos_count: e.out_salvos.len(),
                    },
                )
            })
            .collect();

        // Capture startable epochs (sorted for deterministic comparison)
        let mut startable_epochs: Vec<EpochID> = net._startable_epochs.iter().cloned().collect();
        startable_epochs.sort();

        // Capture node_to_epochs
        let node_to_epochs: HashMap<String, Vec<EpochID>> = net
            ._node_to_epochs
            .iter()
            .map(|(node, ids)| (node.clone(), ids.clone()))
            .collect();

        NetSimSnapshot {
            packets,
            packets_by_location,
            epochs,
            startable_epochs,
            node_to_epochs,
        }
    }

    /// Compare two snapshots and return a description of differences.
    fn diff(&self, other: &NetSimSnapshot) -> Vec<String> {
        let mut diffs = Vec::new();

        // Compare packets
        if self.packets != other.packets {
            diffs.push(format!(
                "Packets differ:\n  before: {:?}\n  after:  {:?}",
                self.packets, other.packets
            ));
        }

        // Compare packets_by_location (including ordering)
        if self.packets_by_location != other.packets_by_location {
            for (loc, before_ids) in &self.packets_by_location {
                match other.packets_by_location.get(loc) {
                    Some(after_ids) if before_ids != after_ids => {
                        diffs.push(format!(
                            "Packet order at {:?} differs:\n  before: {:?}\n  after:  {:?}",
                            loc, before_ids, after_ids
                        ));
                    }
                    None => {
                        diffs.push(format!("Location {:?} missing after undo", loc));
                    }
                    _ => {}
                }
            }
            for loc in other.packets_by_location.keys() {
                if !self.packets_by_location.contains_key(loc) {
                    diffs.push(format!("Extra location {:?} after undo", loc));
                }
            }
        }

        // Compare epochs
        if self.epochs != other.epochs {
            diffs.push(format!(
                "Epochs differ:\n  before: {:?}\n  after:  {:?}",
                self.epochs, other.epochs
            ));
        }

        // Compare startable_epochs
        if self.startable_epochs != other.startable_epochs {
            diffs.push(format!(
                "Startable epochs differ:\n  before: {:?}\n  after:  {:?}",
                self.startable_epochs, other.startable_epochs
            ));
        }

        // Compare node_to_epochs
        if self.node_to_epochs != other.node_to_epochs {
            diffs.push(format!(
                "Node to epochs differ:\n  before: {:?}\n  after:  {:?}",
                self.node_to_epochs, other.node_to_epochs
            ));
        }

        diffs
    }
}

// Helper to extract packet ID from create response
fn get_packet_id(response: &NetActionResponse) -> PacketID {
    match response {
        NetActionResponse::Success(NetActionResponseData::Packet(id), _) => id.clone(),
        _ => panic!("Expected Packet response, got: {:?}", response),
    }
}

// Helper to extract epoch from create response
fn get_created_epoch(response: &NetActionResponse) -> Epoch {
    match response {
        NetActionResponse::Success(NetActionResponseData::CreatedEpoch(epoch), _) => epoch.clone(),
        _ => panic!("Expected CreatedEpoch response, got: {:?}", response),
    }
}

// Helper to extract epoch from start response
fn get_started_epoch(response: &NetActionResponse) -> Epoch {
    match response {
        NetActionResponse::Success(NetActionResponseData::StartedEpoch(epoch), _) => epoch.clone(),
        _ => panic!("Expected StartedEpoch response, got: {:?}", response),
    }
}

// ========== Packet Creation and Consumption Tests ==========

#[test]
fn test_create_packet_outside_net() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    let response = net.do_action(&NetAction::CreatePacket(None));
    assert!(matches!(
        response,
        NetActionResponse::Success(NetActionResponseData::Packet(_), _)
    ));
}

#[test]
fn test_consume_packet() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create a packet
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    // Consume it
    let response = net.do_action(&NetAction::ConsumePacket(packet_id));
    assert!(matches!(
        response,
        NetActionResponse::Success(NetActionResponseData::None, _)
    ));
}

#[test]
fn test_consume_nonexistent_packet_fails() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    let fake_id = Ulid::new();
    let response = net.do_action(&NetAction::ConsumePacket(fake_id));
    assert!(matches!(
        response,
        NetActionResponse::Error(NetActionError::PacketNotFound { .. })
    ));
}

// ========== Epoch Lifecycle Tests ==========

#[test]
fn test_epoch_lifecycle_via_run_until_blocked() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Put a packet on edge A->B
    let response = net.do_action(&NetAction::CreatePacket(None));
    let packet_id = get_packet_id(&response);

    // Manually place packet on edge (simulating it came from node A)
    let edge_location = PacketLocation::Edge(Edge {
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
    net._packets.get_mut(&packet_id).unwrap().location = edge_location.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&edge_location)
        .unwrap()
        .insert(packet_id.clone());

    // Run until blocked - packet should move to input port and trigger epoch
    net.run_until_blocked();

    // Should have one startable epoch
    let startable = net.startable_epoch_ids();
    assert_eq!(startable.len(), 1);

    // Start the epoch
    let epoch_id = startable[0].clone();
    let epoch = get_started_epoch(&net.do_action(&NetAction::StartEpoch(epoch_id.clone())));
    assert!(matches!(epoch.state, EpochState::Running));

    // Consume the packet (simulating node processing)
    net.do_action(&NetAction::ConsumePacket(packet_id));

    // Finish the epoch
    let response = net.do_action(&NetAction::FinishEpoch(epoch_id));
    assert!(matches!(
        response,
        NetActionResponse::Success(NetActionResponseData::FinishedEpoch(_), _)
    ));
}

#[test]
fn test_cannot_start_nonexistent_epoch() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    let fake_id = Ulid::new();
    let response = net.do_action(&NetAction::StartEpoch(fake_id));
    assert!(matches!(
        response,
        NetActionResponse::Error(NetActionError::EpochNotFound { .. })
    ));
}

#[test]
fn test_cannot_finish_epoch_with_packets() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create epoch with packet via CreateEpoch
    // First put packet at input port
    let response = net.do_action(&NetAction::CreatePacket(None));
    let packet_id = get_packet_id(&response);

    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&input_port_loc)
        .unwrap()
        .insert(packet_id.clone());

    // Create and start epoch
    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id)],
    };
    let epoch = get_created_epoch(&net.do_action(&NetAction::CreateEpoch("B".to_string(), salvo)));
    let epoch = get_started_epoch(&net.do_action(&NetAction::StartEpoch(epoch.id)));

    // Try to finish without consuming packet
    let response = net.do_action(&NetAction::FinishEpoch(epoch.id));
    assert!(matches!(
        response,
        NetActionResponse::Error(NetActionError::CannotFinishNonEmptyEpoch { .. })
    ));
}

#[test]
fn test_cannot_finish_epoch_with_unsent_output_salvo() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create packet and place at input port
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&input_port_loc)
        .unwrap()
        .insert(packet_id.clone());

    // Create and start epoch
    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id.clone())],
    };
    let epoch = get_created_epoch(&net.do_action(&NetAction::CreateEpoch("B".to_string(), salvo)));
    let epoch = get_started_epoch(&net.do_action(&NetAction::StartEpoch(epoch.id)));

    // Consume the input packet
    net.do_action(&NetAction::ConsumePacket(packet_id));

    // Create an output packet and load it into output port
    let output_packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(Some(epoch.id))));
    net.do_action(&NetAction::LoadPacketIntoOutputPort(
        output_packet_id,
        "out".to_string(),
    ));

    // Try to finish without sending the output salvo
    let response = net.do_action(&NetAction::FinishEpoch(epoch.id));
    assert!(matches!(
        response,
        NetActionResponse::Error(NetActionError::UnsentOutputSalvo { .. })
    ));
}

// ========== Cancel Epoch Tests ==========

#[test]
fn test_cancel_epoch_destroys_packets() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create packet and place at input port
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&input_port_loc)
        .unwrap()
        .insert(packet_id.clone());

    // Create and start epoch
    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id.clone())],
    };
    let epoch = get_created_epoch(&net.do_action(&NetAction::CreateEpoch("B".to_string(), salvo)));
    let epoch = get_started_epoch(&net.do_action(&NetAction::StartEpoch(epoch.id)));

    // Cancel the epoch
    let response = net.do_action(&NetAction::CancelEpoch(epoch.id));
    match response {
        NetActionResponse::Success(NetActionResponseData::CancelledEpoch(_, destroyed), _) => {
            assert_eq!(destroyed.len(), 1);
            assert_eq!(destroyed[0], packet_id);
        }
        _ => panic!("Expected CancelledEpoch response"),
    }

    // Packet should be gone
    assert!(!net._packets.contains_key(&packet_id));
}

// ========== Run Until Blocked Tests ==========

#[test]
fn test_run_until_blocked_moves_packet_to_input_port() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create packet on edge A->B
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let edge_location = PacketLocation::Edge(Edge {
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
    net._packets.get_mut(&packet_id).unwrap().location = edge_location.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&edge_location)
        .unwrap()
        .insert(packet_id.clone());

    // Run until blocked
    net.run_until_blocked();

    // Packet should have triggered an epoch (moved into node)
    assert_eq!(net.startable_epoch_ids().len(), 1);
}

#[test]
fn test_run_until_blocked_respects_port_capacity() {
    // Create a graph where node B has finite capacity input port but NO salvo conditions
    // This tests that port capacity limits how many packets can wait at the input port
    use crate::graph::Node;
    use std::collections::HashMap;
    let node_b = Node {
        name: "B".to_string(),
        in_ports: {
            let mut ports = HashMap::new();
            ports.insert(
                "in".to_string(),
                Port {
                    slots_spec: PortSlotSpec::Finite(1),
                },
            );
            ports
        },
        out_ports: HashMap::new(),
        in_salvo_conditions: HashMap::new(), // No salvo conditions = packets wait at input port
        out_salvo_conditions: HashMap::new(),
    };

    let nodes = vec![simple_node("A", vec![], vec!["out"]), node_b];
    let edges = vec![edge("A", "out", "B", "in")];
    let graph = Graph::new(nodes, edges);
    let mut net = NetSim::new(graph);

    // Create two packets on edge A->B
    let edge_location = PacketLocation::Edge(Edge {
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

    let packet1 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let packet2 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    // Move both to edge
    for pid in [&packet1, &packet2] {
        net._packets.get_mut(pid).unwrap().location = edge_location.clone();
        net._packets_by_location
            .get_mut(&PacketLocation::OutsideNet)
            .unwrap()
            .shift_remove(pid);
        net._packets_by_location
            .get_mut(&edge_location)
            .unwrap()
            .insert(pid.clone());
    }

    // Run until blocked - only first packet should move (capacity = 1)
    net.run_until_blocked();

    // No epochs created (no salvo conditions), one packet at input port, one still on edge
    assert_eq!(net.startable_epoch_ids().len(), 0);
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    assert_eq!(net.packet_count_at(&input_port_loc), 1);
    assert_eq!(net.packet_count_at(&edge_location), 1);
}

#[test]
fn test_fifo_packet_ordering() {
    // Test that packets are processed in FIFO order on edges
    // We verify this by examining the events emitted during run_until_blocked
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    let edge_location = PacketLocation::Edge(Edge {
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

    // Create packets in order
    let packet1 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let packet2 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let packet3 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    // Add to edge in order (packet1 first)
    for pid in [&packet1, &packet2, &packet3] {
        net._packets.get_mut(pid).unwrap().location = edge_location.clone();
        net._packets_by_location
            .get_mut(&PacketLocation::OutsideNet)
            .unwrap()
            .shift_remove(pid);
        net._packets_by_location
            .get_mut(&edge_location)
            .unwrap()
            .insert(pid.clone());
    }

    // Run - each packet triggers its own epoch (default salvo condition)
    let events = net.run_until_blocked();

    // Find the first PacketMoved event for each packet (when it moved from edge to input port)
    // The order of these events should match the FIFO order: packet1, packet2, packet3
    let packet_move_order: Vec<PacketID> = events
        .iter()
        .filter_map(|event| {
            if let NetEvent::PacketMoved(_, packet_id, _, PacketLocation::InputPort(_, _), _) =
                event
            {
                Some(packet_id.clone())
            } else {
                None
            }
        })
        .collect();

    // We should have 3 packet moves to input ports, in FIFO order
    assert_eq!(
        packet_move_order.len(),
        3,
        "Expected 3 packets to move to input port"
    );
    assert_eq!(
        packet_move_order[0], packet1,
        "First packet to move should be packet1"
    );
    assert_eq!(
        packet_move_order[1], packet2,
        "Second packet to move should be packet2"
    );
    assert_eq!(
        packet_move_order[2], packet3,
        "Third packet to move should be packet3"
    );
}

// ========== Output Salvo Tests ==========

#[test]
fn test_load_packet_into_output_port() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create packet and place at input port of B
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&input_port_loc)
        .unwrap()
        .insert(packet_id.clone());

    // Create and start epoch
    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id.clone())],
    };
    let epoch = get_created_epoch(&net.do_action(&NetAction::CreateEpoch("B".to_string(), salvo)));
    let epoch = get_started_epoch(&net.do_action(&NetAction::StartEpoch(epoch.id)));

    // Load packet into output port
    let response = net.do_action(&NetAction::LoadPacketIntoOutputPort(
        packet_id.clone(),
        "out".to_string(),
    ));
    assert!(matches!(
        response,
        NetActionResponse::Success(NetActionResponseData::None, _)
    ));

    // Packet should be at output port
    let output_loc = PacketLocation::OutputPort(epoch.id, "out".to_string());
    assert_eq!(net.packet_count_at(&output_loc), 1);
}

#[test]
fn test_send_output_salvo() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create packet and place at input port of B
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&input_port_loc)
        .unwrap()
        .insert(packet_id.clone());

    // Create and start epoch
    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id.clone())],
    };
    let epoch = get_created_epoch(&net.do_action(&NetAction::CreateEpoch("B".to_string(), salvo)));
    let epoch = get_started_epoch(&net.do_action(&NetAction::StartEpoch(epoch.id)));

    // Load packet into output port
    net.do_action(&NetAction::LoadPacketIntoOutputPort(
        packet_id.clone(),
        "out".to_string(),
    ));

    // Send output salvo
    let response = net.do_action(&NetAction::SendOutputSalvo(
        epoch.id.clone(),
        "default".to_string(),
    ));
    assert!(matches!(
        response,
        NetActionResponse::Success(NetActionResponseData::None, _)
    ));

    // Packet should now be on edge B->C
    let edge_loc = PacketLocation::Edge(Edge {
        source: PortRef {
            node_name: "B".to_string(),
            port_type: PortType::Output,
            port_name: "out".to_string(),
        },
        target: PortRef {
            node_name: "C".to_string(),
            port_type: PortType::Input,
            port_name: "in".to_string(),
        },
    });
    assert_eq!(net.packet_count_at(&edge_loc), 1);
}

// ========== Create Epoch Tests ==========

#[test]
fn test_create_epoch() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create packet at input port
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&input_port_loc)
        .unwrap()
        .insert(packet_id.clone());

    // Create epoch manually (should be in Startable state)
    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id.clone())],
    };
    let epoch = get_created_epoch(&net.do_action(&NetAction::CreateEpoch("B".to_string(), salvo)));

    assert!(matches!(epoch.state, EpochState::Startable));
    assert_eq!(epoch.node_name, "B");

    // Epoch should be in startable list
    assert!(net.get_startable_epochs().contains(&epoch.id));

    // Now start it
    let epoch = get_started_epoch(&net.do_action(&NetAction::StartEpoch(epoch.id)));
    assert!(matches!(epoch.state, EpochState::Running));
}

#[test]
fn test_create_epoch_validates_node() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![],
    };
    let response = net.do_action(&NetAction::CreateEpoch("NonExistent".to_string(), salvo));
    assert!(matches!(
        response,
        NetActionResponse::Error(NetActionError::NodeNotFound { .. })
    ));
}

#[test]
fn test_create_epoch_validates_packet_location() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create packet but leave it OutsideNet
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id)],
    };
    let response = net.do_action(&NetAction::CreateEpoch("B".to_string(), salvo));
    assert!(matches!(
        response,
        NetActionResponse::Error(NetActionError::PacketNotAtInputPort { .. })
    ));
}

// ========== Undo Tests ==========

#[test]
fn test_undo_create_packet() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create a packet
    let action = NetAction::CreatePacket(None);
    let response = net.do_action(&action);
    let (packet_id, events) = match response {
        NetActionResponse::Success(NetActionResponseData::Packet(id), events) => (id, events),
        _ => panic!("Expected Packet response"),
    };

    // Verify packet exists
    assert!(net.get_packet(&packet_id).is_some());

    // Undo the creation
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Verify packet no longer exists
    assert!(net.get_packet(&packet_id).is_none());
    assert_eq!(net.packet_count_at(&PacketLocation::OutsideNet), 0);
}

#[test]
fn test_undo_consume_packet() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create a packet
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    // Consume the packet
    let action = NetAction::ConsumePacket(packet_id);
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Verify packet is gone
    assert!(net.get_packet(&packet_id).is_none());

    // Undo the consumption
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Verify packet is restored at original location
    assert!(net.get_packet(&packet_id).is_some());
    assert_eq!(
        net.get_packet(&packet_id).unwrap().location,
        PacketLocation::OutsideNet
    );
}

#[test]
fn test_undo_start_epoch() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create packet and place at input port
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&input_port_loc)
        .unwrap()
        .insert(packet_id);

    // Create epoch
    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id)],
    };
    let epoch = get_created_epoch(&net.do_action(&NetAction::CreateEpoch("B".to_string(), salvo)));
    let epoch_id = epoch.id;

    // Start epoch
    let action = NetAction::StartEpoch(epoch_id);
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Verify epoch is Running
    assert!(matches!(
        net.get_epoch(&epoch_id).unwrap().state,
        EpochState::Running
    ));
    assert!(!net.get_startable_epochs().contains(&epoch_id));

    // Undo the start
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Verify epoch is back to Startable
    assert!(matches!(
        net.get_epoch(&epoch_id).unwrap().state,
        EpochState::Startable
    ));
    assert!(net.get_startable_epochs().contains(&epoch_id));
}

#[test]
fn test_undo_finish_epoch() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create packet and place at input port
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&input_port_loc)
        .unwrap()
        .insert(packet_id);

    // Create and start epoch
    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id)],
    };
    let epoch = get_created_epoch(&net.do_action(&NetAction::CreateEpoch("B".to_string(), salvo)));
    let epoch_id = epoch.id;
    net.do_action(&NetAction::StartEpoch(epoch_id));

    // Consume packet so epoch can be finished
    net.do_action(&NetAction::ConsumePacket(packet_id));

    // Finish epoch
    let action = NetAction::FinishEpoch(epoch_id);
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Verify epoch is gone
    assert!(net.get_epoch(&epoch_id).is_none());

    // Undo the finish
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Verify epoch is restored and Running
    assert!(net.get_epoch(&epoch_id).is_some());
    assert!(matches!(
        net.get_epoch(&epoch_id).unwrap().state,
        EpochState::Running
    ));
}

#[test]
fn test_undo_transport_packet_to_location() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create a packet
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    // Transport to input port
    let input_port = PacketLocation::InputPort("B".to_string(), "in".to_string());
    let action = NetAction::TransportPacketToLocation(packet_id, input_port.clone());
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Verify packet is at input port
    assert_eq!(net.get_packet(&packet_id).unwrap().location, input_port);

    // Undo the transport
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Verify packet is back at OutsideNet
    assert_eq!(
        net.get_packet(&packet_id).unwrap().location,
        PacketLocation::OutsideNet
    );
}

#[test]
fn test_undo_run_step() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create packet on edge A->B
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let edge_location = PacketLocation::Edge(Edge {
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
    net._packets.get_mut(&packet_id).unwrap().location = edge_location.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&edge_location)
        .unwrap()
        .insert(packet_id);

    // Run one step
    let action = NetAction::RunStep;
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Verify an epoch was created
    assert_eq!(net.get_startable_epochs().len(), 1);
    let epoch_id = net.get_startable_epochs()[0];

    // Undo the step
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Verify epoch is gone
    assert!(net.get_epoch(&epoch_id).is_none());
    assert_eq!(net.get_startable_epochs().len(), 0);

    // Verify packet is back on edge
    assert_eq!(net.get_packet(&packet_id).unwrap().location, edge_location);
    assert_eq!(net.packet_count_at(&edge_location), 1);
}

#[test]
fn test_undo_preserves_packet_ordering() {
    // Test that undoing packet moves preserves FIFO ordering
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create 3 packets at OutsideNet
    let packet1 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let packet2 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let packet3 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    // Verify initial order at OutsideNet
    let initial_order = net.get_packets_at_location(&PacketLocation::OutsideNet);
    assert_eq!(initial_order, vec![packet1, packet2, packet3]);

    // Transport packet2 away
    let input_port = PacketLocation::InputPort("B".to_string(), "in".to_string());
    let action = NetAction::TransportPacketToLocation(packet2, input_port.clone());
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Verify packet2 is gone from OutsideNet
    let after_move = net.get_packets_at_location(&PacketLocation::OutsideNet);
    assert_eq!(after_move, vec![packet1, packet3]);

    // Undo the transport
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Verify order is restored perfectly (packet2 back in its original position)
    let restored_order = net.get_packets_at_location(&PacketLocation::OutsideNet);
    assert_eq!(
        restored_order,
        vec![packet1, packet2, packet3],
        "Packet order should be perfectly restored after undo"
    );
}

#[test]
fn test_undo_cancel_epoch() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create packet and place at input port
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&input_port_loc)
        .unwrap()
        .insert(packet_id);

    // Create and start epoch
    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id)],
    };
    let epoch = get_created_epoch(&net.do_action(&NetAction::CreateEpoch("B".to_string(), salvo)));
    let epoch_id = epoch.id;
    net.do_action(&NetAction::StartEpoch(epoch_id));

    // Cancel epoch
    let action = NetAction::CancelEpoch(epoch_id);
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Verify epoch and packet are gone
    assert!(net.get_epoch(&epoch_id).is_none());
    assert!(net.get_packet(&packet_id).is_none());

    // Undo the cancel
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Verify epoch is restored
    assert!(net.get_epoch(&epoch_id).is_some());
    assert!(matches!(
        net.get_epoch(&epoch_id).unwrap().state,
        EpochState::Running
    ));

    // Verify packet is restored inside the epoch
    assert!(net.get_packet(&packet_id).is_some());
    assert_eq!(
        net.get_packet(&packet_id).unwrap().location,
        PacketLocation::Node(epoch_id)
    );
}

#[test]
fn test_undo_send_output_salvo() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create packet and place at input port of B
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&input_port_loc)
        .unwrap()
        .insert(packet_id);

    // Create and start epoch
    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id)],
    };
    let epoch = get_created_epoch(&net.do_action(&NetAction::CreateEpoch("B".to_string(), salvo)));
    let epoch_id = epoch.id;
    net.do_action(&NetAction::StartEpoch(epoch_id));

    // Load packet into output port
    net.do_action(&NetAction::LoadPacketIntoOutputPort(
        packet_id,
        "out".to_string(),
    ));

    let output_port_loc = PacketLocation::OutputPort(epoch_id, "out".to_string());
    assert_eq!(net.packet_count_at(&output_port_loc), 1);

    // Send output salvo
    let action = NetAction::SendOutputSalvo(epoch_id, "default".to_string());
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Verify packet is on edge
    let edge_loc = PacketLocation::Edge(Edge {
        source: PortRef {
            node_name: "B".to_string(),
            port_type: PortType::Output,
            port_name: "out".to_string(),
        },
        target: PortRef {
            node_name: "C".to_string(),
            port_type: PortType::Input,
            port_name: "in".to_string(),
        },
    });
    assert_eq!(net.packet_count_at(&edge_loc), 1);
    assert_eq!(net.get_epoch(&epoch_id).unwrap().out_salvos.len(), 1);

    // Undo the send
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Verify packet is back at output port
    assert_eq!(net.packet_count_at(&output_port_loc), 1);
    assert_eq!(net.packet_count_at(&edge_loc), 0);
    assert_eq!(net.get_epoch(&epoch_id).unwrap().out_salvos.len(), 0);
}

#[test]
fn test_undo_create_epoch() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create packet at input port
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&input_port_loc)
        .unwrap()
        .insert(packet_id);

    // Create epoch
    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id)],
    };
    let action = NetAction::CreateEpoch("B".to_string(), salvo);
    let (epoch, events) = match net.do_action(&action) {
        NetActionResponse::Success(NetActionResponseData::CreatedEpoch(e), events) => (e, events),
        _ => panic!("Expected CreatedEpoch response"),
    };

    let epoch_id = epoch.id;

    // Verify epoch exists and packet is inside it
    assert!(net.get_epoch(&epoch_id).is_some());
    assert_eq!(
        net.get_packet(&packet_id).unwrap().location,
        PacketLocation::Node(epoch_id)
    );

    // Undo the creation
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Verify epoch is gone
    assert!(net.get_epoch(&epoch_id).is_none());
    assert!(!net.get_startable_epochs().contains(&epoch_id));

    // Verify packet is back at input port
    assert_eq!(net.get_packet(&packet_id).unwrap().location, input_port_loc);
    assert_eq!(net.packet_count_at(&input_port_loc), 1);
}

// ========== Exact State Reproduction Tests ==========

#[test]
fn test_undo_create_packet_exact_state() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Capture state before
    let before = NetSimSnapshot::capture(&net);

    // Perform action
    let action = NetAction::CreatePacket(None);
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Undo
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Capture state after undo
    let after = NetSimSnapshot::capture(&net);

    // Compare
    let diffs = before.diff(&after);
    assert!(
        diffs.is_empty(),
        "State not exactly restored:\n{}",
        diffs.join("\n")
    );
}

#[test]
fn test_undo_transport_packet_exact_state() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Setup: create a packet
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    // Capture state before transport
    let before = NetSimSnapshot::capture(&net);

    // Transport packet
    let input_port = PacketLocation::InputPort("B".to_string(), "in".to_string());
    let action = NetAction::TransportPacketToLocation(packet_id, input_port);
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Undo
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Capture state after undo
    let after = NetSimSnapshot::capture(&net);

    // Compare
    let diffs = before.diff(&after);
    assert!(
        diffs.is_empty(),
        "State not exactly restored:\n{}",
        diffs.join("\n")
    );
}

#[test]
fn test_undo_run_step_exact_state() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Setup: place packet on edge
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let edge_location = PacketLocation::Edge(Edge {
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
    net._packets.get_mut(&packet_id).unwrap().location = edge_location.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&edge_location)
        .unwrap()
        .insert(packet_id);

    // Capture state before RunStep
    let before = NetSimSnapshot::capture(&net);

    // Run step
    let action = NetAction::RunStep;
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Undo
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Capture state after undo
    let after = NetSimSnapshot::capture(&net);

    // Compare
    let diffs = before.diff(&after);
    assert!(
        diffs.is_empty(),
        "State not exactly restored:\n{}",
        diffs.join("\n")
    );
}

#[test]
fn test_undo_create_epoch_exact_state() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Setup: place packet at input port
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&input_port_loc)
        .unwrap()
        .insert(packet_id);

    // Capture state before CreateEpoch
    let before = NetSimSnapshot::capture(&net);

    // Create epoch
    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id)],
    };
    let action = NetAction::CreateEpoch("B".to_string(), salvo);
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Undo
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Capture state after undo
    let after = NetSimSnapshot::capture(&net);

    // Compare
    let diffs = before.diff(&after);
    assert!(
        diffs.is_empty(),
        "State not exactly restored:\n{}",
        diffs.join("\n")
    );
}

#[test]
fn test_undo_start_epoch_exact_state() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Setup: create epoch
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&input_port_loc)
        .unwrap()
        .insert(packet_id);

    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id)],
    };
    let epoch = get_created_epoch(&net.do_action(&NetAction::CreateEpoch("B".to_string(), salvo)));

    // Capture state before StartEpoch
    let before = NetSimSnapshot::capture(&net);

    // Start epoch
    let action = NetAction::StartEpoch(epoch.id);
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Undo
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Capture state after undo
    let after = NetSimSnapshot::capture(&net);

    // Compare
    let diffs = before.diff(&after);
    assert!(
        diffs.is_empty(),
        "State not exactly restored:\n{}",
        diffs.join("\n")
    );
}

#[test]
fn test_undo_finish_epoch_exact_state() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Setup: create, start, and empty epoch
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&input_port_loc)
        .unwrap()
        .insert(packet_id);

    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id)],
    };
    let epoch = get_created_epoch(&net.do_action(&NetAction::CreateEpoch("B".to_string(), salvo)));
    net.do_action(&NetAction::StartEpoch(epoch.id));
    net.do_action(&NetAction::ConsumePacket(packet_id));

    // Capture state before FinishEpoch
    let before = NetSimSnapshot::capture(&net);

    // Finish epoch
    let action = NetAction::FinishEpoch(epoch.id);
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Undo
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Capture state after undo
    let after = NetSimSnapshot::capture(&net);

    // Compare
    let diffs = before.diff(&after);
    assert!(
        diffs.is_empty(),
        "State not exactly restored:\n{}",
        diffs.join("\n")
    );
}

#[test]
fn test_undo_cancel_epoch_exact_state() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Setup: create and start epoch
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&input_port_loc)
        .unwrap()
        .insert(packet_id);

    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id)],
    };
    let epoch = get_created_epoch(&net.do_action(&NetAction::CreateEpoch("B".to_string(), salvo)));
    net.do_action(&NetAction::StartEpoch(epoch.id));

    // Capture state before CancelEpoch
    let before = NetSimSnapshot::capture(&net);

    // Cancel epoch
    let action = NetAction::CancelEpoch(epoch.id);
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Undo
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Capture state after undo
    let after = NetSimSnapshot::capture(&net);

    // Compare
    let diffs = before.diff(&after);
    assert!(
        diffs.is_empty(),
        "State not exactly restored:\n{}",
        diffs.join("\n")
    );
}

#[test]
fn test_undo_consume_packet_exact_state() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Setup: create packet
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    // Capture state before ConsumePacket
    let before = NetSimSnapshot::capture(&net);

    // Consume packet
    let action = NetAction::ConsumePacket(packet_id);
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Undo
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Capture state after undo
    let after = NetSimSnapshot::capture(&net);

    // Compare
    let diffs = before.diff(&after);
    assert!(
        diffs.is_empty(),
        "State not exactly restored:\n{}",
        diffs.join("\n")
    );
}

#[test]
fn test_undo_send_output_salvo_exact_state() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Setup: create epoch with packet in output port
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&input_port_loc)
        .unwrap()
        .insert(packet_id);

    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id)],
    };
    let epoch = get_created_epoch(&net.do_action(&NetAction::CreateEpoch("B".to_string(), salvo)));
    net.do_action(&NetAction::StartEpoch(epoch.id));
    net.do_action(&NetAction::LoadPacketIntoOutputPort(
        packet_id,
        "out".to_string(),
    ));

    // Capture state before SendOutputSalvo
    let before = NetSimSnapshot::capture(&net);

    // Send output salvo
    let action = NetAction::SendOutputSalvo(epoch.id, "default".to_string());
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Undo
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Capture state after undo
    let after = NetSimSnapshot::capture(&net);

    // Compare
    let diffs = before.diff(&after);
    assert!(
        diffs.is_empty(),
        "State not exactly restored:\n{}",
        diffs.join("\n")
    );
}

#[test]
fn test_undo_load_packet_into_output_port_exact_state() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Setup: create epoch with packet
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&packet_id);
    net._packets_by_location
        .get_mut(&input_port_loc)
        .unwrap()
        .insert(packet_id);

    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id)],
    };
    let epoch = get_created_epoch(&net.do_action(&NetAction::CreateEpoch("B".to_string(), salvo)));
    net.do_action(&NetAction::StartEpoch(epoch.id));

    // Capture state before LoadPacketIntoOutputPort
    let before = NetSimSnapshot::capture(&net);

    // Load packet into output port
    let action = NetAction::LoadPacketIntoOutputPort(packet_id, "out".to_string());
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Undo
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Capture state after undo
    let after = NetSimSnapshot::capture(&net);

    // Compare
    let diffs = before.diff(&after);
    assert!(
        diffs.is_empty(),
        "State not exactly restored:\n{}",
        diffs.join("\n")
    );
}

#[test]
fn test_undo_multiple_packets_exact_ordering() {
    // Test that undoing preserves exact packet ordering with multiple packets
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create 5 packets
    let p1 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let p2 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let p3 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let p4 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let p5 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    // Capture state with 5 packets
    let before = NetSimSnapshot::capture(&net);

    // Move p3 (middle packet) to input port
    let input_port = PacketLocation::InputPort("B".to_string(), "in".to_string());
    let action = NetAction::TransportPacketToLocation(p3, input_port);
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Verify ordering after move: should be [p1, p2, p4, p5]
    let order_after_move = net.get_packets_at_location(&PacketLocation::OutsideNet);
    assert_eq!(order_after_move, vec![p1, p2, p4, p5]);

    // Undo
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Capture state after undo
    let after = NetSimSnapshot::capture(&net);

    // Verify exact ordering restored: should be [p1, p2, p3, p4, p5]
    let order_after_undo = net.get_packets_at_location(&PacketLocation::OutsideNet);
    assert_eq!(
        order_after_undo,
        vec![p1, p2, p3, p4, p5],
        "Exact packet ordering must be restored"
    );

    // Full state comparison
    let diffs = before.diff(&after);
    assert!(
        diffs.is_empty(),
        "State not exactly restored:\n{}",
        diffs.join("\n")
    );
}

#[test]
fn test_undo_complex_run_step_exact_state() {
    // Test undo of RunStep with multiple packets moving and multiple epochs created
    let graph = branching_graph(); // A -> B, A -> C
    let mut net = NetSim::new(graph);

    // Place packets on both edges from A
    let p1 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let p2 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    let edge_a_b = PacketLocation::Edge(Edge {
        source: PortRef {
            node_name: "A".to_string(),
            port_type: PortType::Output,
            port_name: "out1".to_string(),
        },
        target: PortRef {
            node_name: "B".to_string(),
            port_type: PortType::Input,
            port_name: "in".to_string(),
        },
    });
    let edge_a_c = PacketLocation::Edge(Edge {
        source: PortRef {
            node_name: "A".to_string(),
            port_type: PortType::Output,
            port_name: "out2".to_string(),
        },
        target: PortRef {
            node_name: "C".to_string(),
            port_type: PortType::Input,
            port_name: "in".to_string(),
        },
    });

    // Move p1 to edge A->B
    net._packets.get_mut(&p1).unwrap().location = edge_a_b.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&p1);
    net._packets_by_location
        .get_mut(&edge_a_b)
        .unwrap()
        .insert(p1);

    // Move p2 to edge A->C
    net._packets.get_mut(&p2).unwrap().location = edge_a_c.clone();
    net._packets_by_location
        .get_mut(&PacketLocation::OutsideNet)
        .unwrap()
        .shift_remove(&p2);
    net._packets_by_location
        .get_mut(&edge_a_c)
        .unwrap()
        .insert(p2);

    // Capture state before RunStep
    let before = NetSimSnapshot::capture(&net);

    // Run step - should move both packets and create 2 epochs
    let action = NetAction::RunStep;
    let events = match net.do_action(&action) {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success"),
    };

    // Verify 2 epochs were created
    assert_eq!(net.get_startable_epochs().len(), 2);

    // Undo
    net.undo_action(&action, &events)
        .expect("Undo should succeed");

    // Capture state after undo
    let after = NetSimSnapshot::capture(&net);

    // Verify no epochs exist
    assert_eq!(net.get_startable_epochs().len(), 0);

    // Full state comparison
    let diffs = before.diff(&after);
    assert!(
        diffs.is_empty(),
        "State not exactly restored:\n{}",
        diffs.join("\n")
    );
}
