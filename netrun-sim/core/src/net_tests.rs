use super::*;
use crate::test_fixtures::*;

// Helper to extract packet ID from create response
fn get_packet_id(response: &NetActionResponse) -> PacketID {
    match response {
        NetActionResponse::Success(NetActionResponseData::Packet(id), _) => id.clone(),
        _ => panic!("Expected Packet response, got: {:?}", response),
    }
}

// Helper to extract epoch from start response
fn get_started_epoch(response: &NetActionResponse) -> Epoch {
    match response {
        NetActionResponse::Success(NetActionResponseData::StartedEpoch(epoch), _) => {
            epoch.clone()
        }
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
    net.do_action(&NetAction::RunNetUntilBlocked);

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

    // Create epoch with packet via create_and_start_epoch
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
    let epoch = get_started_epoch(
        &net.do_action(&NetAction::CreateAndStartEpoch("B".to_string(), salvo)),
    );

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
    let epoch = get_started_epoch(
        &net.do_action(&NetAction::CreateAndStartEpoch("B".to_string(), salvo)),
    );

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
    let epoch = get_started_epoch(
        &net.do_action(&NetAction::CreateAndStartEpoch("B".to_string(), salvo)),
    );

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
    net.do_action(&NetAction::RunNetUntilBlocked);

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
    net.do_action(&NetAction::RunNetUntilBlocked);

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
    let response = net.do_action(&NetAction::RunNetUntilBlocked);

    // Extract PacketMoved events to verify FIFO order
    let events = match response {
        NetActionResponse::Success(_, events) => events,
        _ => panic!("Expected success response"),
    };

    // Find the first PacketMoved event for each packet (when it moved from edge to input port)
    // The order of these events should match the FIFO order: packet1, packet2, packet3
    let packet_move_order: Vec<PacketID> = events
        .iter()
        .filter_map(|event| {
            if let NetEvent::PacketMoved(_, packet_id, PacketLocation::InputPort(_, _)) = event
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
    let epoch = get_started_epoch(
        &net.do_action(&NetAction::CreateAndStartEpoch("B".to_string(), salvo)),
    );

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
    let epoch = get_started_epoch(
        &net.do_action(&NetAction::CreateAndStartEpoch("B".to_string(), salvo)),
    );

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

// ========== Create And Start Epoch Tests ==========

#[test]
fn test_create_and_start_epoch() {
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

    // Create and start epoch manually
    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id.clone())],
    };
    let epoch = get_started_epoch(
        &net.do_action(&NetAction::CreateAndStartEpoch("B".to_string(), salvo)),
    );

    assert!(matches!(epoch.state, EpochState::Running));
    assert_eq!(epoch.node_name, "B");
}

#[test]
fn test_create_and_start_epoch_validates_node() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![],
    };
    let response = net.do_action(&NetAction::CreateAndStartEpoch(
        "NonExistent".to_string(),
        salvo,
    ));
    assert!(matches!(
        response,
        NetActionResponse::Error(NetActionError::NodeNotFound { .. })
    ));
}

#[test]
fn test_create_and_start_epoch_validates_packet_location() {
    let graph = linear_graph_3();
    let mut net = NetSim::new(graph);

    // Create packet but leave it OutsideNet
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id)],
    };
    let response = net.do_action(&NetAction::CreateAndStartEpoch("B".to_string(), salvo));
    assert!(matches!(
        response,
        NetActionResponse::Error(NetActionError::PacketNotAtInputPort { .. })
    ));
}
