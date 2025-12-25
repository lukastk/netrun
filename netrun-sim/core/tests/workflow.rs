//! End-to-end workflow tests that verify complete packet flows through graphs.

mod common;

use netrun_sim::graph::{Edge, PortRef, PortType};
use netrun_sim::net::{
    Epoch, EpochState, Net, NetAction, NetActionResponse, NetActionResponseData,
    NetEvent, PacketLocation, Salvo,
};

// ========== Helper Functions ==========

fn get_packet_id(response: &NetActionResponse) -> netrun_sim::net::PacketID {
    match response {
        NetActionResponse::Success(NetActionResponseData::Packet(id), _) => id.clone(),
        _ => panic!("Expected Packet response, got: {:?}", response),
    }
}

fn get_started_epoch(response: &NetActionResponse) -> Epoch {
    match response {
        NetActionResponse::Success(NetActionResponseData::StartedEpoch(epoch), _) => epoch.clone(),
        _ => panic!("Expected StartedEpoch response, got: {:?}", response),
    }
}

fn get_finished_epoch(response: &NetActionResponse) -> Epoch {
    match response {
        NetActionResponse::Success(NetActionResponseData::FinishedEpoch(epoch), _) => epoch.clone(),
        _ => panic!("Expected FinishedEpoch response, got: {:?}", response),
    }
}

fn get_events(response: &NetActionResponse) -> Vec<NetEvent> {
    match response {
        NetActionResponse::Success(_, events) => events.clone(),
        _ => panic!("Expected Success response, got: {:?}", response),
    }
}

fn make_edge(source_node: &str, source_port: &str, target_node: &str, target_port: &str) -> Edge {
    Edge {
        source: PortRef {
            node_name: source_node.to_string(),
            port_type: PortType::Output,
            port_name: source_port.to_string(),
        },
        target: PortRef {
            node_name: target_node.to_string(),
            port_type: PortType::Input,
            port_name: target_port.to_string(),
        },
    }
}

// ========== Linear Flow Workflow Tests ==========

#[test]
fn test_complete_linear_flow_a_to_b() {
    // Test a complete flow: create packet -> place on edge -> run -> start epoch -> process -> finish
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    // 1. Create a packet
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    // 2. Transport packet to edge A->B
    let edge_a_b = PacketLocation::Edge(make_edge("A", "out", "B", "in"));
    net.do_action(&NetAction::TransportPacketToLocation(packet_id.clone(), edge_a_b));

    // 3. Run until blocked - packet should move to input port and trigger epoch
    let response = net.do_action(&NetAction::RunNetUntilBlocked);
    let events = get_events(&response);

    // Should have events: PacketMoved (to input port), PacketMoved (to node), EpochCreated, InputSalvoTriggered
    assert!(events.iter().any(|e| matches!(e, NetEvent::EpochCreated(_, _))));

    // 4. Find the startable epoch using public API
    let epoch_ids = net.get_startable_epochs();
    assert_eq!(epoch_ids.len(), 1);

    // 5. Start the epoch
    let epoch = get_started_epoch(&net.do_action(&NetAction::StartEpoch(epoch_ids[0].clone())));
    assert!(matches!(epoch.state, EpochState::Running));
    assert_eq!(epoch.node_name, "B");

    // 6. Consume the packet (simulating node processing)
    net.do_action(&NetAction::ConsumePacket(packet_id));

    // 7. Finish the epoch
    let finished = get_finished_epoch(&net.do_action(&NetAction::FinishEpoch(epoch_ids[0].clone())));
    assert!(matches!(finished.state, EpochState::Finished));
}

#[test]
fn test_linear_flow_with_output_salvo() {
    // Test: packet enters B, B produces output, packet moves to C
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    // 1. Create packet and transport to B's input port
    let input_packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net.do_action(&NetAction::TransportPacketToLocation(input_packet_id.clone(), input_port_loc));

    // 2. Manually create and start epoch at B
    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), input_packet_id.clone())],
    };
    let epoch = get_started_epoch(&net.do_action(&NetAction::CreateAndStartEpoch(
        "B".to_string(),
        salvo,
    )));

    // 3. Consume input packet
    net.do_action(&NetAction::ConsumePacket(input_packet_id));

    // 4. Create output packet inside the epoch
    let output_packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(Some(epoch.id.clone()))));

    // 5. Load output packet into output port
    net.do_action(&NetAction::LoadPacketIntoOutputPort(
        output_packet_id.clone(),
        "out".to_string(),
    ));

    // 6. Send output salvo
    let response = net.do_action(&NetAction::SendOutputSalvo(epoch.id.clone(), "default".to_string()));
    assert!(matches!(response, NetActionResponse::Success(_, _)));

    // 7. Verify packet is now on edge B->C using public API
    let edge_b_c = PacketLocation::Edge(make_edge("B", "out", "C", "in"));
    assert_eq!(net.packet_count_at(&edge_b_c), 1);

    // 8. Finish the epoch
    net.do_action(&NetAction::FinishEpoch(epoch.id));
}

// ========== Branching Flow Workflow Tests ==========

#[test]
fn test_branching_flow() {
    // Test: A produces two outputs, one goes to B, one goes to C
    let graph = common::branching_graph();
    let mut net = Net::new(graph);

    // Place packets on the edges
    let packet1 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let packet2 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    let edge_a_b = PacketLocation::Edge(make_edge("A", "out1", "B", "in"));
    let edge_a_c = PacketLocation::Edge(make_edge("A", "out2", "C", "in"));

    net.do_action(&NetAction::TransportPacketToLocation(packet1.clone(), edge_a_b));
    net.do_action(&NetAction::TransportPacketToLocation(packet2.clone(), edge_a_c));

    // Run until blocked
    net.do_action(&NetAction::RunNetUntilBlocked);

    // Both B and C should have startable epochs
    let epoch_ids = net.get_startable_epochs();
    assert_eq!(epoch_ids.len(), 2);

    // Verify epochs are on correct nodes
    let epoch_nodes: Vec<_> = epoch_ids
        .iter()
        .map(|id| net.get_epoch(id).unwrap().node_name.clone())
        .collect();
    assert!(epoch_nodes.contains(&"B".to_string()));
    assert!(epoch_nodes.contains(&"C".to_string()));
}

// ========== Merging Flow Workflow Tests ==========

#[test]
fn test_merging_flow_both_inputs_required() {
    // Test: C requires inputs from both A and B
    let graph = common::merging_graph();
    let mut net = Net::new(graph);

    // Place packet only on edge A->C
    let packet1 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let edge_a_c = PacketLocation::Edge(make_edge("A", "out", "C", "in1"));
    net.do_action(&NetAction::TransportPacketToLocation(packet1.clone(), edge_a_c));

    // Run until blocked
    net.do_action(&NetAction::RunNetUntilBlocked);

    // C should NOT have a startable epoch (needs both in1 and in2)
    // The default salvo condition is AND of all input ports
    assert_eq!(net.get_startable_epochs().len(), 0);

    // Now add packet from B
    let packet2 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let edge_b_c = PacketLocation::Edge(make_edge("B", "out", "C", "in2"));
    net.do_action(&NetAction::TransportPacketToLocation(packet2.clone(), edge_b_c));

    // Run again
    net.do_action(&NetAction::RunNetUntilBlocked);

    // Now C should have a startable epoch (both inputs present)
    let epoch_ids = net.get_startable_epochs();
    assert_eq!(epoch_ids.len(), 1);
    assert_eq!(net.get_epoch(&epoch_ids[0]).unwrap().node_name, "C");
}

// ========== Diamond Flow Workflow Tests ==========

#[test]
fn test_diamond_flow_synchronization() {
    // Test: A -> B -> D and A -> C -> D
    // D should wait for both B and C to complete
    let graph = common::diamond_graph();
    let mut net = Net::new(graph);

    // Place packets on edges from A
    let packet1 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let packet2 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    let edge_a_b = PacketLocation::Edge(make_edge("A", "out1", "B", "in"));
    let edge_a_c = PacketLocation::Edge(make_edge("A", "out2", "C", "in"));

    net.do_action(&NetAction::TransportPacketToLocation(packet1.clone(), edge_a_b));
    net.do_action(&NetAction::TransportPacketToLocation(packet2.clone(), edge_a_c));

    // Run until blocked
    net.do_action(&NetAction::RunNetUntilBlocked);

    // B and C should have startable epochs, but not D
    let epoch_ids = net.get_startable_epochs();
    assert_eq!(epoch_ids.len(), 2);

    let epoch_nodes: Vec<_> = epoch_ids
        .iter()
        .map(|id| net.get_epoch(id).unwrap().node_name.clone())
        .collect();
    assert!(epoch_nodes.contains(&"B".to_string()));
    assert!(epoch_nodes.contains(&"C".to_string()));
    assert!(!epoch_nodes.contains(&"D".to_string()));
}

// ========== Cancel Epoch Workflow Tests ==========

#[test]
fn test_cancel_epoch_workflow() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    // Create and transport packet
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net.do_action(&NetAction::TransportPacketToLocation(packet_id.clone(), input_port_loc));

    // Create and start epoch
    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id.clone())],
    };
    let epoch = get_started_epoch(&net.do_action(&NetAction::CreateAndStartEpoch(
        "B".to_string(),
        salvo,
    )));

    // Cancel the epoch
    let response = net.do_action(&NetAction::CancelEpoch(epoch.id));

    match response {
        NetActionResponse::Success(NetActionResponseData::CancelledEpoch(cancelled, destroyed), events) => {
            assert_eq!(cancelled.id, epoch.id);
            assert_eq!(destroyed.len(), 1);
            assert_eq!(destroyed[0], packet_id);

            // Should have events for packet consumption and epoch cancellation
            assert!(events.iter().any(|e| matches!(e, NetEvent::PacketConsumed(_, _))));
            assert!(events.iter().any(|e| matches!(e, NetEvent::EpochCancelled(_, _))));
        }
        _ => panic!("Expected CancelledEpoch response"),
    }

    // Packet should be gone - verify via get_packet
    assert!(net.get_packet(&packet_id).is_none());
}

// ========== Multiple Epochs Workflow Tests ==========

#[test]
fn test_multiple_sequential_epochs_on_same_node() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    // Process first packet through B
    let packet1 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net.do_action(&NetAction::TransportPacketToLocation(packet1.clone(), input_port_loc.clone()));

    let salvo1 = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet1.clone())],
    };
    let epoch1 = get_started_epoch(&net.do_action(&NetAction::CreateAndStartEpoch(
        "B".to_string(),
        salvo1,
    )));

    net.do_action(&NetAction::ConsumePacket(packet1));
    net.do_action(&NetAction::FinishEpoch(epoch1.id));

    // Process second packet through B
    let packet2 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    net.do_action(&NetAction::TransportPacketToLocation(packet2.clone(), input_port_loc));

    let salvo2 = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet2.clone())],
    };
    let epoch2 = get_started_epoch(&net.do_action(&NetAction::CreateAndStartEpoch(
        "B".to_string(),
        salvo2,
    )));

    // Second epoch should be on same node
    assert_eq!(epoch2.node_name, "B");
    assert_ne!(epoch1.id, epoch2.id);

    net.do_action(&NetAction::ConsumePacket(packet2));
    net.do_action(&NetAction::FinishEpoch(epoch2.id));
}

// ========== Packet Location Verification Tests ==========

#[test]
fn test_packet_location_tracking() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    // Create packet - should be at OutsideNet
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let packet = net.get_packet(&packet_id).unwrap();
    assert_eq!(packet.location, PacketLocation::OutsideNet);

    // Transport to edge
    let edge_loc = PacketLocation::Edge(make_edge("A", "out", "B", "in"));
    net.do_action(&NetAction::TransportPacketToLocation(packet_id.clone(), edge_loc.clone()));

    // Verify location updated
    let packet = net.get_packet(&packet_id).unwrap();
    assert_eq!(packet.location, edge_loc);

    // Verify packet count at location
    assert_eq!(net.packet_count_at(&edge_loc), 1);
    assert_eq!(net.packet_count_at(&PacketLocation::OutsideNet), 0);
}

#[test]
fn test_get_packets_at_location() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    // Create multiple packets
    let packet1 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let packet2 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    // Both should be at OutsideNet
    let outside_packets = net.get_packets_at_location(&PacketLocation::OutsideNet);
    assert_eq!(outside_packets.len(), 2);
    assert!(outside_packets.contains(&packet1));
    assert!(outside_packets.contains(&packet2));

    // Move one to edge
    let edge_loc = PacketLocation::Edge(make_edge("A", "out", "B", "in"));
    net.do_action(&NetAction::TransportPacketToLocation(packet1.clone(), edge_loc.clone()));

    // Verify locations
    let outside_packets = net.get_packets_at_location(&PacketLocation::OutsideNet);
    assert_eq!(outside_packets.len(), 1);
    assert!(outside_packets.contains(&packet2));

    let edge_packets = net.get_packets_at_location(&edge_loc);
    assert_eq!(edge_packets.len(), 1);
    assert!(edge_packets.contains(&packet1));
}
