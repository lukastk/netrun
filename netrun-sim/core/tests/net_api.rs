//! Integration tests for the Net public API.

mod common;

use netrun_sim::graph::{EdgeRef, PortRef, PortType};
use netrun_sim::net::{
    Epoch, Net, NetAction, NetActionError, NetActionResponse,
    NetActionResponseData, NetEvent, PacketLocation, Salvo,
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

fn get_events(response: &NetActionResponse) -> Vec<NetEvent> {
    match response {
        NetActionResponse::Success(_, events) => events.clone(),
        _ => panic!("Expected Success response, got: {:?}", response),
    }
}

// ========== Net Construction Tests ==========

#[test]
fn test_net_new_with_valid_graph() {
    let graph = common::linear_graph_3();
    let net = Net::new(graph);

    // Net should be created successfully
    assert!(net.graph.nodes().contains_key("A"));
    assert!(net.graph.nodes().contains_key("B"));
    assert!(net.graph.nodes().contains_key("C"));
}

#[test]
fn test_net_new_with_empty_graph() {
    use netrun_sim::graph::Graph;
    let graph = Graph::new(vec![], vec![]);
    let _net = Net::new(graph);
    // Should not panic
}

// ========== Packet Creation Tests ==========

#[test]
fn test_create_packet_outside_net() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let response = net.do_action(&NetAction::CreatePacket(None));

    assert!(matches!(
        response,
        NetActionResponse::Success(NetActionResponseData::Packet(_), _)
    ));

    // Check events
    let events = get_events(&response);
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], NetEvent::PacketCreated(_, _)));
}

#[test]
fn test_create_packet_location_is_outside_net() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    // Verify packet is at OutsideNet
    let packet = net.get_packet(&packet_id).unwrap();
    assert_eq!(packet.location, PacketLocation::OutsideNet);
}

// ========== Packet Consumption Tests ==========

#[test]
fn test_consume_packet() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    let response = net.do_action(&NetAction::ConsumePacket(packet_id));

    assert!(matches!(
        response,
        NetActionResponse::Success(NetActionResponseData::None, _)
    ));
}

#[test]
fn test_consume_nonexistent_packet() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let fake_id = ulid::Ulid::new();
    let response = net.do_action(&NetAction::ConsumePacket(fake_id));

    assert!(matches!(
        response,
        NetActionResponse::Error(NetActionError::PacketNotFound { .. })
    ));
}

#[test]
fn test_packet_not_found_error_contains_id() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let fake_id = ulid::Ulid::new();
    let response = net.do_action(&NetAction::ConsumePacket(fake_id));

    match response {
        NetActionResponse::Error(NetActionError::PacketNotFound { packet_id }) => {
            assert_eq!(packet_id, fake_id);
        }
        _ => panic!("Expected PacketNotFound error"),
    }
}

// ========== Epoch Tests ==========

#[test]
fn test_start_nonexistent_epoch() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let fake_id = ulid::Ulid::new();
    let response = net.do_action(&NetAction::StartEpoch(fake_id));

    assert!(matches!(
        response,
        NetActionResponse::Error(NetActionError::EpochNotFound { .. })
    ));
}

#[test]
fn test_finish_nonexistent_epoch() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let fake_id = ulid::Ulid::new();
    let response = net.do_action(&NetAction::FinishEpoch(fake_id));

    assert!(matches!(
        response,
        NetActionResponse::Error(NetActionError::EpochNotFound { .. })
    ));
}

#[test]
fn test_cancel_nonexistent_epoch() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let fake_id = ulid::Ulid::new();
    let response = net.do_action(&NetAction::CancelEpoch(fake_id));

    assert!(matches!(
        response,
        NetActionResponse::Error(NetActionError::EpochNotFound { .. })
    ));
}

// ========== Create And Start Epoch Tests ==========

#[test]
fn test_create_and_start_epoch_with_invalid_node() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

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
fn test_create_and_start_epoch_node_not_found_error_contains_name() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![],
    };

    let response = net.do_action(&NetAction::CreateAndStartEpoch(
        "MissingNode".to_string(),
        salvo,
    ));

    match response {
        NetActionResponse::Error(NetActionError::NodeNotFound { node_name }) => {
            assert_eq!(node_name, "MissingNode");
        }
        _ => panic!("Expected NodeNotFound error"),
    }
}

// ========== Run Until Blocked Tests ==========

#[test]
fn test_run_until_blocked_on_empty_net() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let response = net.do_action(&NetAction::RunNetUntilBlocked);

    // Should succeed with no events (nothing to do)
    assert!(matches!(
        response,
        NetActionResponse::Success(NetActionResponseData::None, _)
    ));

    let events = get_events(&response);
    assert!(events.is_empty());
}

// ========== Error Display Tests ==========

#[test]
fn test_net_action_error_display() {
    let error = NetActionError::PacketNotFound {
        packet_id: ulid::Ulid::new(),
    };

    // Test that Display is implemented (from thiserror)
    let msg = format!("{}", error);
    assert!(msg.contains("packet not found"));
}

#[test]
fn test_epoch_not_found_error_display() {
    let epoch_id = ulid::Ulid::new();
    let error = NetActionError::EpochNotFound { epoch_id };

    let msg = format!("{}", error);
    assert!(msg.contains("epoch not found"));
    assert!(msg.contains(&epoch_id.to_string()));
}

// ========== NetEvent Tests ==========

#[test]
fn test_packet_created_event_structure() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let response = net.do_action(&NetAction::CreatePacket(None));
    let events = get_events(&response);

    assert_eq!(events.len(), 1);
    match &events[0] {
        NetEvent::PacketCreated(timestamp, packet_id) => {
            assert!(*timestamp > 0);
            assert!(!packet_id.is_nil());
        }
        _ => panic!("Expected PacketCreated event"),
    }
}

#[test]
fn test_packet_consumed_event_structure() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let response = net.do_action(&NetAction::ConsumePacket(packet_id.clone()));
    let events = get_events(&response);

    assert_eq!(events.len(), 1);
    match &events[0] {
        NetEvent::PacketConsumed(timestamp, consumed_id) => {
            assert!(*timestamp > 0);
            assert_eq!(*consumed_id, packet_id);
        }
        _ => panic!("Expected PacketConsumed event"),
    }
}

// ========== NetActionResponse Pattern Matching Tests ==========

#[test]
fn test_response_pattern_matching_success() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let response = net.do_action(&NetAction::CreatePacket(None));

    // Test that we can pattern match on responses
    let packet_id = match response {
        NetActionResponse::Success(NetActionResponseData::Packet(id), events) => {
            assert!(!events.is_empty());
            id
        }
        NetActionResponse::Success(_, _) => panic!("Wrong response data type"),
        NetActionResponse::Error(e) => panic!("Unexpected error: {}", e),
    };

    assert!(!packet_id.is_nil());
}

#[test]
fn test_response_pattern_matching_error() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let response = net.do_action(&NetAction::ConsumePacket(ulid::Ulid::new()));

    match response {
        NetActionResponse::Error(NetActionError::PacketNotFound { packet_id }) => {
            // Successfully matched the specific error variant with data
            assert!(!packet_id.is_nil());
        }
        NetActionResponse::Error(e) => panic!("Wrong error type: {}", e),
        NetActionResponse::Success(_, _) => panic!("Expected error"),
    }
}

// ========== Public Accessor Tests ==========

#[test]
fn test_get_packet() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    // Should find the packet
    let packet = net.get_packet(&packet_id);
    assert!(packet.is_some());
    assert_eq!(packet.unwrap().id, packet_id);

    // Should not find non-existent packet
    let fake_id = ulid::Ulid::new();
    assert!(net.get_packet(&fake_id).is_none());
}

#[test]
fn test_get_epoch() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    // Create packet and transport to input port
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
    net.do_action(&NetAction::TransportPacketToLocation(packet_id.clone(), input_port_loc));

    // Create and start epoch
    let salvo = Salvo {
        salvo_condition: "manual".to_string(),
        packets: vec![("in".to_string(), packet_id)],
    };
    let epoch = get_started_epoch(&net.do_action(&NetAction::CreateAndStartEpoch(
        "B".to_string(),
        salvo,
    )));

    // Should find the epoch
    let found_epoch = net.get_epoch(&epoch.id);
    assert!(found_epoch.is_some());
    assert_eq!(found_epoch.unwrap().node_name, "B");

    // Should not find non-existent epoch
    let fake_id = ulid::Ulid::new();
    assert!(net.get_epoch(&fake_id).is_none());
}

#[test]
fn test_get_startable_epochs() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    // Initially no startable epochs
    assert!(net.get_startable_epochs().is_empty());

    // Create packet and transport to edge
    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let edge_loc = PacketLocation::Edge(EdgeRef {
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
    net.do_action(&NetAction::TransportPacketToLocation(packet_id.clone(), edge_loc));

    // Run until blocked
    net.do_action(&NetAction::RunNetUntilBlocked);

    // Should now have a startable epoch
    let startable = net.get_startable_epochs();
    assert_eq!(startable.len(), 1);
}

#[test]
fn test_packet_count_at() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    // Create some packets
    let _p1 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let _p2 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    // Should be 2 packets at OutsideNet
    assert_eq!(net.packet_count_at(&PacketLocation::OutsideNet), 2);

    // Should be 0 at input port
    let input_port = PacketLocation::InputPort("B".to_string(), "in".to_string());
    assert_eq!(net.packet_count_at(&input_port), 0);
}

#[test]
fn test_get_packets_at_location() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let p1 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
    let p2 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    let packets = net.get_packets_at_location(&PacketLocation::OutsideNet);
    assert_eq!(packets.len(), 2);
    assert!(packets.contains(&p1));
    assert!(packets.contains(&p2));
}

#[test]
fn test_transport_packet_to_location() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

    // Transport to input port
    let input_port = PacketLocation::InputPort("B".to_string(), "in".to_string());
    let result = net.do_action(&NetAction::TransportPacketToLocation(packet_id.clone(), input_port.clone()));
    assert!(matches!(result, NetActionResponse::Success(_, _)));

    // Verify packet is at new location
    let packet = net.get_packet(&packet_id).unwrap();
    assert_eq!(packet.location, input_port);
    assert_eq!(net.packet_count_at(&input_port), 1);
    assert_eq!(net.packet_count_at(&PacketLocation::OutsideNet), 0);
}

#[test]
fn test_transport_packet_to_location_fails_for_nonexistent_packet() {
    let graph = common::linear_graph_3();
    let mut net = Net::new(graph);

    let fake_id = ulid::Ulid::new();
    let input_port = PacketLocation::InputPort("B".to_string(), "in".to_string());

    let result = net.do_action(&NetAction::TransportPacketToLocation(fake_id, input_port));
    assert!(matches!(result, NetActionResponse::Error(NetActionError::PacketNotFound { .. })));
}
