//! Integration tests for the Graph public API.

mod common;

use netrun_sim::graph::{
    Graph, GraphValidationError, Node, Port, PortRef,
    PortSlotSpec, PortType, SalvoConditionTerm, PortState,
    evaluate_salvo_condition,
};
use std::collections::HashMap;

// ========== Graph Construction Tests ==========

#[test]
fn test_graph_new_creates_valid_graph() {
    let graph = common::linear_graph_3();

    // Should have 3 nodes
    assert_eq!(graph.nodes().len(), 3);
    assert!(graph.nodes().contains_key("A"));
    assert!(graph.nodes().contains_key("B"));
    assert!(graph.nodes().contains_key("C"));

    // Should have 2 edges
    assert_eq!(graph.edges().len(), 2);
}

#[test]
fn test_graph_new_with_empty_nodes_and_edges() {
    let graph = Graph::new(vec![], vec![]);

    assert_eq!(graph.nodes().len(), 0);
    assert_eq!(graph.edges().len(), 0);
    assert!(graph.validate().is_empty());
}

#[test]
fn test_graph_accessors() {
    let graph = common::linear_graph_3();

    // Test nodes() accessor
    let nodes = graph.nodes();
    assert!(nodes.get("A").is_some());
    assert!(nodes.get("B").is_some());
    assert!(nodes.get("C").is_some());
    assert!(nodes.get("NonExistent").is_none());

    // Test edges() accessor
    let edges = graph.edges();
    assert_eq!(edges.len(), 2);
}

#[test]
fn test_graph_get_edge_by_tail() {
    let graph = common::linear_graph_3();

    // Edge from A.out should exist
    let port_ref = PortRef {
        node_name: "A".to_string(),
        port_type: PortType::Output,
        port_name: "out".to_string(),
    };
    let edge = graph.get_edge_by_tail(&port_ref);
    assert!(edge.is_some());
    assert_eq!(edge.unwrap().target.node_name, "B");

    // Non-existent edge
    let port_ref_none = PortRef {
        node_name: "C".to_string(),
        port_type: PortType::Output,
        port_name: "out".to_string(),
    };
    assert!(graph.get_edge_by_tail(&port_ref_none).is_none());
}

#[test]
fn test_graph_get_edge_by_head() {
    let graph = common::linear_graph_3();

    // Edge to B.in should exist
    let port_ref = PortRef {
        node_name: "B".to_string(),
        port_type: PortType::Input,
        port_name: "in".to_string(),
    };
    let edge = graph.get_edge_by_head(&port_ref);
    assert!(edge.is_some());
    assert_eq!(edge.unwrap().source.node_name, "A");

    // Non-existent edge (A has no input ports)
    let port_ref_none = PortRef {
        node_name: "A".to_string(),
        port_type: PortType::Input,
        port_name: "in".to_string(),
    };
    assert!(graph.get_edge_by_head(&port_ref_none).is_none());
}

// ========== Graph Validation Tests ==========

#[test]
fn test_valid_graphs_pass_validation() {
    assert!(common::linear_graph_3().validate().is_empty());
    assert!(common::branching_graph().validate().is_empty());
    assert!(common::merging_graph().validate().is_empty());
    assert!(common::diamond_graph().validate().is_empty());
}

#[test]
fn test_validation_catches_nonexistent_node() {
    let nodes = vec![common::simple_node("A", vec![], vec!["out"])];
    let edges = vec![common::edge("A", "out", "NonExistent", "in")];
    let graph = Graph::new(nodes, edges);

    let errors = graph.validate();
    assert!(!errors.is_empty());
    assert!(matches!(
        errors[0],
        GraphValidationError::EdgeReferencesNonexistentNode { .. }
    ));
}

#[test]
fn test_validation_catches_nonexistent_port() {
    let nodes = vec![
        common::simple_node("A", vec![], vec!["out"]),
        common::simple_node("B", vec!["in"], vec![]),
    ];
    let edges = vec![common::edge("A", "nonexistent", "B", "in")];
    let graph = Graph::new(nodes, edges);

    let errors = graph.validate();
    assert!(!errors.is_empty());
    assert!(matches!(
        errors[0],
        GraphValidationError::EdgeReferencesNonexistentPort { .. }
    ));
}

#[test]
fn test_validation_error_display() {
    let nodes = vec![common::simple_node("A", vec![], vec!["out"])];
    let edges = vec![common::edge("A", "out", "Missing", "in")];
    let graph = Graph::new(nodes, edges);

    let errors = graph.validate();
    assert!(!errors.is_empty());

    // Test that Display is implemented (from thiserror)
    let error_msg = format!("{}", errors[0]);
    assert!(error_msg.contains("Missing"));
}

// ========== Salvo Condition Evaluation Tests ==========

#[test]
fn test_evaluate_salvo_condition_non_empty() {
    let mut ports = HashMap::new();
    ports.insert("in".to_string(), Port { slots_spec: PortSlotSpec::Infinite });

    let condition = SalvoConditionTerm::Port {
        port_name: "in".to_string(),
        state: PortState::NonEmpty,
    };

    // Port has 1 packet - should be non-empty
    let mut counts = HashMap::new();
    counts.insert("in".to_string(), 1);
    assert!(evaluate_salvo_condition(&condition, &counts, &ports));

    // Port has 0 packets - should not be non-empty
    counts.insert("in".to_string(), 0);
    assert!(!evaluate_salvo_condition(&condition, &counts, &ports));
}

#[test]
fn test_evaluate_salvo_condition_empty() {
    let mut ports = HashMap::new();
    ports.insert("in".to_string(), Port { slots_spec: PortSlotSpec::Infinite });

    let condition = SalvoConditionTerm::Port {
        port_name: "in".to_string(),
        state: PortState::Empty,
    };

    let mut counts = HashMap::new();
    counts.insert("in".to_string(), 0);
    assert!(evaluate_salvo_condition(&condition, &counts, &ports));

    counts.insert("in".to_string(), 1);
    assert!(!evaluate_salvo_condition(&condition, &counts, &ports));
}

#[test]
fn test_evaluate_salvo_condition_full() {
    let mut ports = HashMap::new();
    ports.insert("in".to_string(), Port { slots_spec: PortSlotSpec::Finite(3) });

    let condition = SalvoConditionTerm::Port {
        port_name: "in".to_string(),
        state: PortState::Full,
    };

    let mut counts = HashMap::new();

    // Port has 3/3 packets - should be full
    counts.insert("in".to_string(), 3);
    assert!(evaluate_salvo_condition(&condition, &counts, &ports));

    // Port has 2/3 packets - should not be full
    counts.insert("in".to_string(), 2);
    assert!(!evaluate_salvo_condition(&condition, &counts, &ports));
}

#[test]
fn test_evaluate_salvo_condition_and() {
    let mut ports = HashMap::new();
    ports.insert("in1".to_string(), Port { slots_spec: PortSlotSpec::Infinite });
    ports.insert("in2".to_string(), Port { slots_spec: PortSlotSpec::Infinite });

    let condition = SalvoConditionTerm::And(vec![
        SalvoConditionTerm::Port {
            port_name: "in1".to_string(),
            state: PortState::NonEmpty,
        },
        SalvoConditionTerm::Port {
            port_name: "in2".to_string(),
            state: PortState::NonEmpty,
        },
    ]);

    let mut counts = HashMap::new();

    // Both non-empty
    counts.insert("in1".to_string(), 1);
    counts.insert("in2".to_string(), 1);
    assert!(evaluate_salvo_condition(&condition, &counts, &ports));

    // Only one non-empty
    counts.insert("in2".to_string(), 0);
    assert!(!evaluate_salvo_condition(&condition, &counts, &ports));
}

#[test]
fn test_evaluate_salvo_condition_or() {
    let mut ports = HashMap::new();
    ports.insert("in1".to_string(), Port { slots_spec: PortSlotSpec::Infinite });
    ports.insert("in2".to_string(), Port { slots_spec: PortSlotSpec::Infinite });

    let condition = SalvoConditionTerm::Or(vec![
        SalvoConditionTerm::Port {
            port_name: "in1".to_string(),
            state: PortState::NonEmpty,
        },
        SalvoConditionTerm::Port {
            port_name: "in2".to_string(),
            state: PortState::NonEmpty,
        },
    ]);

    let mut counts = HashMap::new();

    // Only one non-empty - OR should pass
    counts.insert("in1".to_string(), 1);
    counts.insert("in2".to_string(), 0);
    assert!(evaluate_salvo_condition(&condition, &counts, &ports));

    // Both empty
    counts.insert("in1".to_string(), 0);
    assert!(!evaluate_salvo_condition(&condition, &counts, &ports));
}

#[test]
fn test_evaluate_salvo_condition_not() {
    let mut ports = HashMap::new();
    ports.insert("in".to_string(), Port { slots_spec: PortSlotSpec::Infinite });

    let condition = SalvoConditionTerm::Not(Box::new(SalvoConditionTerm::Port {
        port_name: "in".to_string(),
        state: PortState::Empty,
    }));

    let mut counts = HashMap::new();

    // Port is empty, NOT(empty) = false
    counts.insert("in".to_string(), 0);
    assert!(!evaluate_salvo_condition(&condition, &counts, &ports));

    // Port is not empty, NOT(empty) = true
    counts.insert("in".to_string(), 1);
    assert!(evaluate_salvo_condition(&condition, &counts, &ports));
}

// ========== Serialization Tests ==========

#[test]
fn test_graph_serialization_roundtrip() {
    let graph = common::linear_graph_3();

    // Serialize to JSON
    let json = serde_json::to_string(&graph).expect("Failed to serialize graph");

    // Deserialize back
    let deserialized: Graph = serde_json::from_str(&json).expect("Failed to deserialize graph");

    // Verify structure is preserved
    assert_eq!(deserialized.nodes().len(), graph.nodes().len());
    assert_eq!(deserialized.edges().len(), graph.edges().len());
    assert!(deserialized.validate().is_empty());
}

#[test]
fn test_node_serialization_roundtrip() {
    let node = common::simple_node("TestNode", vec!["in1", "in2"], vec!["out"]);

    let json = serde_json::to_string(&node).expect("Failed to serialize node");
    let deserialized: Node = serde_json::from_str(&json).expect("Failed to deserialize node");

    assert_eq!(deserialized.name, "TestNode");
    assert_eq!(deserialized.in_ports.len(), 2);
    assert_eq!(deserialized.out_ports.len(), 1);
}

#[test]
fn test_validation_error_serialization() {
    let error = GraphValidationError::EdgeReferencesNonexistentNode {
        edge_source: PortRef {
            node_name: "A".to_string(),
            port_type: PortType::Output,
            port_name: "out".to_string(),
        },
        edge_target: PortRef {
            node_name: "B".to_string(),
            port_type: PortType::Input,
            port_name: "in".to_string(),
        },
        missing_node: "B".to_string(),
    };

    let json = serde_json::to_string(&error).expect("Failed to serialize error");
    let deserialized: GraphValidationError = serde_json::from_str(&json)
        .expect("Failed to deserialize error");

    assert_eq!(error, deserialized);
}

// ========== PortRef Display Tests ==========

#[test]
fn test_port_ref_display() {
    let port_ref = PortRef {
        node_name: "NodeA".to_string(),
        port_type: PortType::Output,
        port_name: "port1".to_string(),
    };

    assert_eq!(format!("{}", port_ref), "NodeA.out.port1");

    let input_port_ref = PortRef {
        node_name: "NodeB".to_string(),
        port_type: PortType::Input,
        port_name: "data".to_string(),
    };

    assert_eq!(format!("{}", input_port_ref), "NodeB.in.data");
}
