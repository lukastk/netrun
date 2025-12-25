//! Test fixtures and helper functions for creating common graph patterns.
//!
//! This module provides utilities for testing the netrun-sim library.

use crate::graph::{
    Edge, Node, Port, PortName, PortRef, PortSlotSpec, PortState, PortType,
    SalvoCondition, SalvoConditionTerm, Graph,
};
use std::collections::HashMap;

/// Creates a simple port with infinite capacity.
pub fn infinite_port() -> Port {
    Port {
        slots_spec: PortSlotSpec::Infinite,
    }
}

/// Creates a port with finite capacity.
pub fn finite_port(capacity: u64) -> Port {
    Port {
        slots_spec: PortSlotSpec::Finite(capacity),
    }
}

/// Creates a simple input salvo condition that triggers when all specified ports are non-empty.
pub fn all_ports_non_empty_condition(port_names: Vec<&str>) -> SalvoCondition {
    let terms: Vec<SalvoConditionTerm> = port_names
        .iter()
        .map(|name| SalvoConditionTerm::Port {
            port_name: name.to_string(),
            state: PortState::NonEmpty,
        })
        .collect();

    SalvoCondition {
        max_salvos: 1,
        ports: port_names.iter().map(|s| s.to_string()).collect(),
        term: if terms.len() == 1 {
            terms.into_iter().next().unwrap()
        } else {
            SalvoConditionTerm::And(terms)
        },
    }
}

/// Creates an output salvo condition that triggers when all specified ports are non-empty.
pub fn output_salvo_condition(port_names: Vec<&str>, max_salvos: u64) -> SalvoCondition {
    let terms: Vec<SalvoConditionTerm> = port_names
        .iter()
        .map(|name| SalvoConditionTerm::Port {
            port_name: name.to_string(),
            state: PortState::NonEmpty,
        })
        .collect();

    SalvoCondition {
        max_salvos,
        ports: port_names.iter().map(|s| s.to_string()).collect(),
        term: if terms.len() == 1 {
            terms.into_iter().next().unwrap()
        } else {
            SalvoConditionTerm::And(terms)
        },
    }
}

/// Creates a simple node with specified input and output ports.
pub fn simple_node(
    name: &str,
    in_ports: Vec<&str>,
    out_ports: Vec<&str>,
) -> Node {
    let in_ports_map: HashMap<PortName, Port> = in_ports
        .iter()
        .map(|p| (p.to_string(), infinite_port()))
        .collect();

    let out_ports_map: HashMap<PortName, Port> = out_ports
        .iter()
        .map(|p| (p.to_string(), infinite_port()))
        .collect();

    // Create default input salvo condition (all input ports non-empty)
    let mut in_salvo_conditions = HashMap::new();
    if !in_ports.is_empty() {
        in_salvo_conditions.insert(
            "default".to_string(),
            all_ports_non_empty_condition(in_ports.clone()),
        );
    }

    // Create default output salvo condition (all output ports non-empty)
    let mut out_salvo_conditions = HashMap::new();
    if !out_ports.is_empty() {
        out_salvo_conditions.insert(
            "default".to_string(),
            output_salvo_condition(out_ports.clone(), 0), // unlimited
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

/// Creates an edge between two ports.
pub fn edge(
    source_node: &str,
    source_port: &str,
    target_node: &str,
    target_port: &str,
) -> Edge {
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

/// Creates a simple linear graph: A -> B -> C
///
/// Each node has one input port "in" and one output port "out".
pub fn linear_graph_3() -> Graph {
    let nodes = vec![
        simple_node("A", vec![], vec!["out"]),
        simple_node("B", vec!["in"], vec!["out"]),
        simple_node("C", vec!["in"], vec![]),
    ];

    let edges = vec![
        edge("A", "out", "B", "in"),
        edge("B", "out", "C", "in"),
    ];

    Graph::new(nodes, edges)
}

/// Creates a branching graph: A -> B, A -> C
///
/// Node A has two output ports, B and C each have one input port.
pub fn branching_graph() -> Graph {
    let nodes = vec![
        simple_node("A", vec![], vec!["out1", "out2"]),
        simple_node("B", vec!["in"], vec![]),
        simple_node("C", vec!["in"], vec![]),
    ];

    let edges = vec![
        edge("A", "out1", "B", "in"),
        edge("A", "out2", "C", "in"),
    ];

    Graph::new(nodes, edges)
}

/// Creates a merging graph: A -> C, B -> C
///
/// Nodes A and B each have one output port, C has two input ports.
pub fn merging_graph() -> Graph {
    let nodes = vec![
        simple_node("A", vec![], vec!["out"]),
        simple_node("B", vec![], vec!["out"]),
        simple_node("C", vec!["in1", "in2"], vec![]),
    ];

    let edges = vec![
        edge("A", "out", "C", "in1"),
        edge("B", "out", "C", "in2"),
    ];

    Graph::new(nodes, edges)
}

/// Creates a diamond graph: A -> B -> D, A -> C -> D
pub fn diamond_graph() -> Graph {
    let nodes = vec![
        simple_node("A", vec![], vec!["out1", "out2"]),
        simple_node("B", vec!["in"], vec!["out"]),
        simple_node("C", vec!["in"], vec!["out"]),
        simple_node("D", vec!["in1", "in2"], vec![]),
    ];

    let edges = vec![
        edge("A", "out1", "B", "in"),
        edge("A", "out2", "C", "in"),
        edge("B", "out", "D", "in1"),
        edge("C", "out", "D", "in2"),
    ];

    Graph::new(nodes, edges)
}

/// Creates a node with custom salvo conditions.
pub fn node_with_conditions(
    name: &str,
    in_ports: HashMap<PortName, Port>,
    out_ports: HashMap<PortName, Port>,
    in_salvo_conditions: HashMap<String, SalvoCondition>,
    out_salvo_conditions: HashMap<String, SalvoCondition>,
) -> Node {
    Node {
        name: name.to_string(),
        in_ports,
        out_ports,
        in_salvo_conditions,
        out_salvo_conditions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_graph_creation() {
        let graph = linear_graph_3();
        assert_eq!(graph.nodes().len(), 3);
        assert_eq!(graph.edges().len(), 2);
        assert!(graph.validate().is_empty());
    }

    #[test]
    fn test_branching_graph_creation() {
        let graph = branching_graph();
        assert_eq!(graph.nodes().len(), 3);
        assert_eq!(graph.edges().len(), 2);
        assert!(graph.validate().is_empty());
    }

    #[test]
    fn test_merging_graph_creation() {
        let graph = merging_graph();
        assert_eq!(graph.nodes().len(), 3);
        assert_eq!(graph.edges().len(), 2);
        assert!(graph.validate().is_empty());
    }

    #[test]
    fn test_diamond_graph_creation() {
        let graph = diamond_graph();
        assert_eq!(graph.nodes().len(), 4);
        assert_eq!(graph.edges().len(), 4);
        assert!(graph.validate().is_empty());
    }
}
