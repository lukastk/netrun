//! Graph topology types for flow-based development networks.
//!
//! This module defines the static structure of a network: nodes, ports, edges,
//! and the conditions that govern packet flow (salvo conditions).

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// The name of a port on a node.
pub type PortName = String;

/// Specifies the capacity of a port (how many packets it can hold).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PortSlotSpec {
    /// Port can hold unlimited packets.
    Infinite,
    /// Port can hold at most this many packets.
    Finite(u64),
}

/// A port on a node where packets can enter or exit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Port {
    /// The capacity specification for this port.
    pub slots_spec: PortSlotSpec,
}

/// A predicate on the state of a port, used in salvo conditions.
///
/// These predicates test the current packet count at a port against
/// various conditions like empty, full, or numeric comparisons.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PortState {
    /// Port has zero packets.
    Empty,
    /// Port is at capacity (always false for infinite ports).
    Full,
    /// Port has at least one packet.
    NonEmpty,
    /// Port is below capacity (always true for infinite ports).
    NonFull,
    /// Port has exactly this many packets.
    Equals(u64),
    /// Port has fewer than this many packets.
    LessThan(u64),
    /// Port has more than this many packets.
    GreaterThan(u64),
    /// Port has at most this many packets.
    EqualsOrLessThan(u64),
    /// Port has at least this many packets.
    EqualsOrGreaterThan(u64),
}

/// A boolean expression over port states, used to define when salvos can trigger.
///
/// This forms a simple expression tree that can combine port state checks
/// with logical operators (And, Or, Not).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SalvoConditionTerm {
    /// Check if a specific port matches a state predicate.
    Port { port_name: String, state: PortState },
    /// All sub-terms must be true.
    And(Vec<Self>),
    /// At least one sub-term must be true.
    Or(Vec<Self>),
    /// The sub-term must be false.
    Not(Box<Self>),
}

/// Evaluates a salvo condition term against the current port packet counts.
///
/// # Arguments
/// * `term` - The condition term to evaluate
/// * `port_packet_counts` - Map of port names to their current packet counts
/// * `ports` - Map of port names to their definitions (needed for capacity checks)
///
/// # Returns
/// `true` if the condition is satisfied, `false` otherwise.
pub fn evaluate_salvo_condition(
    term: &SalvoConditionTerm,
    port_packet_counts: &HashMap<PortName, u64>,
    ports: &HashMap<PortName, Port>,
) -> bool {
    match term {
        SalvoConditionTerm::Port { port_name, state } => {
            let count = *port_packet_counts.get(port_name).unwrap_or(&0);
            let port = ports.get(port_name);

            match state {
                PortState::Empty => count == 0,
                PortState::Full => match port {
                    Some(p) => match p.slots_spec {
                        PortSlotSpec::Infinite => false, // Infinite port can never be full
                        PortSlotSpec::Finite(max) => count >= max,
                    },
                    None => false,
                },
                PortState::NonEmpty => count > 0,
                PortState::NonFull => match port {
                    Some(p) => match p.slots_spec {
                        PortSlotSpec::Infinite => true, // Infinite port is always non-full
                        PortSlotSpec::Finite(max) => count < max,
                    },
                    None => true,
                },
                PortState::Equals(n) => count == *n,
                PortState::LessThan(n) => count < *n,
                PortState::GreaterThan(n) => count > *n,
                PortState::EqualsOrLessThan(n) => count <= *n,
                PortState::EqualsOrGreaterThan(n) => count >= *n,
            }
        }
        SalvoConditionTerm::And(terms) => {
            terms.iter().all(|t| evaluate_salvo_condition(t, port_packet_counts, ports))
        }
        SalvoConditionTerm::Or(terms) => {
            terms.iter().any(|t| evaluate_salvo_condition(t, port_packet_counts, ports))
        }
        SalvoConditionTerm::Not(inner) => {
            !evaluate_salvo_condition(inner, port_packet_counts, ports)
        }
    }
}

/// The name of a salvo condition.
pub type SalvoConditionName = String;

/// A condition that defines when packets can trigger an epoch or be sent.
///
/// Salvo conditions are attached to nodes and control the flow of packets:
/// - **Input salvo conditions**: Define when packets at input ports can trigger a new epoch
/// - **Output salvo conditions**: Define when packets at output ports can be sent out
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalvoCondition {
    /// Maximum number of times this condition can trigger per epoch.
    /// For input salvo conditions, this must be 1.
    /// For output salvo conditions, 0 means unlimited.
    pub max_salvos: u64,
    /// The ports whose packets are included when this condition triggers.
    pub ports: Vec<PortName>,
    /// The boolean condition that must be satisfied for this salvo to trigger.
    pub term: SalvoConditionTerm,
}

/// Extracts all port names referenced in a SalvoConditionTerm.
fn collect_ports_from_term(term: &SalvoConditionTerm, ports: &mut HashSet<PortName>) {
    match term {
        SalvoConditionTerm::Port { port_name, .. } => {
            ports.insert(port_name.clone());
        }
        SalvoConditionTerm::And(terms) | SalvoConditionTerm::Or(terms) => {
            for t in terms {
                collect_ports_from_term(t, ports);
            }
        }
        SalvoConditionTerm::Not(inner) => {
            collect_ports_from_term(inner, ports);
        }
    }
}

/// Errors that can occur during graph validation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
pub enum GraphValidationError {
    /// Edge references a node that doesn't exist
    #[error("edge {edge_source} -> {edge_target} references non-existent node '{missing_node}'")]
    EdgeReferencesNonexistentNode {
        edge_source: PortRef,
        edge_target: PortRef,
        missing_node: NodeName,
    },
    /// Edge references a port that doesn't exist on the node
    #[error("edge {edge_source} -> {edge_target} references non-existent port {missing_port}")]
    EdgeReferencesNonexistentPort {
        edge_source: PortRef,
        edge_target: PortRef,
        missing_port: PortRef,
    },
    /// Edge source is not an output port
    #[error("edge source {edge_source} must be an output port")]
    EdgeSourceNotOutputPort {
        edge_source: PortRef,
        edge_target: PortRef,
    },
    /// Edge target is not an input port
    #[error("edge target {edge_target} must be an input port")]
    EdgeTargetNotInputPort {
        edge_source: PortRef,
        edge_target: PortRef,
    },
    /// SalvoCondition.ports references a port that doesn't exist
    #[error("{condition_type} salvo condition '{condition_name}' on node '{node_name}' references non-existent port '{missing_port}'", condition_type = if *is_input_condition { "input" } else { "output" })]
    SalvoConditionReferencesNonexistentPort {
        node_name: NodeName,
        condition_name: SalvoConditionName,
        is_input_condition: bool,
        missing_port: PortName,
    },
    /// SalvoCondition.term references a port that doesn't exist
    #[error("{condition_type} salvo condition '{condition_name}' on node '{node_name}' has term referencing non-existent port '{missing_port}'", condition_type = if *is_input_condition { "input" } else { "output" })]
    SalvoConditionTermReferencesNonexistentPort {
        node_name: NodeName,
        condition_name: SalvoConditionName,
        is_input_condition: bool,
        missing_port: PortName,
    },
    /// Input salvo condition has max_salvos != 1
    #[error("input salvo condition '{condition_name}' on node '{node_name}' has max_salvos={max_salvos}, but must be 1")]
    InputSalvoConditionInvalidMaxSalvos {
        node_name: NodeName,
        condition_name: SalvoConditionName,
        max_salvos: u64,
    },
    /// Duplicate edge (same source and target)
    #[error("duplicate edge: {edge_source} -> {edge_target}")]
    DuplicateEdge {
        edge_source: PortRef,
        edge_target: PortRef,
    },
}

/// The name of a node in the graph.
pub type NodeName = String;

/// A processing unit in the graph with input and output ports.
///
/// Nodes are the fundamental building blocks of a flow-based network.
/// They have:
/// - Input ports where packets arrive
/// - Output ports where packets are sent
/// - Input salvo conditions that define when arriving packets trigger an epoch
/// - Output salvo conditions that define when output packets can be sent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// The unique name of this node.
    pub name: NodeName,
    /// Input ports where packets can arrive.
    pub in_ports: HashMap<PortName, Port>,
    /// Output ports where packets can be sent.
    pub out_ports: HashMap<PortName, Port>,
    /// Conditions that trigger new epochs when satisfied by packets at input ports.
    pub in_salvo_conditions: HashMap<SalvoConditionName, SalvoCondition>,
    /// Conditions that must be satisfied to send packets from output ports.
    pub out_salvo_conditions: HashMap<SalvoConditionName, SalvoCondition>,
}

/// Whether a port is for input or output.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PortType {
    /// An input port (packets flow into the node).
    Input,
    /// An output port (packets flow out of the node).
    Output,
}

/// A reference to a specific port on a specific node.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PortRef {
    /// The name of the node containing this port.
    pub node_name: NodeName,
    /// Whether this is an input or output port.
    pub port_type: PortType,
    /// The name of the port on the node.
    pub port_name: PortName,
}

impl std::fmt::Display for PortRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let port_type_str = match self.port_type {
            PortType::Input => "in",
            PortType::Output => "out",
        };
        write!(f, "{}.{}.{}", self.node_name, port_type_str, self.port_name)
    }
}

/// A connection between two ports in the graph.
///
/// Edges connect output ports to input ports, allowing packets to flow
/// between nodes. Currently edges have no additional properties beyond
/// their endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
}

/// A reference to an edge, identified by its source and target ports.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EdgeRef {
    /// The output port where this edge originates.
    pub source: PortRef,
    /// The input port where this edge terminates.
    pub target: PortRef,
}

impl std::fmt::Display for EdgeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> {}", self.source, self.target)
    }
}

/// The static topology of a flow-based network.
///
/// A Graph defines the structure of a network: which nodes exist, how they're
/// connected, and what conditions govern packet flow. The graph is immutable
/// after creation.
///
/// # Example
///
/// ```
/// use netrun_sim::graph::{Graph, Node, Edge, EdgeRef, PortRef, PortType, Port, PortSlotSpec};
/// use std::collections::HashMap;
///
/// // Create a simple A -> B graph
/// let node_a = Node {
///     name: "A".to_string(),
///     in_ports: HashMap::new(),
///     out_ports: [("out".to_string(), Port { slots_spec: PortSlotSpec::Infinite })].into(),
///     in_salvo_conditions: HashMap::new(),
///     out_salvo_conditions: HashMap::new(),
/// };
/// let node_b = Node {
///     name: "B".to_string(),
///     in_ports: [("in".to_string(), Port { slots_spec: PortSlotSpec::Infinite })].into(),
///     out_ports: HashMap::new(),
///     in_salvo_conditions: HashMap::new(),
///     out_salvo_conditions: HashMap::new(),
/// };
///
/// let edge = (
///     EdgeRef {
///         source: PortRef { node_name: "A".to_string(), port_type: PortType::Output, port_name: "out".to_string() },
///         target: PortRef { node_name: "B".to_string(), port_type: PortType::Input, port_name: "in".to_string() },
///     },
///     Edge {},
/// );
///
/// let graph = Graph::new(vec![node_a, node_b], vec![edge]);
/// assert!(graph.validate().is_empty());
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "GraphData", into = "GraphData")]
pub struct Graph {
    nodes: HashMap<NodeName, Node>,
    edges: HashMap<EdgeRef, Edge>,

    #[serde(skip)]
    edges_by_tail: HashMap<PortRef, EdgeRef>,
    #[serde(skip)]
    edges_by_head: HashMap<PortRef, EdgeRef>,
}

/// Helper struct for serializing/deserializing Graph without the indexes
#[derive(Serialize, Deserialize)]
struct GraphData {
    nodes: Vec<Node>,
    edges: Vec<(EdgeRef, Edge)>,
}

impl From<GraphData> for Graph {
    fn from(data: GraphData) -> Self {
        Graph::new(data.nodes, data.edges)
    }
}

impl From<Graph> for GraphData {
    fn from(graph: Graph) -> Self {
        GraphData {
            nodes: graph.nodes.into_values().collect(),
            edges: graph.edges.into_iter().collect(),
        }
    }
}

impl Graph {
    /// Creates a new Graph from a list of nodes and edges.
    ///
    /// Builds internal indexes for efficient edge lookups by source (tail) and target (head) ports.
    pub fn new(nodes: Vec<Node>, edges: Vec<(EdgeRef, Edge)>) -> Self {
        let nodes_map: HashMap<NodeName, Node> = nodes
            .into_iter()
            .map(|node| (node.name.clone(), node))
            .collect();

        let mut edges_map: HashMap<EdgeRef, Edge> = HashMap::new();
        let mut edges_by_tail: HashMap<PortRef, EdgeRef> = HashMap::new();
        let mut edges_by_head: HashMap<PortRef, EdgeRef> = HashMap::new();

        for (edge_ref, edge) in edges {
            edges_by_tail.insert(edge_ref.source.clone(), edge_ref.clone());
            edges_by_head.insert(edge_ref.target.clone(), edge_ref.clone());
            edges_map.insert(edge_ref, edge);
        }

        Graph {
            nodes: nodes_map,
            edges: edges_map,
            edges_by_tail,
            edges_by_head,
        }
    }

    /// Returns a reference to all nodes in the graph, keyed by name.
    pub fn nodes(&self) -> &HashMap<NodeName, Node> { &self.nodes }

    /// Returns a reference to all edges in the graph, keyed by their endpoints.
    pub fn edges(&self) -> &HashMap<EdgeRef, Edge> { &self.edges }

    /// Returns the edge that has the given output port as its source (tail).
    pub fn get_edge_by_tail(&self, output_port_ref: &PortRef) -> Option<&EdgeRef> {
        self.edges_by_tail.get(output_port_ref)
    }

    /// Returns the edge that has the given input port as its target (head).
    pub fn get_edge_by_head(&self, input_port_ref: &PortRef) -> Option<&EdgeRef> {
        self.edges_by_head.get(input_port_ref)
    }

    /// Validates the graph structure.
    ///
    /// Returns a list of all validation errors found. An empty list means the graph is valid.
    pub fn validate(&self) -> Vec<GraphValidationError> {
        let mut errors = Vec::new();

        // Track seen edges to detect duplicates
        let mut seen_edges: HashSet<(&PortRef, &PortRef)> = HashSet::new();

        // Validate edges
        for edge_ref in self.edges.keys() {
            let source = &edge_ref.source;
            let target = &edge_ref.target;

            // Check for duplicate edges
            if !seen_edges.insert((source, target)) {
                errors.push(GraphValidationError::DuplicateEdge {
                    edge_source: source.clone(),
                    edge_target: target.clone(),
                });
                continue;
            }

            // Validate source node exists
            let source_node = match self.nodes.get(&source.node_name) {
                Some(node) => node,
                None => {
                    errors.push(GraphValidationError::EdgeReferencesNonexistentNode {
                        edge_source: source.clone(),
                        edge_target: target.clone(),
                        missing_node: source.node_name.clone(),
                    });
                    continue;
                }
            };

            // Validate target node exists
            let target_node = match self.nodes.get(&target.node_name) {
                Some(node) => node,
                None => {
                    errors.push(GraphValidationError::EdgeReferencesNonexistentNode {
                        edge_source: source.clone(),
                        edge_target: target.clone(),
                        missing_node: target.node_name.clone(),
                    });
                    continue;
                }
            };

            // Validate source is an output port
            if source.port_type != PortType::Output {
                errors.push(GraphValidationError::EdgeSourceNotOutputPort {
                    edge_source: source.clone(),
                    edge_target: target.clone(),
                });
            } else if !source_node.out_ports.contains_key(&source.port_name) {
                errors.push(GraphValidationError::EdgeReferencesNonexistentPort {
                    edge_source: source.clone(),
                    edge_target: target.clone(),
                    missing_port: source.clone(),
                });
            }

            // Validate target is an input port
            if target.port_type != PortType::Input {
                errors.push(GraphValidationError::EdgeTargetNotInputPort {
                    edge_source: source.clone(),
                    edge_target: target.clone(),
                });
            } else if !target_node.in_ports.contains_key(&target.port_name) {
                errors.push(GraphValidationError::EdgeReferencesNonexistentPort {
                    edge_source: source.clone(),
                    edge_target: target.clone(),
                    missing_port: target.clone(),
                });
            }
        }

        // Validate nodes and their salvo conditions
        for (node_name, node) in &self.nodes {
            // Validate input salvo conditions
            for (cond_name, condition) in &node.in_salvo_conditions {
                // Input salvo conditions must have max_salvos == 1
                if condition.max_salvos != 1 {
                    errors.push(GraphValidationError::InputSalvoConditionInvalidMaxSalvos {
                        node_name: node_name.clone(),
                        condition_name: cond_name.clone(),
                        max_salvos: condition.max_salvos,
                    });
                }

                // Validate ports in condition.ports exist as input ports
                for port_name in &condition.ports {
                    if !node.in_ports.contains_key(port_name) {
                        errors.push(GraphValidationError::SalvoConditionReferencesNonexistentPort {
                            node_name: node_name.clone(),
                            condition_name: cond_name.clone(),
                            is_input_condition: true,
                            missing_port: port_name.clone(),
                        });
                    }
                }

                // Validate ports in condition.term exist as input ports
                let mut term_ports = HashSet::new();
                collect_ports_from_term(&condition.term, &mut term_ports);
                for port_name in term_ports {
                    if !node.in_ports.contains_key(&port_name) {
                        errors.push(GraphValidationError::SalvoConditionTermReferencesNonexistentPort {
                            node_name: node_name.clone(),
                            condition_name: cond_name.clone(),
                            is_input_condition: true,
                            missing_port: port_name,
                        });
                    }
                }
            }

            // Validate output salvo conditions
            for (cond_name, condition) in &node.out_salvo_conditions {
                // Validate ports in condition.ports exist as output ports
                for port_name in &condition.ports {
                    if !node.out_ports.contains_key(port_name) {
                        errors.push(GraphValidationError::SalvoConditionReferencesNonexistentPort {
                            node_name: node_name.clone(),
                            condition_name: cond_name.clone(),
                            is_input_condition: false,
                            missing_port: port_name.clone(),
                        });
                    }
                }

                // Validate ports in condition.term exist as output ports
                let mut term_ports = HashSet::new();
                collect_ports_from_term(&condition.term, &mut term_ports);
                for port_name in term_ports {
                    if !node.out_ports.contains_key(&port_name) {
                        errors.push(GraphValidationError::SalvoConditionTermReferencesNonexistentPort {
                            node_name: node_name.clone(),
                            condition_name: cond_name.clone(),
                            is_input_condition: false,
                            missing_port: port_name,
                        });
                    }
                }
            }
        }

        errors
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper functions for tests
    fn simple_port() -> Port {
        Port { slots_spec: PortSlotSpec::Infinite }
    }

    fn simple_node(name: &str, in_ports: Vec<&str>, out_ports: Vec<&str>) -> Node {
        let in_ports_map: HashMap<PortName, Port> = in_ports
            .iter()
            .map(|p| (p.to_string(), simple_port()))
            .collect();
        let out_ports_map: HashMap<PortName, Port> = out_ports
            .iter()
            .map(|p| (p.to_string(), simple_port()))
            .collect();

        // Default input salvo condition
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

        Node {
            name: name.to_string(),
            in_ports: in_ports_map,
            out_ports: out_ports_map,
            in_salvo_conditions,
            out_salvo_conditions: HashMap::new(),
        }
    }

    fn edge(src_node: &str, src_port: &str, tgt_node: &str, tgt_port: &str) -> (EdgeRef, Edge) {
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

    #[test]
    fn test_valid_graph_passes_validation() {
        let nodes = vec![
            simple_node("A", vec![], vec!["out"]),
            simple_node("B", vec!["in"], vec![]),
        ];
        let edges = vec![edge("A", "out", "B", "in")];
        let graph = Graph::new(nodes, edges);

        let errors = graph.validate();
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_edge_references_nonexistent_source_node() {
        let nodes = vec![
            simple_node("B", vec!["in"], vec![]),
        ];
        // Edge from nonexistent node "A"
        let edges = vec![edge("A", "out", "B", "in")];
        let graph = Graph::new(nodes, edges);

        let errors = graph.validate();
        assert_eq!(errors.len(), 1);
        match &errors[0] {
            GraphValidationError::EdgeReferencesNonexistentNode { missing_node, .. } => {
                assert_eq!(missing_node, "A");
            }
            _ => panic!("Expected EdgeReferencesNonexistentNode, got: {:?}", errors[0]),
        }
    }

    #[test]
    fn test_edge_references_nonexistent_target_node() {
        let nodes = vec![
            simple_node("A", vec![], vec!["out"]),
        ];
        // Edge to nonexistent node "B"
        let edges = vec![edge("A", "out", "B", "in")];
        let graph = Graph::new(nodes, edges);

        let errors = graph.validate();
        assert_eq!(errors.len(), 1);
        match &errors[0] {
            GraphValidationError::EdgeReferencesNonexistentNode { missing_node, .. } => {
                assert_eq!(missing_node, "B");
            }
            _ => panic!("Expected EdgeReferencesNonexistentNode, got: {:?}", errors[0]),
        }
    }

    #[test]
    fn test_edge_references_nonexistent_source_port() {
        let nodes = vec![
            simple_node("A", vec![], vec!["out"]),
            simple_node("B", vec!["in"], vec![]),
        ];
        // Edge from nonexistent port "wrong_port"
        let edges = vec![edge("A", "wrong_port", "B", "in")];
        let graph = Graph::new(nodes, edges);

        let errors = graph.validate();
        assert_eq!(errors.len(), 1);
        match &errors[0] {
            GraphValidationError::EdgeReferencesNonexistentPort { missing_port, .. } => {
                assert_eq!(missing_port.port_name, "wrong_port");
            }
            _ => panic!("Expected EdgeReferencesNonexistentPort, got: {:?}", errors[0]),
        }
    }

    #[test]
    fn test_edge_references_nonexistent_target_port() {
        let nodes = vec![
            simple_node("A", vec![], vec!["out"]),
            simple_node("B", vec!["in"], vec![]),
        ];
        // Edge to nonexistent port "wrong_port"
        let edges = vec![edge("A", "out", "B", "wrong_port")];
        let graph = Graph::new(nodes, edges);

        let errors = graph.validate();
        assert_eq!(errors.len(), 1);
        match &errors[0] {
            GraphValidationError::EdgeReferencesNonexistentPort { missing_port, .. } => {
                assert_eq!(missing_port.port_name, "wrong_port");
            }
            _ => panic!("Expected EdgeReferencesNonexistentPort, got: {:?}", errors[0]),
        }
    }

    #[test]
    fn test_edge_source_must_be_output_port() {
        let nodes = vec![
            simple_node("A", vec!["in"], vec!["out"]),
            simple_node("B", vec!["in"], vec![]),
        ];
        // Edge from input port (wrong type)
        let edges = vec![(
            EdgeRef {
                source: PortRef {
                    node_name: "A".to_string(),
                    port_type: PortType::Input, // Wrong!
                    port_name: "in".to_string(),
                },
                target: PortRef {
                    node_name: "B".to_string(),
                    port_type: PortType::Input,
                    port_name: "in".to_string(),
                },
            },
            Edge {},
        )];
        let graph = Graph::new(nodes, edges);

        let errors = graph.validate();
        assert!(errors.iter().any(|e| matches!(e, GraphValidationError::EdgeSourceNotOutputPort { .. })));
    }

    #[test]
    fn test_edge_target_must_be_input_port() {
        let nodes = vec![
            simple_node("A", vec![], vec!["out"]),
            simple_node("B", vec!["in"], vec!["out"]),
        ];
        // Edge to output port (wrong type)
        let edges = vec![(
            EdgeRef {
                source: PortRef {
                    node_name: "A".to_string(),
                    port_type: PortType::Output,
                    port_name: "out".to_string(),
                },
                target: PortRef {
                    node_name: "B".to_string(),
                    port_type: PortType::Output, // Wrong!
                    port_name: "out".to_string(),
                },
            },
            Edge {},
        )];
        let graph = Graph::new(nodes, edges);

        let errors = graph.validate();
        assert!(errors.iter().any(|e| matches!(e, GraphValidationError::EdgeTargetNotInputPort { .. })));
    }

    #[test]
    fn test_input_salvo_condition_must_have_max_salvos_1() {
        let mut node = simple_node("A", vec!["in"], vec![]);
        // Set max_salvos to something other than 1
        node.in_salvo_conditions.get_mut("default").unwrap().max_salvos = 2;

        let graph = Graph::new(vec![node], vec![]);

        let errors = graph.validate();
        assert_eq!(errors.len(), 1);
        match &errors[0] {
            GraphValidationError::InputSalvoConditionInvalidMaxSalvos { max_salvos, .. } => {
                assert_eq!(*max_salvos, 2);
            }
            _ => panic!("Expected InputSalvoConditionInvalidMaxSalvos, got: {:?}", errors[0]),
        }
    }

    #[test]
    fn test_salvo_condition_ports_must_exist() {
        let mut node = simple_node("A", vec!["in"], vec![]);
        // Reference nonexistent port in condition.ports
        node.in_salvo_conditions.get_mut("default").unwrap().ports = vec!["nonexistent".to_string()];

        let graph = Graph::new(vec![node], vec![]);

        let errors = graph.validate();
        assert!(errors.iter().any(|e| matches!(
            e,
            GraphValidationError::SalvoConditionReferencesNonexistentPort { missing_port, .. }
            if missing_port == "nonexistent"
        )));
    }

    #[test]
    fn test_salvo_condition_term_ports_must_exist() {
        let mut node = simple_node("A", vec!["in"], vec![]);
        // Reference nonexistent port in condition.term
        node.in_salvo_conditions.get_mut("default").unwrap().term = SalvoConditionTerm::Port {
            port_name: "nonexistent".to_string(),
            state: PortState::NonEmpty,
        };

        let graph = Graph::new(vec![node], vec![]);

        let errors = graph.validate();
        assert!(errors.iter().any(|e| matches!(
            e,
            GraphValidationError::SalvoConditionTermReferencesNonexistentPort { missing_port, .. }
            if missing_port == "nonexistent"
        )));
    }

    #[test]
    fn test_output_salvo_condition_ports_must_exist() {
        let mut node = simple_node("A", vec![], vec!["out"]);
        // Add output salvo condition referencing nonexistent port
        node.out_salvo_conditions.insert(
            "test".to_string(),
            SalvoCondition {
                max_salvos: 0,
                ports: vec!["nonexistent".to_string()],
                term: SalvoConditionTerm::Port {
                    port_name: "out".to_string(),
                    state: PortState::NonEmpty,
                },
            },
        );

        let graph = Graph::new(vec![node], vec![]);

        let errors = graph.validate();
        assert!(errors.iter().any(|e| matches!(
            e,
            GraphValidationError::SalvoConditionReferencesNonexistentPort {
                is_input_condition: false,
                missing_port,
                ..
            } if missing_port == "nonexistent"
        )));
    }

    #[test]
    fn test_complex_salvo_condition_term_validation() {
        let mut node = simple_node("A", vec!["in1", "in2"], vec![]);
        // Create complex term with And/Or/Not that references nonexistent port
        node.in_salvo_conditions.get_mut("default").unwrap().term = SalvoConditionTerm::And(vec![
            SalvoConditionTerm::Port {
                port_name: "in1".to_string(),
                state: PortState::NonEmpty,
            },
            SalvoConditionTerm::Or(vec![
                SalvoConditionTerm::Port {
                    port_name: "in2".to_string(),
                    state: PortState::NonEmpty,
                },
                SalvoConditionTerm::Not(Box::new(SalvoConditionTerm::Port {
                    port_name: "nonexistent".to_string(), // This should be caught
                    state: PortState::Empty,
                })),
            ]),
        ]);

        let graph = Graph::new(vec![node], vec![]);

        let errors = graph.validate();
        assert!(errors.iter().any(|e| matches!(
            e,
            GraphValidationError::SalvoConditionTermReferencesNonexistentPort { missing_port, .. }
            if missing_port == "nonexistent"
        )));
    }

    #[test]
    fn test_empty_graph_is_valid() {
        let graph = Graph::new(vec![], vec![]);
        let errors = graph.validate();
        assert!(errors.is_empty());
    }

    #[test]
    fn test_node_without_ports_is_valid() {
        let node = Node {
            name: "A".to_string(),
            in_ports: HashMap::new(),
            out_ports: HashMap::new(),
            in_salvo_conditions: HashMap::new(),
            out_salvo_conditions: HashMap::new(),
        };
        let graph = Graph::new(vec![node], vec![]);
        let errors = graph.validate();
        assert!(errors.is_empty());
    }
}
