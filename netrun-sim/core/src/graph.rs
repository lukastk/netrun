//! Graph topology types for flow-based development networks.
//!
//! This module defines the static structure of a network: nodes, ports, edges,
//! and the conditions that govern packet flow (salvo conditions).

use std::collections::{HashMap, HashSet};

/// The name of a port on a node.
pub type PortName = String;

/// Specifies the capacity of a port (how many packets it can hold).
#[derive(Debug, Clone)]
pub enum PortSlotSpec {
    /// Port can hold unlimited packets.
    Infinite,
    /// Port can hold at most this many packets.
    Finite(u64),
}

/// A port on a node where packets can enter or exit.
#[derive(Debug, Clone)]
pub struct Port {
    /// The capacity specification for this port.
    pub slots_spec: PortSlotSpec,
}

/// A predicate on the state of a port, used in salvo conditions.
///
/// These predicates test the current packet count at a port against
/// various conditions like empty, full, or numeric comparisons.
#[derive(Debug, Clone)]
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

/// Specifies how many packets to take from a port in a salvo.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PacketCount {
    /// Take all packets from the port.
    All,
    /// Take at most this many packets (takes fewer if port has fewer).
    Count(u64),
}

/// Specifies the maximum number of times a salvo condition can trigger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaxSalvos {
    /// No limit on how many times the condition can trigger.
    Infinite,
    /// Can trigger at most this many times.
    Finite(u64),
}

/// A boolean expression over port states, used to define when salvos can trigger.
///
/// This forms a simple expression tree that can combine port state checks
/// with logical operators (And, Or, Not).
#[derive(Debug, Clone)]
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
        SalvoConditionTerm::And(terms) => terms
            .iter()
            .all(|t| evaluate_salvo_condition(t, port_packet_counts, ports)),
        SalvoConditionTerm::Or(terms) => terms
            .iter()
            .any(|t| evaluate_salvo_condition(t, port_packet_counts, ports)),
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
#[derive(Debug, Clone)]
pub struct SalvoCondition {
    /// Maximum number of times this condition can trigger per epoch.
    /// For input salvo conditions, this must be `Finite(1)`.
    pub max_salvos: MaxSalvos,
    /// The ports whose packets are included when this condition triggers,
    /// and how many packets to take from each port.
    pub ports: HashMap<PortName, PacketCount>,
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
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
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
    /// Input salvo condition has max_salvos != Finite(1)
    #[error(
        "input salvo condition '{condition_name}' on node '{node_name}' has max_salvos={max_salvos:?}, but must be Finite(1)"
    )]
    InputSalvoConditionInvalidMaxSalvos {
        node_name: NodeName,
        condition_name: SalvoConditionName,
        max_salvos: MaxSalvos,
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
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "python", pyo3::pyclass(eq, eq_int, frozen, hash))]
pub enum PortType {
    /// An input port (packets flow into the node).
    Input,
    /// An output port (packets flow out of the node).
    Output,
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl PortType {
    fn __repr__(&self) -> String {
        match self {
            PortType::Input => "PortType.Input".to_string(),
            PortType::Output => "PortType.Output".to_string(),
        }
    }
}

/// A reference to a specific port on a specific node.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "python", pyo3::pyclass(eq, frozen, hash, get_all))]
pub struct PortRef {
    /// The name of the node containing this port.
    pub node_name: NodeName,
    /// Whether this is an input or output port.
    pub port_type: PortType,
    /// The name of the port on the node.
    pub port_name: PortName,
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl PortRef {
    #[new]
    fn py_new(node_name: String, port_type: PortType, port_name: String) -> Self {
        PortRef {
            node_name,
            port_type,
            port_name,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "PortRef('{}', {:?}, '{}')",
            self.node_name, self.port_type, self.port_name
        )
    }

    fn __str__(&self) -> String {
        self.to_string()
    }
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
/// between nodes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "python", pyo3::pyclass(eq, frozen, hash, get_all))]
pub struct Edge {
    /// The output port where this edge originates.
    pub source: PortRef,
    /// The input port where this edge terminates.
    pub target: PortRef,
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl Edge {
    #[new]
    fn py_new(source: PortRef, target: PortRef) -> Self {
        Edge { source, target }
    }

    fn __repr__(&self) -> String {
        format!("Edge({}, {})", self.source, self.target)
    }

    fn __str__(&self) -> String {
        self.to_string()
    }
}

impl std::fmt::Display for Edge {
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
/// use netrun_sim::graph::{Graph, Node, Edge, PortRef, PortType, Port, PortSlotSpec};
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
/// let edge = Edge {
///     source: PortRef { node_name: "A".to_string(), port_type: PortType::Output, port_name: "out".to_string() },
///     target: PortRef { node_name: "B".to_string(), port_type: PortType::Input, port_name: "in".to_string() },
/// };
///
/// let graph = Graph::new(vec![node_a, node_b], vec![edge]);
/// assert!(graph.validate().is_empty());
/// ```
#[derive(Debug, Clone)]
pub struct Graph {
    nodes: HashMap<NodeName, Node>,
    edges: HashSet<Edge>,
    edges_by_tail: HashMap<PortRef, Edge>,
    edges_by_head: HashMap<PortRef, Edge>,
}

impl Graph {
    /// Creates a new Graph from a list of nodes and edges.
    ///
    /// Builds internal indexes for efficient edge lookups by source (tail) and target (head) ports.
    pub fn new(nodes: Vec<Node>, edges: Vec<Edge>) -> Self {
        let nodes_map: HashMap<NodeName, Node> = nodes
            .into_iter()
            .map(|node| (node.name.clone(), node))
            .collect();

        let mut edges_set: HashSet<Edge> = HashSet::new();
        let mut edges_by_tail: HashMap<PortRef, Edge> = HashMap::new();
        let mut edges_by_head: HashMap<PortRef, Edge> = HashMap::new();

        for edge in edges {
            edges_by_tail.insert(edge.source.clone(), edge.clone());
            edges_by_head.insert(edge.target.clone(), edge.clone());
            edges_set.insert(edge);
        }

        Graph {
            nodes: nodes_map,
            edges: edges_set,
            edges_by_tail,
            edges_by_head,
        }
    }

    /// Returns a reference to all nodes in the graph, keyed by name.
    pub fn nodes(&self) -> &HashMap<NodeName, Node> {
        &self.nodes
    }

    /// Returns a reference to all edges in the graph.
    pub fn edges(&self) -> &HashSet<Edge> {
        &self.edges
    }

    /// Returns the edge that has the given output port as its source (tail).
    pub fn get_edge_by_tail(&self, output_port_ref: &PortRef) -> Option<&Edge> {
        self.edges_by_tail.get(output_port_ref)
    }

    /// Returns the edge that has the given input port as its target (head).
    pub fn get_edge_by_head(&self, input_port_ref: &PortRef) -> Option<&Edge> {
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
        for edge in &self.edges {
            let source = &edge.source;
            let target = &edge.target;

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
                // Input salvo conditions must have max_salvos == Finite(1)
                if condition.max_salvos != MaxSalvos::Finite(1) {
                    errors.push(GraphValidationError::InputSalvoConditionInvalidMaxSalvos {
                        node_name: node_name.clone(),
                        condition_name: cond_name.clone(),
                        max_salvos: condition.max_salvos.clone(),
                    });
                }

                // Validate ports in condition.ports exist as input ports
                for port_name in condition.ports.keys() {
                    if !node.in_ports.contains_key(port_name) {
                        errors.push(
                            GraphValidationError::SalvoConditionReferencesNonexistentPort {
                                node_name: node_name.clone(),
                                condition_name: cond_name.clone(),
                                is_input_condition: true,
                                missing_port: port_name.clone(),
                            },
                        );
                    }
                }

                // Validate ports in condition.term exist as input ports
                let mut term_ports = HashSet::new();
                collect_ports_from_term(&condition.term, &mut term_ports);
                for port_name in term_ports {
                    if !node.in_ports.contains_key(&port_name) {
                        errors.push(
                            GraphValidationError::SalvoConditionTermReferencesNonexistentPort {
                                node_name: node_name.clone(),
                                condition_name: cond_name.clone(),
                                is_input_condition: true,
                                missing_port: port_name,
                            },
                        );
                    }
                }
            }

            // Validate output salvo conditions
            for (cond_name, condition) in &node.out_salvo_conditions {
                // Validate ports in condition.ports exist as output ports
                for port_name in condition.ports.keys() {
                    if !node.out_ports.contains_key(port_name) {
                        errors.push(
                            GraphValidationError::SalvoConditionReferencesNonexistentPort {
                                node_name: node_name.clone(),
                                condition_name: cond_name.clone(),
                                is_input_condition: false,
                                missing_port: port_name.clone(),
                            },
                        );
                    }
                }

                // Validate ports in condition.term exist as output ports
                let mut term_ports = HashSet::new();
                collect_ports_from_term(&condition.term, &mut term_ports);
                for port_name in term_ports {
                    if !node.out_ports.contains_key(&port_name) {
                        errors.push(
                            GraphValidationError::SalvoConditionTermReferencesNonexistentPort {
                                node_name: node_name.clone(),
                                condition_name: cond_name.clone(),
                                is_input_condition: false,
                                missing_port: port_name,
                            },
                        );
                    }
                }
            }
        }

        errors
    }
}

#[cfg(test)]
#[path = "graph_tests.rs"]
mod tests;
