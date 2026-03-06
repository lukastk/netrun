//! Graph topology types for flow-based development networks.
//!
//! This module defines the static structure of a network: nodes, ports, edges,
//! and the conditions that govern packet flow (salvo conditions).

use indexmap::IndexMap;
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
    /// Always true. Useful for source nodes with no input ports.
    True,
    /// Always false. Useful as a placeholder or with Not.
    False,
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
        SalvoConditionTerm::True => true,
        SalvoConditionTerm::False => false,
        SalvoConditionTerm::Port { port_name, state } => {
            debug_assert!(
                port_packet_counts.contains_key(port_name),
                "Port '{}' not found in packet counts — Graph.validate() should have caught this",
                port_name
            );
            let count = *port_packet_counts.get(port_name).unwrap_or(&0);

            debug_assert!(
                ports.contains_key(port_name),
                "Port '{}' not found in port definitions — Graph.validate() should have caught this",
                port_name
            );
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
        SalvoConditionTerm::True | SalvoConditionTerm::False => {
            // No ports referenced
        }
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
    /// Multiple edges from same output port (fan-out not allowed)
    #[error("output port {output_port} has {edge_count} outgoing edges (only 1 allowed)")]
    MultipleEdgesFromOutputPort {
        output_port: PortRef,
        edge_count: usize,
    },
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
        "input salvo condition '{condition_name}' on node '{node_name}' has max_salvos={max_salvos:?}, but must be Finite(1). Input salvos must have exactly one packet to trigger an epoch."
    )]
    InputSalvoConditionInvalidMaxSalvos {
        node_name: NodeName,
        condition_name: SalvoConditionName,
        max_salvos: MaxSalvos,
    },
    /// Dependency edge is not in the graph's edge set
    #[error("dependency edge {edge} is not in the graph's edge set")]
    DependencyEdgeNotInGraph { edge: Edge },
    /// Node has DependencyRequestConfig but no dependency edges on its input ports
    #[error("node '{node_name}' has dependency_request_config but no dependency edges on its input ports")]
    DependencyRequestConfigWithoutDependencyEdges { node_name: NodeName },
}

/// The name of a node in the graph.
pub type NodeName = String;

/// When a node should automatically emit packet requests through its dependency edges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DependencyRequestTrigger {
    /// On the first RunStep, emit requests through all dependency edges.
    /// One-shot: fires once and never again.
    OnStartup,
    /// When packets arrive but no input salvo condition triggers,
    /// emit requests through dependency edges.
    /// Rate-limited by a per-node request token.
    OnNoSalvoTriggered,
}

/// Configuration for automatic dependency-edge request generation on a node.
#[derive(Debug, Clone)]
pub struct DependencyRequestConfig {
    /// Which conditions trigger automatic requests.
    pub triggers: Vec<DependencyRequestTrigger>,
    /// Label for requests generated by this node (used for deduplication).
    pub label: String,
}

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
    /// Uses IndexMap to preserve insertion order — first satisfied condition wins.
    pub in_salvo_conditions: IndexMap<SalvoConditionName, SalvoCondition>,
    /// Conditions that must be satisfied to send packets from output ports.
    /// Uses IndexMap to preserve insertion order — first satisfied condition wins.
    pub out_salvo_conditions: IndexMap<SalvoConditionName, SalvoCondition>,
    /// Optional configuration for automatic dependency-edge request generation.
    pub dependency_request_config: Option<DependencyRequestConfig>,
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
/// use indexmap::IndexMap;
/// use std::collections::HashMap;
///
/// // Create a simple A -> B graph
/// let node_a = Node {
///     name: "A".to_string(),
///     in_ports: HashMap::new(),
///     out_ports: [("out".to_string(), Port { slots_spec: PortSlotSpec::Infinite })].into(),
///     in_salvo_conditions: IndexMap::new(),
///     out_salvo_conditions: IndexMap::new(),
///     dependency_request_config: None,
/// };
/// let node_b = Node {
///     name: "B".to_string(),
///     in_ports: [("in".to_string(), Port { slots_spec: PortSlotSpec::Infinite })].into(),
///     out_ports: HashMap::new(),
///     in_salvo_conditions: IndexMap::new(),
///     out_salvo_conditions: IndexMap::new(),
///     dependency_request_config: None,
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
    edges_by_head: HashMap<PortRef, Vec<Edge>>,
    dependency_edges: HashSet<Edge>,
}

impl Graph {
    /// Creates a new Graph from a list of nodes and edges.
    ///
    /// Builds internal indexes for efficient edge lookups by source (tail) and target (head) ports.
    pub fn new(nodes: Vec<Node>, edges: Vec<Edge>) -> Self {
        let mut nodes_map: HashMap<NodeName, Node> = HashMap::with_capacity(nodes.len());
        for node in nodes {
            if nodes_map.contains_key(&node.name) {
                panic!("Duplicate node name: '{}'", node.name);
            }
            nodes_map.insert(node.name.clone(), node);
        }

        let mut edges_set: HashSet<Edge> = HashSet::new();
        let mut edges_by_tail: HashMap<PortRef, Edge> = HashMap::new();
        let mut edges_by_head: HashMap<PortRef, Vec<Edge>> = HashMap::new();

        for edge in edges {
            edges_by_tail.insert(edge.source.clone(), edge.clone());
            edges_by_head
                .entry(edge.target.clone())
                .or_default()
                .push(edge.clone());
            edges_set.insert(edge);
        }

        Graph {
            nodes: nodes_map,
            edges: edges_set,
            edges_by_tail,
            edges_by_head,
            dependency_edges: HashSet::new(),
        }
    }

    /// Set which edges are dependency edges. Each must be in the graph's edge set.
    /// Panics if any dependency edge is not in the graph.
    pub fn with_dependency_edges(mut self, dependency_edges: Vec<Edge>) -> Self {
        self.dependency_edges = dependency_edges.into_iter().collect();
        self
    }

    /// Check if an edge is a dependency edge.
    pub fn is_dependency_edge(&self, edge: &Edge) -> bool {
        self.dependency_edges.contains(edge)
    }

    /// Returns a reference to all dependency edges.
    pub fn dependency_edges(&self) -> &HashSet<Edge> {
        &self.dependency_edges
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

    /// Returns all edges that have the given input port as their target (head).
    /// Fan-in is allowed, so multiple edges can connect to the same input port.
    pub fn get_edges_by_head(&self, input_port_ref: &PortRef) -> &[Edge] {
        self.edges_by_head
            .get(input_port_ref)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Validates the graph structure.
    ///
    /// Returns a list of all validation errors found. An empty list means the graph is valid.
    pub fn validate(&self) -> Vec<GraphValidationError> {
        let mut errors = Vec::new();

        // Validate edges (edges is a HashSet, so duplicates are already impossible)
        for edge in &self.edges {
            let source = &edge.source;
            let target = &edge.target;

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

        // Check for multiple edges from same output port (fan-out not allowed)
        let mut edges_from_source: HashMap<&PortRef, usize> = HashMap::new();
        for edge in &self.edges {
            *edges_from_source.entry(&edge.source).or_insert(0) += 1;
        }
        for (port_ref, count) in edges_from_source {
            if count > 1 {
                errors.push(GraphValidationError::MultipleEdgesFromOutputPort {
                    output_port: port_ref.clone(),
                    edge_count: count,
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

        // Validate dependency edges: each must be in the edge set
        for dep_edge in &self.dependency_edges {
            if !self.edges.contains(dep_edge) {
                errors.push(GraphValidationError::DependencyEdgeNotInGraph {
                    edge: dep_edge.clone(),
                });
            }
        }

        // Validate nodes with DependencyRequestConfig have at least one dependency edge on input ports
        for (node_name, node) in &self.nodes {
            if node.dependency_request_config.is_some() {
                let has_dep_edge = self.dependency_edges.iter().any(|dep_edge| {
                    dep_edge.target.node_name == *node_name
                        && dep_edge.target.port_type == PortType::Input
                });
                if !has_dep_edge {
                    errors.push(
                        GraphValidationError::DependencyRequestConfigWithoutDependencyEdges {
                            node_name: node_name.clone(),
                        },
                    );
                }
            }
        }

        errors
    }
}

/// Errors that can occur during a backward cascade traversal.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum CascadeError {
    /// Cycle detected during backward BFS
    #[error("cascade cycle detected at node '{node_name}'")]
    CycleDetected { node_name: NodeName },
    /// An input port has no incoming edges during cascade
    #[error("cascade reached unconnected input port '{port_name}' on node '{node_name}'")]
    UnconnectedInputPort {
        node_name: NodeName,
        port_name: PortName,
    },
}

/// Result of a backward cascade from a set of input ports.
#[derive(Debug, Clone)]
pub struct CascadeResult {
    /// Nodes with no incoming edges (the cascade terminated here).
    pub source_nodes: Vec<NodeName>,
    /// All nodes visited during the cascade.
    pub visited_nodes: Vec<NodeName>,
}

impl Graph {
    /// Backward BFS from a set of input ports, following all edges upstream.
    ///
    /// Returns source nodes (nodes with no incoming edges on any input port).
    /// Errors on cycles or unconnected input ports.
    pub fn cascade_backward(&self, start_ports: &[PortRef]) -> Result<CascadeResult, CascadeError> {
        use std::collections::VecDeque;

        let mut queue: VecDeque<PortRef> = start_ports.iter().cloned().collect();
        let mut visited_nodes: Vec<NodeName> = Vec::new();
        let mut source_nodes: Vec<NodeName> = Vec::new();
        // Track which nodes we've already queued their input ports
        let mut processed_nodes: HashSet<NodeName> = HashSet::new();

        while let Some(input_port_ref) = queue.pop_front() {
            let node_name = &input_port_ref.node_name;

            // Find incoming edges to this input port
            let incoming_edges = self.get_edges_by_head(&input_port_ref);

            if incoming_edges.is_empty() {
                return Err(CascadeError::UnconnectedInputPort {
                    node_name: node_name.clone(),
                    port_name: input_port_ref.port_name.clone(),
                });
            }

            // Follow each edge to the upstream node
            for edge in incoming_edges {
                let upstream_node_name = &edge.source.node_name;

                // Skip already-visited nodes (handles diamond convergence)
                if !processed_nodes.insert(upstream_node_name.clone()) {
                    continue;
                }

                visited_nodes.push(upstream_node_name.clone());

                // Get the upstream node
                let upstream_node = self
                    .nodes
                    .get(upstream_node_name)
                    .expect("Edge references non-existent node");

                // Check if this is a source node (no input ports)
                if upstream_node.in_ports.is_empty() {
                    if !source_nodes.contains(upstream_node_name) {
                        source_nodes.push(upstream_node_name.clone());
                    }
                    continue;
                }

                // Check if all input ports have no incoming edges (also a source)
                let has_any_incoming = upstream_node.in_ports.keys().any(|port_name| {
                    let port_ref = PortRef {
                        node_name: upstream_node_name.clone(),
                        port_type: PortType::Input,
                        port_name: port_name.clone(),
                    };
                    !self.get_edges_by_head(&port_ref).is_empty()
                });

                if !has_any_incoming {
                    if !source_nodes.contains(upstream_node_name) {
                        source_nodes.push(upstream_node_name.clone());
                    }
                    continue;
                }

                // Add all input ports of the upstream node to the queue
                for port_name in upstream_node.in_ports.keys() {
                    queue.push_back(PortRef {
                        node_name: upstream_node_name.clone(),
                        port_type: PortType::Input,
                        port_name: port_name.clone(),
                    });
                }
            }
        }

        Ok(CascadeResult {
            source_nodes,
            visited_nodes,
        })
    }
}

#[cfg(test)]
#[path = "graph_tests.rs"]
mod tests;
