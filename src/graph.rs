use std::collections::{HashMap, HashSet};

pub type PortName = String;

#[derive(Debug)]
pub enum PortSlotSpec {
    Infinite,
    Finite(u64),
}

#[derive(Debug)]
pub struct Port {
    pub slots_spec: PortSlotSpec,
}

#[derive(Debug)]
pub enum PortState {
    Empty,                    // 'E'
    Full,                     // 'F'
    NonEmpty,                 // '!E'
    NonFull,                  // '!F'
    Equals(u64),              // '='
    LessThan(u64),            // '<'
    GreaterThan(u64),         // '>'
    EqualsOrLessThan(u64),    // '<='
    EqualsOrGreaterThan(u64), // '>='
}

#[derive(Debug)]
pub enum SalvoConditionTerm {
    Port { port_name: String, state: PortState },
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
}

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

pub type SalvoConditionName = String;

#[derive(Debug)]
pub struct SalvoCondition {
    pub max_salvos: u64, // 0 = unlimited
    pub ports: Vec<PortName>,
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

#[derive(Debug, PartialEq)]
pub enum GraphValidationError {
    /// Edge references a node that doesn't exist
    EdgeReferencesNonexistentNode {
        edge_source: PortRef,
        edge_target: PortRef,
        missing_node: NodeName,
    },
    /// Edge references a port that doesn't exist on the node
    EdgeReferencesNonexistentPort {
        edge_source: PortRef,
        edge_target: PortRef,
        missing_port: PortRef,
    },
    /// Edge source is not an output port
    EdgeSourceNotOutputPort {
        edge_source: PortRef,
        edge_target: PortRef,
    },
    /// Edge target is not an input port
    EdgeTargetNotInputPort {
        edge_source: PortRef,
        edge_target: PortRef,
    },
    /// SalvoCondition.ports references a port that doesn't exist
    SalvoConditionReferencesNonexistentPort {
        node_name: NodeName,
        condition_name: SalvoConditionName,
        is_input_condition: bool,
        missing_port: PortName,
    },
    /// SalvoCondition.term references a port that doesn't exist
    SalvoConditionTermReferencesNonexistentPort {
        node_name: NodeName,
        condition_name: SalvoConditionName,
        is_input_condition: bool,
        missing_port: PortName,
    },
    /// Input salvo condition has max_salvos != 1
    InputSalvoConditionInvalidMaxSalvos {
        node_name: NodeName,
        condition_name: SalvoConditionName,
        max_salvos: u64,
    },
    /// Duplicate edge (same source and target)
    DuplicateEdge {
        edge_source: PortRef,
        edge_target: PortRef,
    },
}

pub type NodeName = String;

#[derive(Debug)]
pub struct Node {
    pub name: NodeName,
    pub in_ports: HashMap<PortName, Port>,
    pub out_ports: HashMap<PortName, Port>,
    pub in_salvo_conditions: HashMap<SalvoConditionName, SalvoCondition>,
    pub out_salvo_conditions: HashMap<SalvoConditionName, SalvoCondition>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum PortType {
    Input,
    Output,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct PortRef {
    pub node_name: NodeName,
    pub port_type: PortType,
    pub port_name: PortName,
}

#[derive(Debug)]
pub struct Edge {
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct EdgeRef {
    pub source: PortRef,
    pub target: PortRef,
}

#[derive(Debug)]
pub struct Graph {
    nodes: HashMap<NodeName, Node>,
    edges: HashMap<EdgeRef, Edge>,

    edges_by_tail: HashMap<PortRef, EdgeRef>,
    edges_by_head: HashMap<PortRef, EdgeRef>,
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

    pub fn nodes(&self) -> &HashMap<NodeName, Node> { &self.nodes }
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
