use std::collections::HashMap;

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
    pub ports: Vec<PortName>, // TODO: Validate that the ports exist
    pub term: SalvoConditionTerm,
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
}
