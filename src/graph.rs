use std::collections::HashMap;

pub type PortName = String;

#[derive(Debug)]
pub enum PortSlotSpec {
    Infinite,
    Finite(u32),
}

#[derive(Debug)]
pub struct Port {
    pub num_slots: PortSlotSpec,
}

#[derive(Debug)]
pub enum PortState {
    Empty,
    Full,
    NonEmpty,
    NonFull,
    Exact(u32),
    Range(u32, u32),
}

#[derive(Debug)]
pub enum SalvoConditionTerm {
    Port { port_name: String, state: PortState },
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
}

pub type SalvoConditionName = String;

#[derive(Debug)]
pub struct SalvoCondition {
    pub max_salvos: u32, // 0 = unlimited
    pub term: SalvoConditionTerm,
}

pub type NodeName = String;

#[derive(Debug)]
pub struct Node {
    pub in_ports: HashMap<PortName, Port>,
    pub out_ports: HashMap<PortName, Port>,
    pub in_salvo_conditions: HashMap<SalvoConditionName, SalvoCondition>,
    pub out_salvo_conditions: HashMap<SalvoConditionName, SalvoCondition>,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum PortType {
    Input,
    Output,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PortRef {
    pub node_name: NodeName,
    pub port_type: PortType,
    pub port_name: PortName,
}

#[derive(Debug)]
pub struct Edge {
    pub buffer_size: u32,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct EdgeRef {
    pub source: PortRef,
    pub target: PortRef,
}

#[derive(Debug)]
pub struct Graph {
    pub nodes: HashMap<NodeName, Node>,
    pub edges: HashMap<EdgeRef, Edge>,
}
