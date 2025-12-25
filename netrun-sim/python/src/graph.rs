use pyo3::prelude::*;
use pyo3::types::PyList;
use std::collections::HashMap;

use crate::errors::GraphValidationError as PyGraphValidationError;

// Re-export core types for internal use
use netrun_sim::graph::{
    Edge as CoreEdge, Graph as CoreGraph, Node as CoreNode,
    Port as CorePort, PortName, PortRef as CorePortRef, PortSlotSpec as CorePortSlotSpec,
    PortState as CorePortState, PortType as CorePortType, SalvoCondition as CoreSalvoCondition,
    SalvoConditionTerm as CoreSalvoConditionTerm,
};

/// Port capacity specification.
#[pyclass(eq, eq_int)]
#[derive(Clone, PartialEq)]
pub enum PortSlotSpec {
    /// Port can hold unlimited packets.
    Infinite,
    /// Port can hold at most this many packets.
    Finite,
}

#[pymethods]
impl PortSlotSpec {
    #[staticmethod]
    fn infinite() -> Self {
        PortSlotSpec::Infinite
    }

    #[staticmethod]
    fn finite(n: u64) -> PyPortSlotSpecFinite {
        PyPortSlotSpecFinite { capacity: n }
    }

    fn __repr__(&self) -> String {
        match self {
            PortSlotSpec::Infinite => "PortSlotSpec.Infinite".to_string(),
            PortSlotSpec::Finite => "PortSlotSpec.Finite".to_string(),
        }
    }
}

/// Finite port capacity with a specific limit.
#[pyclass(name = "PortSlotSpecFinite")]
#[derive(Clone)]
pub struct PyPortSlotSpecFinite {
    #[pyo3(get)]
    pub capacity: u64,
}

#[pymethods]
impl PyPortSlotSpecFinite {
    fn __repr__(&self) -> String {
        format!("PortSlotSpec.finite({})", self.capacity)
    }
}

impl PyPortSlotSpecFinite {
    pub fn to_core(&self) -> CorePortSlotSpec {
        CorePortSlotSpec::Finite(self.capacity)
    }
}

/// Port state predicate for salvo conditions.
#[pyclass(eq, eq_int)]
#[derive(Clone, PartialEq)]
pub enum PortState {
    Empty,
    Full,
    NonEmpty,
    NonFull,
}

#[pymethods]
impl PortState {
    #[staticmethod]
    fn empty() -> Self {
        PortState::Empty
    }

    #[staticmethod]
    fn full() -> Self {
        PortState::Full
    }

    #[staticmethod]
    fn non_empty() -> Self {
        PortState::NonEmpty
    }

    #[staticmethod]
    fn non_full() -> Self {
        PortState::NonFull
    }

    #[staticmethod]
    fn equals(n: u64) -> PyPortStateNumeric {
        PyPortStateNumeric {
            kind: "equals".to_string(),
            value: n,
        }
    }

    #[staticmethod]
    fn less_than(n: u64) -> PyPortStateNumeric {
        PyPortStateNumeric {
            kind: "less_than".to_string(),
            value: n,
        }
    }

    #[staticmethod]
    fn greater_than(n: u64) -> PyPortStateNumeric {
        PyPortStateNumeric {
            kind: "greater_than".to_string(),
            value: n,
        }
    }

    #[staticmethod]
    fn equals_or_less_than(n: u64) -> PyPortStateNumeric {
        PyPortStateNumeric {
            kind: "equals_or_less_than".to_string(),
            value: n,
        }
    }

    #[staticmethod]
    fn equals_or_greater_than(n: u64) -> PyPortStateNumeric {
        PyPortStateNumeric {
            kind: "equals_or_greater_than".to_string(),
            value: n,
        }
    }

    fn __repr__(&self) -> String {
        match self {
            PortState::Empty => "PortState.Empty".to_string(),
            PortState::Full => "PortState.Full".to_string(),
            PortState::NonEmpty => "PortState.NonEmpty".to_string(),
            PortState::NonFull => "PortState.NonFull".to_string(),
        }
    }
}

impl PortState {
    pub fn to_core(&self) -> CorePortState {
        match self {
            PortState::Empty => CorePortState::Empty,
            PortState::Full => CorePortState::Full,
            PortState::NonEmpty => CorePortState::NonEmpty,
            PortState::NonFull => CorePortState::NonFull,
        }
    }
}

/// Numeric port state predicate.
#[pyclass(name = "PortStateNumeric")]
#[derive(Clone)]
pub struct PyPortStateNumeric {
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub value: u64,
}

#[pymethods]
impl PyPortStateNumeric {
    fn __repr__(&self) -> String {
        format!("PortState.{}({})", self.kind, self.value)
    }
}

impl PyPortStateNumeric {
    pub fn to_core(&self) -> CorePortState {
        match self.kind.as_str() {
            "equals" => CorePortState::Equals(self.value),
            "less_than" => CorePortState::LessThan(self.value),
            "greater_than" => CorePortState::GreaterThan(self.value),
            "equals_or_less_than" => CorePortState::EqualsOrLessThan(self.value),
            "equals_or_greater_than" => CorePortState::EqualsOrGreaterThan(self.value),
            _ => panic!("Invalid port state kind: {}", self.kind),
        }
    }
}

/// Boolean expression over port states.
#[pyclass]
#[derive(Clone)]
pub struct SalvoConditionTerm {
    inner: CoreSalvoConditionTerm,
}

#[pymethods]
impl SalvoConditionTerm {
    /// Create a port state check term.
    #[staticmethod]
    #[pyo3(signature = (port_name, state))]
    fn port(port_name: String, state: &Bound<'_, PyAny>) -> PyResult<Self> {
        let core_state = extract_port_state(state)?;
        Ok(SalvoConditionTerm {
            inner: CoreSalvoConditionTerm::Port {
                port_name,
                state: core_state,
            },
        })
    }

    /// Create an AND term (all sub-terms must be true).
    #[staticmethod]
    fn and_(terms: Vec<SalvoConditionTerm>) -> Self {
        SalvoConditionTerm {
            inner: CoreSalvoConditionTerm::And(terms.into_iter().map(|t| t.inner).collect()),
        }
    }

    /// Create an OR term (at least one sub-term must be true).
    #[staticmethod]
    fn or_(terms: Vec<SalvoConditionTerm>) -> Self {
        SalvoConditionTerm {
            inner: CoreSalvoConditionTerm::Or(terms.into_iter().map(|t| t.inner).collect()),
        }
    }

    /// Create a NOT term (sub-term must be false).
    #[staticmethod]
    fn not_(term: SalvoConditionTerm) -> Self {
        SalvoConditionTerm {
            inner: CoreSalvoConditionTerm::Not(Box::new(term.inner)),
        }
    }

    fn __repr__(&self) -> String {
        format_term(&self.inner)
    }
}

fn format_term(term: &CoreSalvoConditionTerm) -> String {
    match term {
        CoreSalvoConditionTerm::Port { port_name, state } => {
            format!("SalvoConditionTerm.port({:?}, {:?})", port_name, state)
        }
        CoreSalvoConditionTerm::And(terms) => {
            let formatted: Vec<String> = terms.iter().map(format_term).collect();
            format!("SalvoConditionTerm.and_([{}])", formatted.join(", "))
        }
        CoreSalvoConditionTerm::Or(terms) => {
            let formatted: Vec<String> = terms.iter().map(format_term).collect();
            format!("SalvoConditionTerm.or_([{}])", formatted.join(", "))
        }
        CoreSalvoConditionTerm::Not(inner) => {
            format!("SalvoConditionTerm.not_({})", format_term(inner))
        }
    }
}

impl SalvoConditionTerm {
    pub fn to_core(&self) -> CoreSalvoConditionTerm {
        self.inner.clone()
    }

    pub fn from_core(term: &CoreSalvoConditionTerm) -> Self {
        SalvoConditionTerm {
            inner: term.clone(),
        }
    }
}

fn extract_port_state(obj: &Bound<'_, PyAny>) -> PyResult<CorePortState> {
    if let Ok(state) = obj.extract::<PortState>() {
        return Ok(state.to_core());
    }
    if let Ok(numeric) = obj.extract::<PyPortStateNumeric>() {
        return Ok(numeric.to_core());
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "Expected PortState or PortStateNumeric",
    ))
}

/// A port on a node.
#[pyclass]
#[derive(Clone)]
pub struct Port {
    inner: CorePort,
}

#[pymethods]
impl Port {
    #[new]
    #[pyo3(signature = (slots_spec=None))]
    fn new(slots_spec: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let core_spec = match slots_spec {
            Some(obj) => {
                if let Ok(_) = obj.extract::<PortSlotSpec>() {
                    // It's PortSlotSpec.Infinite
                    CorePortSlotSpec::Infinite
                } else if let Ok(finite) = obj.extract::<PyPortSlotSpecFinite>() {
                    finite.to_core()
                } else {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Expected PortSlotSpec or PortSlotSpecFinite",
                    ));
                }
            }
            None => CorePortSlotSpec::Infinite,
        };
        Ok(Port {
            inner: CorePort {
                slots_spec: core_spec,
            },
        })
    }

    #[getter]
    fn slots_spec(&self, py: Python<'_>) -> PyResult<PyObject> {
        match &self.inner.slots_spec {
            CorePortSlotSpec::Infinite => Ok(PortSlotSpec::Infinite.into_pyobject(py)?.unbind().into_any()),
            CorePortSlotSpec::Finite(n) => {
                Ok(PyPortSlotSpecFinite { capacity: *n }.into_pyobject(py)?.unbind().into_any())
            }
        }
    }

    fn __repr__(&self) -> String {
        match &self.inner.slots_spec {
            CorePortSlotSpec::Infinite => "Port(PortSlotSpec.Infinite)".to_string(),
            CorePortSlotSpec::Finite(n) => format!("Port(PortSlotSpec.finite({}))", n),
        }
    }
}

impl Port {
    pub fn to_core(&self) -> CorePort {
        self.inner.clone()
    }

    pub fn from_core(port: &CorePort) -> Self {
        Port {
            inner: port.clone(),
        }
    }
}

/// Port type: Input or Output.
#[pyclass(eq, eq_int)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PortType {
    Input,
    Output,
}

#[pymethods]
impl PortType {
    fn __repr__(&self) -> String {
        match self {
            PortType::Input => "PortType.Input".to_string(),
            PortType::Output => "PortType.Output".to_string(),
        }
    }
}

impl PortType {
    pub fn to_core(&self) -> CorePortType {
        match self {
            PortType::Input => CorePortType::Input,
            PortType::Output => CorePortType::Output,
        }
    }

    pub fn from_core(pt: &CorePortType) -> Self {
        match pt {
            CorePortType::Input => PortType::Input,
            CorePortType::Output => PortType::Output,
        }
    }
}

/// Reference to a specific port on a node.
#[pyclass]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PortRef {
    #[pyo3(get)]
    pub node_name: String,
    #[pyo3(get)]
    pub port_type: PortType,
    #[pyo3(get)]
    pub port_name: String,
}

#[pymethods]
impl PortRef {
    #[new]
    fn new(node_name: String, port_type: PortType, port_name: String) -> Self {
        PortRef {
            node_name,
            port_type,
            port_name,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "PortRef({:?}, {:?}, {:?})",
            self.node_name, self.port_type, self.port_name
        )
    }

    fn __str__(&self) -> String {
        let pt = match self.port_type {
            PortType::Input => "in",
            PortType::Output => "out",
        };
        format!("{}.{}.{}", self.node_name, pt, self.port_name)
    }

    fn __eq__(&self, other: &PortRef) -> bool {
        self == other
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl PortRef {
    pub fn to_core(&self) -> CorePortRef {
        CorePortRef {
            node_name: self.node_name.clone(),
            port_type: self.port_type.to_core(),
            port_name: self.port_name.clone(),
        }
    }

    pub fn from_core(pr: &CorePortRef) -> Self {
        PortRef {
            node_name: pr.node_name.clone(),
            port_type: PortType::from_core(&pr.port_type),
            port_name: pr.port_name.clone(),
        }
    }
}

/// A connection between two ports in the graph.
#[pyclass]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Edge {
    #[pyo3(get)]
    pub source: PortRef,
    #[pyo3(get)]
    pub target: PortRef,
}

#[pymethods]
impl Edge {
    #[new]
    fn new(source: PortRef, target: PortRef) -> Self {
        Edge { source, target }
    }

    fn __repr__(&self) -> String {
        format!("Edge({}, {})", self.source.__repr__(), self.target.__repr__())
    }

    fn __str__(&self) -> String {
        format!("{} -> {}", self.source.__str__(), self.target.__str__())
    }

    fn __eq__(&self, other: &Edge) -> bool {
        self == other
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl Edge {
    pub fn to_core(&self) -> CoreEdge {
        CoreEdge {
            source: self.source.to_core(),
            target: self.target.to_core(),
        }
    }

    pub fn from_core(edge: &CoreEdge) -> Self {
        Edge {
            source: PortRef::from_core(&edge.source),
            target: PortRef::from_core(&edge.target),
        }
    }
}

/// A condition that defines when packets can trigger an epoch or be sent.
#[pyclass]
#[derive(Clone)]
pub struct SalvoCondition {
    #[pyo3(get)]
    pub max_salvos: u64,
    #[pyo3(get)]
    pub ports: Vec<String>,
    #[pyo3(get)]
    pub term: SalvoConditionTerm,
}

#[pymethods]
impl SalvoCondition {
    #[new]
    fn new(max_salvos: u64, ports: Vec<String>, term: SalvoConditionTerm) -> Self {
        SalvoCondition {
            max_salvos,
            ports,
            term,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "SalvoCondition(max_salvos={}, ports={:?}, term={})",
            self.max_salvos,
            self.ports,
            self.term.__repr__()
        )
    }
}

impl SalvoCondition {
    pub fn to_core(&self) -> CoreSalvoCondition {
        CoreSalvoCondition {
            max_salvos: self.max_salvos,
            ports: self.ports.clone(),
            term: self.term.to_core(),
        }
    }

    pub fn from_core(sc: &CoreSalvoCondition) -> Self {
        SalvoCondition {
            max_salvos: sc.max_salvos,
            ports: sc.ports.clone(),
            term: SalvoConditionTerm::from_core(&sc.term),
        }
    }
}

/// A processing node in the graph.
#[pyclass]
#[derive(Clone)]
pub struct Node {
    inner: CoreNode,
}

#[pymethods]
impl Node {
    #[new]
    #[pyo3(signature = (name, in_ports=None, out_ports=None, in_salvo_conditions=None, out_salvo_conditions=None))]
    fn new(
        name: String,
        in_ports: Option<HashMap<String, Port>>,
        out_ports: Option<HashMap<String, Port>>,
        in_salvo_conditions: Option<HashMap<String, SalvoCondition>>,
        out_salvo_conditions: Option<HashMap<String, SalvoCondition>>,
    ) -> Self {
        let in_ports_core: HashMap<PortName, CorePort> = in_ports
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, v.to_core()))
            .collect();
        let out_ports_core: HashMap<PortName, CorePort> = out_ports
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, v.to_core()))
            .collect();
        let in_salvo_core: HashMap<String, CoreSalvoCondition> = in_salvo_conditions
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, v.to_core()))
            .collect();
        let out_salvo_core: HashMap<String, CoreSalvoCondition> = out_salvo_conditions
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, v.to_core()))
            .collect();

        Node {
            inner: CoreNode {
                name,
                in_ports: in_ports_core,
                out_ports: out_ports_core,
                in_salvo_conditions: in_salvo_core,
                out_salvo_conditions: out_salvo_core,
            },
        }
    }

    #[getter]
    fn name(&self) -> &str {
        &self.inner.name
    }

    #[getter]
    fn in_ports(&self) -> HashMap<String, Port> {
        self.inner
            .in_ports
            .iter()
            .map(|(k, v)| (k.clone(), Port::from_core(v)))
            .collect()
    }

    #[getter]
    fn out_ports(&self) -> HashMap<String, Port> {
        self.inner
            .out_ports
            .iter()
            .map(|(k, v)| (k.clone(), Port::from_core(v)))
            .collect()
    }

    #[getter]
    fn in_salvo_conditions(&self) -> HashMap<String, SalvoCondition> {
        self.inner
            .in_salvo_conditions
            .iter()
            .map(|(k, v)| (k.clone(), SalvoCondition::from_core(v)))
            .collect()
    }

    #[getter]
    fn out_salvo_conditions(&self) -> HashMap<String, SalvoCondition> {
        self.inner
            .out_salvo_conditions
            .iter()
            .map(|(k, v)| (k.clone(), SalvoCondition::from_core(v)))
            .collect()
    }

    fn __repr__(&self) -> String {
        format!("Node({:?})", self.inner.name)
    }
}

impl Node {
    pub fn to_core(&self) -> CoreNode {
        self.inner.clone()
    }

    pub fn from_core(node: &CoreNode) -> Self {
        Node {
            inner: node.clone(),
        }
    }
}

/// The static topology of a flow-based network.
#[pyclass]
pub struct Graph {
    pub(crate) inner: CoreGraph,
}

#[pymethods]
impl Graph {
    #[new]
    fn new(nodes: Vec<Node>, edges: Vec<Edge>) -> Self {
        let core_nodes: Vec<CoreNode> = nodes.into_iter().map(|n| n.to_core()).collect();
        let core_edges: Vec<CoreEdge> = edges
            .into_iter()
            .map(|e| e.to_core())
            .collect();
        Graph {
            inner: CoreGraph::new(core_nodes, core_edges),
        }
    }

    /// Returns a dict of all nodes, keyed by name.
    fn nodes(&self) -> HashMap<String, Node> {
        self.inner
            .nodes()
            .iter()
            .map(|(k, v)| (k.clone(), Node::from_core(v)))
            .collect()
    }

    /// Returns a list of all edges.
    fn edges(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty(py);
        for edge in self.inner.edges().iter() {
            list.append(Edge::from_core(edge))?;
        }
        Ok(list.unbind())
    }

    /// Validate the graph and return a list of errors.
    fn validate(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let errors = self.inner.validate();
        let list = PyList::empty(py);
        for err in errors {
            let msg = format!("{}", err);
            list.append(PyGraphValidationError::new_err(msg))?;
        }
        Ok(list.unbind())
    }

    fn __repr__(&self) -> String {
        format!(
            "Graph(nodes={}, edges={})",
            self.inner.nodes().len(),
            self.inner.edges().len()
        )
    }
}

impl Graph {
    pub fn from_core(graph: CoreGraph) -> Self {
        Graph { inner: graph }
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PortSlotSpec>()?;
    m.add_class::<PyPortSlotSpecFinite>()?;
    m.add_class::<PortState>()?;
    m.add_class::<PyPortStateNumeric>()?;
    m.add_class::<SalvoConditionTerm>()?;
    m.add_class::<Port>()?;
    m.add_class::<PortType>()?;
    m.add_class::<PortRef>()?;
    m.add_class::<Edge>()?;
    m.add_class::<SalvoCondition>()?;
    m.add_class::<Node>()?;
    m.add_class::<Graph>()?;
    Ok(())
}
