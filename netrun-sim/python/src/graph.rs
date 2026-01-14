use pyo3::prelude::*;
use pyo3::types::PyList;
use std::collections::HashMap;

use crate::errors::GraphValidationError as PyGraphValidationError;

// Re-export core types that have pyclass directly
pub use netrun_sim::graph::{Edge, PortRef, PortType};

// Import core types with aliases for internal use
use netrun_sim::graph::{
    Graph as CoreGraph, MaxSalvos as CoreMaxSalvos, Node as CoreNode,
    PacketCount as CorePacketCount, Port as CorePort, PortName, PortSlotSpec as CorePortSlotSpec,
    PortState as CorePortState, SalvoCondition as CoreSalvoCondition,
    SalvoConditionTerm as CoreSalvoConditionTerm,
};

/// Specifies how many packets to take from a port in a salvo.
#[pyclass(eq, eq_int)]
#[derive(Clone, PartialEq)]
pub enum PacketCount {
    /// Take all packets from the port.
    All,
}

#[pymethods]
impl PacketCount {
    /// Take all packets from the port.
    #[staticmethod]
    fn all() -> Self {
        PacketCount::All
    }

    /// Take at most n packets (takes fewer if port has fewer).
    #[staticmethod]
    fn count(n: u64) -> PyPacketCountN {
        PyPacketCountN { count: n }
    }

    fn __repr__(&self) -> String {
        match self {
            PacketCount::All => "PacketCount.All".to_string(),
        }
    }
}

impl PacketCount {
    pub fn to_core(&self) -> CorePacketCount {
        match self {
            PacketCount::All => CorePacketCount::All,
        }
    }
}

/// PacketCount with a specific count limit.
#[pyclass(name = "PacketCountN")]
#[derive(Clone)]
pub struct PyPacketCountN {
    #[pyo3(get)]
    pub count: u64,
}

#[pymethods]
impl PyPacketCountN {
    fn __repr__(&self) -> String {
        format!("PacketCount.count({})", self.count)
    }
}

impl PyPacketCountN {
    pub fn to_core(&self) -> CorePacketCount {
        CorePacketCount::Count(self.count)
    }
}

/// Specifies the maximum number of times a salvo condition can trigger.
#[pyclass(eq, eq_int)]
#[derive(Clone, PartialEq)]
pub enum MaxSalvos {
    /// No limit on how many times the condition can trigger.
    Infinite,
}

#[pymethods]
impl MaxSalvos {
    /// No limit on triggers.
    #[staticmethod]
    fn infinite() -> Self {
        MaxSalvos::Infinite
    }

    /// Finite limit on triggers.
    #[staticmethod]
    fn finite(n: u64) -> PyMaxSalvosFinite {
        PyMaxSalvosFinite { max: n }
    }

    fn __repr__(&self) -> String {
        match self {
            MaxSalvos::Infinite => "MaxSalvos.Infinite".to_string(),
        }
    }
}

impl MaxSalvos {
    pub fn to_core(&self) -> CoreMaxSalvos {
        match self {
            MaxSalvos::Infinite => CoreMaxSalvos::Infinite,
        }
    }
}

/// Finite max salvos with a specific limit.
#[pyclass(name = "MaxSalvosFinite")]
#[derive(Clone)]
pub struct PyMaxSalvosFinite {
    #[pyo3(get)]
    pub max: u64,
}

#[pymethods]
impl PyMaxSalvosFinite {
    fn __repr__(&self) -> String {
        format!("MaxSalvos.finite({})", self.max)
    }
}

impl PyMaxSalvosFinite {
    pub fn to_core(&self) -> CoreMaxSalvos {
        CoreMaxSalvos::Finite(self.max)
    }
}

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
    /// Create a term that is always true. Useful for source nodes with no input ports.
    #[staticmethod]
    fn true_() -> Self {
        SalvoConditionTerm {
            inner: CoreSalvoConditionTerm::True,
        }
    }

    /// Create a term that is always false. Useful as a placeholder or with Not.
    #[staticmethod]
    fn false_() -> Self {
        SalvoConditionTerm {
            inner: CoreSalvoConditionTerm::False,
        }
    }

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

    /// Get the term kind: "True", "False", "Port", "And", "Or", or "Not".
    #[getter]
    fn kind(&self) -> String {
        match &self.inner {
            CoreSalvoConditionTerm::True => "True".to_string(),
            CoreSalvoConditionTerm::False => "False".to_string(),
            CoreSalvoConditionTerm::Port { .. } => "Port".to_string(),
            CoreSalvoConditionTerm::And(_) => "And".to_string(),
            CoreSalvoConditionTerm::Or(_) => "Or".to_string(),
            CoreSalvoConditionTerm::Not(_) => "Not".to_string(),
        }
    }

    /// Get the port name (for Port terms only).
    fn get_port_name(&self) -> Option<String> {
        match &self.inner {
            CoreSalvoConditionTerm::Port { port_name, .. } => Some(port_name.clone()),
            _ => None,
        }
    }

    /// Get the port state (for Port terms only).
    fn get_port_state(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        match &self.inner {
            CoreSalvoConditionTerm::Port { state, .. } => {
                let py_state = core_port_state_to_py(py, state)?;
                Ok(Some(py_state))
            }
            _ => Ok(None),
        }
    }

    /// Get the sub-terms (for And and Or terms only).
    fn get_terms(&self) -> Option<Vec<SalvoConditionTerm>> {
        match &self.inner {
            CoreSalvoConditionTerm::And(terms) => Some(
                terms
                    .iter()
                    .map(|t| SalvoConditionTerm { inner: t.clone() })
                    .collect(),
            ),
            CoreSalvoConditionTerm::Or(terms) => Some(
                terms
                    .iter()
                    .map(|t| SalvoConditionTerm { inner: t.clone() })
                    .collect(),
            ),
            _ => None,
        }
    }

    /// Get the inner term (for Not terms only).
    fn get_inner(&self) -> Option<SalvoConditionTerm> {
        match &self.inner {
            CoreSalvoConditionTerm::Not(inner) => Some(SalvoConditionTerm {
                inner: (**inner).clone(),
            }),
            _ => None,
        }
    }

    fn __repr__(&self) -> String {
        format_term(&self.inner)
    }
}

fn format_term(term: &CoreSalvoConditionTerm) -> String {
    match term {
        CoreSalvoConditionTerm::True => "SalvoConditionTerm.true_()".to_string(),
        CoreSalvoConditionTerm::False => "SalvoConditionTerm.false_()".to_string(),
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

fn core_port_state_to_py(py: Python<'_>, state: &CorePortState) -> PyResult<PyObject> {
    match state {
        CorePortState::Empty => Ok(PortState::Empty.into_pyobject(py)?.unbind().into_any()),
        CorePortState::Full => Ok(PortState::Full.into_pyobject(py)?.unbind().into_any()),
        CorePortState::NonEmpty => Ok(PortState::NonEmpty.into_pyobject(py)?.unbind().into_any()),
        CorePortState::NonFull => Ok(PortState::NonFull.into_pyobject(py)?.unbind().into_any()),
        CorePortState::Equals(n) => Ok(PyPortStateNumeric {
            kind: "equals".to_string(),
            value: *n,
        }
        .into_pyobject(py)?
        .unbind()
        .into_any()),
        CorePortState::LessThan(n) => Ok(PyPortStateNumeric {
            kind: "less_than".to_string(),
            value: *n,
        }
        .into_pyobject(py)?
        .unbind()
        .into_any()),
        CorePortState::GreaterThan(n) => Ok(PyPortStateNumeric {
            kind: "greater_than".to_string(),
            value: *n,
        }
        .into_pyobject(py)?
        .unbind()
        .into_any()),
        CorePortState::EqualsOrLessThan(n) => Ok(PyPortStateNumeric {
            kind: "equals_or_less_than".to_string(),
            value: *n,
        }
        .into_pyobject(py)?
        .unbind()
        .into_any()),
        CorePortState::EqualsOrGreaterThan(n) => Ok(PyPortStateNumeric {
            kind: "equals_or_greater_than".to_string(),
            value: *n,
        }
        .into_pyobject(py)?
        .unbind()
        .into_any()),
    }
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
                if obj.extract::<PortSlotSpec>().is_ok() {
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
            CorePortSlotSpec::Infinite => Ok(PortSlotSpec::Infinite
                .into_pyobject(py)?
                .unbind()
                .into_any()),
            CorePortSlotSpec::Finite(n) => Ok(PyPortSlotSpecFinite { capacity: *n }
                .into_pyobject(py)?
                .unbind()
                .into_any()),
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

/// A condition that defines when packets can trigger an epoch or be sent.
#[pyclass]
#[derive(Clone)]
pub struct SalvoCondition {
    max_salvos_internal: CoreMaxSalvos,
    ports_internal: HashMap<String, CorePacketCount>,
    #[pyo3(get)]
    pub term: SalvoConditionTerm,
}

#[pymethods]
impl SalvoCondition {
    /// Create a new SalvoCondition.
    ///
    /// `max_salvos` can be:
    /// - MaxSalvos.Infinite or MaxSalvos.infinite()
    /// - MaxSalvos.finite(n) (returns MaxSalvosFinite)
    ///
    /// `ports` can be:
    /// - A single port name (str) - defaults to PacketCount.All
    /// - A list of port names - each defaults to PacketCount.All
    /// - A dict mapping port names to PacketCount values
    #[new]
    #[pyo3(signature = (max_salvos, ports, term))]
    fn new(
        max_salvos: &Bound<'_, PyAny>,
        ports: &Bound<'_, PyAny>,
        term: SalvoConditionTerm,
    ) -> PyResult<Self> {
        let core_max_salvos = extract_max_salvos(max_salvos)?;
        let ports_map = extract_ports_map(ports)?;
        Ok(SalvoCondition {
            max_salvos_internal: core_max_salvos,
            ports_internal: ports_map,
            term,
        })
    }

    /// Get the max_salvos value.
    #[getter]
    fn max_salvos(&self, py: Python<'_>) -> PyResult<PyObject> {
        match &self.max_salvos_internal {
            CoreMaxSalvos::Infinite => {
                Ok(MaxSalvos::Infinite.into_pyobject(py)?.unbind().into_any())
            }
            CoreMaxSalvos::Finite(n) => Ok(PyMaxSalvosFinite { max: *n }
                .into_pyobject(py)?
                .unbind()
                .into_any()),
        }
    }

    /// Get the ports as a dict mapping port names to their packet counts.
    #[getter]
    fn ports(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = pyo3::types::PyDict::new(py);
        for (port_name, packet_count) in &self.ports_internal {
            let py_count: PyObject = match packet_count {
                CorePacketCount::All => PacketCount::All.into_pyobject(py)?.unbind().into_any(),
                CorePacketCount::Count(n) => PyPacketCountN { count: *n }
                    .into_pyobject(py)?
                    .unbind()
                    .into_any(),
            };
            dict.set_item(port_name, py_count)?;
        }
        Ok(dict.into())
    }

    fn __repr__(&self) -> String {
        let max_salvos_repr = match &self.max_salvos_internal {
            CoreMaxSalvos::Infinite => "MaxSalvos.Infinite".to_string(),
            CoreMaxSalvos::Finite(n) => format!("MaxSalvos.finite({})", n),
        };
        let ports_repr: Vec<String> = self
            .ports_internal
            .iter()
            .map(|(k, v)| {
                let v_str = match v {
                    CorePacketCount::All => "PacketCount.All".to_string(),
                    CorePacketCount::Count(n) => format!("PacketCount.count({})", n),
                };
                format!("{:?}: {}", k, v_str)
            })
            .collect();
        format!(
            "SalvoCondition(max_salvos={}, ports={{{}}}, term={})",
            max_salvos_repr,
            ports_repr.join(", "),
            self.term.__repr__()
        )
    }
}

/// Extract ports map from various Python input types.
fn extract_ports_map(obj: &Bound<'_, PyAny>) -> PyResult<HashMap<String, CorePacketCount>> {
    // Try as a string (single port name)
    if let Ok(s) = obj.extract::<String>() {
        let mut map = HashMap::new();
        map.insert(s, CorePacketCount::All);
        return Ok(map);
    }

    // Try as a list of strings
    if let Ok(list) = obj.extract::<Vec<String>>() {
        let map: HashMap<String, CorePacketCount> = list
            .into_iter()
            .map(|s| (s, CorePacketCount::All))
            .collect();
        return Ok(map);
    }

    // Try as a dict
    if let Ok(dict) = obj.downcast::<pyo3::types::PyDict>() {
        let mut map = HashMap::new();
        for (key, value) in dict.iter() {
            let port_name: String = key.extract()?;
            let packet_count = extract_packet_count(&value)?;
            map.insert(port_name, packet_count);
        }
        return Ok(map);
    }

    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "ports must be a str, list of str, or dict mapping str to PacketCount",
    ))
}

/// Extract a PacketCount from a Python object.
fn extract_packet_count(obj: &Bound<'_, PyAny>) -> PyResult<CorePacketCount> {
    // Try PacketCount.All
    if let Ok(pc) = obj.extract::<PacketCount>() {
        return Ok(pc.to_core());
    }
    // Try PacketCount.count(n)
    if let Ok(pcn) = obj.extract::<PyPacketCountN>() {
        return Ok(pcn.to_core());
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "Expected PacketCount or PacketCountN",
    ))
}

/// Extract a MaxSalvos from a Python object.
fn extract_max_salvos(obj: &Bound<'_, PyAny>) -> PyResult<CoreMaxSalvos> {
    // Try MaxSalvos.Infinite
    if let Ok(ms) = obj.extract::<MaxSalvos>() {
        return Ok(ms.to_core());
    }
    // Try MaxSalvos.finite(n)
    if let Ok(msf) = obj.extract::<PyMaxSalvosFinite>() {
        return Ok(msf.to_core());
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "Expected MaxSalvos or MaxSalvosFinite",
    ))
}

impl SalvoCondition {
    pub fn to_core(&self) -> CoreSalvoCondition {
        CoreSalvoCondition {
            max_salvos: self.max_salvos_internal.clone(),
            ports: self.ports_internal.clone(),
            term: self.term.to_core(),
        }
    }

    pub fn from_core(sc: &CoreSalvoCondition) -> Self {
        SalvoCondition {
            max_salvos_internal: sc.max_salvos.clone(),
            ports_internal: sc.ports.clone(),
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
        // Edge is now directly the core type (re-exported from core)
        Graph {
            inner: CoreGraph::new(core_nodes, edges),
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
            // Edge is now directly the core type (re-exported from core)
            list.append(edge.clone())?;
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
    m.add_class::<PacketCount>()?;
    m.add_class::<PyPacketCountN>()?;
    m.add_class::<MaxSalvos>()?;
    m.add_class::<PyMaxSalvosFinite>()?;
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
