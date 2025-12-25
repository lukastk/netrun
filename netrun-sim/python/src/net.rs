use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::errors::net_action_error_to_py_err;
use crate::graph::{EdgeRef, Graph};

use netrun_sim::net::{
    Epoch as CoreEpoch, EpochState as CoreEpochState, Net as CoreNet, NetAction as CoreNetAction,
    NetActionResponse as CoreNetActionResponse, NetActionResponseData as CoreNetActionResponseData,
    NetEvent as CoreNetEvent, Packet as CorePacket, PacketLocation as CorePacketLocation,
    Salvo as CoreSalvo,
};

/// Convert Rust ULID to Python ulid.ULID
fn ulid_to_python(py: Python<'_>, ulid: &ulid::Ulid) -> PyResult<PyObject> {
    let ulid_module = py.import("ulid")?;
    let ulid_class = ulid_module.getattr("ULID")?;
    // Use from_str class method which accepts the string representation
    let from_str = ulid_class.getattr("from_str")?;
    let result = from_str.call1((ulid.to_string(),))?;
    Ok(result.unbind())
}

/// Convert Python ulid.ULID or string to Rust ULID
fn python_to_ulid(obj: &Bound<'_, PyAny>) -> PyResult<ulid::Ulid> {
    // Try to get as string first
    if let Ok(s) = obj.extract::<String>() {
        return ulid::Ulid::from_string(&s).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid ULID string: {}", e))
        });
    }
    // Try to call str() on the object (for ulid.ULID objects)
    let s = obj.str()?.to_string();
    ulid::Ulid::from_string(&s).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid ULID: {}", e))
    })
}

/// Where a packet is located in the network.
#[pyclass]
#[derive(Clone)]
pub struct PacketLocation {
    inner: CorePacketLocation,
}

#[pymethods]
impl PacketLocation {
    /// Create a Node location (inside an epoch).
    #[staticmethod]
    fn node(epoch_id: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(PacketLocation {
            inner: CorePacketLocation::Node(python_to_ulid(epoch_id)?),
        })
    }

    /// Create an InputPort location.
    #[staticmethod]
    fn input_port(node_name: String, port_name: String) -> Self {
        PacketLocation {
            inner: CorePacketLocation::InputPort(node_name, port_name),
        }
    }

    /// Create an OutputPort location.
    #[staticmethod]
    fn output_port(epoch_id: &Bound<'_, PyAny>, port_name: String) -> PyResult<Self> {
        Ok(PacketLocation {
            inner: CorePacketLocation::OutputPort(python_to_ulid(epoch_id)?, port_name),
        })
    }

    /// Create an Edge location.
    #[staticmethod]
    fn edge(edge_ref: EdgeRef) -> Self {
        PacketLocation {
            inner: CorePacketLocation::Edge(edge_ref.to_core()),
        }
    }

    /// Create an OutsideNet location.
    #[staticmethod]
    fn outside_net() -> Self {
        PacketLocation {
            inner: CorePacketLocation::OutsideNet,
        }
    }

    /// Get the location kind.
    #[getter]
    fn kind(&self) -> String {
        match &self.inner {
            CorePacketLocation::Node(_) => "Node".to_string(),
            CorePacketLocation::InputPort(_, _) => "InputPort".to_string(),
            CorePacketLocation::OutputPort(_, _) => "OutputPort".to_string(),
            CorePacketLocation::Edge(_) => "Edge".to_string(),
            CorePacketLocation::OutsideNet => "OutsideNet".to_string(),
        }
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            CorePacketLocation::Node(id) => format!("PacketLocation.node('{}')", id),
            CorePacketLocation::InputPort(node, port) => {
                format!("PacketLocation.input_port({:?}, {:?})", node, port)
            }
            CorePacketLocation::OutputPort(id, port) => {
                format!("PacketLocation.output_port('{}', {:?})", id, port)
            }
            CorePacketLocation::Edge(er) => format!("PacketLocation.edge({})", er),
            CorePacketLocation::OutsideNet => "PacketLocation.outside_net()".to_string(),
        }
    }
}

impl PacketLocation {
    pub fn to_core(&self) -> CorePacketLocation {
        self.inner.clone()
    }

    pub fn from_core(loc: &CorePacketLocation) -> Self {
        PacketLocation { inner: loc.clone() }
    }
}

/// Epoch lifecycle state.
#[pyclass(eq, eq_int)]
#[derive(Clone, PartialEq)]
pub enum EpochState {
    Startable,
    Running,
    Finished,
}

#[pymethods]
impl EpochState {
    fn __repr__(&self) -> String {
        match self {
            EpochState::Startable => "EpochState.Startable".to_string(),
            EpochState::Running => "EpochState.Running".to_string(),
            EpochState::Finished => "EpochState.Finished".to_string(),
        }
    }
}

impl EpochState {
    pub fn from_core(state: &CoreEpochState) -> Self {
        match state {
            CoreEpochState::Startable => EpochState::Startable,
            CoreEpochState::Running => EpochState::Running,
            CoreEpochState::Finished => EpochState::Finished,
        }
    }
}

/// A packet in the network.
#[pyclass]
#[derive(Clone)]
pub struct Packet {
    #[pyo3(get)]
    pub id: String, // ULID as string for simplicity
    #[pyo3(get)]
    pub location: PacketLocation,
}

#[pymethods]
impl Packet {
    fn get_id(&self, py: Python<'_>) -> PyResult<PyObject> {
        let ulid = ulid::Ulid::from_string(&self.id).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid ULID: {}", e))
        })?;
        ulid_to_python(py, &ulid)
    }

    fn __repr__(&self) -> String {
        format!("Packet(id='{}', location={})", self.id, self.location.__repr__())
    }
}

impl Packet {
    pub fn from_core(packet: &CorePacket) -> Self {
        Packet {
            id: packet.id.to_string(),
            location: PacketLocation::from_core(&packet.location),
        }
    }
}

/// A collection of packets entering or exiting a node.
#[pyclass]
#[derive(Clone)]
pub struct Salvo {
    #[pyo3(get)]
    pub salvo_condition: String,
    #[pyo3(get)]
    pub packets: Vec<(String, String)>, // (port_name, packet_id)
}

#[pymethods]
impl Salvo {
    #[new]
    fn new(salvo_condition: String, packets: Vec<(String, String)>) -> Self {
        Salvo {
            salvo_condition,
            packets,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Salvo(salvo_condition={:?}, packets={:?})",
            self.salvo_condition, self.packets
        )
    }
}

impl Salvo {
    pub fn to_core(&self) -> CoreSalvo {
        CoreSalvo {
            salvo_condition: self.salvo_condition.clone(),
            packets: self
                .packets
                .iter()
                .map(|(port, pid)| {
                    let ulid = ulid::Ulid::from_string(pid).expect("Invalid ULID in salvo");
                    (port.clone(), ulid)
                })
                .collect(),
        }
    }

    pub fn from_core(salvo: &CoreSalvo) -> Self {
        Salvo {
            salvo_condition: salvo.salvo_condition.clone(),
            packets: salvo
                .packets
                .iter()
                .map(|(port, pid)| (port.clone(), pid.to_string()))
                .collect(),
        }
    }
}

/// An execution instance of a node.
#[pyclass]
#[derive(Clone)]
pub struct Epoch {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub node_name: String,
    #[pyo3(get)]
    pub in_salvo: Salvo,
    #[pyo3(get)]
    pub out_salvos: Vec<Salvo>,
    #[pyo3(get)]
    pub state: EpochState,
}

#[pymethods]
impl Epoch {
    fn get_id(&self, py: Python<'_>) -> PyResult<PyObject> {
        let ulid = ulid::Ulid::from_string(&self.id).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid ULID: {}", e))
        })?;
        ulid_to_python(py, &ulid)
    }

    fn start_time(&self) -> PyResult<u64> {
        let ulid = ulid::Ulid::from_string(&self.id).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid ULID: {}", e))
        })?;
        Ok(ulid.timestamp_ms())
    }

    fn __repr__(&self) -> String {
        format!(
            "Epoch(id='{}', node_name={:?}, state={:?})",
            self.id,
            self.node_name,
            self.state.__repr__()
        )
    }
}

impl Epoch {
    pub fn from_core(epoch: &CoreEpoch) -> Self {
        Epoch {
            id: epoch.id.to_string(),
            node_name: epoch.node_name.clone(),
            in_salvo: Salvo::from_core(&epoch.in_salvo),
            out_salvos: epoch.out_salvos.iter().map(Salvo::from_core).collect(),
            state: EpochState::from_core(&epoch.state),
        }
    }
}

/// An action to perform on the network.
#[pyclass]
#[derive(Clone)]
pub struct NetAction {
    inner: NetActionKind,
}

#[derive(Clone)]
enum NetActionKind {
    RunNetUntilBlocked,
    CreatePacket(Option<String>),
    ConsumePacket(String),
    StartEpoch(String),
    FinishEpoch(String),
    CancelEpoch(String),
    CreateAndStartEpoch(String, Salvo),
    LoadPacketIntoOutputPort(String, String),
    SendOutputSalvo(String, String),
    TransportPacketToLocation(String, PacketLocation),
}

#[pymethods]
impl NetAction {
    /// Run automatic packet flow until blocked.
    #[staticmethod]
    fn run_net_until_blocked() -> Self {
        NetAction {
            inner: NetActionKind::RunNetUntilBlocked,
        }
    }

    /// Create a new packet, optionally inside an epoch.
    #[staticmethod]
    #[pyo3(signature = (epoch_id=None))]
    fn create_packet(epoch_id: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let id_str = match epoch_id {
            Some(obj) => Some(obj.str()?.to_string()),
            None => None,
        };
        Ok(NetAction {
            inner: NetActionKind::CreatePacket(id_str),
        })
    }

    /// Remove a packet from the network.
    #[staticmethod]
    fn consume_packet(packet_id: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(NetAction {
            inner: NetActionKind::ConsumePacket(packet_id.str()?.to_string()),
        })
    }

    /// Transition a startable epoch to running.
    #[staticmethod]
    fn start_epoch(epoch_id: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(NetAction {
            inner: NetActionKind::StartEpoch(epoch_id.str()?.to_string()),
        })
    }

    /// Complete a running epoch.
    #[staticmethod]
    fn finish_epoch(epoch_id: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(NetAction {
            inner: NetActionKind::FinishEpoch(epoch_id.str()?.to_string()),
        })
    }

    /// Cancel an epoch and destroy packets.
    #[staticmethod]
    fn cancel_epoch(epoch_id: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(NetAction {
            inner: NetActionKind::CancelEpoch(epoch_id.str()?.to_string()),
        })
    }

    /// Manually create and start an epoch.
    #[staticmethod]
    fn create_and_start_epoch(node_name: String, salvo: Salvo) -> Self {
        NetAction {
            inner: NetActionKind::CreateAndStartEpoch(node_name, salvo),
        }
    }

    /// Move a packet to an output port.
    #[staticmethod]
    fn load_packet_into_output_port(
        packet_id: &Bound<'_, PyAny>,
        port_name: String,
    ) -> PyResult<Self> {
        Ok(NetAction {
            inner: NetActionKind::LoadPacketIntoOutputPort(packet_id.str()?.to_string(), port_name),
        })
    }

    /// Send packets from output ports onto edges.
    #[staticmethod]
    fn send_output_salvo(
        epoch_id: &Bound<'_, PyAny>,
        salvo_condition_name: String,
    ) -> PyResult<Self> {
        Ok(NetAction {
            inner: NetActionKind::SendOutputSalvo(epoch_id.str()?.to_string(), salvo_condition_name),
        })
    }

    /// Transport a packet to a new location.
    #[staticmethod]
    fn transport_packet_to_location(
        packet_id: &Bound<'_, PyAny>,
        destination: PacketLocation,
    ) -> PyResult<Self> {
        Ok(NetAction {
            inner: NetActionKind::TransportPacketToLocation(
                packet_id.str()?.to_string(),
                destination,
            ),
        })
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            NetActionKind::RunNetUntilBlocked => "NetAction.run_net_until_blocked()".to_string(),
            NetActionKind::CreatePacket(id) => match id {
                Some(id) => format!("NetAction.create_packet('{}')", id),
                None => "NetAction.create_packet()".to_string(),
            },
            NetActionKind::ConsumePacket(id) => format!("NetAction.consume_packet('{}')", id),
            NetActionKind::StartEpoch(id) => format!("NetAction.start_epoch('{}')", id),
            NetActionKind::FinishEpoch(id) => format!("NetAction.finish_epoch('{}')", id),
            NetActionKind::CancelEpoch(id) => format!("NetAction.cancel_epoch('{}')", id),
            NetActionKind::CreateAndStartEpoch(name, _) => {
                format!("NetAction.create_and_start_epoch({:?}, ...)", name)
            }
            NetActionKind::LoadPacketIntoOutputPort(pid, port) => {
                format!("NetAction.load_packet_into_output_port('{}', {:?})", pid, port)
            }
            NetActionKind::SendOutputSalvo(eid, cond) => {
                format!("NetAction.send_output_salvo('{}', {:?})", eid, cond)
            }
            NetActionKind::TransportPacketToLocation(pid, loc) => {
                format!(
                    "NetAction.transport_packet_to_location('{}', {})",
                    pid,
                    loc.__repr__()
                )
            }
        }
    }
}

impl NetAction {
    pub fn to_core(&self) -> PyResult<CoreNetAction> {
        match &self.inner {
            NetActionKind::RunNetUntilBlocked => Ok(CoreNetAction::RunNetUntilBlocked),
            NetActionKind::CreatePacket(opt_id) => {
                let ulid_opt = match opt_id {
                    Some(s) => Some(ulid::Ulid::from_string(s).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                            "Invalid ULID: {}",
                            e
                        ))
                    })?),
                    None => None,
                };
                Ok(CoreNetAction::CreatePacket(ulid_opt))
            }
            NetActionKind::ConsumePacket(id) => {
                let ulid = ulid::Ulid::from_string(id).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid ULID: {}", e))
                })?;
                Ok(CoreNetAction::ConsumePacket(ulid))
            }
            NetActionKind::StartEpoch(id) => {
                let ulid = ulid::Ulid::from_string(id).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid ULID: {}", e))
                })?;
                Ok(CoreNetAction::StartEpoch(ulid))
            }
            NetActionKind::FinishEpoch(id) => {
                let ulid = ulid::Ulid::from_string(id).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid ULID: {}", e))
                })?;
                Ok(CoreNetAction::FinishEpoch(ulid))
            }
            NetActionKind::CancelEpoch(id) => {
                let ulid = ulid::Ulid::from_string(id).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid ULID: {}", e))
                })?;
                Ok(CoreNetAction::CancelEpoch(ulid))
            }
            NetActionKind::CreateAndStartEpoch(name, salvo) => {
                Ok(CoreNetAction::CreateAndStartEpoch(name.clone(), salvo.to_core()))
            }
            NetActionKind::LoadPacketIntoOutputPort(pid, port) => {
                let ulid = ulid::Ulid::from_string(pid).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid ULID: {}", e))
                })?;
                Ok(CoreNetAction::LoadPacketIntoOutputPort(ulid, port.clone()))
            }
            NetActionKind::SendOutputSalvo(eid, cond) => {
                let ulid = ulid::Ulid::from_string(eid).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid ULID: {}", e))
                })?;
                Ok(CoreNetAction::SendOutputSalvo(ulid, cond.clone()))
            }
            NetActionKind::TransportPacketToLocation(pid, loc) => {
                let ulid = ulid::Ulid::from_string(pid).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid ULID: {}", e))
                })?;
                Ok(CoreNetAction::TransportPacketToLocation(ulid, loc.to_core()))
            }
        }
    }
}

/// An event that occurred during a network action.
#[pyclass]
#[derive(Clone)]
pub struct NetEvent {
    inner: NetEventKind,
}

#[derive(Clone)]
enum NetEventKind {
    PacketCreated(i128, String),
    PacketConsumed(i128, String),
    EpochCreated(i128, String),
    EpochStarted(i128, String),
    EpochFinished(i128, String),
    EpochCancelled(i128, String),
    PacketMoved(i128, String, PacketLocation),
    InputSalvoTriggered(i128, String, String),
    OutputSalvoTriggered(i128, String, String),
}

#[pymethods]
impl NetEvent {
    #[getter]
    fn kind(&self) -> String {
        match &self.inner {
            NetEventKind::PacketCreated(_, _) => "PacketCreated".to_string(),
            NetEventKind::PacketConsumed(_, _) => "PacketConsumed".to_string(),
            NetEventKind::EpochCreated(_, _) => "EpochCreated".to_string(),
            NetEventKind::EpochStarted(_, _) => "EpochStarted".to_string(),
            NetEventKind::EpochFinished(_, _) => "EpochFinished".to_string(),
            NetEventKind::EpochCancelled(_, _) => "EpochCancelled".to_string(),
            NetEventKind::PacketMoved(_, _, _) => "PacketMoved".to_string(),
            NetEventKind::InputSalvoTriggered(_, _, _) => "InputSalvoTriggered".to_string(),
            NetEventKind::OutputSalvoTriggered(_, _, _) => "OutputSalvoTriggered".to_string(),
        }
    }

    #[getter]
    fn timestamp(&self) -> i128 {
        match &self.inner {
            NetEventKind::PacketCreated(ts, _)
            | NetEventKind::PacketConsumed(ts, _)
            | NetEventKind::EpochCreated(ts, _)
            | NetEventKind::EpochStarted(ts, _)
            | NetEventKind::EpochFinished(ts, _)
            | NetEventKind::EpochCancelled(ts, _)
            | NetEventKind::PacketMoved(ts, _, _)
            | NetEventKind::InputSalvoTriggered(ts, _, _)
            | NetEventKind::OutputSalvoTriggered(ts, _, _) => *ts,
        }
    }

    #[getter]
    fn packet_id(&self) -> Option<String> {
        match &self.inner {
            NetEventKind::PacketCreated(_, id)
            | NetEventKind::PacketConsumed(_, id)
            | NetEventKind::PacketMoved(_, id, _) => Some(id.clone()),
            _ => None,
        }
    }

    #[getter]
    fn epoch_id(&self) -> Option<String> {
        match &self.inner {
            NetEventKind::EpochCreated(_, id)
            | NetEventKind::EpochStarted(_, id)
            | NetEventKind::EpochFinished(_, id)
            | NetEventKind::EpochCancelled(_, id)
            | NetEventKind::InputSalvoTriggered(_, id, _)
            | NetEventKind::OutputSalvoTriggered(_, id, _) => Some(id.clone()),
            _ => None,
        }
    }

    #[getter]
    fn location(&self) -> Option<PacketLocation> {
        match &self.inner {
            NetEventKind::PacketMoved(_, _, loc) => Some(loc.clone()),
            _ => None,
        }
    }

    #[getter]
    fn salvo_condition(&self) -> Option<String> {
        match &self.inner {
            NetEventKind::InputSalvoTriggered(_, _, cond)
            | NetEventKind::OutputSalvoTriggered(_, _, cond) => Some(cond.clone()),
            _ => None,
        }
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            NetEventKind::PacketCreated(ts, id) => {
                format!("NetEvent.PacketCreated(ts={}, packet_id='{}')", ts, id)
            }
            NetEventKind::PacketConsumed(ts, id) => {
                format!("NetEvent.PacketConsumed(ts={}, packet_id='{}')", ts, id)
            }
            NetEventKind::EpochCreated(ts, id) => {
                format!("NetEvent.EpochCreated(ts={}, epoch_id='{}')", ts, id)
            }
            NetEventKind::EpochStarted(ts, id) => {
                format!("NetEvent.EpochStarted(ts={}, epoch_id='{}')", ts, id)
            }
            NetEventKind::EpochFinished(ts, id) => {
                format!("NetEvent.EpochFinished(ts={}, epoch_id='{}')", ts, id)
            }
            NetEventKind::EpochCancelled(ts, id) => {
                format!("NetEvent.EpochCancelled(ts={}, epoch_id='{}')", ts, id)
            }
            NetEventKind::PacketMoved(ts, id, loc) => {
                format!(
                    "NetEvent.PacketMoved(ts={}, packet_id='{}', location={})",
                    ts,
                    id,
                    loc.__repr__()
                )
            }
            NetEventKind::InputSalvoTriggered(ts, eid, cond) => {
                format!(
                    "NetEvent.InputSalvoTriggered(ts={}, epoch_id='{}', condition={:?})",
                    ts, eid, cond
                )
            }
            NetEventKind::OutputSalvoTriggered(ts, eid, cond) => {
                format!(
                    "NetEvent.OutputSalvoTriggered(ts={}, epoch_id='{}', condition={:?})",
                    ts, eid, cond
                )
            }
        }
    }
}

impl NetEvent {
    pub fn from_core(event: &CoreNetEvent) -> Self {
        let inner = match event {
            CoreNetEvent::PacketCreated(ts, id) => NetEventKind::PacketCreated(*ts, id.to_string()),
            CoreNetEvent::PacketConsumed(ts, id) => {
                NetEventKind::PacketConsumed(*ts, id.to_string())
            }
            CoreNetEvent::EpochCreated(ts, id) => NetEventKind::EpochCreated(*ts, id.to_string()),
            CoreNetEvent::EpochStarted(ts, id) => NetEventKind::EpochStarted(*ts, id.to_string()),
            CoreNetEvent::EpochFinished(ts, id) => NetEventKind::EpochFinished(*ts, id.to_string()),
            CoreNetEvent::EpochCancelled(ts, id) => {
                NetEventKind::EpochCancelled(*ts, id.to_string())
            }
            CoreNetEvent::PacketMoved(ts, id, loc) => {
                NetEventKind::PacketMoved(*ts, id.to_string(), PacketLocation::from_core(loc))
            }
            CoreNetEvent::InputSalvoTriggered(ts, eid, cond) => {
                NetEventKind::InputSalvoTriggered(*ts, eid.to_string(), cond.clone())
            }
            CoreNetEvent::OutputSalvoTriggered(ts, eid, cond) => {
                NetEventKind::OutputSalvoTriggered(*ts, eid.to_string(), cond.clone())
            }
        };
        NetEvent { inner }
    }
}

/// Response data from a successful action.
#[pyclass]
#[derive(Clone)]
pub enum NetActionResponseData {
    #[pyo3(constructor = (packet_id))]
    Packet { packet_id: String },
    #[pyo3(constructor = (epoch))]
    StartedEpoch { epoch: Epoch },
    #[pyo3(constructor = (epoch))]
    FinishedEpoch { epoch: Epoch },
    #[pyo3(constructor = (epoch, destroyed_packets))]
    CancelledEpoch {
        epoch: Epoch,
        destroyed_packets: Vec<String>,
    },
    #[pyo3(constructor = ())]
    Empty {},
}

/// The runtime state of a flow-based network.
#[pyclass]
pub struct Net {
    inner: CoreNet,
}

#[pymethods]
impl Net {
    #[new]
    fn new(graph: &Graph) -> Self {
        // Clone the inner graph from the Python Graph wrapper
        let core_graph = graph.inner.clone();
        Net {
            inner: CoreNet::new(core_graph),
        }
    }

    /// Perform an action on the network.
    /// Returns (response_data, events) tuple on success.
    /// Raises an exception on error.
    fn do_action(&mut self, py: Python<'_>, action: &NetAction) -> PyResult<(NetActionResponseData, Py<PyList>)> {
        let core_action = action.to_core()?;
        let response = self.inner.do_action(&core_action);

        match response {
            CoreNetActionResponse::Success(data, events) => {
                let py_data = match data {
                    CoreNetActionResponseData::Packet(id) => NetActionResponseData::Packet {
                        packet_id: id.to_string(),
                    },
                    CoreNetActionResponseData::StartedEpoch(epoch) => {
                        NetActionResponseData::StartedEpoch {
                            epoch: Epoch::from_core(&epoch),
                        }
                    }
                    CoreNetActionResponseData::FinishedEpoch(epoch) => {
                        NetActionResponseData::FinishedEpoch {
                            epoch: Epoch::from_core(&epoch),
                        }
                    }
                    CoreNetActionResponseData::CancelledEpoch(epoch, destroyed) => {
                        NetActionResponseData::CancelledEpoch {
                            epoch: Epoch::from_core(&epoch),
                            destroyed_packets: destroyed.iter().map(|id| id.to_string()).collect(),
                        }
                    }
                    CoreNetActionResponseData::None => NetActionResponseData::Empty {},
                };

                let events_list = PyList::empty(py);
                for event in events {
                    events_list.append(NetEvent::from_core(&event))?;
                }

                Ok((py_data, events_list.unbind()))
            }
            CoreNetActionResponse::Error(err) => Err(net_action_error_to_py_err(err)),
        }
    }

    /// Get the number of packets at a location.
    fn packet_count_at(&self, location: &PacketLocation) -> usize {
        self.inner.packet_count_at(&location.to_core())
    }

    /// Get all packet IDs at a location.
    fn get_packets_at_location(&self, py: Python<'_>, location: &PacketLocation) -> PyResult<Py<PyList>> {
        let packets = self.inner.get_packets_at_location(&location.to_core());
        let list = PyList::empty(py);
        for id in packets {
            list.append(ulid_to_python(py, &id)?)?;
        }
        Ok(list.unbind())
    }

    /// Get an epoch by ID.
    fn get_epoch(&self, epoch_id: &Bound<'_, PyAny>) -> PyResult<Option<Epoch>> {
        let ulid = python_to_ulid(epoch_id)?;
        Ok(self.inner.get_epoch(&ulid).map(Epoch::from_core))
    }

    /// Get all startable epoch IDs.
    fn get_startable_epochs(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let epochs = self.inner.get_startable_epochs();
        let list = PyList::empty(py);
        for id in epochs {
            list.append(ulid_to_python(py, &id)?)?;
        }
        Ok(list.unbind())
    }

    /// Get a packet by ID.
    fn get_packet(&self, packet_id: &Bound<'_, PyAny>) -> PyResult<Option<Packet>> {
        let ulid = python_to_ulid(packet_id)?;
        Ok(self.inner.get_packet(&ulid).map(Packet::from_core))
    }

    /// Get the underlying graph.
    #[getter]
    fn graph(&self) -> Graph {
        Graph::from_core(self.inner.graph.clone())
    }

    fn __repr__(&self) -> String {
        format!(
            "Net(graph=Graph(nodes={}, edges={}))",
            self.inner.graph.nodes().len(),
            self.inner.graph.edges().len()
        )
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PacketLocation>()?;
    m.add_class::<EpochState>()?;
    m.add_class::<Packet>()?;
    m.add_class::<Salvo>()?;
    m.add_class::<Epoch>()?;
    m.add_class::<NetAction>()?;
    m.add_class::<NetEvent>()?;
    m.add_class::<NetActionResponseData>()?;
    m.add_class::<Net>()?;
    Ok(())
}
