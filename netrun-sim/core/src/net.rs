//! Runtime state and operations for flow-based development networks.
//!
//! This module provides the [`NetSim`] type which tracks the runtime state of a network,
//! including packet locations, epoch lifecycles, and provides actions to control packet flow.
//!
//! All mutations to the network state go through [`NetSim::do_action`] which accepts a
//! [`NetAction`] and returns a [`NetActionResponse`] containing any events that occurred.

use crate::_utils::get_utc_now;
use crate::graph::{
    Edge, Graph, MaxSalvos, NodeName, PacketCount, Port, PortName, PortRef, PortSlotSpec, PortType,
    SalvoConditionName, SalvoConditionTerm, evaluate_salvo_condition,
};
use indexmap::IndexSet;
use std::collections::{HashMap, HashSet};
use ulid::Ulid;

/// Unique identifier for a packet (ULID).
pub type PacketID = Ulid;

/// Unique identifier for an epoch (ULID).
pub type EpochID = Ulid;

/// Where a packet is located in the network.
///
/// Packets move through these locations as they flow through the network:
/// - Start outside the net or get created inside an epoch
/// - Move to edges, then to input ports
/// - Get consumed into epochs via salvo conditions
/// - Can be loaded into output ports and sent back to edges
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum PacketLocation {
    /// Inside an epoch (either startable or running).
    Node(EpochID),
    /// Waiting at a node's input port.
    InputPort(NodeName, PortName),
    /// Loaded into an epoch's output port, ready to be sent.
    OutputPort(EpochID, PortName),
    /// In transit on an edge between nodes.
    Edge(Edge),
    /// External to the network (not yet injected or already extracted).
    OutsideNet,
}

/// A unit that flows through the network.
#[derive(Debug)]
pub struct Packet {
    /// Unique identifier for this packet.
    pub id: PacketID,
    /// Current location of this packet.
    pub location: PacketLocation,
}

/// A collection of packets that enter or exit a node together.
///
/// Salvos are created when salvo conditions are satisfied:
/// - Input salvos are created when packets at input ports trigger an epoch
/// - Output salvos are created when packets at output ports are sent out
#[derive(Debug, Clone)]
pub struct Salvo {
    /// The name of the salvo condition that was triggered.
    pub salvo_condition: SalvoConditionName,
    /// The packets in this salvo, paired with their port names.
    pub packets: Vec<(PortName, PacketID)>,
}

/// The lifecycle state of an epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "python", pyo3::pyclass(eq, eq_int))]
pub enum EpochState {
    /// Epoch is created but not yet started. External code must call StartEpoch.
    Startable,
    /// Epoch is actively running. Packets can be created, loaded, and sent.
    Running,
    /// Epoch has completed. No further operations are allowed.
    Finished,
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl EpochState {
    fn __repr__(&self) -> String {
        match self {
            EpochState::Startable => "EpochState.Startable".to_string(),
            EpochState::Running => "EpochState.Running".to_string(),
            EpochState::Finished => "EpochState.Finished".to_string(),
        }
    }
}

/// An execution instance of a node.
///
/// A single node can have multiple simultaneous epochs. Each epoch tracks
/// which packets entered (in_salvo), which have been sent out (out_salvos),
/// and its current lifecycle state.
#[derive(Debug, Clone)]
pub struct Epoch {
    /// Unique identifier for this epoch.
    pub id: EpochID,
    /// The node this epoch is executing on.
    pub node_name: NodeName,
    /// The salvo of packets that triggered this epoch.
    pub in_salvo: Salvo,
    /// Salvos that have been sent out from this epoch.
    pub out_salvos: Vec<Salvo>,
    /// Current lifecycle state.
    pub state: EpochState,
}

impl Epoch {
    /// Returns the timestamp when this epoch was created (milliseconds since Unix epoch).
    pub fn start_time(&self) -> u64 {
        self.id.timestamp_ms()
    }
}

/// Timestamp in milliseconds (UTC).
pub type EventUTC = i128;

/// An action that can be performed on the network.
///
/// All mutations to [`NetSim`] state must go through these actions via [`NetSim::do_action`].
/// This ensures all operations are tracked and produce appropriate events.
#[derive(Debug, Clone)]
pub enum NetAction {
    /// Execute one step of automatic packet flow.
    ///
    /// A single step performs one full iteration of the flow loop:
    /// 1. Move all movable packets from edges to input ports
    /// 2. Trigger all satisfied input salvo conditions
    ///
    /// Returns `StepResult { made_progress }` indicating whether any progress was made.
    /// If no progress was made, the network is "blocked".
    RunStep,
    /// Create a new packet, optionally inside an epoch.
    /// If `None`, packet is created outside the network.
    CreatePacket(Option<EpochID>),
    /// Consume a packet (normal removal from the network).
    ConsumePacket(PacketID),
    /// Destroy a packet (abnormal removal, e.g., due to error or cancellation).
    DestroyPacket(PacketID),
    /// Transition a startable epoch to running state.
    StartEpoch(EpochID),
    /// Complete a running epoch. Fails if epoch still contains packets.
    FinishEpoch(EpochID),
    /// Cancel an epoch and destroy all packets inside it.
    CancelEpoch(EpochID),
    /// Manually create an epoch with specified packets.
    /// Bypasses the normal salvo condition triggering mechanism.
    /// The epoch is created in Startable state - call StartEpoch to begin execution.
    CreateEpoch(NodeName, Salvo),
    /// Move a packet from inside an epoch to one of its output ports.
    LoadPacketIntoOutputPort(PacketID, PortName),
    /// Send packets from output ports onto edges according to a salvo condition.
    SendOutputSalvo(EpochID, SalvoConditionName),
    /// Transport a packet to a new location.
    /// Restrictions:
    /// - Cannot move packets into or out of Running epochs (only Startable allowed)
    /// - Input ports are checked for capacity
    TransportPacketToLocation(PacketID, PacketLocation),
}

/// Errors that can occur when undoing a NetAction
#[derive(Debug, thiserror::Error)]
pub enum UndoError {
    /// Cannot undo because the expected state does not match
    #[error("state mismatch: {0}")]
    StateMismatch(String),

    /// Cannot undo because a required entity was not found
    #[error("entity not found: {0}")]
    NotFound(String),

    /// Cannot undo because the action type is not undoable
    #[error("action not undoable: {0}")]
    NotUndoable(String),

    /// Internal error during undo
    #[error("internal error: {0}")]
    InternalError(String),
}

/// Errors that can occur when performing a NetAction
#[derive(Debug, thiserror::Error)]
pub enum NetActionError {
    /// Packet with the given ID was not found in the network
    #[error("packet not found: {packet_id}")]
    PacketNotFound { packet_id: PacketID },

    /// Epoch with the given ID was not found
    #[error("epoch not found: {epoch_id}")]
    EpochNotFound { epoch_id: EpochID },

    /// Epoch exists but is not in Running state
    #[error("epoch {epoch_id} is not running")]
    EpochNotRunning { epoch_id: EpochID },

    /// Epoch exists but is not in Startable state
    #[error("epoch {epoch_id} is not startable")]
    EpochNotStartable { epoch_id: EpochID },

    /// Cannot finish epoch because it still contains packets
    #[error("cannot finish epoch {epoch_id}: epoch still contains packets")]
    CannotFinishNonEmptyEpoch { epoch_id: EpochID },

    /// Cannot finish epoch because output port still has unsent packets
    #[error("cannot finish epoch {epoch_id}: output port '{port_name}' has unsent packets")]
    UnsentOutputSalvo {
        epoch_id: EpochID,
        port_name: PortName,
    },

    /// Packet is not inside the specified epoch's node location
    #[error("packet {packet_id} is not inside epoch {epoch_id}")]
    PacketNotInNode {
        packet_id: PacketID,
        epoch_id: EpochID,
    },

    /// Output port does not exist on the node
    #[error("output port '{port_name}' not found on node for epoch {epoch_id}")]
    OutputPortNotFound {
        port_name: PortName,
        epoch_id: EpochID,
    },

    /// Output salvo condition does not exist on the node
    #[error("output salvo condition '{condition_name}' not found on node for epoch {epoch_id}")]
    OutputSalvoConditionNotFound {
        condition_name: SalvoConditionName,
        epoch_id: EpochID,
    },

    /// Maximum number of output salvos reached for this condition
    #[error("max output salvos reached for condition '{condition_name}' on epoch {epoch_id}")]
    MaxOutputSalvosReached {
        condition_name: SalvoConditionName,
        epoch_id: EpochID,
    },

    /// Output salvo condition is not satisfied
    #[error("salvo condition '{condition_name}' not met for epoch {epoch_id}")]
    SalvoConditionNotMet {
        condition_name: SalvoConditionName,
        epoch_id: EpochID,
    },

    /// Output port has reached its capacity
    #[error("output port '{port_name}' is full for epoch {epoch_id}")]
    OutputPortFull {
        port_name: PortName,
        epoch_id: EpochID,
    },

    /// Cannot send packets to an output port that has no connected edge
    #[error("output port '{port_name}' on node '{node_name}' is not connected to any edge")]
    CannotPutPacketIntoUnconnectedOutputPort {
        port_name: PortName,
        node_name: NodeName,
    },

    /// Node with the given name was not found in the graph
    #[error("node not found: '{node_name}'")]
    NodeNotFound { node_name: NodeName },

    /// Packet is not at the expected input port
    #[error("packet {packet_id} is not at input port '{port_name}' of node '{node_name}'")]
    PacketNotAtInputPort {
        packet_id: PacketID,
        port_name: PortName,
        node_name: NodeName,
    },

    /// Input port does not exist on the node
    #[error("input port '{port_name}' not found on node '{node_name}'")]
    InputPortNotFound {
        port_name: PortName,
        node_name: NodeName,
    },

    /// Input port has reached its capacity
    #[error("input port '{port_name}' on node '{node_name}' is full")]
    InputPortFull {
        port_name: PortName,
        node_name: NodeName,
    },

    /// Cannot move packet out of a running epoch
    #[error("cannot move packet {packet_id} out of running epoch {epoch_id}")]
    CannotMovePacketFromRunningEpoch {
        packet_id: PacketID,
        epoch_id: EpochID,
    },

    /// Cannot move packet into a running epoch
    #[error("cannot move packet {packet_id} into running epoch {epoch_id}")]
    CannotMovePacketIntoRunningEpoch {
        packet_id: PacketID,
        epoch_id: EpochID,
    },

    /// Edge does not exist in the graph
    #[error("edge not found: {edge}")]
    EdgeNotFound { edge: Edge },
}

/// An event that occurred during a network action.
///
/// Events provide a complete audit trail of all state changes in the network.
/// Each event includes a timestamp and relevant identifiers.
/// Events contain all information needed to undo the operation.
#[derive(Debug, Clone)]
pub enum NetEvent {
    /// A new packet was created.
    PacketCreated(EventUTC, PacketID),
    /// A packet was consumed (normal removal from the network).
    /// Includes the packet's location before consumption for undo support.
    PacketConsumed(EventUTC, PacketID, PacketLocation),
    /// A packet was destroyed (abnormal removal, e.g., epoch cancellation).
    /// Includes the packet's location before destruction for undo support.
    PacketDestroyed(EventUTC, PacketID, PacketLocation),
    /// A new epoch was created (in Startable state).
    EpochCreated(EventUTC, EpochID),
    /// An epoch transitioned from Startable to Running.
    EpochStarted(EventUTC, EpochID),
    /// An epoch completed successfully.
    /// Includes the full epoch state for undo support.
    EpochFinished(EventUTC, Epoch),
    /// An epoch was cancelled.
    /// Includes the full epoch state for undo support.
    EpochCancelled(EventUTC, Epoch),
    /// A packet moved from one location to another.
    /// Includes the index in the source location for perfect undo restoration.
    /// (timestamp, packet_id, from_location, to_location, from_index)
    PacketMoved(EventUTC, PacketID, PacketLocation, PacketLocation, usize),
    /// An input salvo condition was triggered, creating an epoch.
    InputSalvoTriggered(EventUTC, EpochID, SalvoConditionName),
    /// An output salvo condition was triggered, sending packets.
    OutputSalvoTriggered(EventUTC, EpochID, SalvoConditionName),
}

/// Data returned by a successful network action.
#[derive(Debug, Clone)]
pub enum NetActionResponseData {
    /// Result of RunStep: whether any progress was made.
    StepResult {
        /// True if any packets moved or epochs were created.
        /// False if the network is blocked.
        made_progress: bool,
    },
    /// A packet ID (returned by CreatePacket).
    Packet(PacketID),
    /// The created epoch in Startable state (returned by CreateEpoch).
    CreatedEpoch(Epoch),
    /// The started epoch (returned by StartEpoch).
    StartedEpoch(Epoch),
    /// The finished epoch (returned by FinishEpoch).
    FinishedEpoch(Epoch),
    /// The cancelled epoch and IDs of destroyed packets (returned by CancelEpoch).
    CancelledEpoch(Epoch, Vec<PacketID>),
    /// No specific data (returned by ConsumePacket, DestroyPacket, etc.).
    None,
}

/// The result of performing a network action.
#[derive(Debug)]
pub enum NetActionResponse {
    /// Action succeeded, with optional data and a list of events that occurred.
    Success(NetActionResponseData, Vec<NetEvent>),
    /// Action failed with an error.
    Error(NetActionError),
}

/// The runtime state of a flow-based network.
///
/// A `NetSim` is created from a [`Graph`] and tracks:
/// - All packets and their locations
/// - All epochs and their states
/// - Which epochs are startable
///
/// All mutations must go through [`NetSim::do_action`] to ensure proper event tracking.
#[derive(Debug)]
pub struct NetSim {
    /// The graph topology this network is running on.
    pub graph: Graph,
    _packets: HashMap<PacketID, Packet>,
    _packets_by_location: HashMap<PacketLocation, IndexSet<PacketID>>,
    _epochs: HashMap<EpochID, Epoch>,
    _startable_epochs: HashSet<EpochID>,
    _node_to_epochs: HashMap<NodeName, Vec<EpochID>>,
}

impl NetSim {
    /// Creates a new net simulation from a Graph.
    ///
    /// Initializes packet location tracking for all edges and input ports.
    pub fn new(graph: Graph) -> Self {
        let mut packets_by_location: HashMap<PacketLocation, IndexSet<PacketID>> = HashMap::new();

        // Initialize empty packet sets for all edges
        for edge in graph.edges() {
            packets_by_location.insert(PacketLocation::Edge(edge.clone()), IndexSet::new());
        }

        // Initialize empty packet sets for all input ports
        for (node_name, node) in graph.nodes() {
            for port_name in node.in_ports.keys() {
                packets_by_location.insert(
                    PacketLocation::InputPort(node_name.clone(), port_name.clone()),
                    IndexSet::new(),
                );
            }
        }

        // Initialize OutsideNet location for packets created outside the network
        packets_by_location.insert(PacketLocation::OutsideNet, IndexSet::new());

        // Note: Output port locations are created per-epoch when epochs are created
        // Note: Node locations (inside epochs) are created when epochs are created

        NetSim {
            graph,
            _packets: HashMap::new(),
            _packets_by_location: packets_by_location,
            _epochs: HashMap::new(),
            _startable_epochs: HashSet::new(),
            _node_to_epochs: HashMap::new(),
        }
    }

    fn move_packet(&mut self, packet_id: &PacketID, new_location: PacketLocation) {
        let packet = self._packets.get_mut(packet_id).unwrap();
        let packets_at_old_location = self
            ._packets_by_location
            .get_mut(&packet.location)
            .expect("Packet location has no entry in self._packets_by_location.");
        packets_at_old_location.shift_remove(packet_id);
        packet.location = new_location;
        if !self
            ._packets_by_location
            .get_mut(&packet.location)
            .expect("Packet location has no entry in self._packets_by_location")
            .insert(*packet_id)
        {
            panic!("Attempted to move packet to a location that already contains it.");
        }
    }

    // NetActions

    /// Helper: Try to trigger an input salvo condition for a node.
    /// Returns (triggered: bool, events: Vec<NetEvent>).
    fn try_trigger_input_salvo(&mut self, node_name: &NodeName) -> (bool, Vec<NetEvent>) {
        let mut events: Vec<NetEvent> = Vec::new();

        let node = match self.graph.nodes().get(node_name) {
            Some(n) => n,
            None => return (false, events),
        };

        let in_port_names: Vec<PortName> = node.in_ports.keys().cloned().collect();
        let in_ports_clone: HashMap<PortName, Port> = node
            .in_ports
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    Port {
                        slots_spec: match v.slots_spec {
                            PortSlotSpec::Infinite => PortSlotSpec::Infinite,
                            PortSlotSpec::Finite(n) => PortSlotSpec::Finite(n),
                        },
                    },
                )
            })
            .collect();

        // Collect salvo condition data
        struct SalvoConditionData {
            name: SalvoConditionName,
            ports: HashMap<PortName, PacketCount>,
            term: SalvoConditionTerm,
        }

        let salvo_conditions: Vec<SalvoConditionData> = node
            .in_salvo_conditions
            .iter()
            .map(|(name, cond)| SalvoConditionData {
                name: name.clone(),
                ports: cond.ports.clone(),
                term: cond.term.clone(),
            })
            .collect();

        // Check salvo conditions in order - first satisfied one wins
        for salvo_cond_data in salvo_conditions {
            // Calculate packet counts for all input ports
            let port_packet_counts: HashMap<PortName, u64> = in_port_names
                .iter()
                .map(|port_name| {
                    let count = self
                        ._packets_by_location
                        .get(&PacketLocation::InputPort(
                            node_name.clone(),
                            port_name.clone(),
                        ))
                        .map(|packets| packets.len() as u64)
                        .unwrap_or(0);
                    (port_name.clone(), count)
                })
                .collect();

            // Check if salvo condition is satisfied
            if evaluate_salvo_condition(&salvo_cond_data.term, &port_packet_counts, &in_ports_clone)
            {
                // Create a new epoch
                let epoch_id = Ulid::new();

                // Collect packets from the ports listed in salvo_condition.ports
                // Store (packet_id, port_name, from_index) for each packet to move
                let mut salvo_packets: Vec<(PortName, PacketID)> = Vec::new();
                let mut packets_to_move: Vec<(PacketID, PortName, usize)> = Vec::new();

                for (port_name, packet_count) in &salvo_cond_data.ports {
                    let port_location =
                        PacketLocation::InputPort(node_name.clone(), port_name.clone());
                    if let Some(packet_ids) = self._packets_by_location.get(&port_location) {
                        let take_count = match packet_count {
                            PacketCount::All => packet_ids.len(),
                            PacketCount::Count(n) => std::cmp::min(*n as usize, packet_ids.len()),
                        };
                        for (idx, pid) in packet_ids.iter().enumerate().take(take_count) {
                            salvo_packets.push((port_name.clone(), *pid));
                            packets_to_move.push((*pid, port_name.clone(), idx));
                        }
                    }
                }

                // Create the salvo
                let in_salvo = Salvo {
                    salvo_condition: salvo_cond_data.name.clone(),
                    packets: salvo_packets,
                };

                // Create the epoch
                let epoch = Epoch {
                    id: epoch_id,
                    node_name: node_name.clone(),
                    in_salvo,
                    out_salvos: Vec::new(),
                    state: EpochState::Startable,
                };

                // Register the epoch
                self._epochs.insert(epoch_id, epoch);
                self._startable_epochs.insert(epoch_id);
                self._node_to_epochs
                    .entry(node_name.clone())
                    .or_default()
                    .push(epoch_id);

                // Create a location entry for packets inside the epoch
                let epoch_location = PacketLocation::Node(epoch_id);
                self._packets_by_location
                    .insert(epoch_location.clone(), IndexSet::new());

                // Create output port location entries for this epoch
                let node = self
                    .graph
                    .nodes()
                    .get(node_name)
                    .expect("Node not found for epoch creation");
                for out_port_name in node.out_ports.keys() {
                    let output_port_location =
                        PacketLocation::OutputPort(epoch_id, out_port_name.clone());
                    self._packets_by_location
                        .insert(output_port_location, IndexSet::new());
                }

                // Emit events in logical order:
                // 1. InputSalvoTriggered - the condition was met, triggering epoch creation
                // 2. EpochCreated - the epoch is created as a result
                // 3. PacketMoved - packets move into the newly created epoch
                events.push(NetEvent::InputSalvoTriggered(
                    get_utc_now(),
                    epoch_id,
                    salvo_cond_data.name.clone(),
                ));
                events.push(NetEvent::EpochCreated(get_utc_now(), epoch_id));

                // Move packets from input ports into the epoch
                for (pid, port_name, from_index) in &packets_to_move {
                    let from_location =
                        PacketLocation::InputPort(node_name.clone(), port_name.clone());
                    self.move_packet(pid, epoch_location.clone());
                    events.push(NetEvent::PacketMoved(
                        get_utc_now(),
                        *pid,
                        from_location,
                        epoch_location.clone(),
                        *from_index,
                    ));
                }

                // Only one salvo condition can trigger per node per call
                return (true, events);
            }
        }

        (false, events)
    }

    fn run_step(&mut self) -> NetActionResponse {
        let mut all_events: Vec<NetEvent> = Vec::new();
        let mut made_progress = false;

        // Phase 1: Move packets from edges to input ports
        // Collect all edge locations and their first packet (FIFO)
        // We need to extract data before mutating to avoid borrow issues
        struct EdgeMoveCandidate {
            packet_id: PacketID,
            from_location: PacketLocation,
            from_index: usize,
            input_port_location: PacketLocation,
            can_move: bool,
        }

        let mut edge_candidates: Vec<EdgeMoveCandidate> = Vec::new();

        // Iterate through all edge locations in _packets_by_location
        for (location, packets) in &self._packets_by_location {
            if let PacketLocation::Edge(edge_ref) = location {
                // Get the first packet (FIFO order)
                if let Some(first_packet_id) = packets.first() {
                    let target_node_name = edge_ref.target.node_name.clone();
                    let target_port_name = edge_ref.target.port_name.clone();

                    // Check if the target input port has space
                    let node = self
                        .graph
                        .nodes()
                        .get(&target_node_name)
                        .expect("Edge targets a non-existent node");
                    let port = node
                        .in_ports
                        .get(&target_port_name)
                        .expect("Edge targets a non-existent input port");

                    let input_port_location = PacketLocation::InputPort(
                        target_node_name.clone(),
                        target_port_name.clone(),
                    );
                    let current_count = self
                        ._packets_by_location
                        .get(&input_port_location)
                        .map(|packets| packets.len() as u64)
                        .unwrap_or(0);

                    let can_move = match port.slots_spec {
                        PortSlotSpec::Infinite => true,
                        PortSlotSpec::Finite(max_slots) => current_count < max_slots,
                    };

                    edge_candidates.push(EdgeMoveCandidate {
                        packet_id: *first_packet_id,
                        from_location: location.clone(),
                        from_index: 0, // First packet is always at index 0
                        input_port_location,
                        can_move,
                    });
                }
            }
        }

        // Phase 1: Move packets from edges to input ports
        for candidate in edge_candidates {
            if !candidate.can_move {
                continue;
            }

            // Move the packet to the input port
            self.move_packet(&candidate.packet_id, candidate.input_port_location.clone());
            all_events.push(NetEvent::PacketMoved(
                get_utc_now(),
                candidate.packet_id,
                candidate.from_location,
                candidate.input_port_location.clone(),
                candidate.from_index,
            ));
            made_progress = true;
        }

        // Phase 2: Check salvo conditions for all nodes with packets at input ports
        let mut nodes_with_input_packets: Vec<NodeName> = Vec::new();
        for (location, packets) in &self._packets_by_location {
            if let PacketLocation::InputPort(node_name, _) = location
                && !packets.is_empty()
                && !nodes_with_input_packets.contains(node_name)
            {
                nodes_with_input_packets.push(node_name.clone());
            }
        }

        for node_name in nodes_with_input_packets {
            let (triggered, events) = self.try_trigger_input_salvo(&node_name);
            all_events.extend(events);
            if triggered {
                made_progress = true;
            }
        }

        NetActionResponse::Success(
            NetActionResponseData::StepResult { made_progress },
            all_events,
        )
    }

    fn create_packet(&mut self, maybe_epoch_id: &Option<EpochID>) -> NetActionResponse {
        // Check that epoch_id exists and is running
        if let Some(epoch_id) = maybe_epoch_id {
            if !self._epochs.contains_key(epoch_id) {
                return NetActionResponse::Error(NetActionError::EpochNotFound {
                    epoch_id: *epoch_id,
                });
            }
            if !matches!(self._epochs[epoch_id].state, EpochState::Running) {
                return NetActionResponse::Error(NetActionError::EpochNotRunning {
                    epoch_id: *epoch_id,
                });
            }
        }

        let packet_location = match maybe_epoch_id {
            Some(epoch_id) => PacketLocation::Node(*epoch_id),
            None => PacketLocation::OutsideNet,
        };

        let packet = Packet {
            id: Ulid::new(),
            location: packet_location.clone(),
        };

        let packet_id = packet.id;
        self._packets.insert(packet.id, packet);

        // Add packet to location index
        self._packets_by_location
            .entry(packet_location)
            .or_default()
            .insert(packet_id);

        NetActionResponse::Success(
            NetActionResponseData::Packet(packet_id),
            vec![NetEvent::PacketCreated(get_utc_now(), packet_id)],
        )
    }

    fn consume_packet(&mut self, packet_id: &PacketID) -> NetActionResponse {
        if !self._packets.contains_key(packet_id) {
            return NetActionResponse::Error(NetActionError::PacketNotFound {
                packet_id: *packet_id,
            });
        }

        let location = self._packets[packet_id].location.clone();

        if let Some(packets) = self._packets_by_location.get_mut(&location) {
            if packets.shift_remove(packet_id) {
                self._packets.remove(packet_id);
                NetActionResponse::Success(
                    NetActionResponseData::None,
                    vec![NetEvent::PacketConsumed(
                        get_utc_now(),
                        *packet_id,
                        location,
                    )],
                )
            } else {
                panic!(
                    "Packet with ID {} not found in location {:?}",
                    packet_id, location
                );
            }
        } else {
            panic!("Packet location {:?} not found", location);
        }
    }

    fn destroy_packet(&mut self, packet_id: &PacketID) -> NetActionResponse {
        if !self._packets.contains_key(packet_id) {
            return NetActionResponse::Error(NetActionError::PacketNotFound {
                packet_id: *packet_id,
            });
        }

        let location = self._packets[packet_id].location.clone();

        if let Some(packets) = self._packets_by_location.get_mut(&location) {
            if packets.shift_remove(packet_id) {
                self._packets.remove(packet_id);
                NetActionResponse::Success(
                    NetActionResponseData::None,
                    vec![NetEvent::PacketDestroyed(
                        get_utc_now(),
                        *packet_id,
                        location,
                    )],
                )
            } else {
                panic!(
                    "Packet with ID {} not found in location {:?}",
                    packet_id, location
                );
            }
        } else {
            panic!("Packet location {:?} not found", location);
        }
    }

    fn start_epoch(&mut self, epoch_id: &EpochID) -> NetActionResponse {
        if let Some(epoch) = self._epochs.get_mut(epoch_id) {
            if !self._startable_epochs.contains(epoch_id) {
                return NetActionResponse::Error(NetActionError::EpochNotStartable {
                    epoch_id: *epoch_id,
                });
            }
            debug_assert!(
                matches!(epoch.state, EpochState::Startable),
                "Epoch state is not Startable but was in net._startable_epochs."
            );
            epoch.state = EpochState::Running;
            self._startable_epochs.remove(epoch_id);
            NetActionResponse::Success(
                NetActionResponseData::StartedEpoch(epoch.clone()),
                vec![NetEvent::EpochStarted(get_utc_now(), *epoch_id)],
            )
        } else {
            NetActionResponse::Error(NetActionError::EpochNotFound {
                epoch_id: *epoch_id,
            })
        }
    }

    fn finish_epoch(&mut self, epoch_id: &EpochID) -> NetActionResponse {
        // Check if epoch exists
        let epoch = if let Some(epoch) = self._epochs.get(epoch_id) {
            epoch.clone()
        } else {
            return NetActionResponse::Error(NetActionError::EpochNotFound {
                epoch_id: *epoch_id,
            });
        };

        // Check if epoch is running
        if epoch.state != EpochState::Running {
            return NetActionResponse::Error(NetActionError::EpochNotRunning {
                epoch_id: *epoch_id,
            });
        }

        // No packets may remain inside the epoch
        let epoch_loc = PacketLocation::Node(*epoch_id);
        if let Some(packets) = self._packets_by_location.get(&epoch_loc) {
            if !packets.is_empty() {
                return NetActionResponse::Error(NetActionError::CannotFinishNonEmptyEpoch {
                    epoch_id: *epoch_id,
                });
            }
        } else {
            panic!("Epoch {} not found in location {:?}", epoch_id, epoch_loc);
        }

        // No packets may remain in output ports (unsent salvos)
        let node = self
            .graph
            .nodes()
            .get(&epoch.node_name)
            .expect("Epoch references non-existent node");
        for port_name in node.out_ports.keys() {
            let output_port_loc = PacketLocation::OutputPort(*epoch_id, port_name.clone());
            if let Some(packets) = self._packets_by_location.get(&output_port_loc)
                && !packets.is_empty()
            {
                return NetActionResponse::Error(NetActionError::UnsentOutputSalvo {
                    epoch_id: *epoch_id,
                    port_name: port_name.clone(),
                });
            }
        }

        // All checks passed - finish the epoch
        // Clone epoch state before modifying for the event (captures Running state)
        let epoch_before_finish = self._epochs[epoch_id].clone();

        let mut epoch = self._epochs.remove(epoch_id).unwrap();
        epoch.state = EpochState::Finished;

        // Clean up location entries
        self._packets_by_location.remove(&epoch_loc);
        for port_name in node.out_ports.keys() {
            let output_port_loc = PacketLocation::OutputPort(*epoch_id, port_name.clone());
            self._packets_by_location.remove(&output_port_loc);
        }

        // Remove from _node_to_epochs
        if let Some(epoch_ids) = self._node_to_epochs.get_mut(&epoch.node_name) {
            epoch_ids.retain(|id| id != epoch_id);
        }

        NetActionResponse::Success(
            NetActionResponseData::FinishedEpoch(epoch),
            vec![NetEvent::EpochFinished(get_utc_now(), epoch_before_finish)],
        )
    }

    fn cancel_epoch(&mut self, epoch_id: &EpochID) -> NetActionResponse {
        // Check if epoch exists and capture it for the event
        let epoch_for_event = if let Some(epoch) = self._epochs.get(epoch_id) {
            epoch.clone()
        } else {
            return NetActionResponse::Error(NetActionError::EpochNotFound {
                epoch_id: *epoch_id,
            });
        };

        let mut events: Vec<NetEvent> = Vec::new();
        let mut destroyed_packets: Vec<PacketID> = Vec::new();

        // Collect packets inside the epoch (Node location)
        let epoch_location = PacketLocation::Node(*epoch_id);
        if let Some(packet_ids) = self._packets_by_location.get(&epoch_location) {
            destroyed_packets.extend(packet_ids.iter().cloned());
        }

        // Collect packets in the epoch's output ports
        let node = self
            .graph
            .nodes()
            .get(&epoch_for_event.node_name)
            .expect("Epoch references non-existent node");
        for port_name in node.out_ports.keys() {
            let output_port_location = PacketLocation::OutputPort(*epoch_id, port_name.clone());
            if let Some(packet_ids) = self._packets_by_location.get(&output_port_location) {
                destroyed_packets.extend(packet_ids.iter().cloned());
            }
        }

        // Remove packets from _packets and _packets_by_location, emit events with location
        for packet_id in &destroyed_packets {
            let packet = self
                ._packets
                .remove(packet_id)
                .expect("Packet in location map not found in packets map");
            let packet_location = packet.location.clone();
            if let Some(packets_at_location) = self._packets_by_location.get_mut(&packet_location) {
                packets_at_location.shift_remove(packet_id);
            }
            events.push(NetEvent::PacketDestroyed(
                get_utc_now(),
                *packet_id,
                packet_location,
            ));
        }

        // Remove output port location entries for this epoch
        for port_name in node.out_ports.keys() {
            let output_port_location = PacketLocation::OutputPort(*epoch_id, port_name.clone());
            self._packets_by_location.remove(&output_port_location);
        }

        // Remove the epoch's node location entry
        self._packets_by_location.remove(&epoch_location);

        // Update _startable_epochs if epoch was startable
        self._startable_epochs.remove(epoch_id);

        // Update _node_to_epochs
        if let Some(epoch_ids) = self._node_to_epochs.get_mut(&epoch_for_event.node_name) {
            epoch_ids.retain(|id| id != epoch_id);
        }

        // Remove epoch from _epochs
        let epoch = self._epochs.remove(epoch_id).expect("Epoch should exist");

        events.push(NetEvent::EpochCancelled(get_utc_now(), epoch_for_event));

        NetActionResponse::Success(
            NetActionResponseData::CancelledEpoch(epoch, destroyed_packets),
            events,
        )
    }

    fn create_epoch(&mut self, node_name: &NodeName, salvo: &Salvo) -> NetActionResponse {
        // Validate node exists
        let node = match self.graph.nodes().get(node_name) {
            Some(node) => node,
            None => {
                return NetActionResponse::Error(NetActionError::NodeNotFound {
                    node_name: node_name.clone(),
                });
            }
        };

        // Validate all packets in salvo
        for (port_name, packet_id) in &salvo.packets {
            // Validate input port exists
            if !node.in_ports.contains_key(port_name) {
                return NetActionResponse::Error(NetActionError::InputPortNotFound {
                    port_name: port_name.clone(),
                    node_name: node_name.clone(),
                });
            }

            // Validate packet exists
            let packet = match self._packets.get(packet_id) {
                Some(packet) => packet,
                None => {
                    return NetActionResponse::Error(NetActionError::PacketNotFound {
                        packet_id: *packet_id,
                    });
                }
            };

            // Validate packet is at the input port of this node
            let expected_location = PacketLocation::InputPort(node_name.clone(), port_name.clone());
            if packet.location != expected_location {
                return NetActionResponse::Error(NetActionError::PacketNotAtInputPort {
                    packet_id: *packet_id,
                    port_name: port_name.clone(),
                    node_name: node_name.clone(),
                });
            }
        }

        let mut events: Vec<NetEvent> = Vec::new();

        // Create the epoch in Startable state
        let epoch_id = Ulid::new();
        let epoch = Epoch {
            id: epoch_id,
            node_name: node_name.clone(),
            in_salvo: salvo.clone(),
            out_salvos: Vec::new(),
            state: EpochState::Startable,
        };

        // Register the epoch
        self._epochs.insert(epoch_id, epoch.clone());
        self._startable_epochs.insert(epoch_id);
        self._node_to_epochs
            .entry(node_name.clone())
            .or_default()
            .push(epoch_id);

        // Create location entry for packets inside the epoch
        let epoch_location = PacketLocation::Node(epoch_id);
        self._packets_by_location
            .insert(epoch_location.clone(), IndexSet::new());

        // Create output port location entries for this epoch
        for port_name in node.out_ports.keys() {
            let output_port_location = PacketLocation::OutputPort(epoch_id, port_name.clone());
            self._packets_by_location
                .insert(output_port_location, IndexSet::new());
        }

        events.push(NetEvent::EpochCreated(get_utc_now(), epoch_id));

        // Move packets from input ports into the epoch
        for (port_name, packet_id) in &salvo.packets {
            let from_location = PacketLocation::InputPort(node_name.clone(), port_name.clone());

            // Get the index of this packet in its source location before moving
            let from_index = self
                ._packets_by_location
                .get(&from_location)
                .and_then(|packets| packets.get_index_of(packet_id))
                .expect("Packet should exist at from_location");

            self.move_packet(packet_id, epoch_location.clone());
            events.push(NetEvent::PacketMoved(
                get_utc_now(),
                *packet_id,
                from_location,
                epoch_location.clone(),
                from_index,
            ));
        }

        NetActionResponse::Success(NetActionResponseData::CreatedEpoch(epoch), events)
    }

    fn load_packet_into_output_port(
        &mut self,
        packet_id: &PacketID,
        port_name: &String,
    ) -> NetActionResponse {
        let (epoch_id, old_location) = if let Some(packet) = self._packets.get(packet_id) {
            if let PacketLocation::Node(epoch_id) = packet.location {
                (epoch_id, packet.location.clone())
            } else {
                // We don't know the epoch_id since the packet isn't in a node
                // Use a placeholder - this is an edge case where we can't provide full context
                return NetActionResponse::Error(NetActionError::PacketNotInNode {
                    packet_id: *packet_id,
                    epoch_id: Ulid::nil(), // Placeholder since packet isn't in any epoch
                });
            }
        } else {
            return NetActionResponse::Error(NetActionError::PacketNotFound {
                packet_id: *packet_id,
            });
        };

        let node_name = self
            ._epochs
            .get(&epoch_id)
            .expect("The epoch id in the location of a packet could not be found.")
            .node_name
            .clone();
        let node = self
            .graph
            .nodes()
            .get(&node_name)
            .expect("Packet located in a non-existing node (yet the node has an epoch).");

        if !node.out_ports.contains_key(port_name) {
            return NetActionResponse::Error(NetActionError::OutputPortNotFound {
                port_name: port_name.clone(),
                epoch_id,
            });
        }

        let port = node.out_ports.get(port_name).unwrap();
        let output_port_location = PacketLocation::OutputPort(epoch_id, port_name.clone());
        let port_packets = self
            ._packets_by_location
            .get(&output_port_location)
            .expect("No entry in NetSim._packets_by_location found for output port.");

        // Check if the output port is full
        if let PortSlotSpec::Finite(num_slots) = port.slots_spec
            && port_packets.len() as u64 >= num_slots
        {
            return NetActionResponse::Error(NetActionError::OutputPortFull {
                port_name: port_name.clone(),
                epoch_id,
            });
        }

        // Get the index before moving
        let from_index = self
            ._packets_by_location
            .get(&old_location)
            .and_then(|packets| packets.get_index_of(packet_id))
            .expect("Packet should exist at old_location");

        let new_location = output_port_location;
        self.move_packet(packet_id, new_location.clone());
        NetActionResponse::Success(
            NetActionResponseData::None,
            vec![NetEvent::PacketMoved(
                get_utc_now(),
                *packet_id,
                old_location,
                new_location,
                from_index,
            )],
        )
    }

    fn send_output_salvo(
        &mut self,
        epoch_id: &EpochID,
        salvo_condition_name: &SalvoConditionName,
    ) -> NetActionResponse {
        // Get epoch
        let epoch = if let Some(epoch) = self._epochs.get(epoch_id) {
            epoch
        } else {
            return NetActionResponse::Error(NetActionError::EpochNotFound {
                epoch_id: *epoch_id,
            });
        };

        // Get node
        let node = self
            .graph
            .nodes()
            .get(&epoch.node_name)
            .expect("Node associated with epoch could not be found.");

        // Get salvo condition
        let salvo_condition =
            if let Some(salvo_condition) = node.out_salvo_conditions.get(salvo_condition_name) {
                salvo_condition
            } else {
                return NetActionResponse::Error(NetActionError::OutputSalvoConditionNotFound {
                    condition_name: salvo_condition_name.clone(),
                    epoch_id: *epoch_id,
                });
            };

        // Check if max salvos reached for this specific condition
        if let MaxSalvos::Finite(max) = salvo_condition.max_salvos {
            let condition_salvo_count = epoch
                .out_salvos
                .iter()
                .filter(|s| s.salvo_condition == *salvo_condition_name)
                .count() as u64;
            if condition_salvo_count >= max {
                return NetActionResponse::Error(NetActionError::MaxOutputSalvosReached {
                    condition_name: salvo_condition_name.clone(),
                    epoch_id: *epoch_id,
                });
            }
        }

        // Check that the salvo condition is met
        let port_packet_counts: HashMap<PortName, u64> = node
            .out_ports
            .keys()
            .map(|port_name| {
                let count = self
                    ._packets_by_location
                    .get(&PacketLocation::OutputPort(*epoch_id, port_name.clone()))
                    .map(|packets| packets.len() as u64)
                    .unwrap_or(0);
                (port_name.clone(), count)
            })
            .collect();
        if !evaluate_salvo_condition(&salvo_condition.term, &port_packet_counts, &node.out_ports) {
            return NetActionResponse::Error(NetActionError::SalvoConditionNotMet {
                condition_name: salvo_condition_name.clone(),
                epoch_id: *epoch_id,
            });
        }

        // Get the locations to send packets to
        // Tuple: (packet_id, port_name, from_location, to_location, from_index)
        let mut packets_to_move: Vec<(PacketID, PortName, PacketLocation, PacketLocation, usize)> =
            Vec::new();
        for (port_name, packet_count) in &salvo_condition.ports {
            let from_location = PacketLocation::OutputPort(*epoch_id, port_name.clone());
            let packets = self
                ._packets_by_location
                .get(&from_location)
                .unwrap_or_else(|| {
                    panic!(
                        "Output port '{}' of node '{}' does not have an entry in self._packets_by_location",
                        port_name,
                        node.name.clone()
                    )
                })
                .clone();
            let edge_ref = if let Some(edge_ref) = self.graph.get_edge_by_tail(&PortRef {
                node_name: node.name.clone(),
                port_type: PortType::Output,
                port_name: port_name.clone(),
            }) {
                edge_ref.clone()
            } else {
                return NetActionResponse::Error(
                    NetActionError::CannotPutPacketIntoUnconnectedOutputPort {
                        port_name: port_name.clone(),
                        node_name: node.name.clone(),
                    },
                );
            };
            let to_location = PacketLocation::Edge(edge_ref.clone());
            let take_count = match packet_count {
                PacketCount::All => packets.len(),
                PacketCount::Count(n) => std::cmp::min(*n as usize, packets.len()),
            };
            // Capture index along with packet_id (enumerate gives us the index)
            for (idx, packet_id) in packets.into_iter().enumerate().take(take_count) {
                packets_to_move.push((
                    packet_id,
                    port_name.clone(),
                    from_location.clone(),
                    to_location.clone(),
                    idx,
                ));
            }
        }

        // Create a Salvo and add it to the epoch
        let salvo = Salvo {
            salvo_condition: salvo_condition_name.clone(),
            packets: packets_to_move
                .iter()
                .map(|(packet_id, port_name, _, _, _)| (port_name.clone(), *packet_id))
                .collect(),
        };
        self._epochs
            .get_mut(epoch_id)
            .unwrap()
            .out_salvos
            .push(salvo);

        // Move packets
        let mut net_events = Vec::new();
        for (packet_id, _port_name, from_location, to_location, from_index) in packets_to_move {
            net_events.push(NetEvent::PacketMoved(
                get_utc_now(),
                packet_id,
                from_location,
                to_location.clone(),
                from_index,
            ));
            self.move_packet(&packet_id, to_location);
        }

        // Emit OutputSalvoTriggered event
        net_events.push(NetEvent::OutputSalvoTriggered(
            get_utc_now(),
            *epoch_id,
            salvo_condition_name.clone(),
        ));

        NetActionResponse::Success(NetActionResponseData::None, net_events)
    }

    fn transport_packet_to_location(
        &mut self,
        packet_id: &PacketID,
        destination: &PacketLocation,
    ) -> NetActionResponse {
        // Validate packet exists
        let packet = if let Some(p) = self._packets.get(packet_id) {
            p
        } else {
            return NetActionResponse::Error(NetActionError::PacketNotFound {
                packet_id: *packet_id,
            });
        };
        let current_location = packet.location.clone();

        // Check if moving FROM a running epoch
        match &current_location {
            PacketLocation::Node(epoch_id) => {
                if let Some(epoch) = self._epochs.get(epoch_id)
                    && epoch.state == EpochState::Running
                {
                    return NetActionResponse::Error(
                        NetActionError::CannotMovePacketFromRunningEpoch {
                            packet_id: *packet_id,
                            epoch_id: *epoch_id,
                        },
                    );
                }
            }
            PacketLocation::OutputPort(epoch_id, _) => {
                if let Some(epoch) = self._epochs.get(epoch_id)
                    && epoch.state == EpochState::Running
                {
                    return NetActionResponse::Error(
                        NetActionError::CannotMovePacketFromRunningEpoch {
                            packet_id: *packet_id,
                            epoch_id: *epoch_id,
                        },
                    );
                }
            }
            _ => {}
        }

        // Check if moving TO a running epoch
        match destination {
            PacketLocation::Node(epoch_id) => {
                if let Some(epoch) = self._epochs.get(epoch_id) {
                    if epoch.state == EpochState::Running {
                        return NetActionResponse::Error(
                            NetActionError::CannotMovePacketIntoRunningEpoch {
                                packet_id: *packet_id,
                                epoch_id: *epoch_id,
                            },
                        );
                    }
                } else {
                    return NetActionResponse::Error(NetActionError::EpochNotFound {
                        epoch_id: *epoch_id,
                    });
                }
            }
            PacketLocation::OutputPort(epoch_id, port_name) => {
                if let Some(epoch) = self._epochs.get(epoch_id) {
                    if epoch.state == EpochState::Running {
                        return NetActionResponse::Error(
                            NetActionError::CannotMovePacketIntoRunningEpoch {
                                packet_id: *packet_id,
                                epoch_id: *epoch_id,
                            },
                        );
                    }
                    // Check that output port exists on the node
                    let node = self
                        .graph
                        .nodes()
                        .get(&epoch.node_name)
                        .expect("Node associated with epoch could not be found.");
                    if !node.out_ports.contains_key(port_name) {
                        return NetActionResponse::Error(NetActionError::OutputPortNotFound {
                            port_name: port_name.clone(),
                            epoch_id: *epoch_id,
                        });
                    }
                } else {
                    return NetActionResponse::Error(NetActionError::EpochNotFound {
                        epoch_id: *epoch_id,
                    });
                }
            }
            PacketLocation::InputPort(node_name, port_name) => {
                // Check node exists
                let node = if let Some(n) = self.graph.nodes().get(node_name) {
                    n
                } else {
                    return NetActionResponse::Error(NetActionError::NodeNotFound {
                        node_name: node_name.clone(),
                    });
                };
                // Check port exists
                let port = if let Some(p) = node.in_ports.get(port_name) {
                    p
                } else {
                    return NetActionResponse::Error(NetActionError::InputPortNotFound {
                        port_name: port_name.clone(),
                        node_name: node_name.clone(),
                    });
                };
                // Check capacity
                let current_count = self
                    ._packets_by_location
                    .get(destination)
                    .map(|s| s.len())
                    .unwrap_or(0);
                let is_full = match &port.slots_spec {
                    PortSlotSpec::Infinite => false,
                    PortSlotSpec::Finite(capacity) => current_count >= *capacity as usize,
                };
                if is_full {
                    return NetActionResponse::Error(NetActionError::InputPortFull {
                        port_name: port_name.clone(),
                        node_name: node_name.clone(),
                    });
                }
            }
            PacketLocation::Edge(edge) => {
                // Check edge exists in graph
                if !self.graph.edges().contains(edge) {
                    return NetActionResponse::Error(NetActionError::EdgeNotFound {
                        edge: edge.clone(),
                    });
                }
            }
            PacketLocation::OutsideNet => {
                // Always allowed
            }
        }

        // Get the index before moving
        let from_index = self
            ._packets_by_location
            .get(&current_location)
            .and_then(|packets| packets.get_index_of(packet_id))
            .expect("Packet should exist at current_location");

        // Move the packet
        self.move_packet(packet_id, destination.clone());

        NetActionResponse::Success(
            NetActionResponseData::None,
            vec![NetEvent::PacketMoved(
                get_utc_now(),
                *packet_id,
                current_location,
                destination.clone(),
                from_index,
            )],
        )
    }

    /// Perform an action on the network.
    ///
    /// This is the primary way to mutate the network state. All actions produce
    /// a response containing either success data and events, or an error.
    ///
    /// # Example
    ///
    /// ```
    /// use netrun_sim::net::{NetSim, NetAction, NetActionResponse, NetActionResponseData};
    /// use netrun_sim::graph::{Graph, Node, Port, PortSlotSpec};
    /// use std::collections::HashMap;
    ///
    /// let node = Node {
    ///     name: "A".to_string(),
    ///     in_ports: HashMap::new(),
    ///     out_ports: HashMap::new(),
    ///     in_salvo_conditions: HashMap::new(),
    ///     out_salvo_conditions: HashMap::new(),
    /// };
    /// let graph = Graph::new(vec![node], vec![]);
    /// let mut net = NetSim::new(graph);
    ///
    /// // Create a packet outside the network
    /// let response = net.do_action(&NetAction::CreatePacket(None));
    /// match response {
    ///     NetActionResponse::Success(NetActionResponseData::Packet(id), events) => {
    ///         println!("Created packet {}", id);
    ///     }
    ///     _ => panic!("Expected success"),
    /// }
    /// ```
    pub fn do_action(&mut self, action: &NetAction) -> NetActionResponse {
        match action {
            NetAction::RunStep => self.run_step(),
            NetAction::CreatePacket(maybe_epoch_id) => self.create_packet(maybe_epoch_id),
            NetAction::ConsumePacket(packet_id) => self.consume_packet(packet_id),
            NetAction::DestroyPacket(packet_id) => self.destroy_packet(packet_id),
            NetAction::StartEpoch(epoch_id) => self.start_epoch(epoch_id),
            NetAction::FinishEpoch(epoch_id) => self.finish_epoch(epoch_id),
            NetAction::CancelEpoch(epoch_id) => self.cancel_epoch(epoch_id),
            NetAction::CreateEpoch(node_name, salvo) => self.create_epoch(node_name, salvo),
            NetAction::LoadPacketIntoOutputPort(packet_id, port_name) => {
                self.load_packet_into_output_port(packet_id, port_name)
            }
            NetAction::SendOutputSalvo(epoch_id, salvo_condition_name) => {
                self.send_output_salvo(epoch_id, salvo_condition_name)
            }
            NetAction::TransportPacketToLocation(packet_id, location) => {
                self.transport_packet_to_location(packet_id, location)
            }
        }
    }

    // ========== Public Accessors ==========

    /// Get the number of packets at a given location.
    pub fn packet_count_at(&self, location: &PacketLocation) -> usize {
        self._packets_by_location
            .get(location)
            .map(|s| s.len())
            .unwrap_or(0)
    }

    /// Get all packets at a given location.
    pub fn get_packets_at_location(&self, location: &PacketLocation) -> Vec<PacketID> {
        self._packets_by_location
            .get(location)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get an epoch by ID.
    pub fn get_epoch(&self, epoch_id: &EpochID) -> Option<&Epoch> {
        self._epochs.get(epoch_id)
    }

    /// Get all startable epoch IDs.
    pub fn get_startable_epochs(&self) -> Vec<EpochID> {
        self._startable_epochs.iter().cloned().collect()
    }

    /// Get a packet by ID.
    pub fn get_packet(&self, packet_id: &PacketID) -> Option<&Packet> {
        self._packets.get(packet_id)
    }

    /// Run the network until blocked, returning all events that occurred.
    ///
    /// This is a convenience method that repeatedly calls `RunStep` until no more
    /// progress can be made. Equivalent to:
    /// ```ignore
    /// while !net.is_blocked() {
    ///     net.do_action(&NetAction::RunStep);
    /// }
    /// ```
    pub fn run_until_blocked(&mut self) -> Vec<NetEvent> {
        let mut all_events = Vec::new();
        while !self.is_blocked() {
            if let NetActionResponse::Success(_, events) = self.do_action(&NetAction::RunStep) {
                all_events.extend(events);
            }
        }
        all_events
    }

    /// Check if the network is blocked (no progress can be made by RunStep).
    ///
    /// Returns true if:
    /// - No packets can move from edges to input ports (all destinations full or no packets on edges)
    /// - No input salvo conditions can be triggered
    pub fn is_blocked(&self) -> bool {
        // Check Phase 1: Can any packet move from an edge to an input port?
        for (location, packets) in &self._packets_by_location {
            if let PacketLocation::Edge(edge_ref) = location {
                if packets.is_empty() {
                    continue;
                }

                let target_node_name = &edge_ref.target.node_name;
                let target_port_name = &edge_ref.target.port_name;

                let node = match self.graph.nodes().get(target_node_name) {
                    Some(n) => n,
                    None => continue,
                };
                let port = match node.in_ports.get(target_port_name) {
                    Some(p) => p,
                    None => continue,
                };

                let input_port_location =
                    PacketLocation::InputPort(target_node_name.clone(), target_port_name.clone());
                let current_count = self
                    ._packets_by_location
                    .get(&input_port_location)
                    .map(|p| p.len() as u64)
                    .unwrap_or(0);

                let can_move = match port.slots_spec {
                    PortSlotSpec::Infinite => true,
                    PortSlotSpec::Finite(max_slots) => current_count < max_slots,
                };

                if can_move {
                    return false; // Not blocked - a packet can move
                }
            }
        }

        // Check Phase 2: Can any salvo condition be triggered?
        for (location, packets) in &self._packets_by_location {
            if let PacketLocation::InputPort(node_name, _) = location {
                if packets.is_empty() {
                    continue;
                }

                // Check if any salvo condition on this node can be triggered
                if self.can_trigger_input_salvo(node_name) {
                    return false; // Not blocked - a salvo condition can trigger
                }
            }
        }

        true // Blocked - no progress possible
    }

    /// Helper: Check if any input salvo condition can be triggered for a node.
    fn can_trigger_input_salvo(&self, node_name: &NodeName) -> bool {
        let node = match self.graph.nodes().get(node_name) {
            Some(n) => n,
            None => return false,
        };

        let in_port_names: Vec<PortName> = node.in_ports.keys().cloned().collect();

        // Calculate packet counts for all input ports
        let port_packet_counts: HashMap<PortName, u64> = in_port_names
            .iter()
            .map(|port_name| {
                let count = self
                    ._packets_by_location
                    .get(&PacketLocation::InputPort(
                        node_name.clone(),
                        port_name.clone(),
                    ))
                    .map(|packets| packets.len() as u64)
                    .unwrap_or(0);
                (port_name.clone(), count)
            })
            .collect();

        // Check if any salvo condition is satisfied
        for (_name, cond) in &node.in_salvo_conditions {
            if evaluate_salvo_condition(&cond.term, &port_packet_counts, &node.in_ports) {
                return true;
            }
        }

        false
    }

    // ========== Undo Implementation ==========

    /// Undo a previously executed action.
    ///
    /// Takes the original action and the events it produced.
    /// Returns `Ok(())` on success, or an error if undo is not possible.
    ///
    /// # Restrictions
    /// - Actions must be undone in reverse order (LIFO)
    /// - State may have changed since the action (undo may fail)
    ///
    /// # Example
    /// ```ignore
    /// let action = NetAction::CreatePacket(None);
    /// let response = net.do_action(&action);
    /// if let NetActionResponse::Success(_, events) = response {
    ///     // Later, to undo:
    ///     net.undo_action(&action, &events)?;
    /// }
    /// ```
    pub fn undo_action(
        &mut self,
        action: &NetAction,
        events: &[NetEvent],
    ) -> Result<(), UndoError> {
        // Process events in reverse order
        for event in events.iter().rev() {
            self.undo_event(action, event)?;
        }
        Ok(())
    }

    /// Undo a single event.
    fn undo_event(&mut self, action: &NetAction, event: &NetEvent) -> Result<(), UndoError> {
        match event {
            NetEvent::PacketCreated(_, packet_id) => self.undo_packet_created(packet_id),
            NetEvent::PacketConsumed(_, packet_id, location) => {
                self.undo_packet_consumed(packet_id, location)
            }
            NetEvent::PacketDestroyed(_, packet_id, location) => {
                self.undo_packet_destroyed(packet_id, location)
            }
            NetEvent::EpochCreated(_, epoch_id) => self.undo_epoch_created(epoch_id),
            NetEvent::EpochStarted(_, epoch_id) => self.undo_epoch_started(epoch_id),
            NetEvent::EpochFinished(_, epoch) => self.undo_epoch_finished(epoch),
            NetEvent::EpochCancelled(_, epoch) => self.undo_epoch_cancelled(epoch),
            NetEvent::PacketMoved(_, packet_id, from, to, from_index) => {
                self.undo_packet_moved(packet_id, from, to, *from_index)
            }
            NetEvent::InputSalvoTriggered(_, _, _) => {
                // Informational only - no state to undo
                Ok(())
            }
            NetEvent::OutputSalvoTriggered(_, epoch_id, _) => {
                // Pop the last out_salvo from the epoch
                self.undo_output_salvo_triggered(epoch_id, action)
            }
        }
    }

    /// Undo PacketCreated: Remove the packet from the network.
    fn undo_packet_created(&mut self, packet_id: &PacketID) -> Result<(), UndoError> {
        // Get packet's location
        let location = match self._packets.get(packet_id) {
            Some(p) => p.location.clone(),
            None => {
                return Err(UndoError::NotFound(format!(
                    "packet {} not found",
                    packet_id
                )));
            }
        };

        // Remove from location index
        if let Some(packets) = self._packets_by_location.get_mut(&location) {
            packets.shift_remove(packet_id);
        }

        // Remove from packets map
        self._packets.remove(packet_id);

        Ok(())
    }

    /// Undo PacketConsumed: Recreate the packet at its previous location.
    fn undo_packet_consumed(
        &mut self,
        packet_id: &PacketID,
        location: &PacketLocation,
    ) -> Result<(), UndoError> {
        self.recreate_packet(packet_id, location)
    }

    /// Undo PacketDestroyed: Recreate the packet at its previous location.
    fn undo_packet_destroyed(
        &mut self,
        packet_id: &PacketID,
        location: &PacketLocation,
    ) -> Result<(), UndoError> {
        self.recreate_packet(packet_id, location)
    }

    /// Helper: Recreate a packet at a given location.
    fn recreate_packet(
        &mut self,
        packet_id: &PacketID,
        location: &PacketLocation,
    ) -> Result<(), UndoError> {
        // Check packet doesn't already exist
        if self._packets.contains_key(packet_id) {
            return Err(UndoError::StateMismatch(format!(
                "packet {} already exists",
                packet_id
            )));
        }

        // Create the packet
        let packet = Packet {
            id: *packet_id,
            location: location.clone(),
        };
        self._packets.insert(*packet_id, packet);

        // Add to location index
        self._packets_by_location
            .entry(location.clone())
            .or_default()
            .insert(*packet_id);

        Ok(())
    }

    /// Undo EpochCreated: Remove the epoch from all indices.
    fn undo_epoch_created(&mut self, epoch_id: &EpochID) -> Result<(), UndoError> {
        // Get epoch info before removing
        let epoch = match self._epochs.get(epoch_id) {
            Some(e) => e.clone(),
            None => {
                return Err(UndoError::NotFound(format!("epoch {} not found", epoch_id)));
            }
        };

        // Remove from _epochs
        self._epochs.remove(epoch_id);

        // Remove from _startable_epochs if present
        self._startable_epochs.remove(epoch_id);

        // Remove from _node_to_epochs (and clean up empty entries)
        if let Some(epoch_ids) = self._node_to_epochs.get_mut(&epoch.node_name) {
            epoch_ids.retain(|id| id != epoch_id);
            // Remove the entry entirely if empty to restore exact state
            if epoch_ids.is_empty() {
                self._node_to_epochs.remove(&epoch.node_name);
            }
        }

        // Remove location entries for the epoch
        let epoch_location = PacketLocation::Node(*epoch_id);
        self._packets_by_location.remove(&epoch_location);

        // Remove output port location entries
        if let Some(node) = self.graph.nodes().get(&epoch.node_name) {
            for port_name in node.out_ports.keys() {
                let output_port_location = PacketLocation::OutputPort(*epoch_id, port_name.clone());
                self._packets_by_location.remove(&output_port_location);
            }
        }

        Ok(())
    }

    /// Undo EpochStarted: Change state back to Startable, add to _startable_epochs.
    fn undo_epoch_started(&mut self, epoch_id: &EpochID) -> Result<(), UndoError> {
        let epoch = match self._epochs.get_mut(epoch_id) {
            Some(e) => e,
            None => {
                return Err(UndoError::NotFound(format!("epoch {} not found", epoch_id)));
            }
        };

        // Verify epoch is in Running state
        if epoch.state != EpochState::Running {
            return Err(UndoError::StateMismatch(format!(
                "epoch {} is not in Running state, cannot undo start",
                epoch_id
            )));
        }

        // Change state back to Startable
        epoch.state = EpochState::Startable;

        // Add back to _startable_epochs
        self._startable_epochs.insert(*epoch_id);

        Ok(())
    }

    /// Undo EpochFinished: Restore the epoch from the event.
    fn undo_epoch_finished(&mut self, epoch: &Epoch) -> Result<(), UndoError> {
        let epoch_id = epoch.id;

        // Check epoch doesn't already exist
        if self._epochs.contains_key(&epoch_id) {
            return Err(UndoError::StateMismatch(format!(
                "epoch {} already exists",
                epoch_id
            )));
        }

        // Restore the epoch with its original state (from before finish)
        // Note: epoch in the event captures state before finish (Running)
        self._epochs.insert(epoch_id, epoch.clone());

        // Recreate location entries
        let epoch_location = PacketLocation::Node(epoch_id);
        self._packets_by_location
            .insert(epoch_location, IndexSet::new());

        // Recreate output port location entries
        if let Some(node) = self.graph.nodes().get(&epoch.node_name) {
            for port_name in node.out_ports.keys() {
                let output_port_location = PacketLocation::OutputPort(epoch_id, port_name.clone());
                self._packets_by_location
                    .insert(output_port_location, IndexSet::new());
            }
        }

        // Add back to _node_to_epochs
        self._node_to_epochs
            .entry(epoch.node_name.clone())
            .or_default()
            .push(epoch_id);

        Ok(())
    }

    /// Undo EpochCancelled: Restore the epoch from the event.
    /// Note: Packets are restored via PacketDestroyed events (processed in reverse order).
    fn undo_epoch_cancelled(&mut self, epoch: &Epoch) -> Result<(), UndoError> {
        let epoch_id = epoch.id;

        // Check epoch doesn't already exist
        if self._epochs.contains_key(&epoch_id) {
            return Err(UndoError::StateMismatch(format!(
                "epoch {} already exists",
                epoch_id
            )));
        }

        // Restore the epoch with its original state
        self._epochs.insert(epoch_id, epoch.clone());

        // Recreate location entries
        let epoch_location = PacketLocation::Node(epoch_id);
        self._packets_by_location
            .insert(epoch_location, IndexSet::new());

        // Recreate output port location entries
        if let Some(node) = self.graph.nodes().get(&epoch.node_name) {
            for port_name in node.out_ports.keys() {
                let output_port_location = PacketLocation::OutputPort(epoch_id, port_name.clone());
                self._packets_by_location
                    .insert(output_port_location, IndexSet::new());
            }
        }

        // Add back to _node_to_epochs
        self._node_to_epochs
            .entry(epoch.node_name.clone())
            .or_default()
            .push(epoch_id);

        // If epoch was startable, add to _startable_epochs
        if epoch.state == EpochState::Startable {
            self._startable_epochs.insert(epoch_id);
        }

        Ok(())
    }

    /// Undo PacketMoved: Move packet back from `to` to `from` at `from_index`.
    fn undo_packet_moved(
        &mut self,
        packet_id: &PacketID,
        from: &PacketLocation,
        to: &PacketLocation,
        from_index: usize,
    ) -> Result<(), UndoError> {
        // Verify packet exists and is at `to` location
        let packet = match self._packets.get(packet_id) {
            Some(p) => p,
            None => {
                return Err(UndoError::NotFound(format!(
                    "packet {} not found",
                    packet_id
                )));
            }
        };

        if packet.location != *to {
            return Err(UndoError::StateMismatch(format!(
                "packet {} is not at expected location {:?}, found at {:?}",
                packet_id, to, packet.location
            )));
        }

        // Remove from `to` location
        if let Some(packets) = self._packets_by_location.get_mut(to) {
            packets.shift_remove(packet_id);
        }

        // Insert back into `from` at original index using shift_insert
        let packets_at_from = self._packets_by_location.entry(from.clone()).or_default();
        packets_at_from.shift_insert(from_index, *packet_id);

        // Update packet's location
        self._packets.get_mut(packet_id).unwrap().location = from.clone();

        Ok(())
    }

    /// Undo OutputSalvoTriggered: Pop the last out_salvo from the epoch.
    fn undo_output_salvo_triggered(
        &mut self,
        epoch_id: &EpochID,
        action: &NetAction,
    ) -> Result<(), UndoError> {
        // Only pop out_salvo for SendOutputSalvo action
        // For RunStep, salvo info isn't stored in out_salvos
        if !matches!(action, NetAction::SendOutputSalvo(_, _)) {
            return Ok(());
        }

        let epoch = match self._epochs.get_mut(epoch_id) {
            Some(e) => e,
            None => {
                return Err(UndoError::NotFound(format!("epoch {} not found", epoch_id)));
            }
        };

        // Pop the last out_salvo
        if epoch.out_salvos.pop().is_none() {
            return Err(UndoError::StateMismatch(format!(
                "epoch {} has no out_salvos to pop",
                epoch_id
            )));
        }

        Ok(())
    }

    // ========== Internal Test Helpers ==========

    #[cfg(test)]
    pub fn startable_epoch_ids(&self) -> Vec<EpochID> {
        self.get_startable_epochs()
    }
}

#[cfg(test)]
#[path = "net_tests.rs"]
mod tests;
