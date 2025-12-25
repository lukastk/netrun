//! Runtime state and operations for flow-based development networks.
//!
//! This module provides the [`Net`] type which tracks the runtime state of a network,
//! including packet locations, epoch lifecycles, and provides actions to control packet flow.
//!
//! All mutations to the network state go through [`Net::do_action`] which accepts a
//! [`NetAction`] and returns a [`NetActionResponse`] containing any events that occurred.

use crate::_utils::get_utc_now;
use crate::graph::{EdgeRef, Graph, NodeName, Port, PortSlotSpec, PortName, PortType, PortRef, SalvoConditionName, SalvoConditionTerm, evaluate_salvo_condition};
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
    Edge(EdgeRef),
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
#[derive(Debug, Clone, PartialEq)]
pub enum EpochState {
    /// Epoch is created but not yet started. External code must call StartEpoch.
    Startable,
    /// Epoch is actively running. Packets can be created, loaded, and sent.
    Running,
    /// Epoch has completed. No further operations are allowed.
    Finished,
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
/// All mutations to [`Net`] state must go through these actions via [`Net::do_action`].
/// This ensures all operations are tracked and produce appropriate events.
#[derive(Debug)]
pub enum NetAction {
    /// Run automatic packet flow until no more progress can be made.
    /// Moves packets from edges to input ports and triggers input salvo conditions.
    RunNetUntilBlocked,
    /// Create a new packet, optionally inside an epoch.
    /// If `None`, packet is created outside the network.
    CreatePacket(Option<EpochID>),
    /// Remove a packet from the network entirely.
    ConsumePacket(PacketID),
    /// Transition a startable epoch to running state.
    StartEpoch(EpochID),
    /// Complete a running epoch. Fails if epoch still contains packets.
    FinishEpoch(EpochID),
    /// Cancel an epoch and destroy all packets inside it.
    CancelEpoch(EpochID),
    /// Manually create and start an epoch with specified packets.
    /// Bypasses the normal salvo condition triggering mechanism.
    CreateAndStartEpoch(NodeName, Salvo),
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

    /// Packet is not inside the specified epoch's node location
    #[error("packet {packet_id} is not inside epoch {epoch_id}")]
    PacketNotInNode { packet_id: PacketID, epoch_id: EpochID },

    /// Output port does not exist on the node
    #[error("output port '{port_name}' not found on node for epoch {epoch_id}")]
    OutputPortNotFound { port_name: PortName, epoch_id: EpochID },

    /// Output salvo condition does not exist on the node
    #[error("output salvo condition '{condition_name}' not found on node for epoch {epoch_id}")]
    OutputSalvoConditionNotFound { condition_name: SalvoConditionName, epoch_id: EpochID },

    /// Maximum number of output salvos reached for this condition
    #[error("max output salvos reached for condition '{condition_name}' on epoch {epoch_id}")]
    MaxOutputSalvosReached { condition_name: SalvoConditionName, epoch_id: EpochID },

    /// Output salvo condition is not satisfied
    #[error("salvo condition '{condition_name}' not met for epoch {epoch_id}")]
    SalvoConditionNotMet { condition_name: SalvoConditionName, epoch_id: EpochID },

    /// Output port has reached its capacity
    #[error("output port '{port_name}' is full for epoch {epoch_id}")]
    OutputPortFull { port_name: PortName, epoch_id: EpochID },

    /// Cannot send packets to an output port that has no connected edge
    #[error("output port '{port_name}' on node '{node_name}' is not connected to any edge")]
    CannotPutPacketIntoUnconnectedOutputPort { port_name: PortName, node_name: NodeName },

    /// Node with the given name was not found in the graph
    #[error("node not found: '{node_name}'")]
    NodeNotFound { node_name: NodeName },

    /// Packet is not at the expected input port
    #[error("packet {packet_id} is not at input port '{port_name}' of node '{node_name}'")]
    PacketNotAtInputPort { packet_id: PacketID, port_name: PortName, node_name: NodeName },

    /// Input port does not exist on the node
    #[error("input port '{port_name}' not found on node '{node_name}'")]
    InputPortNotFound { port_name: PortName, node_name: NodeName },

    /// Input port has reached its capacity
    #[error("input port '{port_name}' on node '{node_name}' is full")]
    InputPortFull { port_name: PortName, node_name: NodeName },

    /// Cannot move packet out of a running epoch
    #[error("cannot move packet {packet_id} out of running epoch {epoch_id}")]
    CannotMovePacketFromRunningEpoch { packet_id: PacketID, epoch_id: EpochID },

    /// Cannot move packet into a running epoch
    #[error("cannot move packet {packet_id} into running epoch {epoch_id}")]
    CannotMovePacketIntoRunningEpoch { packet_id: PacketID, epoch_id: EpochID },

    /// Edge does not exist in the graph
    #[error("edge not found: {edge_ref}")]
    EdgeNotFound { edge_ref: EdgeRef },
}

/// An event that occurred during a network action.
///
/// Events provide a complete audit trail of all state changes in the network.
/// Each event includes a timestamp and relevant identifiers.
#[derive(Debug, Clone)]
pub enum NetEvent {
    /// A new packet was created.
    PacketCreated(EventUTC, PacketID),
    /// A packet was removed from the network.
    PacketConsumed(EventUTC, PacketID),
    /// A new epoch was created (in Startable state).
    EpochCreated(EventUTC, EpochID),
    /// An epoch transitioned from Startable to Running.
    EpochStarted(EventUTC, EpochID),
    /// An epoch completed successfully.
    EpochFinished(EventUTC, EpochID),
    /// An epoch was cancelled.
    EpochCancelled(EventUTC, EpochID),
    /// A packet moved to a new location.
    PacketMoved(EventUTC, PacketID, PacketLocation),
    /// An input salvo condition was triggered, creating an epoch.
    InputSalvoTriggered(EventUTC, EpochID, SalvoConditionName),
    /// An output salvo condition was triggered, sending packets.
    OutputSalvoTriggered(EventUTC, EpochID, SalvoConditionName),
}

/// Data returned by a successful network action.
#[derive(Debug)]
pub enum NetActionResponseData {
    /// A packet ID (returned by CreatePacket).
    Packet(PacketID),
    /// The started epoch (returned by StartEpoch, CreateAndStartEpoch).
    StartedEpoch(Epoch),
    /// The finished epoch (returned by FinishEpoch).
    FinishedEpoch(Epoch),
    /// The cancelled epoch and IDs of destroyed packets (returned by CancelEpoch).
    CancelledEpoch(Epoch, Vec<PacketID>),
    /// No specific data (returned by RunNetUntilBlocked, ConsumePacket, etc.).
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
/// A `Net` is created from a [`Graph`] and tracks:
/// - All packets and their locations
/// - All epochs and their states
/// - Which epochs are startable
///
/// All mutations must go through [`Net::do_action`] to ensure proper event tracking.
#[derive(Debug)]
pub struct Net {
    /// The graph topology this network is running on.
    pub graph: Graph,
    _packets: HashMap<PacketID, Packet>,
    _packets_by_location: HashMap<PacketLocation, IndexSet<PacketID>>,
    _epochs: HashMap<EpochID, Epoch>,
    _startable_epochs: HashSet<EpochID>,
    _node_to_epochs: HashMap<NodeName, Vec<EpochID>>,
}

impl Net {
    /// Creates a new Net from a Graph.
    ///
    /// Initializes packet location tracking for all edges and input ports.
    pub fn new(graph: Graph) -> Self {
        let mut packets_by_location: HashMap<PacketLocation, IndexSet<PacketID>> = HashMap::new();

        // Initialize empty packet sets for all edges
        for edge_ref in graph.edges().keys() {
            packets_by_location.insert(PacketLocation::Edge(edge_ref.clone()), IndexSet::new());
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

        Net {
            graph,
            _packets: HashMap::new(),
            _packets_by_location: packets_by_location,
            _epochs: HashMap::new(),
            _startable_epochs: HashSet::new(),
            _node_to_epochs: HashMap::new(),
        }
    }

    fn move_packet(&mut self, packet_id: &PacketID, new_location: PacketLocation) {
        let packet = self._packets.get_mut(&packet_id).unwrap();
        let packets_at_old_location = self._packets_by_location.get_mut(&packet.location)
            .expect("Packet location has no entry in self._packets_by_location.");
        packets_at_old_location.shift_remove(packet_id);
        packet.location = new_location;
        if !self._packets_by_location.get_mut(&packet.location)
                .expect("Packet location has no entry in self._packets_by_location")
                .insert(packet_id.clone()) {
            panic!("Attempted to move packet to a location that already contains it.");
        }
    }

    // NetActions

    fn run_until_blocked(&mut self) -> NetActionResponse {
        let mut all_events: Vec<NetEvent> = Vec::new();

        loop {
            let mut made_progress = false;

            // Collect all edge locations and their first packet (FIFO)
            // We need to extract data before mutating to avoid borrow issues
            struct EdgeMoveCandidate {
                packet_id: PacketID,
                target_node_name: NodeName,
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
                        let node = self.graph.nodes().get(&target_node_name)
                            .expect("Edge targets a non-existent node");
                        let port = node.in_ports.get(&target_port_name)
                            .expect("Edge targets a non-existent input port");

                        let input_port_location = PacketLocation::InputPort(target_node_name.clone(), target_port_name.clone());
                        let current_count = self._packets_by_location
                            .get(&input_port_location)
                            .map(|packets| packets.len() as u64)
                            .unwrap_or(0);

                        let can_move = match port.slots_spec {
                            PortSlotSpec::Infinite => true,
                            PortSlotSpec::Finite(max_slots) => current_count < max_slots,
                        };

                        edge_candidates.push(EdgeMoveCandidate {
                            packet_id: first_packet_id.clone(),
                            target_node_name,
                            input_port_location,
                            can_move,
                        });
                    }
                }
            }

            // Process each edge that can move a packet
            for candidate in edge_candidates {
                if !candidate.can_move {
                    continue;
                }

                // Move the packet to the input port
                self.move_packet(&candidate.packet_id, candidate.input_port_location.clone());
                all_events.push(NetEvent::PacketMoved(get_utc_now(), candidate.packet_id.clone(), candidate.input_port_location.clone()));
                made_progress = true;

                // Check input salvo conditions on the target node
                // Extract all needed data from the graph first
                let node = self.graph.nodes().get(&candidate.target_node_name)
                    .expect("Edge targets a non-existent node");

                let in_port_names: Vec<PortName> = node.in_ports.keys().cloned().collect();
                let in_ports_clone: HashMap<PortName, Port> = node.in_ports.iter()
                    .map(|(k, v)| (k.clone(), Port { slots_spec: match v.slots_spec {
                        PortSlotSpec::Infinite => PortSlotSpec::Infinite,
                        PortSlotSpec::Finite(n) => PortSlotSpec::Finite(n),
                    }}))
                    .collect();

                // Collect salvo condition data
                struct SalvoConditionData {
                    name: SalvoConditionName,
                    ports: Vec<PortName>,
                    term: SalvoConditionTerm,
                }

                let salvo_conditions: Vec<SalvoConditionData> = node.in_salvo_conditions.iter()
                    .map(|(name, cond)| SalvoConditionData {
                        name: name.clone(),
                        ports: cond.ports.clone(),
                        term: cond.term.clone(),
                    })
                    .collect();

                // Check salvo conditions in order - first satisfied one wins
                for salvo_cond_data in salvo_conditions {
                    // Calculate packet counts for all input ports
                    let port_packet_counts: HashMap<PortName, u64> = in_port_names.iter()
                        .map(|port_name| {
                            let count = self._packets_by_location
                                .get(&PacketLocation::InputPort(candidate.target_node_name.clone(), port_name.clone()))
                                .map(|packets| packets.len() as u64)
                                .unwrap_or(0);
                            (port_name.clone(), count)
                        })
                        .collect();

                    // Check if salvo condition is satisfied
                    if evaluate_salvo_condition(&salvo_cond_data.term, &port_packet_counts, &in_ports_clone) {
                        // Create a new epoch
                        let epoch_id = Ulid::new();

                        // Collect packets from the ports listed in salvo_condition.ports
                        let mut salvo_packets: Vec<(PortName, PacketID)> = Vec::new();
                        let mut packets_to_move: Vec<(PacketID, PortName)> = Vec::new();

                        for port_name in &salvo_cond_data.ports {
                            let port_location = PacketLocation::InputPort(candidate.target_node_name.clone(), port_name.clone());
                            if let Some(packet_ids) = self._packets_by_location.get(&port_location) {
                                for pid in packet_ids.iter() {
                                    salvo_packets.push((port_name.clone(), pid.clone()));
                                    packets_to_move.push((pid.clone(), port_name.clone()));
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
                            id: epoch_id.clone(),
                            node_name: candidate.target_node_name.clone(),
                            in_salvo,
                            out_salvos: Vec::new(),
                            state: EpochState::Startable,
                        };

                        // Register the epoch
                        self._epochs.insert(epoch_id.clone(), epoch);
                        self._startable_epochs.insert(epoch_id.clone());
                        self._node_to_epochs
                            .entry(candidate.target_node_name.clone())
                            .or_insert_with(Vec::new)
                            .push(epoch_id.clone());

                        // Create a location entry for packets inside the epoch
                        let epoch_location = PacketLocation::Node(epoch_id.clone());
                        self._packets_by_location.insert(epoch_location.clone(), IndexSet::new());

                        // Create output port location entries for this epoch
                        let node = self.graph.nodes().get(&candidate.target_node_name)
                            .expect("Node not found for epoch creation");
                        for port_name in node.out_ports.keys() {
                            let output_port_location = PacketLocation::OutputPort(epoch_id.clone(), port_name.clone());
                            self._packets_by_location.insert(output_port_location, IndexSet::new());
                        }

                        // Move packets from input ports into the epoch
                        for (pid, _port_name) in &packets_to_move {
                            self.move_packet(pid, epoch_location.clone());
                            all_events.push(NetEvent::PacketMoved(get_utc_now(), pid.clone(), epoch_location.clone()));
                        }

                        all_events.push(NetEvent::EpochCreated(get_utc_now(), epoch_id.clone()));
                        all_events.push(NetEvent::InputSalvoTriggered(get_utc_now(), epoch_id.clone(), salvo_cond_data.name.clone()));

                        // Only one salvo condition can trigger per node per iteration
                        break;
                    }
                }
            }

            // If no progress was made, we're blocked
            if !made_progress {
                break;
            }
        }

        NetActionResponse::Success(NetActionResponseData::None, all_events)
    }

    fn create_packet(&mut self, maybe_epoch_id: &Option<EpochID>) -> NetActionResponse {
        // Check that epoch_id exists and is running
        if let Some(epoch_id) = maybe_epoch_id {
            if !self._epochs.contains_key(&epoch_id) {
                return NetActionResponse::Error(NetActionError::EpochNotFound {
                    epoch_id: epoch_id.clone(),
                });
            }
            if !matches!(self._epochs[&epoch_id].state, EpochState::Running) {
                return NetActionResponse::Error(NetActionError::EpochNotRunning {
                    epoch_id: epoch_id.clone(),
                });
            }
        }

        let packet_location = match maybe_epoch_id {
            Some(epoch_id) => PacketLocation::Node(epoch_id.clone()),
            None => PacketLocation::OutsideNet,
        };

        let packet = Packet {
            id: Ulid::new(),
            location: packet_location.clone(),
        };

        let packet_id = packet.id.clone();
        self._packets.insert(packet.id.clone(), packet);

        // Add packet to location index
        self._packets_by_location
            .entry(packet_location)
            .or_insert_with(IndexSet::new)
            .insert(packet_id.clone());

        NetActionResponse::Success(
            NetActionResponseData::Packet(packet_id),
            vec![NetEvent::PacketCreated(get_utc_now(), packet_id)]
        )
    }

    fn consume_packet(&mut self, packet_id: &PacketID) -> NetActionResponse {
        if !self._packets.contains_key(packet_id) {
            return NetActionResponse::Error(NetActionError::PacketNotFound {
                packet_id: packet_id.clone(),
            });
        }

        if let Some(packets) = self
            ._packets_by_location
            .get_mut(&self._packets[packet_id].location)
        {
            if packets.shift_remove(packet_id) {
                self._packets.remove(packet_id);
                NetActionResponse::Success(
                    NetActionResponseData::None,
                    vec![NetEvent::PacketConsumed(get_utc_now(), packet_id.clone())]
                )
            } else {
                panic!(
                    "Packet with ID {} not found in location {:?}",
                    packet_id, self._packets[packet_id].location
                );
            }
        } else {
            panic!("Packet location {:?} not found", self._packets[packet_id].location);
        }
    }

    fn start_epoch(&mut self, epoch_id: &EpochID) -> NetActionResponse {
        if let Some(epoch) = self._epochs.get_mut(epoch_id) {
            if !self._startable_epochs.contains(epoch_id) {
                return NetActionResponse::Error(NetActionError::EpochNotStartable {
                    epoch_id: epoch_id.clone(),
                });
            }
            debug_assert!(matches!(epoch.state, EpochState::Startable),
                "Epoch state is not Startable but was in net._startable_epochs.");
            epoch.state = EpochState::Running;
            self._startable_epochs.remove(epoch_id);
            NetActionResponse::Success(
                NetActionResponseData::StartedEpoch(epoch.clone()),
                vec![NetEvent::EpochStarted(get_utc_now(), epoch_id.clone())]
            )
        } else {
            return NetActionResponse::Error(NetActionError::EpochNotFound {
                epoch_id: epoch_id.clone(),
            });
        }
    }

    fn finish_epoch(&mut self, epoch_id: &EpochID) -> NetActionResponse {
        if let Some(epoch) = self._epochs.get(&epoch_id) {
            if let EpochState::Running = epoch.state {
                // No packets may remain in the epoch by the time it has ended.
                let epoch_loc = PacketLocation::Node(epoch_id.clone());
                if let Some(packets) = self._packets_by_location.get(&epoch_loc) {
                    if packets.len() > 0 {
                        return NetActionResponse::Error(NetActionError::CannotFinishNonEmptyEpoch {
                            epoch_id: epoch_id.clone(),
                        });
                    }

                    let mut epoch = self._epochs.remove(&epoch_id).unwrap();
                    epoch.state = EpochState::Finished;
                    self._packets_by_location.remove(&epoch_loc);
                    NetActionResponse::Success(
                        NetActionResponseData::FinishedEpoch(epoch),
                        vec![NetEvent::EpochFinished(get_utc_now(), epoch_id.clone())]
                    )
                } else {
                    panic!("Epoch {} not found in location {:?}", epoch_id, epoch_loc);
                }
            } else {
                return NetActionResponse::Error(NetActionError::EpochNotRunning {
                    epoch_id: epoch_id.clone(),
                });
            }
        } else {
            return NetActionResponse::Error(NetActionError::EpochNotFound {
                epoch_id: epoch_id.clone(),
            });
        }
    }

    fn cancel_epoch(&mut self, epoch_id: &EpochID) -> NetActionResponse {
        // Check if epoch exists
        let epoch = if let Some(epoch) = self._epochs.get(epoch_id) {
            epoch.clone()
        } else {
            return NetActionResponse::Error(NetActionError::EpochNotFound {
                epoch_id: epoch_id.clone(),
            });
        };

        let mut events: Vec<NetEvent> = Vec::new();
        let mut destroyed_packets: Vec<PacketID> = Vec::new();

        // Collect packets inside the epoch (Node location)
        let epoch_location = PacketLocation::Node(epoch_id.clone());
        if let Some(packet_ids) = self._packets_by_location.get(&epoch_location) {
            destroyed_packets.extend(packet_ids.iter().cloned());
        }

        // Collect packets in the epoch's output ports
        let node = self.graph.nodes().get(&epoch.node_name)
            .expect("Epoch references non-existent node");
        for port_name in node.out_ports.keys() {
            let output_port_location = PacketLocation::OutputPort(epoch_id.clone(), port_name.clone());
            if let Some(packet_ids) = self._packets_by_location.get(&output_port_location) {
                destroyed_packets.extend(packet_ids.iter().cloned());
            }
        }

        // Remove packets from _packets and _packets_by_location, emit events
        for packet_id in &destroyed_packets {
            let packet = self._packets.remove(packet_id)
                .expect("Packet in location map not found in packets map");
            if let Some(packets_at_location) = self._packets_by_location.get_mut(&packet.location) {
                packets_at_location.shift_remove(packet_id);
            }
            events.push(NetEvent::PacketConsumed(get_utc_now(), packet_id.clone()));
        }

        // Remove output port location entries for this epoch
        for port_name in node.out_ports.keys() {
            let output_port_location = PacketLocation::OutputPort(epoch_id.clone(), port_name.clone());
            self._packets_by_location.remove(&output_port_location);
        }

        // Remove the epoch's node location entry
        self._packets_by_location.remove(&epoch_location);

        // Update _startable_epochs if epoch was startable
        self._startable_epochs.remove(epoch_id);

        // Update _node_to_epochs
        if let Some(epoch_ids) = self._node_to_epochs.get_mut(&epoch.node_name) {
            epoch_ids.retain(|id| id != epoch_id);
        }

        // Remove epoch from _epochs
        let epoch = self._epochs.remove(epoch_id)
            .expect("Epoch should exist");

        events.push(NetEvent::EpochCancelled(get_utc_now(), epoch_id.clone()));

        NetActionResponse::Success(
            NetActionResponseData::CancelledEpoch(epoch, destroyed_packets),
            events
        )
    }

    fn create_and_start_epoch(&mut self, node_name: &NodeName, salvo: &Salvo) -> NetActionResponse {
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
                        packet_id: packet_id.clone(),
                    });
                }
            };

            // Validate packet is at the input port of this node
            let expected_location = PacketLocation::InputPort(node_name.clone(), port_name.clone());
            if packet.location != expected_location {
                return NetActionResponse::Error(NetActionError::PacketNotAtInputPort {
                    packet_id: packet_id.clone(),
                    port_name: port_name.clone(),
                    node_name: node_name.clone(),
                });
            }
        }

        let mut events: Vec<NetEvent> = Vec::new();

        // Create the epoch
        let epoch_id = Ulid::new();
        let epoch = Epoch {
            id: epoch_id.clone(),
            node_name: node_name.clone(),
            in_salvo: salvo.clone(),
            out_salvos: Vec::new(),
            state: EpochState::Running,
        };

        // Register the epoch
        self._epochs.insert(epoch_id.clone(), epoch.clone());
        self._node_to_epochs
            .entry(node_name.clone())
            .or_insert_with(Vec::new)
            .push(epoch_id.clone());

        // Create location entry for packets inside the epoch
        let epoch_location = PacketLocation::Node(epoch_id.clone());
        self._packets_by_location.insert(epoch_location.clone(), IndexSet::new());

        // Create output port location entries for this epoch
        for port_name in node.out_ports.keys() {
            let output_port_location = PacketLocation::OutputPort(epoch_id.clone(), port_name.clone());
            self._packets_by_location.insert(output_port_location, IndexSet::new());
        }

        events.push(NetEvent::EpochCreated(get_utc_now(), epoch_id.clone()));

        // Move packets from input ports into the epoch
        for (_, packet_id) in &salvo.packets {
            self.move_packet(packet_id, epoch_location.clone());
            events.push(NetEvent::PacketMoved(get_utc_now(), packet_id.clone(), epoch_location.clone()));
        }

        events.push(NetEvent::EpochStarted(get_utc_now(), epoch_id.clone()));

        NetActionResponse::Success(
            NetActionResponseData::StartedEpoch(epoch),
            events
        )
    }

    fn load_packet_into_output_port(&mut self, packet_id: &PacketID, port_name: &String) -> NetActionResponse {
        let (epoch_id, old_location) = if let Some(packet) = self._packets.get(packet_id) {
            if let PacketLocation::Node(epoch_id) = packet.location {
                (epoch_id, packet.location.clone())
            } else {
                // We don't know the epoch_id since the packet isn't in a node
                // Use a placeholder - this is an edge case where we can't provide full context
                return NetActionResponse::Error(NetActionError::PacketNotInNode {
                    packet_id: packet_id.clone(),
                    epoch_id: Ulid::nil(), // Placeholder since packet isn't in any epoch
                })
            }
        } else {
            return NetActionResponse::Error(NetActionError::PacketNotFound {
                packet_id: packet_id.clone(),
            });
        };

        let node_name = self._epochs.get(&epoch_id)
            .expect("The epoch id in the location of a packet could not be found.")
            .node_name.clone();
        let node = self.graph.nodes().get(&node_name)
            .expect("Packet located in a non-existing node (yet the node has an epoch).");

        if !node.out_ports.contains_key(port_name) {
            return NetActionResponse::Error(NetActionError::OutputPortNotFound {
                port_name: port_name.clone(),
                epoch_id: epoch_id.clone(),
            })
        }

        let port = node.out_ports.get(port_name).unwrap();
        let port_packets = self._packets_by_location.get(&old_location)
            .expect("No entry in Net._packets_by_location found for output port.");

        // Check if the output port is full
        if let PortSlotSpec::Finite(num_slots) = port.slots_spec {
            if num_slots >= port_packets.len() as u64 {
                return NetActionResponse::Error(NetActionError::OutputPortFull {
                    port_name: port_name.clone(),
                    epoch_id: epoch_id.clone(),
                })
            }
        }

        let new_location = PacketLocation::OutputPort(epoch_id, port_name.clone());
        self.move_packet(&packet_id, new_location.clone());
        NetActionResponse::Success(
            NetActionResponseData::None,
            vec![NetEvent::PacketMoved(get_utc_now(), epoch_id, new_location)]
        )
    }

    fn send_output_salvo(&mut self, epoch_id: &EpochID, salvo_condition_name: &SalvoConditionName) -> NetActionResponse {
        // Get epoch
        let epoch = if let Some(epoch) = self._epochs.get(epoch_id) {
            epoch
        } else {
            return NetActionResponse::Error(NetActionError::EpochNotFound {
                epoch_id: epoch_id.clone(),
            });
        };

        // Get node
        let node = self.graph.nodes().get(&epoch.node_name)
            .expect("Node associated with epoch could not be found.");

        // Get salvo condition
        let salvo_condition = if let Some(salvo_condition) = node.out_salvo_conditions.get(salvo_condition_name) {
            salvo_condition
        } else {
            return NetActionResponse::Error(NetActionError::OutputSalvoConditionNotFound {
                condition_name: salvo_condition_name.clone(),
                epoch_id: epoch_id.clone(),
            });
        };

        // Check if max salvos reached (max_salvos = 0 means unlimited)
        if salvo_condition.max_salvos > 0 && epoch.out_salvos.len() as u64 >= salvo_condition.max_salvos {
            return NetActionResponse::Error(NetActionError::MaxOutputSalvosReached {
                condition_name: salvo_condition_name.clone(),
                epoch_id: epoch_id.clone(),
            });
        }

        // Check that the salvo condition is met
        let port_packet_counts: HashMap<PortName, u64> = node.out_ports.keys()
            .map(|port_name| {
                let count = self._packets_by_location
                    .get(&PacketLocation::OutputPort(epoch_id.clone(), port_name.clone()))
                    .map(|packets| packets.len() as u64)
                    .unwrap_or(0);
                (port_name.clone(), count)
            })
            .collect();
        if !evaluate_salvo_condition(&salvo_condition.term, &port_packet_counts, &node.out_ports) {
            return NetActionResponse::Error(NetActionError::SalvoConditionNotMet {
                condition_name: salvo_condition_name.clone(),
                epoch_id: epoch_id.clone(),
            });
        }

        // Get the locations to send packets to
        let mut packets_to_move: Vec<(PacketID, PortName, PacketLocation)> = Vec::new();
        for port_name in &salvo_condition.ports {
            let packets = self._packets_by_location.get(&PacketLocation::OutputPort(epoch_id.clone(), port_name.clone()))
                .expect(format!("Output port '{}' of node '{}' does not have an entry in self._packets_by_location", port_name, node.name.clone()).as_str())
                .clone();
            let edge_ref = if let Some(edge_ref) = self.graph.get_edge_by_tail(&PortRef { node_name: node.name.clone(), port_type: PortType::Output, port_name: port_name.clone() }) {
                edge_ref.clone()
            } else {
                return NetActionResponse::Error(NetActionError::CannotPutPacketIntoUnconnectedOutputPort {
                    port_name: port_name.clone(),
                    node_name: node.name.clone(),
                });
            };
            let new_location = PacketLocation::Edge(edge_ref.clone());
            for packet_id in packets {
                packets_to_move.push((packet_id.clone(), port_name.clone(), new_location.clone()));
            }
        }

        // Create a Salvo and add it to the epoch
        let salvo = Salvo {
            salvo_condition: salvo_condition_name.clone(),
            packets: packets_to_move.iter().map(|(packet_id, port_name, _)| {
                (port_name.clone(), packet_id.clone())
            }).collect()
        };
        self._epochs.get_mut(&epoch_id).unwrap().out_salvos.push(salvo);

        // Move packets
        let mut net_events = Vec::new();
        for (packet_id, _port_name, new_location) in packets_to_move {
            net_events.push(NetEvent::PacketMoved(get_utc_now(), packet_id.clone(), new_location.clone()));
            self.move_packet(&packet_id, new_location);
        }

        NetActionResponse::Success(
            NetActionResponseData::None,
            net_events
        )
    }

    fn transport_packet_to_location(&mut self, packet_id: &PacketID, destination: &PacketLocation) -> NetActionResponse {
        // Validate packet exists
        let packet = if let Some(p) = self._packets.get(packet_id) {
            p
        } else {
            return NetActionResponse::Error(NetActionError::PacketNotFound {
                packet_id: packet_id.clone(),
            });
        };
        let current_location = packet.location.clone();

        // Check if moving FROM a running epoch
        match &current_location {
            PacketLocation::Node(epoch_id) => {
                if let Some(epoch) = self._epochs.get(epoch_id) {
                    if epoch.state == EpochState::Running {
                        return NetActionResponse::Error(NetActionError::CannotMovePacketFromRunningEpoch {
                            packet_id: packet_id.clone(),
                            epoch_id: epoch_id.clone(),
                        });
                    }
                }
            }
            PacketLocation::OutputPort(epoch_id, _) => {
                if let Some(epoch) = self._epochs.get(epoch_id) {
                    if epoch.state == EpochState::Running {
                        return NetActionResponse::Error(NetActionError::CannotMovePacketFromRunningEpoch {
                            packet_id: packet_id.clone(),
                            epoch_id: epoch_id.clone(),
                        });
                    }
                }
            }
            _ => {}
        }

        // Check if moving TO a running epoch
        match destination {
            PacketLocation::Node(epoch_id) => {
                if let Some(epoch) = self._epochs.get(epoch_id) {
                    if epoch.state == EpochState::Running {
                        return NetActionResponse::Error(NetActionError::CannotMovePacketIntoRunningEpoch {
                            packet_id: packet_id.clone(),
                            epoch_id: epoch_id.clone(),
                        });
                    }
                } else {
                    return NetActionResponse::Error(NetActionError::EpochNotFound {
                        epoch_id: epoch_id.clone(),
                    });
                }
            }
            PacketLocation::OutputPort(epoch_id, port_name) => {
                if let Some(epoch) = self._epochs.get(epoch_id) {
                    if epoch.state == EpochState::Running {
                        return NetActionResponse::Error(NetActionError::CannotMovePacketIntoRunningEpoch {
                            packet_id: packet_id.clone(),
                            epoch_id: epoch_id.clone(),
                        });
                    }
                    // Check that output port exists on the node
                    let node = self.graph.nodes().get(&epoch.node_name)
                        .expect("Node associated with epoch could not be found.");
                    if !node.out_ports.contains_key(port_name) {
                        return NetActionResponse::Error(NetActionError::OutputPortNotFound {
                            port_name: port_name.clone(),
                            epoch_id: epoch_id.clone(),
                        });
                    }
                } else {
                    return NetActionResponse::Error(NetActionError::EpochNotFound {
                        epoch_id: epoch_id.clone(),
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
                let current_count = self._packets_by_location
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
            PacketLocation::Edge(edge_ref) => {
                // Check edge exists in graph
                if !self.graph.edges().contains_key(edge_ref) {
                    return NetActionResponse::Error(NetActionError::EdgeNotFound {
                        edge_ref: edge_ref.clone(),
                    });
                }
            }
            PacketLocation::OutsideNet => {
                // Always allowed
            }
        }

        // Move the packet
        self.move_packet(packet_id, destination.clone());

        NetActionResponse::Success(
            NetActionResponseData::None,
            vec![NetEvent::PacketMoved(get_utc_now(), packet_id.clone(), destination.clone())]
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
    /// use netrun_sim::net::{Net, NetAction, NetActionResponse, NetActionResponseData};
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
    /// let mut net = Net::new(graph);
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
            NetAction::RunNetUntilBlocked => self.run_until_blocked(),
            NetAction::CreatePacket(maybe_epoch_id) => self.create_packet(maybe_epoch_id),
            NetAction::ConsumePacket(packet_id) => self.consume_packet(packet_id),
            NetAction::StartEpoch(epoch_id) => self.start_epoch(epoch_id),
            NetAction::FinishEpoch(epoch_id) => self.finish_epoch(epoch_id),
            NetAction::CancelEpoch(epoch_id) => self.cancel_epoch(epoch_id),
            NetAction::CreateAndStartEpoch(node_name, salvo) => self.create_and_start_epoch(node_name, salvo),
            NetAction::LoadPacketIntoOutputPort(packet_id, port_name) => self.load_packet_into_output_port(packet_id, port_name),
            NetAction::SendOutputSalvo(epoch_id, salvo_condition_name) => self.send_output_salvo(epoch_id, salvo_condition_name),
            NetAction::TransportPacketToLocation(packet_id, location) => self.transport_packet_to_location(packet_id, location),
        }
    }

    // ========== Public Accessors ==========

    /// Get the number of packets at a given location.
    pub fn packet_count_at(&self, location: &PacketLocation) -> usize {
        self._packets_by_location.get(location).map(|s| s.len()).unwrap_or(0)
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

    // ========== Internal Test Helpers ==========

    #[cfg(test)]
    pub fn startable_epoch_ids(&self) -> Vec<EpochID> {
        self.get_startable_epochs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::*;

    // Helper to extract packet ID from create response
    fn get_packet_id(response: &NetActionResponse) -> PacketID {
        match response {
            NetActionResponse::Success(NetActionResponseData::Packet(id), _) => id.clone(),
            _ => panic!("Expected Packet response, got: {:?}", response),
        }
    }

    // Helper to extract epoch from start response
    fn get_started_epoch(response: &NetActionResponse) -> Epoch {
        match response {
            NetActionResponse::Success(NetActionResponseData::StartedEpoch(epoch), _) => epoch.clone(),
            _ => panic!("Expected StartedEpoch response, got: {:?}", response),
        }
    }

    // ========== Packet Creation and Consumption Tests ==========

    #[test]
    fn test_create_packet_outside_net() {
        let graph = linear_graph_3();
        let mut net = Net::new(graph);

        let response = net.do_action(&NetAction::CreatePacket(None));
        assert!(matches!(response, NetActionResponse::Success(NetActionResponseData::Packet(_), _)));
    }

    #[test]
    fn test_consume_packet() {
        let graph = linear_graph_3();
        let mut net = Net::new(graph);

        // Create a packet
        let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

        // Consume it
        let response = net.do_action(&NetAction::ConsumePacket(packet_id));
        assert!(matches!(response, NetActionResponse::Success(NetActionResponseData::None, _)));
    }

    #[test]
    fn test_consume_nonexistent_packet_fails() {
        let graph = linear_graph_3();
        let mut net = Net::new(graph);

        let fake_id = Ulid::new();
        let response = net.do_action(&NetAction::ConsumePacket(fake_id));
        assert!(matches!(response, NetActionResponse::Error(NetActionError::PacketNotFound { .. })));
    }

    // ========== Epoch Lifecycle Tests ==========

    #[test]
    fn test_epoch_lifecycle_via_run_until_blocked() {
        let graph = linear_graph_3();
        let mut net = Net::new(graph);

        // Put a packet on edge A->B
        let response = net.do_action(&NetAction::CreatePacket(None));
        let packet_id = get_packet_id(&response);

        // Manually place packet on edge (simulating it came from node A)
        let edge_location = PacketLocation::Edge(EdgeRef {
            source: PortRef {
                node_name: "A".to_string(),
                port_type: PortType::Output,
                port_name: "out".to_string(),
            },
            target: PortRef {
                node_name: "B".to_string(),
                port_type: PortType::Input,
                port_name: "in".to_string(),
            },
        });
        net._packets.get_mut(&packet_id).unwrap().location = edge_location.clone();
        net._packets_by_location.get_mut(&PacketLocation::OutsideNet).unwrap().shift_remove(&packet_id);
        net._packets_by_location.get_mut(&edge_location).unwrap().insert(packet_id.clone());

        // Run until blocked - packet should move to input port and trigger epoch
        net.do_action(&NetAction::RunNetUntilBlocked);

        // Should have one startable epoch
        let startable = net.startable_epoch_ids();
        assert_eq!(startable.len(), 1);

        // Start the epoch
        let epoch_id = startable[0].clone();
        let epoch = get_started_epoch(&net.do_action(&NetAction::StartEpoch(epoch_id.clone())));
        assert!(matches!(epoch.state, EpochState::Running));

        // Consume the packet (simulating node processing)
        net.do_action(&NetAction::ConsumePacket(packet_id));

        // Finish the epoch
        let response = net.do_action(&NetAction::FinishEpoch(epoch_id));
        assert!(matches!(response, NetActionResponse::Success(NetActionResponseData::FinishedEpoch(_), _)));
    }

    #[test]
    fn test_cannot_start_nonexistent_epoch() {
        let graph = linear_graph_3();
        let mut net = Net::new(graph);

        let fake_id = Ulid::new();
        let response = net.do_action(&NetAction::StartEpoch(fake_id));
        assert!(matches!(response, NetActionResponse::Error(NetActionError::EpochNotFound { .. })));
    }

    #[test]
    fn test_cannot_finish_epoch_with_packets() {
        let graph = linear_graph_3();
        let mut net = Net::new(graph);

        // Create epoch with packet via create_and_start_epoch
        // First put packet at input port
        let response = net.do_action(&NetAction::CreatePacket(None));
        let packet_id = get_packet_id(&response);

        let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
        net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
        net._packets_by_location.get_mut(&PacketLocation::OutsideNet).unwrap().shift_remove(&packet_id);
        net._packets_by_location.get_mut(&input_port_loc).unwrap().insert(packet_id.clone());

        // Create and start epoch
        let salvo = Salvo {
            salvo_condition: "manual".to_string(),
            packets: vec![("in".to_string(), packet_id)],
        };
        let epoch = get_started_epoch(&net.do_action(&NetAction::CreateAndStartEpoch("B".to_string(), salvo)));

        // Try to finish without consuming packet
        let response = net.do_action(&NetAction::FinishEpoch(epoch.id));
        assert!(matches!(response, NetActionResponse::Error(NetActionError::CannotFinishNonEmptyEpoch { .. })));
    }

    // ========== Cancel Epoch Tests ==========

    #[test]
    fn test_cancel_epoch_destroys_packets() {
        let graph = linear_graph_3();
        let mut net = Net::new(graph);

        // Create packet and place at input port
        let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
        let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
        net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
        net._packets_by_location.get_mut(&PacketLocation::OutsideNet).unwrap().shift_remove(&packet_id);
        net._packets_by_location.get_mut(&input_port_loc).unwrap().insert(packet_id.clone());

        // Create and start epoch
        let salvo = Salvo {
            salvo_condition: "manual".to_string(),
            packets: vec![("in".to_string(), packet_id.clone())],
        };
        let epoch = get_started_epoch(&net.do_action(&NetAction::CreateAndStartEpoch("B".to_string(), salvo)));

        // Cancel the epoch
        let response = net.do_action(&NetAction::CancelEpoch(epoch.id));
        match response {
            NetActionResponse::Success(NetActionResponseData::CancelledEpoch(_, destroyed), _) => {
                assert_eq!(destroyed.len(), 1);
                assert_eq!(destroyed[0], packet_id);
            }
            _ => panic!("Expected CancelledEpoch response"),
        }

        // Packet should be gone
        assert!(!net._packets.contains_key(&packet_id));
    }

    // ========== Run Until Blocked Tests ==========

    #[test]
    fn test_run_until_blocked_moves_packet_to_input_port() {
        let graph = linear_graph_3();
        let mut net = Net::new(graph);

        // Create packet on edge A->B
        let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
        let edge_location = PacketLocation::Edge(EdgeRef {
            source: PortRef {
                node_name: "A".to_string(),
                port_type: PortType::Output,
                port_name: "out".to_string(),
            },
            target: PortRef {
                node_name: "B".to_string(),
                port_type: PortType::Input,
                port_name: "in".to_string(),
            },
        });
        net._packets.get_mut(&packet_id).unwrap().location = edge_location.clone();
        net._packets_by_location.get_mut(&PacketLocation::OutsideNet).unwrap().shift_remove(&packet_id);
        net._packets_by_location.get_mut(&edge_location).unwrap().insert(packet_id.clone());

        // Run until blocked
        net.do_action(&NetAction::RunNetUntilBlocked);

        // Packet should have triggered an epoch (moved into node)
        assert_eq!(net.startable_epoch_ids().len(), 1);
    }

    #[test]
    fn test_run_until_blocked_respects_port_capacity() {
        // Create a graph where node B has finite capacity input port but NO salvo conditions
        // This tests that port capacity limits how many packets can wait at the input port
        use std::collections::HashMap;
        use crate::graph::Node;
        let node_b = Node {
            name: "B".to_string(),
            in_ports: {
                let mut ports = HashMap::new();
                ports.insert("in".to_string(), Port { slots_spec: PortSlotSpec::Finite(1) });
                ports
            },
            out_ports: HashMap::new(),
            in_salvo_conditions: HashMap::new(),  // No salvo conditions = packets wait at input port
            out_salvo_conditions: HashMap::new(),
        };

        let nodes = vec![
            simple_node("A", vec![], vec!["out"]),
            node_b,
        ];
        let edges = vec![edge("A", "out", "B", "in")];
        let graph = Graph::new(nodes, edges);
        let mut net = Net::new(graph);

        // Create two packets on edge A->B
        let edge_location = PacketLocation::Edge(EdgeRef {
            source: PortRef {
                node_name: "A".to_string(),
                port_type: PortType::Output,
                port_name: "out".to_string(),
            },
            target: PortRef {
                node_name: "B".to_string(),
                port_type: PortType::Input,
                port_name: "in".to_string(),
            },
        });

        let packet1 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
        let packet2 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

        // Move both to edge
        for pid in [&packet1, &packet2] {
            net._packets.get_mut(pid).unwrap().location = edge_location.clone();
            net._packets_by_location.get_mut(&PacketLocation::OutsideNet).unwrap().shift_remove(pid);
            net._packets_by_location.get_mut(&edge_location).unwrap().insert(pid.clone());
        }

        // Run until blocked - only first packet should move (capacity = 1)
        net.do_action(&NetAction::RunNetUntilBlocked);

        // No epochs created (no salvo conditions), one packet at input port, one still on edge
        assert_eq!(net.startable_epoch_ids().len(), 0);
        let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
        assert_eq!(net.packet_count_at(&input_port_loc), 1);
        assert_eq!(net.packet_count_at(&edge_location), 1);
    }

    #[test]
    fn test_fifo_packet_ordering() {
        // Test that packets are processed in FIFO order on edges
        // We verify this by examining the events emitted during run_until_blocked
        let graph = linear_graph_3();
        let mut net = Net::new(graph);

        let edge_location = PacketLocation::Edge(EdgeRef {
            source: PortRef {
                node_name: "A".to_string(),
                port_type: PortType::Output,
                port_name: "out".to_string(),
            },
            target: PortRef {
                node_name: "B".to_string(),
                port_type: PortType::Input,
                port_name: "in".to_string(),
            },
        });

        // Create packets in order
        let packet1 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
        let packet2 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
        let packet3 = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

        // Add to edge in order (packet1 first)
        for pid in [&packet1, &packet2, &packet3] {
            net._packets.get_mut(pid).unwrap().location = edge_location.clone();
            net._packets_by_location.get_mut(&PacketLocation::OutsideNet).unwrap().shift_remove(pid);
            net._packets_by_location.get_mut(&edge_location).unwrap().insert(pid.clone());
        }

        // Run - each packet triggers its own epoch (default salvo condition)
        let response = net.do_action(&NetAction::RunNetUntilBlocked);

        // Extract PacketMoved events to verify FIFO order
        let events = match response {
            NetActionResponse::Success(_, events) => events,
            _ => panic!("Expected success response"),
        };

        // Find the first PacketMoved event for each packet (when it moved from edge to input port)
        // The order of these events should match the FIFO order: packet1, packet2, packet3
        let packet_move_order: Vec<PacketID> = events.iter()
            .filter_map(|event| {
                if let NetEvent::PacketMoved(_, packet_id, PacketLocation::InputPort(_, _)) = event {
                    Some(packet_id.clone())
                } else {
                    None
                }
            })
            .collect();

        // We should have 3 packet moves to input ports, in FIFO order
        assert_eq!(packet_move_order.len(), 3, "Expected 3 packets to move to input port");
        assert_eq!(packet_move_order[0], packet1, "First packet to move should be packet1");
        assert_eq!(packet_move_order[1], packet2, "Second packet to move should be packet2");
        assert_eq!(packet_move_order[2], packet3, "Third packet to move should be packet3");
    }

    // ========== Output Salvo Tests ==========

    #[test]
    fn test_load_packet_into_output_port() {
        let graph = linear_graph_3();
        let mut net = Net::new(graph);

        // Create packet and place at input port of B
        let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
        let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
        net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
        net._packets_by_location.get_mut(&PacketLocation::OutsideNet).unwrap().shift_remove(&packet_id);
        net._packets_by_location.get_mut(&input_port_loc).unwrap().insert(packet_id.clone());

        // Create and start epoch
        let salvo = Salvo {
            salvo_condition: "manual".to_string(),
            packets: vec![("in".to_string(), packet_id.clone())],
        };
        let epoch = get_started_epoch(&net.do_action(&NetAction::CreateAndStartEpoch("B".to_string(), salvo)));

        // Load packet into output port
        let response = net.do_action(&NetAction::LoadPacketIntoOutputPort(packet_id.clone(), "out".to_string()));
        assert!(matches!(response, NetActionResponse::Success(NetActionResponseData::None, _)));

        // Packet should be at output port
        let output_loc = PacketLocation::OutputPort(epoch.id, "out".to_string());
        assert_eq!(net.packet_count_at(&output_loc), 1);
    }

    #[test]
    fn test_send_output_salvo() {
        let graph = linear_graph_3();
        let mut net = Net::new(graph);

        // Create packet and place at input port of B
        let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
        let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
        net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
        net._packets_by_location.get_mut(&PacketLocation::OutsideNet).unwrap().shift_remove(&packet_id);
        net._packets_by_location.get_mut(&input_port_loc).unwrap().insert(packet_id.clone());

        // Create and start epoch
        let salvo = Salvo {
            salvo_condition: "manual".to_string(),
            packets: vec![("in".to_string(), packet_id.clone())],
        };
        let epoch = get_started_epoch(&net.do_action(&NetAction::CreateAndStartEpoch("B".to_string(), salvo)));

        // Load packet into output port
        net.do_action(&NetAction::LoadPacketIntoOutputPort(packet_id.clone(), "out".to_string()));

        // Send output salvo
        let response = net.do_action(&NetAction::SendOutputSalvo(epoch.id.clone(), "default".to_string()));
        assert!(matches!(response, NetActionResponse::Success(NetActionResponseData::None, _)));

        // Packet should now be on edge B->C
        let edge_loc = PacketLocation::Edge(EdgeRef {
            source: PortRef {
                node_name: "B".to_string(),
                port_type: PortType::Output,
                port_name: "out".to_string(),
            },
            target: PortRef {
                node_name: "C".to_string(),
                port_type: PortType::Input,
                port_name: "in".to_string(),
            },
        });
        assert_eq!(net.packet_count_at(&edge_loc), 1);
    }

    // ========== Create And Start Epoch Tests ==========

    #[test]
    fn test_create_and_start_epoch() {
        let graph = linear_graph_3();
        let mut net = Net::new(graph);

        // Create packet at input port
        let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));
        let input_port_loc = PacketLocation::InputPort("B".to_string(), "in".to_string());
        net._packets.get_mut(&packet_id).unwrap().location = input_port_loc.clone();
        net._packets_by_location.get_mut(&PacketLocation::OutsideNet).unwrap().shift_remove(&packet_id);
        net._packets_by_location.get_mut(&input_port_loc).unwrap().insert(packet_id.clone());

        // Create and start epoch manually
        let salvo = Salvo {
            salvo_condition: "manual".to_string(),
            packets: vec![("in".to_string(), packet_id.clone())],
        };
        let epoch = get_started_epoch(&net.do_action(&NetAction::CreateAndStartEpoch("B".to_string(), salvo)));

        assert!(matches!(epoch.state, EpochState::Running));
        assert_eq!(epoch.node_name, "B");
    }

    #[test]
    fn test_create_and_start_epoch_validates_node() {
        let graph = linear_graph_3();
        let mut net = Net::new(graph);

        let salvo = Salvo {
            salvo_condition: "manual".to_string(),
            packets: vec![],
        };
        let response = net.do_action(&NetAction::CreateAndStartEpoch("NonExistent".to_string(), salvo));
        assert!(matches!(response, NetActionResponse::Error(NetActionError::NodeNotFound { .. })));
    }

    #[test]
    fn test_create_and_start_epoch_validates_packet_location() {
        let graph = linear_graph_3();
        let mut net = Net::new(graph);

        // Create packet but leave it OutsideNet
        let packet_id = get_packet_id(&net.do_action(&NetAction::CreatePacket(None)));

        let salvo = Salvo {
            salvo_condition: "manual".to_string(),
            packets: vec![("in".to_string(), packet_id)],
        };
        let response = net.do_action(&NetAction::CreateAndStartEpoch("B".to_string(), salvo));
        assert!(matches!(response, NetActionResponse::Error(NetActionError::PacketNotAtInputPort { .. })));
    }
}
