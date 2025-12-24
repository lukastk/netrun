use crate::_utils::get_utc_now;
use crate::graph::{EdgeRef, Graph, NodeName, Port, PortSlotSpec, PortName, PortType, PortRef, SalvoConditionName, SalvoConditionTerm, evaluate_salvo_condition};
use indexmap::IndexSet;
use std::collections::{HashMap, HashSet};
use std::panic;
use ulid::Ulid;

pub type PacketID = Ulid;
pub type EpochID = Ulid;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum PacketLocation {
    Node(EpochID),
    InputPort(NodeName, PortName),
    OutputPort(EpochID, PortName),
    Edge(EdgeRef),
    OutsideNet,
}

#[derive(Debug)]
pub struct Packet {
    pub id: PacketID,
    pub location: PacketLocation,
}

#[derive(Debug, Clone)]
pub struct Salvo {
    pub salvo_condition: SalvoConditionName,
    pub packets: Vec<(PortName, PacketID)>,
}

#[derive(Debug, Clone)]
pub enum EpochState {
    Startable,
    Running,
    Finished,
}

#[derive(Debug, Clone)]
pub struct Epoch {
    pub id: EpochID,
    pub node_name: NodeName,
    pub in_salvo: Salvo,        // Salvo received by this epoch
    pub out_salvos: Vec<Salvo>, // Salvos sent out from this epoch
    pub state: EpochState,
}

impl Epoch {
    pub fn start_time(&self) -> u64 {
        self.id.timestamp_ms()
    }
}

pub type EventUTC = i128; // Milliseconds

#[derive(Debug)]
pub enum NetAction {
    RunNetUntilBlocked,
    CreatePacket(Option<EpochID>),
    ConsumePacket(PacketID),
    StartEpoch(EpochID),
    FinishEpoch(EpochID),
    CancelEpoch(EpochID),
    CreateAndStartEpoch(NodeName, Salvo),
    LoadPacketIntoOutputPort(PacketID, PortName),
    SendOutputSalvo(EpochID, SalvoConditionName),
}

#[derive(Debug)]
pub enum NetActionError {
    PacketNotFound {message: String},
    EpochNotFound {message: String},
    EpochNotRunning {message: String},
    EpochNotStartable {message: String},
    CannotFinishNonEmptyEpoch {message: String},
    PacketNotInNode {message: String},
    OutputPortNotFound {message: String},
    OutputSalvoConditionNotFound {message: String},
    MaxOutputSalvosReached {message: String},
    SalvoConditionNotMet {message: String},
    OutputPortFull {message: String},
    CannotPutPacketIntoUnconnectedOutputPort {message: String},
}

#[derive(Debug)]
pub enum NetEvent {
    PacketCreated(EventUTC, PacketID),
    PacketConsumed(EventUTC, PacketID),
    EpochCreated(EventUTC, EpochID),
    EpochStarted(EventUTC, EpochID),
    EpochFinished(EventUTC, EpochID),
    EpochCancelled(EventUTC, EpochID),
    PacketMoved(EventUTC, PacketID, PacketLocation),
    InputSalvoTriggered(EventUTC, EpochID, SalvoConditionName),
    OutputSalvoTriggered(EventUTC, EpochID, SalvoConditionName),
    NodeEnabled(EventUTC, NodeName),
    NodeDisabled(EventUTC, NodeName),
}

#[derive(Debug)]
pub enum NetActionResponseData {
    Packet(PacketID),
    StartedEpoch(Epoch),
    FinishedEpoch(Epoch),
    None,
}

#[derive(Debug)]
pub enum NetActionResponse {
    Success(NetActionResponseData, Vec<NetEvent>),
    Error(NetActionError),
}

#[derive(Debug)]
pub struct Net<'a> {
    pub graph: Graph,
    _packets: HashMap<PacketID, Packet>,
    _packets_by_location: HashMap<PacketLocation, IndexSet<PacketID>>,
    _ports: HashMap<PortRef, &'a Port>,
    _epochs: HashMap<EpochID, Epoch>,
    _startable_epochs: HashSet<EpochID>,
    _node_to_epochs: HashMap<NodeName, Vec<EpochID>>,
}

impl Net<'_> {
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
                    message: "Cannot create packet in non-existent epoch".to_string(),
                });
            }
            if !matches!(self._epochs[&epoch_id].state, EpochState::Running) {
                return NetActionResponse::Error(NetActionError::EpochNotRunning {
                    message: "Cannot create packet in non-running epoch".to_string(),
                });
            }
        }

        let packet_location = match maybe_epoch_id {
            Some(epoch_id) => PacketLocation::Node(epoch_id.clone()),
            None => PacketLocation::OutsideNet,
        };

        let packet = Packet {
            id: Ulid::new(),
            location: packet_location,
        };

        let packet_id = packet.id.clone();
        self._packets.insert(packet.id, packet);
        NetActionResponse::Success(
            NetActionResponseData::Packet(packet_id),
            vec![NetEvent::PacketCreated(get_utc_now(), packet_id)]
        )
    }

    fn consume_packet(&mut self, packet_id: &PacketID) -> NetActionResponse {
        if !self._packets.contains_key(packet_id) {
            return NetActionResponse::Error(NetActionError::PacketNotFound {
                message: "Packet not found".to_string()
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
                    message: "Epoch not ready to start".to_string()
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
                message: "Epoch not found".to_string()
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
                            message: "All packets must have either been consumed or left the node by the end of the epoch".to_string()
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
                    message: "Epoch not running".to_string()
                });
            }
        } else {
            return NetActionResponse::Error(NetActionError::EpochNotFound {
                message: "Epoch not found".to_string()
            });
        }
    }

    fn cancel_epoch(&mut self, epoch_id: &EpochID) -> NetActionResponse {
        panic!("Not implemented")
    }

    fn create_and_start_epoch(&mut self, node_name: &NodeName, salvo: &Salvo) -> NetActionResponse {
        panic!("Not implemented")
    }

    fn load_packet_into_output_port(&mut self, packet_id: &PacketID, port_name: &String) -> NetActionResponse {
        let (epoch_id, old_location) = if let Some(packet) = self._packets.get(packet_id) {
            if let PacketLocation::Node(epoch_id) = packet.location {
                (epoch_id, packet.location.clone())
            } else {
                return NetActionResponse::Error(NetActionError::PacketNotInNode {
                    message: "Packet must be inside a node for it to be loaded into an output port".to_string()
                })
            }
        } else {
            return NetActionResponse::Error(NetActionError::PacketNotFound {
                message: "Packet not found".to_string()
            });
        };

        let node_name = self._epochs.get(&epoch_id)
            .expect("The epoch id in the location of a packet could not be found.")
            .node_name.clone();
        let node = self.graph.nodes().get(&node_name)
            .expect("Packet located in a non-existing node (yet the node has an epoch).");

        if !node.out_ports.contains_key(port_name) {
            return NetActionResponse::Error(NetActionError::OutputPortNotFound {
                message: "Port not found".to_string()
            })
        }

        let port = node.out_ports.get(port_name).unwrap();
        let port_packets = self._packets_by_location.get(&old_location)
            .expect("No entry in Net._packets_by_location found for output port.");

        // Check if the output port is full
        if let PortSlotSpec::Finite(num_slots) = port.slots_spec {
            if num_slots >= port_packets.len() as u64 {
                return NetActionResponse::Error(NetActionError::OutputPortFull {
                    message: "Output port is full".to_string()
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
                message: "Epoch not found".to_string()
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
                message: "Salvo condition not found".to_string()
            });
        };

        // Check if max salvos reached
        if epoch.out_salvos.len() as u64 >= salvo_condition.max_salvos {
            return NetActionResponse::Error(NetActionError::MaxOutputSalvosReached {
                message: "Salvo condition reached max".to_string()
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
                message: "Salvo condition not met".to_string()
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
                    message: format!("Output port '{}' of node '{}' is not connected", port_name, node.name.clone())
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
        for (packet_id, port_name, new_location) in packets_to_move {
            net_events.push(NetEvent::PacketMoved(get_utc_now(), packet_id.clone(), new_location.clone()));
            self.move_packet(&packet_id, new_location);
        }

        NetActionResponse::Success(
            NetActionResponseData::None,
            net_events
        )
    }

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
        }
    }
}
