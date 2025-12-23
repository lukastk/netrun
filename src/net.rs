use crate::_utils::get_utc_now;
use crate::graph::{EdgeRef, Graph, NodeName, Port, PortName, PortRef, SalvoConditionName};
use indexmap::IndexSet;
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::panic;
use ulid::Ulid;

pub type PacketID = Ulid;
pub type EpochID = Ulid;

#[derive(Debug, PartialEq, Eq, Hash)]
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
    pub packets: HashSet<PacketID>,
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
    CreatePacket(Option<EpochID>),
    ConsumePacket(PacketID),
    StartEpoch(EpochID),
    FinishEpoch(EpochID),
    CancelEpoch(EpochID),
    CreateAndStartEpoch(EpochID, Salvo),
    LoadPacketIntoOutputPort(PacketID, PortName),
    SendOutputSalvo(EpochID, SalvoConditionName),
}

#[derive(Debug)]
pub enum NetActionError {
    PacketNotFound {message: String, packet_id: PacketID},
    EpochNotFound {message: String, epoch_id: EpochID},
    EpochNotRunning {message: String, epoch_id: EpochID},
    EpochNotStartable {message: String, epoch_id: EpochID},
    CannotFinishNonEmptyEpoch {message: String, epoch_id: EpochID, packets: Vec<PacketID>},
    PacketNotInNode {message: String, packet_id: PacketID},
    OutputPortNotFound {message: String, node_name: NodeName, port_name: PortName},
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
    InputPortLoaded(EventUTC, EpochID, PortName),
    OutputPortLoaded(EventUTC, EpochID, PortName),
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
    Success(NetActionResponseData, NetEvent),
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
    pub fn run_until_blocked(&mut self) -> Vec<NetEvent> {}

    pub fn get_ready_epochs(&self) -> Vec<EpochID> {}

    pub fn do_action(&mut self, action: NetAction) -> NetActionResponse {
        match action {
            NetAction::CreatePacket(maybe_epoch_id) => {
                // Check that epoch_id exists and is running
                if let Some(epoch_id) = maybe_epoch_id {
                    if !self._epochs.contains_key(&epoch_id) {
                        return NetActionResponse::Error(NetActionError::EpochNotFound {
                            epoch_id,
                            message: "Cannot create packet in non-existent epoch".to_string(),
                        });
                    } else if !self._epochs[&epoch_id].is_running {
                        return NetActionResponse::Error(NetActionError::EpochNotRunning {
                            epoch_id,
                            message: "Cannot create packet in non-running epoch".to_string(),
                        });
                    }
                }

                let packet_location = match maybe_epoch_id {
                    Some(epoch_id) => PacketLocation::Node(epoch_id),
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
                    NetEvent::PacketCreated(get_utc_now(), packet_id)
                )
            }
            NetAction::ConsumePacket(packet_id) => {
                if !self._packets.contains_key(&packet_id) {
                    return NetActionResponse::Error(NetActionError::PacketNotFound {
                        packet_id,
                        message: "Packet not found".to_string()
                    });
                }

                if let Some(packets) = self
                    ._packets_by_location
                    .get_mut(&self._packets[&packet_id].location)
                {
                    if packets.shift_remove(&packet_id) {
                        self._packets.remove(&packet_id);
                        NetActionResponse::Success(
                            NetActionResponseData::None,
                            NetEvent::PacketConsumed(get_utc_now(), packet_id)
                        )
                    } else {
                        panic!(
                            "Packet with ID {} not found in location {:?}",
                            packet_id, self._packets[&packet_id].location
                        );
                    }
                } else {
                    panic!("Packet location {:?} not found", self._packets[&packet_id].location);
                }
            }
            NetAction::StartEpoch(epoch_id) => {
                if let Some(epoch) = self._epochs.get_mut(&epoch_id) {
                    if !self._startable_epochs.contains(&epoch_id) {
                        return NetActionResponse::Error(NetActionError::EpochNotStartable {
                            epoch_id,
                            message: "Epoch not ready to start".to_string()
                        });
                    }
                    debug_assert!(matches!(epoch.state, EpochState::Startable),
                        "Epoch state is not Startable but was in net._startable_epochs.");
                    epoch.state = EpochState::Running;
                    self._startable_epochs.remove(&epoch_id);
                    NetActionResponse::Success(
                        NetActionResponseData::StartedEpoch(epoch.clone()),
                        NetEvent::EpochStarted(get_utc_now(), epoch_id)
                    )
                } else {
                    return NetActionResponse::Error(NetActionError::EpochNotFound {
                        epoch_id,
                        message: "Epoch not found".to_string()
                    });
                }
            }
            NetAction::FinishEpoch(epoch_id) => {
                if let Some(epoch) = self._epochs.get(&epoch_id) {
                    if let EpochState::Running = epoch.state {
                        // No packets may remain in the epoch by the time it has ended.
                        let epoch_loc = PacketLocation::Node(epoch_id);
                        if let Some(packets) = self._packets_by_location.get(&epoch_loc) {
                            if packets.len() > 0 {
                                return NetActionResponse::Error(NetActionError::CannotFinishNonEmptyEpoch {
                                    epoch_id,
                                    packets: packets.iter().copied().collect(),
                                    message: "All packets must have either been consumed or left the node by the end of the epoch".to_string()
                                });
                            }

                            let epoch = self._epochs.remove(&epoch_id).unwrap();
                            epoch.state = EpochState::Finished;
                            self._packets_by_location.remove(&epoch_loc);
                            NetActionResponse::Success(
                                NetActionResponseData::FinishedEpoch(epoch),
                                NetEvent::EpochFinished(get_utc_now(), epoch_id)
                            )
                        } else {
                            panic!("Epoch {} not found in location {:?}", epoch_id, epoch_loc);
                        }
                    } else {
                        return NetActionResponse::Error(NetActionError::EpochNotRunning {
                            epoch_id,
                            message: "Epoch not running".to_string()
                        });
                    }
                } else {
                    return NetActionResponse::Error(NetActionError::EpochNotFound {
                        epoch_id,
                        message: "Epoch not found".to_string()
                    });
                }
            }
            NetAction::CancelEpoch(epoch_id) => {
                panic!("Not implemented")
            }
            NetAction::CreateAndStartEpoch(epoch_id, salvo) => {
                panic!("Not implemented")
            }
            NetAction::LoadPacketIntoOutputPort(packet_id, port_name) => {
                if let Some(packet) = self._packets.get_mut(&packet_id) {
                    if let PacketLocation::Node(epoch_id) = packet.location {
                        let node_name = self._epochs.get(&epoch_id)
                            .expect("The epoch id in the location of a packet could not be found.")
                            .node_name;
                        let node = self.graph.nodes.get(&node_name)
                            .expect("Packet located in a non-existing node (yet the node has an epoch).");
                        if let Some(port) = node.out_ports.get(&port_name) {
                            packet.location = PacketLocation::OutputPort(epoch_id, port_name.clone());
                            if !self._packets_by_location.get_mut(&packet.location)
                                .expect("No entry in _packets_by_location found for output port.")
                                .insert(packet_id) {
                                panic!("Packet {} already exists in output port {} in node {}", packet_id, port_name, node_name)
                            }
                            NetActionResponse::Success(
                                NetActionResponseData::None,
                                NetEvent::OutputPortLoaded(get_utc_now(), epoch_id, port_name)
                            )
                        } else {
                            return NetActionResponse::Error(NetActionError::OutputPortNotFound {
                                node_name,
                                port_name,
                                message: "Port not found".to_string()
                            })
                        }
                    } else {
                        return NetActionResponse::Error(NetActionError::PacketNotInNode {
                            packet_id,
                            message: "Packet must be inside a node for it to be loaded into an output port".to_string()
                        })
                    }
                } else {
                    return NetActionResponse::Error(NetActionError::PacketNotFound {
                        packet_id,
                        message: "Packet not found".to_string()
                    });
                }
            }
            NetAction::SendOutputSalvo(epoch_id, salvo_condition_name) => {
                panic!("Not implemented")
            }
        }
    }

    pub fn do_actions(&mut self, actions: Vec<NetAction>) -> Vec<(NetActionResponse, Vec<NetEvent>)> {
        actions
            .into_iter()
            .map(|action| {
                let net_action_response = self.do_action(action);
                let net_events = self.run_until_blocked();
                (net_action_response, net_events)
            })
            .collect()
    }
}
