use crate::_utils::get_utc_now;
use crate::graph::{EdgeRef, Graph, NodeName, Port, PortName, PortRef, SalvoConditionName};
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use ulid::Ulid;

pub type PacketID = Ulid;
pub type EpochID = Ulid;

#[derive(Debug)]
pub struct Packet {
    pub id: PacketID,
}

#[derive(Debug)]
pub struct Salvo {
    pub salvo_condition: SalvoConditionName,
    pub packets: HashSet<PacketID>,
}

#[derive(Debug)]
pub struct Epoch {
    pub id: EpochID,
    pub node_name: NodeName,
    pub in_salvo: Salvo,        // Salvo received by this epoch
    pub out_salvos: Vec<Salvo>, // Salvos sent out from this epoch
}

impl Epoch {
    pub fn start_time(&self) -> u64 {
        self.id.timestamp_ms()
    }
}

pub type EventUTC = i128; // Milliseconds

#[derive(Debug)]
pub enum NetEvent {
    PacketCreated(EventUTC, PacketID),
    PacketConsumed(EventUTC, PacketID),
    EpochStarted(EventUTC, EpochID),
    EpochEnded(EventUTC, EpochID),
    PacketMovedIntoInputPort(EventUTC, PacketID, PortRef),
    PacketMovedIntoOutputPort(EventUTC, PacketID, PortRef),
    PacketMovedIntoEdge(EventUTC, PacketID, EdgeRef),
    InputSalvoTriggered(EventUTC, EpochID, SalvoConditionName),
    OutputSalvoTriggered(EventUTC, EpochID, SalvoConditionName),
    NodeEnabled(EventUTC, NodeName),
    NodeDisabled(EventUTC, NodeName),
}

#[derive(Debug)]
pub struct Net<'a> {
    pub graph: Graph,
    pub packets: HashSet<PacketID>,

    _ports: HashMap<PortRef, &'a Port>,

    current_epochs: VecDeque<EpochID>,
    pending_epochs: VecDeque<EpochID>,

    output_port_packets: HashMap<(EpochID, PortName), VecDeque<PacketID>>, // Each epoch has its own queue of packets for each output port
}

#[derive(Debug)]
pub enum NetError {}

#[derive(Debug)]
pub struct RunResult {
    pub transpired_events: Vec<NetEvent>,
    pub finished_epochs: Vec<Epoch>,
    pub consumed_packets: Vec<Packet>,
    pub errors: Vec<NetError>,
}

impl Net<'_> {
    pub fn run_until_blocked(&mut self) -> RunResult {}

    // Actions

    pub fn create_packet(&mut self, node: NodeName) -> PacketID {}

    pub fn consume_packet(&mut self, packet_id: PacketID) {}

    pub fn start_epoch(&mut self, id: EpochID) {}

    pub fn end_epoch(&mut self, id: EpochID) {}

    pub fn load_packet_into_output_port(&mut self, packet_id: PacketID, port_ref: PortRef) {}

    pub fn send_output_salvo(
        &mut self,
        epoch_id: EpochID,
        salvo_condition_name: SalvoConditionName,
    ) {
    }
}
