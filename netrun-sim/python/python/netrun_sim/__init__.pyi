"""Type stubs for netrun_sim - Flow-based development runtime simulation."""

from typing import Dict, List, Optional, Tuple, Union
from ulid import ULID

# === Exceptions ===

class NetrunError(Exception):
    """Base exception for all netrun errors."""
    ...

class PacketNotFoundError(NetrunError):
    """Packet with the given ID was not found."""
    ...

class EpochNotFoundError(NetrunError):
    """Epoch with the given ID was not found."""
    ...

class EpochNotRunningError(NetrunError):
    """Epoch exists but is not in Running state."""
    ...

class EpochNotStartableError(NetrunError):
    """Epoch exists but is not in Startable state."""
    ...

class CannotFinishNonEmptyEpochError(NetrunError):
    """Cannot finish epoch because it still contains packets."""
    ...

class PacketNotInNodeError(NetrunError):
    """Packet is not inside the specified epoch's node location."""
    ...

class OutputPortNotFoundError(NetrunError):
    """Output port does not exist on the node."""
    ...

class OutputPortFullError(NetrunError):
    """Output port has reached its capacity."""
    ...

class SalvoConditionNotFoundError(NetrunError):
    """Salvo condition with the given name was not found."""
    ...

class SalvoConditionNotMetError(NetrunError):
    """Salvo condition exists but its term is not satisfied."""
    ...

class MaxSalvosExceededError(NetrunError):
    """Maximum number of salvos reached for this condition."""
    ...

class NodeNotFoundError(NetrunError):
    """Node with the given name was not found."""
    ...

class PacketNotAtInputPortError(NetrunError):
    """Packet is not at the expected input port."""
    ...

class InputPortNotFoundError(NetrunError):
    """Input port does not exist on the node."""
    ...

class InputPortFullError(NetrunError):
    """Input port has reached its capacity."""
    ...

class CannotMovePacketFromRunningEpochError(NetrunError):
    """Cannot move packet out of a running epoch."""
    ...

class CannotMovePacketIntoRunningEpochError(NetrunError):
    """Cannot move packet into a running epoch."""
    ...

class EdgeNotFoundError(NetrunError):
    """Edge does not exist in the graph."""
    ...

class UnconnectedOutputPortError(NetrunError):
    """Output port is not connected to any edge."""
    ...

class GraphValidationError(NetrunError):
    """Graph validation failed."""
    ...


# === Graph Types ===

class PortSlotSpec:
    """Port capacity specification."""
    Infinite: PortSlotSpec
    Finite: PortSlotSpec

    @staticmethod
    def infinite() -> PortSlotSpec: ...

    @staticmethod
    def finite(n: int) -> PortSlotSpecFinite: ...


class PortSlotSpecFinite:
    """Finite port capacity with a specific limit."""
    @property
    def capacity(self) -> int: ...


class PortState:
    """Port state predicate for salvo conditions."""
    Empty: PortState
    Full: PortState
    NonEmpty: PortState
    NonFull: PortState

    @staticmethod
    def empty() -> PortState: ...

    @staticmethod
    def full() -> PortState: ...

    @staticmethod
    def non_empty() -> PortState: ...

    @staticmethod
    def non_full() -> PortState: ...

    @staticmethod
    def equals(n: int) -> PortStateNumeric: ...

    @staticmethod
    def less_than(n: int) -> PortStateNumeric: ...

    @staticmethod
    def greater_than(n: int) -> PortStateNumeric: ...

    @staticmethod
    def equals_or_less_than(n: int) -> PortStateNumeric: ...

    @staticmethod
    def equals_or_greater_than(n: int) -> PortStateNumeric: ...


class PortStateNumeric:
    """Numeric port state predicate."""
    @property
    def kind(self) -> str: ...
    @property
    def value(self) -> int: ...


class SalvoConditionTerm:
    """Boolean expression over port states."""

    @staticmethod
    def port(port_name: str, state: Union[PortState, PortStateNumeric]) -> SalvoConditionTerm: ...

    @staticmethod
    def and_(terms: List[SalvoConditionTerm]) -> SalvoConditionTerm: ...

    @staticmethod
    def or_(terms: List[SalvoConditionTerm]) -> SalvoConditionTerm: ...

    @staticmethod
    def not_(term: SalvoConditionTerm) -> SalvoConditionTerm: ...


class Port:
    """A port on a node."""

    def __init__(self, slots_spec: Optional[Union[PortSlotSpec, PortSlotSpecFinite]] = None) -> None: ...

    @property
    def slots_spec(self) -> Union[PortSlotSpec, PortSlotSpecFinite]: ...


class PortType:
    """Port type: Input or Output."""
    Input: PortType
    Output: PortType


class PortRef:
    """Reference to a specific port on a node."""

    def __init__(self, node_name: str, port_type: PortType, port_name: str) -> None: ...

    @property
    def node_name(self) -> str: ...
    @property
    def port_type(self) -> PortType: ...
    @property
    def port_name(self) -> str: ...

    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...


class Edge:
    """A connection between two ports in the graph."""

    def __init__(self, source: PortRef, target: PortRef) -> None: ...

    @property
    def source(self) -> PortRef: ...
    @property
    def target(self) -> PortRef: ...

    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...


class SalvoCondition:
    """A condition that defines when packets can trigger an epoch or be sent."""

    def __init__(self, max_salvos: int, ports: List[str], term: SalvoConditionTerm) -> None: ...

    @property
    def max_salvos(self) -> int: ...
    @property
    def ports(self) -> List[str]: ...
    @property
    def term(self) -> SalvoConditionTerm: ...


class Node:
    """A processing node in the graph."""

    def __init__(
        self,
        name: str,
        in_ports: Optional[Dict[str, Port]] = None,
        out_ports: Optional[Dict[str, Port]] = None,
        in_salvo_conditions: Optional[Dict[str, SalvoCondition]] = None,
        out_salvo_conditions: Optional[Dict[str, SalvoCondition]] = None,
    ) -> None: ...

    @property
    def name(self) -> str: ...
    @property
    def in_ports(self) -> Dict[str, Port]: ...
    @property
    def out_ports(self) -> Dict[str, Port]: ...
    @property
    def in_salvo_conditions(self) -> Dict[str, SalvoCondition]: ...
    @property
    def out_salvo_conditions(self) -> Dict[str, SalvoCondition]: ...


class Graph:
    """The static topology of a flow-based network."""

    def __init__(self, nodes: List[Node], edges: List[Edge]) -> None: ...

    def nodes(self) -> Dict[str, Node]: ...
    def edges(self) -> List[Edge]: ...
    def validate(self) -> List[GraphValidationError]: ...


# === Net Types ===

class PacketLocation:
    """Where a packet is located in the network."""

    @staticmethod
    def node(epoch_id: Union[ULID, str]) -> PacketLocation: ...

    @staticmethod
    def input_port(node_name: str, port_name: str) -> PacketLocation: ...

    @staticmethod
    def output_port(epoch_id: Union[ULID, str], port_name: str) -> PacketLocation: ...

    @staticmethod
    def edge(edge: Edge) -> PacketLocation: ...

    @staticmethod
    def outside_net() -> PacketLocation: ...

    @property
    def kind(self) -> str: ...


class EpochState:
    """Epoch lifecycle state."""
    Startable: EpochState
    Running: EpochState
    Finished: EpochState


class Packet:
    """A packet in the network."""

    @property
    def id(self) -> str: ...
    @property
    def location(self) -> PacketLocation: ...

    def get_id(self) -> ULID: ...


class Salvo:
    """A collection of packets entering or exiting a node."""

    def __init__(self, salvo_condition: str, packets: List[Tuple[str, str]]) -> None: ...

    @property
    def salvo_condition(self) -> str: ...
    @property
    def packets(self) -> List[Tuple[str, str]]: ...


class Epoch:
    """An execution instance of a node."""

    @property
    def id(self) -> str: ...
    @property
    def node_name(self) -> str: ...
    @property
    def in_salvo(self) -> Salvo: ...
    @property
    def out_salvos(self) -> List[Salvo]: ...
    @property
    def state(self) -> EpochState: ...

    def get_id(self) -> ULID: ...
    def start_time(self) -> int: ...


class NetAction:
    """An action to perform on the network."""

    @staticmethod
    def run_net_until_blocked() -> NetAction: ...

    @staticmethod
    def create_packet(epoch_id: Optional[Union[ULID, str]] = None) -> NetAction: ...

    @staticmethod
    def consume_packet(packet_id: Union[ULID, str]) -> NetAction: ...

    @staticmethod
    def start_epoch(epoch_id: Union[ULID, str]) -> NetAction: ...

    @staticmethod
    def finish_epoch(epoch_id: Union[ULID, str]) -> NetAction: ...

    @staticmethod
    def cancel_epoch(epoch_id: Union[ULID, str]) -> NetAction: ...

    @staticmethod
    def create_and_start_epoch(node_name: str, salvo: Salvo) -> NetAction: ...

    @staticmethod
    def load_packet_into_output_port(packet_id: Union[ULID, str], port_name: str) -> NetAction: ...

    @staticmethod
    def send_output_salvo(epoch_id: Union[ULID, str], salvo_condition_name: str) -> NetAction: ...

    @staticmethod
    def transport_packet_to_location(packet_id: Union[ULID, str], destination: PacketLocation) -> NetAction: ...


class NetEvent:
    """An event that occurred during a network action."""

    @property
    def kind(self) -> str: ...
    @property
    def timestamp(self) -> int: ...
    @property
    def packet_id(self) -> Optional[str]: ...
    @property
    def epoch_id(self) -> Optional[str]: ...
    @property
    def location(self) -> Optional[PacketLocation]: ...
    @property
    def salvo_condition(self) -> Optional[str]: ...


class NetActionResponseData:
    """Response data from a successful action."""

    class Packet:
        packet_id: str

    class StartedEpoch:
        epoch: Epoch

    class FinishedEpoch:
        epoch: Epoch

    class CancelledEpoch:
        epoch: Epoch
        destroyed_packets: List[str]

    class Empty:
        pass


class Net:
    """The runtime state of a flow-based network."""

    def __init__(self, graph: Graph) -> None: ...

    def do_action(self, action: NetAction) -> Tuple[NetActionResponseData, List[NetEvent]]: ...

    def packet_count_at(self, location: PacketLocation) -> int: ...

    def get_packets_at_location(self, location: PacketLocation) -> List[ULID]: ...

    def get_epoch(self, epoch_id: Union[ULID, str]) -> Optional[Epoch]: ...

    def get_startable_epochs(self) -> List[ULID]: ...

    def get_packet(self, packet_id: Union[ULID, str]) -> Optional[Packet]: ...

    @property
    def graph(self) -> Graph: ...


# === Re-exports ===

__all__ = [
    # Exceptions
    "NetrunError",
    "PacketNotFoundError",
    "EpochNotFoundError",
    "EpochNotRunningError",
    "EpochNotStartableError",
    "CannotFinishNonEmptyEpochError",
    "PacketNotInNodeError",
    "OutputPortNotFoundError",
    "OutputPortFullError",
    "SalvoConditionNotFoundError",
    "SalvoConditionNotMetError",
    "MaxSalvosExceededError",
    "NodeNotFoundError",
    "PacketNotAtInputPortError",
    "InputPortNotFoundError",
    "InputPortFullError",
    "CannotMovePacketFromRunningEpochError",
    "CannotMovePacketIntoRunningEpochError",
    "EdgeNotFoundError",
    "UnconnectedOutputPortError",
    "GraphValidationError",
    # Graph types
    "PortSlotSpec",
    "PortSlotSpecFinite",
    "PortState",
    "PortStateNumeric",
    "SalvoConditionTerm",
    "Port",
    "PortType",
    "PortRef",
    "Edge",
    "SalvoCondition",
    "Node",
    "Graph",
    # Net types
    "PacketLocation",
    "EpochState",
    "Packet",
    "Salvo",
    "Epoch",
    "NetAction",
    "NetEvent",
    "NetActionResponseData",
    "Net",
]
