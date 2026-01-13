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

class UnsentOutputSalvoError(NetrunError):
    """Cannot finish epoch because output port still has unsent packets."""
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

class PacketCount:
    """Specifies how many packets to take from a port in a salvo."""
    All: PacketCount

    @staticmethod
    def all() -> PacketCount: ...

    @staticmethod
    def count(n: int) -> PacketCountN: ...


class PacketCountN:
    """PacketCount with a specific count limit."""
    @property
    def count(self) -> int: ...


class MaxSalvos:
    """Specifies the maximum number of times a salvo condition can trigger."""
    Infinite: MaxSalvos

    @staticmethod
    def infinite() -> MaxSalvos: ...

    @staticmethod
    def finite(n: int) -> MaxSalvosFinite: ...


class MaxSalvosFinite:
    """Finite max salvos with a specific limit."""
    @property
    def max(self) -> int: ...


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
    """Boolean expression over port states.

    Use the static methods to construct terms:
    - `true_()` / `false_()`: Constant boolean values
    - `port(name, state)`: Check if a port matches a state predicate
    - `and_(terms)` / `or_(terms)`: Logical combinations
    - `not_(term)`: Logical negation

    Example:
        # Trigger when both input ports have packets
        term = SalvoConditionTerm.and_([
            SalvoConditionTerm.port("in1", PortState.non_empty()),
            SalvoConditionTerm.port("in2", PortState.non_empty()),
        ])

        # Source node with no input ports
        term = SalvoConditionTerm.true_()
    """

    @staticmethod
    def true_() -> SalvoConditionTerm:
        """Create a term that is always true. Useful for source nodes with no input ports."""
        ...

    @staticmethod
    def false_() -> SalvoConditionTerm:
        """Create a term that is always false. Useful as a placeholder or with not_()."""
        ...

    @staticmethod
    def port(port_name: str, state: Union[PortState, PortStateNumeric]) -> SalvoConditionTerm:
        """Create a term that checks if a port matches a state predicate."""
        ...

    @staticmethod
    def and_(terms: List[SalvoConditionTerm]) -> SalvoConditionTerm:
        """Create a term that is true when all sub-terms are true."""
        ...

    @staticmethod
    def or_(terms: List[SalvoConditionTerm]) -> SalvoConditionTerm:
        """Create a term that is true when at least one sub-term is true."""
        ...

    @staticmethod
    def not_(term: SalvoConditionTerm) -> SalvoConditionTerm:
        """Create a term that is true when the sub-term is false."""
        ...

    @property
    def kind(self) -> str:
        """Get the term kind: 'True', 'False', 'Port', 'And', 'Or', or 'Not'."""
        ...

    def get_port_name(self) -> Optional[str]:
        """Get the port name (for Port terms only, None otherwise)."""
        ...

    def get_port_state(self) -> Optional[Union[PortState, PortStateNumeric]]:
        """Get the port state (for Port terms only, None otherwise)."""
        ...

    def get_terms(self) -> Optional[List[SalvoConditionTerm]]:
        """Get the sub-terms (for And/Or terms only, None otherwise)."""
        ...

    def get_inner(self) -> Optional[SalvoConditionTerm]:
        """Get the inner term (for Not terms only, None otherwise)."""
        ...


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


# Type alias for ports parameter
SalvoConditionPorts = Union[str, List[str], Dict[str, Union[PacketCount, PacketCountN]]]


class SalvoCondition:
    """A condition that defines when packets can trigger an epoch or be sent.

    Args:
        max_salvos: Maximum number of times this condition can trigger.
            Use MaxSalvos.finite(n) for a finite limit or MaxSalvos.Infinite for no limit.
        ports: Which ports' packets are included. Can be:
            - A single port name (str) - defaults to PacketCount.All
            - A list of port names - each defaults to PacketCount.All
            - A dict mapping port names to PacketCount values
        term: The boolean expression that must be satisfied.
    """

    def __init__(self, max_salvos: Union[MaxSalvos, MaxSalvosFinite], ports: SalvoConditionPorts, term: SalvoConditionTerm) -> None: ...

    @property
    def max_salvos(self) -> Union[MaxSalvos, MaxSalvosFinite]: ...
    @property
    def ports(self) -> Dict[str, Union[PacketCount, PacketCountN]]: ...
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


# === NetSim Types ===

class PacketLocation:
    """Where a packet is located in the network.

    Packets can be in one of five locations:
    - `node(epoch_id)`: Inside a running/startable epoch
    - `input_port(node_name, port_name)`: Waiting at a node's input port
    - `output_port(epoch_id, port_name)`: Loaded into an epoch's output port
    - `edge(edge)`: In transit between nodes
    - `outside_net()`: External to the network

    Example:
        loc = PacketLocation.input_port("NodeB", "in")
        assert loc.kind == "InputPort"
        assert loc.node_name == "NodeB"
        assert loc.port_name == "in"
    """

    @staticmethod
    def node(epoch_id: Union[ULID, str]) -> PacketLocation:
        """Create a location inside an epoch."""
        ...

    @staticmethod
    def input_port(node_name: str, port_name: str) -> PacketLocation:
        """Create a location at a node's input port."""
        ...

    @staticmethod
    def output_port(epoch_id: Union[ULID, str], port_name: str) -> PacketLocation:
        """Create a location at an epoch's output port."""
        ...

    @staticmethod
    def edge(edge: Edge) -> PacketLocation:
        """Create a location on an edge (in transit)."""
        ...

    @staticmethod
    def outside_net() -> PacketLocation:
        """Create a location outside the network."""
        ...

    @property
    def kind(self) -> str:
        """Get the location kind: 'Node', 'InputPort', 'OutputPort', 'Edge', or 'OutsideNet'."""
        ...

    @property
    def node_name(self) -> Optional[str]:
        """Get the node name (for InputPort locations only, None otherwise)."""
        ...

    @property
    def port_name(self) -> Optional[str]:
        """Get the port name (for InputPort and OutputPort locations only, None otherwise)."""
        ...

    @property
    def epoch_id(self) -> Optional[str]:
        """Get the epoch ID (for Node and OutputPort locations only, None otherwise)."""
        ...

    def get_edge(self) -> Optional[Edge]:
        """Get the edge (for Edge locations only, None otherwise)."""
        ...


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
    """An action to perform on the network.

    Actions are executed via `NetSim.do_action()` and return a tuple of
    (response_data, events).

    Example:
        # Create and start an epoch manually
        salvo = Salvo(salvo_condition="trigger", packets=[("in", packet_id)])
        response, events = net.do_action(NetAction.create_epoch("NodeB", salvo))
        epoch = response.epoch  # CreatedEpoch response

        response, events = net.do_action(NetAction.start_epoch(epoch.id))
        epoch = response.epoch  # StartedEpoch response
    """

    @staticmethod
    def run_net_until_blocked() -> NetAction:
        """Run automatic packet flow until no progress can be made.

        Moves packets from edges to input ports, then checks input salvo
        conditions to create new epochs. Repeats until blocked.
        """
        ...

    @staticmethod
    def create_packet(epoch_id: Optional[Union[ULID, str]] = None) -> NetAction:
        """Create a new packet.

        Args:
            epoch_id: If provided, create packet inside this epoch.
                     If None, create packet outside the network.

        Returns NetActionResponseData.Packet with the new packet ID.
        """
        ...

    @staticmethod
    def consume_packet(packet_id: Union[ULID, str]) -> NetAction:
        """Consume a packet (normal removal from the network)."""
        ...

    @staticmethod
    def destroy_packet(packet_id: Union[ULID, str]) -> NetAction:
        """Destroy a packet (abnormal removal, e.g., due to error)."""
        ...

    @staticmethod
    def start_epoch(epoch_id: Union[ULID, str]) -> NetAction:
        """Start a Startable epoch, transitioning it to Running state.

        Returns NetActionResponseData.StartedEpoch with the epoch.
        """
        ...

    @staticmethod
    def finish_epoch(epoch_id: Union[ULID, str]) -> NetAction:
        """Finish a Running epoch.

        The epoch must be empty (no packets inside) and all output salvos
        must have been sent.

        Returns NetActionResponseData.FinishedEpoch with the epoch.
        """
        ...

    @staticmethod
    def cancel_epoch(epoch_id: Union[ULID, str]) -> NetAction:
        """Cancel an epoch and destroy all packets inside it.

        Returns NetActionResponseData.CancelledEpoch with the epoch and
        list of destroyed packet IDs.
        """
        ...

    @staticmethod
    def create_epoch(node_name: str, salvo: Salvo) -> NetAction:
        """Manually create an epoch with specified packets.

        Bypasses the normal salvo condition triggering mechanism.
        The epoch is created in Startable state - call start_epoch() to
        begin execution.

        Args:
            node_name: The node to create the epoch on.
            salvo: The input salvo (packets must be at the node's input ports).
                  For source nodes with no inputs, use an empty packets list.

        Returns NetActionResponseData.CreatedEpoch with the epoch.
        """
        ...

    @staticmethod
    def load_packet_into_output_port(packet_id: Union[ULID, str], port_name: str) -> NetAction:
        """Move a packet from inside an epoch to one of its output ports."""
        ...

    @staticmethod
    def send_output_salvo(epoch_id: Union[ULID, str], salvo_condition_name: str) -> NetAction:
        """Send packets from output ports onto edges.

        The salvo condition must be satisfied for this to succeed.
        """
        ...

    @staticmethod
    def transport_packet_to_location(packet_id: Union[ULID, str], destination: PacketLocation) -> NetAction:
        """Transport a packet to a new location.

        Cannot move packets into or out of Running epochs.
        """
        ...


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
    """Response data from a successful action.

    The response type depends on the action:
    - `create_packet()` -> `Packet`
    - `create_epoch()` -> `CreatedEpoch`
    - `start_epoch()` -> `StartedEpoch`
    - `finish_epoch()` -> `FinishedEpoch`
    - `cancel_epoch()` -> `CancelledEpoch`
    - Other actions -> `Empty`

    Use `isinstance()` or check the type to determine which response you have.
    """

    class Packet:
        """Response from create_packet()."""
        packet_id: str

    class CreatedEpoch:
        """Response from create_epoch(). Epoch is in Startable state."""
        epoch: Epoch

    class StartedEpoch:
        """Response from start_epoch(). Epoch is in Running state."""
        epoch: Epoch

    class FinishedEpoch:
        """Response from finish_epoch(). Epoch is in Finished state."""
        epoch: Epoch

    class CancelledEpoch:
        """Response from cancel_epoch()."""
        epoch: Epoch
        destroyed_packets: List[str]

    class Empty:
        """Response from actions that don't return specific data."""
        pass


class NetSim:
    """The runtime state of a flow-based network.

    NetSim simulates packet flow through a network of nodes. It tracks packet
    locations, validates flow conditions, and manages epoch lifecycles.

    Example:
        # Create a network
        graph = Graph(nodes=[...], edges=[...])
        net = NetSim(graph)

        # Create a packet and place it on an edge
        response, events = net.do_action(NetAction.create_packet())
        packet_id = response.packet_id

        edge = graph.edges()[0]
        net.do_action(NetAction.transport_packet_to_location(
            packet_id,
            PacketLocation.edge(edge)
        ))

        # Run the network - packet will flow to input ports and trigger epochs
        net.do_action(NetAction.run_net_until_blocked())

        # Check for startable epochs
        startable = net.get_startable_epochs()
    """

    def __init__(self, graph: Graph) -> None:
        """Create a new network simulation from a graph topology."""
        ...

    def do_action(self, action: NetAction) -> Tuple[NetActionResponseData, List[NetEvent]]:
        """Execute an action on the network.

        Args:
            action: The action to perform.

        Returns:
            A tuple of (response_data, events) where response_data contains
            action-specific results and events is a list of things that happened.

        Raises:
            Various exceptions depending on the action (see exception classes).
        """
        ...

    def packet_count_at(self, location: PacketLocation) -> int:
        """Get the number of packets at a location."""
        ...

    def get_packets_at_location(self, location: PacketLocation) -> List[ULID]:
        """Get the IDs of all packets at a location."""
        ...

    def get_epoch(self, epoch_id: Union[ULID, str]) -> Optional[Epoch]:
        """Get an epoch by ID, or None if not found."""
        ...

    def get_startable_epochs(self) -> List[ULID]:
        """Get IDs of all epochs in Startable state."""
        ...

    def get_packet(self, packet_id: Union[ULID, str]) -> Optional[Packet]:
        """Get a packet by ID, or None if not found."""
        ...

    @property
    def graph(self) -> Graph:
        """The graph topology this simulation is running on."""
        ...


# === Re-exports ===

__all__ = [
    # Exceptions
    "NetrunError",
    "PacketNotFoundError",
    "EpochNotFoundError",
    "EpochNotRunningError",
    "EpochNotStartableError",
    "CannotFinishNonEmptyEpochError",
    "UnsentOutputSalvoError",
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
    "PacketCount",
    "PacketCountN",
    "MaxSalvos",
    "MaxSalvosFinite",
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
    "SalvoConditionPorts",
    "Node",
    "Graph",
    # NetSim types
    "PacketLocation",
    "EpochState",
    "Packet",
    "Salvo",
    "Epoch",
    "NetAction",
    "NetEvent",
    "NetActionResponseData",
    "NetSim",
]
