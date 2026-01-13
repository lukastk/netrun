# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Deferred Packets and Actions
#
# When `defer_net_actions=True`, packet operations are queued rather than executed immediately.
# This allows clean retry without orphaned packets.

# %%
#|default_exp deferred

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();

# %%
#|export
from typing import Any, Callable, List, Optional, Union
from enum import Enum
from dataclasses import dataclass

from netrun_sim import Packet

from netrun.errors import DeferredPacketIdAccessError


class DeferredPacket:
    """
    A placeholder for a packet when defer_net_actions=True.

    Behaves like a Packet for node operations (loading to output ports, etc.),
    but the actual PacketID is not assigned until deferred actions are committed.
    """

    def __init__(self, deferred_id: str, value: Any = None, value_func: Optional[Callable] = None):
        """
        Initialize a deferred packet with a temporary internal ID.

        Args:
            deferred_id: Internal ID used to track this packet until commit
            value: Direct value stored in the packet (if not using value_func)
            value_func: Value function to call when consuming (if not using value)
        """
        self._deferred_id = deferred_id
        self._resolved_packet: Optional[Packet] = None
        self._value = value
        self._value_func = value_func

    @property
    def id(self) -> str:
        """
        Get the packet ID.

        Raises DeferredPacketIdAccessError if not yet committed.
        """
        if self._resolved_packet is None:
            raise DeferredPacketIdAccessError()
        return self._resolved_packet.id

    @property
    def deferred_id(self) -> str:
        """Get the internal deferred ID (always available)."""
        return self._deferred_id

    @property
    def is_resolved(self) -> bool:
        """Check if this deferred packet has been resolved to a real packet."""
        return self._resolved_packet is not None

    def _resolve(self, packet: Packet) -> None:
        """Internal: resolve this deferred packet to a real packet."""
        self._resolved_packet = packet

    @property
    def location(self):
        """Get the packet location (only available after resolution)."""
        if self._resolved_packet is None:
            raise DeferredPacketIdAccessError()
        return self._resolved_packet.location

    def __repr__(self) -> str:
        if self._resolved_packet is not None:
            return f"DeferredPacket(resolved={self._resolved_packet.id})"
        return f"DeferredPacket(deferred_id={self._deferred_id})"

# %%
#|export
class DeferredActionType(Enum):
    """Types of deferred actions."""
    CREATE_PACKET = "create_packet"
    CREATE_PACKET_FROM_FUNC = "create_packet_from_func"
    CONSUME_PACKET = "consume_packet"
    LOAD_OUTPUT_PORT = "load_output_port"
    SEND_OUTPUT_SALVO = "send_output_salvo"


@dataclass
class DeferredAction:
    """A single deferred action to be committed or discarded."""
    action_type: DeferredActionType
    # For CREATE_PACKET: value to store
    value: Any = None
    # For CREATE_PACKET_FROM_FUNC: the value function
    value_func: Optional[Callable] = None
    # For CREATE_PACKET/CREATE_PACKET_FROM_FUNC: the deferred packet created
    deferred_packet: Optional[DeferredPacket] = None
    # For CONSUME_PACKET: the packet (or deferred packet) being consumed
    packet: Optional[Union[Packet, DeferredPacket]] = None
    # For CONSUME_PACKET: the consumed value (stored for unconsume on retry)
    consumed_value: Any = None
    # For LOAD_OUTPUT_PORT: port name
    port_name: Optional[str] = None
    # For SEND_OUTPUT_SALVO: salvo condition name
    salvo_condition_name: Optional[str] = None


class DeferredActionQueue:
    """
    Queue of deferred actions for a node execution.

    Used when defer_net_actions=True to buffer operations until successful completion.
    """

    def __init__(self):
        self._actions: List[DeferredAction] = []
        self._deferred_packet_counter = 0
        # Track consumed values for unconsume on retry
        self._consumed_values: dict[str, Any] = {}

    def create_packet(self, value: Any) -> DeferredPacket:
        """Queue a packet creation with a direct value."""
        deferred_id = f"deferred-{self._deferred_packet_counter}"
        self._deferred_packet_counter += 1
        deferred_packet = DeferredPacket(deferred_id, value=value)
        self._actions.append(DeferredAction(
            action_type=DeferredActionType.CREATE_PACKET,
            value=value,
            deferred_packet=deferred_packet,
        ))
        return deferred_packet

    def create_packet_from_func(self, func: Callable) -> DeferredPacket:
        """Queue a packet creation with a value function."""
        deferred_id = f"deferred-{self._deferred_packet_counter}"
        self._deferred_packet_counter += 1
        deferred_packet = DeferredPacket(deferred_id, value_func=func)
        self._actions.append(DeferredAction(
            action_type=DeferredActionType.CREATE_PACKET_FROM_FUNC,
            value_func=func,
            deferred_packet=deferred_packet,
        ))
        return deferred_packet

    def consume_packet(self, packet: Union[Packet, DeferredPacket], value: Any) -> None:
        """Queue a packet consumption."""
        packet_id = packet._deferred_id if isinstance(packet, DeferredPacket) else packet.id
        self._consumed_values[packet_id] = value
        self._actions.append(DeferredAction(
            action_type=DeferredActionType.CONSUME_PACKET,
            packet=packet,
            consumed_value=value,
        ))

    def load_output_port(self, port_name: str, packet: Union[Packet, DeferredPacket]) -> None:
        """Queue loading a packet to an output port."""
        self._actions.append(DeferredAction(
            action_type=DeferredActionType.LOAD_OUTPUT_PORT,
            port_name=port_name,
            packet=packet,
        ))

    def send_output_salvo(self, salvo_condition_name: str) -> None:
        """Queue sending an output salvo."""
        self._actions.append(DeferredAction(
            action_type=DeferredActionType.SEND_OUTPUT_SALVO,
            salvo_condition_name=salvo_condition_name,
        ))

    @property
    def actions(self) -> List[DeferredAction]:
        """Get all queued actions."""
        return self._actions

    @property
    def consumed_values(self) -> dict[str, Any]:
        """Get all consumed values (for unconsume on retry)."""
        return self._consumed_values

    def clear(self) -> None:
        """Clear all queued actions."""
        self._actions = []
        self._consumed_values = {}
        self._deferred_packet_counter = 0
