# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Checkpointing and State Serialization
#
# This module provides functionality for saving and restoring Net state,
# enabling checkpointing, recovery, and reproducibility.
#
# Key capabilities:
# - Save complete Net state to a checkpoint directory
# - Restore a Net from a checkpoint
# - Save/load just the Net definition (without runtime state)

# %%
#|default_exp checkpoint

# %%
#|export
import json
import pickle
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime

from netrun_sim import (
    NetSim,
    Graph,
    NetAction,
    PacketLocation,
    PortRef,
    PortType,
)

from netrun.errors import NetNotPausedError

# %%
#|export
@dataclass
class PacketState:
    """Serializable state of a packet."""
    packet_id: str
    location_kind: str  # "outside_net", "input_port", "output_port", "edge", "node"
    location_data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PacketState":
        return cls(**data)


@dataclass
class CheckpointMetadata:
    """Metadata for a checkpoint."""
    timestamp: str
    netrun_version: str = "0.1.0"
    packet_count: int = 0
    has_history: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CheckpointMetadata":
        return cls(**data)

# %%
#|export


def serialize_packet_location(location: PacketLocation) -> PacketState:
    """
    Convert a PacketLocation to a serializable PacketState.

    Args:
        location: The PacketLocation from netrun_sim

    Returns:
        PacketState with location info
    """
    kind = location.kind

    if kind == "OutsideNet":
        return PacketState(
            packet_id="",  # Will be set by caller
            location_kind="outside_net",
        )
    elif kind == "InputPort":
        return PacketState(
            packet_id="",
            location_kind="input_port",
            location_data={
                "node_name": location.node_name,
                "port_name": location.port_name,
            }
        )
    elif kind == "OutputPort":
        return PacketState(
            packet_id="",
            location_kind="output_port",
            location_data={
                "epoch_id": location.epoch_id,
                "port_name": location.port_name,
            }
        )
    elif kind == "Edge":
        edge = location.get_edge()
        return PacketState(
            packet_id="",
            location_kind="edge",
            location_data={
                "source_node": edge.source.node_name,
                "source_port": edge.source.port_name,
                "target_node": edge.target.node_name,
                "target_port": edge.target.port_name,
            }
        )
    elif kind == "Node":
        return PacketState(
            packet_id="",
            location_kind="node",
            location_data={
                "epoch_id": location.epoch_id,
            }
        )
    else:
        raise ValueError(f"Unknown location kind: {kind}")


def deserialize_packet_location(state: PacketState, graph: Graph) -> PacketLocation:
    """
    Convert a PacketState back to a PacketLocation.

    Args:
        state: The serialized packet state
        graph: The graph (needed to reconstruct Edge locations)

    Returns:
        PacketLocation for use with netrun_sim
    """
    kind = state.location_kind
    data = state.location_data

    if kind == "outside_net":
        return PacketLocation.outside_net()
    elif kind == "input_port":
        return PacketLocation.input_port(
            data["node_name"],
            data["port_name"]
        )
    elif kind == "output_port":
        return PacketLocation.output_port(
            data["epoch_id"],
            data["port_name"]
        )
    elif kind == "edge":
        # Find the edge in the graph
        for edge in graph.edges():
            if (edge.source.node_name == data["source_node"] and
                edge.source.port_name == data["source_port"] and
                edge.target.node_name == data["target_node"] and
                edge.target.port_name == data["target_port"]):
                return PacketLocation.edge(edge)
        raise ValueError(f"Edge not found: {data}")
    elif kind == "node":
        return PacketLocation.node(data["epoch_id"])
    else:
        raise ValueError(f"Unknown location kind: {kind}")

# %%
#|export
def get_all_packet_states(net_sim: NetSim, packet_ids: List[str], graph: Graph) -> List[PacketState]:
    """
    Get the state of all packets.

    Args:
        net_sim: The NetSim instance
        packet_ids: List of packet IDs to get state for
        graph: The Graph (needed for node name lookup for packets inside epochs)

    Returns:
        List of PacketState objects
    """
    # Build a mapping of epoch_id (as string) -> node_name
    # Note: When checkpointing a paused net, there should be no running epochs
    # (they complete during pause), only startable epochs
    epoch_to_node = {}
    for epoch_id in net_sim.get_startable_epochs():
        epoch = net_sim.get_epoch(epoch_id)
        # Store as string to ensure consistent lookup
        epoch_to_node[str(epoch_id)] = epoch.node_name

    states = []
    for packet_id in packet_ids:
        try:
            packet = net_sim.get_packet(packet_id)
            state = serialize_packet_location(packet.location)
            state.packet_id = packet_id

            # Special handling for packets inside epochs:
            # Convert to input_port location - run_until_blocked will trigger salvo
            if state.location_kind == "node":
                epoch_id_str = state.location_data.get("epoch_id")
                if epoch_id_str in epoch_to_node:
                    node_name = epoch_to_node[epoch_id_str]
                    # Get the first input port of this node
                    node = graph.nodes()[node_name]
                    input_ports = list(node.in_ports.keys())
                    if input_ports:
                        state.location_kind = "input_port"
                        state.location_data = {
                            "node_name": node_name,
                            "port_name": input_ports[0],
                        }

            states.append(state)
        except Exception:
            # Packet might have been consumed/destroyed
            pass
    return states

# %%
#|export
def save_checkpoint_state(
    checkpoint_dir: Path,
    net_definition_toml: str,
    packet_states: List[PacketState],
    packet_values: Dict[str, Any],
    node_configs: Dict[str, Dict[str, Any]],
    node_exec_paths: Dict[str, Dict[str, str]],
    node_factories: Dict[str, Dict[str, Any]],
    port_types: Dict[str, Dict[str, Any]],
    history_data: Optional[List[Dict[str, Any]]] = None,
    metadata: Optional[CheckpointMetadata] = None,
) -> None:
    """
    Save all checkpoint data to a directory.

    Args:
        checkpoint_dir: Directory to save checkpoint files
        net_definition_toml: TOML string of net definition
        packet_states: List of packet states
        packet_values: Dict mapping packet_id to value
        node_configs: Node configuration options
        node_exec_paths: Node execution function paths
        node_factories: Node factory info
        port_types: Port type specifications
        history_data: Optional event history
        metadata: Optional checkpoint metadata
    """
    checkpoint_dir = Path(checkpoint_dir)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    # Save metadata
    if metadata is None:
        metadata = CheckpointMetadata(
            timestamp=datetime.now().isoformat(),
            packet_count=len(packet_states),
            has_history=history_data is not None,
        )
    with open(checkpoint_dir / "metadata.json", "w") as f:
        json.dump(metadata.to_dict(), f, indent=2)

    # Save net definition
    with open(checkpoint_dir / "net_definition.toml", "w") as f:
        f.write(net_definition_toml)

    # Save packet states as JSON
    packet_states_data = [s.to_dict() for s in packet_states]
    with open(checkpoint_dir / "packet_states.json", "w") as f:
        json.dump(packet_states_data, f, indent=2)

    # Save packet values as pickle
    with open(checkpoint_dir / "packet_values.pkl", "wb") as f:
        pickle.dump(packet_values, f)

    # Save node configs as JSON
    config_data = {
        "node_configs": node_configs,
        "node_exec_paths": node_exec_paths,
        "node_factories": node_factories,
        "port_types": port_types,
    }
    with open(checkpoint_dir / "node_configs.json", "w") as f:
        json.dump(config_data, f, indent=2)

    # Save history if provided
    if history_data is not None:
        with open(checkpoint_dir / "history.jsonl", "w") as f:
            for entry in history_data:
                f.write(json.dumps(entry) + "\n")

# %%
#|export
@dataclass
class LoadedCheckpoint:
    """Data loaded from a checkpoint."""
    metadata: CheckpointMetadata
    net_definition_toml: str
    packet_states: List[PacketState]
    packet_values: Dict[str, Any]
    node_configs: Dict[str, Dict[str, Any]]
    node_exec_paths: Dict[str, Dict[str, str]]
    node_factories: Dict[str, Dict[str, Any]]
    port_types: Dict[str, Dict[str, Any]]
    history_data: Optional[List[Dict[str, Any]]] = None


def load_checkpoint_state(checkpoint_dir: Union[str, Path]) -> LoadedCheckpoint:
    """
    Load checkpoint data from a directory.

    Args:
        checkpoint_dir: Directory containing checkpoint files

    Returns:
        LoadedCheckpoint with all checkpoint data
    """
    checkpoint_dir = Path(checkpoint_dir)

    # Load metadata
    with open(checkpoint_dir / "metadata.json", "r") as f:
        metadata = CheckpointMetadata.from_dict(json.load(f))

    # Load net definition
    with open(checkpoint_dir / "net_definition.toml", "r") as f:
        net_definition_toml = f.read()

    # Load packet states
    with open(checkpoint_dir / "packet_states.json", "r") as f:
        packet_states_data = json.load(f)
    packet_states = [PacketState.from_dict(d) for d in packet_states_data]

    # Load packet values
    with open(checkpoint_dir / "packet_values.pkl", "rb") as f:
        packet_values = pickle.load(f)

    # Load node configs
    with open(checkpoint_dir / "node_configs.json", "r") as f:
        config_data = json.load(f)
    node_configs = config_data.get("node_configs", {})
    node_exec_paths = config_data.get("node_exec_paths", {})
    node_factories = config_data.get("node_factories", {})
    port_types = config_data.get("port_types", {})

    # Load history if it exists
    history_data = None
    history_path = checkpoint_dir / "history.jsonl"
    if history_path.exists():
        history_data = []
        with open(history_path, "r") as f:
            for line in f:
                if line.strip():
                    history_data.append(json.loads(line))

    return LoadedCheckpoint(
        metadata=metadata,
        net_definition_toml=net_definition_toml,
        packet_states=packet_states,
        packet_values=packet_values,
        node_configs=node_configs,
        node_exec_paths=node_exec_paths,
        node_factories=node_factories,
        port_types=port_types,
        history_data=history_data,
    )

# %%
#|export
def restore_packets_to_net(
    net_sim: NetSim,
    packet_states: List[PacketState],
    graph: Graph,
) -> Dict[str, str]:
    """
    Recreate packets in a NetSim at their saved locations.

    Args:
        net_sim: The NetSim to add packets to
        packet_states: Saved packet states
        graph: The graph (for edge reconstruction)

    Returns:
        Mapping from old packet IDs to new packet IDs
    """
    id_mapping: Dict[str, str] = {}

    for state in packet_states:
        old_id = state.packet_id
        location = deserialize_packet_location(state, graph)

        # Create packet outside the net first
        resp, _ = net_sim.do_action(NetAction.create_packet(None))
        new_id = resp.packet_id

        # Transport to the saved location
        if state.location_kind != "outside_net":
            net_sim.do_action(NetAction.transport_packet_to_location(new_id, location))

        id_mapping[old_id] = new_id

    return id_mapping
