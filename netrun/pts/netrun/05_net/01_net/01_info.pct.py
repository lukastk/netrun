# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp net._net._info

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %% [markdown]
# # NodeInfo and EdgeInfo Classes
#
# Helper classes for inspecting nodes and edges in a running Net.

# %%
#|export
from typing import Any, TYPE_CHECKING

import netrun_sim

if TYPE_CHECKING:
    from netrun.net._net._net import Net
    from netrun.net.config import NodeConfig, EdgeConfig, PortConfig, NodeExecutionConfig

# %%
#|export
class NodeInfo:
    """Information about a node in the Net.

    Provides convenient access to node configuration and runtime state
    with helper methods for inspection and packet injection.

    This class holds internal references to the Net and should only be
    used from the same context that owns the Net (not thread-safe).
    """

    def __init__(self, net: "Net", node_name: str):
        """Initialize NodeInfo.

        Args:
            net: Reference to the parent Net.
            node_name: The node name.
        """
        self._net = net
        self._name = node_name

    @property
    def name(self) -> str:
        """Node name."""
        return self._name

    @property
    def cfg(self) -> "NodeConfig":
        """Copy of the node configuration.

        Returns a copy to prevent accidental modifications.
        """
        from netrun.net.config import NodeConfig
        for node_config in self._net._config_resolved.graph.nodes:
            if node_config.name == self._name:
                return node_config.model_copy(deep=True)
        raise KeyError(f"Node '{self._name}' not found in config")

    @property
    def in_ports(self) -> dict[str, "PortConfig"]:
        """Input port configurations."""
        return self.cfg.in_ports

    @property
    def out_ports(self) -> dict[str, "PortConfig"]:
        """Output port configurations."""
        return self.cfg.out_ports

    @property
    def in_port_names(self) -> list[str]:
        """List of input port names."""
        return list(self.in_ports.keys())

    @property
    def out_port_names(self) -> list[str]:
        """List of output port names."""
        return list(self.out_ports.keys())

    @property
    def epochs(self) -> list:
        """All epochs (running + startable) for this node.

        Returns list of Epoch objects from netrun-sim.
        """
        epochs = []
        # Get all epochs from netsim and filter by node name
        for epoch_id in self._net._netsim.get_startable_epochs():
            epoch = self._net._netsim.get_epoch(epoch_id)
            if epoch and epoch.node_name == self._name:
                epochs.append(epoch)
        # Also check running epochs
        for epoch_id in self._net._running_epochs:
            epoch = self._net._netsim.get_epoch(epoch_id)
            if epoch and epoch.node_name == self._name:
                epochs.append(epoch)
        return epochs

    @property
    def running_epochs(self) -> list:
        """Epochs currently running for this node.

        Returns list of Epoch objects from netrun-sim.
        """
        epochs = []
        for epoch_id in self._net._running_epochs:
            epoch = self._net._netsim.get_epoch(epoch_id)
            if epoch and epoch.node_name == self._name:
                epochs.append(epoch)
        return epochs

    @property
    def startable_epochs(self) -> list:
        """Epochs waiting to be started for this node.

        Returns list of Epoch objects from netrun-sim.
        """
        epochs = []
        for epoch_id in self._net._netsim.get_startable_epochs():
            epoch = self._net._netsim.get_epoch(epoch_id)
            if epoch and epoch.node_name == self._name:
                epochs.append(epoch)
        return epochs

    @property
    def epoch_count(self) -> int:
        """Total number of epochs (running + startable)."""
        return len(self.epochs)

    @property
    def is_busy(self) -> bool:
        """True if any epochs are running for this node."""
        return len(self.running_epochs) > 0

    def packets_at_input_port(self, port_name: str) -> list:
        """Get packets waiting at a specific input port.

        Args:
            port_name: The input port name.

        Returns:
            List of Packet objects from netrun-sim.
        """
        location = netrun_sim.PacketLocation.input_port(self._name, port_name)
        packet_ids = self._net._netsim.get_packets_at_location(location)
        packets = []
        for packet_id in packet_ids:
            packet = self._net._netsim.get_packet(packet_id)
            if packet:
                packets.append(packet)
        return packets

    def packets_at_all_input_ports(self) -> dict[str, list]:
        """Get all packets at all input ports.

        Returns:
            Dict mapping port_name -> list of Packet objects.
        """
        result = {}
        for port_name in self.in_port_names:
            result[port_name] = self.packets_at_input_port(port_name)
        return result

    def packets_in_epoch(self, epoch_id: str) -> list:
        """Get packets inside a specific epoch.

        Args:
            epoch_id: The epoch ID.

        Returns:
            List of Packet objects from netrun-sim.
        """
        location = netrun_sim.PacketLocation.node(epoch_id)
        packet_ids = self._net._netsim.get_packets_at_location(location)
        packets = []
        for packet_id in packet_ids:
            packet = self._net._netsim.get_packet(packet_id)
            if packet:
                packets.append(packet)
        return packets

    def inject_packets(self, port_name: str, values: list[Any]) -> list[str]:
        """Inject packets with values into an input port.

        Creates packets outside the network and transports them to the
        specified input port. This is a convenience method combining
        create_external_packets() and inject_packet().

        Args:
            port_name: The input port name.
            values: List of values for the packets.

        Returns:
            List of created packet IDs.
        """
        if not isinstance(values, list):
            raise ValueError("values must be a list")
        return self._net.inject_data(self._name, port_name, values)

    def inject_packet(self, port_name: str, value: Any) -> str:
        """Inject a single packet with a value into an input port.

        Args:
            port_name: The input port name.
            value: The value for the packet.

        Returns:
            The packet ID of the injected packet.
        """
        return self.inject_packets(port_name, [value])[0]

    def inject(self, ports: dict[str, list[Any] | Any], plural: bool = False) -> dict[str, list[str]]:
        """Inject packets into multiple input ports.

        Args:
            ports: Dict mapping port_name -> value(s) to inject.
            plural: If False (default), each value is treated as a single
                    packet value (even if it's a list). If True, values must
                    be lists and each element becomes a separate packet.

        Returns:
            Dict mapping port_name -> list of created packet IDs.

        Example:
            # plural=False (default): each value is one packet
            packet_ids = node.inject({
                "in1": [1, 2, 3],  # 1 packet with value [1, 2, 3]
                "in2": "hello",    # 1 packet with value "hello"
            })

            # plural=True: each list element is a packet
            packet_ids = node.inject({
                "in1": [1, 2, 3],  # 3 packets
                "in2": ["a", "b"],  # 2 packets
            }, plural=True)
        """
        result = {}
        for port_name, values in ports.items():
            if plural:
                if not isinstance(values, list):
                    raise ValueError(f"Values for port '{port_name}' must be a list when plural=True")
                result[port_name] = self.inject_packets(port_name, values)
            else:
                result[port_name] = [self.inject_packet(port_name, values)]
        return result

    @property
    def incoming_edges(self) -> list["EdgeInfo"]:
        """Edges targeting this node's input ports."""
        edges = []
        for edge in self._net._graph.edges():
            if edge.target.node_name == self._name:
                edges.append(EdgeInfo(self._net, edge))
        return edges

    @property
    def outgoing_edges(self) -> list["EdgeInfo"]:
        """Edges from this node's output ports."""
        edges = []
        for edge in self._net._graph.edges():
            if edge.source.node_name == self._name:
                edges.append(EdgeInfo(self._net, edge))
        return edges

    @property
    def execution_config(self) -> "NodeExecutionConfig | None":
        """Execution settings for this node."""
        return self._net._node_execution_configs.get(self._name)

    @property
    def pools(self) -> list[str]:
        """Pool names this node can execute on."""
        config = self.execution_config
        if config is None:
            return []
        return list(config.pools) if config.pools else []

    def __repr__(self) -> str:
        return f"NodeInfo(name={self._name!r})"


class EdgeInfo:
    """Information about an edge in the Net.

    Provides convenient access to edge configuration and runtime state
    with helper methods for inspecting packets in transit.

    This class holds internal references to the Net and should only be
    used from the same context that owns the Net (not thread-safe).
    """

    def __init__(self, net: "Net", edge: "netrun_sim.Edge"):
        """Initialize EdgeInfo.

        Args:
            net: Reference to the parent Net.
            edge: The netrun-sim Edge object.
        """
        self._net = net
        self._edge = edge

    @property
    def cfg(self) -> "EdgeConfig":
        """Copy of the edge configuration.

        Returns a copy to prevent accidental modifications.
        """
        from netrun.net.config import EdgeConfig, PortRefConfig
        return EdgeConfig(
            source=PortRefConfig(
                node_name=self._edge.source.node_name,
                port_name=self._edge.source.port_name,
                port_type="output",
            ),
            target=PortRefConfig(
                node_name=self._edge.target.node_name,
                port_name=self._edge.target.port_name,
                port_type="input",
            ),
        )

    @property
    def source_node(self) -> str:
        """Source node name."""
        return self._edge.source.node_name

    @property
    def source_port(self) -> str:
        """Source port name."""
        return self._edge.source.port_name

    @property
    def target_node(self) -> str:
        """Target node name."""
        return self._edge.target.node_name

    @property
    def target_port(self) -> str:
        """Target port name."""
        return self._edge.target.port_name

    @property
    def source(self) -> tuple[str, str]:
        """Source as (node_name, port_name) tuple."""
        return (self.source_node, self.source_port)

    @property
    def target(self) -> tuple[str, str]:
        """Target as (node_name, port_name) tuple."""
        return (self.target_node, self.target_port)

    @property
    def packets_in_transit(self) -> list:
        """Packets currently on this edge.

        Returns list of Packet objects from netrun-sim.
        """
        location = netrun_sim.PacketLocation.edge(self._edge)
        packet_ids = self._net._netsim.get_packets_at_location(location)
        packets = []
        for packet_id in packet_ids:
            packet = self._net._netsim.get_packet(packet_id)
            if packet:
                packets.append(packet)
        return packets

    @property
    def packet_count(self) -> int:
        """Number of packets in transit on this edge."""
        location = netrun_sim.PacketLocation.edge(self._edge)
        return self._net._netsim.packet_count_at(location)

    @property
    def has_packets(self) -> bool:
        """True if any packets are in transit on this edge."""
        return self.packet_count > 0

    def source_node_info(self) -> "NodeInfo":
        """Get NodeInfo for the source node."""
        return NodeInfo(self._net, self.source_node)

    def target_node_info(self) -> "NodeInfo":
        """Get NodeInfo for the target node."""
        return NodeInfo(self._net, self.target_node)

    def __repr__(self) -> str:
        return f"EdgeInfo({self.source_node}.{self.source_port} -> {self.target_node}.{self.target_port})"
