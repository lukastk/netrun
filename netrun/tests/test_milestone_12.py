"""
Milestone 12: Checkpointing and State Serialization

Tests for checkpoint functionality including:
- PacketState serialization
- CheckpointMetadata
- save_checkpoint_state / load_checkpoint_state
- Net.save_checkpoint / Net.load_checkpoint
- Net.save_definition / Net.load_definition
"""
import pytest
import tempfile
import json
import pickle
from pathlib import Path

from netrun import (
    Net, Node, Graph, Edge, Port, PortType, PortRef,
    SalvoCondition, SalvoConditionTerm, MaxSalvos, PortState,
    NetState, NetAction, PacketLocation,
    NetNotPausedError,
    PacketState,
    CheckpointMetadata,
    LoadedCheckpoint,
    serialize_packet_location,
    deserialize_packet_location,
    get_all_packet_states,
    save_checkpoint_state,
    load_checkpoint_state,
    restore_packets_to_net,
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def simple_graph():
    """Create a simple source -> sink graph."""
    source = Node(
        name="Source",
        out_ports={"out": Port()},
        out_salvo_conditions={
            "send": SalvoCondition(
                MaxSalvos.infinite(),
                "out",
                SalvoConditionTerm.port("out", PortState.non_empty())
            )
        }
    )
    sink = Node(
        name="Sink",
        in_ports={"in": Port()},
        in_salvo_conditions={
            "receive": SalvoCondition(
                MaxSalvos.finite(1),
                "in",
                SalvoConditionTerm.port("in", PortState.non_empty())
            )
        }
    )
    edges = [
        Edge(
            PortRef("Source", PortType.Output, "out"),
            PortRef("Sink", PortType.Input, "in")
        )
    ]
    return Graph([source, sink], edges)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for checkpoints."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


# ============================================================================
# PacketState Tests
# ============================================================================

class TestPacketState:
    """Tests for PacketState dataclass."""

    def test_create_outside_net(self):
        """Test creating a PacketState for outside_net location."""
        state = PacketState(
            packet_id="test_id",
            location_kind="outside_net",
        )
        assert state.packet_id == "test_id"
        assert state.location_kind == "outside_net"
        assert state.location_data == {}

    def test_create_input_port(self):
        """Test creating a PacketState for input_port location."""
        state = PacketState(
            packet_id="test_id",
            location_kind="input_port",
            location_data={"node_name": "Sink", "port_name": "in"}
        )
        assert state.location_kind == "input_port"
        assert state.location_data["node_name"] == "Sink"
        assert state.location_data["port_name"] == "in"

    def test_to_dict(self):
        """Test conversion to dict."""
        state = PacketState(
            packet_id="test_id",
            location_kind="input_port",
            location_data={"node_name": "Node", "port_name": "port"}
        )
        d = state.to_dict()
        assert d["packet_id"] == "test_id"
        assert d["location_kind"] == "input_port"
        assert d["location_data"]["node_name"] == "Node"

    def test_from_dict(self):
        """Test creation from dict."""
        d = {
            "packet_id": "test_id",
            "location_kind": "outside_net",
            "location_data": {}
        }
        state = PacketState.from_dict(d)
        assert state.packet_id == "test_id"
        assert state.location_kind == "outside_net"


# ============================================================================
# Serialize/Deserialize Location Tests
# ============================================================================

class TestSerializeLocation:
    """Tests for serialize_packet_location."""

    def test_serialize_outside_net(self):
        """Test serializing outside_net location."""
        location = PacketLocation.outside_net()
        state = serialize_packet_location(location)
        assert state.location_kind == "outside_net"

    def test_serialize_input_port(self):
        """Test serializing input_port location."""
        location = PacketLocation.input_port("Node", "port")
        state = serialize_packet_location(location)
        assert state.location_kind == "input_port"
        assert state.location_data["node_name"] == "Node"
        assert state.location_data["port_name"] == "port"


class TestDeserializeLocation:
    """Tests for deserialize_packet_location."""

    def test_deserialize_outside_net(self, simple_graph):
        """Test deserializing outside_net location."""
        state = PacketState(
            packet_id="",
            location_kind="outside_net",
        )
        location = deserialize_packet_location(state, simple_graph)
        assert location.kind == "OutsideNet"

    def test_deserialize_input_port(self, simple_graph):
        """Test deserializing input_port location."""
        state = PacketState(
            packet_id="",
            location_kind="input_port",
            location_data={"node_name": "Sink", "port_name": "in"}
        )
        location = deserialize_packet_location(state, simple_graph)
        assert location.kind == "InputPort"


# ============================================================================
# CheckpointMetadata Tests
# ============================================================================

class TestCheckpointMetadata:
    """Tests for CheckpointMetadata dataclass."""

    def test_create_metadata(self):
        """Test creating metadata."""
        meta = CheckpointMetadata(
            timestamp="2024-01-01T00:00:00",
            packet_count=5,
            has_history=True,
        )
        assert meta.timestamp == "2024-01-01T00:00:00"
        assert meta.packet_count == 5
        assert meta.has_history is True

    def test_to_dict(self):
        """Test conversion to dict."""
        meta = CheckpointMetadata(
            timestamp="2024-01-01T00:00:00",
            packet_count=3,
        )
        d = meta.to_dict()
        assert d["timestamp"] == "2024-01-01T00:00:00"
        assert d["packet_count"] == 3

    def test_from_dict(self):
        """Test creation from dict."""
        d = {
            "timestamp": "2024-01-01T00:00:00",
            "netrun_version": "0.1.0",
            "packet_count": 2,
            "has_history": False,
        }
        meta = CheckpointMetadata.from_dict(d)
        assert meta.timestamp == "2024-01-01T00:00:00"
        assert meta.packet_count == 2


# ============================================================================
# Save/Load Checkpoint State Tests
# ============================================================================

class TestSaveLoadCheckpointState:
    """Tests for save_checkpoint_state and load_checkpoint_state."""

    def test_save_and_load(self, temp_dir, simple_graph):
        """Test saving and loading checkpoint state."""
        # Create some test data
        packet_states = [
            PacketState(packet_id="pkt1", location_kind="outside_net"),
            PacketState(
                packet_id="pkt2",
                location_kind="input_port",
                location_data={"node_name": "Sink", "port_name": "in"}
            ),
        ]
        packet_values = {"pkt1": {"data": "hello"}, "pkt2": {"data": "world"}}
        node_configs = {"Source": {"pool": None, "retries": 0}}
        node_exec_paths = {}
        node_factories = {}
        port_types = {}

        # Create net TOML
        net = Net(simple_graph)
        net_toml = net.to_toml()

        # Save
        save_checkpoint_state(
            checkpoint_dir=temp_dir / "checkpoint",
            net_definition_toml=net_toml,
            packet_states=packet_states,
            packet_values=packet_values,
            node_configs=node_configs,
            node_exec_paths=node_exec_paths,
            node_factories=node_factories,
            port_types=port_types,
        )

        # Load
        loaded = load_checkpoint_state(temp_dir / "checkpoint")

        assert loaded.net_definition_toml == net_toml
        assert len(loaded.packet_states) == 2
        assert loaded.packet_values["pkt1"]["data"] == "hello"
        assert loaded.node_configs["Source"]["retries"] == 0

    def test_saves_all_files(self, temp_dir, simple_graph):
        """Test that all expected files are created."""
        net = Net(simple_graph)

        save_checkpoint_state(
            checkpoint_dir=temp_dir / "checkpoint",
            net_definition_toml=net.to_toml(),
            packet_states=[],
            packet_values={},
            node_configs={},
            node_exec_paths={},
            node_factories={},
            port_types={},
        )

        checkpoint_dir = temp_dir / "checkpoint"
        assert (checkpoint_dir / "metadata.json").exists()
        assert (checkpoint_dir / "net_definition.toml").exists()
        assert (checkpoint_dir / "packet_states.json").exists()
        assert (checkpoint_dir / "packet_values.pkl").exists()
        assert (checkpoint_dir / "node_configs.json").exists()

    def test_history_saved_if_provided(self, temp_dir, simple_graph):
        """Test that history is saved when provided."""
        net = Net(simple_graph)

        history_data = [
            {"type": "action", "action": "RunNetUntilBlocked"},
            {"type": "event", "event": "PacketCreated"},
        ]

        save_checkpoint_state(
            checkpoint_dir=temp_dir / "checkpoint",
            net_definition_toml=net.to_toml(),
            packet_states=[],
            packet_values={},
            node_configs={},
            node_exec_paths={},
            node_factories={},
            port_types={},
            history_data=history_data,
        )

        assert (temp_dir / "checkpoint" / "history.jsonl").exists()

        loaded = load_checkpoint_state(temp_dir / "checkpoint")
        assert loaded.history_data is not None
        assert len(loaded.history_data) == 2


# ============================================================================
# Net.save_checkpoint Tests
# ============================================================================

class TestNetSaveCheckpoint:
    """Tests for Net.save_checkpoint method."""

    def test_requires_paused_state(self, simple_graph):
        """Test that save_checkpoint requires PAUSED or STOPPED state."""
        net = Net(simple_graph)

        # Net is in CREATED state - should raise
        with pytest.raises(NetNotPausedError):
            net.save_checkpoint("/tmp/checkpoint")

    def test_allows_paused_state(self, simple_graph, temp_dir):
        """Test that save_checkpoint works in PAUSED state."""
        net = Net(simple_graph)

        # Set up exec functions
        def source_exec(ctx, packets):
            pkt = ctx.create_packet({"test": "data"})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for _, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)

        # Start and pause
        net.inject_source_epoch("Source")
        net.start()
        net.pause()

        # Should now be allowed
        net.save_checkpoint(temp_dir / "checkpoint")

        assert (temp_dir / "checkpoint" / "metadata.json").exists()

    def test_allows_stopped_state(self, simple_graph, temp_dir):
        """Test that save_checkpoint works in STOPPED state."""
        net = Net(simple_graph)

        def source_exec(ctx, packets):
            pass  # Do nothing

        net.set_node_exec("Source", source_exec)

        net.inject_source_epoch("Source")
        net.start()
        net.stop()

        # Should be allowed in STOPPED state
        net.save_checkpoint(temp_dir / "checkpoint")
        assert (temp_dir / "checkpoint").exists()


# ============================================================================
# Net.load_checkpoint Tests
# ============================================================================

class TestNetLoadCheckpoint:
    """Tests for Net.load_checkpoint method."""

    def test_load_empty_checkpoint(self, simple_graph, temp_dir):
        """Test loading a checkpoint with no packets."""
        net = Net(simple_graph)

        def source_exec(ctx, packets):
            pass

        net.set_node_exec("Source", source_exec)
        net.inject_source_epoch("Source")
        net.start()
        net.pause()

        # Save
        net.save_checkpoint(temp_dir / "checkpoint")

        # Load
        net2 = Net.load_checkpoint(temp_dir / "checkpoint", resolve_funcs=False)

        assert net2._graph is not None
        assert "Source" in set(net2._graph.nodes().keys())
        assert "Sink" in set(net2._graph.nodes().keys())

    def test_load_checkpoint_with_packets(self, simple_graph, temp_dir):
        """Test loading a checkpoint that has packets waiting."""
        net = Net(simple_graph)
        results = []

        def source_exec(ctx, packets):
            # Create packet but don't trigger sink
            pkt = ctx.create_packet({"value": 42})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for _, pkts in packets.items():
                for pkt in pkts:
                    results.append(ctx.consume_packet(pkt))

        net.set_node_exec("Source", source_exec)
        # Don't set sink exec - packet will wait at input port

        net.inject_source_epoch("Source")
        net.start()
        net.pause()

        # Save checkpoint with packet waiting at Sink's input port
        net.save_checkpoint(temp_dir / "checkpoint")

        # Load into new net
        net2 = Net.load_checkpoint(temp_dir / "checkpoint", resolve_funcs=False)

        # Set up sink exec on new net
        def sink_exec2(ctx, packets):
            for _, pkts in packets.items():
                for pkt in pkts:
                    results.append(ctx.consume_packet(pkt))

        net2.set_node_exec("Sink", sink_exec2)

        # Run - should process the waiting packet
        net2.start()

        assert len(results) == 1, f"Expected 1 result, got {len(results)}"
        assert results[0]["value"] == 42


# ============================================================================
# Net.save_definition / load_definition Tests
# ============================================================================

class TestNetDefinition:
    """Tests for Net.save_definition and Net.load_definition."""

    def test_save_definition(self, simple_graph, temp_dir):
        """Test saving net definition."""
        net = Net(simple_graph)
        net.save_definition(temp_dir / "net.toml")

        assert (temp_dir / "net.toml").exists()

        # Should be valid TOML with node definitions
        with open(temp_dir / "net.toml") as f:
            content = f.read()
        # TOML format uses nested keys like [nodes.Source.out_ports.out]
        assert "nodes.Source" in content
        assert "nodes.Sink" in content

    def test_load_definition(self, simple_graph, temp_dir):
        """Test loading net definition."""
        net = Net(simple_graph)
        net.save_definition(temp_dir / "net.toml")

        net2 = Net.load_definition(temp_dir / "net.toml", resolve_funcs=False)

        assert set(net2._graph.nodes().keys()) == set(net._graph.nodes().keys())


# ============================================================================
# Integration Tests
# ============================================================================

class TestCheckpointIntegration:
    """Integration tests for checkpoint functionality."""

    def test_full_checkpoint_restore_cycle(self, temp_dir):
        """Test a complete checkpoint and restore cycle."""
        # Create a 3-node pipeline
        source = Node(
            name="Source",
            out_ports={"out": Port()},
            out_salvo_conditions={
                "send": SalvoCondition(
                    MaxSalvos.infinite(),
                    "out",
                    SalvoConditionTerm.port("out", PortState.non_empty())
                )
            }
        )
        processor = Node(
            name="Processor",
            in_ports={"in": Port()},
            out_ports={"out": Port()},
            in_salvo_conditions={
                "receive": SalvoCondition(
                    MaxSalvos.finite(1),
                    "in",
                    SalvoConditionTerm.port("in", PortState.non_empty())
                )
            },
            out_salvo_conditions={
                "send": SalvoCondition(
                    MaxSalvos.infinite(),
                    "out",
                    SalvoConditionTerm.port("out", PortState.non_empty())
                )
            }
        )
        sink = Node(
            name="Sink",
            in_ports={"in": Port()},
            in_salvo_conditions={
                "receive": SalvoCondition(
                    MaxSalvos.finite(1),
                    "in",
                    SalvoConditionTerm.port("in", PortState.non_empty())
                )
            }
        )
        edges = [
            Edge(
                PortRef("Source", PortType.Output, "out"),
                PortRef("Processor", PortType.Input, "in")
            ),
            Edge(
                PortRef("Processor", PortType.Output, "out"),
                PortRef("Sink", PortType.Input, "in")
            ),
        ]
        graph = Graph([source, processor, sink], edges)

        # First run: Source sends, Processor processes, pause before Sink
        net = Net(graph)
        results = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet({"step": 1})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def processor_exec(ctx, packets):
            for _, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    value["processed"] = True
                    out_pkt = ctx.create_packet(value)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")

        # Don't set sink exec - packet waits there

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Processor", processor_exec)

        net.inject_source_epoch("Source")
        net.start()
        net.pause()

        # Save checkpoint
        net.save_checkpoint(temp_dir / "checkpoint")

        # Load into new net
        net2 = Net.load_checkpoint(temp_dir / "checkpoint", resolve_funcs=False)

        def sink_exec(ctx, packets):
            for _, pkts in packets.items():
                for pkt in pkts:
                    results.append(ctx.consume_packet(pkt))

        net2.set_node_exec("Sink", sink_exec)

        # Continue execution
        net2.start()

        # Verify the packet was restored and processed
        assert len(results) == 1
        assert results[0]["step"] == 1
        assert results[0]["processed"] is True

    def test_checkpoint_preserves_node_configs(self, simple_graph, temp_dir):
        """Test that node configs are preserved across checkpoint."""
        net = Net(simple_graph)

        # Set some config
        net.set_node_config("Source", timeout=30.0, defer_net_actions=True, retries=2)

        def source_exec(ctx, packets):
            pass

        net.set_node_exec("Source", source_exec)
        net.inject_source_epoch("Source")
        net.start()
        net.pause()

        net.save_checkpoint(temp_dir / "checkpoint")

        # Load checkpoint data manually to verify
        loaded = load_checkpoint_state(temp_dir / "checkpoint")
        assert "Source" in loaded.node_configs
        assert loaded.node_configs["Source"]["timeout"] == 30.0
        assert loaded.node_configs["Source"]["retries"] == 2
