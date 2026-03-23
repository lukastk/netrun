# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Output Queue Functionality
#
# Tests for output queues, routing, and the `error_on_undeclared_output` config.

# %%
#|default_exp net.test_output_queues

# %%
#|export
import pytest
import asyncio
from datetime import datetime

from netrun.net._net import (
    Net,
    ConsumedOutputPacket,
)
from netrun.net.config import (
    NetConfig,
    GraphConfig,
    NodeConfig,
    PortConfig,
    PoolConfig,
    MainPoolConfig,
    OutputQueueConfig,
)

# %% [markdown]
# ## Basic dataclass and config tests

# %%
#|export
def test_consumed_output_packet_dataclass():
    """Test ConsumedOutputPacket dataclass creation and fields."""
    timestamp = datetime.now()
    packet = ConsumedOutputPacket(
        packet_id="test_id",
        value={"data": "test"},
        from_node="TestNode",
        from_port="out",
        queue_name="results",
        timestamp=timestamp,
        epoch_id="epoch_123",
    )

    assert packet.packet_id == "test_id"
    assert packet.value == {"data": "test"}
    assert packet.from_node == "TestNode"
    assert packet.from_port == "out"
    assert packet.queue_name == "results"
    assert packet.timestamp == timestamp
    assert packet.epoch_id == "epoch_123"

# %%
#|export
def test_output_queue_config():
    """Test OutputQueueConfig creation."""
    config = OutputQueueConfig(
        ports=[("Sink", "out"), ("Logger", "out")]
    )

    assert len(config.ports) == 2
    assert ("Sink", "out") in config.ports
    assert ("Logger", "out") in config.ports

# %%
#|export
def test_net_config_with_output_queues():
    """Test NetConfig with output queue configuration."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Sink", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
        },
        error_on_undeclared_output=False,
    )

    assert "results" in config.output_queues
    assert config.error_on_undeclared_output is False

# %% [markdown]
# ## Net output queue initialization

# %%
#|export
def test_net_initializes_output_queues():
    """Test that Net initializes output queues from config."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Sink", out_ports={"out": PortConfig()}),
            NodeConfig(name="Logger", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
            "logs": OutputQueueConfig(ports=[("Logger", "out")]),
        },
    )

    net = Net(config)

    # Check queues were created
    assert "results" in net._output_queues
    assert "logs" in net._output_queues

    # Check port-to-queue mapping
    assert net._port_to_queue[("Sink", "out")] == "results"
    assert net._port_to_queue[("Logger", "out")] == "logs"

# %% [markdown]
# ## Output queue query methods

# %%
#|export
def test_net_try_get_output_empty():
    """Test try_get_output returns None for empty queue."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Sink", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
        },
    )

    net = Net(config)
    result = net.try_get_output("results")
    assert result is None

# %%
#|export
def test_net_flush_output_queue_empty():
    """Test flush_output_queue returns empty list for empty queue."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Sink", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
        },
    )

    net = Net(config)
    result = net.flush_output_queue("results")
    assert result == []

# %% [markdown]
# ## Packet routing tests

# %%
#|export
def test_net_route_orphaned_packet_to_queue():
    """Test _route_orphaned_packet routes to configured queue."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Sink", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
        },
    )

    net = Net(config)

    # Register a packet value
    net._packet_store.register("pkt_123", {"data": "test"})

    # Route the packet
    net._route_orphaned_packet(
        packet_id="pkt_123",
        from_node="Sink",
        from_port="out",
        epoch_id="epoch_1",
    )

    # Default: get just the value
    value = net.try_get_output("results")
    assert value == {"data": "test"}

# %%
#|export
def test_net_route_orphaned_packet_to_queue_with_metadata():
    """Test _route_orphaned_packet with include_metadata=True."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Sink", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
        },
    )

    net = Net(config)

    # Register a packet value
    net._packet_store.register("pkt_123", {"data": "test"})

    # Route the packet
    net._route_orphaned_packet(
        packet_id="pkt_123",
        from_node="Sink",
        from_port="out",
        epoch_id="epoch_1",
    )

    # Get with metadata
    packet = net.try_get_output("results", include_metadata=True)
    assert isinstance(packet, ConsumedOutputPacket)
    assert packet.packet_id == "pkt_123"
    assert packet.value == {"data": "test"}
    assert packet.from_node == "Sink"
    assert packet.from_port == "out"
    assert packet.queue_name == "results"
    assert packet.epoch_id == "epoch_1"

# %% [markdown]
# ## error_on_undeclared_output tests

# %%
#|export
def test_net_route_orphaned_packet_discard():
    """Test _route_orphaned_packet discards when error_on_undeclared_output is False."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Unknown", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={},  # No specific queues
        error_on_undeclared_output=False,
    )

    net = Net(config)

    # Register a packet value
    net._packet_store.register("pkt_789", "discard_value")

    # Route the packet (should be discarded)
    net._route_orphaned_packet(
        packet_id="pkt_789",
        from_node="Unknown",
        from_port="out",
        epoch_id="epoch_3",
    )

    # Packet value should be consumed (discarded)
    with pytest.raises(KeyError):
        net._packet_store.peek("pkt_789")

# %%
#|export
def test_net_route_orphaned_packet_error():
    """Test _route_orphaned_packet raises when error_on_undeclared_output is True."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Unknown", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={},  # No specific queues
        error_on_undeclared_output=True,
    )

    net = Net(config)

    # Register a packet value
    net._packet_store.register("pkt_error", "error_value")

    # Route the packet (should raise error)
    with pytest.raises(RuntimeError, match="no output queue configured"):
        net._route_orphaned_packet(
            packet_id="pkt_error",
            from_node="Unknown",
            from_port="out",
            epoch_id="epoch_4",
        )

# %% [markdown]
# ## Queue not found errors

# %%
#|export
def test_net_output_queue_not_found():
    """Test output queue methods raise KeyError for unknown queue."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Sink", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={},  # No queues configured
    )

    net = Net(config)

    with pytest.raises(KeyError, match="not found"):
        net.try_get_output("unknown")

    with pytest.raises(KeyError, match="not found"):
        net.flush_output_queue("unknown")

# %% [markdown]
# ## Queue name resolution

# %%
#|export
def test_net_resolve_queue_name_by_node_port():
    """Test _resolve_queue_name with node/port lookup."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Sink", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
        },
    )

    net = Net(config)
    resolved = net._resolve_queue_name(None, node="Sink", port="out")
    assert resolved == "results"

# %%
#|export
def test_net_resolve_queue_name_errors():
    """Test _resolve_queue_name raises errors for invalid args."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Sink", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
        },
    )

    net = Net(config)

    # Neither queue_name nor node/port
    with pytest.raises(ValueError, match="Must specify"):
        net._resolve_queue_name(None, None, None)

    # Both queue_name and node/port
    with pytest.raises(ValueError, match="Cannot specify both"):
        net._resolve_queue_name("results", node="Sink", port="out")

    # Only node, no port
    with pytest.raises(ValueError, match="Must specify both"):
        net._resolve_queue_name(None, node="Sink", port=None)

    # Node/port not configured
    with pytest.raises(KeyError, match="No output queue configured"):
        net._resolve_queue_name(None, node="Unknown", port="out")

# %% [markdown]
# ## Async output methods

# %%
#|export
@pytest.mark.asyncio
async def test_net_get_output_with_timeout():
    """Test get_output with timeout raises TimeoutError."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Sink", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
        },
    )

    net = Net(config)

    with pytest.raises(asyncio.TimeoutError):
        await net.get_output("results", timeout=0.01)

# %%
#|export
@pytest.mark.asyncio
async def test_net_get_output_returns_value_by_default():
    """Test get_output returns just the value by default."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Sink", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
        },
    )

    net = Net(config)

    # Manually add a packet to the queue
    net._packet_store.register("pkt_async", {"async": "data"})
    net._route_orphaned_packet("pkt_async", "Sink", "out", "epoch_async")

    # Default: get just the value
    value = await net.get_output("results", timeout=1.0)
    assert value == {"async": "data"}
    assert not isinstance(value, ConsumedOutputPacket)

# %%
#|export
@pytest.mark.asyncio
async def test_net_get_output_with_metadata():
    """Test get_output with include_metadata=True returns ConsumedOutputPacket."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Sink", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
        },
    )

    net = Net(config)

    # Manually add a packet to the queue
    net._packet_store.register("pkt_async", {"async": "data"})
    net._route_orphaned_packet("pkt_async", "Sink", "out", "epoch_async")

    # Get with metadata
    packet = await net.get_output("results", timeout=1.0, include_metadata=True)
    assert isinstance(packet, ConsumedOutputPacket)
    assert packet.value == {"async": "data"}
    assert packet.from_node == "Sink"
    assert packet.from_port == "out"

# %%
#|export
@pytest.mark.asyncio
async def test_net_get_output_by_node_port():
    """Test get_output with node/port keywords."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Sink", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
        },
    )

    net = Net(config)

    # Add a packet
    net._packet_store.register("pkt_node", "node_port_data")
    net._route_orphaned_packet("pkt_node", "Sink", "out", "epoch_np")

    # Default: get just the value using node/port
    value = await net.get_output(node="Sink", port="out", timeout=1.0)
    assert value == "node_port_data"

# %% [markdown]
# ## Flush methods

# %%
#|export
def test_net_flush_output_queue_returns_values_by_default():
    """Test flush_output_queue returns values by default."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Sink", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
        },
    )

    net = Net(config)

    # Add multiple packets
    for i in range(3):
        net._packet_store.register(f"pkt_{i}", f"value_{i}")
        net._route_orphaned_packet(f"pkt_{i}", "Sink", "out", f"epoch_{i}")

    # Default: get just values
    values = net.flush_output_queue("results")
    assert values == ["value_0", "value_1", "value_2"]

# %%
#|export
def test_net_flush_output_queue_with_metadata():
    """Test flush_output_queue with include_metadata=True."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Sink", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
        },
    )

    net = Net(config)

    # Add multiple packets
    for i in range(3):
        net._packet_store.register(f"pkt_{i}", f"value_{i}")
        net._route_orphaned_packet(f"pkt_{i}", "Sink", "out", f"epoch_{i}")

    # Get with metadata
    packets = net.flush_output_queue("results", include_metadata=True)
    assert len(packets) == 3
    for i, packet in enumerate(packets):
        assert isinstance(packet, ConsumedOutputPacket)
        assert packet.value == f"value_{i}"
        assert packet.from_node == "Sink"

# %%
#|export
def test_net_flush_all_output_queues_returns_values_by_default():
    """Test flush_all_output_queues returns values by default."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Sink", out_ports={"out": PortConfig()}),
            NodeConfig(name="Logger", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
            "logs": OutputQueueConfig(ports=[("Logger", "out")]),
        },
    )

    net = Net(config)

    # Add packets to results queue
    net._packet_store.register("pkt_r1", "result_1")
    net._route_orphaned_packet("pkt_r1", "Sink", "out", "epoch_r1")

    # Add packets to logs queue
    net._packet_store.register("pkt_l1", "log_1")
    net._route_orphaned_packet("pkt_l1", "Logger", "out", "epoch_l1")

    # Default: get just values
    all_output = net.flush_all_output_queues()
    assert all_output == {
        "results": ["result_1"],
        "logs": ["log_1"],
    }

# %%
#|export
def test_net_flush_all_output_queues_with_metadata():
    """Test flush_all_output_queues with include_metadata=True."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Sink", out_ports={"out": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
        },
    )

    net = Net(config)

    net._packet_store.register("pkt_1", "value_1")
    net._route_orphaned_packet("pkt_1", "Sink", "out", "epoch_1")

    all_output = net.flush_all_output_queues(include_metadata=True)
    assert "results" in all_output
    assert len(all_output["results"]) == 1
    assert isinstance(all_output["results"][0], ConsumedOutputPacket)
    assert all_output["results"][0].value == "value_1"
