# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Packet Creation and Injection

# %%
#|default_exp net.test_packet_injection

# %%
#|export
import pytest

from netrun.net._net import Net
from netrun.net.config import (
    NetConfig,
    GraphConfig,
    NodeConfig,
    PortConfig,
    PoolConfig,
    MainPoolConfig,
    SalvoConditionConfig,
    SalvoConditionTermPortConfig,
    MaxSalvosFiniteConfig,
    PacketCountAllConfig,
    PortStateNonEmptyConfig,
    EdgeConfig,
)

# %%
#|export
def _create_simple_net() -> Net:
    """Create a simple Net for testing."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(
                name="Source",
                in_ports={"in": PortConfig()},
                out_ports={"out": PortConfig()},
                in_salvo_conditions={
                    "default": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"in": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(
                            port_name="in",
                            state=PortStateNonEmptyConfig(),
                        ),
                    ),
                },
            ),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
    )

    return Net(config)

# %%
#|export
def test__create_external_packet():
    """Test create_external_packet creates a packet with value."""
    net = _create_simple_net()

    packet_id = net._create_external_packet({"test": "data"})

    assert packet_id is not None
    assert isinstance(packet_id, str)
    # Verify value is stored
    assert net._packet_store._get(packet_id) == {"test": "data"}
    # Verify packet exists in netsim
    packet = net._netsim.get_packet(packet_id)
    assert packet is not None

# %%
#|export
def test__create_external_packet_various_types():
    """Test create_external_packet with various value types."""
    net = _create_simple_net()

    # Dict
    p1 = net._create_external_packet({"key": "value"})
    assert net._packet_store._get(p1) == {"key": "value"}

    # String
    p2 = net._create_external_packet("hello")
    assert net._packet_store._get(p2) == "hello"

    # Number
    p3 = net._create_external_packet(42)
    assert net._packet_store._get(p3) == 42

    # List
    p4 = net._create_external_packet([1, 2, 3])
    assert net._packet_store._get(p4) == [1, 2, 3]

    # None
    p5 = net._create_external_packet(None)
    assert net._packet_store._get(p5) is None

# %%
#|export
def test__create_external_packets():
    """Test create_external_packets creates multiple packets."""
    net = _create_simple_net()

    values = [{"id": 1}, {"id": 2}, {"id": 3}]
    packet_ids = net._create_external_packets(values)

    assert len(packet_ids) == 3
    for i, packet_id in enumerate(packet_ids):
        assert net._packet_store._get(packet_id) == values[i]

# %%
#|export
def test__create_external_packets_empty():
    """Test create_external_packets with empty list."""
    net = _create_simple_net()

    packet_ids = net._create_external_packets([])

    assert packet_ids == []

# %%
#|export
def test__inject_packet():
    """Test inject_packet transports packet to input port."""
    net = _create_simple_net()

    # Create a packet
    packet_id = net._create_external_packet({"data": "test"})

    # Inject it
    net._inject_packet(packet_id, "Source", "in")

    # Verify packet is at input port
    packet = net._netsim.get_packet(packet_id)
    location = packet.location
    assert location.kind == "InputPort"
    assert location.node_name == "Source"
    assert location.port_name == "in"

# %%
#|export
def test_inject_data():
    """Test inject_data creates and injects packets in one call."""
    net = _create_simple_net()

    values = [{"id": 1}, {"id": 2}]
    packet_ids = net.inject_data("Source", "in", values)

    assert len(packet_ids) == 2

    # Verify all packets are at input port
    for i, packet_id in enumerate(packet_ids):
        # Check value
        assert net._packet_store._get(packet_id) == values[i]
        # Check location
        packet = net._netsim.get_packet(packet_id)
        location = packet.location
        assert location.kind == "InputPort"
        assert location.node_name == "Source"
        assert location.port_name == "in"

# %%
#|export
def test_inject_data_empty():
    """Test inject_data with empty list."""
    net = _create_simple_net()

    packet_ids = net.inject_data("Source", "in", [])

    assert packet_ids == []

# %%
#|export
def test_inject_data_triggers_epoch():
    """Test that injected data can trigger an epoch."""
    net = _create_simple_net()

    # Initially no startable epochs
    assert net.get_startable_epochs() == []

    # Inject data
    net.inject_data("Source", "in", [{"data": "trigger"}])

    # Run simulation step (without auto-starting epochs)
    import asyncio
    asyncio.run(net.run_until_blocked(auto_start_epochs=False))

    # Now there should be a startable epoch
    startable = net.get_startable_epochs()
    assert len(startable) == 1

# %%
#|export
def test_inject_multiple_packets_same_port():
    """Test injecting multiple packets to the same port."""
    net = _create_simple_net()

    # Inject 5 packets
    values = [f"data_{i}" for i in range(5)]
    packet_ids = net.inject_data("Source", "in", values)

    assert len(packet_ids) == 5

    # All should be at the same input port
    for packet_id in packet_ids:
        packet = net._netsim.get_packet(packet_id)
        location = packet.location
        assert location.kind == "InputPort"
        assert location.node_name == "Source"

# %%
#|export
def test__create_and_inject_separately():
    """Test creating packets first, then injecting them."""
    net = _create_simple_net()

    # Create packets first
    p1 = net._create_external_packet("value1")
    p2 = net._create_external_packet("value2")

    # Verify they're outside the net
    assert net._netsim.get_packet(p1).location.kind == "OutsideNet"
    assert net._netsim.get_packet(p2).location.kind == "OutsideNet"

    # Inject them
    net._inject_packet(p1, "Source", "in")
    net._inject_packet(p2, "Source", "in")

    # Now they should be at input port
    assert net._netsim.get_packet(p1).location.kind == "InputPort"
    assert net._netsim.get_packet(p2).location.kind == "InputPort"
