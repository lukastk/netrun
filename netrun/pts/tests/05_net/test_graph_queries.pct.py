# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Graph Query Methods

# %%
#|default_exp net.test_graph_queries

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
    EdgeConfig,
)

# %%
#|export
def _create_net_with_edges() -> Net:
    """Create a Net with connected and unconnected ports."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(
                name="Source",
                out_ports={"out1": PortConfig(), "out2": PortConfig()},
            ),
            NodeConfig(
                name="Middle",
                in_ports={"in": PortConfig()},
                out_ports={"out": PortConfig()},
            ),
            NodeConfig(
                name="Sink",
                in_ports={"in": PortConfig()},
                out_ports={"dangling": PortConfig()},  # Unconnected output
            ),
        ],
        edges=[
            EdgeConfig(source_str="Source.out1", target_str="Middle.in"),
            EdgeConfig(source_str="Middle.out", target_str="Sink.in"),
            # Source.out2 and Sink.dangling are unconnected
        ],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
    )

    return Net(config)

# %%
#|export
def test_get_edges_from_port_connected():
    """Test get_edges_from_port returns edges for connected port."""
    net = _create_net_with_edges()

    edges = net.get_edges_from_port("Source", "out1")

    assert len(edges) == 1
    assert edges[0].source.node_name == "Source"
    assert edges[0].source.port_name == "out1"
    assert edges[0].target.node_name == "Middle"
    assert edges[0].target.port_name == "in"

# %%
#|export
def test_get_edges_from_port_unconnected():
    """Test get_edges_from_port returns empty list for unconnected port."""
    net = _create_net_with_edges()

    edges = net.get_edges_from_port("Source", "out2")

    assert edges == []

# %%
#|export
def test_get_edges_from_port_sink_dangling():
    """Test get_edges_from_port returns empty for sink's dangling port."""
    net = _create_net_with_edges()

    edges = net.get_edges_from_port("Sink", "dangling")

    assert edges == []

# %%
#|export
def test_get_edges_from_port_nonexistent():
    """Test get_edges_from_port returns empty for nonexistent port."""
    net = _create_net_with_edges()

    edges = net.get_edges_from_port("Source", "nonexistent")

    assert edges == []

# %%
#|export
def test_has_downstream_connection_true():
    """Test has_downstream_connection returns True for connected port."""
    net = _create_net_with_edges()

    result = net.has_downstream_connection("Source", "out1")

    assert result is True

# %%
#|export
def test_has_downstream_connection_false():
    """Test has_downstream_connection returns False for unconnected port."""
    net = _create_net_with_edges()

    result = net.has_downstream_connection("Source", "out2")

    assert result is False

# %%
#|export
def test_has_downstream_connection_sink():
    """Test has_downstream_connection for sink's dangling port."""
    net = _create_net_with_edges()

    result = net.has_downstream_connection("Sink", "dangling")

    assert result is False

# %%
#|export
def test_has_downstream_connection_middle():
    """Test has_downstream_connection for middle node."""
    net = _create_net_with_edges()

    result = net.has_downstream_connection("Middle", "out")

    assert result is True

# %%
#|export
def test_graph_with_multiple_edges_from_same_port():
    """Test get_edges_from_port with fan-out topology."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(
                name="Source",
                out_ports={"out": PortConfig()},
            ),
            NodeConfig(name="SinkA", in_ports={"in": PortConfig()}),
            NodeConfig(name="SinkB", in_ports={"in": PortConfig()}),
        ],
        edges=[
            EdgeConfig(source_str="Source.out", target_str="SinkA.in"),
            EdgeConfig(source_str="Source.out", target_str="SinkB.in"),
        ],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
    )

    net = Net(config)

    edges = net.get_edges_from_port("Source", "out")

    assert len(edges) == 2
    target_nodes = {e.target.node_name for e in edges}
    assert target_nodes == {"SinkA", "SinkB"}
