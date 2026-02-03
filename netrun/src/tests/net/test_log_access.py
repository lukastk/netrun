# Tests for log access improvements

import pytest
from datetime import datetime, timedelta

from netrun.net._net import Net
from netrun.net.config import (
    NetConfig,
    GraphConfig,
    NodeConfig,
    PortConfig,
    PoolConfig,
    MainPoolConfig,
)


def _create_simple_net() -> Net:
    """Create a simple Net for testing."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="NodeA", in_ports={"in": PortConfig()}),
            NodeConfig(name="NodeB", in_ports={"in": PortConfig()}),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(id="main", spec=MainPoolConfig())},
        graph=graph_config,
    )

    return Net(config)


def test_list_epoch_log_ids_empty():
    """Test list_epoch_log_ids returns empty list when no logs."""
    net = _create_simple_net()

    result = net.list_epoch_log_ids()

    assert result == []


def test_list_epoch_log_ids_with_logs():
    """Test list_epoch_log_ids returns epoch IDs with logs."""
    net = _create_simple_net()

    # Manually add some logs
    net._epoch_print_logs["epoch_1"] = [(datetime.now(), "log1")]
    net._epoch_print_logs["epoch_2"] = [(datetime.now(), "log2")]

    result = net.list_epoch_log_ids()

    assert len(result) == 2
    assert "epoch_1" in result
    assert "epoch_2" in result


def test_list_node_log_names_empty():
    """Test list_node_log_names returns empty list when no logs."""
    net = _create_simple_net()

    result = net.list_node_log_names()

    assert result == []


def test_list_node_log_names_with_logs():
    """Test list_node_log_names returns node names with logs."""
    net = _create_simple_net()

    # Manually add some logs
    net._node_print_logs["NodeA"] = [(datetime.now(), "log1")]
    net._node_print_logs["NodeB"] = [(datetime.now(), "log2")]

    result = net.list_node_log_names()

    assert len(result) == 2
    assert "NodeA" in result
    assert "NodeB" in result


def test_get_all_logs_chronological_empty():
    """Test get_all_logs_chronological returns empty list when no logs."""
    net = _create_simple_net()

    result = net.get_all_logs_chronological()

    assert result == []


def test_get_all_logs_chronological_sorted():
    """Test get_all_logs_chronological returns logs sorted by timestamp."""
    net = _create_simple_net()

    # Create timestamps in non-chronological order
    t1 = datetime.now()
    t2 = t1 + timedelta(seconds=1)
    t3 = t1 + timedelta(seconds=2)

    # Add logs out of order
    net._epoch_print_logs["epoch_1"] = [(t2, "middle")]
    net._epoch_print_logs["epoch_2"] = [(t1, "first"), (t3, "last")]

    result = net.get_all_logs_chronological()

    assert len(result) == 3
    # Check sorted order
    assert result[0][3] == "first"
    assert result[1][3] == "middle"
    assert result[2][3] == "last"
    # Check timestamps are ascending
    assert result[0][0] <= result[1][0] <= result[2][0]


def test_get_all_logs_chronological_format():
    """Test get_all_logs_chronological returns correct tuple format."""
    net = _create_simple_net()

    timestamp = datetime.now()
    net._epoch_print_logs["epoch_123"] = [(timestamp, "test message")]

    result = net.get_all_logs_chronological()

    assert len(result) == 1
    log_entry = result[0]
    assert len(log_entry) == 4
    assert log_entry[0] == timestamp  # timestamp
    assert log_entry[1] == "epoch_123"  # epoch_id
    # node_name will be "unknown" since epoch doesn't exist in netsim
    assert log_entry[2] == "unknown"  # node_name
    assert log_entry[3] == "test message"  # message


def test_list_epoch_log_ids_returns_copy():
    """Test list_epoch_log_ids returns a copy, not the internal dict keys."""
    net = _create_simple_net()
    net._epoch_print_logs["epoch_1"] = [(datetime.now(), "log")]

    result = net.list_epoch_log_ids()
    result.append("fake_epoch")

    # Internal state should not be modified
    assert "fake_epoch" not in net._epoch_print_logs


def test_list_node_log_names_returns_copy():
    """Test list_node_log_names returns a copy, not the internal dict keys."""
    net = _create_simple_net()
    net._node_print_logs["NodeA"] = [(datetime.now(), "log")]

    result = net.list_node_log_names()
    result.append("FakeNode")

    # Internal state should not be modified
    assert "FakeNode" not in net._node_print_logs
