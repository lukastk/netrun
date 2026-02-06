# Tests for log access and epoch metadata

import pytest
from datetime import datetime, timedelta
from types import SimpleNamespace

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
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
    )

    return Net(config)


def _mock_epoch(node_name: str):
    """Create a mock epoch object with a node_name attribute."""
    return SimpleNamespace(node_name=node_name)


# --- _epochs tracking ---


def test_epochs_initially_empty():
    """Test _epochs starts empty."""
    net = _create_simple_net()
    assert net._epochs == {}


def test_epochs_used_by_handle_print_buffer():
    """Test _handle_print_buffer uses _epochs for node-level logging."""
    net = _create_simple_net()

    # Simulate that _execute_epoch recorded the epoch
    net._epochs["epoch_1"] = _mock_epoch("NodeA")

    timestamp = datetime.now()
    net._handle_print_buffer("epoch_1", [(timestamp, "hello\n")])

    # Should store in epoch log
    assert "epoch_1" in net._epoch_print_logs
    assert len(net._epoch_print_logs["epoch_1"]) == 1

    # Should also store in node log (using _epochs lookup)
    assert "NodeA" in net._node_print_logs
    assert len(net._node_print_logs["NodeA"]) == 1
    assert net._node_print_logs["NodeA"][0] == (timestamp, "hello\n")


def test_handle_print_buffer_unknown_epoch_skips_node_log():
    """Test _handle_print_buffer skips node-level logging for unknown epochs."""
    net = _create_simple_net()

    # Don't register epoch in _epochs
    timestamp = datetime.now()
    net._handle_print_buffer("epoch_unknown", [(timestamp, "orphaned\n")])

    # Should store in epoch log
    assert "epoch_unknown" in net._epoch_print_logs

    # Should NOT store in any node log
    assert len(net._node_print_logs) == 0


def test_handle_print_buffer_multiple_nodes():
    """Test _handle_print_buffer routes to correct node logs."""
    net = _create_simple_net()

    net._epochs["epoch_a1"] = _mock_epoch("NodeA")
    net._epochs["epoch_a2"] = _mock_epoch("NodeA")
    net._epochs["epoch_b1"] = _mock_epoch("NodeB")

    t1 = datetime.now()
    t2 = t1 + timedelta(seconds=1)
    t3 = t1 + timedelta(seconds=2)

    net._handle_print_buffer("epoch_a1", [(t1, "a1\n")])
    net._handle_print_buffer("epoch_b1", [(t2, "b1\n")])
    net._handle_print_buffer("epoch_a2", [(t3, "a2\n")])

    assert len(net._node_print_logs["NodeA"]) == 2
    assert len(net._node_print_logs["NodeB"]) == 1


# --- get_all_logs_chronological ---


def test_get_all_logs_chronological_uses_epochs():
    """Test get_all_logs_chronological resolves node names from _epochs."""
    net = _create_simple_net()

    # Register epoch metadata (as _execute_epoch would)
    net._epochs["epoch_1"] = _mock_epoch("NodeA")
    net._epochs["epoch_2"] = _mock_epoch("NodeB")

    t1 = datetime.now()
    t2 = t1 + timedelta(seconds=1)

    net._epoch_print_logs["epoch_1"] = [(t1, "from A")]
    net._epoch_print_logs["epoch_2"] = [(t2, "from B")]

    result = net.get_all_logs_chronological()

    assert len(result) == 2
    # Check node names are properly resolved (not "unknown")
    assert result[0] == (t1, "epoch_1", "NodeA", "from A")
    assert result[1] == (t2, "epoch_2", "NodeB", "from B")


def test_get_all_logs_chronological_format():
    """Test get_all_logs_chronological returns correct tuple format."""
    net = _create_simple_net()

    timestamp = datetime.now()
    net._epochs["epoch_123"] = _mock_epoch("NodeA")
    net._epoch_print_logs["epoch_123"] = [(timestamp, "test message")]

    result = net.get_all_logs_chronological()

    assert len(result) == 1
    log_entry = result[0]
    assert len(log_entry) == 4
    assert log_entry[0] == timestamp
    assert log_entry[1] == "epoch_123"
    assert log_entry[2] == "NodeA"
    assert log_entry[3] == "test message"


def test_get_all_logs_chronological_unknown_epoch_fallback():
    """Test get_all_logs_chronological uses 'unknown' for unregistered epochs."""
    net = _create_simple_net()

    timestamp = datetime.now()
    # Epoch log exists but no corresponding _epochs entry
    net._epoch_print_logs["orphan_epoch"] = [(timestamp, "orphan msg")]

    result = net.get_all_logs_chronological()

    assert len(result) == 1
    assert result[0][2] == "unknown"


# --- list_epoch_log_ids / list_node_log_names ---


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


# --- get_all_logs_chronological sorting ---


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

    net._epochs["epoch_1"] = _mock_epoch("NodeA")
    net._epochs["epoch_2"] = _mock_epoch("NodeB")

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


# --- list returns copies ---


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
