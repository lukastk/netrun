# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Log Access and Epoch Metadata

# %%
#|default_exp net.test_log_access

# %%
#|export
import pytest
from datetime import datetime, timedelta, timezone

from netrun.net._net import Net, EpochRecord
from netrun.net.config import (
    NetConfig,
    GraphConfig,
    NodeConfig,
    PortConfig,
    PoolConfig,
    MainPoolConfig,
)

# %%
#|export
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

# %%
#|export
def _mock_epoch_record(epoch_id: str, node_name: str) -> EpochRecord:
    """Create a mock EpochRecord for testing."""
    return EpochRecord(
        id=epoch_id,
        node_name=node_name,
        in_salvo=None,
        out_salvos=[],
        state=None,
        orphaned_packets=[],
        created_at=datetime.now(tz=timezone.utc),
    )

# %% [markdown]
# ## EpochRecord

# %%
#|export
def test_epoch_record_fields():
    """Test EpochRecord has correct default field values."""
    record = _mock_epoch_record("e1", "NodeA")
    assert record.id == "e1"
    assert record.node_name == "NodeA"
    assert record.logs == []
    assert record.was_cancelled is False
    assert record.started_at is None
    assert record.ended_at is None
    assert record.destroyed_packets == []

# %%
#|export
def test_epoch_record_start_time():
    """Test EpochRecord.start_time returns ms timestamp from created_at."""
    dt = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    record = EpochRecord(
        id="e1", node_name="N", in_salvo=None, out_salvos=[], state=None,
        orphaned_packets=[], created_at=dt,
    )
    expected_ms = int(dt.timestamp() * 1000)
    assert record.start_time() == expected_ms

# %%
#|export
def test_epoch_record_get_logs_returns_copy():
    """Test EpochRecord.get_logs returns a copy of print_logs."""
    record = _mock_epoch_record("e1", "NodeA")
    t = datetime.now(tz=timezone.utc)
    record.logs.append((t, "hello\n"))

    logs = record.get_logs()
    assert len(logs) == 1
    logs.append((t, "extra\n"))
    assert len(record.logs) == 1  # original not modified

# %%
#|export
def test_epoch_record_print_log(capsys):
    """Test EpochRecord.print_log outputs correctly."""
    record = _mock_epoch_record("e1", "NodeA")
    t = datetime.now(tz=timezone.utc)
    record.logs = [(t, "line1\n"), (t, "line2\n")]

    record.print_logs(include_timestamps=False)
    captured = capsys.readouterr()
    assert captured.out == "line1\nline2\n"

# %%
#|export
def test_epoch_record_print_log_with_timestamps(capsys):
    """Test EpochRecord.print_log with timestamps."""
    record = _mock_epoch_record("e1", "NodeA")
    t = datetime(2025, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
    record.logs = [(t, "msg\n")]

    record.print_logs(include_timestamps=True)
    captured = capsys.readouterr()
    assert "[10:30:00.000]" in captured.out
    assert "msg" in captured.out

# %%
#|export
def test_epoch_record_cancellation_tracking():
    """Test EpochRecord cancellation fields can be set."""
    record = _mock_epoch_record("e1", "NodeA")
    record.was_cancelled = True
    record.ended_at = datetime.now(tz=timezone.utc)
    record.destroyed_packets = ["pkt1", "pkt2"]

    assert record.was_cancelled is True
    assert record.ended_at is not None
    assert record.destroyed_packets == ["pkt1", "pkt2"]

# %% [markdown]
# ## _epochs tracking

# %%
#|export
def test_epochs_initially_empty():
    """Test _epochs starts empty."""
    net = _create_simple_net()
    assert net._epochs == {}

# %%
#|export
def test_epochs_property_returns_copy():
    """Test Net.epochs returns a shallow copy."""
    net = _create_simple_net()
    record = _mock_epoch_record("e1", "NodeA")
    net._epochs["e1"] = record

    result = net.epochs
    assert "e1" in result
    assert result["e1"] is record

    # Mutating the copy should not affect the original
    result["e2"] = _mock_epoch_record("e2", "NodeB")
    assert "e2" not in net._epochs

# %%
#|export
def test_epochs_used_by_handle_print_buffer():
    """Test _handle_print_buffer stores logs on EpochRecord."""
    net = _create_simple_net()

    # Simulate that _execute_epoch recorded the epoch
    net._epochs["epoch_1"] = _mock_epoch_record("epoch_1", "NodeA")

    timestamp = datetime.now()
    net._handle_print_buffer("epoch_1", [(timestamp, "hello\n")])

    # Should store on epoch record's print_logs
    assert len(net._epochs["epoch_1"].logs) == 1
    assert net._epochs["epoch_1"].logs[0] == (timestamp, "hello\n")

    # Should be retrievable via get_node_log
    node_logs = net.get_node_logs("NodeA")
    assert len(node_logs) == 1
    assert node_logs[0] == (timestamp, "hello\n")

# %%
#|export
def test_handle_print_buffer_unknown_epoch_skips_node_log():
    """Test _handle_print_buffer skips logging for unknown epochs."""
    net = _create_simple_net()

    # Don't register epoch in _epochs
    timestamp = datetime.now()
    net._handle_print_buffer("epoch_unknown", [(timestamp, "orphaned\n")])

    # Should not appear in any node log
    assert net.list_node_log_names() == []

    # Epoch record doesn't exist, so no print_logs stored
    assert "epoch_unknown" not in net._epochs

# %%
#|export
def test_handle_print_buffer_multiple_nodes():
    """Test _handle_print_buffer routes to correct node logs."""
    net = _create_simple_net()

    net._epochs["epoch_a1"] = _mock_epoch_record("epoch_a1", "NodeA")
    net._epochs["epoch_a2"] = _mock_epoch_record("epoch_a2", "NodeA")
    net._epochs["epoch_b1"] = _mock_epoch_record("epoch_b1", "NodeB")

    t1 = datetime.now()
    t2 = t1 + timedelta(seconds=1)
    t3 = t1 + timedelta(seconds=2)

    net._handle_print_buffer("epoch_a1", [(t1, "a1\n")])
    net._handle_print_buffer("epoch_b1", [(t2, "b1\n")])
    net._handle_print_buffer("epoch_a2", [(t3, "a2\n")])

    node_a_logs = net.get_node_logs("NodeA")
    node_b_logs = net.get_node_logs("NodeB")
    assert len(node_a_logs) == 2
    assert len(node_b_logs) == 1

    # Also check epoch records
    assert len(net._epochs["epoch_a1"].logs) == 1
    assert len(net._epochs["epoch_a2"].logs) == 1
    assert len(net._epochs["epoch_b1"].logs) == 1

# %% [markdown]
# ## get_epoch_log

# %%
#|export
def test_get_epoch_log_from_record():
    """Test get_epoch_log reads from EpochRecord.logs."""
    net = _create_simple_net()
    record = _mock_epoch_record("e1", "NodeA")
    t = datetime.now()
    record.logs = [(t, "msg\n")]
    net._epochs["e1"] = record

    result = net.get_epoch_log("e1")
    assert result == [(t, "msg\n")]

# %%
#|export
def test_get_epoch_log_unknown_epoch():
    """Test get_epoch_log returns empty list for unknown epoch."""
    net = _create_simple_net()
    result = net.get_epoch_log("nonexistent")
    assert result == []

# %% [markdown]
# ## get_all_logs_chronological

# %%
#|export
def test_get_all_logs_chronological_uses_epochs():
    """Test get_all_logs_chronological resolves node names from _epochs."""
    net = _create_simple_net()

    t1 = datetime.now()
    t2 = t1 + timedelta(seconds=1)

    record1 = _mock_epoch_record("epoch_1", "NodeA")
    record1.logs = [(t1, "from A")]
    net._epochs["epoch_1"] = record1

    record2 = _mock_epoch_record("epoch_2", "NodeB")
    record2.logs = [(t2, "from B")]
    net._epochs["epoch_2"] = record2

    result = net.get_all_logs_chronological()

    assert len(result) == 2
    assert result[0] == (t1, "epoch_1", "NodeA", "from A")
    assert result[1] == (t2, "epoch_2", "NodeB", "from B")

# %%
#|export
def test_get_all_logs_chronological_format():
    """Test get_all_logs_chronological returns correct tuple format."""
    net = _create_simple_net()

    timestamp = datetime.now()
    record = _mock_epoch_record("epoch_123", "NodeA")
    record.logs = [(timestamp, "test message")]
    net._epochs["epoch_123"] = record

    result = net.get_all_logs_chronological()

    assert len(result) == 1
    log_entry = result[0]
    assert len(log_entry) == 4
    assert log_entry[0] == timestamp
    assert log_entry[1] == "epoch_123"
    assert log_entry[2] == "NodeA"
    assert log_entry[3] == "test message"

# %% [markdown]
# ## list_epoch_log_ids / list_node_log_names

# %%
#|export
def test_list_epoch_log_ids_empty():
    """Test list_epoch_log_ids returns empty list when no logs."""
    net = _create_simple_net()

    result = net.list_epoch_log_ids()

    assert result == []

# %%
#|export
def test_list_epoch_log_ids_with_logs():
    """Test list_epoch_log_ids returns epoch IDs with logs."""
    net = _create_simple_net()

    record1 = _mock_epoch_record("epoch_1", "NodeA")
    record1.logs = [(datetime.now(), "log1")]
    net._epochs["epoch_1"] = record1

    record2 = _mock_epoch_record("epoch_2", "NodeB")
    record2.logs = [(datetime.now(), "log2")]
    net._epochs["epoch_2"] = record2

    result = net.list_epoch_log_ids()

    assert len(result) == 2
    assert "epoch_1" in result
    assert "epoch_2" in result

# %%
#|export
def test_list_epoch_log_ids_excludes_empty():
    """Test list_epoch_log_ids excludes epochs with no logs."""
    net = _create_simple_net()

    record1 = _mock_epoch_record("epoch_1", "NodeA")
    record1.logs = [(datetime.now(), "log1")]
    net._epochs["epoch_1"] = record1

    # epoch_2 has no logs
    net._epochs["epoch_2"] = _mock_epoch_record("epoch_2", "NodeB")

    result = net.list_epoch_log_ids()
    assert result == ["epoch_1"]

# %%
#|export
def test_list_node_log_names_empty():
    """Test list_node_log_names returns empty list when no logs."""
    net = _create_simple_net()

    result = net.list_node_log_names()

    assert result == []

# %%
#|export
def test_list_node_log_names_with_logs():
    """Test list_node_log_names returns node names with logs."""
    net = _create_simple_net()

    # Add logs via epoch records
    record_a = _mock_epoch_record("epoch_a", "NodeA")
    record_a.logs = [(datetime.now(), "log1")]
    net._epochs["epoch_a"] = record_a

    record_b = _mock_epoch_record("epoch_b", "NodeB")
    record_b.logs = [(datetime.now(), "log2")]
    net._epochs["epoch_b"] = record_b

    result = net.list_node_log_names()

    assert len(result) == 2
    assert "NodeA" in result
    assert "NodeB" in result

# %% [markdown]
# ## get_all_logs_chronological sorting

# %%
#|export
def test_get_all_logs_chronological_empty():
    """Test get_all_logs_chronological returns empty list when no logs."""
    net = _create_simple_net()

    result = net.get_all_logs_chronological()

    assert result == []

# %%
#|export
def test_get_all_logs_chronological_sorted():
    """Test get_all_logs_chronological returns logs sorted by timestamp."""
    net = _create_simple_net()

    # Create timestamps in non-chronological order
    t1 = datetime.now()
    t2 = t1 + timedelta(seconds=1)
    t3 = t1 + timedelta(seconds=2)

    record1 = _mock_epoch_record("epoch_1", "NodeA")
    record1.logs = [(t2, "middle")]
    net._epochs["epoch_1"] = record1

    record2 = _mock_epoch_record("epoch_2", "NodeB")
    record2.logs = [(t1, "first"), (t3, "last")]
    net._epochs["epoch_2"] = record2

    result = net.get_all_logs_chronological()

    assert len(result) == 3
    # Check sorted order
    assert result[0][3] == "first"
    assert result[1][3] == "middle"
    assert result[2][3] == "last"
    # Check timestamps are ascending
    assert result[0][0] <= result[1][0] <= result[2][0]

# %% [markdown]
# ## list returns copies

# %%
#|export
def test_list_epoch_log_ids_returns_copy():
    """Test list_epoch_log_ids returns a copy, not the internal state."""
    net = _create_simple_net()
    record = _mock_epoch_record("epoch_1", "NodeA")
    record.logs = [(datetime.now(), "log")]
    net._epochs["epoch_1"] = record

    result = net.list_epoch_log_ids()
    result.append("fake_epoch")

    # Internal state should not be modified
    assert "fake_epoch" not in net._epochs

# %%
#|export
def test_list_node_log_names_returns_copy():
    """Test list_node_log_names returns a copy, not the internal state."""
    net = _create_simple_net()
    record = _mock_epoch_record("epoch_1", "NodeA")
    record.logs = [(datetime.now(), "log")]
    net._epochs["epoch_1"] = record

    result = net.list_node_log_names()
    result.append("FakeNode")

    # Internal state should not be modified
    assert "FakeNode" not in net.list_node_log_names()

# %% [markdown]
# ## Exception Queue

# %%
#|export
def test_exception_queue_initially_empty():
    """Test that exception_queue starts empty."""
    net = _create_simple_net()
    assert net.exception_queue == []

# %%
#|export
def test_propagate_exceptions_empty_noop():
    """Test that propagate_exceptions() with empty queue doesn't raise."""
    net = _create_simple_net()
    net.propagate_exceptions()  # Should not raise

# %%
#|export
def test_propagate_exceptions_single():
    """Test propagate_exceptions() raises a single queued exception."""
    net = _create_simple_net()
    exc = ValueError("test error")
    net._exception_queue.append(exc)

    with pytest.raises(ValueError, match="test error"):
        net.propagate_exceptions()

    # Queue should be cleared
    assert net.exception_queue == []

# %%
#|export
def test_propagate_exceptions_multiple():
    """Test propagate_exceptions() raises ExceptionGroup for multiple exceptions."""
    net = _create_simple_net()
    exc1 = ValueError("error 1")
    exc2 = RuntimeError("error 2")
    net._exception_queue.extend([exc1, exc2])

    with pytest.raises(ExceptionGroup) as exc_info:
        net.propagate_exceptions()

    assert len(exc_info.value.exceptions) == 2
    assert net.exception_queue == []

# %%
#|export
def test_exception_queue_returns_copy():
    """Test that exception_queue returns a copy, not the internal list."""
    net = _create_simple_net()
    exc = ValueError("test")
    net._exception_queue.append(exc)

    queue_copy = net.exception_queue
    queue_copy.append(RuntimeError("extra"))

    # Internal queue should not be modified
    assert len(net._exception_queue) == 1

# %%
#|export
def test_get_effective_exception_config_defaults():
    """Test _get_effective_exception_config with default NetConfig."""
    net = _create_simple_net()
    # Default: propagate=True, print=False
    propagate, print_exc = net._get_effective_exception_config(None)
    assert propagate is True
    assert print_exc is False

# %%
#|export
def test_get_effective_exception_config_node_override():
    """Test _get_effective_exception_config with node-level overrides."""
    from netrun.net.config import NodeExecutionConfig

    net = _create_simple_net()

    # Node overrides propagate to False
    config = NodeExecutionConfig(propagate_exceptions=False, print_exceptions=True)
    propagate, print_exc = net._get_effective_exception_config(config)
    assert propagate is False
    assert print_exc is True

# %%
#|export
def test_get_effective_exception_config_node_none_inherits():
    """Test _get_effective_exception_config inherits when node values are None."""
    from netrun.net.config import NodeExecutionConfig

    net = _create_simple_net()

    # Node leaves both as None -> inherit from NetConfig
    config = NodeExecutionConfig(propagate_exceptions=None, print_exceptions=None)
    propagate, print_exc = net._get_effective_exception_config(config)
    assert propagate is True  # NetConfig default
    assert print_exc is False  # NetConfig default
