"""
Milestone 8 Tests: Logging and History

This milestone adds:
- Event history recording (NetAction and NetEvent)
- JSONL file persistence for history
- Node-level logging with stdout capture
"""

import asyncio
import json
import tempfile
import time
from datetime import datetime
from pathlib import Path

import pytest

from netrun import (
    Graph, Node, Edge, Port, PortType, PortRef, PortState,
    MaxSalvos, SalvoCondition, SalvoConditionTerm,
    Net, NetState, NodeConfig,
    HistoryEntry, EventHistory, NodeLogEntry, NodeLog, NodeLogManager,
    StdoutCapture, capture_stdout,
)


# ==============================================================================
# Fixtures
# ==============================================================================

@pytest.fixture
def simple_node():
    """A simple source node for testing."""
    return Node(
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


@pytest.fixture
def simple_graph(simple_node):
    """A simple one-node graph."""
    return Graph([simple_node], [])


@pytest.fixture
def pipeline_graph():
    """A Source -> Processor -> Sink pipeline."""
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
    return Graph([source, processor, sink], edges)


# ==============================================================================
# HistoryEntry Tests
# ==============================================================================

class TestHistoryEntry:
    """Test HistoryEntry dataclass."""

    def test_create_action_entry(self):
        """Can create an action entry."""
        entry = HistoryEntry(
            id="test-id",
            timestamp=datetime.now(),
            entry_type="action",
            action_type="RunNetUntilBlocked",
        )
        assert entry.entry_type == "action"
        assert entry.action_type == "RunNetUntilBlocked"

    def test_create_event_entry(self):
        """Can create an event entry."""
        entry = HistoryEntry(
            id="test-id",
            timestamp=datetime.now(),
            entry_type="event",
            event_type="PacketCreated",
            action_id="action-id",
        )
        assert entry.entry_type == "event"
        assert entry.event_type == "PacketCreated"
        assert entry.action_id == "action-id"

    def test_to_dict(self):
        """Entry can be serialized to dict."""
        now = datetime.now()
        entry = HistoryEntry(
            id="test-id",
            timestamp=now,
            entry_type="action",
            action_type="StartEpoch",
            data={"epoch_id": "e1"},
        )
        d = entry.to_dict()
        assert d["id"] == "test-id"
        assert d["type"] == "action"
        assert d["action"] == "StartEpoch"
        assert d["data"]["epoch_id"] == "e1"

    def test_from_dict(self):
        """Entry can be deserialized from dict."""
        d = {
            "id": "test-id",
            "timestamp": "2024-01-01T12:00:00",
            "type": "event",
            "event": "EpochFinished",
            "action_id": "a1",
            "data": {"success": True},
        }
        entry = HistoryEntry.from_dict(d)
        assert entry.id == "test-id"
        assert entry.entry_type == "event"
        assert entry.event_type == "EpochFinished"
        assert entry.action_id == "a1"


# ==============================================================================
# EventHistory Tests
# ==============================================================================

class TestEventHistory:
    """Test EventHistory class."""

    def test_create_empty_history(self):
        """Can create empty history."""
        history = EventHistory()
        assert len(history) == 0

    def test_record_action(self):
        """Can record an action."""
        history = EventHistory()
        action_id = history.record_action("RunNetUntilBlocked")
        assert len(history) == 1
        assert action_id is not None

    def test_record_event(self):
        """Can record an event."""
        history = EventHistory()
        action_id = history.record_action("StartEpoch")
        event_id = history.record_event("EpochStarted", action_id=action_id)
        assert len(history) == 2
        assert event_id is not None

    def test_get_actions(self):
        """Can get only action entries."""
        history = EventHistory()
        history.record_action("Action1")
        history.record_event("Event1")
        history.record_action("Action2")

        actions = history.get_actions()
        assert len(actions) == 2
        assert all(a.entry_type == "action" for a in actions)

    def test_get_events(self):
        """Can get only event entries."""
        history = EventHistory()
        history.record_action("Action1")
        history.record_event("Event1")
        history.record_event("Event2")

        events = history.get_events()
        assert len(events) == 2
        assert all(e.entry_type == "event" for e in events)

    def test_get_events_for_action(self):
        """Can get events for a specific action."""
        history = EventHistory()
        action1_id = history.record_action("Action1")
        history.record_event("Event1", action_id=action1_id)
        history.record_event("Event2", action_id=action1_id)

        action2_id = history.record_action("Action2")
        history.record_event("Event3", action_id=action2_id)

        events = history.get_events_for_action(action1_id)
        assert len(events) == 2

    def test_max_size_limit(self):
        """History respects max_size limit."""
        history = EventHistory(max_size=5)

        for i in range(10):
            history.record_action(f"Action{i}")

        assert len(history) == 5

    def test_file_persistence(self):
        """History can persist to JSONL file."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            temp_path = Path(f.name)

        try:
            history = EventHistory(file_path=temp_path, chunk_size=2)

            history.record_action("Action1")
            history.record_action("Action2")
            history.record_action("Action3")  # This should trigger flush

            history.flush()

            # Read the file
            with open(temp_path) as f:
                lines = f.readlines()

            assert len(lines) == 3
            for line in lines:
                entry = json.loads(line)
                assert "id" in entry
                assert "type" in entry
        finally:
            temp_path.unlink()

    def test_flush_clears_pending(self):
        """Flush clears pending entries."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            temp_path = Path(f.name)

        try:
            history = EventHistory(file_path=temp_path, chunk_size=100)

            history.record_action("Action1")
            history.record_action("Action2")

            # Pending entries should exist
            assert len(history._pending_entries) == 2

            history.flush()

            # Pending should be cleared
            assert len(history._pending_entries) == 0
        finally:
            temp_path.unlink()

    def test_clear_history(self):
        """Can clear all history."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            temp_path = Path(f.name)

        try:
            history = EventHistory(file_path=temp_path)
            history.record_action("Action1")
            history.flush()

            assert len(history) == 1
            assert temp_path.exists()

            history.clear()

            assert len(history) == 0
            assert not temp_path.exists()
        finally:
            if temp_path.exists():
                temp_path.unlink()


# ==============================================================================
# NodeLog Tests
# ==============================================================================

class TestNodeLog:
    """Test NodeLog class."""

    def test_create_log(self):
        """Can create a node log."""
        log = NodeLog("TestNode")
        assert log.node_name == "TestNode"
        assert len(log) == 0

    def test_add_entry(self):
        """Can add log entries."""
        log = NodeLog("TestNode")
        log.add("Test message", epoch_id="e1")
        assert len(log) == 1

    def test_get_entries(self):
        """Can get log entries."""
        log = NodeLog("TestNode")
        log.add("Message 1", epoch_id="e1")
        log.add("Message 2", epoch_id="e1")
        log.add("Message 3", epoch_id="e2")

        entries = log.get_entries()
        assert len(entries) == 3

    def test_get_entries_by_epoch(self):
        """Can filter entries by epoch."""
        log = NodeLog("TestNode")
        log.add("Message 1", epoch_id="e1")
        log.add("Message 2", epoch_id="e1")
        log.add("Message 3", epoch_id="e2")

        entries = log.get_entries(epoch_id="e1")
        assert len(entries) == 2

    def test_max_entries_limit(self):
        """Log respects max_entries limit."""
        log = NodeLog("TestNode", max_entries=5)

        for i in range(10):
            log.add(f"Message {i}")

        assert len(log) == 5

    def test_entry_has_timestamp(self):
        """Log entries have timestamps."""
        log = NodeLog("TestNode")
        log.add("Test message")

        entries = log.get_entries()
        assert entries[0].timestamp is not None


# ==============================================================================
# NodeLogManager Tests
# ==============================================================================

class TestNodeLogManager:
    """Test NodeLogManager class."""

    def test_create_manager(self):
        """Can create a log manager."""
        manager = NodeLogManager()
        assert manager is not None

    def test_get_or_create_log(self):
        """get_log creates log if not exists."""
        manager = NodeLogManager()
        log = manager.get_log("TestNode")
        assert log is not None
        assert log.node_name == "TestNode"

    def test_same_log_returned(self):
        """get_log returns same log for same node."""
        manager = NodeLogManager()
        log1 = manager.get_log("TestNode")
        log2 = manager.get_log("TestNode")
        assert log1 is log2

    def test_get_node_log(self):
        """Can get log entries via manager."""
        manager = NodeLogManager()
        log = manager.get_log("TestNode")
        log.add("Test message", epoch_id="e1")

        entries = manager.get_node_log("TestNode")
        assert len(entries) == 1

    def test_get_epoch_log(self):
        """Can get epoch-specific log entries."""
        manager = NodeLogManager()
        log = manager.get_log("TestNode")
        log.add("Message 1", epoch_id="e1")
        log.add("Message 2", epoch_id="e2")

        entries = manager.get_epoch_log("TestNode", "e1")
        assert len(entries) == 1

    def test_clear_all_logs(self):
        """Can clear all logs."""
        manager = NodeLogManager()
        manager.get_log("Node1").add("Message 1")
        manager.get_log("Node2").add("Message 2")

        manager.clear()

        # Logs should be cleared
        assert len(manager.get_node_log("Node1")) == 0


# ==============================================================================
# StdoutCapture Tests
# ==============================================================================

class TestStdoutCapture:
    """Test stdout capture functionality."""

    def test_capture_print(self):
        """Can capture print statements."""
        log = NodeLog("TestNode")

        with capture_stdout(log, "e1"):
            print("Hello, World!")

        entries = log.get_entries()
        assert len(entries) == 1
        assert entries[0].message == "Hello, World!"
        assert entries[0].epoch_id == "e1"

    def test_capture_multiple_prints(self):
        """Can capture multiple print statements."""
        log = NodeLog("TestNode")

        with capture_stdout(log, "e1"):
            print("Line 1")
            print("Line 2")
            print("Line 3")

        entries = log.get_entries()
        assert len(entries) == 3

    def test_echo_to_stdout(self):
        """Can echo captured output to stdout."""
        import io
        import sys

        log = NodeLog("TestNode")
        original_stdout = sys.stdout
        test_stdout = io.StringIO()

        try:
            sys.stdout = test_stdout
            with capture_stdout(log, "e1", echo=True):
                print("Test output")

            # Reset stdout to get value
            sys.stdout = original_stdout
            output = test_stdout.getvalue()

            assert "Test output" in output
            assert len(log.get_entries()) == 1
        finally:
            sys.stdout = original_stdout

    def test_stdout_restored_after_capture(self):
        """Stdout is properly restored after capture."""
        import sys

        log = NodeLog("TestNode")
        original_stdout = sys.stdout

        with capture_stdout(log, "e1"):
            pass

        assert sys.stdout is original_stdout

    def test_capture_handles_exception(self):
        """Capture properly handles exceptions."""
        import sys

        log = NodeLog("TestNode")
        original_stdout = sys.stdout

        try:
            with capture_stdout(log, "e1"):
                print("Before error")
                raise ValueError("Test error")
        except ValueError:
            pass

        assert sys.stdout is original_stdout
        assert len(log.get_entries()) == 1


# ==============================================================================
# Net Integration Tests
# ==============================================================================

class TestNetEventHistory:
    """Test EventHistory integration with Net."""

    def test_net_has_event_history(self, simple_graph):
        """Net has event_history property."""
        net = Net(simple_graph)
        assert net.event_history is not None
        assert isinstance(net.event_history, EventHistory)

    def test_net_history_config(self, simple_graph):
        """Net accepts history configuration."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            temp_path = Path(f.name)

        try:
            net = Net(
                simple_graph,
                history_max_size=100,
                history_file=temp_path,
                history_chunk_size=10,
                history_flush_on_pause=True,
            )

            assert net.event_history.file_path == temp_path
            assert net.event_history.flush_on_pause is True
        finally:
            temp_path.unlink(missing_ok=True)


class TestNetNodeLogging:
    """Test node logging integration with Net."""

    def test_net_has_node_log_manager(self, simple_graph):
        """Net has node_log_manager property."""
        net = Net(simple_graph)
        assert net.node_log_manager is not None
        assert isinstance(net.node_log_manager, NodeLogManager)

    def test_get_node_log(self, simple_graph):
        """Net.get_node_log works."""
        net = Net(simple_graph, on_error="continue")

        def source_exec(ctx, packets):
            print("Hello from Source!")
            pkt = ctx.create_packet({"value": 1})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        net.set_node_exec("Source", source_exec)
        net.inject_source_epoch("Source")
        net.run_step()

        log_entries = net.get_node_log("Source")
        assert len(log_entries) == 1
        assert "Hello from Source!" in log_entries[0].message

    def test_get_epoch_log(self, simple_graph):
        """Net.get_epoch_log works."""
        net = Net(simple_graph, on_error="continue")

        epoch_ids = []

        def source_exec(ctx, packets):
            epoch_ids.append(ctx.epoch_id)
            print(f"Epoch {ctx.epoch_id}")
            pkt = ctx.create_packet({"value": 1})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        net.set_node_exec("Source", source_exec)
        net.inject_source_epoch("Source")
        net.run_step()

        log_entries = net.get_epoch_log("Source", epoch_ids[0])
        assert len(log_entries) == 1

    def test_capture_stdout_disabled(self, simple_graph):
        """Can disable stdout capture."""
        net = Net(simple_graph, on_error="continue")

        def source_exec(ctx, packets):
            print("This should not be captured")
            pkt = ctx.create_packet({"value": 1})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        net.set_node_exec("Source", source_exec)
        net.set_node_config("Source", capture_stdout=False)
        net.inject_source_epoch("Source")
        net.run_step()

        log_entries = net.get_node_log("Source")
        assert len(log_entries) == 0

    def test_echo_stdout(self, simple_graph):
        """Can echo captured stdout."""
        import io
        import sys

        net = Net(simple_graph, on_error="continue")

        def source_exec(ctx, packets):
            print("Echo test")
            pkt = ctx.create_packet({"value": 1})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        net.set_node_exec("Source", source_exec)
        net.set_node_config("Source", capture_stdout=True, echo_stdout=True)

        # Capture actual stdout
        original_stdout = sys.stdout
        test_stdout = io.StringIO()

        try:
            sys.stdout = test_stdout
            net.inject_source_epoch("Source")
            net.run_step()
            sys.stdout = original_stdout
            output = test_stdout.getvalue()

            # Both captured and echoed
            log_entries = net.get_node_log("Source")
            assert len(log_entries) == 1
            assert "Echo test" in output
        finally:
            sys.stdout = original_stdout


class TestHistoryFlushOnPause:
    """Test history flush on pause."""

    def test_flush_on_pause(self, simple_graph):
        """History flushes when net is paused."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            temp_path = Path(f.name)

        try:
            net = Net(
                simple_graph,
                history_file=temp_path,
                history_flush_on_pause=True,
                on_error="continue",
            )

            # Add something to history manually
            net.event_history.record_action("TestAction")

            # Pause should flush
            net.pause()

            # Check file was written
            assert temp_path.stat().st_size > 0
        finally:
            temp_path.unlink(missing_ok=True)


# ==============================================================================
# Async Tests
# ==============================================================================

class TestAsyncLogging:
    """Test logging with async execution."""

    @pytest.mark.asyncio
    async def test_async_stdout_capture(self, simple_graph):
        """Stdout capture works with async nodes."""
        net = Net(simple_graph, on_error="continue")

        async def async_source_exec(ctx, packets):
            print("Async hello!")
            await asyncio.sleep(0.01)
            print("After sleep")
            pkt = ctx.create_packet({"value": 1})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        net.set_node_exec("Source", async_source_exec)
        net.inject_source_epoch("Source")
        await net.async_start()

        log_entries = net.get_node_log("Source")
        assert len(log_entries) == 2
        messages = [e.message for e in log_entries]
        assert "Async hello!" in messages
        assert "After sleep" in messages
