"""Tests for Milestone 4: Error Handling and Retries."""

import pytest
import time
import tempfile
from pathlib import Path
from datetime import datetime

from netrun import (
    # Graph types
    Graph,
    Node,
    Edge,
    Port,
    PortType,
    PortRef,
    PortState,
    MaxSalvos,
    SalvoCondition,
    SalvoConditionTerm,
    # Net
    Net,
    NetState,
    # DLQ
    DeadLetterQueue,
    DeadLetterEntry,
    # Errors
    NodeExecutionFailed,
)


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def source_sink_graph():
    """Create a simple Source -> Sink graph."""
    source_node = Node(
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

    sink_node = Node(
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
        ),
    ]

    return Graph([source_node, sink_node], edges)


# =============================================================================
# Dead Letter Queue Tests
# =============================================================================

class TestDeadLetterQueue:
    """Tests for DeadLetterQueue class."""

    def test_dlq_memory_mode(self):
        """Test DLQ in memory mode."""
        dlq = DeadLetterQueue(mode="memory")
        assert dlq.mode == "memory"
        assert len(dlq) == 0

    def test_dlq_add_entry(self):
        """Test adding entries to DLQ."""
        dlq = DeadLetterQueue(mode="memory")

        entry = DeadLetterEntry(
            epoch_id="epoch-123",
            node_name="TestNode",
            exception=ValueError("Test error"),
            retry_count=2,
            retry_timestamps=[datetime.now()],
            retry_exceptions=[ValueError("Error 1"), ValueError("Error 2")],
            input_packets={"in": ["pkt-1", "pkt-2"]},
            packet_values={"pkt-1": "value1"},
            timestamp=datetime.now(),
        )

        dlq.add(entry)
        assert len(dlq) == 1

        entries = dlq.get_all()
        assert len(entries) == 1
        assert entries[0].epoch_id == "epoch-123"
        assert entries[0].node_name == "TestNode"

    def test_dlq_get_by_node(self):
        """Test filtering DLQ entries by node."""
        dlq = DeadLetterQueue(mode="memory")

        # Add entries for different nodes
        for i, node in enumerate(["NodeA", "NodeB", "NodeA"]):
            entry = DeadLetterEntry(
                epoch_id=f"epoch-{i}",
                node_name=node,
                exception=ValueError(f"Error {i}"),
                retry_count=0,
                retry_timestamps=[datetime.now()],
                retry_exceptions=[ValueError(f"Error {i}")],
                input_packets={},
                packet_values={},
                timestamp=datetime.now(),
            )
            dlq.add(entry)

        assert len(dlq) == 3
        assert len(dlq.get_by_node("NodeA")) == 2
        assert len(dlq.get_by_node("NodeB")) == 1
        assert len(dlq.get_by_node("NodeC")) == 0

    def test_dlq_clear(self):
        """Test clearing DLQ."""
        dlq = DeadLetterQueue(mode="memory")

        entry = DeadLetterEntry(
            epoch_id="epoch-1",
            node_name="Test",
            exception=ValueError("Error"),
            retry_count=0,
            retry_timestamps=[datetime.now()],
            retry_exceptions=[],
            input_packets={},
            packet_values={},
            timestamp=datetime.now(),
        )
        dlq.add(entry)
        assert len(dlq) == 1

        dlq.clear()
        assert len(dlq) == 0

    def test_dlq_file_mode(self):
        """Test DLQ in file mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "dlq.json"
            dlq = DeadLetterQueue(mode="file", file_path=file_path)

            entry = DeadLetterEntry(
                epoch_id="epoch-1",
                node_name="Test",
                exception=ValueError("Error"),
                retry_count=0,
                retry_timestamps=[datetime.now()],
                retry_exceptions=[ValueError("Error")],
                input_packets={"in": ["pkt-1"]},
                packet_values={},
                timestamp=datetime.now(),
            )
            dlq.add(entry)

            # File should exist
            assert file_path.exists()

            # Check file contents
            import json
            with open(file_path) as f:
                data = json.load(f)
            assert len(data) == 1
            assert data[0]["epoch_id"] == "epoch-1"
            assert data[0]["node_name"] == "Test"

    def test_dlq_callback_mode(self):
        """Test DLQ in callback mode."""
        received = []

        def callback(entry):
            received.append(entry)

        dlq = DeadLetterQueue(mode="callback", callback=callback)

        entry = DeadLetterEntry(
            epoch_id="epoch-1",
            node_name="Test",
            exception=ValueError("Error"),
            retry_count=0,
            retry_timestamps=[datetime.now()],
            retry_exceptions=[],
            input_packets={},
            packet_values={},
            timestamp=datetime.now(),
        )
        dlq.add(entry)

        # Callback should have been called
        assert len(received) == 1
        assert received[0].epoch_id == "epoch-1"

        # Entry should NOT be stored in memory
        assert len(dlq) == 0


# =============================================================================
# Retry Tests
# =============================================================================

class TestRetryExecution:
    """Tests for retry logic in execution."""

    def test_retry_success_on_second_attempt(self, source_sink_graph):
        """Test that execution succeeds on retry."""
        net = Net(source_sink_graph, on_error="raise")
        call_count = [0]

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            call_count[0] += 1
            if call_count[0] < 2:
                raise ValueError(f"Fail on attempt {call_count[0]}")
            # Succeed on second attempt
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)
        net.set_node_config("Sink", retries=2, defer_net_actions=True)

        net.inject_source_epoch("Source")
        net.start()

        # Should have succeeded after retry
        assert call_count[0] == 2
        assert net.state == NetState.PAUSED  # Normal completion

    def test_retry_exhaustion_goes_to_dlq(self, source_sink_graph):
        """Test that exhausted retries put entry in DLQ."""
        net = Net(source_sink_graph, on_error="continue")

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            raise ValueError("Always fails")

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)
        net.set_node_config("Sink", retries=2, defer_net_actions=True)

        net.inject_source_epoch("Source")
        net.start()

        # Should have entry in DLQ
        assert len(net.dead_letter_queue) == 1
        entry = net.dead_letter_queue.get_all()[0]
        assert entry.node_name == "Sink"
        assert entry.retry_count == 2  # 0-indexed, so 2 means 3 attempts total

    def test_retry_wait_timing(self, source_sink_graph):
        """Test that retry_wait delays between retries."""
        net = Net(source_sink_graph, on_error="continue")
        timestamps = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            timestamps.append(time.time())
            if len(timestamps) < 3:
                raise ValueError(f"Fail #{len(timestamps)}")
            # Succeed on third attempt
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)
        net.set_node_config("Sink", retries=2, defer_net_actions=True, retry_wait=0.1)

        net.inject_source_epoch("Source")
        net.start()

        # Should have 3 timestamps
        assert len(timestamps) == 3

        # Check delays between attempts
        delay1 = timestamps[1] - timestamps[0]
        delay2 = timestamps[2] - timestamps[1]
        assert delay1 >= 0.09  # Allow some timing slack
        assert delay2 >= 0.09

    def test_failed_func_called_on_each_retry(self, source_sink_graph):
        """Test that failed_func is called on each failure."""
        net = Net(source_sink_graph, on_error="continue")
        failed_calls = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            raise ValueError("Always fails")

        def sink_failed(failure_ctx):
            failed_calls.append(failure_ctx.retry_count)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec, failed_func=sink_failed)
        net.set_node_config("Sink", retries=2, defer_net_actions=True)

        net.inject_source_epoch("Source")
        net.start()

        # Should have been called 3 times (initial + 2 retries)
        assert len(failed_calls) == 3
        assert failed_calls == [0, 1, 2]

    def test_retry_context_has_correct_info(self, source_sink_graph):
        """Test that retry context has correct retry info."""
        net = Net(source_sink_graph, on_error="continue")
        contexts = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            contexts.append({
                "retry_count": ctx.retry_count,
                "retry_timestamps": len(ctx.retry_timestamps),
                "retry_exceptions": len(ctx.retry_exceptions),
            })
            if ctx.retry_count < 2:
                raise ValueError(f"Fail {ctx.retry_count}")
            # Succeed on third attempt
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)
        net.set_node_config("Sink", retries=2, defer_net_actions=True)

        net.inject_source_epoch("Source")
        net.start()

        assert len(contexts) == 3
        # First attempt
        assert contexts[0]["retry_count"] == 0
        assert contexts[0]["retry_timestamps"] == 0
        assert contexts[0]["retry_exceptions"] == 0
        # Second attempt
        assert contexts[1]["retry_count"] == 1
        assert contexts[1]["retry_timestamps"] == 1
        assert contexts[1]["retry_exceptions"] == 1
        # Third attempt
        assert contexts[2]["retry_count"] == 2
        assert contexts[2]["retry_timestamps"] == 2
        assert contexts[2]["retry_exceptions"] == 2


# =============================================================================
# Error Handling Mode Tests
# =============================================================================

class TestErrorHandlingWithRetries:
    """Tests for error handling modes with retries."""

    def test_on_error_raise_after_retries(self, source_sink_graph):
        """Test that on_error=raise raises after retries exhausted."""
        net = Net(source_sink_graph, on_error="raise")

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            raise ValueError("Always fails")

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)
        net.set_node_config("Sink", retries=1, defer_net_actions=True)

        net.inject_source_epoch("Source")

        with pytest.raises(NodeExecutionFailed) as exc_info:
            net.start()

        assert exc_info.value.node_name == "Sink"
        assert net.state == NetState.PAUSED

    def test_on_error_pause_after_retries(self, source_sink_graph):
        """Test that on_error=pause pauses after retries exhausted."""
        net = Net(source_sink_graph, on_error="pause")

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            raise ValueError("Always fails")

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)
        net.set_node_config("Sink", retries=1, defer_net_actions=True)

        net.inject_source_epoch("Source")
        net.start()

        assert net.state == NetState.PAUSED
        assert len(net.dead_letter_queue) == 1

    def test_on_error_continue_after_retries(self, source_sink_graph):
        """Test that on_error=continue continues after retries exhausted."""
        net = Net(source_sink_graph, on_error="continue")
        source_completed = [False]

        def source_exec(ctx, packets):
            source_completed[0] = True
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            raise ValueError("Always fails")

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)
        net.set_node_config("Sink", retries=1, defer_net_actions=True)

        net.inject_source_epoch("Source")
        net.start()

        # Source should have completed
        assert source_completed[0]
        # Net should still be in normal state (PAUSED after completion)
        assert net.state == NetState.PAUSED
        # Failed epoch should be in DLQ
        assert len(net.dead_letter_queue) == 1


# =============================================================================
# DLQ Configuration Tests
# =============================================================================

class TestDLQConfiguration:
    """Tests for dead letter queue configuration."""

    def test_dlq_disabled_per_node(self, source_sink_graph):
        """Test that DLQ can be disabled per node."""
        net = Net(source_sink_graph, on_error="continue")

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            raise ValueError("Always fails")

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)
        net.set_node_config("Sink", dead_letter_queue=False)

        net.inject_source_epoch("Source")
        net.start()

        # DLQ should be empty since disabled for Sink
        assert len(net.dead_letter_queue) == 0

    def test_net_level_dlq_callback(self, source_sink_graph):
        """Test net-level DLQ callback configuration."""
        received = []

        def dlq_callback(entry):
            received.append(entry)

        net = Net(source_sink_graph, on_error="continue", dead_letter_queue=dlq_callback)

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            raise ValueError("Always fails")

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)

        net.inject_source_epoch("Source")
        net.start()

        # Callback should have been called
        assert len(received) == 1
        assert received[0].node_name == "Sink"

    def test_net_level_dlq_file(self, source_sink_graph):
        """Test net-level DLQ file configuration."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dlq_path = Path(tmpdir) / "dlq.json"

            net = Net(
                source_sink_graph,
                on_error="continue",
                dead_letter_queue="file",
                dead_letter_path=dlq_path,
            )

            def source_exec(ctx, packets):
                pkt = ctx.create_packet("data")
                ctx.load_output_port("out", pkt)
                ctx.send_output_salvo("send")

            def sink_exec(ctx, packets):
                raise ValueError("Always fails")

            net.set_node_exec("Source", source_exec)
            net.set_node_exec("Sink", sink_exec)

            net.inject_source_epoch("Source")
            net.start()

            # File should exist with entry
            assert dlq_path.exists()
            import json
            with open(dlq_path) as f:
                data = json.load(f)
            assert len(data) == 1
            assert data[0]["node_name"] == "Sink"


# =============================================================================
# Error Callback Tests
# =============================================================================

class TestErrorCallbackWithRetries:
    """Tests for error callback with retries."""

    def test_error_callback_called_after_retries(self, source_sink_graph):
        """Test that error_callback is called after retries exhausted."""
        errors = []

        def error_cb(exception, node_name, epoch_id):
            errors.append((type(exception).__name__, node_name))

        net = Net(source_sink_graph, on_error="continue", error_callback=error_cb)

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            raise ValueError("Always fails")

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)
        net.set_node_config("Sink", retries=2, defer_net_actions=True)

        net.inject_source_epoch("Source")
        net.start()

        # Error callback should be called once (after all retries)
        assert len(errors) == 1
        assert errors[0] == ("ValueError", "Sink")
