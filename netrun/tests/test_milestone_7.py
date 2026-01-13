"""
Milestone 7 Tests: Rate Limiting and Parallel Epoch Control

This milestone adds:
- max_parallel_epochs: Limit concurrent epochs per node
- rate_limit_per_second: Limit epoch starts per second per node
"""

import asyncio
import time
import pytest

from netrun import (
    Graph, Node, Edge, Port, PortType, PortRef, PortState,
    MaxSalvos, SalvoCondition, SalvoConditionTerm,
    Net, NetState, NodeConfig,
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
# NodeConfig Tests
# ==============================================================================

class TestNodeConfigLimits:
    """Test NodeConfig rate limit and parallel epoch fields."""

    def test_default_config_has_no_limits(self):
        """Default config should have no limits."""
        config = NodeConfig()
        assert config.max_parallel_epochs is None
        assert config.rate_limit_per_second is None

    def test_set_max_parallel_epochs(self):
        """Can set max_parallel_epochs."""
        config = NodeConfig(max_parallel_epochs=2)
        assert config.max_parallel_epochs == 2

    def test_set_rate_limit(self):
        """Can set rate_limit_per_second."""
        config = NodeConfig(rate_limit_per_second=10.0)
        assert config.rate_limit_per_second == 10.0

    def test_set_both_limits(self):
        """Can set both limits together."""
        config = NodeConfig(max_parallel_epochs=3, rate_limit_per_second=5.0)
        assert config.max_parallel_epochs == 3
        assert config.rate_limit_per_second == 5.0


class TestNetConfigLimits:
    """Test setting limits via Net.set_node_config."""

    def test_set_node_max_parallel_epochs(self, simple_graph):
        """Can set max_parallel_epochs on a node."""
        net = Net(simple_graph)
        net.set_node_config("Source", max_parallel_epochs=2)
        config = net.get_node_config("Source")
        assert config.max_parallel_epochs == 2

    def test_set_node_rate_limit(self, simple_graph):
        """Can set rate_limit_per_second on a node."""
        net = Net(simple_graph)
        net.set_node_config("Source", rate_limit_per_second=5.0)
        config = net.get_node_config("Source")
        assert config.rate_limit_per_second == 5.0


# ==============================================================================
# Max Parallel Epochs Tests
# ==============================================================================

class TestMaxParallelEpochs:
    """Test max_parallel_epochs enforcement."""

    def test_get_running_epochs_count_initially_zero(self, simple_graph):
        """Running epochs count should be 0 initially."""
        net = Net(simple_graph, on_error="continue")
        assert net.get_running_epochs_count("Source") == 0

    def test_running_epochs_count_increases_during_execution(self, simple_graph):
        """Running epochs count increases when epoch is executing."""
        net = Net(simple_graph, on_error="continue")

        running_count_during = None

        def source_exec(ctx, packets):
            nonlocal running_count_during
            running_count_during = net.get_running_epochs_count("Source")
            # Create and send a packet
            pkt = ctx.create_packet({"value": 1})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        net.set_node_exec("Source", source_exec)
        net.inject_source_epoch("Source")
        net.run_step()

        # During execution, count should have been 1
        assert running_count_during == 1
        # After execution, count should be back to 0
        assert net.get_running_epochs_count("Source") == 0

    def test_max_parallel_epochs_limits_execution(self, pipeline_graph):
        """max_parallel_epochs should limit concurrent executions."""
        net = Net(pipeline_graph, on_error="continue")

        # Track execution order and overlaps
        execution_log = []
        active_executions = {"Processor": 0}
        max_concurrent = {"Processor": 0}

        def source_exec(ctx, packets):
            # Create 5 packets
            for i in range(5):
                pkt = ctx.create_packet({"id": i})
                ctx.load_output_port("out", pkt)
                ctx.send_output_salvo("send")

        def processor_exec(ctx, packets):
            active_executions["Processor"] += 1
            max_concurrent["Processor"] = max(
                max_concurrent["Processor"],
                active_executions["Processor"]
            )

            for port_name, pkts in packets.items():
                for pkt in pkts:
                    val = ctx.consume_packet(pkt)
                    execution_log.append(val["id"])
                    out_pkt = ctx.create_packet({"id": val["id"], "processed": True})
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")

            active_executions["Processor"] -= 1

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Processor", processor_exec)
        net.set_node_exec("Sink", sink_exec)

        # Set max parallel epochs to 1 for Processor
        net.set_node_config("Processor", max_parallel_epochs=1)

        net.inject_source_epoch("Source")
        net.start()

        # Max concurrent should be 1 due to limit
        assert max_concurrent["Processor"] == 1
        # All 5 packets should be processed
        assert len(execution_log) == 5

    def test_can_start_epoch_respects_max_parallel(self, simple_graph):
        """_can_start_epoch should return False when limit reached."""
        net = Net(simple_graph, on_error="continue")
        net.set_node_config("Source", max_parallel_epochs=1)

        # Initially should be able to start
        can_start, wait_time = net._can_start_epoch("Source")
        assert can_start is True
        assert wait_time is None

        # Manually record an epoch as running
        net._running_epochs_by_node["Source"] = {"epoch1"}

        # Now should not be able to start
        can_start, wait_time = net._can_start_epoch("Source")
        assert can_start is False
        assert wait_time is None  # No wait time for max_parallel


# ==============================================================================
# Rate Limiting Tests
# ==============================================================================

class TestRateLimiting:
    """Test rate_limit_per_second enforcement."""

    def test_rate_limit_allows_first_execution(self, simple_graph):
        """First execution should always be allowed."""
        net = Net(simple_graph, on_error="continue")
        net.set_node_config("Source", rate_limit_per_second=1.0)

        can_start, wait_time = net._can_start_epoch("Source")
        assert can_start is True
        assert wait_time is None

    def test_rate_limit_returns_wait_time(self, simple_graph):
        """After execution, should return wait time until next allowed."""
        net = Net(simple_graph, on_error="continue")
        net.set_node_config("Source", rate_limit_per_second=2.0)  # 2 per second = 0.5s interval

        # Record an epoch start
        net._record_epoch_start("Source", "epoch1")

        # Immediately check again
        can_start, wait_time = net._can_start_epoch("Source")
        assert can_start is False
        assert wait_time is not None
        # Wait time should be approximately 0.5 seconds (rate limit window)
        assert 0 < wait_time <= 0.5

    def test_rate_limit_allows_after_window(self, simple_graph):
        """After waiting the full window, execution should be allowed."""
        net = Net(simple_graph, on_error="continue")
        net.set_node_config("Source", rate_limit_per_second=10.0)  # 10 per second = 0.1s interval

        # Record an epoch start
        net._record_epoch_start("Source", "epoch1")

        # Wait for the rate limit window
        time.sleep(0.15)  # A bit more than 0.1s

        # Should be able to start now
        can_start, wait_time = net._can_start_epoch("Source")
        assert can_start is True

    def test_rate_limit_cleans_old_timestamps(self, simple_graph):
        """Old timestamps (> 1 second) should be cleaned up."""
        net = Net(simple_graph, on_error="continue")
        net.set_node_config("Source", rate_limit_per_second=10.0)

        # Add an old timestamp
        old_time = time.time() - 2.0  # 2 seconds ago
        net._epoch_start_times_by_node["Source"] = [old_time]

        # Check can_start - should clean up old timestamp
        can_start, wait_time = net._can_start_epoch("Source")

        assert can_start is True
        # Old timestamp should be removed
        assert len(net._epoch_start_times_by_node.get("Source", [])) == 0

    def test_rate_limit_enforced_in_run_step(self, pipeline_graph):
        """Rate limiting should be enforced during run_step."""
        net = Net(pipeline_graph, on_error="continue")

        start_times = []

        def source_exec(ctx, packets):
            for i in range(3):
                pkt = ctx.create_packet({"id": i})
                ctx.load_output_port("out", pkt)
                ctx.send_output_salvo("send")

        def processor_exec(ctx, packets):
            start_times.append(time.time())
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    val = ctx.consume_packet(pkt)
                    out_pkt = ctx.create_packet(val)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Processor", processor_exec)
        net.set_node_exec("Sink", sink_exec)

        # Limit processor to 10 per second (0.1s between executions)
        net.set_node_config("Processor", rate_limit_per_second=10.0)

        net.inject_source_epoch("Source")
        net.start()

        # Should have 3 processor executions
        assert len(start_times) == 3

        # Check minimum intervals between executions
        for i in range(1, len(start_times)):
            interval = start_times[i] - start_times[i-1]
            # Should be at least 0.09s (allowing small timing variance)
            assert interval >= 0.09, f"Interval {interval} too short"


# ==============================================================================
# Async Rate Limiting Tests
# ==============================================================================

class TestAsyncRateLimiting:
    """Test rate limiting in async execution."""

    @pytest.mark.asyncio
    async def test_async_rate_limit_waits(self, pipeline_graph):
        """Async execution should also respect rate limits."""
        net = Net(pipeline_graph, on_error="continue")

        start_times = []

        async def async_source_exec(ctx, packets):
            for i in range(3):
                pkt = ctx.create_packet({"id": i})
                ctx.load_output_port("out", pkt)
                ctx.send_output_salvo("send")

        async def async_processor_exec(ctx, packets):
            start_times.append(time.time())
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    val = ctx.consume_packet(pkt)
                    out_pkt = ctx.create_packet(val)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")

        async def async_sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Source", async_source_exec)
        net.set_node_exec("Processor", async_processor_exec)
        net.set_node_exec("Sink", async_sink_exec)

        # Limit processor to 10 per second
        net.set_node_config("Processor", rate_limit_per_second=10.0)

        net.inject_source_epoch("Source")
        await net.async_start()

        # Should have 3 processor executions
        assert len(start_times) == 3

        # Check intervals
        for i in range(1, len(start_times)):
            interval = start_times[i] - start_times[i-1]
            assert interval >= 0.09, f"Interval {interval} too short"


# ==============================================================================
# Combined Limits Tests
# ==============================================================================

class TestCombinedLimits:
    """Test max_parallel_epochs and rate_limit together."""

    def test_both_limits_applied(self, pipeline_graph):
        """Both limits should be applied simultaneously."""
        net = Net(pipeline_graph, on_error="continue")

        execution_count = [0]

        def source_exec(ctx, packets):
            for i in range(5):
                pkt = ctx.create_packet({"id": i})
                ctx.load_output_port("out", pkt)
                ctx.send_output_salvo("send")

        def processor_exec(ctx, packets):
            execution_count[0] += 1
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    val = ctx.consume_packet(pkt)
                    out_pkt = ctx.create_packet(val)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Processor", processor_exec)
        net.set_node_exec("Sink", sink_exec)

        # Set both limits
        net.set_node_config("Processor", max_parallel_epochs=1, rate_limit_per_second=20.0)

        net.inject_source_epoch("Source")
        net.start()

        # All 5 should be processed
        assert execution_count[0] == 5


# ==============================================================================
# Epoch Tracking Tests
# ==============================================================================

class TestEpochTracking:
    """Test epoch start/end tracking."""

    def test_record_epoch_start(self, simple_graph):
        """_record_epoch_start should track the epoch."""
        net = Net(simple_graph, on_error="continue")

        assert "Source" not in net._running_epochs_by_node

        net._record_epoch_start("Source", "epoch1")

        assert "epoch1" in net._running_epochs_by_node["Source"]
        assert len(net._epoch_start_times_by_node["Source"]) == 1

    def test_record_epoch_end(self, simple_graph):
        """_record_epoch_end should untrack the epoch."""
        net = Net(simple_graph, on_error="continue")

        net._record_epoch_start("Source", "epoch1")
        assert "epoch1" in net._running_epochs_by_node["Source"]

        net._record_epoch_end("Source", "epoch1")
        assert "epoch1" not in net._running_epochs_by_node["Source"]

    def test_epoch_end_called_on_success(self, simple_graph):
        """_record_epoch_end should be called even on success."""
        net = Net(simple_graph, on_error="continue")

        def source_exec(ctx, packets):
            pkt = ctx.create_packet({"value": 1})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        net.set_node_exec("Source", source_exec)
        net.inject_source_epoch("Source")
        net.run_step()

        # After execution, running epochs should be empty
        assert net.get_running_epochs_count("Source") == 0

    def test_epoch_end_called_on_failure(self, simple_graph):
        """_record_epoch_end should be called even on failure."""
        net = Net(simple_graph, on_error="continue")

        def failing_exec(ctx, packets):
            raise ValueError("Test error")

        net.set_node_exec("Source", failing_exec)
        net.inject_source_epoch("Source")
        net.run_step()

        # After execution (even failed), running epochs should be empty
        assert net.get_running_epochs_count("Source") == 0

    def test_epoch_end_called_on_cancel(self, simple_graph):
        """_record_epoch_end should be called on epoch cancellation."""
        from netrun.errors import EpochCancelled

        net = Net(simple_graph, on_error="continue")

        def cancel_exec(ctx, packets):
            ctx.cancel()

        net.set_node_exec("Source", cancel_exec)
        net.inject_source_epoch("Source")

        try:
            net.run_step()
        except EpochCancelled:
            pass

        # After cancellation, running epochs should be empty
        assert net.get_running_epochs_count("Source") == 0


# ==============================================================================
# Edge Cases
# ==============================================================================

class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_zero_rate_limit_ignored(self, simple_graph):
        """Rate limit of 0 should be ignored (no limit)."""
        net = Net(simple_graph, on_error="continue")
        net.set_node_config("Source", rate_limit_per_second=0.0)

        net._record_epoch_start("Source", "epoch1")

        can_start, wait_time = net._can_start_epoch("Source")
        # Should be able to start (0 rate limit = no limit)
        assert can_start is True

    def test_negative_rate_limit_ignored(self, simple_graph):
        """Negative rate limit should be ignored."""
        net = Net(simple_graph, on_error="continue")
        net.set_node_config("Source", rate_limit_per_second=-1.0)

        net._record_epoch_start("Source", "epoch1")

        can_start, wait_time = net._can_start_epoch("Source")
        # Negative rate limit doesn't enforce limiting
        assert can_start is True

    def test_no_limit_allows_all(self, simple_graph):
        """With no limits set, all executions should be allowed."""
        net = Net(simple_graph, on_error="continue")

        # No limits set (default config)
        for i in range(10):
            net._record_epoch_start("Source", f"epoch{i}")

        can_start, wait_time = net._can_start_epoch("Source")
        assert can_start is True
        assert wait_time is None

    def test_multiple_nodes_independent_limits(self, pipeline_graph):
        """Each node should have independent limits."""
        net = Net(pipeline_graph, on_error="continue")

        net.set_node_config("Source", max_parallel_epochs=1)
        net.set_node_config("Processor", max_parallel_epochs=2)
        net.set_node_config("Sink", max_parallel_epochs=3)

        # Record epochs for each node
        net._record_epoch_start("Source", "s1")
        net._record_epoch_start("Processor", "p1")
        net._record_epoch_start("Sink", "sn1")

        # Source at limit
        can_start, _ = net._can_start_epoch("Source")
        assert can_start is False

        # Processor below limit
        can_start, _ = net._can_start_epoch("Processor")
        assert can_start is True

        # Sink below limit
        can_start, _ = net._can_start_epoch("Sink")
        assert can_start is True
