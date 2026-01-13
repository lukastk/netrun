"""
Tests for Milestone 5: Async Execution
"""

import pytest
import asyncio
from datetime import datetime

from netrun.core import (
    Graph, Node, Edge, Port, PortType, PortRef, PortState,
    MaxSalvos, SalvoCondition, SalvoConditionTerm,
    Net, NetState, NodeConfig, NodeExecFuncs,
    NodeExecutionContext, NodeFailureContext,
    NodeExecutionFailed, EpochCancelled,
    _is_async_func,
)


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def simple_graph():
    """Create a simple Source -> Sink graph."""
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
def pipeline_graph():
    """Create a Source -> Processor -> Sink graph."""
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


# ============================================================================
# Test _is_async_func
# ============================================================================

class TestIsAsyncFunc:
    """Tests for the _is_async_func helper."""

    def test_sync_function_returns_false(self):
        def sync_func():
            pass
        assert _is_async_func(sync_func) is False

    def test_async_function_returns_true(self):
        async def async_func():
            pass
        assert _is_async_func(async_func) is True

    def test_lambda_returns_false(self):
        assert _is_async_func(lambda: None) is False

    def test_none_returns_false(self):
        assert _is_async_func(None) is False

    def test_method_sync(self):
        class MyClass:
            def method(self):
                pass
        obj = MyClass()
        assert _is_async_func(obj.method) is False

    def test_method_async(self):
        class MyClass:
            async def method(self):
                pass
        obj = MyClass()
        assert _is_async_func(obj.method) is True


# ============================================================================
# Test Async exec_func Execution
# ============================================================================

class TestAsyncExecFunc:
    """Tests for async exec_func execution."""

    @pytest.mark.asyncio
    async def test_async_exec_func_basic(self, simple_graph):
        """Test basic async exec_func execution."""
        net = Net(simple_graph, consumed_packet_storage=True)
        execution_log = []

        async def async_source(ctx, packets):
            execution_log.append("source_start")
            await asyncio.sleep(0.01)
            pkt = ctx.create_packet({"value": "async_data"})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")
            execution_log.append("source_end")

        async def async_sink(ctx, packets):
            execution_log.append("sink_start")
            await asyncio.sleep(0.01)
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    execution_log.append(f"sink_consumed: {value}")
            execution_log.append("sink_end")

        net.set_node_exec("Source", async_source)
        net.set_node_exec("Sink", async_sink)

        net.inject_source_epoch("Source")
        await net.async_start()

        assert "source_start" in execution_log
        assert "source_end" in execution_log
        assert "sink_start" in execution_log
        assert "sink_end" in execution_log
        assert "sink_consumed: {'value': 'async_data'}" in execution_log

    @pytest.mark.asyncio
    async def test_async_exec_func_with_deferred_actions(self, simple_graph):
        """Test async exec_func with deferred actions."""
        net = Net(simple_graph, consumed_packet_storage=True)
        results = []

        async def async_source(ctx, packets):
            await asyncio.sleep(0.01)
            pkt = ctx.create_packet({"data": "deferred"})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        async def async_sink(ctx, packets):
            await asyncio.sleep(0.01)
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    results.append(value)

        net.set_node_exec("Source", async_source)
        net.set_node_exec("Sink", async_sink)
        net.set_node_config("Sink", defer_net_actions=True)

        net.inject_source_epoch("Source")
        await net.async_start()

        assert len(results) == 1
        assert results[0] == {"data": "deferred"}


# ============================================================================
# Test Mixed Sync/Async Nodes
# ============================================================================

class TestMixedSyncAsync:
    """Tests for mixed sync and async node functions."""

    @pytest.mark.asyncio
    async def test_sync_source_async_sink(self, simple_graph):
        """Test sync source with async sink."""
        net = Net(simple_graph, consumed_packet_storage=True)
        execution_log = []

        def sync_source(ctx, packets):
            execution_log.append("sync_source")
            pkt = ctx.create_packet({"origin": "sync"})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        async def async_sink(ctx, packets):
            execution_log.append("async_sink_start")
            await asyncio.sleep(0.01)
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    execution_log.append(f"received: {value['origin']}")
            execution_log.append("async_sink_end")

        net.set_node_exec("Source", sync_source)
        net.set_node_exec("Sink", async_sink)

        net.inject_source_epoch("Source")
        await net.async_start()

        assert execution_log == [
            "sync_source",
            "async_sink_start",
            "received: sync",
            "async_sink_end"
        ]

    @pytest.mark.asyncio
    async def test_async_source_sync_sink(self, simple_graph):
        """Test async source with sync sink."""
        net = Net(simple_graph, consumed_packet_storage=True)
        execution_log = []

        async def async_source(ctx, packets):
            execution_log.append("async_source_start")
            await asyncio.sleep(0.01)
            pkt = ctx.create_packet({"origin": "async"})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")
            execution_log.append("async_source_end")

        def sync_sink(ctx, packets):
            execution_log.append("sync_sink")
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    execution_log.append(f"received: {value['origin']}")

        net.set_node_exec("Source", async_source)
        net.set_node_exec("Sink", sync_sink)

        net.inject_source_epoch("Source")
        await net.async_start()

        assert execution_log == [
            "async_source_start",
            "async_source_end",
            "sync_sink",
            "received: async"
        ]

    @pytest.mark.asyncio
    async def test_pipeline_mixed_nodes(self, pipeline_graph):
        """Test pipeline with mixed sync/async nodes."""
        net = Net(pipeline_graph, consumed_packet_storage=True)
        execution_log = []
        results = []

        def sync_source(ctx, packets):
            execution_log.append("sync_source")
            pkt = ctx.create_packet({"value": 10})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        async def async_processor(ctx, packets):
            execution_log.append("async_processor_start")
            await asyncio.sleep(0.01)
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    result = {**value, "processed": True, "doubled": value["value"] * 2}
                    out_pkt = ctx.create_packet(result)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")
            execution_log.append("async_processor_end")

        def sync_sink(ctx, packets):
            execution_log.append("sync_sink")
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    results.append(value)

        net.set_node_exec("Source", sync_source)
        net.set_node_exec("Processor", async_processor)
        net.set_node_exec("Sink", sync_sink)

        net.inject_source_epoch("Source")
        await net.async_start()

        assert len(results) == 1
        assert results[0]["processed"] is True
        assert results[0]["doubled"] == 20


# ============================================================================
# Test async_run_step
# ============================================================================

class TestAsyncRunStep:
    """Tests for async_run_step method."""

    @pytest.mark.asyncio
    async def test_async_run_step_basic(self, simple_graph):
        """Test async_run_step executes pending epochs."""
        net = Net(simple_graph, consumed_packet_storage=True)
        executed = []

        async def async_source(ctx, packets):
            executed.append("source")
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        async def async_sink(ctx, packets):
            executed.append("sink")
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Source", async_source)
        net.set_node_exec("Sink", async_sink)

        net.inject_source_epoch("Source")

        # First step: execute source
        await net.async_run_step()
        assert "source" in executed
        assert "sink" not in executed

        # Second step: execute sink
        await net.async_run_step()
        assert "sink" in executed

    @pytest.mark.asyncio
    async def test_async_run_step_start_epochs_false(self, simple_graph):
        """Test async_run_step with start_epochs=False."""
        net = Net(simple_graph, consumed_packet_storage=True)
        executed = []

        async def async_source(ctx, packets):
            executed.append("source")

        net.set_node_exec("Source", async_source)
        net.inject_source_epoch("Source")

        await net.async_run_step(start_epochs=False)
        assert executed == []  # Nothing executed when start_epochs=False

    @pytest.mark.asyncio
    async def test_async_run_step_on_stopped_raises(self, simple_graph):
        """Test async_run_step raises on stopped net."""
        net = Net(simple_graph)
        net.stop()
        with pytest.raises(RuntimeError, match="Cannot run_step on a stopped net"):
            await net.async_run_step()


# ============================================================================
# Test async_start
# ============================================================================

class TestAsyncStart:
    """Tests for async_start method."""

    @pytest.mark.asyncio
    async def test_async_start_runs_until_blocked(self, simple_graph):
        """Test async_start runs network until fully blocked."""
        net = Net(simple_graph, consumed_packet_storage=True)
        results = []

        async def async_source(ctx, packets):
            for i in range(3):
                pkt = ctx.create_packet({"index": i})
                ctx.load_output_port("out", pkt)
                ctx.send_output_salvo("send")

        async def async_sink(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    results.append(value)

        net.set_node_exec("Source", async_source)
        net.set_node_exec("Sink", async_sink)

        net.inject_source_epoch("Source")
        await net.async_start()

        assert len(results) == 3
        # Ordering may vary, just check we have all indices
        assert sorted([r["index"] for r in results]) == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_async_start_calls_start_stop_funcs(self, simple_graph):
        """Test async_start calls start/stop functions."""
        net = Net(simple_graph, consumed_packet_storage=True)
        lifecycle = []

        async def async_start(net):
            lifecycle.append("start_async")

        async def async_stop(net):
            lifecycle.append("stop_async")

        def sync_start(net):
            lifecycle.append("start_sync")

        def sync_stop(net):
            lifecycle.append("stop_sync")

        async def source_exec(ctx, packets):
            pass

        # Register async start/stop for Source
        net.set_node_exec("Source", source_exec, start_func=async_start, stop_func=async_stop)
        # Register sync start/stop for Sink
        net.set_node_exec("Sink", lambda ctx, pkts: None, start_func=sync_start, stop_func=sync_stop)

        net.inject_source_epoch("Source")
        await net.async_start()

        assert "start_async" in lifecycle
        assert "start_sync" in lifecycle
        assert "stop_async" in lifecycle
        assert "stop_sync" in lifecycle
        # Start functions should be called before stop functions
        start_indices = [lifecycle.index("start_async"), lifecycle.index("start_sync")]
        stop_indices = [lifecycle.index("stop_async"), lifecycle.index("stop_sync")]
        assert all(s < e for s in start_indices for e in stop_indices)


# ============================================================================
# Test Async Error Handling
# ============================================================================

class TestAsyncErrorHandling:
    """Tests for async error handling."""

    @pytest.mark.asyncio
    async def test_async_on_error_raise(self, simple_graph):
        """Test async exec_func with on_error=raise."""
        net = Net(simple_graph, on_error="raise")

        async def async_source(ctx, packets):
            raise ValueError("async error")

        net.set_node_exec("Source", async_source)
        net.inject_source_epoch("Source")

        with pytest.raises(NodeExecutionFailed) as exc_info:
            await net.async_start()

        assert "async error" in str(exc_info.value.original_exception)

    @pytest.mark.asyncio
    async def test_async_on_error_continue(self, simple_graph):
        """Test async exec_func with on_error=continue."""
        net = Net(simple_graph, on_error="continue", dead_letter_queue="memory")

        async def async_source(ctx, packets):
            raise ValueError("async continue error")

        net.set_node_exec("Source", async_source)
        net.inject_source_epoch("Source")

        await net.async_start()  # Should not raise

        # Check DLQ
        dlq_entries = net.dead_letter_queue.get_all()
        assert len(dlq_entries) == 1
        assert "async continue error" in str(dlq_entries[0].exception)

    @pytest.mark.asyncio
    async def test_async_retry_success(self, simple_graph):
        """Test async exec_func with retries succeeding on second attempt."""
        net = Net(simple_graph, consumed_packet_storage=True)
        attempt_count = [0]

        async def async_source(ctx, packets):
            attempt_count[0] += 1
            if attempt_count[0] < 2:
                raise ValueError("first attempt fails")
            pkt = ctx.create_packet("success")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        async def async_sink(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Source", async_source)
        net.set_node_exec("Sink", async_sink)
        net.set_node_config("Source", retries=2, defer_net_actions=True, retry_wait=0.01)

        net.inject_source_epoch("Source")
        await net.async_start()

        assert attempt_count[0] == 2  # First failed, second succeeded

    @pytest.mark.asyncio
    async def test_async_failed_func_called(self, simple_graph):
        """Test async failed_func is called on failure."""
        net = Net(simple_graph, on_error="continue")
        failure_log = []

        async def async_source(ctx, packets):
            raise ValueError("always fails")

        async def async_failed(failure_ctx):
            failure_log.append({
                "retry_count": failure_ctx.retry_count,
                "exception": str(failure_ctx.exception),
            })

        net.set_node_exec("Source", async_source, failed_func=async_failed)
        net.set_node_config("Source", retries=1, defer_net_actions=True, retry_wait=0.01)

        net.inject_source_epoch("Source")
        await net.async_start()

        # Should have 2 failure logs (initial + 1 retry)
        assert len(failure_log) == 2
        assert failure_log[0]["retry_count"] == 0
        assert failure_log[1]["retry_count"] == 1


# ============================================================================
# Test Async Value Functions
# ============================================================================

class TestAsyncValueFunctions:
    """Tests for async value functions."""

    @pytest.mark.asyncio
    async def test_async_value_func_consumed(self, simple_graph):
        """Test async value function is properly awaited."""
        net = Net(simple_graph, consumed_packet_storage=True)
        results = []

        async def async_source(ctx, packets):
            async def async_value_func():
                await asyncio.sleep(0.01)
                return {"async_computed": True}

            pkt = ctx.create_packet_from_value_func(async_value_func)
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        async def async_sink(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    # Use async_consume_packet for async value functions
                    value = await ctx.async_consume_packet(pkt)
                    results.append(value)

        net.set_node_exec("Source", async_source)
        net.set_node_exec("Sink", async_sink)

        net.inject_source_epoch("Source")
        await net.async_start()

        assert len(results) == 1
        assert results[0]["async_computed"] is True


# ============================================================================
# Test async_pause and async_stop
# ============================================================================

class TestAsyncPauseStop:
    """Tests for async_pause and async_stop methods."""

    @pytest.mark.asyncio
    async def test_async_pause(self, simple_graph):
        """Test async_pause sets state to PAUSED."""
        net = Net(simple_graph)
        await net.async_pause()
        assert net.state == NetState.PAUSED

    @pytest.mark.asyncio
    async def test_async_stop(self, simple_graph):
        """Test async_stop sets state to STOPPED."""
        net = Net(simple_graph)
        await net.async_stop()
        assert net.state == NetState.STOPPED


# ============================================================================
# Test async_wait_until_blocked
# ============================================================================

class TestAsyncWaitUntilBlocked:
    """Tests for async_wait_until_blocked method."""

    @pytest.mark.asyncio
    async def test_async_wait_returns_when_not_running(self, simple_graph):
        """Test async_wait_until_blocked returns immediately when not running."""
        net = Net(simple_graph)
        # Should return immediately since net is not running
        await net.async_wait_until_blocked()
        assert net.state == NetState.CREATED


# ============================================================================
# Test Async Epoch Cancellation
# ============================================================================

class TestAsyncEpochCancellation:
    """Tests for async epoch cancellation."""

    @pytest.mark.asyncio
    async def test_async_cancel_epoch(self, simple_graph):
        """Test cancelling epoch in async exec_func."""
        net = Net(simple_graph, on_error="continue")
        cancelled = [False]

        async def async_source(ctx, packets):
            cancelled[0] = True
            ctx.cancel_epoch()

        net.set_node_exec("Source", async_source)
        net.inject_source_epoch("Source")

        await net.async_start()

        assert cancelled[0] is True
        # No DLQ entry for cancelled epochs
        assert len(net.dead_letter_queue.get_all()) == 0
