"""Tests for Milestone 3: Single-Threaded Execution."""

import pytest
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
    NetAction,
    Salvo,
    PacketLocation,
    # Net
    Net,
    NetState,
    # Contexts
    NodeExecutionContext,
    # Errors
    NodeExecutionFailed,
    EpochCancelled,
    EpochTimeout,
)


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def source_only_graph():
    """Create a graph with just a Source node (no inputs)."""
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
    return Graph([source_node], [])


@pytest.fixture
def linear_graph():
    """Create a simple Source -> Processor -> Sink graph."""
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

    processor_node = Node(
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
            PortRef("Processor", PortType.Input, "in")
        ),
        Edge(
            PortRef("Processor", PortType.Output, "out"),
            PortRef("Sink", PortType.Input, "in")
        ),
    ]

    return Graph([source_node, processor_node, sink_node], edges)


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


def create_epoch_for_source(net, node_name="Source"):
    """Helper to create and start an epoch for a source node (no inputs)."""
    return net.inject_source_epoch(node_name)


def put_packet_on_edge(net, edge, value):
    """Helper to create a packet and place it on an edge."""
    action = NetAction.create_packet(None)
    events = net._sim.do_action(action)
    packet_id = None
    for e in events[1]:
        if hasattr(e, 'packet_id'):
            packet_id = e.packet_id
            break

    net._value_store.store_value(packet_id, value)

    action = NetAction.transport_packet_to_location(
        packet_id,
        PacketLocation.edge(edge)
    )
    net._sim.do_action(action)
    return packet_id


# =============================================================================
# Basic Execution Tests
# =============================================================================

class TestRunStep:
    """Tests for run_step() method."""

    def test_run_step_on_empty_net(self, source_only_graph):
        """Test run_step on a net with no packets."""
        net = Net(source_only_graph)

        def source_exec(ctx, packets):
            pass

        net.set_node_exec("Source", source_exec)
        net.run_step()

        # Should complete without error
        assert net.state == NetState.RUNNING

    def test_run_step_with_start_epochs_false(self, source_sink_graph):
        """Test run_step with start_epochs=False doesn't execute epochs."""
        net = Net(source_sink_graph)
        executed = []

        def source_exec(ctx, packets):
            executed.append("source")

        def sink_exec(ctx, packets):
            executed.append("sink")

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)

        # Put a packet on the edge
        edge = list(source_sink_graph.edges())[0]
        put_packet_on_edge(net, edge, "test")

        # run_step with start_epochs=False shouldn't execute
        net.run_step(start_epochs=False)

        assert len(executed) == 0
        # But epoch should be created (startable)
        startable = net.get_startable_epochs()
        assert len(startable) == 1

    def test_run_step_executes_startable_epoch(self, source_sink_graph):
        """Test that run_step executes startable epochs."""
        net = Net(source_sink_graph, consumed_packet_storage=True)
        executed = []
        received_values = []

        def source_exec(ctx, packets):
            executed.append("source")

        def sink_exec(ctx, packets):
            executed.append("sink")
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    received_values.append(value)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)

        # Put a packet on the edge
        edge = list(source_sink_graph.edges())[0]
        put_packet_on_edge(net, edge, "test_data")

        # Run step - should execute the Sink epoch
        net.run_step()

        assert "sink" in executed
        assert received_values == ["test_data"]

    def test_run_step_on_stopped_net_raises(self, source_only_graph):
        """Test that run_step on stopped net raises error."""
        net = Net(source_only_graph)
        net.stop()

        with pytest.raises(RuntimeError, match="stopped"):
            net.run_step()

    def test_run_step_on_paused_net_does_nothing(self, source_only_graph):
        """Test that run_step on paused net does nothing."""
        net = Net(source_only_graph)
        executed = []

        def source_exec(ctx, packets):
            executed.append(1)

        net.set_node_exec("Source", source_exec)
        net.pause()

        net.run_step()

        assert len(executed) == 0


# =============================================================================
# Start/Stop Function Tests
# =============================================================================

class TestStartStopFunctions:
    """Tests for start_node_func and stop_node_func."""

    def test_start_calls_start_funcs(self, source_only_graph):
        """Test that start() calls start_node_func for all nodes."""
        net = Net(source_only_graph)
        start_called = []
        stop_called = []

        def source_exec(ctx, packets):
            pass

        def source_start(n):
            start_called.append("Source")

        def source_stop(n):
            stop_called.append("Source")

        net.set_node_exec("Source", source_exec, start_func=source_start, stop_func=source_stop)

        net.start()

        assert "Source" in start_called
        assert "Source" in stop_called

    def test_start_funcs_called_before_execution(self, source_sink_graph):
        """Test that start functions are called before any execution."""
        net = Net(source_sink_graph)
        events = []

        def source_exec(ctx, packets):
            events.append("source_exec")
            # Create and send a packet
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def source_start(n):
            events.append("source_start")

        def source_stop(n):
            events.append("source_stop")

        def sink_exec(ctx, packets):
            events.append("sink_exec")
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Source", source_exec, start_func=source_start, stop_func=source_stop)
        net.set_node_exec("Sink", sink_exec)

        # Manually create a source epoch
        create_epoch_for_source(net)

        net.start()

        # start should be called first
        assert events[0] == "source_start"
        # stop should be called last
        assert events[-1] == "source_stop"


# =============================================================================
# Linear Flow Execution Tests
# =============================================================================

class TestLinearFlowExecution:
    """Tests for executing a linear pipeline."""

    def test_simple_pipeline_execution(self, linear_graph):
        """Test executing a simple Source -> Processor -> Sink pipeline."""
        net = Net(linear_graph, consumed_packet_storage=True)
        execution_order = []
        received_data = []

        def source_exec(ctx, packets):
            execution_order.append("Source")
            # Create a packet and send it
            pkt = ctx.create_packet({"message": "hello"})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def processor_exec(ctx, packets):
            execution_order.append("Processor")
            # Consume input, transform, and send
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    # Transform the data
                    new_value = {"message": value["message"].upper()}
                    new_pkt = ctx.create_packet(new_value)
                    ctx.load_output_port("out", new_pkt)
                    ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            execution_order.append("Sink")
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    received_data.append(value)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Processor", processor_exec)
        net.set_node_exec("Sink", sink_exec)

        # Manually create a Source epoch to kick things off
        create_epoch_for_source(net)

        net.start()

        # Should have executed in order
        assert execution_order == ["Source", "Processor", "Sink"]
        assert received_data == [{"message": "HELLO"}]

    def test_source_sink_pipeline(self, source_sink_graph):
        """Test simple Source -> Sink pipeline."""
        net = Net(source_sink_graph, consumed_packet_storage=True)
        received = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("hello")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    received.append(value)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)

        # Create source epoch
        create_epoch_for_source(net)

        net.start()

        assert received == ["hello"]


# =============================================================================
# Error Handling Tests
# =============================================================================

class TestErrorHandling:
    """Tests for error handling during execution."""

    def test_on_error_raise(self, source_sink_graph):
        """Test that on_error='raise' raises the exception."""
        net = Net(source_sink_graph, on_error="raise")

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            raise ValueError("Test error")

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)

        create_epoch_for_source(net)

        with pytest.raises(NodeExecutionFailed) as exc_info:
            net.start()

        assert exc_info.value.node_name == "Sink"
        assert isinstance(exc_info.value.original_exception, ValueError)

    def test_on_error_pause(self, source_sink_graph):
        """Test that on_error='pause' pauses the net."""
        net = Net(source_sink_graph, on_error="pause")

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            raise ValueError("Test error")

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)

        create_epoch_for_source(net)

        net.start()

        assert net.state == NetState.PAUSED

    def test_on_error_continue(self, source_sink_graph):
        """Test that on_error='continue' continues execution."""
        net = Net(source_sink_graph, on_error="continue")
        error_count = [0]

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            error_count[0] += 1
            raise ValueError("Test error")

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)

        create_epoch_for_source(net)

        net.start()

        # Should have attempted execution
        assert error_count[0] == 1
        # Net should be paused after all execution completes
        assert net.state == NetState.PAUSED

    def test_failed_func_called_on_error(self, source_sink_graph):
        """Test that failed_func is called when execution fails."""
        net = Net(source_sink_graph, on_error="continue")
        failed_called = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            raise ValueError("Test error")

        def sink_failed(ctx):
            failed_called.append({
                "node": ctx.node_name,
                "exception_type": type(ctx.exception).__name__,
            })

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec, failed_func=sink_failed)

        create_epoch_for_source(net)

        net.start()

        assert len(failed_called) == 1
        assert failed_called[0]["node"] == "Sink"
        assert failed_called[0]["exception_type"] == "ValueError"

    def test_error_callback_called(self, source_sink_graph):
        """Test that error_callback is called on error."""
        errors = []

        def error_cb(exception, node_name, epoch_id):
            errors.append((type(exception).__name__, node_name))

        net = Net(source_sink_graph, on_error="continue", error_callback=error_cb)

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            raise ValueError("Test error")

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)

        create_epoch_for_source(net)

        net.start()

        assert len(errors) == 1
        assert errors[0] == ("ValueError", "Sink")


# =============================================================================
# Epoch Cancellation Tests
# =============================================================================

class TestEpochCancellation:
    """Tests for epoch cancellation via ctx.cancel_epoch()."""

    def test_cancel_epoch_stops_execution(self, source_sink_graph):
        """Test that cancel_epoch() stops the epoch."""
        net = Net(source_sink_graph, on_error="continue")
        after_cancel = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            ctx.cancel_epoch()
            after_cancel.append("should not reach here")

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)

        create_epoch_for_source(net)

        net.start()

        assert len(after_cancel) == 0


# =============================================================================
# Nodes Without exec_func Tests
# =============================================================================

class TestNodesWithoutExecFunc:
    """Tests for nodes that don't have exec_func defined."""

    def test_node_without_exec_func_stays_startable(self, source_sink_graph):
        """Test that epochs for nodes without exec_func stay startable."""
        net = Net(source_sink_graph)

        # Only set exec_func for Source, not Sink
        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        net.set_node_exec("Source", source_exec)
        # Don't set exec_func for Sink

        # Create source epoch
        create_epoch_for_source(net)

        # Start - should execute Source but not Sink
        net.start()

        # Should have a startable epoch for Sink
        startable = net.get_startable_epochs()
        assert len(startable) == 1

        epoch = net.get_epoch(startable[0])
        assert epoch.node_name == "Sink"


# =============================================================================
# Deferred Actions Tests
# =============================================================================

class TestDeferredActionsInExecution:
    """Tests for deferred actions during execution."""

    def test_deferred_actions_committed_on_success(self, linear_graph):
        """Test that deferred actions are committed when execution succeeds."""
        net = Net(linear_graph, consumed_packet_storage=True)
        received_data = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("from_source")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def processor_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    new_pkt = ctx.create_packet(f"processed: {value}")
                    ctx.load_output_port("out", new_pkt)
                    ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    received_data.append(value)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Processor", processor_exec)
        net.set_node_config("Processor", defer_net_actions=True)
        net.set_node_exec("Sink", sink_exec)

        # Start the pipeline
        create_epoch_for_source(net)

        net.start()

        assert received_data == ["processed: from_source"]
