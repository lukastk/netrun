"""Tests for Milestone 2: Node Execution Contexts."""

import pytest
from datetime import datetime, timedelta

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
    # Net
    Net,
    NetState,
    # Contexts
    NodeExecutionContext,
    NodeFailureContext,
    # Deferred
    DeferredPacket,
    DeferredActionQueue,
    DeferredAction,
    DeferredActionType,
    # Errors
    DeferredPacketIdAccessError,
    EpochCancelled,
)
# Internal functions for testing
from netrun.net import _commit_deferred_actions, _unconsume_packets_for_retry


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def simple_graph():
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
        )
    ]

    return Graph([source_node, sink_node], edges)


@pytest.fixture
def processor_graph():
    """Create a Source -> Processor -> Sink graph."""
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
def net_with_running_epoch(simple_graph):
    """Create a Net with a running epoch for testing context operations."""
    net = Net(simple_graph, consumed_packet_storage=True)

    # Manually create and start an epoch for testing
    # First, create a packet outside the net
    action = NetAction.create_packet(None)  # None = outside the net
    events = net._sim.do_action(action)
    packet_id = None
    for event in events:
        if hasattr(event, 'packet_id'):
            packet_id = event.packet_id
            break

    # Store a value for the packet
    net._value_store.store_value(packet_id, {"test": "data"})

    # Transport packet to Source's output port
    # First we need to create an epoch for Source
    # Create and start an epoch manually
    from netrun import Salvo
    salvo = Salvo({})  # Empty input salvo for Source (it has no input ports)
    action = NetAction.create_and_start_epoch("Source", salvo)
    events = net._sim.do_action(action)

    # Get the epoch ID
    epoch_id = None
    for event in events:
        if hasattr(event, 'epoch_id'):
            epoch_id = event.epoch_id
            break

    return net, epoch_id, packet_id


# =============================================================================
# DeferredPacket Tests
# =============================================================================

class TestDeferredPacket:
    """Tests for the DeferredPacket class."""

    def test_deferred_packet_creation(self):
        """Test creating a deferred packet."""
        dp = DeferredPacket("deferred-0")
        assert dp.deferred_id == "deferred-0"
        assert dp.is_resolved is False

    def test_deferred_packet_id_raises_before_resolve(self):
        """Test that accessing id raises DeferredPacketIdAccessError."""
        dp = DeferredPacket("deferred-0")
        with pytest.raises(DeferredPacketIdAccessError):
            _ = dp.id

    def test_deferred_packet_location_raises_before_resolve(self):
        """Test that accessing location raises DeferredPacketIdAccessError."""
        dp = DeferredPacket("deferred-0")
        with pytest.raises(DeferredPacketIdAccessError):
            _ = dp.location

    def test_deferred_packet_repr_unresolved(self):
        """Test repr of unresolved deferred packet."""
        dp = DeferredPacket("deferred-123")
        assert "deferred-123" in repr(dp)
        assert "resolved" not in repr(dp).lower()

    def test_deferred_packet_deferred_id_always_available(self):
        """Test that deferred_id is always available."""
        dp = DeferredPacket("test-id")
        assert dp.deferred_id == "test-id"


# =============================================================================
# DeferredActionQueue Tests
# =============================================================================

class TestDeferredActionQueue:
    """Tests for the DeferredActionQueue class."""

    def test_queue_creation(self):
        """Test creating an empty queue."""
        queue = DeferredActionQueue()
        assert len(queue.actions) == 0
        assert len(queue.consumed_values) == 0

    def test_create_packet_queues_action(self):
        """Test that create_packet adds an action to the queue."""
        queue = DeferredActionQueue()
        dp = queue.create_packet({"value": 42})

        assert len(queue.actions) == 1
        assert queue.actions[0].action_type == DeferredActionType.CREATE_PACKET
        assert queue.actions[0].value == {"value": 42}
        assert queue.actions[0].deferred_packet is dp

    def test_create_packet_returns_deferred_packet(self):
        """Test that create_packet returns a DeferredPacket."""
        queue = DeferredActionQueue()
        dp = queue.create_packet("test")

        assert isinstance(dp, DeferredPacket)
        assert dp.is_resolved is False

    def test_create_packet_from_func_queues_action(self):
        """Test that create_packet_from_func adds an action."""
        queue = DeferredActionQueue()
        func = lambda: "computed"
        dp = queue.create_packet_from_func(func)

        assert len(queue.actions) == 1
        assert queue.actions[0].action_type == DeferredActionType.CREATE_PACKET_FROM_FUNC
        assert queue.actions[0].value_func is func
        assert queue.actions[0].deferred_packet is dp

    def test_consume_packet_queues_action(self):
        """Test that consume_packet adds an action and tracks value."""
        queue = DeferredActionQueue()
        dp = queue.create_packet("test")
        queue.consume_packet(dp, "consumed_value")

        assert len(queue.actions) == 2
        assert queue.actions[1].action_type == DeferredActionType.CONSUME_PACKET
        assert queue.actions[1].consumed_value == "consumed_value"
        assert queue.consumed_values[dp.deferred_id] == "consumed_value"

    def test_load_output_port_queues_action(self):
        """Test that load_output_port adds an action."""
        queue = DeferredActionQueue()
        dp = queue.create_packet("test")
        queue.load_output_port("out", dp)

        assert len(queue.actions) == 2
        assert queue.actions[1].action_type == DeferredActionType.LOAD_OUTPUT_PORT
        assert queue.actions[1].port_name == "out"
        assert queue.actions[1].packet is dp

    def test_send_output_salvo_queues_action(self):
        """Test that send_output_salvo adds an action."""
        queue = DeferredActionQueue()
        queue.send_output_salvo("send")

        assert len(queue.actions) == 1
        assert queue.actions[0].action_type == DeferredActionType.SEND_OUTPUT_SALVO
        assert queue.actions[0].salvo_condition_name == "send"

    def test_clear_resets_queue(self):
        """Test that clear resets the queue."""
        queue = DeferredActionQueue()
        queue.create_packet("test")
        queue.send_output_salvo("send")

        queue.clear()

        assert len(queue.actions) == 0
        assert len(queue.consumed_values) == 0

    def test_multiple_packets_get_unique_ids(self):
        """Test that multiple packets get unique deferred IDs."""
        queue = DeferredActionQueue()
        dp1 = queue.create_packet("value1")
        dp2 = queue.create_packet("value2")
        dp3 = queue.create_packet_from_func(lambda: "value3")

        assert dp1.deferred_id != dp2.deferred_id
        assert dp2.deferred_id != dp3.deferred_id
        assert dp1.deferred_id != dp3.deferred_id


# =============================================================================
# NodeExecutionContext Tests - Properties
# =============================================================================

class TestNodeExecutionContextProperties:
    """Tests for NodeExecutionContext properties."""

    def test_context_properties(self, simple_graph):
        """Test that context properties are accessible."""
        net = Net(simple_graph)
        ts = [datetime.now()]
        exc = [ValueError("test")]

        ctx = NodeExecutionContext(
            net=net,
            epoch_id="epoch-123",
            node_name="Source",
            defer_net_actions=False,
            retry_count=1,
            retry_timestamps=ts,
            retry_exceptions=exc,
        )

        assert ctx.epoch_id == "epoch-123"
        assert ctx.node_name == "Source"
        assert ctx.retry_count == 1
        assert ctx.retry_timestamps == ts
        assert ctx.retry_exceptions == exc

    def test_retry_properties_default_empty(self, simple_graph):
        """Test that retry properties default to empty."""
        net = Net(simple_graph)
        ctx = NodeExecutionContext(
            net=net,
            epoch_id="epoch-123",
            node_name="Source",
        )

        assert ctx.retry_count == 0
        assert ctx.retry_timestamps == []
        assert ctx.retry_exceptions == []

    def test_retry_timestamps_returns_copy(self, simple_graph):
        """Test that retry_timestamps returns a copy."""
        net = Net(simple_graph)
        ts = [datetime.now()]
        ctx = NodeExecutionContext(
            net=net,
            epoch_id="epoch-123",
            node_name="Source",
            retry_timestamps=ts,
        )

        returned_ts = ctx.retry_timestamps
        returned_ts.append(datetime.now())

        # Original should be unchanged
        assert len(ctx.retry_timestamps) == 1

    def test_retry_exceptions_returns_copy(self, simple_graph):
        """Test that retry_exceptions returns a copy."""
        net = Net(simple_graph)
        exc = [ValueError("test")]
        ctx = NodeExecutionContext(
            net=net,
            epoch_id="epoch-123",
            node_name="Source",
            retry_exceptions=exc,
        )

        returned_exc = ctx.retry_exceptions
        returned_exc.append(RuntimeError("new"))

        # Original should be unchanged
        assert len(ctx.retry_exceptions) == 1


# =============================================================================
# NodeExecutionContext Tests - Deferred Mode
# =============================================================================

class TestNodeExecutionContextDeferred:
    """Tests for NodeExecutionContext in deferred mode."""

    def test_deferred_mode_creates_queue(self, simple_graph):
        """Test that deferred mode creates a queue."""
        net = Net(simple_graph)
        ctx = NodeExecutionContext(
            net=net,
            epoch_id="epoch-123",
            node_name="Source",
            defer_net_actions=True,
        )

        assert ctx._get_deferred_queue() is not None

    def test_non_deferred_mode_no_queue(self, simple_graph):
        """Test that non-deferred mode has no queue."""
        net = Net(simple_graph)
        ctx = NodeExecutionContext(
            net=net,
            epoch_id="epoch-123",
            node_name="Source",
            defer_net_actions=False,
        )

        assert ctx._get_deferred_queue() is None

    def test_create_packet_deferred_returns_deferred_packet(self, simple_graph):
        """Test that create_packet in deferred mode returns DeferredPacket."""
        net = Net(simple_graph)
        ctx = NodeExecutionContext(
            net=net,
            epoch_id="epoch-123",
            node_name="Source",
            defer_net_actions=True,
        )

        packet = ctx.create_packet({"test": "value"})

        assert isinstance(packet, DeferredPacket)
        assert packet.is_resolved is False

    def test_create_packet_from_func_deferred_returns_deferred_packet(self, simple_graph):
        """Test that create_packet_from_value_func in deferred mode returns DeferredPacket."""
        net = Net(simple_graph)
        ctx = NodeExecutionContext(
            net=net,
            epoch_id="epoch-123",
            node_name="Source",
            defer_net_actions=True,
        )

        packet = ctx.create_packet_from_value_func(lambda: "computed")

        assert isinstance(packet, DeferredPacket)
        assert packet.is_resolved is False

    def test_load_output_port_deferred_queues_action(self, simple_graph):
        """Test that load_output_port in deferred mode queues action."""
        net = Net(simple_graph)
        ctx = NodeExecutionContext(
            net=net,
            epoch_id="epoch-123",
            node_name="Source",
            defer_net_actions=True,
        )

        packet = ctx.create_packet("test")
        ctx.load_output_port("out", packet)

        queue = ctx._get_deferred_queue()
        assert len(queue.actions) == 2
        assert queue.actions[1].action_type == DeferredActionType.LOAD_OUTPUT_PORT

    def test_send_output_salvo_deferred_queues_action(self, simple_graph):
        """Test that send_output_salvo in deferred mode queues action."""
        net = Net(simple_graph)
        ctx = NodeExecutionContext(
            net=net,
            epoch_id="epoch-123",
            node_name="Source",
            defer_net_actions=True,
        )

        ctx.send_output_salvo("send")

        queue = ctx._get_deferred_queue()
        assert len(queue.actions) == 1
        assert queue.actions[0].action_type == DeferredActionType.SEND_OUTPUT_SALVO


# =============================================================================
# NodeExecutionContext Tests - Cancel Epoch
# =============================================================================

class TestNodeExecutionContextCancelEpoch:
    """Tests for cancel_epoch functionality."""

    def test_cancel_epoch_raises_exception(self, simple_graph):
        """Test that cancel_epoch raises EpochCancelled."""
        net = Net(simple_graph)
        ctx = NodeExecutionContext(
            net=net,
            epoch_id="epoch-123",
            node_name="Source",
        )

        with pytest.raises(EpochCancelled) as exc_info:
            ctx.cancel_epoch()

        assert exc_info.value.node_name == "Source"
        assert exc_info.value.epoch_id == "epoch-123"


# =============================================================================
# NodeFailureContext Tests
# =============================================================================

class TestNodeFailureContext:
    """Tests for NodeFailureContext."""

    def test_failure_context_properties(self):
        """Test that failure context properties are accessible."""
        ts = [datetime.now()]
        exc_list = [ValueError("error1")]
        input_salvo = {"in": []}
        packet_values = {"pkt-1": "value1"}
        exc = RuntimeError("final error")

        ctx = NodeFailureContext(
            epoch_id="epoch-456",
            node_name="Processor",
            retry_count=2,
            retry_timestamps=ts,
            retry_exceptions=exc_list,
            input_salvo=input_salvo,
            packet_values=packet_values,
            exception=exc,
        )

        assert ctx.epoch_id == "epoch-456"
        assert ctx.node_name == "Processor"
        assert ctx.retry_count == 2
        assert ctx.retry_timestamps == ts
        assert ctx.retry_exceptions == exc_list
        assert ctx.input_salvo == input_salvo
        assert ctx.packet_values == packet_values
        assert ctx.exception is exc

    def test_input_salvo_returns_copy(self):
        """Test that input_salvo returns a copy."""
        input_salvo = {"in": []}
        ctx = NodeFailureContext(
            epoch_id="epoch-456",
            node_name="Processor",
            retry_count=0,
            retry_timestamps=[],
            retry_exceptions=[],
            input_salvo=input_salvo,
            packet_values={},
            exception=RuntimeError(),
        )

        returned = ctx.input_salvo
        returned["new_port"] = []

        assert "new_port" not in ctx.input_salvo

    def test_packet_values_returns_copy(self):
        """Test that packet_values returns a copy."""
        packet_values = {"pkt-1": "value1"}
        ctx = NodeFailureContext(
            epoch_id="epoch-456",
            node_name="Processor",
            retry_count=0,
            retry_timestamps=[],
            retry_exceptions=[],
            input_salvo={},
            packet_values=packet_values,
            exception=RuntimeError(),
        )

        returned = ctx.packet_values
        returned["pkt-2"] = "value2"

        assert "pkt-2" not in ctx.packet_values


# =============================================================================
# Unconsume Logic Tests
# =============================================================================

class TestUnconsumeLogic:
    """Tests for the unconsume logic used in retries."""

    def test_unconsume_restores_value(self, simple_graph):
        """Test that unconsume restores packet values."""
        net = Net(simple_graph, consumed_packet_storage=True)

        # Store a value
        net._value_store.store_value("pkt-1", "original_value")

        # Consume it
        consumed = net._value_store.consume("pkt-1")
        assert consumed == "original_value"
        assert not net._value_store.has_value("pkt-1")

        # Unconsume it
        _unconsume_packets_for_retry(net, {"pkt-1": "original_value"})

        # Should be available again
        assert net._value_store.has_value("pkt-1")
        assert net._value_store.get_value("pkt-1") == "original_value"

    def test_unconsume_multiple_packets(self, simple_graph):
        """Test unconsuming multiple packets."""
        net = Net(simple_graph, consumed_packet_storage=True)

        # Store and consume multiple values
        net._value_store.store_value("pkt-1", "value1")
        net._value_store.store_value("pkt-2", "value2")
        net._value_store.consume("pkt-1")
        net._value_store.consume("pkt-2")

        # Unconsume both
        _unconsume_packets_for_retry(net, {
            "pkt-1": "value1",
            "pkt-2": "value2",
        })

        # Both should be available
        assert net._value_store.has_value("pkt-1")
        assert net._value_store.has_value("pkt-2")


# =============================================================================
# DeferredPacketIdAccessError Tests
# =============================================================================

class TestDeferredPacketIdAccessError:
    """Tests for DeferredPacketIdAccessError."""

    def test_error_message(self):
        """Test the error message is informative."""
        err = DeferredPacketIdAccessError()
        msg = str(err).lower()
        assert "packet id" in msg
        assert "commit" in msg or "defer" in msg


# =============================================================================
# Integration Tests
# =============================================================================

class TestContextIntegration:
    """Integration tests for context operations."""

    def test_consumed_values_tracked_in_context(self, simple_graph):
        """Test that consumed values are tracked in the context."""
        net = Net(simple_graph, consumed_packet_storage=True)

        # Store a value
        net._value_store.store_value("pkt-1", "test_value")

        # Create context
        ctx = NodeExecutionContext(
            net=net,
            epoch_id="epoch-123",
            node_name="Sink",
            defer_net_actions=True,  # Deferred so we don't need real packet
        )

        # We need a real packet to consume, but in deferred mode we can test
        # the tracking mechanism through the deferred queue
        dp = ctx.create_packet("created_value")
        # The created value is tracked in the deferred queue

        assert len(ctx._get_deferred_queue().actions) == 1

    def test_deferred_queue_complex_workflow(self, simple_graph):
        """Test a complex workflow with deferred actions."""
        net = Net(simple_graph)
        ctx = NodeExecutionContext(
            net=net,
            epoch_id="epoch-123",
            node_name="Source",
            defer_net_actions=True,
        )

        # Create packets
        pkt1 = ctx.create_packet({"data": 1})
        pkt2 = ctx.create_packet_from_value_func(lambda: {"data": 2})

        # Load to output port
        ctx.load_output_port("out", pkt1)
        ctx.load_output_port("out", pkt2)

        # Send salvo
        ctx.send_output_salvo("send")

        # Check queue
        queue = ctx._get_deferred_queue()
        assert len(queue.actions) == 5

        # Check types
        types = [a.action_type for a in queue.actions]
        assert types[0] == DeferredActionType.CREATE_PACKET
        assert types[1] == DeferredActionType.CREATE_PACKET_FROM_FUNC
        assert types[2] == DeferredActionType.LOAD_OUTPUT_PORT
        assert types[3] == DeferredActionType.LOAD_OUTPUT_PORT
        assert types[4] == DeferredActionType.SEND_OUTPUT_SALVO
