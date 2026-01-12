"""Tests for Milestone 1: Core Foundation.

Tests cover:
- Error types and hierarchy
- PacketValueStore functionality
- Basic Net construction and configuration
- NetSim wrapper methods
"""

import pytest
import asyncio
import tempfile
from pathlib import Path

from netrun.core import (
    # Error types
    NetrunRuntimeError,
    PacketTypeMismatch,
    ValueFunctionFailed,
    NodeExecutionFailed,
    EpochTimeout,
    EpochCancelled,
    NetNotPausedError,
    DeferredPacketIdAccessError,
    # Classes
    PacketValueStore,
    NodeConfig,
    NodeExecFuncs,
    Net,
    NetState,
    # Re-exported types
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
    # Re-exported errors
    NodeNotFoundError,
)


# =============================================================================
# Fixtures
# =============================================================================

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
def packet_value_store():
    """Create a basic PacketValueStore."""
    return PacketValueStore(consumed_storage=True, consumed_storage_limit=10)


# =============================================================================
# Error Types Tests
# =============================================================================

class TestErrorTypes:
    """Test error type definitions and inheritance."""

    def test_netrun_runtime_error_is_base(self):
        """NetrunRuntimeError is base for all netrun-specific errors."""
        assert issubclass(PacketTypeMismatch, NetrunRuntimeError)
        assert issubclass(ValueFunctionFailed, NetrunRuntimeError)
        assert issubclass(NodeExecutionFailed, NetrunRuntimeError)
        assert issubclass(EpochTimeout, NetrunRuntimeError)
        assert issubclass(EpochCancelled, NetrunRuntimeError)
        assert issubclass(NetNotPausedError, NetrunRuntimeError)
        assert issubclass(DeferredPacketIdAccessError, NetrunRuntimeError)

    def test_packet_type_mismatch(self):
        """PacketTypeMismatch stores packet info."""
        err = PacketTypeMismatch("pkt-123", "DataFrame", "dict", "input_port")
        assert err.packet_id == "pkt-123"
        assert err.expected_type == "DataFrame"
        assert err.actual_type == "dict"
        assert err.port_name == "input_port"
        assert "pkt-123" in str(err)
        assert "input_port" in str(err)

    def test_packet_type_mismatch_without_port(self):
        """PacketTypeMismatch works without port name."""
        err = PacketTypeMismatch("pkt-456", "int", "str")
        assert err.port_name is None
        assert "pkt-456" in str(err)

    def test_value_function_failed(self):
        """ValueFunctionFailed stores original exception."""
        original = ValueError("bad value")
        err = ValueFunctionFailed("pkt-789", original)
        assert err.packet_id == "pkt-789"
        assert err.original_exception is original

    def test_node_execution_failed(self):
        """NodeExecutionFailed stores node and epoch info."""
        original = RuntimeError("exec error")
        err = NodeExecutionFailed("MyNode", "epoch-123", original)
        assert err.node_name == "MyNode"
        assert err.epoch_id == "epoch-123"
        assert err.original_exception is original

    def test_epoch_timeout(self):
        """EpochTimeout stores timeout info."""
        err = EpochTimeout("MyNode", "epoch-456", 30.0)
        assert err.node_name == "MyNode"
        assert err.epoch_id == "epoch-456"
        assert err.timeout_seconds == 30.0

    def test_epoch_cancelled(self):
        """EpochCancelled stores node and epoch info."""
        err = EpochCancelled("MyNode", "epoch-789")
        assert err.node_name == "MyNode"
        assert err.epoch_id == "epoch-789"

    def test_net_not_paused_error(self):
        """NetNotPausedError stores operation name."""
        err = NetNotPausedError("save_checkpoint")
        assert err.operation == "save_checkpoint"
        assert "save_checkpoint" in str(err)

    def test_deferred_packet_id_access_error(self):
        """DeferredPacketIdAccessError has descriptive message."""
        err = DeferredPacketIdAccessError()
        assert "deferred" in str(err).lower()


# =============================================================================
# PacketValueStore Tests
# =============================================================================

class TestPacketValueStore:
    """Test PacketValueStore functionality."""

    def test_store_and_get_direct_value(self, packet_value_store):
        """Store and retrieve direct values."""
        packet_value_store.store_value("pkt-1", {"key": "value"})
        assert packet_value_store.get_value("pkt-1") == {"key": "value"}

    def test_store_and_get_multiple_values(self, packet_value_store):
        """Store and retrieve multiple values."""
        packet_value_store.store_value("pkt-1", "first")
        packet_value_store.store_value("pkt-2", "second")
        packet_value_store.store_value("pkt-3", "third")

        assert packet_value_store.get_value("pkt-1") == "first"
        assert packet_value_store.get_value("pkt-2") == "second"
        assert packet_value_store.get_value("pkt-3") == "third"

    def test_has_value(self, packet_value_store):
        """Check if packet has value."""
        packet_value_store.store_value("pkt-1", "value")
        assert packet_value_store.has_value("pkt-1")
        assert not packet_value_store.has_value("pkt-2")

    def test_get_nonexistent_raises_keyerror(self, packet_value_store):
        """Getting nonexistent packet raises KeyError."""
        with pytest.raises(KeyError):
            packet_value_store.get_value("nonexistent")

    def test_value_function_sync(self, packet_value_store):
        """Value functions are called on get."""
        call_count = [0]

        def lazy_value():
            call_count[0] += 1
            return f"computed-{call_count[0]}"

        packet_value_store.store_value_func("pkt-lazy", lazy_value)

        # First call
        assert packet_value_store.get_value("pkt-lazy") == "computed-1"
        assert call_count[0] == 1

        # Second call - function called again
        assert packet_value_store.get_value("pkt-lazy") == "computed-2"
        assert call_count[0] == 2

    def test_value_function_failure_wraps_exception(self, packet_value_store):
        """Failed value function raises ValueFunctionFailed."""
        def bad_func():
            raise ValueError("intentional error")

        packet_value_store.store_value_func("pkt-bad", bad_func)

        with pytest.raises(ValueFunctionFailed) as exc_info:
            packet_value_store.get_value("pkt-bad")

        assert exc_info.value.packet_id == "pkt-bad"
        assert isinstance(exc_info.value.original_exception, ValueError)

    def test_consume_removes_from_active(self, packet_value_store):
        """Consume removes value from active storage."""
        packet_value_store.store_value("pkt-1", "value")

        value = packet_value_store.consume("pkt-1")
        assert value == "value"
        assert not packet_value_store.has_value("pkt-1")

    def test_consume_keeps_in_consumed_storage(self, packet_value_store):
        """Consumed values are kept if consumed_storage is enabled."""
        packet_value_store.store_value("pkt-1", "value")
        packet_value_store.consume("pkt-1")

        # Can still retrieve from consumed storage
        assert packet_value_store.get_consumed_value("pkt-1") == "value"
        # Also accessible via get_value (falls back to consumed storage)
        assert packet_value_store.get_value("pkt-1") == "value"

    def test_consumed_storage_limit(self):
        """Consumed storage respects limit."""
        store = PacketValueStore(consumed_storage=True, consumed_storage_limit=3)

        # Consume 5 packets
        for i in range(5):
            store.store_value(f"pkt-{i}", f"value-{i}")
            store.consume(f"pkt-{i}")

        # Only last 3 should be in consumed storage
        assert store.get_consumed_value("pkt-0") is None
        assert store.get_consumed_value("pkt-1") is None
        assert store.get_consumed_value("pkt-2") == "value-2"
        assert store.get_consumed_value("pkt-3") == "value-3"
        assert store.get_consumed_value("pkt-4") == "value-4"

    def test_unconsume_restores_value(self, packet_value_store):
        """Unconsume restores a value to active storage."""
        packet_value_store.store_value("pkt-1", "original")
        packet_value_store.consume("pkt-1")

        # Unconsume with potentially different value
        packet_value_store.unconsume("pkt-1", "restored")

        assert packet_value_store.has_value("pkt-1")
        assert packet_value_store.get_value("pkt-1") == "restored"
        # Removed from consumed storage
        assert packet_value_store.get_consumed_value("pkt-1") is None

    def test_remove_value(self, packet_value_store):
        """Remove completely removes a value."""
        packet_value_store.store_value("pkt-1", "value")
        packet_value_store.consume("pkt-1")

        packet_value_store.remove("pkt-1")

        assert not packet_value_store.has_value("pkt-1")
        assert packet_value_store.get_consumed_value("pkt-1") is None

    def test_file_storage(self):
        """File-based storage works."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = PacketValueStore(storage_path=tmpdir)
            store.store_value("pkt-1", {"data": "value"})

            # File should exist
            file_path = Path(tmpdir) / "pkt-1.pkl"
            assert file_path.exists()

            # Value should be retrievable
            assert store.get_value("pkt-1") == {"data": "value"}

    @pytest.mark.asyncio
    async def test_async_value_function(self):
        """Async value functions are awaited."""
        store = PacketValueStore()

        async def async_lazy():
            await asyncio.sleep(0.01)
            return "async-result"

        store.store_value_func("pkt-async", async_lazy)

        value = await store.async_get_value("pkt-async")
        assert value == "async-result"

    @pytest.mark.asyncio
    async def test_async_consume(self):
        """Async consume works."""
        store = PacketValueStore(consumed_storage=True)

        async def async_lazy():
            return "async-value"

        store.store_value_func("pkt-1", async_lazy)
        value = await store.async_consume("pkt-1")

        assert value == "async-value"
        assert not store.has_value("pkt-1")
        assert store.get_consumed_value("pkt-1") == "async-value"


# =============================================================================
# NodeConfig Tests
# =============================================================================

class TestNodeConfig:
    """Test NodeConfig validation."""

    def test_default_config(self):
        """Default config has sensible defaults."""
        config = NodeConfig()
        assert config.pool is None
        assert config.retries == 0
        assert config.defer_net_actions is False
        assert config.timeout is None

    def test_retries_requires_defer(self):
        """retries > 0 requires defer_net_actions=True."""
        with pytest.raises(ValueError, match="defer_net_actions must be True"):
            NodeConfig(retries=3, defer_net_actions=False)

    def test_retries_with_defer_works(self):
        """retries > 0 works with defer_net_actions=True."""
        config = NodeConfig(retries=3, defer_net_actions=True)
        assert config.retries == 3
        assert config.defer_net_actions is True

    def test_all_options(self):
        """All options can be set."""
        config = NodeConfig(
            pool="worker",
            max_parallel_epochs=5,
            rate_limit_per_second=10.0,
            defer_net_actions=True,
            retries=2,
            retry_wait=1.0,
            timeout=30.0,
            dead_letter_queue=True,
            capture_stdout=True,
            echo_stdout=True,
            pool_init_mode="global",
        )
        assert config.pool == "worker"
        assert config.max_parallel_epochs == 5


# =============================================================================
# Net Class Tests
# =============================================================================

class TestNet:
    """Test Net class construction and configuration."""

    def test_basic_construction(self, simple_graph):
        """Net can be constructed with a graph."""
        net = Net(simple_graph)
        assert net.graph is simple_graph
        assert net.state == NetState.CREATED

    def test_on_error_validation(self, simple_graph):
        """on_error must be valid option."""
        # Valid options work
        Net(simple_graph, on_error="continue")
        Net(simple_graph, on_error="pause")
        Net(simple_graph, on_error="raise")

        # Invalid option raises
        with pytest.raises(ValueError, match="on_error must be"):
            Net(simple_graph, on_error="invalid")

    def test_set_node_exec(self, simple_graph):
        """set_node_exec stores execution functions."""
        net = Net(simple_graph)

        def exec_func(ctx, packets):
            pass

        def start_func(net):
            pass

        net.set_node_exec("Source", exec_func, start_func=start_func)

        funcs = net.get_node_exec_funcs("Source")
        assert funcs is not None
        assert funcs.exec_func is exec_func
        assert funcs.start_func is start_func
        assert funcs.stop_func is None
        assert funcs.failed_func is None

    def test_set_node_exec_invalid_node(self, simple_graph):
        """set_node_exec raises for invalid node."""
        net = Net(simple_graph)

        def exec_func(ctx, packets):
            pass

        with pytest.raises(NodeNotFoundError):
            net.set_node_exec("NonexistentNode", exec_func)

    def test_set_node_config(self, simple_graph):
        """set_node_config stores configuration."""
        net = Net(simple_graph)
        net.set_node_config("Source", timeout=30.0, capture_stdout=False)

        config = net.get_node_config("Source")
        assert config.timeout == 30.0
        assert config.capture_stdout is False

    def test_set_node_config_invalid_node(self, simple_graph):
        """set_node_config raises for invalid node."""
        net = Net(simple_graph)

        with pytest.raises(NodeNotFoundError):
            net.set_node_config("NonexistentNode", timeout=10.0)

    def test_set_node_config_invalid_option(self, simple_graph):
        """set_node_config raises for invalid option."""
        net = Net(simple_graph)

        with pytest.raises(ValueError, match="Unknown config option"):
            net.set_node_config("Source", invalid_option=True)

    def test_set_node_config_updates_existing(self, simple_graph):
        """set_node_config updates existing config."""
        net = Net(simple_graph)
        net.set_node_config("Source", timeout=30.0)
        net.set_node_config("Source", capture_stdout=False)

        config = net.get_node_config("Source")
        assert config.timeout == 30.0  # Preserved
        assert config.capture_stdout is False  # Updated

    def test_get_node_config_returns_default(self, simple_graph):
        """get_node_config returns default for unconfigured node."""
        net = Net(simple_graph)
        config = net.get_node_config("Source")

        assert config.pool is None
        assert config.retries == 0

    def test_value_store_access(self, simple_graph):
        """Value store is accessible."""
        net = Net(
            simple_graph,
            consumed_packet_storage=True,
            consumed_packet_storage_limit=100,
        )

        store = net.value_store
        assert isinstance(store, PacketValueStore)

        # Can use it
        store.store_value("pkt-1", "value")
        assert store.get_value("pkt-1") == "value"

    def test_state_transitions(self, simple_graph):
        """Net state can be changed."""
        net = Net(simple_graph)
        assert net.state == NetState.CREATED

        net.pause()
        assert net.state == NetState.PAUSED

        net.stop()
        assert net.state == NetState.STOPPED

    def test_save_checkpoint_requires_paused(self, simple_graph):
        """save_checkpoint raises if not paused."""
        net = Net(simple_graph)

        with pytest.raises(NetNotPausedError):
            net.save_checkpoint("/tmp/checkpoint")

    def test_wrapper_methods_exist(self, simple_graph):
        """NetSim wrapper methods are available."""
        net = Net(simple_graph)

        # These should not raise
        epochs = net.get_startable_epochs()
        assert isinstance(epochs, list)

        source_epochs = net.get_startable_epochs_by_node("Source")
        assert isinstance(source_epochs, list)


# =============================================================================
# Re-export Tests
# =============================================================================

class TestReExports:
    """Test that netrun_sim types are properly re-exported."""

    def test_graph_types_available(self):
        """Graph types are importable from netrun.core."""
        from netrun.core import Graph, Node, Edge, Port, PortType, PortRef
        assert Graph is not None
        assert Node is not None
        assert Edge is not None

    def test_net_types_available(self):
        """Net types are importable from netrun.core."""
        from netrun.core import NetSim, Packet, Epoch, PacketLocation
        assert NetSim is not None
        assert Packet is not None

    def test_error_types_from_netrun_sim(self):
        """netrun_sim errors are re-exported."""
        from netrun.core import (
            NodeNotFoundError,
            PacketNotFoundError,
            EpochNotFoundError,
        )
        assert NodeNotFoundError is not None
        assert PacketNotFoundError is not None
        assert EpochNotFoundError is not None
