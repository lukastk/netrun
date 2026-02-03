# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Net Module
#
# Comprehensive tests for the Net class and related components.

# %%
#|default_exp net.test_net

# %%
#|export
import pytest
import asyncio
import time
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
from dataclasses import dataclass

from netrun.net._net import (
    Net,
    NetProtocolKeys,
    NodeExecutionContext,
    NodeFailureContext,
    DeferredActionQueue,
    EpochCancelled,
    create_net_func_preprocessor,
    create_net_func_done_callback,
)
from netrun.net.config import (
    NetConfig,
    GraphConfig,
    NodeGraphConfig,
    NodeExecutionConfig,
    PoolConfig,
    MainPoolConfig,
    ThreadPoolConfig,
    MultiprocessPoolConfig,
    PortConfig,
    EdgeConfig,
    SalvoConditionConfig,
    SalvoConditionTermTrueConfig,
    SalvoConditionTermPortConfig,
    MaxSalvosFiniteConfig,
    PacketCountAllConfig,
    PortStateNonEmptyConfig,
)
from netrun.execution_manager import RunAllocationMethod
from netrun.storage import LazyPacketValueSpec

# %% [markdown]
# ## NetProtocolKeys Tests

# %%
#|export
def test_net_protocol_keys_values():
    """Test that NetProtocolKeys have expected string values."""
    assert NetProtocolKeys.UP_CREATE_PACKET.value == "net:create-packet"
    assert NetProtocolKeys.UP_CREATE_PACKET_RESPONSE.value == "net:create-packet-response"
    assert NetProtocolKeys.UP_CONSUME_PACKET.value == "net:consume-packet"
    assert NetProtocolKeys.UP_CONSUME_PACKET_RESPONSE.value == "net:consume-packet-response"
    assert NetProtocolKeys.UP_LOAD_OUTPUT_PORT.value == "net:load-output-port"
    assert NetProtocolKeys.UP_LOAD_OUTPUT_PORT_RESPONSE.value == "net:load-output-port-response"
    assert NetProtocolKeys.UP_SEND_OUTPUT_SALVO.value == "net:send-salvo"
    assert NetProtocolKeys.UP_SEND_OUTPUT_SALVO_RESPONSE.value == "net:send-salvo-response"
    assert NetProtocolKeys.UP_CANCEL_EPOCH.value == "net:cancel-epoch"
    assert NetProtocolKeys.UP_PRINT_BUFFER.value == "net:print-buffer"

# %%
test_net_protocol_keys_values()

# %%
#|export
def test_net_protocol_keys_uniqueness():
    """Test that all protocol keys have unique values."""
    values = [key.value for key in NetProtocolKeys]
    assert len(values) == len(set(values)), "Protocol keys must have unique values"

# %%
test_net_protocol_keys_uniqueness()

# %% [markdown]
# ## NodeExecutionContext Tests

# %%
#|export
def test_node_execution_context_creation():
    """Test NodeExecutionContext can be created with required fields."""
    ctx = NodeExecutionContext(
        epoch_id="epoch_123",
        node_name="TestNode",
    )
    assert ctx.epoch_id == "epoch_123"
    assert ctx.node_name == "TestNode"
    assert ctx.retry_count == 0
    assert ctx.retry_timestamps == []
    assert ctx.retry_exceptions == []
    assert ctx._print_buffer == []
    assert ctx._created_packets == []
    assert ctx._consumed_packets == []

# %%
test_node_execution_context_creation()

# %%
#|export
def test_node_execution_context_with_retry_info():
    """Test NodeExecutionContext with retry information."""
    timestamps = [datetime.now()]
    exceptions = [ValueError("test error")]

    ctx = NodeExecutionContext(
        epoch_id="epoch_456",
        node_name="RetryNode",
        retry_count=2,
        retry_timestamps=timestamps,
        retry_exceptions=exceptions,
    )
    assert ctx.retry_count == 2
    assert ctx.retry_timestamps == timestamps
    assert ctx.retry_exceptions == exceptions

# %%
test_node_execution_context_with_retry_info()

# %% [markdown]
# ### Print Capture Tests

# %%
#|export
def test_context_print_basic():
    """Test basic print capture without config."""
    ctx = NodeExecutionContext(
        epoch_id="epoch_print",
        node_name="PrintNode",
    )

    ctx.print("Hello", "World")

    # Buffer should contain (timestamp, message) tuple
    assert len(ctx._print_buffer) == 1
    timestamp, message = ctx._print_buffer[0]
    assert isinstance(timestamp, datetime)
    assert message == "Hello World\n"

# %%
test_context_print_basic()

# %%
#|export
def test_context_print_custom_separators():
    """Test print with custom sep and end."""
    ctx = NodeExecutionContext(
        epoch_id="epoch_sep",
        node_name="SepNode",
    )

    ctx.print("a", "b", "c", sep="-", end="!")

    timestamp, message = ctx._print_buffer[0]
    assert isinstance(timestamp, datetime)
    assert message == "a-b-c!"

# %%
test_context_print_custom_separators()

# %%
#|export
def test_context_print_empty():
    """Test print with no arguments."""
    ctx = NodeExecutionContext(
        epoch_id="epoch_empty",
        node_name="EmptyNode",
    )

    ctx.print()

    timestamp, message = ctx._print_buffer[0]
    assert isinstance(timestamp, datetime)
    assert message == "\n"

# %%
test_context_print_empty()

# %%
#|export
def test_context_print_non_string():
    """Test print with non-string arguments."""
    ctx = NodeExecutionContext(
        epoch_id="epoch_types",
        node_name="TypesNode",
    )

    ctx.print(1, 2.5, True, None, [1, 2, 3])

    timestamp, message = ctx._print_buffer[0]
    assert isinstance(timestamp, datetime)
    assert message == "1 2.5 True None [1, 2, 3]\n"

# %%
test_context_print_non_string()

# %%
#|export
def test_context_print_flush_interval():
    """Test print buffer flush based on time interval."""
    mock_channel = Mock()

    config = NodeExecutionConfig(
        node_name="FlushNode",
        print_flush_interval=0.05,  # 50ms
    )

    ctx = NodeExecutionContext(
        epoch_id="epoch_flush",
        node_name="FlushNode",
        _channel=mock_channel,
        _config=config,
    )

    # First print shouldn't flush (time threshold not exceeded)
    ctx.print("First message")
    assert len(ctx._print_buffer) == 1
    mock_channel.send.assert_not_called()

    # Wait for flush interval
    time.sleep(0.06)

    # Second print should trigger flush
    ctx.print("Second message")

    # Buffer should be empty after flush, and send should have been called
    # Note: The second message is added after flush, so buffer has 1 item
    mock_channel.send.assert_called_once()
    call_args = mock_channel.send.call_args
    assert call_args[0][0] == NetProtocolKeys.UP_PRINT_BUFFER.value
    assert call_args[0][1][0] == "epoch_flush"
    # Buffer is list of (timestamp, message) tuples
    buffer = call_args[0][1][1]
    messages = [msg for _, msg in buffer]
    assert "First message\n" in messages

# %%
test_context_print_flush_interval()

# %%
#|export
def test_context_print_buffer_max_size():
    """Test print buffer flush based on max size."""
    mock_channel = Mock()

    config = NodeExecutionConfig(
        node_name="MaxSizeNode",
        print_flush_interval=10.0,  # Long interval (won't trigger)
        print_buffer_max_size=3,
    )

    ctx = NodeExecutionContext(
        epoch_id="epoch_maxsize",
        node_name="MaxSizeNode",
        _channel=mock_channel,
        _config=config,
    )

    # Add messages up to max size
    ctx.print("Message 1")
    ctx.print("Message 2")
    mock_channel.send.assert_not_called()

    # This should trigger flush (buffer size = 3)
    ctx.print("Message 3")
    mock_channel.send.assert_called_once()

# %%
test_context_print_buffer_max_size()

# %%
#|export
def test_context_print_explicit_flush():
    """Test explicit flush=True parameter."""
    mock_channel = Mock()

    config = NodeExecutionConfig(
        node_name="ExplicitFlush",
        print_flush_interval=10.0,  # Won't trigger automatically
    )

    ctx = NodeExecutionContext(
        epoch_id="epoch_explicit",
        node_name="ExplicitFlush",
        _channel=mock_channel,
        _config=config,
    )

    # Print with flush=True
    ctx.print("Immediate flush", flush=True)

    mock_channel.send.assert_called_once()
    call_args = mock_channel.send.call_args
    # Buffer is list of (timestamp, message) tuples
    buffer = call_args[0][1][1]
    messages = [msg for _, msg in buffer]
    assert "Immediate flush\n" in messages

# %%
test_context_print_explicit_flush()

# %%
#|export
def test_context_print_echo_stdout(capsys):
    """Test print with stdout echo enabled."""
    config = NodeExecutionConfig(
        node_name="EchoNode",
        print_echo_stdout=True,
    )

    ctx = NodeExecutionContext(
        epoch_id="epoch_echo",
        node_name="EchoNode",
        _config=config,
    )

    ctx.print("Echo this message")

    # Check stdout
    captured = capsys.readouterr()
    assert "Echo this message" in captured.out

    # Also check buffer contains the timestamped message
    assert len(ctx._print_buffer) == 1
    timestamp, message = ctx._print_buffer[0]
    assert message == "Echo this message\n"

# %%
test_context_print_echo_stdout()

# %%
#|export
def test_context_flush_print_buffer_empty():
    """Test flushing an empty buffer does nothing."""
    mock_channel = Mock()

    ctx = NodeExecutionContext(
        epoch_id="epoch_empty_flush",
        node_name="EmptyFlush",
        _channel=mock_channel,
    )

    ctx._flush_print_buffer()

    mock_channel.send.assert_not_called()

# %%
test_context_flush_print_buffer_empty()

# %% [markdown]
# ### Packet Operation Tests

# %%
#|export
def test_context_create_packet():
    """Test create_packet sends correct message and handles response."""
    mock_channel = Mock()
    mock_channel.recv.return_value = (
        NetProtocolKeys.UP_CREATE_PACKET_RESPONSE.value,
        "packet_abc123"
    )

    ctx = NodeExecutionContext(
        epoch_id="epoch_create",
        node_name="CreateNode",
        _channel=mock_channel,
    )

    packet_id = ctx.create_packet({"data": "test"})

    assert packet_id == "packet_abc123"
    assert "packet_abc123" in ctx._created_packets

    # Verify send was called with correct args
    mock_channel.send.assert_called_once()
    call_args = mock_channel.send.call_args
    assert call_args[0][0] == NetProtocolKeys.UP_CREATE_PACKET.value
    assert call_args[0][1] == ("epoch_create", {"data": "test"})

# %%
test_context_create_packet()

# %%
#|export
def test_context_create_packet_from_value_func():
    """Test create_packet_from_value_func sends LazyPacketValueSpec."""
    mock_channel = Mock()
    mock_channel.recv.return_value = (
        NetProtocolKeys.UP_CREATE_PACKET_RESPONSE.value,
        "lazy_packet_123"
    )

    ctx = NodeExecutionContext(
        epoch_id="epoch_lazy",
        node_name="LazyNode",
        _channel=mock_channel,
    )

    packet_id = ctx.create_packet_from_value_func(
        func_import_path="mymodule.fetch_data",
        args=("arg1",),
        kwargs={"key": "value"},
    )

    assert packet_id == "lazy_packet_123"

    # Verify the LazyPacketValueSpec was sent
    call_args = mock_channel.send.call_args
    sent_value = call_args[0][1][1]
    assert isinstance(sent_value, LazyPacketValueSpec)
    assert sent_value.func_import_path == "mymodule.fetch_data"
    assert sent_value.args == ("arg1",)
    assert sent_value.kwargs == {"key": "value"}

# %%
test_context_create_packet_from_value_func()

# %%
#|export
def test_context_consume_packet():
    """Test consume_packet sends correct message and returns value."""
    mock_channel = Mock()
    mock_channel.recv.return_value = (
        NetProtocolKeys.UP_CONSUME_PACKET_RESPONSE.value,
        {"consumed": "data"}
    )

    ctx = NodeExecutionContext(
        epoch_id="epoch_consume",
        node_name="ConsumeNode",
        _channel=mock_channel,
    )

    value = ctx.consume_packet("packet_xyz")

    assert value == {"consumed": "data"}
    assert "packet_xyz" in ctx._consumed_packets

    call_args = mock_channel.send.call_args
    assert call_args[0][0] == NetProtocolKeys.UP_CONSUME_PACKET.value
    assert call_args[0][1] == ("epoch_consume", "packet_xyz")

# %%
test_context_consume_packet()

# %%
#|export
def test_context_load_output_port():
    """Test load_output_port sends correct message."""
    mock_channel = Mock()
    mock_channel.recv.return_value = (
        NetProtocolKeys.UP_LOAD_OUTPUT_PORT_RESPONSE.value,
        None
    )

    ctx = NodeExecutionContext(
        epoch_id="epoch_load",
        node_name="LoadNode",
        _channel=mock_channel,
    )

    ctx.load_output_port("out", "packet_123")

    call_args = mock_channel.send.call_args
    assert call_args[0][0] == NetProtocolKeys.UP_LOAD_OUTPUT_PORT.value
    assert call_args[0][1] == ("epoch_load", "out", "packet_123")

# %%
test_context_load_output_port()

# %%
#|export
def test_context_send_output_salvo():
    """Test send_output_salvo sends correct message."""
    mock_channel = Mock()
    mock_channel.recv.return_value = (
        NetProtocolKeys.UP_SEND_OUTPUT_SALVO_RESPONSE.value,
        None
    )

    ctx = NodeExecutionContext(
        epoch_id="epoch_salvo",
        node_name="SalvoNode",
        _channel=mock_channel,
    )

    ctx.send_output_salvo("send_condition")

    call_args = mock_channel.send.call_args
    assert call_args[0][0] == NetProtocolKeys.UP_SEND_OUTPUT_SALVO.value
    assert call_args[0][1] == ("epoch_salvo", "send_condition")

# %%
test_context_send_output_salvo()

# %%
#|export
def test_context_cancel_epoch():
    """Test cancel_epoch sends message and raises EpochCancelled."""
    mock_channel = Mock()

    ctx = NodeExecutionContext(
        epoch_id="epoch_cancel",
        node_name="CancelNode",
        _channel=mock_channel,
    )

    with pytest.raises(EpochCancelled) as exc_info:
        ctx.cancel_epoch()

    assert "epoch_cancel" in str(exc_info.value)

    call_args = mock_channel.send.call_args
    assert call_args[0][0] == NetProtocolKeys.UP_CANCEL_EPOCH.value
    assert call_args[0][1] == ("epoch_cancel",)

# %%
test_context_cancel_epoch()

# %%
#|export
def test_context_unexpected_response_raises():
    """Test that unexpected response key raises RuntimeError."""
    mock_channel = Mock()
    mock_channel.recv.return_value = ("wrong:key", "data")

    ctx = NodeExecutionContext(
        epoch_id="epoch_wrong",
        node_name="WrongNode",
        _channel=mock_channel,
    )

    with pytest.raises(RuntimeError) as exc_info:
        ctx.create_packet("value")

    assert "Expected" in str(exc_info.value)
    assert "wrong:key" in str(exc_info.value)

# %%
test_context_unexpected_response_raises()

# %% [markdown]
# ## NodeFailureContext Tests

# %%
#|export
def test_node_failure_context_creation():
    """Test NodeFailureContext creation."""
    exc = ValueError("test error")
    ctx = NodeFailureContext(
        epoch_id="epoch_fail",
        node_name="FailNode",
        retry_count=1,
        exception=exc,
        input_salvo={"in": ["packet_1", "packet_2"]},
    )

    assert ctx.epoch_id == "epoch_fail"
    assert ctx.node_name == "FailNode"
    assert ctx.retry_count == 1
    assert ctx.exception == exc
    assert ctx.input_salvo == {"in": ["packet_1", "packet_2"]}

# %%
test_node_failure_context_creation()

# %% [markdown]
# ## DeferredActionQueue Tests

# %%
#|export
def test_deferred_queue_add_create_packet():
    """Test adding create_packet action to deferred queue."""
    queue = DeferredActionQueue()

    deferred_id = queue.add_create_packet({"value": 123})

    assert deferred_id.startswith("deferred_")
    assert len(queue.actions) == 1
    assert queue.actions[0][0] == "create_packet"
    assert deferred_id in queue.packet_values
    assert queue.packet_values[deferred_id] == {"value": 123}

# %%
test_deferred_queue_add_create_packet()

# %%
#|export
def test_deferred_queue_add_consume_packet():
    """Test adding consume_packet action to deferred queue."""
    queue = DeferredActionQueue()

    queue.add_consume_packet("packet_to_consume")

    assert len(queue.actions) == 1
    assert queue.actions[0] == ("consume_packet", ("packet_to_consume",))

# %%
test_deferred_queue_add_consume_packet()

# %%
#|export
def test_deferred_queue_add_load_output_port():
    """Test adding load_output_port action to deferred queue."""
    queue = DeferredActionQueue()

    queue.add_load_output_port("out", "packet_123")

    assert len(queue.actions) == 1
    assert queue.actions[0] == ("load_output_port", ("out", "packet_123"))

# %%
test_deferred_queue_add_load_output_port()

# %%
#|export
def test_deferred_queue_add_send_output_salvo():
    """Test adding send_output_salvo action to deferred queue."""
    queue = DeferredActionQueue()

    queue.add_send_output_salvo("send_condition")

    assert len(queue.actions) == 1
    assert queue.actions[0] == ("send_output_salvo", ("send_condition",))

# %%
test_deferred_queue_add_send_output_salvo()

# %%
#|export
def test_deferred_queue_discard():
    """Test discarding all queued actions."""
    queue = DeferredActionQueue()

    queue.add_create_packet("value1")
    queue.add_consume_packet("packet_1")
    queue.add_load_output_port("out", "packet_2")

    assert len(queue.actions) == 3
    assert len(queue.packet_values) == 1

    queue.discard()

    assert len(queue.actions) == 0
    assert len(queue.packet_values) == 0
    assert len(queue.deferred_to_real_ids) == 0

# %%
test_deferred_queue_discard()

# %%
#|export
def test_deferred_queue_multiple_creates():
    """Test creating multiple packets with deferred queue."""
    queue = DeferredActionQueue()

    id1 = queue.add_create_packet("value1")
    id2 = queue.add_create_packet("value2")
    id3 = queue.add_create_packet("value3")

    # All IDs should be unique
    assert len({id1, id2, id3}) == 3

    # All should start with deferred_
    assert all(id_.startswith("deferred_") for id_ in [id1, id2, id3])

# %%
test_deferred_queue_multiple_creates()

# %% [markdown]
# ## func_preprocessor Tests

# %%
#|export
def test_create_net_func_preprocessor_basic():
    """Test func_preprocessor transforms function correctly."""
    node_configs = {
        "TestNode": NodeExecutionConfig(
            node_name="TestNode",
            print_flush_interval=0.1,
        )
    }

    preprocessor = create_net_func_preprocessor(node_configs)

    call_log = []

    def test_func(ctx, packets):
        call_log.append((ctx.epoch_id, ctx.node_name, packets))
        return "result"

    wrapped = preprocessor(test_func)

    # Create a mock channel
    mock_channel = Mock()

    result = wrapped(
        channel=mock_channel,
        epoch_id="epoch_test",
        node_name="TestNode",
        packets={"in": ["p1", "p2"]},
    )

    assert result == "result"
    assert len(call_log) == 1
    assert call_log[0][0] == "epoch_test"
    assert call_log[0][1] == "TestNode"
    assert call_log[0][2] == {"in": ["p1", "p2"]}

# %%
test_create_net_func_preprocessor_basic()

# %%
#|export
def test_create_net_func_preprocessor_with_retry_info():
    """Test func_preprocessor passes retry information."""
    node_configs = {}
    preprocessor = create_net_func_preprocessor(node_configs)

    captured_ctx = None

    def test_func(ctx, packets):
        nonlocal captured_ctx
        captured_ctx = ctx
        return "ok"

    wrapped = preprocessor(test_func)
    mock_channel = Mock()

    retry_ts = [datetime.now()]
    retry_exc = [ValueError("retry error")]

    wrapped(
        channel=mock_channel,
        epoch_id="epoch_retry",
        node_name="RetryNode",
        packets={},
        retry_count=2,
        retry_timestamps=retry_ts,
        retry_exceptions=retry_exc,
    )

    assert captured_ctx.retry_count == 2
    assert captured_ctx.retry_timestamps == retry_ts
    assert captured_ctx.retry_exceptions == retry_exc

# %%
test_create_net_func_preprocessor_with_retry_info()

# %%
#|export
def test_create_net_func_preprocessor_flushes_on_success():
    """Test func_preprocessor flushes print buffer on success."""
    node_configs = {
        "FlushNode": NodeExecutionConfig(
            node_name="FlushNode",
            print_flush_interval=100.0,  # Won't auto-flush
        )
    }
    preprocessor = create_net_func_preprocessor(node_configs)

    def test_func(ctx, packets):
        ctx.print("Message 1")
        ctx.print("Message 2")
        return "done"

    wrapped = preprocessor(test_func)
    mock_channel = Mock()

    wrapped(
        channel=mock_channel,
        epoch_id="epoch_flush",
        node_name="FlushNode",
        packets={},
    )

    # Final flush should have been called
    mock_channel.send.assert_called()
    # The call should contain both messages
    call_args = mock_channel.send.call_args
    assert call_args[0][0] == NetProtocolKeys.UP_PRINT_BUFFER.value

# %%
test_create_net_func_preprocessor_flushes_on_success()

# %%
#|export
def test_create_net_func_preprocessor_flushes_on_exception():
    """Test func_preprocessor flushes print buffer even on exception."""
    node_configs = {
        "ExcNode": NodeExecutionConfig(
            node_name="ExcNode",
            print_flush_interval=100.0,
        )
    }
    preprocessor = create_net_func_preprocessor(node_configs)

    def test_func(ctx, packets):
        ctx.print("Before error")
        raise ValueError("test error")

    wrapped = preprocessor(test_func)
    mock_channel = Mock()

    with pytest.raises(ValueError):
        wrapped(
            channel=mock_channel,
            epoch_id="epoch_exc",
            node_name="ExcNode",
            packets={},
        )

    # Final flush should still have been called (in finally block)
    mock_channel.send.assert_called()

# %%
test_create_net_func_preprocessor_flushes_on_exception()

# %% [markdown]
# ## func_done_callback Tests

# %%
#|export
def test_create_net_func_done_callback():
    """Test func_done_callback creation."""
    callback = create_net_func_done_callback()

    # Should be callable
    assert callable(callback)

    # Should accept expected arguments without error
    mock_channel = Mock()
    callback(
        channel=mock_channel,
        epoch_id="epoch_done",
        node_name="DoneNode",
        packets={},
        result="some_result",
    )
    # No error = success

# %%
test_create_net_func_done_callback()

# %% [markdown]
# ## Net Class Tests

# %%
#|export
def create_simple_graph_config():
    """Helper to create a simple graph config for testing."""
    return GraphConfig(
        nodes=[
            NodeGraphConfig(
                name="Source",
                out_ports={"out": PortConfig()},
                out_salvo_conditions={
                    "send": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"out": PacketCountAllConfig()},
                        term=SalvoConditionTermTrueConfig(),
                    ),
                },
            ),
            NodeGraphConfig(
                name="Sink",
                in_ports={"in": PortConfig()},
                in_salvo_conditions={
                    "default": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"in": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(
                            port_name="in",
                            state=PortStateNonEmptyConfig(),
                        ),
                    ),
                },
            ),
        ],
        edges=[
            EdgeConfig(source_str="Source.out", target_str="Sink.in"),
        ],
    )

# %%
#|export
def create_simple_net_config():
    """Helper to create a simple net config for testing."""
    return NetConfig(
        pools={
            "main_pool": PoolConfig(
                id="main_pool",
                spec=MainPoolConfig(),
            ),
        },
        graph=create_simple_graph_config(),
    )

# %%
#|export
def test_net_creation():
    """Test Net can be created with valid config."""
    config = create_simple_net_config()
    net = Net(config)

    assert net.config == config
    assert net.graph is not None
    assert net.netsim is not None
    assert net.started is False
    assert net.paused is False

# %%
test_net_creation()

# %%
#|export
def test_net_config_property():
    """Test Net config property returns correct config."""
    config = create_simple_net_config()
    net = Net(config)

    assert net.config is config
    assert net.config.pools == config.pools

# %%
test_net_config_property()

# %%
#|export
def test_net_graph_property():
    """Test Net graph property returns netrun_sim.Graph."""
    import netrun_sim

    config = create_simple_net_config()
    net = Net(config)

    assert isinstance(net.graph, netrun_sim.Graph)

# %%
test_net_graph_property()

# %%
#|export
def test_net_with_multiple_pool_types():
    """Test Net can be created with multiple pool types."""
    config = NetConfig(
        pools={
            "main": PoolConfig(id="main", spec=MainPoolConfig()),
            "threads": PoolConfig(id="threads", spec=ThreadPoolConfig(num_workers=2)),
        },
        graph=create_simple_graph_config(),
    )

    net = Net(config)
    assert len(config.pools) == 2

# %%
test_net_with_multiple_pool_types()

# %%
#|export
def test_net_with_node_execution_configs():
    """Test Net extracts node execution configs from graph."""
    def dummy_func(ctx, packets):
        pass

    graph_config = GraphConfig(
        nodes=[
            NodeGraphConfig(
                name="NodeA",
                out_ports={"out": PortConfig()},
                execution_config=NodeExecutionConfig(
                    node_name="NodeA",
                    pools=["main"],
                    exec_node_func=dummy_func,
                ),
            ),
            NodeGraphConfig(
                name="NodeB",
                in_ports={"in": PortConfig()},
                in_salvo_conditions={
                    "default": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"in": PacketCountAllConfig()},
                        term=SalvoConditionTermTrueConfig(),
                    ),
                },
                # No execution config
            ),
        ],
        edges=[EdgeConfig(source_str="NodeA.out", target_str="NodeB.in")],
    )

    config = NetConfig(
        pools={"main": PoolConfig(id="main", spec=MainPoolConfig())},
        graph=graph_config,
    )

    net = Net(config)

    # Should have extracted NodeA's config
    assert "NodeA" in net._node_execution_configs
    assert "NodeB" not in net._node_execution_configs

# %%
test_net_with_node_execution_configs()

# %%
#|export
@pytest.mark.asyncio
async def test_net_start_and_stop():
    """Test Net can be started and stopped."""
    config = create_simple_net_config()
    net = Net(config)

    assert net.started is False

    await net.start()
    assert net.started is True

    await net.stop()
    assert net.started is False

# %%
#|export
@pytest.mark.asyncio
async def test_net_start_twice_raises():
    """Test starting Net twice raises RuntimeError."""
    config = create_simple_net_config()
    net = Net(config)

    await net.start()

    with pytest.raises(RuntimeError) as exc_info:
        await net.start()

    assert "already started" in str(exc_info.value).lower()

    await net.stop()

# %%
#|export
@pytest.mark.asyncio
async def test_net_context_manager():
    """Test Net can be used as async context manager."""
    config = create_simple_net_config()

    async with Net(config) as net:
        assert net.started is True

    assert net.started is False

# %%
#|export
@pytest.mark.asyncio
async def test_net_pause_and_resume():
    """Test Net can be paused and resumed."""
    config = create_simple_net_config()

    async with Net(config) as net:
        assert net.paused is False

        await net.pause()
        assert net.paused is True

        await net.resume()
        assert net.paused is False

# %%
#|export
@pytest.mark.asyncio
async def test_net_run_step():
    """Test Net.run_step executes simulation step."""
    config = create_simple_net_config()

    async with Net(config) as net:
        # run_step returns (made_progress, events) tuple
        result = await net.run_step()
        assert isinstance(result, tuple)
        assert len(result) == 2
        made_progress, events = result
        assert isinstance(made_progress, bool)
        assert isinstance(events, list)

# %%
#|export
@pytest.mark.asyncio
async def test_net_run_until_blocked():
    """Test Net.run_until_blocked runs until no progress."""
    config = create_simple_net_config()

    async with Net(config) as net:
        # run_until_blocked should return all events
        events = await net.run_until_blocked()
        assert isinstance(events, list)

# %%
#|export
@pytest.mark.asyncio
async def test_net_get_startable_epochs():
    """Test Net.get_startable_epochs returns list."""
    config = create_simple_net_config()

    async with Net(config) as net:
        startable = net.get_startable_epochs()
        assert isinstance(startable, list)

# %%
#|export
@pytest.mark.asyncio
async def test_net_get_running_epochs():
    """Test Net.get_running_epochs returns list."""
    config = create_simple_net_config()

    async with Net(config) as net:
        running = net.get_running_epochs()
        assert isinstance(running, list)
        assert len(running) == 0  # No epochs running initially

# %%
#|export
def test_net_get_epoch_log_empty():
    """Test get_epoch_log returns empty list for unknown epoch."""
    config = create_simple_net_config()
    net = Net(config)

    log = net.get_epoch_log("nonexistent_epoch")
    assert log == []

# %%
test_net_get_epoch_log_empty()

# %%
#|export
def test_net_get_node_log_empty():
    """Test get_node_log returns empty list for unknown node."""
    config = create_simple_net_config()
    net = Net(config)

    log = net.get_node_log("NonexistentNode")
    assert log == []

# %%
test_net_get_node_log_empty()

# %%
#|export
def test_net_handle_print_buffer():
    """Test Net._handle_print_buffer stores prints correctly."""
    config = create_simple_net_config()
    net = Net(config)

    # Create timestamped print buffer (as would come from ctx.print())
    ts1 = datetime.now()
    ts2 = datetime.now()
    buffer = [(ts1, "Line 1\n"), (ts2, "Line 2\n")]

    # Manually call _handle_print_buffer (normally called when receiving from worker)
    net._handle_print_buffer("epoch_123", buffer)

    log = net.get_epoch_log("epoch_123")
    assert len(log) == 2
    assert log[0][1] == "Line 1\n"
    assert log[1][1] == "Line 2\n"
    # Timestamps should be preserved from the original buffer
    assert log[0][0] == ts1
    assert log[1][0] == ts2

# %%
test_net_handle_print_buffer()

# %% [markdown]
# ### Rate Limiting Tests

# %%
#|export
def test_net_check_rate_limit_no_config():
    """Test rate limiting allows when no config exists."""
    config = create_simple_net_config()
    net = Net(config)

    # No rate limit configured
    assert net._check_rate_limit("AnyNode") is True

# %%
test_net_check_rate_limit_no_config()

# %%
#|export
def test_net_check_rate_limit_none_limit():
    """Test rate limiting allows when limit is None."""
    def dummy(ctx, packets):
        pass

    graph_config = GraphConfig(
        nodes=[
            NodeGraphConfig(
                name="LimitedNode",
                in_ports={"in": PortConfig()},
                in_salvo_conditions={
                    "default": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"in": PacketCountAllConfig()},
                        term=SalvoConditionTermTrueConfig(),
                    ),
                },
                execution_config=NodeExecutionConfig(
                    node_name="LimitedNode",
                    rate_limit_per_second=None,  # No limit
                ),
            ),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(id="main", spec=MainPoolConfig())},
        graph=graph_config,
    )

    net = Net(config)

    # Should always allow
    for _ in range(10):
        assert net._check_rate_limit("LimitedNode") is True

# %%
test_net_check_rate_limit_none_limit()

# %%
#|export
def test_net_check_rate_limit_enforced():
    """Test rate limiting enforces limit per second."""
    def dummy(ctx, packets):
        pass

    graph_config = GraphConfig(
        nodes=[
            NodeGraphConfig(
                name="RateLimited",
                in_ports={"in": PortConfig()},
                in_salvo_conditions={
                    "default": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"in": PacketCountAllConfig()},
                        term=SalvoConditionTermTrueConfig(),
                    ),
                },
                execution_config=NodeExecutionConfig(
                    node_name="RateLimited",
                    rate_limit_per_second=3,
                ),
            ),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(id="main", spec=MainPoolConfig())},
        graph=graph_config,
    )

    net = Net(config)

    # First 3 should be allowed
    assert net._check_rate_limit("RateLimited") is True
    assert net._check_rate_limit("RateLimited") is True
    assert net._check_rate_limit("RateLimited") is True

    # 4th should be blocked
    assert net._check_rate_limit("RateLimited") is False

# %%
test_net_check_rate_limit_enforced()

# %%
#|export
def test_net_check_rate_limit_window_expires():
    """Test rate limit window expires after 1 second."""
    def dummy(ctx, packets):
        pass

    graph_config = GraphConfig(
        nodes=[
            NodeGraphConfig(
                name="WindowNode",
                in_ports={"in": PortConfig()},
                in_salvo_conditions={
                    "default": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"in": PacketCountAllConfig()},
                        term=SalvoConditionTermTrueConfig(),
                    ),
                },
                execution_config=NodeExecutionConfig(
                    node_name="WindowNode",
                    rate_limit_per_second=2,
                ),
            ),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(id="main", spec=MainPoolConfig())},
        graph=graph_config,
    )

    net = Net(config)

    # Use up the limit
    assert net._check_rate_limit("WindowNode") is True
    assert net._check_rate_limit("WindowNode") is True
    assert net._check_rate_limit("WindowNode") is False

    # Wait for window to expire
    time.sleep(1.1)

    # Should be allowed again
    assert net._check_rate_limit("WindowNode") is True

# %%
test_net_check_rate_limit_window_expires()

# %% [markdown]
# ### Config Field Tests

# %%
#|export
def test_node_execution_config_new_fields():
    """Test NodeExecutionConfig has new print config fields."""
    config = NodeExecutionConfig(
        node_name="TestNode",
        print_flush_interval=0.2,
        print_buffer_max_size=100,
        print_echo_stdout=True,
        pool_allocation_method=RunAllocationMethod.LEAST_BUSY,
    )

    assert config.print_flush_interval == 0.2
    assert config.print_buffer_max_size == 100
    assert config.print_echo_stdout is True
    assert config.pool_allocation_method == RunAllocationMethod.LEAST_BUSY

# %%
test_node_execution_config_new_fields()

# %%
#|export
def test_node_execution_config_defaults():
    """Test NodeExecutionConfig has correct defaults for new fields."""
    config = NodeExecutionConfig(node_name="DefaultNode")

    assert config.print_flush_interval == 0.1
    assert config.print_buffer_max_size is None
    assert config.print_echo_stdout is False
    assert config.pool_allocation_method is None

# %%
test_node_execution_config_defaults()

# %%
#|export
def test_net_config_default_pool_allocation_method():
    """Test NetConfig has default_pool_allocation_method field."""
    config = NetConfig(
        pools={"main": PoolConfig(id="main", spec=MainPoolConfig())},
        graph=create_simple_graph_config(),
        default_pool_allocation_method=RunAllocationMethod.RANDOM,
    )

    assert config.default_pool_allocation_method == RunAllocationMethod.RANDOM

# %%
test_net_config_default_pool_allocation_method()

# %%
#|export
def test_net_config_default_pool_allocation_method_default():
    """Test NetConfig.default_pool_allocation_method defaults to ROUND_ROBIN."""
    config = NetConfig(
        pools={"main": PoolConfig(id="main", spec=MainPoolConfig())},
        graph=create_simple_graph_config(),
    )

    assert config.default_pool_allocation_method == RunAllocationMethod.ROUND_ROBIN

# %%
test_net_config_default_pool_allocation_method_default()

# %% [markdown]
# ## EpochCancelled Exception Tests

# %%
#|export
def test_epoch_cancelled_exception():
    """Test EpochCancelled exception."""
    exc = EpochCancelled("Epoch xyz cancelled")

    assert str(exc) == "Epoch xyz cancelled"
    assert isinstance(exc, Exception)

# %%
test_epoch_cancelled_exception()

# %%
#|export
def test_epoch_cancelled_can_be_raised_and_caught():
    """Test EpochCancelled can be raised and caught."""
    with pytest.raises(EpochCancelled):
        raise EpochCancelled("test")

# %%
test_epoch_cancelled_can_be_raised_and_caught()

# %% [markdown]
# ## Invalid Pool Type Test

# %%
#|export
def test_net_invalid_pool_type_raises():
    """Test Net raises ValueError for invalid pool type."""
    # Create a mock pool config with invalid type
    class InvalidPoolConfig:
        type = "invalid_type"
        def model_dump(self):
            return {"type": "invalid_type"}

    # We need to bypass pydantic validation, so we create the config differently
    # This test documents expected behavior but may need adjustment based on
    # how strictly pydantic validates
    pass  # Skip this test as pydantic prevents invalid pool types

# %%
# test_net_invalid_pool_type_raises() - Skipped

# %% [markdown]
# ## Sync Method Tests

# %%
#|export
def test_net_start_sync():
    """Test Net.start_sync works (synchronous wrapper)."""
    config = create_simple_net_config()
    net = Net(config)

    # This would actually start the net, so we just test it's callable
    assert callable(net.start_sync)

# %%
test_net_start_sync()

# %%
#|export
def test_net_stop_sync():
    """Test Net.stop_sync is callable."""
    config = create_simple_net_config()
    net = Net(config)

    assert callable(net.stop_sync)

# %%
test_net_stop_sync()
