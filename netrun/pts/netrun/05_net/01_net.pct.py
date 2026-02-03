# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp net._net

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %% [markdown]
# # Net Class
#
# The `Net` class is the main orchestrator that:
# 1. Wraps `netrun-sim` to manage packet flow simulation
# 2. Executes node functions via `ExecutionManager`
# 3. Manages packet values in `PacketStore`
# 4. Handles errors, retries, and print capture

# %%
#|export
import asyncio
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, NoReturn
from collections.abc import Callable

import netrun_sim
from netrun.pool.thread import ThreadPool
from netrun.pool.multiprocess import MultiprocessPool
from netrun.pool.aio import SingleWorkerPool
from netrun.pool.remote import RemotePoolClient
from netrun.net.config import NetConfig, NodeExecutionConfig
from netrun.execution_manager import ExecutionManager, PoolType, RunAllocationMethod
from netrun.rpc.base import SyncRPCChannel
from netrun._iutils import get_timestamp_utc
from netrun.storage import PacketStore, PacketStoreConfig, LazyPacketValueSpec

# %% [markdown]
# ## Net Protocol Keys
#
# Communication keys for Net <-> Worker messages.

# %%
#|export
class NetProtocolKeys(Enum):
    """Protocol keys for Net <-> Worker communication."""

    # Upstream (Worker -> Net) - sent via channel when send_channel=True
    UP_CREATE_PACKET = "net:create-packet"
    """Create a new packet. Args: (epoch_id, value_or_lazy)"""

    UP_CREATE_PACKET_RESPONSE = "net:create-packet-response"
    """Response with packet ID. Args: (packet_id,)"""

    UP_CONSUME_PACKET = "net:consume-packet"
    """Consume a packet. Args: (epoch_id, packet_id)"""

    UP_CONSUME_PACKET_RESPONSE = "net:consume-packet-response"
    """Response with packet value. Args: (value,)"""

    UP_LOAD_OUTPUT_PORT = "net:load-output-port"
    """Load packet into output port. Args: (epoch_id, port_name, packet_id)"""

    UP_LOAD_OUTPUT_PORT_RESPONSE = "net:load-output-port-response"
    """Acknowledgement. Args: ()"""

    UP_SEND_OUTPUT_SALVO = "net:send-salvo"
    """Send output salvo. Args: (epoch_id, salvo_condition_name)"""

    UP_SEND_OUTPUT_SALVO_RESPONSE = "net:send-salvo-response"
    """Acknowledgement. Args: ()"""

    UP_CANCEL_EPOCH = "net:cancel-epoch"
    """Cancel the current epoch. Args: (epoch_id,)"""

    UP_PRINT_BUFFER = "net:print-buffer"
    """Send captured print output. Args: (epoch_id, buffer: list[str])"""

# %% [markdown]
# ## NodeExecutionContext
#
# The context object passed to `exec_node_func(ctx, packets)`.

# %%
#|export
class EpochCancelled(Exception):
    """Raised when an epoch is cancelled via ctx.cancel_epoch()."""
    pass


@dataclass
class NodeExecutionContext:
    """Context object passed to node execution functions.

    This is the primary interface for nodes to interact with the Net during execution.
    All operations are synchronous and block until complete.
    """

    # Identity
    epoch_id: str
    node_name: str

    # Retry info
    retry_count: int = 0
    retry_timestamps: list[datetime] = field(default_factory=list)
    retry_exceptions: list[Exception] = field(default_factory=list)

    # Internal (not for user access)
    _channel: SyncRPCChannel = field(repr=False, default=None)
    _config: NodeExecutionConfig = field(repr=False, default=None)
    _print_buffer: list[tuple[datetime, str]] = field(default_factory=list, repr=False)
    _last_print_flush: float = field(default_factory=time.time, repr=False)
    _created_packets: list[str] = field(default_factory=list, repr=False)
    _consumed_packets: list[str] = field(default_factory=list, repr=False)

    def create_packet(self, value: Any) -> str:
        """Create a new packet with the given value.

        Args:
            value: The value to store in the packet.

        Returns:
            The packet ID (or a deferred ID if defer_net_actions=True).
        """
        self._channel.send(
            NetProtocolKeys.UP_CREATE_PACKET.value,
            (self.epoch_id, value)
        )

        key, data = self._channel.recv()
        if key != NetProtocolKeys.UP_CREATE_PACKET_RESPONSE.value:
            raise RuntimeError(f"Expected {NetProtocolKeys.UP_CREATE_PACKET_RESPONSE.value}, got {key}")

        packet_id = data
        self._created_packets.append(packet_id)
        return packet_id

    def create_packet_from_value_func(
        self,
        func_import_path: str,
        args: tuple = (),
        kwargs: dict | None = None,
    ) -> str:
        """Create a packet with a lazy value that is evaluated on consumption.

        Args:
            func_import_path: Import path to the function that produces the value.
            args: Positional arguments to pass to the function.
            kwargs: Keyword arguments to pass to the function.

        Returns:
            The packet ID.
        """
        if kwargs is None:
            kwargs = {}

        lazy_spec = LazyPacketValueSpec(
            func_import_path=func_import_path,
            args=args,
            kwargs=kwargs,
        )

        self._channel.send(
            NetProtocolKeys.UP_CREATE_PACKET.value,
            (self.epoch_id, lazy_spec)
        )

        key, data = self._channel.recv()
        if key != NetProtocolKeys.UP_CREATE_PACKET_RESPONSE.value:
            raise RuntimeError(f"Expected {NetProtocolKeys.UP_CREATE_PACKET_RESPONSE.value}, got {key}")

        packet_id = data
        self._created_packets.append(packet_id)
        return packet_id

    def consume_packet(self, packet_id: str) -> Any:
        """Consume a packet and return its value.

        The packet is removed from the network.

        Args:
            packet_id: The ID of the packet to consume.

        Returns:
            The packet's value.
        """
        self._channel.send(
            NetProtocolKeys.UP_CONSUME_PACKET.value,
            (self.epoch_id, packet_id)
        )

        key, data = self._channel.recv()
        if key != NetProtocolKeys.UP_CONSUME_PACKET_RESPONSE.value:
            raise RuntimeError(f"Expected {NetProtocolKeys.UP_CONSUME_PACKET_RESPONSE.value}, got {key}")

        self._consumed_packets.append(packet_id)
        return data

    def load_output_port(self, port_name: str, packet_id: str) -> None:
        """Load a packet into an output port.

        Args:
            port_name: The name of the output port.
            packet_id: The ID of the packet to load.
        """
        self._channel.send(
            NetProtocolKeys.UP_LOAD_OUTPUT_PORT.value,
            (self.epoch_id, port_name, packet_id)
        )

        key, data = self._channel.recv()
        if key != NetProtocolKeys.UP_LOAD_OUTPUT_PORT_RESPONSE.value:
            raise RuntimeError(f"Expected {NetProtocolKeys.UP_LOAD_OUTPUT_PORT_RESPONSE.value}, got {key}")

    def send_output_salvo(self, salvo_condition_name: str) -> None:
        """Send packets from output ports onto edges.

        Args:
            salvo_condition_name: The name of the output salvo condition to trigger.
        """
        self._channel.send(
            NetProtocolKeys.UP_SEND_OUTPUT_SALVO.value,
            (self.epoch_id, salvo_condition_name)
        )

        key, data = self._channel.recv()
        if key != NetProtocolKeys.UP_SEND_OUTPUT_SALVO_RESPONSE.value:
            raise RuntimeError(f"Expected {NetProtocolKeys.UP_SEND_OUTPUT_SALVO_RESPONSE.value}, got {key}")

    def cancel_epoch(self) -> NoReturn:
        """Cancel the current epoch.

        This immediately terminates the epoch execution.
        Raises EpochCancelled which should not be caught by user code.
        """
        self._channel.send(
            NetProtocolKeys.UP_CANCEL_EPOCH.value,
            (self.epoch_id,)
        )
        raise EpochCancelled(f"Epoch {self.epoch_id} cancelled")

    def print(self, *args, sep: str = " ", end: str = "\n", flush: bool = False) -> None:
        """Capture print output with periodic flushing.

        This method behaves like the built-in print() but captures output
        and periodically sends it back to the Net. Each print is timestamped
        at the time it is called.

        Args:
            *args: Values to print (same as builtin print).
            sep: Separator between values (default: " ").
            end: String to append at end (default: "\\n").
            flush: If True, immediately flush buffer to Net.
        """
        # Capture timestamp immediately when print is called
        timestamp = get_timestamp_utc()

        # Format the message (same as builtin print)
        message = sep.join(str(arg) for arg in args) + end

        # Optionally echo to stdout
        if self._config is not None and self._config.print_echo_stdout:
            import builtins
            builtins.print(*args, sep=sep, end=end, flush=True)

        # Add to buffer with timestamp
        self._print_buffer.append((timestamp, message))

        # Check if we should flush
        now = time.time()
        flush_interval = self._config.print_flush_interval if self._config else 0.1
        time_threshold_exceeded = (now - self._last_print_flush) >= flush_interval

        buffer_max_size = self._config.print_buffer_max_size if self._config else None
        buffer_size_exceeded = (
            buffer_max_size is not None and
            len(self._print_buffer) >= buffer_max_size
        )

        should_flush = flush or time_threshold_exceeded or buffer_size_exceeded

        if should_flush and self._print_buffer:
            self._flush_print_buffer()

    def _flush_print_buffer(self) -> None:
        """Send buffered prints to Net via channel."""
        if not self._print_buffer:
            return

        buffer = self._print_buffer.copy()
        self._print_buffer.clear()
        self._last_print_flush = time.time()

        self._channel.send(
            NetProtocolKeys.UP_PRINT_BUFFER.value,
            (self.epoch_id, buffer)
        )
        # Note: Print buffer sends don't require a response

# %% [markdown]
# ## NodeFailureContext
#
# Context passed to `on_node_failure` callbacks.

# %%
#|export
@dataclass
class NodeFailureContext:
    """Context object passed to on_node_failure callbacks."""
    epoch_id: str
    node_name: str
    retry_count: int
    exception: Exception
    retry_timestamps: list[datetime] = field(default_factory=list)
    retry_exceptions: list[Exception] = field(default_factory=list)
    input_salvo: dict[str, list[str]] = field(default_factory=dict)  # port_name -> list[packet_id]

# %% [markdown]
# ## Deferred Action Queue
#
# For `defer_net_actions=True`, packet operations are queued and only committed on success.

# %%
#|export
@dataclass
class DeferredActionQueue:
    """Queue of net actions to be committed on success or discarded on failure."""

    actions: list[tuple[str, Any]] = field(default_factory=list)  # (action_type, args)
    packet_values: dict[str, Any] = field(default_factory=dict)   # deferred_id -> value
    deferred_to_real_ids: dict[str, str] = field(default_factory=dict)

    def add_create_packet(self, value: Any) -> str:
        """Queue a packet creation. Returns deferred ID."""
        deferred_id = f"deferred_{uuid.uuid4()}"
        self.actions.append(("create_packet", (deferred_id, value)))
        self.packet_values[deferred_id] = value
        return deferred_id

    def add_consume_packet(self, packet_id: str) -> None:
        """Queue a packet consumption."""
        self.actions.append(("consume_packet", (packet_id,)))

    def add_load_output_port(self, port_name: str, packet_id: str) -> None:
        """Queue loading a packet into an output port."""
        self.actions.append(("load_output_port", (port_name, packet_id)))

    def add_send_output_salvo(self, salvo_condition_name: str) -> None:
        """Queue sending an output salvo."""
        self.actions.append(("send_output_salvo", (salvo_condition_name,)))

    def commit(self, netsim: netrun_sim.NetSim, packet_store: PacketStore, epoch_id: str) -> dict[str, str]:
        """Commit all actions. Returns deferred_id -> real_id mapping."""
        from ulid import ULID

        for action_type, args in self.actions:
            if action_type == "create_packet":
                deferred_id, value = args
                real_id = str(ULID())
                self.deferred_to_real_ids[deferred_id] = real_id
                packet_store.register(real_id, value)
                netsim.do_action(netrun_sim.NetAction.create_packet(epoch_id))
            elif action_type == "consume_packet":
                packet_id, = args
                # Map deferred IDs to real IDs if needed
                real_packet_id = self.deferred_to_real_ids.get(packet_id, packet_id)
                packet_store.consume(real_packet_id)
                netsim.do_action(netrun_sim.NetAction.consume_packet(real_packet_id))
            elif action_type == "load_output_port":
                port_name, packet_id = args
                real_packet_id = self.deferred_to_real_ids.get(packet_id, packet_id)
                netsim.do_action(netrun_sim.NetAction.load_packet_into_output_port(real_packet_id, port_name))
            elif action_type == "send_output_salvo":
                salvo_condition_name, = args
                netsim.do_action(netrun_sim.NetAction.send_output_salvo(epoch_id, salvo_condition_name))

        return self.deferred_to_real_ids

    def discard(self) -> None:
        """Discard all queued actions (on failure/retry)."""
        self.actions.clear()
        self.packet_values.clear()
        self.deferred_to_real_ids.clear()

# %% [markdown]
# ## func_preprocessor and func_done_callback
#
# These functions transform node functions to accept context-creation arguments
# and handle final buffer flush.

# %%
#|export
def create_net_func_preprocessor(node_execution_configs: dict[str, NodeExecutionConfig]) -> Callable:
    """Create a func_preprocessor for Net execution.

    The preprocessor transforms `exec_node_func(ctx, packets)` into a wrapped function
    that accepts context-creation arguments and creates the NodeExecutionContext locally
    in the worker.

    Args:
        node_execution_configs: Mapping of node names to their execution configs.

    Returns:
        A preprocessor function.
    """
    def preprocessor(exec_node_func: Callable) -> Callable:
        """Transform exec_node_func(ctx, packets) -> wrapped(channel, epoch_id, node_name, packets, ...)"""

        def wrapped(
            channel: SyncRPCChannel,
            epoch_id: str,
            node_name: str,
            packets: dict[str, list[str]],
            retry_count: int = 0,
            retry_timestamps: list[datetime] | None = None,
            retry_exceptions: list[Exception] | None = None,
        ):
            config = node_execution_configs.get(node_name)

            ctx = NodeExecutionContext(
                epoch_id=epoch_id,
                node_name=node_name,
                retry_count=retry_count,
                retry_timestamps=retry_timestamps or [],
                retry_exceptions=retry_exceptions or [],
                _channel=channel,
                _config=config,
            )

            try:
                result = exec_node_func(ctx, packets)
                return result
            finally:
                # Always flush remaining buffer
                ctx._flush_print_buffer()

        return wrapped

    return preprocessor


def create_net_func_done_callback() -> Callable:
    """Create func_done_callback that handles post-execution cleanup.

    The callback is called after function execution with the same args/kwargs
    that were passed to the function, plus the result.

    Returns:
        A done callback function.
    """
    def callback(
        channel: SyncRPCChannel,
        epoch_id: str,
        node_name: str,
        packets: dict[str, list[str]],
        retry_count: int = 0,
        retry_timestamps: list[datetime] | None = None,
        retry_exceptions: list[Exception] | None = None,
        *,
        result=None,
    ):
        # The wrapped function already handles the final flush in its finally block.
        # This callback is available for additional cleanup if needed.
        pass

    return callback

# %% [markdown]
# ## Net Class

# %%
#|export
class Net:
    """Main orchestrator for flow-based network execution.

    The Net class bridges netrun-sim (packet flow simulation) with actual node
    function execution via ExecutionManager.
    """

    def __init__(self, config: NetConfig):
        """Initialize the Net with the given configuration.

        Args:
            config: The NetConfig defining pools, graph, and execution settings.
        """
        self._config: NetConfig = config
        self._graph: netrun_sim.Graph = self.config.graph.get_graph()
        self._netsim = netrun_sim.NetSim(self._graph)
        self._started: bool = False
        self._paused: bool = False
        self._stopping: bool = False

        # Packet value storage
        self._packet_store = PacketStore(PacketStoreConfig())

        # Print log storage
        self._epoch_print_logs: dict[str, list[tuple[datetime, str]]] = {}
        self._node_print_logs: dict[str, list[tuple[datetime, str]]] = {}

        # Rate limiting tracking
        self._epoch_start_times: dict[str, list[float]] = {}  # node_name -> list of timestamps

        # Running epoch tracking
        self._running_epochs: set[str] = set()

        # Build node execution configs lookup
        self._node_execution_configs: dict[str, NodeExecutionConfig] = {}
        for node_config in self.config.graph.nodes:
            if node_config.execution_config is not None:
                self._node_execution_configs[node_config.name] = node_config.execution_config

        # Create func_preprocessor with node configs
        func_preprocessor = create_net_func_preprocessor(self._node_execution_configs)
        func_done_callback = create_net_func_done_callback()

        # Build ExecutionManager config
        _exec_manager_config = {}
        for pool_name, pool_config in self.config.pools.items():
            match pool_config.spec.type:
                case "main":
                    pool_type = SingleWorkerPool
                case "thread":
                    pool_type = ThreadPool
                case "multiprocess":
                    pool_type = MultiprocessPool
                case "remote":
                    pool_type = RemotePoolClient
                case _:
                    raise ValueError(f"Invalid pool type: {pool_config.spec.type}")

            _init_kwargs = pool_config.spec.model_dump()
            _init_kwargs.pop("type")
            _init_kwargs["func_preprocessor"] = func_preprocessor
            _init_kwargs["func_done_callback"] = func_done_callback
            _exec_manager_config[pool_name] = (pool_type, _init_kwargs)

        self._execution_manager = ExecutionManager(_exec_manager_config)

        # Background task for main loop
        self._background_task: asyncio.Task | None = None

    @property
    def config(self) -> NetConfig:
        """Get the Net configuration."""
        return self._config

    @property
    def graph(self) -> netrun_sim.Graph:
        """Get the netrun-sim Graph."""
        return self._graph

    @property
    def netsim(self) -> netrun_sim.NetSim:
        """Get the netrun-sim NetSim instance."""
        return self._netsim

    @property
    def pools(self) -> list[tuple[str, type[PoolType]]]:
        """Get list of (pool_id, pool_type) tuples."""
        return self._execution_manager.pools

    @property
    def started(self) -> bool:
        """Check if the Net has been started."""
        return self._started

    @property
    def paused(self) -> bool:
        """Check if the Net is paused."""
        return self._paused

    def _get_node_execution_config(self, node_name: str) -> NodeExecutionConfig | None:
        """Get the execution config for a node."""
        return self._node_execution_configs.get(node_name)

    def _check_rate_limit(self, node_name: str) -> bool:
        """Check if node can start a new epoch based on rate limit.

        Rate limit is global across all pools for the node.

        Args:
            node_name: The name of the node.

        Returns:
            True if the node can start a new epoch, False otherwise.
        """
        config = self._get_node_execution_config(node_name)
        if config is None or config.rate_limit_per_second is None:
            return True

        now = time.time()
        window_start = now - 1.0  # 1 second window

        # Clean old entries
        self._epoch_start_times[node_name] = [
            t for t in self._epoch_start_times.get(node_name, [])
            if t > window_start
        ]

        # Check limit
        if len(self._epoch_start_times[node_name]) >= config.rate_limit_per_second:
            return False

        # Record this start
        self._epoch_start_times.setdefault(node_name, []).append(now)
        return True

    def _handle_print_buffer(self, epoch_id: str, buffer: list[tuple[datetime, str]]) -> None:
        """Handle print buffer received from a worker.

        Args:
            epoch_id: The epoch that sent the prints.
            buffer: List of (timestamp, message) tuples. Timestamps are captured
                    at the time ctx.print() was called in the worker.
        """
        # Store by epoch
        if epoch_id not in self._epoch_print_logs:
            self._epoch_print_logs[epoch_id] = []
        for timestamp, line in buffer:
            self._epoch_print_logs[epoch_id].append((timestamp, line))

        # Also store by node (get node_name from epoch)
        try:
            epoch = self._netsim.get_epoch(epoch_id)
            if epoch is not None:
                node_name = epoch.node_name
                if node_name not in self._node_print_logs:
                    self._node_print_logs[node_name] = []
                for timestamp, line in buffer:
                    self._node_print_logs[node_name].append((timestamp, line))
        except (ValueError, KeyError):
            # Epoch not found or invalid ID - skip node-level logging
            pass

    def get_epoch_log(self, epoch_id: str) -> list[tuple[datetime, str]]:
        """Get print output for a specific epoch.

        Args:
            epoch_id: The epoch ID.

        Returns:
            List of (timestamp, message) tuples.
        """
        return list(self._epoch_print_logs.get(epoch_id, []))

    def get_node_log(self, node_name: str) -> list[tuple[datetime, str]]:
        """Get all print output for a node (across all epochs).

        Args:
            node_name: The node name.

        Returns:
            List of (timestamp, message) tuples.
        """
        return list(self._node_print_logs.get(node_name, []))

    async def start(self) -> None:
        """Start the Net.

        This starts the ExecutionManager and all pools.
        """
        if self._started:
            raise RuntimeError("Net already started")

        await self._execution_manager.start()
        self._started = True

    def start_sync(self) -> None:
        """Start the Net synchronously.

        Blocking wrapper for start().
        """
        asyncio.run(self.start())

    async def stop(self) -> None:
        """Stop the Net gracefully.

        This stops the background task and closes the ExecutionManager.
        """
        self._stopping = True

        # Cancel background task if running
        if self._background_task is not None:
            self._background_task.cancel()
            try:
                await self._background_task
            except asyncio.CancelledError:
                pass
            self._background_task = None

        await self._execution_manager.close()
        self._started = False
        self._stopping = False

    def stop_sync(self) -> None:
        """Stop the Net synchronously.

        Blocking wrapper for stop().
        """
        asyncio.run(self.stop())

    async def pause(self) -> None:
        """Pause the Net.

        Finish running epochs but don't start new ones.
        """
        self._paused = True

    async def resume(self) -> None:
        """Resume the Net after pausing."""
        self._paused = False

    async def run_step(self) -> tuple[bool, list]:
        """Run one simulation step.

        This moves packets through the network and checks for startable epochs.

        Returns:
            Tuple of (made_progress, events) where:
            - made_progress: True if any packets were moved
            - events: List of NetEvents that occurred during the step
        """
        result = self._netsim.run_step()
        # netrun-sim returns (bool, events)
        if isinstance(result, tuple):
            made_progress, events = result
            return (made_progress, list(events) if not isinstance(events, list) else events)
        # Fallback for direct list return
        events = list(result) if not isinstance(result, list) else result
        return (len(events) > 0, events)

    async def run_until_blocked(self) -> list:
        """Run the simulation until no more progress can be made.

        Returns:
            All NetEvents that occurred.
        """
        all_events = []
        while True:
            made_progress, events = await self.run_step()
            all_events.extend(events)
            if not made_progress:
                break
        return all_events

    def get_startable_epochs(self) -> list[str]:
        """Get list of epoch IDs that are ready to start."""
        epochs = self._netsim.get_startable_epochs()
        return list(epochs) if not isinstance(epochs, list) else epochs

    def get_running_epochs(self) -> list[str]:
        """Get list of currently running epoch IDs."""
        return list(self._running_epochs)

    async def __aenter__(self) -> "Net":
        """Context manager entry - starts the Net."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - stops the Net."""
        await self.stop()

# %% [markdown]
# ## Example: Net with Multiple Pool Types
#
# This example demonstrates creating a Net with all four pool types:
# - **main**: Runs in the main event loop (SingleWorkerPool)
# - **thread**: Runs in worker threads (ThreadPool)
# - **multiprocess**: Runs in separate processes (MultiprocessPool)
# - **remote**: Runs on a remote server (RemotePoolClient)

# %%
#|eval: false
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
    SalvoConditionTermPortConfig,
    MaxSalvosFiniteConfig,
    PacketCountAllConfig,
    PortStateNonEmptyConfig,
)

# Define node execution functions
def source_node(ctx: NodeExecutionContext, packets: dict[str, list[str]]):
    """Source node that creates initial packets."""
    ctx.print(f"Source node executing in epoch {ctx.epoch_id}")
    # Create an output packet
    packet_id = ctx.create_packet({"message": "Hello from source!"})
    ctx.load_output_port("out", packet_id)
    ctx.send_output_salvo("send")
    ctx.print("Source node completed")

def processor_node(ctx: NodeExecutionContext, packets: dict[str, list[str]]):
    """Processor node that transforms packets."""
    ctx.print(f"Processor node executing in epoch {ctx.epoch_id}")
    # Consume input packets
    for packet_id in packets.get("in", []):
        value = ctx.consume_packet(packet_id)
        ctx.print(f"Processing: {value}")
        # Create transformed output
        transformed = {"original": value, "processed": True}
        out_id = ctx.create_packet(transformed)
        ctx.load_output_port("out", out_id)
    ctx.send_output_salvo("send")
    ctx.print("Processor node completed")

def sink_node(ctx: NodeExecutionContext, packets: dict[str, list[str]]):
    """Sink node that consumes final packets."""
    ctx.print(f"Sink node executing in epoch {ctx.epoch_id}")
    for packet_id in packets.get("in", []):
        value = ctx.consume_packet(packet_id)
        ctx.print(f"Sink received: {value}")
    ctx.print("Sink node completed")

# %% [markdown]
# ### Create the Net Configuration

# %%
#|eval: false
# Define pools - one of each type (excluding remote for this example)
pools = {
    "main_pool": PoolConfig(
        id="main_pool",
        spec=MainPoolConfig(),
    ),
    "thread_pool": PoolConfig(
        id="thread_pool",
        spec=ThreadPoolConfig(num_workers=2),
    ),
    "process_pool": PoolConfig(
        id="process_pool",
        spec=MultiprocessPoolConfig(num_processes=1, threads_per_process=2),
    ),
}

# Define a simple linear graph: Source -> Processor -> Sink
graph_config = GraphConfig(
    nodes=[
        # Source node (runs on main pool)
        NodeGraphConfig(
            name="Source",
            out_ports={"out": PortConfig()},
            out_salvo_conditions={
                "send": SalvoConditionConfig(
                    max_salvos=MaxSalvosFiniteConfig(max=1),
                    ports={"out": PacketCountAllConfig()},
                    term=SalvoConditionTermPortConfig(
                        port_name="out",
                        state=PortStateNonEmptyConfig(),
                    ),
                ),
            },
            execution_config=NodeExecutionConfig(
                node_name="Source",
                pools=["main_pool"],
                exec_node_func=source_node,
                print_echo_stdout=True,  # Echo prints to stdout
            ),
        ),
        # Processor node (runs on thread pool)
        NodeGraphConfig(
            name="Processor",
            in_ports={"in": PortConfig()},
            out_ports={"out": PortConfig()},
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
            out_salvo_conditions={
                "send": SalvoConditionConfig(
                    max_salvos=MaxSalvosFiniteConfig(max=1),
                    ports={"out": PacketCountAllConfig()},
                    term=SalvoConditionTermPortConfig(
                        port_name="out",
                        state=PortStateNonEmptyConfig(),
                    ),
                ),
            },
            execution_config=NodeExecutionConfig(
                node_name="Processor",
                pools=["thread_pool"],
                exec_node_func=processor_node,
                print_flush_interval=0.05,  # Flush prints every 50ms
            ),
        ),
        # Sink node (runs on multiprocess pool)
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
            execution_config=NodeExecutionConfig(
                node_name="Sink",
                pools=["process_pool"],
                exec_node_func=sink_node,
            ),
        ),
    ],
    edges=[
        EdgeConfig(source_str="Source.out", target_str="Processor.in"),
        EdgeConfig(source_str="Processor.out", target_str="Sink.in"),
    ],
)

# Create the Net configuration
net_config = NetConfig(
    pools=pools,
    graph=graph_config,
)

print("Net configuration created successfully!")
print(f"Pools: {list(net_config.pools.keys())}")
print(f"Nodes: {[n.name for n in net_config.graph.nodes]}")

# %% [markdown]
# ### Create and Start the Net

# %%
#|eval: false
# Create the Net
net = Net(net_config)

print(f"Net created with graph: {net.graph}")
print(f"Started: {net.started}")

# %% [markdown]
# ### Run the Net (async context)

# %%
#|eval: false
async def run_example():
    """Run the example Net."""
    async with Net(net_config) as net:
        print("Net started!")
        print(f"Pools: {net.pools}")

        # Run simulation steps
        events = await net.run_until_blocked()
        print(f"Events after run_until_blocked: {len(events)}")

        # Check for startable epochs
        startable = net.get_startable_epochs()
        print(f"Startable epochs: {startable}")

        # Get print logs (will be populated after epochs execute)
        source_log = net.get_node_log("Source")
        processor_log = net.get_node_log("Processor")
        sink_log = net.get_node_log("Sink")

        print(f"\nSource logs: {len(source_log)} entries")
        print(f"Processor logs: {len(processor_log)} entries")
        print(f"Sink logs: {len(sink_log)} entries")

        print("\nNet stopping...")

    print("Net stopped!")

# Run the example
# asyncio.run(run_example())

# %% [markdown]
# ### Example with Remote Pool
#
# To use a remote pool, you need to run a RemotePoolServer:
#
# ```python
# # On the server machine:
# from netrun.execution_manager import create_execution_manager_server
#
# server = create_execution_manager_server(worker_name="net_worker")
# await server.serve("0.0.0.0", 8765)
#
# # On the client:
# from netrun.net.config import RemotePoolConfig
#
# pools = {
#     "remote_pool": PoolConfig(
#         id="remote_pool",
#         spec=RemotePoolConfig(
#             url="ws://server-hostname:8765",
#             worker_name="net_worker",
#             num_processes=2,
#             threads_per_process=4,
#         ),
#     ),
# }
# ```
