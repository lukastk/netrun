# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # The Net Class
#
# The main `Net` class wraps `netrun-sim`'s `NetSim` and provides the high-level API
# for running flow-based networks.

# %%
#|default_exp net

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();

# %%
#|export
import asyncio
import inspect
import time
from typing import Any, Callable, Dict, List, Optional, Union
from pathlib import Path
from enum import Enum, auto
from datetime import datetime

# Re-export graph types from netrun_sim
from netrun_sim import (
    Graph,
    Node,
    Edge,
    Port,
    PortType,
    PortRef,
    PortSlotSpec,
    PortState,
    PacketCount,
    MaxSalvos,
    SalvoCondition,
    SalvoConditionTerm,
    NetSim,
    NetAction,
    NetEvent,
    NetActionResponseData,
    Packet,
    PacketLocation,
    Epoch,
    EpochState,
    Salvo,
    NodeNotFoundError,
)

from netrun.errors import (
    NetrunRuntimeError,
    NodeExecutionFailed,
    EpochTimeout,
    EpochCancelled,
    NetNotPausedError,
)
from netrun.storage import PacketValueStore
from netrun.config import NodeConfig, NodeExecFuncs
from netrun.dlq import DeadLetterQueue, DeadLetterEntry
from netrun.deferred import (
    DeferredPacket,
    DeferredAction,
    DeferredActionType,
    DeferredActionQueue,
)
from netrun.context import NodeExecutionContext, NodeFailureContext


class NetState(Enum):
    """The current state of the Net."""
    CREATED = auto()      # Net created but not started
    RUNNING = auto()      # Net is actively running
    PAUSED = auto()       # Net is paused (can resume)
    STOPPED = auto()      # Net is stopped (cannot resume)

# %% [markdown]
# ## Helper Functions
#
# These functions are used internally by the Net class.

# %%
#|export
def _is_async_func(func: Optional[Callable]) -> bool:
    """Check if a function is async (coroutine function)."""
    if func is None:
        return False
    return asyncio.iscoroutinefunction(func) or inspect.iscoroutinefunction(func)


def _commit_deferred_actions(
    net: "Net",
    epoch_id: str,
    queue: DeferredActionQueue,
) -> dict[str, Packet]:
    """
    Commit all deferred actions to NetSim.

    Returns a mapping from deferred_id to real Packet.
    """
    # Map from deferred_id to real packet
    resolved_packets: dict[str, Packet] = {}

    for action in queue.actions:
        if action.action_type == DeferredActionType.CREATE_PACKET:
            # Create the packet
            net_action = NetAction.create_packet(epoch_id)
            response_data, _ = net._sim.do_action(net_action)

            # Get the packet ID from response data
            packet_id = response_data.packet_id

            # Store the value
            net._value_store.store_value(packet_id, action.value)

            # Resolve the deferred packet
            real_packet = net._sim.get_packet(packet_id)
            if action.deferred_packet is not None:
                action.deferred_packet._resolve(real_packet)
                resolved_packets[action.deferred_packet.deferred_id] = real_packet

        elif action.action_type == DeferredActionType.CREATE_PACKET_FROM_FUNC:
            # Create the packet
            net_action = NetAction.create_packet(epoch_id)
            response_data, _ = net._sim.do_action(net_action)

            # Get the packet ID from response data
            packet_id = response_data.packet_id

            # Store the value function
            net._value_store.store_value_func(packet_id, action.value_func)

            # Resolve the deferred packet
            real_packet = net._sim.get_packet(packet_id)
            if action.deferred_packet is not None:
                action.deferred_packet._resolve(real_packet)
                resolved_packets[action.deferred_packet.deferred_id] = real_packet

        elif action.action_type == DeferredActionType.CONSUME_PACKET:
            # Consume was already done for value retrieval, just commit to NetSim
            packet = action.packet
            if isinstance(packet, DeferredPacket):
                if not packet.is_resolved:
                    raise RuntimeError("Trying to consume unresolved deferred packet on commit")
                packet_id = packet.id
            else:
                packet_id = packet.id

            net_action = NetAction.consume_packet(packet_id)
            net._sim.do_action(net_action)

        elif action.action_type == DeferredActionType.LOAD_OUTPUT_PORT:
            packet = action.packet
            if isinstance(packet, DeferredPacket):
                if not packet.is_resolved:
                    raise RuntimeError("Trying to load unresolved deferred packet on commit")
                packet_id = packet.id
            else:
                packet_id = packet.id

            net_action = NetAction.load_packet_into_output_port(packet_id, action.port_name)
            net._sim.do_action(net_action)

        elif action.action_type == DeferredActionType.SEND_OUTPUT_SALVO:
            net_action = NetAction.send_output_salvo(epoch_id, action.salvo_condition_name)
            net._sim.do_action(net_action)

    return resolved_packets


def _unconsume_packets_for_retry(
    net: "Net",
    consumed_values: dict[str, Any],
) -> None:
    """
    Restore consumed packet values for retry.

    Called when an epoch fails and will be retried.
    """
    for packet_id, value in consumed_values.items():
        net._value_store.unconsume(packet_id, value)

# %% [markdown]
# ## Net Class

# %%
#|export
class Net:
    """
    High-level runtime for flow-based development graphs.

    Wraps `netrun-sim`'s `NetSim` to provide:
    - Actual node execution logic
    - Packet value storage
    - Configuration and control methods

    The underlying `NetSim` is hidden from users - all interactions
    go through this class's methods.
    """

    def __init__(
        self,
        graph: Graph,
        *,
        # Packet storage
        consumed_packet_storage: bool = False,
        consumed_packet_storage_limit: Optional[int] = None,
        packet_storage_path: Optional[Union[str, Path]] = None,
        # Pools
        thread_pools: Optional[Dict[str, dict]] = None,
        process_pools: Optional[Dict[str, dict]] = None,
        # Error handling
        on_error: str = "pause",  # "continue", "pause", "raise"
        error_callback: Optional[Callable] = None,
        # Dead letter queue
        dead_letter_queue: str = "memory",  # "memory", "file", or callback
        dead_letter_path: Optional[Union[str, Path]] = None,
        dead_letter_callback: Optional[Callable] = None,
        # History
        history_max_size: Optional[int] = None,
        history_file: Optional[Union[str, Path]] = None,
        history_chunk_size: int = 100,
        history_flush_on_pause: bool = True,
    ):
        """
        Create a new Net from a graph.

        Args:
            graph: The network topology (from netrun_sim.Graph)
            consumed_packet_storage: Keep values after consumption
            consumed_packet_storage_limit: Max consumed values to keep
            packet_storage_path: Path for file-based packet storage
            thread_pools: Thread pool configurations {"name": {"size": N}}
            process_pools: Process pool configurations {"name": {"size": N}}
            on_error: Error handling mode ("continue", "pause", "raise")
            error_callback: Called on any node error
            dead_letter_queue: DLQ mode ("memory", "file", or callback)
            dead_letter_path: Path for file-based DLQ
            dead_letter_callback: Callback for DLQ
            history_max_size: Max events in memory
            history_file: Path for history persistence
            history_chunk_size: Events per history write
            history_flush_on_pause: Flush history when paused
        """
        # Validate on_error
        if on_error not in ("continue", "pause", "raise"):
            raise ValueError(f"on_error must be 'continue', 'pause', or 'raise', got '{on_error}'")

        # Store the graph and create internal NetSim
        self._graph = graph
        self._sim = NetSim(graph)

        # Packet value storage
        self._value_store = PacketValueStore(
            consumed_storage=consumed_packet_storage,
            consumed_storage_limit=consumed_packet_storage_limit,
            storage_path=packet_storage_path,
        )

        # Node configurations and execution functions
        self._node_configs: Dict[str, NodeConfig] = {}
        self._node_exec_funcs: Dict[str, NodeExecFuncs] = {}

        # Pool configurations (to be implemented in Milestone 6)
        self._thread_pools_config = thread_pools or {}
        self._process_pools_config = process_pools or {}

        # Error handling
        self._on_error = on_error
        self._error_callback = error_callback

        # Dead letter queue
        dlq_path = Path(dead_letter_path) if dead_letter_path else None
        if callable(dead_letter_queue):
            # If a callable is passed, use callback mode
            self._dead_letter_queue = DeadLetterQueue(
                mode="callback",
                callback=dead_letter_queue,
            )
        else:
            self._dead_letter_queue = DeadLetterQueue(
                mode=dead_letter_queue,
                file_path=dlq_path,
                callback=dead_letter_callback,
            )

        # History config (to be implemented in Milestone 8)
        self._history_max_size = history_max_size
        self._history_file = Path(history_file) if history_file else None
        self._history_chunk_size = history_chunk_size
        self._history_flush_on_pause = history_flush_on_pause

        # Runtime state
        self._state = NetState.CREATED
        # Track manually-created Running epochs that need execution
        self._pending_running_epochs: set[str] = set()

    # -------------------------------------------------------------------------
    # Properties
    # -------------------------------------------------------------------------

    @property
    def graph(self) -> Graph:
        """The network graph topology."""
        return self._graph

    @property
    def state(self) -> NetState:
        """The current state of the Net."""
        return self._state

    @property
    def dead_letter_queue(self) -> DeadLetterQueue:
        """The dead letter queue for failed epochs."""
        return self._dead_letter_queue

    @property
    def value_store(self) -> PacketValueStore:
        """The packet value store."""
        return self._value_store

    # -------------------------------------------------------------------------
    # Node Configuration
    # -------------------------------------------------------------------------

    def set_node_exec(
        self,
        node_name: str,
        exec_func: Callable,
        start_func: Optional[Callable] = None,
        stop_func: Optional[Callable] = None,
        failed_func: Optional[Callable] = None,
    ) -> None:
        """
        Set execution functions for a node.

        Args:
            node_name: Name of the node
            exec_func: Main execution function (required)
            start_func: Called when net starts (optional)
            stop_func: Called when net stops (optional)
            failed_func: Called after failed execution (optional)
        """
        # Validate node exists
        nodes = self._sim.graph.nodes()
        if node_name not in nodes:
            raise NodeNotFoundError(f"Node '{node_name}' not found in graph")

        self._node_exec_funcs[node_name] = NodeExecFuncs(
            exec_func=exec_func,
            start_func=start_func,
            stop_func=stop_func,
            failed_func=failed_func,
        )

    def set_node_config(self, node_name: str, **options) -> None:
        """
        Set configuration options for a node.

        Args:
            node_name: Name of the node
            **options: Configuration options (see NodeConfig)
        """
        # Validate node exists
        nodes = self._sim.graph.nodes()
        if node_name not in nodes:
            raise NodeNotFoundError(f"Node '{node_name}' not found in graph")

        # Validate option names
        valid_options = {f.name for f in NodeConfig.__dataclass_fields__.values()}
        for opt_name in options:
            if opt_name not in valid_options:
                raise ValueError(f"Unknown config option: '{opt_name}'")

        # Get existing config or create default
        if node_name in self._node_configs:
            # Update existing config
            current = self._node_configs[node_name]
            # Create new config with updated values
            config_dict = {
                field: getattr(current, field)
                for field in valid_options
            }
            config_dict.update(options)
            self._node_configs[node_name] = NodeConfig(**config_dict)
        else:
            # Create new config
            self._node_configs[node_name] = NodeConfig(**options)

    def get_node_config(self, node_name: str) -> NodeConfig:
        """Get the configuration for a node (returns default if not set)."""
        return self._node_configs.get(node_name, NodeConfig())

    def get_node_exec_funcs(self, node_name: str) -> Optional[NodeExecFuncs]:
        """Get the execution functions for a node."""
        return self._node_exec_funcs.get(node_name)

    # -------------------------------------------------------------------------
    # Wrapper Methods (hide NetSim)
    # -------------------------------------------------------------------------

    def get_startable_epochs(self) -> list[str]:
        """Get list of epoch IDs that are ready to start."""
        return list(self._sim.get_startable_epochs())

    def get_startable_epochs_by_node(self, node_name: str) -> list[str]:
        """Get list of startable epoch IDs for a specific node."""
        all_startable = self._sim.get_startable_epochs()
        result = []
        for epoch_id in all_startable:
            epoch = self._sim.get_epoch(epoch_id)
            if epoch and epoch.node_name == node_name:
                result.append(epoch_id)
        return result

    def get_epoch(self, epoch_id: str) -> Optional[Epoch]:
        """Get an epoch by ID."""
        return self._sim.get_epoch(epoch_id)

    def get_packet(self, packet_id: str) -> Optional[Packet]:
        """Get a packet by ID."""
        return self._sim.get_packet(packet_id)

    def inject_source_epoch(self, node_name: str) -> str:
        """
        Inject a source epoch for a node with no input ports.

        Returns the epoch ID.
        """
        # Create an empty salvo for source nodes (no input condition needed)
        # Use empty string as placeholder for salvo condition
        salvo = Salvo("__manual_inject__", [])
        action = NetAction.create_and_start_epoch(node_name, salvo)
        response_data, _ = self._sim.do_action(action)

        # Get the epoch ID from the response data
        epoch_id = response_data.epoch.id

        # Track this epoch for execution
        self._pending_running_epochs.add(epoch_id)

        return epoch_id

    # -------------------------------------------------------------------------
    # Internal Execution Methods
    # -------------------------------------------------------------------------

    def _get_input_packets(self, epoch: Epoch) -> dict[str, list[Packet]]:
        """Get the input packets for an epoch, grouped by port name."""
        input_packets: dict[str, list[Packet]] = {}

        # Get packets from the input salvo
        in_salvo = epoch.in_salvo
        if in_salvo is None:
            return input_packets

        # in_salvo.packets is a list of (port_name, packet_id) tuples
        for port_name, packet_id in in_salvo.packets:
            if port_name not in input_packets:
                input_packets[port_name] = []
            packet = self._sim.get_packet(str(packet_id))
            if packet is not None:
                input_packets[port_name].append(packet)

        return input_packets

    def _execute_epoch(self, epoch_id: str) -> None:
        """
        Execute a single epoch with retry support.

        This is the main execution logic for a node.
        """
        epoch = self._sim.get_epoch(epoch_id)
        if epoch is None:
            raise ValueError(f"Epoch {epoch_id} not found")

        node_name = epoch.node_name
        config = self.get_node_config(node_name)
        exec_funcs = self.get_node_exec_funcs(node_name)

        # Skip if no exec_func defined
        if exec_funcs is None or exec_funcs.exec_func is None:
            return

        # Start the epoch if not already Running
        if epoch.state == EpochState.Startable:
            action = NetAction.start_epoch(epoch_id)
            self._sim.do_action(action)

        # Remove from pending running epochs if present
        self._pending_running_epochs.discard(epoch_id)

        # Get input packets
        input_packets = self._get_input_packets(epoch)

        # Build input packet IDs for dead letter queue
        input_packet_ids = {}
        for port_name, pkts in input_packets.items():
            input_packet_ids[port_name] = [str(pkt.id) for pkt in pkts]

        # Retry state
        max_attempts = config.retries + 1
        retry_timestamps: List[datetime] = []
        retry_exceptions: List[Exception] = []
        final_exception = None
        success = False

        # Track start time for timeout
        start_time = time.time()

        for attempt in range(max_attempts):
            retry_count = attempt
            exception_raised = None

            # Create fresh execution context for each attempt
            ctx = NodeExecutionContext(
                net=self,
                epoch_id=epoch_id,
                node_name=node_name,
                defer_net_actions=config.defer_net_actions,
                retry_count=retry_count,
                retry_timestamps=retry_timestamps.copy(),
                retry_exceptions=retry_exceptions.copy(),
            )

            try:
                # Check for timeout before execution
                if config.timeout is not None:
                    elapsed = time.time() - start_time
                    if elapsed >= config.timeout:
                        raise EpochTimeout(node_name, epoch_id, config.timeout)

                # Execute the node function
                exec_funcs.exec_func(ctx, input_packets)

                # Success - commit deferred actions if any
                if config.defer_net_actions and ctx._deferred_queue is not None:
                    _commit_deferred_actions(self, epoch_id, ctx._deferred_queue)

                # Finish the epoch
                action = NetAction.finish_epoch(epoch_id)
                self._sim.do_action(action)
                success = True
                break

            except EpochCancelled:
                # Epoch was cancelled by the node
                action = NetAction.cancel_epoch(epoch_id)
                self._sim.do_action(action)
                raise

            except (EpochTimeout, Exception) as e:
                exception_raised = e
                retry_timestamps.append(datetime.now())
                retry_exceptions.append(e)

                # Call failed_func after each failure
                if exec_funcs.failed_func is not None:
                    failure_ctx = NodeFailureContext(
                        epoch_id=epoch_id,
                        node_name=node_name,
                        retry_count=retry_count,
                        retry_timestamps=retry_timestamps.copy(),
                        retry_exceptions=retry_exceptions.copy(),
                        input_salvo=input_packets,
                        packet_values=ctx._get_consumed_values(),
                        exception=exception_raised,
                    )
                    try:
                        exec_funcs.failed_func(failure_ctx)
                    except Exception:
                        pass

                # Check if we have more retries
                if attempt < max_attempts - 1:
                    # Unconsume packets for retry
                    if config.defer_net_actions:
                        consumed_values = ctx._get_consumed_values()
                        _unconsume_packets_for_retry(self, consumed_values)

                    # Wait before retry
                    if config.retry_wait > 0:
                        time.sleep(config.retry_wait)

                    continue
                else:
                    # Max retries exceeded
                    final_exception = exception_raised

        # Handle final failure
        if not success and final_exception is not None:
            # Cancel the epoch
            action = NetAction.cancel_epoch(epoch_id)
            self._sim.do_action(action)

            # Add to dead letter queue if enabled
            if config.dead_letter_queue:
                dlq_entry = DeadLetterEntry(
                    epoch_id=epoch_id,
                    node_name=node_name,
                    exception=final_exception,
                    retry_count=len(retry_exceptions) - 1,
                    retry_timestamps=retry_timestamps,
                    retry_exceptions=retry_exceptions,
                    input_packets=input_packet_ids,
                    packet_values=ctx._get_consumed_values() if ctx else {},
                    timestamp=datetime.now(),
                )
                self._dead_letter_queue.add(dlq_entry)

            # Call error callback if set
            if self._error_callback is not None:
                try:
                    self._error_callback(final_exception, node_name, epoch_id)
                except Exception:
                    pass

            # Handle based on on_error setting
            if self._on_error == "raise":
                self._state = NetState.PAUSED
                raise NodeExecutionFailed(node_name, epoch_id, final_exception) from final_exception
            elif self._on_error == "pause":
                self._state = NetState.PAUSED
            # "continue" - just keep going

    async def _execute_epoch_async(self, epoch_id: str) -> None:
        """
        Execute a single epoch asynchronously with retry support.

        This is the async version of _execute_epoch for nodes with async exec_func.
        """
        epoch = self._sim.get_epoch(epoch_id)
        if epoch is None:
            raise ValueError(f"Epoch {epoch_id} not found")

        node_name = epoch.node_name
        config = self.get_node_config(node_name)
        exec_funcs = self.get_node_exec_funcs(node_name)

        # Skip if no exec_func defined
        if exec_funcs is None or exec_funcs.exec_func is None:
            return

        # Start the epoch if not already Running
        if epoch.state == EpochState.Startable:
            action = NetAction.start_epoch(epoch_id)
            self._sim.do_action(action)

        # Remove from pending running epochs if present
        self._pending_running_epochs.discard(epoch_id)

        # Get input packets
        input_packets = self._get_input_packets(epoch)

        # Build input packet IDs for dead letter queue
        input_packet_ids = {}
        for port_name, pkts in input_packets.items():
            input_packet_ids[port_name] = [str(pkt.id) for pkt in pkts]

        # Retry state
        max_attempts = config.retries + 1
        retry_timestamps: List[datetime] = []
        retry_exceptions: List[Exception] = []
        final_exception = None
        success = False

        # Track start time for timeout
        start_time = time.time()

        for attempt in range(max_attempts):
            retry_count = attempt
            exception_raised = None

            # Create fresh execution context for each attempt
            ctx = NodeExecutionContext(
                net=self,
                epoch_id=epoch_id,
                node_name=node_name,
                defer_net_actions=config.defer_net_actions,
                retry_count=retry_count,
                retry_timestamps=retry_timestamps.copy(),
                retry_exceptions=retry_exceptions.copy(),
            )

            try:
                # Check for timeout before execution
                if config.timeout is not None:
                    elapsed = time.time() - start_time
                    if elapsed >= config.timeout:
                        raise EpochTimeout(node_name, epoch_id, config.timeout)

                # Execute the node function (async)
                result = exec_funcs.exec_func(ctx, input_packets)
                if asyncio.iscoroutine(result):
                    await result

                # Success - commit deferred actions if any
                if config.defer_net_actions and ctx._deferred_queue is not None:
                    _commit_deferred_actions(self, epoch_id, ctx._deferred_queue)

                # Finish the epoch
                action = NetAction.finish_epoch(epoch_id)
                self._sim.do_action(action)
                success = True
                break

            except EpochCancelled:
                action = NetAction.cancel_epoch(epoch_id)
                self._sim.do_action(action)
                raise

            except (EpochTimeout, Exception) as e:
                exception_raised = e
                retry_timestamps.append(datetime.now())
                retry_exceptions.append(e)

                # Call failed_func after each failure
                if exec_funcs.failed_func is not None:
                    failure_ctx = NodeFailureContext(
                        epoch_id=epoch_id,
                        node_name=node_name,
                        retry_count=retry_count,
                        retry_timestamps=retry_timestamps.copy(),
                        retry_exceptions=retry_exceptions.copy(),
                        input_salvo=input_packets,
                        packet_values=ctx._get_consumed_values(),
                        exception=exception_raised,
                    )
                    try:
                        result = exec_funcs.failed_func(failure_ctx)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception:
                        pass

                # Check if we have more retries
                if attempt < max_attempts - 1:
                    if config.defer_net_actions:
                        consumed_values = ctx._get_consumed_values()
                        _unconsume_packets_for_retry(self, consumed_values)

                    # Wait before retry (async sleep)
                    if config.retry_wait > 0:
                        await asyncio.sleep(config.retry_wait)

                    continue
                else:
                    final_exception = exception_raised

        # Handle final failure
        if not success and final_exception is not None:
            action = NetAction.cancel_epoch(epoch_id)
            self._sim.do_action(action)

            if config.dead_letter_queue:
                dlq_entry = DeadLetterEntry(
                    epoch_id=epoch_id,
                    node_name=node_name,
                    exception=final_exception,
                    retry_count=len(retry_exceptions) - 1,
                    retry_timestamps=retry_timestamps,
                    retry_exceptions=retry_exceptions,
                    input_packets=input_packet_ids,
                    packet_values=ctx._get_consumed_values() if ctx else {},
                    timestamp=datetime.now(),
                )
                self._dead_letter_queue.add(dlq_entry)

            if self._error_callback is not None:
                try:
                    result = self._error_callback(final_exception, node_name, epoch_id)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:
                    pass

            if self._on_error == "raise":
                self._state = NetState.PAUSED
                raise NodeExecutionFailed(node_name, epoch_id, final_exception) from final_exception
            elif self._on_error == "pause":
                self._state = NetState.PAUSED

    def _call_start_funcs(self) -> None:
        """Call start_node_func for all nodes that have one defined."""
        for node_name in self._graph.nodes():
            exec_funcs = self.get_node_exec_funcs(node_name)
            if exec_funcs is not None and exec_funcs.start_func is not None:
                exec_funcs.start_func(self)

    def _call_stop_funcs(self) -> None:
        """Call stop_node_func for all nodes that have one defined."""
        for node_name in self._graph.nodes():
            exec_funcs = self.get_node_exec_funcs(node_name)
            if exec_funcs is not None and exec_funcs.stop_func is not None:
                exec_funcs.stop_func(self)

    # -------------------------------------------------------------------------
    # Sync Execution Methods
    # -------------------------------------------------------------------------

    def run_step(self, start_epochs: bool = True, threaded: bool = False) -> None:
        """
        Run one step of the network.

        This method:
        1. Runs NetSim until blocked (moves packets, creates startable epochs)
        2. If start_epochs=True, executes all startable epochs
        3. Returns when no more progress can be made in this step

        Args:
            start_epochs: Whether to start and execute ready epochs
            threaded: Run in background thread (Milestone 6)
        """
        if threaded:
            raise NotImplementedError("threaded=True will be implemented in Milestone 6")

        if self._state == NetState.STOPPED:
            raise RuntimeError("Cannot run_step on a stopped net")

        if self._state == NetState.PAUSED:
            return  # Don't do anything if paused

        self._state = NetState.RUNNING

        # Run NetSim until blocked
        action = NetAction.run_net_until_blocked()
        self._sim.do_action(action)

        if not start_epochs:
            return

        # Combine startable epochs and pending running epochs
        startable = list(self._sim.get_startable_epochs())
        pending_running = list(self._pending_running_epochs)
        epochs_to_execute = startable + pending_running

        for epoch_id in epochs_to_execute:
            # Convert ULID to string if needed
            epoch_id = str(epoch_id)

            if self._state == NetState.PAUSED:
                break  # Stop if we got paused during execution

            epoch = self._sim.get_epoch(epoch_id)
            if epoch is None:
                continue

            node_name = epoch.node_name
            exec_funcs = self.get_node_exec_funcs(node_name)

            # Skip nodes without exec_func
            if exec_funcs is None or exec_funcs.exec_func is None:
                continue

            try:
                self._execute_epoch(epoch_id)
            except EpochCancelled:
                pass  # Epoch was cancelled, continue with others
            except NodeExecutionFailed:
                if self._on_error == "raise":
                    raise
                # For "pause" and "continue", error is already handled

    def start(self, threaded: bool = False) -> None:
        """
        Start the network and run until fully blocked.

        This method:
        1. Calls start_node_func for all nodes
        2. Runs run_step() in a loop until no more progress
        3. Calls stop_node_func for all nodes when done

        Args:
            threaded: Run in background thread (Milestone 6)
        """
        if threaded:
            raise NotImplementedError("threaded=True will be implemented in Milestone 6")

        if self._state == NetState.STOPPED:
            raise RuntimeError("Cannot start a stopped net")

        # Call start functions
        self._call_start_funcs()

        self._state = NetState.RUNNING

        try:
            # Run until fully blocked
            while self._state == NetState.RUNNING:
                # Check what epochs we can execute before this step
                startable_before = set(self._sim.get_startable_epochs())
                pending_before = set(self._pending_running_epochs)
                epochs_before = startable_before | pending_before

                self.run_step(start_epochs=True)

                # After run_step, move any new packets from edges to input ports
                # This ensures epochs created by output packets are visible
                action = NetAction.run_net_until_blocked()
                self._sim.do_action(action)

                # Check what epochs we can execute after this step
                startable_after = set(self._sim.get_startable_epochs())
                pending_after = set(self._pending_running_epochs)
                epochs_after = startable_after | pending_after

                # Check if we're fully blocked
                # Fully blocked = no epochs to execute and no progress was made
                can_execute = False
                for epoch_id in epochs_after:
                    epoch = self._sim.get_epoch(str(epoch_id))
                    if epoch:
                        exec_funcs = self.get_node_exec_funcs(epoch.node_name)
                        if exec_funcs and exec_funcs.exec_func:
                            can_execute = True
                            break

                if not can_execute and epochs_before == epochs_after:
                    # No progress made and no executable epochs
                    break

        finally:
            # Call stop functions
            self._call_stop_funcs()
            if self._state == NetState.RUNNING:
                self._state = NetState.PAUSED

    def pause(self) -> None:
        """
        Pause the network (finish running epochs, don't start new ones).

        (To be implemented in Milestone 6)
        """
        # For now, just set state
        self._state = NetState.PAUSED

    def stop(self) -> None:
        """
        Stop the network entirely.

        (To be implemented in Milestone 6)
        """
        self._state = NetState.STOPPED

    # -------------------------------------------------------------------------
    # Async Execution Methods (Milestone 5)
    # -------------------------------------------------------------------------

    async def _call_start_funcs_async(self) -> None:
        """Async version: call start_node_func for all nodes."""
        for node_name in self._graph.nodes():
            exec_funcs = self.get_node_exec_funcs(node_name)
            if exec_funcs is not None and exec_funcs.start_func is not None:
                result = exec_funcs.start_func(self)
                if asyncio.iscoroutine(result):
                    await result

    async def _call_stop_funcs_async(self) -> None:
        """Async version: call stop_node_func for all nodes."""
        for node_name in self._graph.nodes():
            exec_funcs = self.get_node_exec_funcs(node_name)
            if exec_funcs is not None and exec_funcs.stop_func is not None:
                result = exec_funcs.stop_func(self)
                if asyncio.iscoroutine(result):
                    await result

    async def async_run_step(self, start_epochs: bool = True) -> None:
        """
        Async version of run_step.

        Run one step of the network asynchronously.

        This method:
        1. Runs NetSim until blocked (moves packets, creates startable epochs)
        2. If start_epochs=True, executes all startable epochs
        3. Returns when no more progress can be made in this step

        Supports both sync and async node exec_funcs - sync funcs are awaited as-is,
        async funcs are properly awaited.

        Args:
            start_epochs: Whether to start and execute ready epochs
        """
        if self._state == NetState.STOPPED:
            raise RuntimeError("Cannot run_step on a stopped net")

        if self._state == NetState.PAUSED:
            return

        self._state = NetState.RUNNING

        # Run NetSim until blocked
        action = NetAction.run_net_until_blocked()
        self._sim.do_action(action)

        if not start_epochs:
            return

        # Combine startable epochs and pending running epochs
        startable = list(self._sim.get_startable_epochs())
        pending_running = list(self._pending_running_epochs)
        epochs_to_execute = startable + pending_running

        for epoch_id in epochs_to_execute:
            epoch_id = str(epoch_id)

            if self._state == NetState.PAUSED:
                break

            epoch = self._sim.get_epoch(epoch_id)
            if epoch is None:
                continue

            node_name = epoch.node_name
            exec_funcs = self.get_node_exec_funcs(node_name)

            if exec_funcs is None or exec_funcs.exec_func is None:
                continue

            try:
                # Check if exec_func is async
                if _is_async_func(exec_funcs.exec_func):
                    await self._execute_epoch_async(epoch_id)
                else:
                    self._execute_epoch(epoch_id)
            except EpochCancelled:
                pass
            except NodeExecutionFailed:
                if self._on_error == "raise":
                    raise

    async def async_start(self) -> None:
        """
        Async version of start.

        Start the network and run until fully blocked, asynchronously.

        This method:
        1. Calls start_node_func for all nodes (async if they are async)
        2. Runs async_run_step() in a loop until no more progress
        3. Calls stop_node_func for all nodes when done (async if they are async)

        Supports both sync and async node functions mixed together.
        """
        if self._state == NetState.STOPPED:
            raise RuntimeError("Cannot start a stopped net")

        # Call start functions (async-aware)
        await self._call_start_funcs_async()

        self._state = NetState.RUNNING

        try:
            while self._state == NetState.RUNNING:
                startable_before = set(self._sim.get_startable_epochs())
                pending_before = set(self._pending_running_epochs)
                epochs_before = startable_before | pending_before

                await self.async_run_step(start_epochs=True)

                action = NetAction.run_net_until_blocked()
                self._sim.do_action(action)

                startable_after = set(self._sim.get_startable_epochs())
                pending_after = set(self._pending_running_epochs)
                epochs_after = startable_after | pending_after

                can_execute = False
                for epoch_id in epochs_after:
                    epoch = self._sim.get_epoch(str(epoch_id))
                    if epoch:
                        exec_funcs = self.get_node_exec_funcs(epoch.node_name)
                        if exec_funcs and exec_funcs.exec_func:
                            can_execute = True
                            break

                if not can_execute and epochs_before == epochs_after:
                    break

        finally:
            await self._call_stop_funcs_async()
            if self._state == NetState.RUNNING:
                self._state = NetState.PAUSED

    async def async_pause(self) -> None:
        """
        Async version of pause.

        Pause the network (finish running epochs, don't start new ones).
        """
        self._state = NetState.PAUSED

    async def async_stop(self) -> None:
        """
        Async version of stop.

        Stop the network entirely.
        """
        self._state = NetState.STOPPED

    async def async_wait_until_blocked(self) -> None:
        """
        Wait until the network is fully blocked.

        This is useful when the network is running in a separate task.
        (Full implementation in Milestone 6 with threaded support)
        """
        while self._state == NetState.RUNNING:
            await asyncio.sleep(0.01)

    # -------------------------------------------------------------------------
    # Checkpoint Methods (Milestone 13)
    # -------------------------------------------------------------------------

    def save_checkpoint(self, path: Union[str, Path]) -> None:
        """
        Save a complete checkpoint of the network state.

        Requires the net to be paused.

        (To be implemented in Milestone 13)
        """
        if self._state != NetState.PAUSED:
            raise NetNotPausedError("save_checkpoint")
        raise NotImplementedError("save_checkpoint will be implemented in Milestone 13")

    @classmethod
    def load_checkpoint(cls, path: Union[str, Path]) -> "Net":
        """
        Load a network from a checkpoint.

        (To be implemented in Milestone 13)
        """
        raise NotImplementedError("load_checkpoint will be implemented in Milestone 13")
