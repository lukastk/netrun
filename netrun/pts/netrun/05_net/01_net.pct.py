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
import signal
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
from netrun.net.config import NetConfig, NodeExecutionConfig, NodeVariable, OutputQueueConfig, PortConfig, PortTypeConfig
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


class PacketTypeMismatch(Exception):
    """Raised when a packet value doesn't match the expected port type."""

    def __init__(
        self,
        port_name: str,
        expected: str,
        actual: str,
        packet_id: str,
        node_name: str,
    ):
        self.port_name = port_name
        self.expected = expected
        self.actual = actual
        self.packet_id = packet_id
        self.node_name = node_name
        super().__init__(
            f"Port '{node_name}.{port_name}' expects {expected}, got {actual} "
            f"(packet {packet_id})"
        )


@dataclass
class NodeExecutionContext:
    """Context object passed to node execution functions.

    This is the primary interface for nodes to interact with the Net during execution.
    All operations are deferred and committed when the function completes.

    Packet operations (create, consume, load_output_port, send_output_salvo) are
    queued locally and executed atomically when the epoch completes successfully.
    """

    # Identity
    epoch_id: str
    node_name: str

    # Retry info
    retry_count: int = 0
    retry_timestamps: list[datetime] = field(default_factory=list)
    retry_exceptions: list[Exception] = field(default_factory=list)

    # Internal (not for user access)
    _config: NodeExecutionConfig = field(repr=False, default=None)
    _print_buffer: list[tuple[datetime, str]] = field(default_factory=list, repr=False)
    _created_packets: list[str] = field(default_factory=list, repr=False)
    _consumed_packets: list[str] = field(default_factory=list, repr=False)

    # Input packet values passed from Net (packet_id -> value)
    _input_packet_values: dict[str, Any] = field(default_factory=dict, repr=False)

    # Output port configurations for type validation
    _out_ports: dict[str, PortConfig] = field(default_factory=dict, repr=False)

    # Deferred actions queue
    _deferred_actions: "DeferredActionQueue" = field(default_factory=lambda: DeferredActionQueue(), repr=False)

    # Track if epoch was cancelled
    _cancelled: bool = field(default=False, repr=False)

    # Node variables (merged net-level + node-level)
    _node_vars: dict[str, "NodeVariable"] | None = field(default=None, repr=False)

    def create_packet(self, value: Any) -> str:
        """Create a new packet with the given value.

        Args:
            value: The value to store in the packet.

        Returns:
            A deferred packet ID (format: "deferred_{uuid}").
            This ID is valid within this epoch and will be mapped
            to a real packet ID when the epoch commits.
        """
        deferred_id = self._deferred_actions.add_create_packet(value)
        self._created_packets.append(deferred_id)
        return deferred_id

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
            A deferred packet ID.
        """
        if kwargs is None:
            kwargs = {}

        lazy_spec = LazyPacketValueSpec(
            func_import_path=func_import_path,
            args=args,
            kwargs=kwargs,
        )

        deferred_id = self._deferred_actions.add_create_packet(lazy_spec)
        self._created_packets.append(deferred_id)
        return deferred_id

    def consume_packet(self, packet_id: str) -> Any:
        """Consume a packet and return its value.

        The packet is removed from the network when the epoch commits.

        Args:
            packet_id: The ID of the packet to consume.

        Returns:
            The packet's value.

        Raises:
            KeyError: If the packet_id is not in the input packets for this epoch.
        """
        if packet_id not in self._input_packet_values:
            raise KeyError(f"Packet {packet_id} not found in input packets for epoch {self.epoch_id}")

        value = self._input_packet_values[packet_id]
        self._deferred_actions.add_consume_packet(packet_id)
        self._consumed_packets.append(packet_id)
        return value

    def load_output_port(self, port_name: str, packet_id: str) -> None:
        """Load a packet into an output port.

        Validates packet type if the port has a port_type configured.

        Args:
            port_name: The name of the output port.
            packet_id: The ID of the packet to load (can be a deferred ID).

        Raises:
            PacketTypeMismatch: If packet value doesn't match port's expected type.
        """
        # Validate type if port has type configured
        port_config = self._out_ports.get(port_name)
        if port_config and port_config.port_type is not None:
            value = self._get_packet_value(packet_id)
            # Skip validation for lazy values (not yet materialized)
            if not isinstance(value, LazyPacketValueSpec):
                self._validate_port_type(port_name, port_config.port_type, value, packet_id)

        self._deferred_actions.add_load_output_port(port_name, packet_id)

    def _get_packet_value(self, packet_id: str) -> Any:
        """Get the value of a packet by ID.

        Handles both input packets and deferred (created) packets.

        Args:
            packet_id: The packet ID (real or deferred).

        Returns:
            The packet's value.

        Raises:
            KeyError: If packet_id is unknown.
        """
        # Check if it's an input packet
        if packet_id in self._input_packet_values:
            return self._input_packet_values[packet_id]

        # Check if it's a deferred packet we created
        if packet_id in self._deferred_actions.packet_values:
            return self._deferred_actions.packet_values[packet_id]

        raise KeyError(f"Unknown packet ID: {packet_id}")

    def _validate_port_type(
        self,
        port_name: str,
        port_type: "str | type | PortTypeConfig",
        value: Any,
        packet_id: str,
    ) -> None:
        """Validate a value against a port type specification.

        Args:
            port_name: Name of the port being validated.
            port_type: The type specification.
            value: The value to validate.
            packet_id: The packet ID (for error messages).

        Raises:
            PacketTypeMismatch: If validation fails.
        """
        expected, matches = self._check_type(port_type, value)

        if not matches:
            actual = type(value).__name__
            raise PacketTypeMismatch(
                port_name=port_name,
                expected=expected,
                actual=actual,
                packet_id=packet_id,
                node_name=self.node_name,
            )

    def _check_type(self, port_type: "str | type | PortTypeConfig", value: Any) -> tuple[str, bool]:
        """Check if value matches port type.

        Args:
            port_type: The type specification.
            value: The value to check.

        Returns:
            Tuple of (expected_type_name, matches).

        Note:
            String import paths (containing ".") are imported and used for isinstance checks.
            This preserves isinstance capability after serialization roundtrips.
        """
        if isinstance(port_type, str):
            if "." in port_type:
                # Full import path - import the type and use isinstance
                try:
                    type_obj = self._import_type(port_type)
                    type_name = port_type.rsplit(".", 1)[-1]
                    return (type_name, isinstance(value, type_obj))
                except (ImportError, AttributeError):
                    # Fall back to name match if import fails
                    type_name = port_type.rsplit(".", 1)[-1]
                    return (type_name, type(value).__name__ == type_name)
            else:
                # Simple name - do name match
                return (port_type, type(value).__name__ == port_type)

        if isinstance(port_type, type):
            # Type object: use isinstance
            return (port_type.__name__, isinstance(value, port_type))

        if isinstance(port_type, PortTypeConfig):
            # Config object - check isinstance_check flag
            if port_type.isinstance_check and "." in port_type.name:
                try:
                    type_obj = self._import_type(port_type.name)
                    type_name = port_type.name.rsplit(".", 1)[-1]
                    return (type_name, isinstance(value, type_obj))
                except (ImportError, AttributeError):
                    pass
            # Fall back to name match
            type_name = port_type.name.rsplit(".", 1)[-1] if "." in port_type.name else port_type.name
            return (type_name, type(value).__name__ == type_name)

        # Unknown type spec - no validation
        return ("any", True)

    def _import_type(self, import_path: str) -> type:
        """Import a type from a dotted import path.

        Args:
            import_path: Dotted import path (e.g., "pandas.DataFrame").

        Returns:
            The imported type.

        Raises:
            ImportError: If the module cannot be imported.
            AttributeError: If the type doesn't exist in the module.
        """
        import importlib
        module_path, type_name = import_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, type_name)

    def send_output_salvo(self, salvo_condition_name: str) -> None:
        """Send packets from output ports onto edges.

        Args:
            salvo_condition_name: The name of the output salvo condition to trigger.
        """
        self._deferred_actions.add_send_output_salvo(salvo_condition_name)

    def cancel_epoch(self) -> NoReturn:
        """Cancel the current epoch.

        This immediately terminates the epoch execution.
        All deferred actions are discarded.
        Raises EpochCancelled which should not be caught by user code.
        """
        self._cancelled = True
        self._deferred_actions.discard()
        raise EpochCancelled(f"Epoch {self.epoch_id} cancelled")

    def print(self, *args, sep: str = " ", end: str = "\n", flush: bool = False) -> None:
        """Capture print output.

        This method behaves like the built-in print() but captures output
        with timestamps. Each print is timestamped at the time it is called.
        All captured output is returned to the Net when the epoch completes.

        Args:
            *args: Values to print (same as builtin print).
            sep: Separator between values (default: " ").
            end: String to append at end (default: "\\n").
            flush: Ignored (kept for API compatibility with builtin print).
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

    @property
    def vars(self) -> dict[str, Any]:
        """Resolved node variables (net-level merged with node-level overrides).

        Returns a dict mapping variable names to their resolved Python values.
        """
        if self._node_vars is None:
            return {}
        return {name: var.resolve_value() for name, var in self._node_vars.items()}

    def _get_execution_result(self) -> "NodeExecutionResult":
        """Get the execution result including deferred actions and print buffer.

        This is called by the func_preprocessor after the node function completes.
        """
        return NodeExecutionResult(
            cancelled=self._cancelled,
            deferred_actions=self._deferred_actions,
            print_buffer=self._print_buffer.copy(),
            created_packets=self._created_packets.copy(),
            consumed_packets=self._consumed_packets.copy(),
        )

# %% [markdown]
# ## NodeExecutionResult
#
# Result returned from a node function execution, containing deferred actions and print buffer.

# %%
#|export
@dataclass
class NodeExecutionResult:
    """Result of a node function execution.

    This is returned by the func_preprocessor wrapper and contains all the
    deferred actions and captured prints that need to be processed by Net.
    """
    cancelled: bool
    """Whether the epoch was cancelled via ctx.cancel_epoch()."""

    deferred_actions: "DeferredActionQueue"
    """Queue of deferred packet operations to commit."""

    print_buffer: list[tuple[datetime, str]]
    """Captured print output with timestamps."""

    created_packets: list[str]
    """List of deferred packet IDs that were created."""

    consumed_packets: list[str]
    """List of packet IDs that were consumed."""

    func_result: Any = None
    """The return value from the node function (if any)."""

    exception: Exception | None = None
    """Exception raised by the node function (if any)."""

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
# ## OutputPacket
#
# A packet retrieved from an output queue.

# %%
#|export
@dataclass
class OutputPacket:
    """A packet retrieved from an output queue.

    Output packets are produced when packets are sent from unconnected output ports
    (ports with no downstream edges). They are routed to output queues based on the
    Net configuration.
    """

    packet_id: str
    """The packet's ULID."""

    value: Any
    """The packet's value."""

    from_node: str
    """The node that produced this packet."""

    from_port: str
    """The output port that produced this packet."""

    queue_name: str
    """The queue this packet was retrieved from."""

    timestamp: datetime
    """When the packet arrived in the queue."""

    epoch_id: str
    """The epoch that produced this packet."""

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

    def discard(self) -> None:
        """Discard all queued actions (on failure/retry)."""
        self.actions.clear()
        self.packet_values.clear()
        self.deferred_to_real_ids.clear()

# %% [markdown]
# ## func_preprocessor and func_done_callback
#
# These functions transform node functions to accept context-creation arguments
# and return NodeExecutionResult with all deferred actions.

# %%
#|export
import importlib

@dataclass
class NetFuncPreprocessorNodeConfig:
    """Picklable configuration for a node's preprocessor behavior.

    This captures only the serializable parts needed for preprocessing,
    avoiding closures that can't be pickled for multiprocess pools.
    """
    factory: str | None
    """Factory module path for lazy resolution (None if not factory-based)."""

    factory_args: dict[str, Any]
    """Arguments to pass to factory's get_node_funcs()."""

    out_ports: dict[str, dict[str, Any]]
    """Output port configs (serialized as dicts for pickling)."""

    # Copy of NodeExecutionConfig fields needed for context creation
    capture_prints: bool = True
    print_flush_interval: float = 0.1
    print_buffer_max_size: int | None = None
    print_echo_stdout: bool = False
    retries: int = 0
    retry_wait: float = 0.0
    timeout: float | None = None

    node_vars: dict[str, dict[str, str]] | None = None
    """Merged node variables (serialized as dicts for pickling)."""

    @classmethod
    def from_node_config(
        cls,
        exec_config: NodeExecutionConfig,
        out_ports: dict[str, PortConfig],
        factory: str | None,
        factory_args: dict[str, Any],
        node_vars: dict[str, dict[str, str]] | None = None,
    ) -> "NetFuncPreprocessorNodeConfig":
        """Create from execution config, port configs, and factory info."""
        return cls(
            factory=factory,
            factory_args=factory_args,
            out_ports={name: port.model_dump() for name, port in out_ports.items()},
            capture_prints=exec_config.capture_prints,
            print_flush_interval=exec_config.print_flush_interval,
            print_buffer_max_size=exec_config.print_buffer_max_size,
            print_echo_stdout=exec_config.print_echo_stdout,
            retries=exec_config.retries,
            retry_wait=exec_config.retry_wait,
            timeout=exec_config.timeout,
            node_vars=node_vars,
        )


class NetFuncPreprocessor:
    """Picklable func_preprocessor for Net execution.

    Unlike the closure-based version, this class can be pickled for multiprocess pools.
    It resolves factory-based node functions lazily on each worker.
    """

    def __init__(
        self,
        node_configs: dict[str, NetFuncPreprocessorNodeConfig],
    ):
        """Initialize the preprocessor.

        Args:
            node_configs: Mapping of node names to their preprocessor configs.
        """
        self._node_configs = node_configs
        # Worker-local cache for resolved factory functions
        self._resolved_funcs: dict[str, Callable] = {}

    def _resolve_factory(self, node_name: str) -> Callable | None:
        """Resolve factory function on worker (lazy).

        Called on first use for each node on each worker.
        """
        if node_name in self._resolved_funcs:
            return self._resolved_funcs[node_name]

        config = self._node_configs.get(node_name)
        if config is None or config.factory is None:
            return None

        # Import factory module and call get_node_funcs
        module = importlib.import_module(config.factory)
        get_node_funcs = getattr(module, "get_node_funcs")
        exec_func, _, _, _ = get_node_funcs(**config.factory_args)

        self._resolved_funcs[node_name] = exec_func
        return exec_func

    def __call__(self, exec_node_func: Callable) -> Callable:
        """Transform exec_node_func -> wrapped function.

        If exec_node_func is a _FactoryPlaceholder, resolves the actual function
        from the factory on this worker.
        """
        preprocessor_self = self  # Capture for inner function

        def wrapped(
            epoch_id: str,
            node_name: str,
            packets: dict[str, list[str]],
            packet_values: dict[str, Any],
            retry_count: int = 0,
            retry_timestamps: list[datetime] | None = None,
            retry_exceptions: list[Exception] | None = None,
        ) -> NodeExecutionResult:
            config = preprocessor_self._node_configs.get(node_name)
            out_ports = {}
            if config:
                out_ports = {
                    name: PortConfig.model_validate(port_dict)
                    for name, port_dict in config.out_ports.items()
                }

            # Create execution config for context (with essential fields)
            exec_config = None
            if config:
                exec_config = NodeExecutionConfig(
                    capture_prints=config.capture_prints,
                    print_flush_interval=config.print_flush_interval,
                    print_buffer_max_size=config.print_buffer_max_size,
                    print_echo_stdout=config.print_echo_stdout,
                    retries=config.retries,
                    retry_wait=config.retry_wait,
                    timeout=config.timeout,
                )

            # Reconstruct NodeVariable objects from serialized dicts
            _node_vars = None
            if config and config.node_vars:
                _node_vars = {
                    n: NodeVariable.model_validate(v)
                    for n, v in config.node_vars.items()
                }

            ctx = NodeExecutionContext(
                epoch_id=epoch_id,
                node_name=node_name,
                retry_count=retry_count,
                retry_timestamps=retry_timestamps or [],
                retry_exceptions=retry_exceptions or [],
                _config=exec_config,
                _input_packet_values=packet_values,
                _out_ports=out_ports,
                _node_vars=_node_vars,
            )

            # Determine the actual function to call
            actual_func = exec_node_func
            if isinstance(exec_node_func, _FactoryPlaceholder):
                # Resolve factory function on this worker
                resolved = preprocessor_self._resolve_factory(node_name)
                if resolved is None:
                    raise RuntimeError(
                        f"Failed to resolve factory function for node '{node_name}'"
                    )
                actual_func = resolved

            func_result = None
            exception = None

            try:
                func_result = actual_func(ctx, packets)
            except EpochCancelled:
                # Expected when ctx.cancel_epoch() is called
                pass
            except Exception as e:
                exception = e

            # Get the execution result with all deferred actions
            result = ctx._get_execution_result()
            result.func_result = func_result
            result.exception = exception

            return result

        return wrapped


class _FactoryPlaceholder:
    """Placeholder for factory-based node functions.

    Registered with workers instead of actual functions when the node uses
    a factory. The NetFuncPreprocessor resolves the actual function lazily.
    """

    def __init__(self, node_name: str):
        self.node_name = node_name

    def __repr__(self) -> str:
        return f"_FactoryPlaceholder({self.node_name!r})"


def create_net_func_preprocessor(
    node_execution_configs: dict[str, NodeExecutionConfig],
    node_out_ports: dict[str, dict[str, PortConfig]] | None = None,
    node_factories: dict[str, tuple[str, dict[str, Any]]] | None = None,
    net_node_vars: dict[str, NodeVariable] | None = None,
) -> NetFuncPreprocessor:
    """Create a func_preprocessor for Net execution.

    The preprocessor transforms `exec_node_func(ctx, packets)` into a wrapped function
    that:
    1. Creates a NodeExecutionContext with input packet values
    2. Runs the node function (resolving factory functions lazily)
    3. Returns NodeExecutionResult with deferred actions and print buffer

    Args:
        node_execution_configs: Mapping of node names to their execution configs.
        node_out_ports: Mapping of node names to their output port configs (for type validation).
        node_factories: Mapping of node names to (factory_path, factory_args) tuples for
            factory-based nodes. Factory functions are resolved lazily on workers.
        net_node_vars: Net-level default node variables.

    Returns:
        A picklable NetFuncPreprocessor instance.
    """
    node_out_ports = node_out_ports or {}
    node_factories = node_factories or {}

    # Convert to picklable config format
    node_configs = {}
    for node_name, config in node_execution_configs.items():
        out_ports = node_out_ports.get(node_name, {})
        factory_info = node_factories.get(node_name)
        factory = factory_info[0] if factory_info else None
        factory_args = factory_info[1] if factory_info else {}

        # Merge net-level + node-level vars (node overrides net)
        merged_vars = None
        if net_node_vars or config.node_vars:
            merged = {}
            if net_node_vars:
                merged.update({k: v.model_dump() for k, v in net_node_vars.items()})
            if config.node_vars:
                merged.update({k: v.model_dump() for k, v in config.node_vars.items()})
            if merged:
                merged_vars = merged

        node_configs[node_name] = NetFuncPreprocessorNodeConfig.from_node_config(
            config, out_ports, factory, factory_args, node_vars=merged_vars,
        )

    return NetFuncPreprocessor(node_configs)


def _net_func_done_callback_noop(*args, **kwargs):
    """No-op done callback for Net execution.

    All work is done by the preprocessor wrapper, so this is a no-op.
    Defined at module level to be picklable for multiprocess pools.
    """
    pass


def create_net_func_done_callback() -> Callable:
    """Create func_done_callback for Net execution.

    This callback is no longer needed since all state is returned in NodeExecutionResult,
    but we provide a no-op for compatibility.

    Returns:
        A no-op callback function (module-level function for pickling).
    """
    return _net_func_done_callback_noop

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
        self._config_resolved: NetConfig = config.resolve()
        self._graph: netrun_sim.Graph = self._config_resolved.graph.get_graph()
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

        # Dead letter queue for failed epochs
        self._dead_letter_queue: list[dict[str, Any]] = []

        # Build node execution configs lookup (using resolved config for runtime)
        self._node_execution_configs: dict[str, NodeExecutionConfig] = {}
        for node_config in self._config_resolved.graph.nodes:
            if node_config.execution_config is not None:
                self._node_execution_configs[node_config.name] = node_config.execution_config

        # Build node output ports lookup (for type validation)
        self._node_out_ports: dict[str, dict[str, PortConfig]] = {}
        for node_config in self._config_resolved.graph.nodes:
            if node_config.out_ports:
                self._node_out_ports[node_config.name] = node_config.out_ports

        # Build node factories lookup (for lazy resolution on workers)
        self._node_factories: dict[str, tuple[str, dict[str, Any]]] = {}
        for node_config in self._config_resolved.graph.nodes:
            if node_config.factory:
                self._node_factories[node_config.name] = (
                    node_config.factory,
                    node_config.factory_args,
                )

        # Create func_preprocessor with node configs
        func_preprocessor = create_net_func_preprocessor(
            self._node_execution_configs,
            self._node_out_ports,
            self._node_factories,
            net_node_vars=self._config_resolved.node_vars,
        )
        func_done_callback = create_net_func_done_callback()

        # Build ExecutionManager config
        _exec_manager_config = {}
        for pool_name, pool_config in self._config_resolved.pools.items():
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

        # SIGINT handling
        self._sigint_received: bool = False
        self._original_sigint_handler = None

        # Output queues for collecting packets from unconnected output ports
        self._output_queues: dict[str, asyncio.Queue[OutputPacket]] = {}
        self._port_to_queue: dict[tuple[str, str], str] = {}  # (node_name, port_name) -> queue_name

        # Initialize queues from config
        for queue_name, queue_config in self._config.output_queues.items():
            self._output_queues[queue_name] = asyncio.Queue()
            for node_name, port_name in queue_config.ports:
                self._port_to_queue[(node_name, port_name)] = queue_name

        # Initialize catch-all queue if configured
        if self._config.catch_all_output_queue:
            if self._config.catch_all_output_queue not in self._output_queues:
                self._output_queues[self._config.catch_all_output_queue] = asyncio.Queue()

    @property
    def config(self) -> NetConfig:
        """Get the original (unresolved) Net configuration.

        This returns the config as it was passed to the constructor, before
        any factory expansion or import path resolution. Use this for
        serialization or introspection of the original config.
        """
        return self._config

    @property
    def config_resolved(self) -> NetConfig:
        """Get the resolved Net configuration.

        This returns the config with all factories expanded and import paths
        resolved to actual callables. This is used internally for execution.
        """
        return self._config_resolved

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

    @property
    def dead_letter_queue(self) -> list[dict[str, Any]]:
        """Get the dead letter queue containing failed epochs.

        Each entry contains:
        - epoch_id: The failed epoch ID
        - node_name: The node name
        - error: The exception that caused failure
        - retry_count: Number of retries attempted
        - retry_timestamps: Timestamps of each retry attempt
        - retry_exceptions: Exceptions from each retry attempt
        - packets: The input packets for the epoch
        """
        return list(self._dead_letter_queue)

    def clear_dead_letter_queue(self) -> list[dict[str, Any]]:
        """Clear the dead letter queue and return its contents.

        Returns:
            The contents of the dead letter queue before clearing.
        """
        items = self._dead_letter_queue.copy()
        self._dead_letter_queue.clear()
        return items

    def _route_orphaned_packet(
        self,
        packet_id: str,
        from_node: str,
        from_port: str,
        epoch_id: str,
    ) -> None:
        """Route an orphaned packet to the appropriate output queue.

        Called when a packet is sent from an unconnected output port.

        Args:
            packet_id: The packet ID.
            from_node: The node that produced this packet.
            from_port: The output port that produced this packet.
            epoch_id: The epoch that produced this packet.
        """
        # Check if port is configured for a specific queue
        queue_name = self._port_to_queue.get((from_node, from_port))

        if queue_name is None:
            # Not in a specific queue - check catch-all
            if self._config.catch_all_output_queue:
                queue_name = self._config.catch_all_output_queue
            elif self._config.undeclared_output_behavior == "error":
                raise RuntimeError(
                    f"Packet {packet_id} sent from unconnected output port "
                    f"'{from_port}' on node '{from_node}' but no output queue configured"
                )
            else:
                # Discard: consume value and return
                self._packet_store.consume(packet_id)
                return

        # Get value and create OutputPacket
        value = self._packet_store.consume(packet_id)
        output_packet = OutputPacket(
            packet_id=packet_id,
            value=value,
            from_node=from_node,
            from_port=from_port,
            queue_name=queue_name,
            timestamp=get_timestamp_utc(),
            epoch_id=epoch_id,
        )

        # Add to queue
        self._output_queues[queue_name].put_nowait(output_packet)

    # --- Output Queue Retrieval API ---

    async def get_output(
        self,
        queue_name: str | None = None,
        *,
        node: str | None = None,
        port: str | None = None,
        timeout: float | None = None,
    ) -> OutputPacket:
        """Get the next packet from an output queue.

        Args:
            queue_name: Name of the output queue. Mutually exclusive with node/port.
            node: Node name (keyword-only). Must be used with port.
            port: Port name (keyword-only). Must be used with node.
            timeout: Max seconds to wait. None = wait forever.

        Returns:
            The next OutputPacket from the queue.

        Raises:
            ValueError: If neither queue_name nor node/port specified.
            ValueError: If both queue_name and node/port specified.
            KeyError: If queue_name not found or node/port not configured.
            asyncio.TimeoutError: If timeout exceeded.
        """
        queue_name = self._resolve_queue_name(queue_name, node, port)
        queue = self._output_queues.get(queue_name)
        if queue is None:
            raise KeyError(f"Output queue '{queue_name}' not found")

        if timeout is not None:
            return await asyncio.wait_for(queue.get(), timeout=timeout)
        else:
            return await queue.get()

    def try_get_output(
        self,
        queue_name: str | None = None,
        *,
        node: str | None = None,
        port: str | None = None,
    ) -> OutputPacket | None:
        """Get a packet if available, otherwise return None.

        Non-blocking - returns immediately.

        Args:
            queue_name: Name of the output queue. Mutually exclusive with node/port.
            node: Node name (keyword-only). Must be used with port.
            port: Port name (keyword-only). Must be used with node.

        Returns:
            The next OutputPacket from the queue, or None if empty.
        """
        queue_name = self._resolve_queue_name(queue_name, node, port)
        queue = self._output_queues.get(queue_name)
        if queue is None:
            raise KeyError(f"Output queue '{queue_name}' not found")

        try:
            return queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    def get_all_outputs(
        self,
        queue_name: str | None = None,
        *,
        node: str | None = None,
        port: str | None = None,
    ) -> list[OutputPacket]:
        """Get all currently available packets from a queue.

        Non-blocking - returns whatever is currently in the queue.

        Args:
            queue_name: Name of the output queue. Mutually exclusive with node/port.
            node: Node name (keyword-only). Must be used with port.
            port: Port name (keyword-only). Must be used with node.

        Returns:
            List of OutputPackets currently in the queue.
        """
        queue_name = self._resolve_queue_name(queue_name, node, port)
        queue = self._output_queues.get(queue_name)
        if queue is None:
            raise KeyError(f"Output queue '{queue_name}' not found")

        packets = []
        while True:
            try:
                packets.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return packets

    def has_output(
        self,
        queue_name: str | None = None,
        *,
        node: str | None = None,
        port: str | None = None,
    ) -> bool:
        """Check if the queue has any packets available.

        Args:
            queue_name: Name of the output queue. Mutually exclusive with node/port.
            node: Node name (keyword-only). Must be used with port.
            port: Port name (keyword-only). Must be used with node.

        Returns:
            True if the queue has packets, False otherwise.
        """
        queue_name = self._resolve_queue_name(queue_name, node, port)
        queue = self._output_queues.get(queue_name)
        if queue is None:
            raise KeyError(f"Output queue '{queue_name}' not found")
        return not queue.empty()

    def output_count(
        self,
        queue_name: str | None = None,
        *,
        node: str | None = None,
        port: str | None = None,
    ) -> int:
        """Get the number of packets in the queue.

        Args:
            queue_name: Name of the output queue. Mutually exclusive with node/port.
            node: Node name (keyword-only). Must be used with port.
            port: Port name (keyword-only). Must be used with node.

        Returns:
            Number of packets currently in the queue.
        """
        queue_name = self._resolve_queue_name(queue_name, node, port)
        queue = self._output_queues.get(queue_name)
        if queue is None:
            raise KeyError(f"Output queue '{queue_name}' not found")
        return queue.qsize()

    def list_output_queues(self) -> list[str]:
        """List all configured output queue names.

        Returns:
            List of output queue names.
        """
        return list(self._output_queues.keys())

    def _resolve_queue_name(
        self,
        queue_name: str | None,
        node: str | None,
        port: str | None,
    ) -> str:
        """Resolve queue name from either queue_name or node/port.

        Args:
            queue_name: Direct queue name.
            node: Node name (requires port).
            port: Port name (requires node).

        Returns:
            The resolved queue name.

        Raises:
            ValueError: If arguments are invalid.
            KeyError: If node/port not configured for any queue.
        """
        has_queue_name = queue_name is not None
        has_node_port = node is not None or port is not None

        if has_queue_name and has_node_port:
            raise ValueError("Cannot specify both queue_name and node/port")
        if not has_queue_name and not has_node_port:
            raise ValueError("Must specify either queue_name or node/port")

        if has_queue_name:
            return queue_name

        if node is None or port is None:
            raise ValueError("Must specify both node and port")

        resolved = self._port_to_queue.get((node, port))
        if resolved is None:
            raise KeyError(f"No output queue configured for port '{port}' on node '{node}'")
        return resolved

    # --- Graph Query API ---

    def get_edges_from_port(self, node_name: str, port_name: str) -> list:
        """Get all edges connected to an output port.

        Useful for checking if an output port has downstream connections.

        Args:
            node_name: The node name.
            port_name: The output port name.

        Returns:
            List of Edge objects connected to this port.
            Empty list if port is unconnected (dangling).
        """
        edges = []
        for edge in self._graph.edges():
            if edge.source.node_name == node_name and edge.source.port_name == port_name:
                edges.append(edge)
        return edges

    def has_downstream_connection(self, node_name: str, port_name: str) -> bool:
        """Check if an output port has any downstream connections.

        Returns False if the port is "dangling" (no edges connected).

        Args:
            node_name: The node name.
            port_name: The output port name.

        Returns:
            True if the port has at least one downstream edge, False otherwise.
        """
        return len(self.get_edges_from_port(node_name, port_name)) > 0

    # --- Packet Creation & Injection API ---

    def create_external_packet(self, value: Any) -> str:
        """Create a packet outside the network with a value.

        The packet is created in the OutsideNet location and can be
        transported to input ports using inject_packet().

        Args:
            value: The value to store in the packet.

        Returns:
            The packet ID (ULID string).
        """
        response, _ = self._netsim.do_action(netrun_sim.NetAction.create_packet(None))
        packet_id = str(response.packet_id)
        self._packet_store.register(packet_id, value)
        return packet_id

    def create_external_packets(self, values: list[Any]) -> list[str]:
        """Create multiple packets outside the network.

        Args:
            values: List of values to store in packets.

        Returns:
            List of packet IDs in the same order as values.
        """
        packet_ids = []
        for value in values:
            packet_id = self.create_external_packet(value)
            packet_ids.append(packet_id)
        return packet_ids

    def inject_packet(self, packet_id: str, node_name: str, port_name: str) -> None:
        """Transport a packet to a node's input port.

        Args:
            packet_id: The packet ID to transport.
            node_name: Target node name.
            port_name: Target input port name.

        Raises:
            RuntimeError: If the transport fails (e.g., port doesn't exist or is full).
        """
        response, _ = self._netsim.do_action(
            netrun_sim.NetAction.transport_packet_to_location(
                packet_id,
                netrun_sim.PacketLocation.input_port(node_name, port_name),
            )
        )
        # Check for errors in response
        if hasattr(response, 'error') and response.error:
            raise RuntimeError(f"Failed to inject packet: {response.error}")

    def inject_data(self, node_name: str, port_name: str, values: list[Any]) -> list[str]:
        """Create packets with values and inject them into a node's input port.

        This is a convenience method that combines create_external_packets()
        and inject_packet() into a single call.

        Args:
            node_name: Target node name.
            port_name: Target input port name.
            values: List of values to create packets for.

        Returns:
            List of created packet IDs.

        Example:
            packet_ids = net.inject_data("Source", "in", [
                {"id": 1, "data": "hello"},
                {"id": 2, "data": "world"},
            ])
        """
        packet_ids = self.create_external_packets(values)
        for packet_id in packet_ids:
            self.inject_packet(packet_id, node_name, port_name)
        return packet_ids

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

    def list_epoch_log_ids(self) -> list[str]:
        """Get all epoch IDs that have print logs.

        Returns:
            List of epoch IDs with logs.
        """
        return list(self._epoch_print_logs.keys())

    def list_node_log_names(self) -> list[str]:
        """Get all node names that have print logs.

        Returns:
            List of node names with logs.
        """
        return list(self._node_print_logs.keys())

    def get_all_logs_chronological(self) -> list[tuple[datetime, str, str, str]]:
        """Get all print logs across all epochs, sorted by timestamp.

        Returns:
            List of (timestamp, epoch_id, node_name, message) tuples,
            sorted by timestamp ascending.
        """
        all_logs = []

        for epoch_id, logs in self._epoch_print_logs.items():
            # Get node_name for this epoch
            try:
                epoch = self._netsim.get_epoch(epoch_id)
                node_name = epoch.node_name if epoch else "unknown"
            except (ValueError, KeyError):
                node_name = "unknown"

            for timestamp, message in logs:
                all_logs.append((timestamp, epoch_id, node_name, message))

        # Sort by timestamp
        all_logs.sort(key=lambda x: x[0])
        return all_logs

    async def start(self) -> None:
        """Start the Net.

        This starts the ExecutionManager, all pools, and registers node functions.
        """
        if self._started:
            raise RuntimeError("Net already started")

        await self._execution_manager.start()

        # Register node functions with all workers
        await self._register_node_functions()

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

    async def start_background(self) -> None:
        """Start the Net in a background task.

        This starts the Net and creates a background task that continuously
        runs the execution loop. The loop will:
        - Run simulation steps to move packets
        - Execute startable epochs
        - Handle SIGINT for graceful shutdown

        Use `stop()` to gracefully stop the background execution.
        """
        if not self._started:
            await self.start()

        if self._background_task is not None:
            raise RuntimeError("Background task already running")

        # Install SIGINT handler
        self._install_sigint_handler()

        # Start the background loop
        self._background_task = asyncio.create_task(self._run_loop())

    async def _run_loop(self) -> None:
        """Main execution loop for background mode.

        This loop:
        1. Checks if we should stop or pause
        2. Runs simulation steps to move packets
        3. Executes any startable epochs
        4. Yields to allow other tasks to run
        """
        try:
            while not self._stopping and not self._sigint_received:
                if self._paused:
                    # When paused, just sleep and check again
                    await asyncio.sleep(0.01)
                    continue

                # Run simulation step
                made_progress, _ = await self.run_step()

                # Execute any startable epochs
                startable = self.get_startable_epochs()
                if startable:
                    # Execute epochs concurrently
                    tasks = [
                        asyncio.create_task(self._execute_epoch(epoch_id))
                        for epoch_id in startable
                    ]
                    if tasks:
                        # Wait for all epoch executions to complete
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        # Log any exceptions
                        for epoch_id, result in zip(startable, results):
                            if isinstance(result, Exception):
                                import sys
                                print(f"Error executing epoch {epoch_id}: {result}", file=sys.stderr)

                # Small yield to allow other tasks
                if not made_progress and not startable:
                    # If nothing happened, sleep a bit to avoid busy-waiting
                    await asyncio.sleep(0.001)
                else:
                    # Just yield to allow other tasks
                    await asyncio.sleep(0)
        finally:
            # Restore SIGINT handler
            self._restore_sigint_handler()

    def _install_sigint_handler(self) -> None:
        """Install a SIGINT handler for graceful shutdown."""
        def sigint_handler(signum, frame):
            self._sigint_received = True

        self._original_sigint_handler = signal.signal(signal.SIGINT, sigint_handler)

    def _restore_sigint_handler(self) -> None:
        """Restore the original SIGINT handler."""
        if self._original_sigint_handler is not None:
            signal.signal(signal.SIGINT, self._original_sigint_handler)
            self._original_sigint_handler = None

    async def wait_until_done(self) -> None:
        """Wait for the background task to complete.

        This blocks until:
        - All epochs have finished and no packets can move
        - SIGINT is received
        - `stop()` is called from another task
        """
        if self._background_task is not None:
            await self._background_task

    def is_blocked(self) -> bool:
        """Check if the network is blocked (no progress can be made).

        Returns:
            True if no packets can move and no epochs are running.
        """
        # Check if there are any startable or running epochs
        if self.get_startable_epochs():
            return False
        if self._running_epochs:
            return False

        # Try a run_step to see if any packets can move
        # Note: This is a sync check, so we just examine the state
        # without actually running the step
        return not self._netsim.can_make_progress() if hasattr(self._netsim, 'can_make_progress') else True

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

    def _get_func_key(self, node_name: str) -> str:
        """Get the function key for a node's exec_node_func.

        Args:
            node_name: The node name.

        Returns:
            A unique function key for this node.
        """
        return f"net:node:{node_name}"

    def _get_input_packet_values(self, epoch_id: str) -> tuple[dict[str, list[str]], dict[str, Any]]:
        """Get the input packets and their values for an epoch.

        Args:
            epoch_id: The epoch ID.

        Returns:
            Tuple of (packets, packet_values) where:
            - packets: dict mapping port_name -> list of packet_ids
            - packet_values: dict mapping packet_id -> value
        """
        epoch = self._netsim.get_epoch(epoch_id)

        # Get input salvo (packets by port)
        packets: dict[str, list[str]] = {}
        packet_values: dict[str, Any] = {}

        # The epoch's in_salvo is a Salvo object with .packets as list of (port_name, packet_id) tuples
        in_salvo = epoch.in_salvo
        if in_salvo:
            # Convert list of (port_name, packet_id) tuples to dict[port_name, list[packet_id]]
            for port_name, packet_id in in_salvo.packets:
                if port_name not in packets:
                    packets[port_name] = []
                packets[port_name].append(str(packet_id))
                # Peek at the value from PacketStore (don't consume yet - that happens in _commit_epoch_result)
                value = self._packet_store._get(packet_id)
                packet_values[str(packet_id)] = value

        return packets, packet_values

    def _commit_epoch_result(self, epoch_id: str, result: NodeExecutionResult) -> dict[str, str]:
        """Commit the deferred actions from an epoch's execution result.

        Args:
            epoch_id: The epoch ID.
            result: The NodeExecutionResult from the worker.

        Returns:
            Mapping of deferred packet IDs to real packet IDs.
        """
        from ulid import ULID

        deferred_to_real: dict[str, str] = {}

        # Process each deferred action in order
        for action_type, args in result.deferred_actions.actions:
            if action_type == "create_packet":
                deferred_id, value = args

                # Create packet in netsim (inside the epoch) - netsim assigns the real ID
                response, _ = self._netsim.do_action(netrun_sim.NetAction.create_packet(epoch_id))
                real_id = str(response.packet_id)
                deferred_to_real[deferred_id] = real_id

                # Store value in PacketStore using the netsim-assigned ID
                self._packet_store.register(real_id, value)

            elif action_type == "consume_packet":
                packet_id, = args
                # Map deferred IDs if needed (though consume usually uses real IDs)
                real_packet_id = deferred_to_real.get(packet_id, packet_id)

                # Consume from PacketStore
                self._packet_store.consume(real_packet_id)

                # Consume in netsim
                self._netsim.do_action(netrun_sim.NetAction.consume_packet(real_packet_id))

            elif action_type == "load_output_port":
                port_name, packet_id = args
                # Map deferred ID to real ID
                real_packet_id = deferred_to_real.get(packet_id, packet_id)

                # Load into output port in netsim
                self._netsim.do_action(
                    netrun_sim.NetAction.load_packet_into_output_port(real_packet_id, port_name)
                )

            elif action_type == "send_output_salvo":
                salvo_condition_name, = args

                # Send output salvo in netsim
                self._netsim.do_action(
                    netrun_sim.NetAction.send_output_salvo(epoch_id, salvo_condition_name)
                )

                # Check for orphaned packets (sent to unconnected output ports)
                # and route them to output queues
                epoch = self._netsim.get_epoch(epoch_id)
                if epoch.orphaned_packets:
                    for orphaned_info in epoch.orphaned_packets:
                        self._route_orphaned_packet(
                            packet_id=orphaned_info.packet_id,
                            from_node=epoch.node_name,
                            from_port=orphaned_info.from_port,
                            epoch_id=epoch_id,
                        )

        return deferred_to_real

    async def execute_epoch(self, epoch_id: str) -> NodeExecutionResult | None:
        """Execute a single startable epoch.

        This is the public API for manually executing epochs. It:
        1. Checks rate limiting
        2. Starts the epoch in netsim
        3. Dispatches the node function to a worker
        4. Waits for completion
        5. Commits deferred actions (on success) or handles failure with retries

        Args:
            epoch_id: The ID of the epoch to execute. Must be a startable epoch.

        Returns:
            The NodeExecutionResult if execution succeeded, None if skipped
            (e.g., due to rate limiting or no execution function).

        Raises:
            Exception: If the node function raises and max retries exceeded.

        Example:
            startable = net.get_startable_epochs()
            if startable:
                result = await net.execute_epoch(startable[0])
        """
        return await self._execute_epoch(epoch_id)

    async def _execute_epoch(self, epoch_id: str) -> NodeExecutionResult | None:
        """Internal: Execute a single startable epoch with retry support.

        Args:
            epoch_id: The ID of the epoch to execute.

        Returns:
            The NodeExecutionResult if execution succeeded, None if skipped.
        """
        epoch = self._netsim.get_epoch(epoch_id)
        node_name = epoch.node_name
        config = self._get_node_execution_config(node_name)

        # Check if node has an execution function (either direct or via factory)
        has_exec_func = (
            config is not None and
            (config.exec_node_func is not None or node_name in self._node_factories)
        )
        if not has_exec_func:
            # No execution function - just mark as running and finish immediately
            self._netsim.do_action(netrun_sim.NetAction.start_epoch(epoch_id))
            self._netsim.do_action(netrun_sim.NetAction.finish_epoch(epoch_id))
            return None

        # Check rate limiting
        if not self._check_rate_limit(node_name):
            return None  # Will be retried on next call

        # Get input packet values
        packets, packet_values = self._get_input_packet_values(epoch_id)

        # Transition epoch to Running
        self._netsim.do_action(netrun_sim.NetAction.start_epoch(epoch_id))
        self._running_epochs.add(epoch_id)

        try:
            return await self._execute_epoch_with_retry(
                epoch_id=epoch_id,
                node_name=node_name,
                config=config,
                packets=packets,
                packet_values=packet_values,
                retry_count=0,
                retry_timestamps=[],
                retry_exceptions=[],
            )
        finally:
            self._running_epochs.discard(epoch_id)

    async def _execute_epoch_with_retry(
        self,
        epoch_id: str,
        node_name: str,
        config: NodeExecutionConfig,
        packets: dict[str, list[str]],
        packet_values: dict[str, Any],
        retry_count: int,
        retry_timestamps: list[datetime],
        retry_exceptions: list[Exception],
    ) -> NodeExecutionResult | None:
        """Execute an epoch with retry support.

        Args:
            epoch_id: The epoch ID.
            node_name: The node name.
            config: The node's execution config.
            packets: Input packets by port name.
            packet_values: Packet ID to value mapping.
            retry_count: Current retry count.
            retry_timestamps: Timestamps of previous retry attempts.
            retry_exceptions: Exceptions from previous retry attempts.

        Returns:
            The NodeExecutionResult if successful, None if cancelled.
        """
        # Determine allocation method
        allocation_method = (
            config.pool_allocation_method or
            self._config.default_pool_allocation_method
        )

        # Get func key for this node
        func_key = self._get_func_key(node_name)

        # Dispatch to worker
        job_result = await self._execution_manager.run_allocate(
            pool_worker_ids=config.pools,
            allocation_method=allocation_method,
            func_import_path_or_key=func_key,
            send_channel=False,  # Deferred mode doesn't need channel
            func_args=(epoch_id, node_name, packets, packet_values),
            func_kwargs={
                "retry_count": retry_count,
                "retry_timestamps": retry_timestamps,
                "retry_exceptions": retry_exceptions,
            },
        )

        # Extract NodeExecutionResult from job result
        execution_result: NodeExecutionResult = job_result.result

        # Handle print buffer
        if execution_result.print_buffer:
            self._handle_print_buffer(epoch_id, execution_result.print_buffer)

        # Check for errors
        if execution_result.exception is not None:
            return await self._handle_epoch_failure(
                epoch_id=epoch_id,
                node_name=node_name,
                config=config,
                packets=packets,
                packet_values=packet_values,
                error=execution_result.exception,
                retry_count=retry_count,
                retry_timestamps=retry_timestamps,
                retry_exceptions=retry_exceptions,
            )

        if execution_result.cancelled:
            # Epoch was cancelled via ctx.cancel_epoch()
            self._netsim.do_action(netrun_sim.NetAction.cancel_epoch(epoch_id))
            return execution_result

        # Success - commit deferred actions
        self._commit_epoch_result(epoch_id, execution_result)

        # Finish the epoch
        self._netsim.do_action(netrun_sim.NetAction.finish_epoch(epoch_id))

        return execution_result

    async def _handle_epoch_failure(
        self,
        epoch_id: str,
        node_name: str,
        config: NodeExecutionConfig,
        packets: dict[str, list[str]],
        packet_values: dict[str, Any],
        error: Exception,
        retry_count: int,
        retry_timestamps: list[datetime],
        retry_exceptions: list[Exception],
    ) -> NodeExecutionResult | None:
        """Handle a failed epoch execution with retry logic.

        Args:
            epoch_id: The epoch ID.
            node_name: The node name.
            config: The node's execution config.
            packets: Input packets by port name.
            packet_values: Packet ID to value mapping.
            error: The exception that occurred.
            retry_count: Current retry count.
            retry_timestamps: Timestamps of previous retry attempts.
            retry_exceptions: Exceptions from previous retry attempts.

        Returns:
            NodeExecutionResult if retry succeeds, None if max retries exceeded.

        Raises:
            Exception: If on_error is "raise" and max retries exceeded.
        """
        # Update retry tracking
        new_retry_timestamps = retry_timestamps + [get_timestamp_utc()]
        new_retry_exceptions = retry_exceptions + [error]

        # Call on_node_failure callback if configured
        if config.on_node_failure is not None:
            failure_ctx = NodeFailureContext(
                epoch_id=epoch_id,
                node_name=node_name,
                retry_count=retry_count,
                exception=error,
                retry_timestamps=new_retry_timestamps,
                retry_exceptions=new_retry_exceptions,
                input_salvo=packets,
            )
            await self._call_failure_callback(config.on_node_failure, failure_ctx)

        # Check if we should retry
        if retry_count < config.retries:
            # Wait before retry
            if config.retry_wait > 0:
                await asyncio.sleep(config.retry_wait)

            # Retry (deferred actions were discarded, so we start fresh)
            return await self._execute_epoch_with_retry(
                epoch_id=epoch_id,
                node_name=node_name,
                config=config,
                packets=packets,
                packet_values=packet_values,
                retry_count=retry_count + 1,
                retry_timestamps=new_retry_timestamps,
                retry_exceptions=new_retry_exceptions,
            )
        else:
            # Max retries exceeded - cancel the epoch
            self._netsim.do_action(netrun_sim.NetAction.cancel_epoch(epoch_id))

            # Store in dead letter queue
            self._dead_letter_queue.append({
                "epoch_id": epoch_id,
                "node_name": node_name,
                "error": error,
                "retry_count": retry_count,
                "retry_timestamps": new_retry_timestamps,
                "retry_exceptions": new_retry_exceptions,
                "packets": packets,
            })

            # Re-raise the error
            raise error

    async def _call_failure_callback(
        self,
        callback: Callable | str,
        failure_ctx: NodeFailureContext,
    ) -> None:
        """Call an on_node_failure callback.

        Args:
            callback: The callback function or import path.
            failure_ctx: The failure context.
        """
        if isinstance(callback, str):
            # Import from path
            import importlib
            module_path, func_name = callback.rsplit(".", 1)
            module = importlib.import_module(module_path)
            callback = getattr(module, func_name)

        # Call the callback (may be sync or async)
        result = callback(failure_ctx)
        if asyncio.iscoroutine(result):
            await result

    async def execute_startable_epochs(self) -> list[tuple[str, NodeExecutionResult | None]]:
        """Execute all currently startable epochs.

        Returns:
            List of (epoch_id, result) tuples for epochs that were executed.
            Results may be None if the epoch was skipped (no exec func or rate limited).
        """
        results = []
        startable = self.get_startable_epochs()

        for epoch_id in startable:
            try:
                result = await self._execute_epoch(epoch_id)
                results.append((epoch_id, result))
            except Exception as e:
                # Store error but continue with other epochs
                results.append((epoch_id, None))
                # Re-raise if on_error is "raise"
                # For now, just log the error
                import sys
                print(f"Error executing epoch {epoch_id}: {e}", file=sys.stderr)

        return results

    def _import_from_path(self, import_path: str) -> Any:
        """Import a function or type from a dotted import path.

        Args:
            import_path: Dotted import path (e.g., "myapp.utils.process_data").

        Returns:
            The imported object.

        Raises:
            ImportError: If the module cannot be imported.
            AttributeError: If the attribute doesn't exist in the module.
        """
        import importlib
        module_path, name = import_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, name)

    async def _register_node_functions(self) -> None:
        """Register all node functions with the execution manager pools.

        This sends the exec_node_func for each node to all workers in the
        configured pools, so they can be called by function key.

        For factory-based nodes, registers a _FactoryPlaceholder that the
        NetFuncPreprocessor will resolve lazily on each worker.

        String import paths are resolved to actual functions before registration.
        """
        for node_config in self._config_resolved.graph.nodes:
            if node_config.execution_config is None:
                continue

            config = node_config.execution_config
            func_key = self._get_func_key(node_config.name)

            # Determine what to register
            if node_config.name in self._node_factories:
                # Factory-based node: register placeholder for lazy resolution
                exec_func = _FactoryPlaceholder(node_config.name)
            elif config.exec_node_func is not None:
                # Regular node: register the function directly
                exec_func = config.exec_node_func
                # Resolve string import path if needed
                if isinstance(exec_func, str):
                    exec_func = self._import_from_path(exec_func)
            else:
                # No function to register
                continue

            # Register with each pool the node can use
            for pool_id in config.pools:
                await self._execution_manager.send_function_to_pool(
                    pool_id=pool_id,
                    func_key=func_key,
                    func=exec_func,
                )

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
    NodeConfig,
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
        NodeConfig(
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
        NodeConfig(
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
        NodeConfig(
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
