# Net Class Implementation Plan

This document outlines the plan for implementing the `Net` class, which orchestrates flow-based network execution by bridging `netrun-sim` (packet flow simulation) with actual node function execution.

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [NodeExecutionContext](#nodeexecutioncontext)
4. [Print Capture System](#print-capture-system)
5. [Epoch Execution Flow](#epoch-execution-flow)
6. [Packet Management](#packet-management)
7. [Error Handling and Retries](#error-handling-and-retries)
8. [Net Lifecycle Methods](#net-lifecycle-methods)
9. [Implementation Phases](#implementation-phases)

---

## Overview

The `Net` class is the main orchestrator that:

1. **Wraps `netrun-sim`**: Manages the `NetSim` instance that tracks packet locations and epoch states
2. **Executes node functions**: Dispatches `exec_node_func` to worker pools via `ExecutionManager`
3. **Manages packet values**: Stores actual data in `PacketStore` while `netrun-sim` tracks locations
4. **Handles errors and retries**: Implements retry logic with deferred net actions
5. **Captures output**: Collects `ctx.print()` calls from nodes and streams them back

### Key Separation of Concerns

| Component | Responsibility |
|-----------|----------------|
| `netrun-sim` | Packet locations, epoch states, salvo conditions, graph topology |
| `PacketStore` | Actual packet values, lazy evaluation, hashing |
| `ExecutionManager` | Worker pool management, function dispatch |
| `Net` | Orchestration, lifecycle, error handling, print capture |
| `NodeExecutionContext` | Node-facing API for packet operations during execution |

---

## Architecture

### High-Level Flow

```
User Code                    Net                         Workers (via ExecutionManager)
    |                         |                                    |
    |-- net.start() --------->|                                    |
    |                         |-- start pools ------------------>  |
    |                         |                                    |
    |-- inject packets ------>|                                    |
    |                         |-- netsim.run_step() (internal)     |
    |                         |                                    |
    |                         |<- epoch becomes Startable          |
    |                         |                                    |
    |                         |-- dispatch exec_node_func -------> |
    |                         |   (epoch_id, node_name, packets)   |
    |                         |                                    |
    |                         |       [worker creates ctx]         |
    |                         |       [runs exec_node_func(ctx, packets)]
    |                         |       [ctx.print() -> buffer]      |
    |                         |       [ctx.create_packet() -> channel]
    |                         |                                    |
    |                         |<-- PRINT_BUFFER (periodic) --------|
    |                         |<-- CREATE_PACKET -----------------|
    |                         |<-- CONSUME_PACKET ----------------|
    |                         |<-- LOAD_OUTPUT_PORT ---------------|
    |                         |<-- SEND_OUTPUT_SALVO --------------|
    |                         |<-- RUN_COMPLETE -------------------|
    |                         |                                    |
    |                         |-- netsim actions (create, consume, etc.)
    |                         |-- netsim.finish_epoch()            |
    |                         |                                    |
    |<-- events/callbacks ----|                                    |
```

### Protocol Keys (Net <-> Worker)

Extend `ExecutionManagerProtocolKeys` with Net-specific messages:

```python
class NetProtocolKeys(Enum):
    # Downstream (Net -> Worker) - sent via ExecutionManager.run()
    # (Uses func_args/func_kwargs to pass epoch info)

    # Upstream (Worker -> Net) - sent via channel when send_channel=True
    UP_CREATE_PACKET = "net:create-packet"        # (epoch_id, value_or_lazy)
    UP_CONSUME_PACKET = "net:consume-packet"      # (epoch_id, packet_id)
    UP_LOAD_OUTPUT_PORT = "net:load-output-port"  # (epoch_id, port_name, packet_id)
    UP_SEND_OUTPUT_SALVO = "net:send-salvo"       # (epoch_id, salvo_condition_name)
    UP_CANCEL_EPOCH = "net:cancel-epoch"          # (epoch_id,)
    UP_PRINT_BUFFER = "net:print-buffer"          # (epoch_id, buffer: list[str])
    UP_EPOCH_COMPLETE = "net:epoch-complete"      # (epoch_id, success, error?)
```

---

## NodeExecutionContext

The `ctx` object passed to `exec_node_func(ctx, packets)` provides the node-facing API.

**Note**: Node functions are **sync only**. All `ctx` methods are synchronous and block until the operation completes (communicating with Net via the RPC channel).

### Class Definition

```python
@dataclass
class NodeExecutionContext:
    """Context object passed to node execution functions."""

    # Identity
    epoch_id: str
    node_name: str

    # Retry info
    retry_count: int = 0
    retry_timestamps: list[datetime] = field(default_factory=list)
    retry_exceptions: list[Exception] = field(default_factory=list)

    # Internal (not for user access)
    _channel: SyncRPCChannel  # For communicating back to Net
    _config: NodeExecutionConfig
    _print_buffer: list[str] = field(default_factory=list)
    _last_print_flush: float = field(default_factory=time.time)
    _created_packets: list[str] = field(default_factory=list)  # For deferred mode
    _consumed_packets: list[str] = field(default_factory=list)

    # Packet operations
    def create_packet(self, value: Any) -> str: ...
    def create_packet_from_value_func(self, func: Callable, *args, **kwargs) -> str: ...
    def consume_packet(self, packet_id: str) -> Any: ...
    def load_output_port(self, port_name: str, packet_id: str) -> None: ...
    def send_output_salvo(self, salvo_condition_name: str) -> None: ...

    # Epoch control
    def cancel_epoch(self) -> NoReturn: ...

    # Print capture
    def print(self, *args, sep=" ", end="\n", flush=False) -> None: ...
```

### Context Creation via func_preprocessor

The `func_preprocessor` transforms node functions to accept context-creation arguments:

```python
def create_net_func_preprocessor(node_execution_configs: dict[str, NodeExecutionConfig]):
    """Create a func_preprocessor for Net execution."""

    def preprocessor(exec_node_func: Callable) -> Callable:
        """Transform exec_node_func(ctx, packets) -> wrapped(channel, epoch_id, node_name, packets, ...)"""

        def wrapped(channel, epoch_id: str, node_name: str, packets: dict, retry_count: int = 0, ...):
            config = node_execution_configs[node_name]

            ctx = NodeExecutionContext(
                epoch_id=epoch_id,
                node_name=node_name,
                retry_count=retry_count,
                _channel=channel,
                _config=config,
            )

            try:
                return exec_node_func(ctx, packets)
            finally:
                # Final flush handled by func_done_callback
                pass

        return wrapped

    return preprocessor
```

---

## Print Capture System

### Design Goals

1. **Configurable flush interval**: By default, flush every 100ms
2. **Per-node configuration**: Can override at node level
3. **Optional stdout echo**: Can print to actual stdout while capturing
4. **Final flush on completion**: Always flush remaining buffer when node finishes

### Configuration

Add to `NodeExecutionConfig`:

```python
class NodeExecutionConfig(BaseModel):
    # ... existing fields ...

    capture_prints: bool = True
    """If True, ctx.print() captures output instead of printing to stdout."""

    print_flush_interval: float = 0.1  # 100ms
    """How often to flush the print buffer back to Net (in seconds)."""

    print_buffer_max_size: int | None = None
    """Max buffer size before forced flush. None = unlimited (default)."""

    print_echo_stdout: bool = False
    """If True, also print to actual stdout when ctx.print() is called."""

    pool_allocation_method: RunAllocationMethod | None = None
    """How to select a worker when node has multiple pools. None = use Net default."""
```

Add to `NetConfig`:

```python
class NetConfig(BaseModel):
    # ... existing fields ...

    default_pool_allocation_method: RunAllocationMethod = RunAllocationMethod.ROUND_ROBIN
    """Default allocation method for nodes with multiple pools."""
```

### ctx.print() Implementation

```python
def print(self, *args, sep: str = " ", end: str = "\n", flush: bool = False) -> None:
    """Capture print output with periodic flushing.

    Args:
        *args: Values to print (same as builtin print)
        sep: Separator between values (default: " ")
        end: String to append at end (default: "\n")
        flush: If True, immediately flush buffer to Net
    """
    # Format the message (same as builtin print)
    message = sep.join(str(arg) for arg in args) + end

    # Optionally echo to stdout
    if self._config.print_echo_stdout:
        import builtins
        builtins.print(*args, sep=sep, end=end, flush=True)

    # Add to buffer
    self._print_buffer.append(message)

    # Check if we should flush
    now = time.time()
    time_threshold_exceeded = (now - self._last_print_flush) >= self._config.print_flush_interval
    buffer_size_exceeded = (
        self._config.print_buffer_max_size is not None and
        len(self._print_buffer) >= self._config.print_buffer_max_size
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
```

### func_done_callback for Final Flush

```python
def create_net_func_done_callback():
    """Create func_done_callback that flushes remaining prints."""

    def callback(channel, epoch_id: str, node_name: str, packets: dict, **kwargs):
        result = kwargs.get('result')

        # The context's final buffer flush happens here
        # We need to reconstruct enough to send the final buffer
        # Actually, the context object still exists at this point,
        # so we can pass it through or store it somewhere accessible

        # Alternative: have the wrapped function return (result, final_buffer)
        # and the callback extracts and sends it
        pass

    return callback
```

**Better approach**: Have the wrapped function handle final flush:

```python
def wrapped(channel, epoch_id: str, node_name: str, packets: dict, ...):
    ctx = NodeExecutionContext(...)

    try:
        result = exec_node_func(ctx, packets)
        return result
    finally:
        # Always flush remaining buffer
        ctx._flush_print_buffer()
```

### Net-Side Print Handling

The Net receives print buffers and stores them:

```python
class Net:
    def __init__(self, ...):
        # ...
        self._epoch_print_logs: dict[str, list[tuple[datetime, str]]] = {}
        self._node_print_logs: dict[str, list[tuple[datetime, str]]] = {}

    async def _handle_worker_message(self, pool_id: str, msg: WorkerMessage):
        if msg.key == NetProtocolKeys.UP_PRINT_BUFFER.value:
            epoch_id, buffer = msg.data
            timestamp = get_timestamp_utc()

            # Store by epoch
            if epoch_id not in self._epoch_print_logs:
                self._epoch_print_logs[epoch_id] = []
            for line in buffer:
                self._epoch_print_logs[epoch_id].append((timestamp, line))

            # Also store by node (get node_name from epoch)
            epoch = self._netsim.get_epoch(epoch_id)
            node_name = epoch.node_name
            if node_name not in self._node_print_logs:
                self._node_print_logs[node_name] = []
            for line in buffer:
                self._node_print_logs[node_name].append((timestamp, line))

            # Optionally: emit event for real-time streaming
            if self._on_print_callback:
                self._on_print_callback(epoch_id, node_name, buffer)

    def get_epoch_log(self, epoch_id: str) -> list[tuple[datetime, str]]:
        """Get print output for a specific epoch."""
        return list(self._epoch_print_logs.get(epoch_id, []))

    def get_node_log(self, node_name: str) -> list[tuple[datetime, str]]:
        """Get all print output for a node (across all epochs)."""
        return list(self._node_print_logs.get(node_name, []))
```

---

## Epoch Execution Flow

### State Machine

```
[Packets arrive at input ports]
           |
           v
    +-------------+
    |  Startable  |  <-- netrun-sim creates epoch when salvo condition triggers
    +-------------+
           |
           | Net calls netsim.start_epoch(epoch_id)
           v
    +-------------+
    |   Running   |  <-- Net dispatches to worker
    +-------------+
           |
           |-----> [Success] --> netsim.finish_epoch(epoch_id) --> Done
           |
           |-----> [Failure] --> retry? --> back to Running
           |                         |
           |                         +--> max retries --> netsim.cancel_epoch(epoch_id)
           |
           +-----> [Cancel]  --> netsim.cancel_epoch(epoch_id)
```

### Dispatch Flow

```python
async def _execute_epoch(self, epoch_id: str):
    """Execute a single epoch."""
    epoch = self._netsim.get_epoch(epoch_id)
    node_name = epoch.node_name
    config = self._get_node_execution_config(node_name)

    if config is None or config.exec_node_func is None:
        # No execution function - epoch stays Startable
        return

    # Check rate limiting (global across all pools for this node)
    if not self._check_rate_limit(node_name):
        return  # Will be retried on next run_step

    # Get input packets (packet IDs from the salvo)
    input_salvo = epoch.input_salvo  # dict[port_name, list[PacketID]]

    # Transition to Running
    self._netsim.do_action(netrun_sim.NetAction.start_epoch(epoch_id))

    # Determine allocation method (node-specific or net default)
    allocation_method = (
        config.pool_allocation_method or
        self._config.default_pool_allocation_method
    )

    # Build list of (pool_id, worker_id) pairs for all configured pools
    pool_worker_ids = []
    for pool_id in config.pools:
        pool_worker_ids.append(pool_id)  # run_allocate expands pool_id to all workers

    # Dispatch to worker using allocation
    try:
        result = await self._execution_manager.run_allocate(
            pool_worker_ids=pool_worker_ids,
            allocation_method=allocation_method,
            func_import_path_or_key=self._get_func_key(config),
            send_channel=True,  # Always need channel for ctx communication
            func_args=(epoch_id, node_name, input_salvo),
            func_kwargs={
                "retry_count": 0,
                # ... other context args
            },
        )

        # Success - finish the epoch
        self._netsim.do_action(netrun_sim.NetAction.finish_epoch(epoch_id))

    except Exception as e:
        await self._handle_epoch_failure(epoch_id, e, retry_count=0)

def _check_rate_limit(self, node_name: str) -> bool:
    """Check if node can start a new epoch based on rate limit.

    Rate limit is global across all pools for the node.
    """
    config = self._get_node_execution_config(node_name)
    if config.rate_limit_per_second is None:
        return True

    # Track epoch starts per node
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
    self._epoch_start_times[node_name].append(now)
    return True
```

---

## Packet Management

### Packet Operations in NodeExecutionContext

```python
def create_packet(self, value: Any) -> str:
    """Create a new packet with the given value.

    Returns:
        The packet ID (or a deferred ID if defer_net_actions=True)
    """
    # Send to Net, which stores in PacketStore and creates in netsim
    self._channel.send(
        NetProtocolKeys.UP_CREATE_PACKET.value,
        (self.epoch_id, value)
    )

    # Wait for response with packet_id
    key, data = self._channel.recv()
    assert key == "net:create-packet-response"
    packet_id = data

    self._created_packets.append(packet_id)
    return packet_id

def consume_packet(self, packet_id: str) -> Any:
    """Consume a packet and return its value.

    The packet is removed from the network.
    """
    self._channel.send(
        NetProtocolKeys.UP_CONSUME_PACKET.value,
        (self.epoch_id, packet_id)
    )

    # Wait for response with value
    key, data = self._channel.recv()
    assert key == "net:consume-packet-response"
    value = data

    self._consumed_packets.append(packet_id)
    return value
```

### Net-Side Packet Handling

```python
async def _handle_create_packet(self, epoch_id: str, value: Any) -> str:
    """Handle packet creation request from worker."""
    # Generate packet ID
    packet_id = str(ULID())

    # Store value
    self._packet_store.register(packet_id, value)

    # Create in netsim (inside the epoch)
    self._netsim.do_action(
        netrun_sim.NetAction.create_packet(epoch_id)
    )

    return packet_id
```

### Deferred Net Actions

When `defer_net_actions=True`, packet operations are queued and only committed on success.

**Deferred packet IDs** use the format `"deferred_{uuid}"` to make it explicit that the ID is temporary:

```python
def _generate_deferred_packet_id() -> str:
    """Generate a temporary packet ID for deferred mode."""
    return f"deferred_{uuid.uuid4()}"
```

```python
class DeferredActionQueue:
    """Queue of net actions to be committed on success or discarded on failure."""

    def __init__(self):
        self.actions: list[tuple[str, Any]] = []  # (action_type, args)
        self.packet_values: dict[str, Any] = {}   # deferred_id -> value
        self.deferred_to_real_ids: dict[str, str] = {}

    def add_create_packet(self, value: Any) -> str:
        """Queue a packet creation. Returns deferred ID."""
        deferred_id = f"deferred_{uuid.uuid4()}"
        self.actions.append(("create_packet", deferred_id))
        self.packet_values[deferred_id] = value
        return deferred_id

    def add_consume_packet(self, packet_id: str):
        self.actions.append(("consume_packet", packet_id))

    # ... etc

    def commit(self, netsim, packet_store, epoch_id) -> dict[str, str]:
        """Commit all actions. Returns deferred_id -> real_id mapping."""
        for action_type, args in self.actions:
            if action_type == "create_packet":
                deferred_id = args
                real_id = str(ULID())
                self.deferred_to_real_ids[deferred_id] = real_id
                packet_store.register(real_id, self.packet_values[deferred_id])
                netsim.do_action(netrun_sim.NetAction.create_packet(epoch_id))
            # ... handle other action types
        return self.deferred_to_real_ids

    def discard(self):
        """Discard all queued actions (on failure/retry)."""
        self.actions.clear()
        self.packet_values.clear()
        self.deferred_to_real_ids.clear()
```

When using deferred IDs in `load_output_port()` or other operations, they are translated to real IDs on commit.

---

## Error Handling and Retries

### Retry Flow

```python
async def _handle_epoch_failure(self, epoch_id: str, error: Exception, retry_count: int):
    """Handle a failed epoch execution."""
    epoch = self._netsim.get_epoch(epoch_id)
    node_name = epoch.node_name
    config = self._get_node_execution_config(node_name)

    # Call on_node_failure callback if configured
    if config.on_node_failure:
        failure_ctx = NodeFailureContext(
            epoch_id=epoch_id,
            node_name=node_name,
            retry_count=retry_count,
            exception=error,
            # ... etc
        )
        await self._call_failure_callback(config.on_node_failure, failure_ctx)

    # Check if we should retry
    if retry_count < config.retries:
        # Wait before retry
        if config.retry_wait > 0:
            await asyncio.sleep(config.retry_wait)

        # Retry (deferred actions were discarded, so we start fresh)
        await self._execute_epoch_with_retry(epoch_id, retry_count + 1)
    else:
        # Max retries exceeded - cancel the epoch
        self._netsim.do_action(netrun_sim.NetAction.cancel_epoch(epoch_id))

        # Store in dead letter queue if configured
        if self._config.dead_letter_queue:
            self._dead_letter_queue.add(epoch_id, error, retry_count)

        # Handle based on on_error setting
        if self._config.on_error == "raise":
            raise error
        elif self._config.on_error == "pause":
            await self.pause()
```

---

## Net Lifecycle Methods

### Core Methods

```python
class Net:
    # Startup
    async def start(self) -> None: ...
    def start_sync(self) -> None: ...  # Blocking wrapper

    # Running
    async def run_step(self) -> None: ...        # Execute one step
    async def run_until_blocked(self) -> None: ... # Run until no more progress

    # Control
    async def pause(self) -> None: ...   # Finish running epochs, stop starting new ones
    async def stop(self) -> None: ...    # Graceful shutdown

    # Status
    def is_running(self) -> bool: ...
    def is_paused(self) -> bool: ...
    def is_blocked(self) -> bool: ...

    # Queries
    def get_startable_epochs(self) -> list[str]: ...
    def get_running_epochs(self) -> list[str]: ...
    def get_epoch_log(self, epoch_id: str) -> list[tuple[datetime, str]]: ...
    def get_node_log(self, node_name: str) -> list[tuple[datetime, str]]: ...
```

### Background Execution

```python
async def start_background(self) -> None:
    """Start the net in a background task."""
    self._background_task = asyncio.create_task(self._run_loop())

async def _run_loop(self):
    """Main execution loop."""
    while not self._stopping:
        if self._paused:
            await asyncio.sleep(0.01)
            continue

        # Run simulation step
        events = self._netsim.run_step()

        # Check for startable epochs
        for epoch_id in self._netsim.get_startable_epochs():
            if not self._paused:
                asyncio.create_task(self._execute_epoch(epoch_id))

        # Process incoming messages from workers
        await self._process_worker_messages()

        # Small yield to allow other tasks
        await asyncio.sleep(0)
```

---

## Implementation Phases

### Phase 1: Basic Structure ✅
- [x] Implement `NodeExecutionContext` with print capture
- [x] Implement `func_preprocessor` and `func_done_callback` for Net
- [x] Add Net protocol keys
- [x] Basic `Net.__init__` setup with ExecutionManager

### Phase 2: Epoch Execution ✅
- [x] Implement `_execute_epoch` dispatch
- [x] Implement packet operations (create, consume) - using deferred mode
- [x] Implement `load_output_port` and `send_output_salvo`
- [x] Wire up netrun-sim actions via `_commit_epoch_result`
- [x] Implement `execute_startable_epochs()` to dispatch all startable epochs
- [x] Implement `_register_node_functions()` to register node funcs with workers
- [x] Updated architecture to use deferred mode (no bidirectional RPC during execution)

### Phase 3: Print Capture ✅
- [x] Implement `ctx.print()` with buffering and flush interval (timestamps captured at call time)
- [x] Implement Net-side print log storage
- [x] Add `get_epoch_log()` and `get_node_log()` methods
- [x] Add `print_echo_stdout` support

### Phase 4: Error Handling ✅
- [x] Implement retry logic (`_handle_epoch_failure`, `_execute_epoch_with_retry`)
- [x] Deferred net actions already implemented in Phase 2
- [x] Implement `on_node_failure` callbacks (`_call_failure_callback`)
- [x] Implement dead letter queue (`dead_letter_queue`, `clear_dead_letter_queue`)

### Phase 5: Lifecycle ✅ (partial)
- [x] Implement `start()`, `stop()`, `pause()`
- [x] Implement `run_step()` and `run_until_blocked()`
- [ ] Implement background execution
- [ ] SIGINT handling

### Phase 6: Testing & Polish
- [x] Unit tests for NodeExecutionContext
- [ ] Integration tests for full epoch flow
- [ ] Tests for retry behavior
- [x] Tests for print capture timing
- [ ] Documentation and examples

---

## Design Decisions

1. **Async vs Sync node functions**: Node functions (`exec_node_func`) are **sync only**. No async support needed. This simplifies the context implementation and avoids complexity with async print buffering.

2. **Deferred packet IDs**: When `defer_net_actions=True`, `create_packet()` returns a temporary ID in the format `"deferred_{uuid}"`. This makes it explicit that the ID is temporary without requiring a wrapper class. On commit, these are mapped to real packet IDs.

3. **Print buffer size limits**: Configurable via `print_buffer_max_size`, but **default is unlimited** (`None`). When set, buffer is force-flushed when it exceeds the limit.

4. **Multi-pool nodes**: Use `ExecutionManager.run_allocate()` with configurable `RunAllocationMethod`. The allocation method can be:
   - Set globally on the Net (default for all nodes)
   - Overridden per-Node in `NodeExecutionConfig`

5. **Rate limiting**: `rate_limit_per_second` is **global across all pools** for that node. It limits the total number of epoch starts per second for the node, regardless of which pool executes them.
