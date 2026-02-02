# netrun Project Specification

`netrun` is a Python package for running flow-based development (FBD) graphs. It uses `netrun-sim` to manage the core network runtime logic while providing actual node execution, packet handling, and higher-level APIs.

## Table of Contents

**Implemented:**
1. [Internal Utilities](#internal-utilities) - Timestamps and hashing
2. [Storage](#storage) - Packet value storage with lazy evaluation
3. [RPC Layer](#rpc-layer) - Bidirectional message-passing channels
4. [Pool Layer](#pool-layer) - Worker pool management
5. [ExecutionManager](#executionmanager) - High-level execution orchestration
6. [Net Configuration](#net-configuration) - Pydantic DSL for graph and pool configuration

**Partially Implemented:**
7. [Net Class](#net-class-partial) - Integration with netrun-sim (stub)

**Planned:**
8. [Node Execution](#node-execution-planned) - Actual node function execution
9. [Packets and Values](#packets-and-values-planned) - Runtime packet handling
10. [Error Handling and Retries](#error-handling-and-retries-planned)
11. [Logging and History](#logging-and-history-planned)
12. [Port Types](#port-types-planned)
13. [Node Factories](#node-factories-planned)
14. [Checkpointing and State Serialization](#checkpointing-and-state-serialization-planned)

---

# IMPLEMENTED FEATURES

---

## Internal Utilities

Internal utilities in `netrun._iutils`.

### Base Utilities (`netrun._iutils._base`)

```python
from netrun._iutils._base import get_timestamp_utc, patch_to

# Get current UTC timestamp
timestamp = get_timestamp_utc()  # Returns datetime.datetime

# Decorator to patch methods onto a class
@patch_to(MyClass)
def new_method(self, arg):
    ...
```

### Hashing (`netrun._iutils.hashing`)

```python
from netrun._iutils.hashing import hash, HashMethod

# Hash any data (uses pickle serialization)
h = hash({"key": "value"}, method=HashMethod.xxh64)

# Available methods
class HashMethod(Enum):
    adler32 = "adler32"
    crc32 = "crc32"
    sha256 = "sha256"
    blake2b = "blake2b"
    xxh64 = "xxh64"  # Default, fastest

# Direct hash functions for bytes
from netrun._iutils.hashing import adler32, crc32, sha256, blake2b, xxh64
h = xxh64(b"raw bytes")
```

---

## Storage

Thread-safe storage for packet values with lazy evaluation support.

### PacketStoreConfig

```python
from netrun.storage import PacketStoreConfig
from netrun._iutils.hashing import HashMethod

config = PacketStoreConfig(
    hash_method=HashMethod.xxh64,        # Hash algorithm (default: xxh64)
    hash_pickle_protocol=4,               # Pickle protocol for hashing
    try_json_dump_in_hash=False,          # Try JSON before pickle
    evaluate_lazy_value_for_hash=False,   # Evaluate lazy values when hashing
)
```

### LazyPacketValueSpec

For deferred value evaluation:

```python
from netrun.storage import LazyPacketValueSpec

# Lazy value: function is called on consume()
lazy = LazyPacketValueSpec(
    func_import_path="mymodule.fetch_data",  # Import path to function
    args=("arg1", "arg2"),                    # Positional arguments
    kwargs={"key": "value"},                  # Keyword arguments
)
```

### PacketStore

```python
from netrun.storage import PacketStore, PacketStoreConfig, LazyPacketValueSpec

store = PacketStore(PacketStoreConfig())

# Register a direct value
packet_id = ULID()
store.register(packet_id, {"data": 123})

# Register a lazy value
store.register(packet_id, LazyPacketValueSpec(
    func_import_path="mymodule.expensive_query",
    args=(),
    kwargs={},
))

# Consume (removes from store, evaluates lazy values)
value = store.consume(packet_id)

# Destroy without returning value
store.destroy(packet_id)

# Check existence
exists = store.exists(packet_id)

# List all packet IDs
ids = store.list_ids()

# Get hash of packet value
h = store.get_hash(packet_id)

# Persistence
store.save("/path/to/store.pkl")
store.load("/path/to/store.pkl")
```

### Exceptions

```python
from netrun.storage import LazyPacketValueEvaluationError

# Raised when lazy value evaluation fails
# Fields: packet_id, original_exception
```

---

## RPC Layer

The RPC layer provides bidirectional (key, data) message passing between components.

### Base Protocols (`netrun.rpc.base`)

```python
from typing import Protocol, Any

class RPCChannel(Protocol):
    """Async bidirectional message channel."""
    async def send(self, key: str, data: Any) -> None: ...
    async def recv(self, timeout: float | None = None) -> tuple[str, Any]: ...
    async def try_recv(self) -> tuple[str, Any] | None: ...
    async def close(self) -> None: ...
    @property
    def is_closed(self) -> bool: ...

class SyncRPCChannel(Protocol):
    """Sync bidirectional message channel (for workers)."""
    def send(self, key: str, data: Any) -> None: ...
    def recv(self, timeout: float | None = None) -> tuple[str, Any]: ...
    def try_recv(self) -> tuple[str, Any] | None: ...
    def close(self) -> None: ...
    @property
    def is_closed(self) -> bool: ...
```

### Standard Keys

```python
from netrun.rpc.base import RPC_KEY_SHUTDOWN, RPC_KEY_ERROR, RPC_KEY_BROKEN

# RPC_KEY_SHUTDOWN - Request graceful shutdown
# RPC_KEY_ERROR - Error notification
# RPC_KEY_BROKEN - Channel broken notification
```

### Exceptions

| Exception | Description |
|-----------|-------------|
| `RPCError` | Base class for RPC errors |
| `ChannelClosed` | Channel was closed normally |
| `ChannelBroken` | Channel failed unexpectedly |
| `RecvTimeout` | Receive operation timed out |

### Implementations

#### AsyncChannel (`netrun.rpc.aio`)

For communication between async tasks within the same event loop:

```python
from netrun.rpc.aio import create_async_channel_pair

parent_channel, child_channel = create_async_channel_pair()

# Both channels are AsyncChannel (async)
await parent_channel.send("request", {"data": 123})
key, response = await parent_channel.recv(timeout=5.0)
```

#### ThreadChannel (`netrun.rpc.thread`)

For communication between main thread (async) and worker threads (sync):

```python
from netrun.rpc.thread import create_thread_channel_pair

parent_channel, (to_child_queue, from_child_queue) = create_thread_channel_pair()

# parent_channel: ThreadChannel (async, for main thread)
# Queues are passed to worker thread to create SyncThreadChannel
```

#### ProcessChannel (`netrun.rpc.multiprocess`)

For communication between main process (async) and worker processes (sync):

```python
from netrun.rpc.multiprocess import create_queue_pair

parent_channel, (to_child_queue, from_child_queue) = create_queue_pair()

# parent_channel: ProcessChannel (async)
# In subprocess: SyncProcessChannel(to_child_queue, from_child_queue)
```

#### WebSocketChannel (`netrun.rpc.remote`)

For network communication over WebSockets:

```python
from netrun.rpc.remote import connect, serve, serve_background

# Client
async with connect("ws://localhost:8765") as channel:
    await channel.send("request", data)
    key, response = await channel.recv()

# Server (blocking)
async def handler(channel: WebSocketChannel):
    key, data = await channel.recv()
    await channel.send("response", process(data))

await serve(handler, "0.0.0.0", 8765)

# Server (background)
async with serve_background(handler, "0.0.0.0", 8765):
    # Server running in background
    ...
```

---

## Pool Layer

The Pool layer manages collections of workers that process messages.

### Base Types (`netrun.pool.base`)

```python
from dataclasses import dataclass
from typing import Callable, Any

WorkerId = int
WorkerFn = Callable[[SyncRPCChannel, int], None]  # (channel, worker_id) -> None

@dataclass
class WorkerMessage:
    worker_id: WorkerId
    key: str
    data: Any
```

### Pool Protocol

```python
class Pool(Protocol):
    @property
    def num_workers(self) -> int: ...
    @property
    def is_running(self) -> bool: ...

    async def start(self) -> None: ...
    async def close(self, timeout: float | None = None) -> None: ...
    async def send(self, worker_id: WorkerId, key: str, data: Any) -> None: ...
    async def recv(self, timeout: float | None = None) -> WorkerMessage: ...
    async def try_recv(self) -> WorkerMessage | None: ...
    async def broadcast(self, key: str, data: Any) -> None: ...

    # Context manager support
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *args) -> None: ...
```

### Exceptions

| Exception | Description |
|-----------|-------------|
| `PoolError` | Base class for pool errors |
| `PoolNotStarted` | Operation attempted before `start()` |
| `PoolAlreadyStarted` | `start()` called on running pool |
| `WorkerError(worker_id, message)` | Base class for worker errors |
| `WorkerException(worker_id, exception)` | Worker raised an exception |
| `WorkerCrashed(worker_id, details)` | Worker process/thread died |
| `WorkerTimeout(worker_id, timeout)` | Worker operation timed out |

### ThreadPool (`netrun.pool.thread`)

Multiple worker threads in the same process:

```python
from netrun.rpc.base import ChannelClosed
from netrun.pool.thread import ThreadPool

def my_worker(channel, worker_id):
    """Worker function running in a thread."""
    try:
        while True:
            key, data = channel.recv()
            result = process(data)
            channel.send("result", result)
    except ChannelClosed:
        pass  # Normal shutdown

async with ThreadPool(my_worker, num_workers=4) as pool:
    await pool.send(worker_id=0, key="task", data={"x": 1})
    msg = await pool.recv(timeout=5.0)
    print(f"Worker {msg.worker_id}: {msg.data}")
```

### MultiprocessPool (`netrun.pool.multiprocess`)

Multiple subprocesses, each with multiple worker threads:

```python
from netrun.pool.multiprocess import MultiprocessPool

# Worker function must be importable (defined at module level)
async with MultiprocessPool(
    worker_fn=my_worker,
    num_processes=2,
    threads_per_process=4,
    redirect_output=True,       # Capture stdout/stderr (default: True)
    buffer_output=True,         # Buffer output (default: True)
    output_flush_interval=0.1,  # Auto-flush interval in seconds
) as pool:
    # Total workers = num_processes * threads_per_process = 8
    # Worker IDs: 0-7 (flat addressing)

    await pool.send(worker_id=0, key="task", data={...})
    msg = await pool.recv()

    # Get captured output: list of (timestamp, is_stdout, text)
    output = await pool.flush_stdout(process_idx=0, timeout=5.0)
    all_output = await pool.flush_all_stdout(timeout=5.0)
```

**Worker ID Mapping:**
```
worker_id = process_idx * threads_per_process + thread_idx
```

### RemotePool (`netrun.pool.remote`)

Network-based pool hosting via WebSockets.

**Server:**
```python
from netrun.pool.remote import RemotePoolServer

server = RemotePoolServer()
server.register_worker("my_worker", my_worker_fn)

# Serve (blocking)
await server.serve("0.0.0.0", 8765)

# Or serve in background
async with server.serve_background("0.0.0.0", 8765):
    print(server.registered_workers)  # ["my_worker"]
    ...
```

**Client:**
```python
from netrun.pool.remote import RemotePoolClient

async with RemotePoolClient(
    url="ws://localhost:8765",
    worker_name="my_worker",
    num_processes=2,
    threads_per_process=4,
    redirect_output=True,
    buffer_output=True,
) as client:
    await client.send(0, "task", data)
    msg = await client.recv(timeout=5.0)

    # Get stdout from remote processes
    output = await client.flush_stdout(process_idx=0, timeout=5.0)
```

### SingleWorkerPool (`netrun.pool.aio`)

Single async coroutine worker in the main event loop:

```python
from netrun.rpc.base import ChannelClosed
from netrun.pool.aio import SingleWorkerPool

async def async_worker(channel, worker_id):
    """Async worker function (uses AsyncChannel)."""
    try:
        while True:
            key, data = await channel.recv()
            result = await async_process(data)
            await channel.send("result", result)
    except ChannelClosed:
        pass

async with SingleWorkerPool(async_worker) as pool:
    assert pool.num_workers == 1
    await pool.send(worker_id=0, key="task", data={...})
    msg = await pool.recv()
```

---

## ExecutionManager

High-level orchestration for executing functions across different pool types.

### Overview

ExecutionManager manages multiple pools and provides a unified interface for:
- Registering functions with workers
- Executing functions with automatic result collection
- Worker allocation strategies
- Job tracking

### Basic Usage

```python
from netrun.execution_manager import ExecutionManager
from netrun.pool.thread import ThreadPool
from netrun.pool.multiprocess import MultiprocessPool

manager = ExecutionManager({
    "thread_pool": (ThreadPool, {"num_workers": 4}),
    "process_pool": (MultiprocessPool, {
        "num_processes": 2,
        "threads_per_process": 2,
    }),
})

async with manager:
    # Register a function with all workers in a pool
    await manager.send_function_to_pool("thread_pool", "add", lambda x, y: x + y)

    # Or register with a specific worker
    await manager.send_function("thread_pool", worker_id=0, func_key="add", func=lambda x, y: x + y)

    # Execute the function
    result = await manager.run(
        pool_id="thread_pool",
        worker_id=0,
        func_import_path_or_key="add",
        send_channel=False,
        func_args=(1, 2),
        func_kwargs={},
    )

    print(result.result)  # 3
```

### JobResult

```python
@dataclass
class JobResult:
    timestamp_utc_submitted: datetime
    timestamp_utc_started: datetime
    timestamp_utc_completed: datetime
    func_import_path_or_key: str
    pool_id: str
    worker_id: int
    converted_to_str: bool  # True if result wasn't pickleable
    result: Any             # Function return value
```

### SubmittedJobInfo

For tracking in-flight jobs:

```python
@dataclass
class SubmittedJobInfo:
    run_id: str
    timestamp_utc_submitted: datetime
    timestamp_utc_started: datetime | None
    func_import_path_or_key: str
    pool_id: str
    worker_id: int
```

### Worker Allocation

```python
from netrun.execution_manager import RunAllocationMethod

# Allocate work automatically across workers
result = await manager.run_allocate(
    pool_worker_ids=[("thread_pool", 0), ("thread_pool", 1), ("thread_pool", 2)],
    allocation_method=RunAllocationMethod.LEAST_BUSY,  # or ROUND_ROBIN, RANDOM
    func_import_path_or_key="add",
    send_channel=False,
    func_args=(1, 2),
    func_kwargs={},
)
```

### Query Methods

```python
# Get pool information
manager.pools  # list of (pool_id, pool_type)
manager.started  # bool

# Get worker counts
manager.get_num_workers("thread_pool")  # int
manager.get_worker_ids("thread_pool")   # list[str] like ["thread_pool:0", "thread_pool:1", ...]

# Get active jobs for a worker
jobs = manager.get_worker_jobs("thread_pool", worker_id=0)  # list[SubmittedJobInfo]

# For MultiprocessPool: get process indices
manager.get_process_ids("process_pool")  # list[int]
```

### Stdout Capture

For pools with stdout capture enabled:

```python
# Get output from specific process
output = await manager.flush_pool_stdout("process_pool", process_idx=0, timeout=5.0)
# Returns: list[tuple[datetime, bool, str]]  # (timestamp, is_stdout, text)

# Get output from all processes
all_output = await manager.flush_all_pool_stdout("process_pool", timeout=5.0)
# Returns: dict[int, list[tuple[datetime, bool, str]]]
```

### Using Import Paths

Functions can be specified by import path (required for multiprocess/remote pools):

```python
result = await manager.run(
    pool_id="process_pool",
    worker_id=0,
    func_import_path_or_key="mymodule.myfunction",  # Will be imported
    send_channel=False,
    func_args=(1, 2),
    func_kwargs={},
)
```

### Function Preprocessor and Done Callback

The `func_preprocessor` and `func_done_callback` enable custom execution environments (like `NodeExecutionContext`) to be created inside workers without serialization issues.

**Why this design?**
- Context objects (like `NodeExecutionContext`) need to communicate back to the Net via RPC channels
- Wrapping functions with context *before* sending to workers causes pickling errors
- Instead, `func_preprocessor` runs *inside* the worker where it can create context locally

**func_preprocessor**: Transforms the function before execution. The preprocessor receives the original function and returns a new function with a different signature that accepts context-creation arguments.

**func_done_callback**: Called after execution completes with the same arguments that were passed to the function. Used for cleanup like flushing print buffers.

```python
# Example: Creating NodeExecutionContext inside workers

def my_preprocessor(exec_node_func):
    """Transform exec_node_func(ctx, packets) -> wrapped(epoch_id, node_name, packets, ...)"""
    def wrapped(epoch_id, node_name, packets, channel, ...):
        # Create context locally in worker (no serialization needed)
        ctx = NodeExecutionContext(epoch_id=epoch_id, channel=channel, ...)
        return exec_node_func(ctx, packets)
    return wrapped

def my_done_callback(epoch_id, node_name, packets, channel, ..., result=None):
    """Called after execution - flush print buffer, cleanup, etc."""
    # Flush any captured prints back to Net via channel
    channel.send("PRINT_BUFFER", ctx.get_print_buffer())

manager = ExecutionManager({
    "thread_pool": (ThreadPool, {
        "num_workers": 4,
        "func_preprocessor": my_preprocessor,
        "func_done_callback": my_done_callback,
    }),
})
```

### Remote ExecutionManager

For remote execution, use `create_execution_manager_server`:

```python
from netrun.execution_manager import create_execution_manager_server
from netrun.pool.remote import RemotePoolClient

# Server side
server = create_execution_manager_server(
    worker_name="executor",
    func_preprocessor=None,  # Optional
)
async with server.serve_background("0.0.0.0", 8765):
    ...

# Client side
manager = ExecutionManager({
    "remote": (RemotePoolClient, {
        "url": "ws://localhost:8765",
        "worker_name": "executor",
        "num_processes": 2,
        "threads_per_process": 4,
    }),
})
```

---

## Net Configuration

Comprehensive Pydantic models for defining flow-based networks. All models support JSON serialization and conversion to `netrun-sim` types.

### Port Configuration

```python
from netrun.net.config import (
    PortConfig,
    PortSlotSpecInfiniteConfig,
    PortSlotSpecFiniteConfig,
)

# Infinite capacity (default)
port = PortConfig(slots_spec=PortSlotSpecInfiniteConfig())

# Limited capacity
port = PortConfig(slots_spec=PortSlotSpecFiniteConfig(capacity=5))

# Convert to netrun-sim
netrun_sim_port = port.to_netrun_sim()
```

### Port State Predicates

For salvo condition terms:

```python
from netrun.net.config import (
    PortStateEmptyConfig,
    PortStateFullConfig,
    PortStateNonEmptyConfig,
    PortStateNonFullConfig,
    PortStateEqualsConfig,
    PortStateLessThanConfig,
    PortStateGreaterThanConfig,
    PortStateEqualsOrLessThanConfig,
    PortStateEqualsOrGreaterThanConfig,
)

# Examples
empty = PortStateEmptyConfig()           # Port has 0 packets
non_empty = PortStateNonEmptyConfig()    # Port has >= 1 packet
equals_5 = PortStateEqualsConfig(n=5)    # Port has exactly 5 packets
```

### Salvo Conditions

```python
from netrun.net.config import (
    SalvoConditionConfig,
    SalvoConditionTermPortConfig,
    SalvoConditionTermAndConfig,
    SalvoConditionTermOrConfig,
    SalvoConditionTermNotConfig,
    SalvoConditionTermTrueConfig,
    SalvoConditionTermFalseConfig,
    MaxSalvosInfiniteConfig,
    MaxSalvosFiniteConfig,
    PacketCountAllConfig,
    PacketCountNConfig,
)

# Example: trigger when both input ports are non-empty
condition = SalvoConditionConfig(
    max_salvos=MaxSalvosFiniteConfig(n=1),  # Trigger at most once
    ports={
        "in1": PacketCountAllConfig(),  # Take all packets from in1
        "in2": PacketCountNConfig(n=1), # Take at most 1 packet from in2
    },
    term=SalvoConditionTermAndConfig(terms=[
        SalvoConditionTermPortConfig(port_name="in1", port_state=PortStateNonEmptyConfig()),
        SalvoConditionTermPortConfig(port_name="in2", port_state=PortStateNonEmptyConfig()),
    ]),
)

# Convert to netrun-sim
netrun_sim_condition = condition.to_netrun_sim()
```

### Node Graph Configuration

```python
from netrun.net.config import NodeGraphConfig

node = NodeGraphConfig(
    in_ports={"in1": PortConfig(), "in2": PortConfig()},
    out_ports={"out": PortConfig()},
    in_salvo_conditions={"default": condition},
    out_salvo_conditions={"send": output_condition},
)
```

### Edge Configuration

```python
from netrun.net.config import EdgeConfig, PortRefConfig

# Full form
edge = EdgeConfig(
    source=PortRefConfig(node_name="A", port_type="output", port_name="out"),
    target=PortRefConfig(node_name="B", port_type="input", port_name="in"),
)

# Shorthand form
edge = EdgeConfig(
    source_str="A.out",
    target_str="B.in",
)
```

### Graph Configuration

```python
from netrun.net.config import GraphConfig

graph_config = GraphConfig(
    nodes={
        "NodeA": NodeGraphConfig(...),
        "NodeB": NodeGraphConfig(...),
    },
    edges=[
        EdgeConfig(source_str="NodeA.out", target_str="NodeB.in"),
    ],
)

# Convert to netrun-sim Graph
netrun_sim_graph = graph_config.to_netrun_sim()
```

### Pool Configuration

```python
from netrun.net.config import (
    MainPoolConfig,
    ThreadPoolConfig,
    MultiprocessPoolConfig,
    RemotePoolConfig,
)

# Main thread/event loop
main = MainPoolConfig()

# Thread pool
threads = ThreadPoolConfig(num_workers=4)

# Multiprocess pool
processes = MultiprocessPoolConfig(
    num_processes=2,
    threads_per_process=4,
    redirect_output=True,
    buffer_output=True,
)

# Remote pool
remote = RemotePoolConfig(
    url="ws://localhost:8765",
    worker_name="executor",
    num_processes=2,
    threads_per_process=4,
)
```

### Node Execution Configuration

```python
from netrun.net.config import NodeExecutionConfig

exec_config = NodeExecutionConfig(
    exec_node_func="mymodule.exec_func",      # Import path or callable
    start_node_func="mymodule.start_func",    # Optional
    stop_node_func="mymodule.stop_func",      # Optional
    on_node_failure="mymodule.failure_func",  # Optional

    pool="thread_pool",                        # Pool to run in (None = main)
    defer_startup=False,                       # Defer node startup
    max_parallel_epochs=None,                  # Max simultaneous epochs
    rate_limit_per_second=None,                # Max epoch starts per second

    defer_net_actions=False,                   # Buffer actions until success
    retries=0,                                 # Retry attempts on failure
    retry_wait=0.0,                            # Seconds between retries
    timeout=None,                              # Epoch timeout in seconds

    capture_prints=True,                       # Capture print statements
)
```

### Complete Net Configuration

```python
from netrun.net.config import NetConfig

net_config = NetConfig(
    pools={
        "thread_pool": ThreadPoolConfig(num_workers=4),
        "process_pool": MultiprocessPoolConfig(num_processes=2, threads_per_process=2),
    },
    graph=graph_config,
    node_execution_configs={
        "NodeA": NodeExecutionConfig(exec_node_func="mymodule.node_a_exec"),
        "NodeB": NodeExecutionConfig(exec_node_func="mymodule.node_b_exec", pool="thread_pool"),
    },
    dead_letter_memory=True,     # Store failed packets in memory
    dead_letter_file=None,       # Or path to file
    dead_letter_callback=None,   # Or callback function
)
```

---

# PARTIALLY IMPLEMENTED

---

## Net Class (Partial)

The `Net` class provides integration with `netrun-sim`. Currently has basic structure but execution logic is incomplete.

### Current Implementation

```python
from netrun.net import Net
from netrun.net.config import NetConfig

net = Net(config=net_config)

# Properties
net.config      # NetConfig
net.graph       # netrun_sim.Graph
net.pools       # list[tuple[str, PoolType]]
net.started     # bool

# Methods (implemented but incomplete)
net.start_pools()        # Start the execution manager
net.start()              # Start the net
net.stop()               # Stop the net
await net.async_run_step()  # Run one simulation step
net.run_step()           # Sync wrapper
```

### Context Classes (Stubs)

```python
from netrun.net import NodeExecutionContext, NodeFailureContext

# Both are empty stubs - implementation pending
```

### What's Missing

- Actual node function execution
- Packet value management integration
- Error handling and retries
- Epoch lifecycle management
- Most async/sync API methods

---

# PLANNED FEATURES

---

## Node Execution (Planned)

### Architecture: Context Creation in Workers

Node execution functions have signature `exec_node_func(ctx, packets)` where `ctx` is a `NodeExecutionContext`. The context provides methods like `ctx.create_packet()`, `ctx.consume_packet()`, and `ctx.print()` that communicate back to the Net.

**The problem:** The context needs an RPC channel to communicate with the Net, but wrapping functions with context before sending to workers causes pickling errors.

**The solution:** Use `func_preprocessor` and `func_done_callback` (see ExecutionManager section) to create context *inside* workers:

```python
# 1. User writes node function
def my_node(ctx: NodeExecutionContext, packets):
    value = ctx.consume_packet(packets["in"][0])
    ctx.print(f"Processing {value}")  # Captured, not printed
    out = ctx.create_packet(value * 2)
    ctx.load_output_port("out", out)

# 2. Net configures ExecutionManager with preprocessor
def create_context_preprocessor(exec_node_func):
    def wrapped(epoch_id, node_name, packets, channel):
        ctx = NodeExecutionContext(epoch_id, node_name, channel)
        return exec_node_func(ctx, packets)
    return wrapped

def flush_context_callback(epoch_id, node_name, packets, channel, result=None):
    # Flush captured prints back to Net
    pass

# 3. ExecutionManager.run() is called with context-creation args
await manager.run(
    pool_id="workers",
    worker_id=0,
    func_import_path_or_key="mymodule.my_node",
    send_channel=True,  # Channel passed to wrapped function
    func_args=(epoch_id, node_name, packets),
    func_kwargs={},
)
```

### NodeExecutionContext

The `ctx` object passed to `exec_node_func`:

```python
class NodeExecutionContext:
    epoch_id: EpochID
    node_name: str
    retry_count: int                    # Current retry attempt (0 = first)
    retry_timestamps: list[datetime]    # Previous retry timestamps
    retry_exceptions: list[Exception]   # Previous retry exceptions

    # Access to the Net (use sparingly)
    _net: Net

    # Packet operations (sync or async depending on exec_node_func)
    def create_packet(value: Any) -> Packet
    def create_packet_from_value_func(func: Callable[[], Any]) -> Packet
    def consume_packet(packet: Packet) -> Any
    def load_output_port(port_name: str, packet: Packet) -> None
    def send_output_salvo(salvo_condition_name: str) -> None

    # Epoch control
    def cancel_epoch() -> NoReturn  # Raises EpochCancelled
```

### NodeFailureContext

```python
class NodeFailureContext:
    epoch_id: EpochID
    node_name: str
    retry_count: int
    retry_timestamps: list[datetime]
    retry_exceptions: list[Exception]

    input_salvo: dict[str, list[Packet]]
    packet_values: dict[PacketID, Any]
```

### Execution Flow

1. Input salvo triggers epoch creation (via `netrun-sim`)
2. Epoch transitions to Running state
3. `exec_node_func(ctx, packets)` is called
4. Node creates output packets, loads to ports, sends salvos
5. Epoch finishes (or fails/times out)

---

## Packets and Values (Planned)

### Creating Packets

```python
# Direct value
packet = ctx.create_packet({"key": "value"})

# Value function (called on consumption)
packet = ctx.create_packet_from_value_func(lambda: fetch_from_s3(key))
```

### Consuming Packets

```python
value = ctx.consume_packet(packet)
```

### Deferred Packets

When `defer_net_actions=True`, packets are not committed until successful completion. This enables clean retries.

---

## Error Handling and Retries (Planned)

### Retry Configuration

```python
NodeExecutionConfig(
    retries=3,
    retry_wait=1.0,
    defer_net_actions=True,  # Required for retries
)
```

### Net-Level Error Handling

```python
NetConfig(
    on_error="pause",  # "continue", "pause", "raise"
)
```

### Dead Letter Queue

Failed packets stored for inspection:

```python
NetConfig(
    dead_letter_memory=True,
    dead_letter_file="./dlq/",
    dead_letter_callback=my_callback,
)
```

---

## Logging and History (Planned)

### Event History

Every `NetAction` and `NetEvent` recorded:

```python
NetConfig(
    history_max_size=10000,
    history_file="./history.jsonl",
)
```

### Node-Level Logging

Print capture per node:

```python
NodeExecutionConfig(
    capture_prints=True,
)

# Access logs
net.get_node_log("my_node")
net.get_epoch_log(epoch_id)
```

---

## Port Types (Planned)

Type checking on packet values:

```python
# By class name
port_type = "DataFrame"

# By class
port_type = pandas.DataFrame

# With isinstance
port_type = {"class": MyClass, "isinstance": True}
```

---

## Node Factories (Planned)

Generate node specs and execution functions:

```python
def get_node_spec(**args) -> dict:
    return {"name": "...", "in_ports": {...}, ...}

def get_node_funcs(**args) -> tuple:
    return (exec_func, start_func, stop_func, failed_func)
```

---

## Checkpointing and State Serialization (Planned)

### Saving State

```python
net.pause()
net.save_checkpoint("./checkpoint/")
```

### Loading State

```python
net = Net.load_checkpoint("./checkpoint/")
```

---

## Error Types

```python
# netrun-specific errors
class NetrunError(Exception): ...
class PacketTypeMismatch(NetrunError): ...
class ValueFunctionFailed(NetrunError): ...
class NodeExecutionFailed(NetrunError): ...
class EpochTimeout(NetrunError): ...
class EpochCancelled(NetrunError): ...
```
