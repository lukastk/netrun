# netrun Project Specification

`netrun` is a Python package for running flow-based development (FBD) graphs. It uses `netrun-sim` to manage the core network runtime logic while providing actual node execution, packet handling, and higher-level APIs.

## Table of Contents

1. [Overview](#overview)
2. [The Net Class](#the-net-class)
3. [Node Configuration](#node-configuration)
4. [Node Execution](#node-execution)
5. [Packets and Values](#packets-and-values)
6. [Pools (Thread and Process)](#pools-thread-and-process)
7. [Error Handling and Retries](#error-handling-and-retries)
8. [Logging and History](#logging-and-history)
9. [Port Types](#port-types)
10. [DSL Format](#dsl-format)
11. [Node Factories](#node-factories)
12. [Checkpointing and State Serialization](#checkpointing-and-state-serialization)

---

## Overview

`netrun` wraps `netrun-sim` to provide:
- Actual execution of node logic
- Packet value storage and retrieval
- Thread and process pool management
- Error handling with retries
- Comprehensive logging and history
- A human-readable DSL for net serialization
- Checkpointing and state restoration

The package hides `netrun-sim` from the user - all interactions appear as a single unified `netrun` API. Users should never need to interact with `NetSim` directly; all functionality is exposed through `Net` wrapper methods.

---

## The Net Class

### Construction

```python
from netrun import Net
from netrun_sim import Graph

graph = Graph(nodes, edges)
net = Net(graph, **config)
```

### Setting Node Execution Functions

```python
net.set_node_exec(
    node_name: str,
    exec_node_func: Callable,
    start_node_func: Optional[Callable] = None,
    stop_node_func: Optional[Callable] = None,
    exec_failed_node_func: Optional[Callable] = None,
)
```

**Function signatures:**

```python
def exec_node_func(ctx: NodeExecutionContext, packets: dict[str, list[Packet]]) -> None
async def exec_node_func(ctx: NodeExecutionContext, packets: dict[str, list[Packet]]) -> None

def start_node_func(net: Net) -> None
async def start_node_func(net: Net) -> None

def stop_node_func(net: Net) -> None
async def stop_node_func(net: Net) -> None

def exec_failed_node_func(ctx: NodeExecutionContext) -> None
async def exec_failed_node_func(ctx: NodeFailureContext) -> None
```

- `exec_node_func`: Main execution function, called when an epoch starts
- `start_node_func`: Called when the net starts (constructor)
- `stop_node_func`: Called when the net stops (destructor)
- `exec_failed_node_func`: Called after each failed execution attempt (including the last one after max retries)

All functions can be either sync or async.

**Note:** If a node does not have `exec_node_func` set, epochs will be created for it (entering `Startable` state) but will never be started. The net will continue running other nodes. Use `net.get_startable_epochs()` or `net.get_startable_epochs_by_node(node_name)` to query unstarted epochs.

### Setting Node Configuration

```python
net.set_node_config(
    node_name: str,
    **options
)
```

See [Node Configuration](#node-configuration) for available options.

### Running the Net

**Synchronous execution (main thread):**

```python
# Run one step: execute RunNetUntilBlocked, start ready epochs, wait for completion
net.run_step()

# Run until fully blocked (no more events possible)
net.start()
```

By default `start_epochs=True` in `run_step`, but if `start_epochs=False`, then no ready epochs will be started during the step.

**Asynchronous execution:**

```python
await net.async_run_step()
await net.async_start()
```

**Background execution (separate thread):**

```python
net.start(threaded=True)
net.run_step(threaded=True)

# Control methods for background execution:
net.wait_until_blocked()       # Block until net is blocked
await net.async_wait_until_blocked()
net.poll()                      # Check status (blocked, running, etc.)
net.pause()                     # Finish current epochs, don't start new ones
await net.async_pause()
net.stop()                      # Stop the net entirely
await net.async_stop()
```

### SIGINT Handling

When a SIGINT is received, the net performs the equivalent of `net.pause()`:
- All currently running epochs are allowed to finish
- No new epochs are started
- The net then stops gracefully

---

## Node Configuration

Configuration options set via `net.set_node_config()`:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `pool` | `str \| list[str]` | `None` | Thread/process pool(s) to run in (if `None`, runs in main thread/event loop) |
| `max_parallel_epochs` | `int` | `None` | Maximum simultaneous epochs for this node |
| `rate_limit_per_second` | `float` | `None` | Maximum epoch starts per second |
| `defer_net_actions` | `bool` | `False` | Buffer NetSim actions until successful completion |
| `retries` | `int` | `0` | Number of retry attempts on failure |
| `retry_wait` | `float` | `0` | Time to wait between retries |
| `timeout` | `float` | `None` | Epoch timeout in seconds (wall-clock from epoch start) |
| `dead_letter_queue` | `bool` | `True` | Store destroyed packets for inspection |
| `capture_stdout` | `bool` | `True` | Capture print statements to node log |

**Constraints:**
- If `retries > 0`, then `defer_net_actions` must be `True` (enforced at net creation)

---

## Node Execution

### NodeExecutionContext

The `ctx` object passed to `exec_node_func` provides:

```python
class NodeExecutionContext:
    epoch_id: EpochID
    node_name: str
    retry_count: int                    # Current retry attempt (0 = first attempt)
    retry_timestamps: list[datetime]    # Timestamps of previous retry attempts
    retry_exceptions: list[Exception]   # Exceptions from previous retries

    # Access to the Net (use sparingly)
    _net: Net

    # Packet operations (sync or async depending on exec_node_func)
    def create_packet(value: Any) -> Packet
    def create_packet_from_value_func(func: Callable[[], Any]) -> Packet
    def consume_packet(packet: Packet) -> Any
    def load_output_port(port_name: str, packet: Packet) -> None
    def send_output_salvo(salvo_condition_name: str) -> None

    # Epoch control
    def cancel_epoch() -> NoReturn  # Raises EpochCancelled exception
```

### NodeFailureContext

Similar to `NodeExecutionContext`, but does not have any packet operations or `cancel_epoch`, and also has references to packets and any received packet values.

```python
class NodeFailureContext:
    epoch_id: EpochID
    node_name: str
    retry_count: int                    # Current retry attempt (0 = first attempt)
    retry_timestamps: list[datetime]    # Timestamps of previous retry attempts
    retry_exceptions: list[Exception]   # Exceptions from previous retries

    input_salvo: dict[str, list[Packet]] # The input salvo
    packet_values: dict[PacketID, Any] # Contains the actual values that were received upon consumption in the epoch.
```

**Sync vs Async:**
- If `exec_node_func` is async, all `ctx` methods are async and must be awaited
- If `exec_node_func` is sync, all `ctx` methods are sync
- Mixing (e.g., passing async ctx to sync helper) is an error

### Execution Flow

1. Input salvo triggers epoch creation (via `netrun-sim`)
2. Epoch transitions to Running state
3. `exec_node_func(ctx, packets)` is called
   - `packets` is `dict[str, list[Packet]]` keyed by input port name
   - Packets must be explicitly consumed via `ctx.consume_packet()`
4. Node creates output packets, loads them to output ports, sends salvos
5. Epoch finishes (or fails/times out)

### Cancellation

From within a node:
```python
def exec_node_func(ctx, packets):
    if some_condition:
        ctx.cancel_epoch()  # Raises EpochCancelled
```

---

## Packets and Values

Packets are containers for values. Values are stored separately from the `netrun-sim` packet tracking.

### Creating Packets

```python
# Direct value
packet = ctx.create_packet({"key": "value"})

# Value function (called on consumption)
packet = ctx.create_packet_from_value_func(lambda: fetch_from_s3(key))
```

Value functions:
- Called lazily on consumption
- Can be sync or async
- **Must be pure/idempotent** (may be called multiple times on retry). This is critical: if a node with `defer_net_actions=True` fails and retries, the value function will be called again. Side effects (counters, external state mutations) will execute multiple times. This is not enforced at runtime but is a requirement for correct behavior.
- Timeout includes value function execution time

### Consuming Packets

```python
value = ctx.consume_packet(packet)
```

- Returns the stored value or calls the value function
- Removes value from storage
- Reports consumption to `netrun-sim`, unless `defer_net_actions` is `True` in which case the action will only be reported given that the epoch successfully runs.

### Packet Storage Configuration

```python
net = Net(graph,
    consumed_packet_storage=True,       # Store consumed packet values
    consumed_packet_storage_limit=1000, # Max packets to keep
)
```

Can also optionally store pickled packets in a folder, saved under their `PacketID`.

---

## Pools (Thread and Process)

### Defining Pools

```python
net = Net(graph,
    thread_pools={
        "io": {"size": 10},
        "compute": {"size": 4},
    },
    process_pools={
        "heavy": {"size": 4},
    },
)
```

### Assigning Nodes to Pools

```python
net.set_node_config("my_node", pool="io")
net.set_node_config("other_node", pool=["io", "compute"])  # Multiple pools
```

### Pool Selection

When a node is assigned to multiple pools, selection algorithm chooses which pool to use:
- Default: "least_busy" (pool with most available workers)
- Other algorithms can be configured

### start/stop Functions in Pools

By default, `start_node_func` and `stop_node_func` run once per thread/process in the pool.

For thread pools, optionally run once globally:
```python
net.set_node_config("my_node",
    pool="io",
    pool_init_mode="global"  # vs "per_worker" (default)
)
```

When `pool_init_mode="global"`, user is responsible for thread-safety.

### Process Pool Requirements

For process pools:
- **Packet values must be picklable.** If a non-picklable value is passed, a runtime error will be raised with a clear message.
- **Async node functions are supported.** Each worker process maintains its own event loop for executing async functions.

---

## Error Handling and Retries

### Retry Configuration

```python
net.set_node_config("my_node",
    retries=3,              # Max retry attempts
    defer_net_actions=True, # Required for retries
)
```

### Deferred Net Actions

When `defer_net_actions=True`:
- All packet operations (create, consume, load, send) are queued
- Queue is only committed to `netrun-sim` on successful completion
- On failure, queue is discarded (but kept for logging)
- Enables clean retry without orphaned packets

**DeferredPacket:** When `defer_net_actions=True`, `ctx.create_packet()` returns a `DeferredPacket` object instead of a regular `Packet`. This object behaves identically to a `Packet` for all node operations (loading to output ports, etc.), but:
- The actual `PacketID` is not assigned until the deferred actions are committed
- Accessing the `id` property on a `DeferredPacket` raises an exception to alert the user that the ID is not yet available
- On successful commit, all `DeferredPacket` objects are resolved to real packets with assigned IDs

**Unconsuming on Retry:** When a node fails and retries, `netrun` "unconsumes" packets by:
- Restoring the packet values so they can be consumed again
- Not reporting the consumption to `netrun-sim` (since actions are deferred)
- This is why value functions may be called multiple times on retry

### Failure Flow

1. `exec_node_func` raises exception
2. `exec_failed_node_func` called (if defined)
3. If retries remaining: retry from step 1
4. If no retries remaining: epoch cancelled, packets destroyed

### Net-Level Error Handling

```python
net = Net(graph,
    on_error="pause",         # Options: "continue", "pause", "raise". Default is "pause"
    error_callback=my_func,   # Called on any node error
)
```

Error handling modes:
- `"continue"`: Net continues running other nodes; failed epoch is cancelled
- `"pause"`: Net pauses (finishes running epochs, doesn't start new ones)
- `"raise"`: Net pauses first, then raises the exception to the caller

### Dead Letter Queue

Failed packets are stored for inspection:

```python
net = Net(graph,
    dead_letter_queue="memory",  # "memory", "file", or callback
    dead_letter_path="./dlq/",   # For "file" mode
    dead_letter_callback=func,   # For callback mode
)
```

Access:
```python
net.dead_letter_queue.get_all()
net.dead_letter_queue.get_by_node("my_node")
```

---

## Logging and History

### Event History

Every `NetAction` and `NetEvent` is recorded with traceability:

```python
net = Net(graph,
    history_max_size=10000,          # Max events in memory
    history_file="./history.jsonl",  # Persist to file
    history_chunk_size=100,          # Write in chunks
    history_flush_on_pause=True,     # Flush when paused/blocked
)
```

History format (JSONL):
```json
{"type": "action", "action": "RunNetUntilBlocked", "timestamp": "...", "id": "..."}
{"type": "event", "event": "PacketCreated", "action_id": "...", "timestamp": "...", ...}
```

### Node-Level Logging

Each node has its own log capturing print statements:

```python
# In node execution
def exec_node_func(ctx, packets):
    print("Processing...")  # Captured to node log

# Access logs
net.get_node_log("my_node")
net.get_epoch_log(epoch_id)
```

**Implementation:** Print capture is done by injecting a custom `print` into the function's `__globals__`. Can be disabled per-node:

```python
net.set_node_config("my_node", capture_stdout=False)
```

### Configuring stdout echo

```python
net.set_node_config("my_node",
    capture_stdout=True,
    echo_stdout=True,  # Also print to actual stdout
)
```

---

## Port Types

### Defining Port Types

```python
# By class name (string)
port_type = "DataFrame"  # packet_value.__class__.__name__ == "DataFrame"

# By actual class
port_type = pandas.DataFrame

# With isinstance (default)
port_type = {"class": MyClass, "isinstance": True}

# Exact type match
port_type = {"class": MyClass, "isinstance": False}

# Subclass check
port_type = {"class": MyClass, "subclass": True}
```

### Type Checking

Type checking occurs:
- **Input ports:** When a packet is consumed via `ctx.consume_packet()` (not at epoch creation, since the value may not be known until consumption)
- **Output ports:** When a packet is loaded into an output port via `ctx.load_output_port()`

On type mismatch, a `PacketTypeMismatch` error is raised (defined at `netrun` level, not `netrun-sim`).

---

## DSL Format

Nets can be fully serialized to a human-readable DSL. TOML is the primary format.

### Basic Structure

```toml
[net]
# Net-level configuration
on_error = "continue"
history_file = "./history.jsonl"

[net.thread_pools.io]
size = 10

[net.process_pools.heavy]
size = 4

[net.meta]
# Freeform metadata
author = "..."
version = "1.0"
```

### Node Definition

```toml
[nodes.MyNode]
in_ports = ["in1", "in2"]
out_ports = ["out"]

# Or with port configuration
[nodes.MyNode.in_ports.in1]
slots = 5
type = "DataFrame"

[nodes.MyNode.in_salvo_conditions.default]
when = "nonempty(in1) and nonempty(in2)"
ports = ["in1", "in2"]  # Optional; if omitted, all input ports are included

[nodes.MyNode.out_salvo_conditions.send]
when = "nonempty(out)"
max_salvos = "infinite"  # or a positive integer

[nodes.MyNode.options]
pool = "io"
retries = 3
defer_net_actions = true
timeout = 30.0

[nodes.MyNode.meta]
description = "Processes data"
```

### Node Execution Functions

**Direct import path:**
```toml
[nodes.MyNode]
exec_node_func = "my_module.my_exec_func"
start_node_func = "my_module.my_start_func"
stop_node_func = "my_module.my_stop_func"
exec_failed_node_func = "my_module.my_failed_func"
```

**Using factories:**
```toml
[nodes.MyNode]
factory = "my_module.my_factory"
factory_args = { num_inputs = 3, timeout = 30 }
```

### Edges

```toml
edges = [
    { from = "A.out", to = "B.in" },
    { from = "B.out", to = "C.in1" },
    { from = "B.out", to = "D.in" },
]

# Or with metadata
[[edges]]
from = "A.out"
to = "B.in"
[edges.meta]
label = "main flow"
```

### Salvo Condition Expression Language

The `when` field uses expressions equivalent to `netrun-sim` `SalvoConditionTerm`:

| Expression | Meaning |
|------------|---------|
| `nonempty(port)` | Port has at least one packet |
| `empty(port)` | Port has no packets |
| `full(port)` | Port is at capacity |
| `count(port) >= N` | Port has at least N packets |
| `count(port) == N` | Port has exactly N packets |
| `expr1 and expr2` | Both conditions true |
| `expr1 or expr2` | Either condition true |
| `not expr` | Condition is false |

Parentheses for grouping: `(nonempty(a) or nonempty(b)) and not full(c)`

---

## Node Factories

Node factories are Python modules that generate node specifications and execution functions.

### Structure

A node factory module must contain:

```python
# my_factory.py

def get_node_spec(**args) -> dict:
    """
    Returns a dictionary that can be passed as kwargs to netrun_sim.Node.
    Should be lightweight and suitable for UI introspection.
    """
    return {
        "name": "...",
        "in_ports": {...},
        "out_ports": {...},
        "in_salvo_conditions": {...},
        "out_salvo_conditions": {...},
    }

def get_node_funcs(**args) -> tuple:
    """
    Returns (exec_node_func, start_node_func, stop_node_func, exec_failed_node_func).
    Any can be None.
    """
    return (exec_func, start_func, stop_func, failed_func)
```

Both functions take the same arguments.

### Usage

```python
node_spec, exec_func, start_func, stop_func, failed_func = Net.from_factory(
    "my_module.my_factory",
    num_inputs=3,
    timeout=30,
)
```

### Serialization

For serialization, factory arguments must be JSON-serializable or import paths (strings).

Example factory with callable argument:
```python
def get_node_spec(func: str | Callable, timeout: int = 30):
    if isinstance(func, str):
        func = import_from_path(func)
    # Inspect func to determine ports...
    return {...}
```

In DSL:
```toml
[nodes.MyNode]
factory = "my_module.func_to_node"
factory_args = { func = "my_module.my_processor", timeout = 30 }
```

---

## Checkpointing and State Serialization

### Saving State

**Important:** The net must be paused before saving a checkpoint. Call `net.pause()` first, or an exception will be raised. This ensures no epochs are in a running state during serialization.

```python
# Pause the net first
net.pause()

# Save complete state
net.save_checkpoint("./checkpoint/")

# This creates:
# ./checkpoint/net_state.json    - Serialized NetSim state
# ./checkpoint/packets.pkl       - Pickled packet values
# ./checkpoint/history.jsonl     - Event history (if configured)
```

### Loading State

```python
net = Net.load_checkpoint("./checkpoint/")
```

### Partial Serialization

```python
# Just the net definition (no runtime state)
net.save_definition("./net.toml")

# Load definition only
net = Net.load_definition("./net.toml")
```

---

## Error Types

`netrun` extends `netrun-sim` errors using inheritance rather than duplicating definitions. All `netrun-sim` error types are re-exported for convenience, and `netrun`-specific errors inherit from a common base.

```python
# netrun-sim errors are re-exported
from netrun import GraphValidationError, NetActionError  # from netrun-sim

# netrun-specific errors extend a common base
class NetrunError(Exception):
    """Base class for netrun-specific errors."""
    pass

class PacketTypeMismatch(NetrunError): ...
class ValueFunctionFailed(NetrunError): ...
class NodeExecutionFailed(NetrunError): ...
class EpochTimeout(NetrunError): ...
class EpochCancelled(NetrunError): ...
```

| Error | Description |
|-------|-------------|
| `PacketTypeMismatch` | Packet value doesn't match port type |
| `ValueFunctionFailed` | Value function raised an exception |
| `NodeExecutionFailed` | Node exec function raised an exception |
| `EpochTimeout` | Epoch exceeded configured timeout |
| `EpochCancelled` | Epoch was cancelled (via `ctx.cancel_epoch()`) |

---

## Example: Complete Net

```python
from netrun import Net
from netrun_sim import Graph, Node, Edge, PortRef, PortType

# Define graph
nodes = [
    Node(name="Source", out_ports={"out": {...}}, ...),
    Node(name="Process", in_ports={"in": {...}}, out_ports={"out": {...}}, ...),
    Node(name="Sink", in_ports={"in": {...}}, ...),
]
edges = [
    Edge(source=PortRef("Source", PortType.Output, "out"),
         target=PortRef("Process", PortType.Input, "in")),
    Edge(source=PortRef("Process", PortType.Output, "out"),
         target=PortRef("Sink", PortType.Input, "in")),
]
graph = Graph(nodes, edges)

# Create net
net = Net(graph,
    thread_pools={"main": {"size": 4}},
    history_file="./history.jsonl",
)

# Configure nodes
net.set_node_exec("Source", source_exec)
net.set_node_exec("Process", process_exec, start_func=init_processor)
net.set_node_exec("Sink", sink_exec)

net.set_node_config("Process",
    pool="main",
    retries=2,
    defer_net_actions=True,
    timeout=30.0,
)

# Run
net.start()
```
