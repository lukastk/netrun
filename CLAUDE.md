# netrun - Flow-Based Development Runtime

This repository contains the **netrun** project, a flow-based development (FBD) runtime system.

## Project Structure

The project is split into two main components:

### netrun-sim (Simulation Engine)

`netrun-sim` is a Rust library that simulates the flow of packets through a network of interconnected nodes. It does **not** execute actual node logic or manage packet data—instead, it tracks packet locations, validates flow conditions, and manages the lifecycle of node executions (called "epochs").

This separation of concerns allows the actual execution and data storage to be implemented independently of the flow mechanics.

### netrun (Runtime)

`netrun` is a pure Python package that will be built on top of `netrun-sim`. It provides:
- RPC (Remote Procedure Call) communication primitives
- Worker pool management (threads, processes, remote)
- High-level execution orchestration via ExecutionManager
- (Planned) Integration with netrun-sim for flow-based execution

**See `netrun/PROJECT_SPEC.md` for the full specification.**

**Important:** The `netrun` package uses **nblite** for literate programming. Before writing any code for `netrun`, you **must** read `netrun/NBLITE_INSTRUCTIONS.md` carefully. Key points:
- Source code lives in `.pct.py` files (percent notebooks), not in the exported Python modules
- Never edit files in `src/netrun/` directly - they are auto-generated
- After editing `.pct.py` files, run `nbl export --reverse` then `nbl export`

## Repository Structure

```
repo/
├── CLAUDE.md               # This file
├── README.md               # Project README
├── netrun-sim/             # Simulation engine (Rust + Python bindings)
│   ├── Cargo.toml          # Rust workspace root
│   ├── core/               # Rust library
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   │   ├── lib.rs      # Module exports
│   │   │   ├── _utils.rs   # Utility functions
│   │   │   ├── graph.rs    # Graph topology types
│   │   │   ├── graph_tests.rs  # Graph tests (separate file)
│   │   │   ├── net.rs      # Network runtime state
│   │   │   └── net_tests.rs    # Net tests (separate file)
│   │   ├── tests/          # Integration tests
│   │   └── examples/       # Rust examples
│   └── python/             # Python bindings (PyO3)
│       ├── Cargo.toml      # PyO3 crate
│       ├── pyproject.toml  # Maturin config
│       ├── src/            # Rust binding code
│       ├── python/         # Python package
│       │   └── netrun_sim/
│       └── examples/       # Python examples
└── netrun/                 # Runtime (pure Python, nblite project)
    ├── PROJECT_SPEC.md     # Full specification
    ├── NBLITE_INSTRUCTIONS.md  # How to write code (READ THIS FIRST)
    ├── nblite.toml         # nblite configuration
    ├── nbs/                # Jupyter notebooks (.ipynb)
    │   ├── netrun/         # Source notebooks
    │   └── tests/          # Test notebooks
    ├── pts/                # Percent notebooks (.pct.py) - EDIT THESE
    │   ├── netrun/         # Source percent notebooks
    │   └── tests/          # Test percent notebooks
    └── src/                # Auto-generated code (DO NOT EDIT)
        ├── netrun/         # Generated Python package
        └── tests/          # Generated test files
```

---

# netrun Package Documentation

## Current Implementation Status

### Fully Implemented

1. **RPC Layer** (`netrun.rpc`) - Bidirectional message-passing channels
2. **Pool Layer** (`netrun.pool`) - Worker pool management
3. **ExecutionManager** (`netrun.execution_manager`) - High-level execution orchestration
4. **Storage** (`netrun.storage`) - Packet value storage

### Not Yet Implemented

1. **Net Module** (`netrun.net`) - Integration with netrun-sim (stub only)

---

## Module Structure

### Internal Utilities (`netrun._iutils`)

- `_base` - Timestamp generation, patching decorators
- `hashing` - Hash computation utilities

### Storage (`netrun.storage`)

- `PacketStore` - Thread-safe storage for packet values
- `LazyPacketValueSpec` - Lazy value specification for deferred evaluation
- `PacketStoreConfig` - Configuration for hashing and evaluation

### RPC Layer (`netrun.rpc`)

The RPC layer provides bidirectional (key, data) message passing between components.

**Base Classes** (`netrun.rpc.base`):
- `RPCChannel` - Protocol for async bidirectional message passing
- `SyncRPCChannel` - Protocol for sync bidirectional message passing
- Exceptions: `RPCError`, `ChannelClosed`, `ChannelBroken`, `RecvTimeout`

**Implementations**:

| Module | Classes | Use Case |
|--------|---------|----------|
| `rpc.aio` | `AsyncChannel` | Async task communication via `asyncio.Queue` |
| `rpc.thread` | `ThreadChannel`, `SyncThreadChannel` | Thread communication via `queue.Queue` |
| `rpc.multiprocess` | `ProcessChannel`, `SyncProcessChannel` | Process communication via `multiprocessing.Queue` |
| `rpc.remote` | `WebSocketChannel` | Network communication via WebSockets |

### Pool Layer (`netrun.pool`)

The Pool layer manages collections of workers that process messages.

**Base Classes** (`netrun.pool.base`):
- `Pool` - Protocol for worker pools
- `WorkerMessage` - Message from a worker (worker_id, key, data)
- `WorkerFn` - Type for worker function: `Callable[[SyncRPCChannel, int], None]`
- Exceptions: `PoolError`, `PoolNotStarted`, `PoolAlreadyStarted`, `WorkerException`, `WorkerCrashed`, `WorkerTimeout`

**Implementations**:

| Module | Class | Description |
|--------|-------|-------------|
| `pool.thread` | `ThreadPool` | Multiple worker threads in same process |
| `pool.multiprocess` | `MultiprocessPool` | Multiple subprocesses, each with worker threads |
| `pool.remote` | `RemotePoolServer`, `RemotePoolClient` | Network-based pool hosting via WebSockets |
| `pool.aio` | `SingleWorkerPool` | Single async coroutine in main event loop |

**Common Pool API**:
```python
pool = ThreadPool(worker_fn, num_workers=4)
await pool.start()

await pool.send(worker_id=0, key="task", data={"x": 1})
msg = await pool.recv(timeout=5.0)  # Returns WorkerMessage
await pool.broadcast(key="config", data={...})  # Send to all workers

result = await pool.try_recv()  # Returns None if no message

await pool.close()

# Or use as context manager:
async with ThreadPool(worker_fn, num_workers=4) as pool:
    ...
```

**MultiprocessPool Features**:
- stdout/stderr capture with timestamps
- Output buffering with configurable flush intervals
- `flush_stdout(process_idx)` / `flush_all_stdout()` methods

**RemotePool Usage**:
```python
# Server side
server = RemotePoolServer()
server.register_worker("my_worker", worker_fn)
async with server.serve_background("0.0.0.0", 8765):
    ...

# Client side
async with RemotePoolClient("ws://localhost:8765") as client:
    await client.create_pool("my_worker", num_processes=2)
    await client.send(0, "task", data)
    msg = await client.recv()
```

### ExecutionManager (`netrun.execution_manager`)

High-level orchestration for executing functions across different pool types.

**Key Classes**:
- `ExecutionManager` - Main orchestrator
- `JobResult` - Result from job execution (timestamps, result, print buffer)
- `RunAllocationMethod` - Worker selection strategy (ROUND_ROBIN, RANDOM, LEAST_BUSY)

**Usage**:
```python
manager = ExecutionManager({
    "thread_pool": (ThreadPool, {"num_workers": 4}),
    "process_pool": (MultiprocessPool, {"num_processes": 2, "threads_per_process": 2}),
})

async with manager:
    # Send a function to workers
    await manager.send_function_to_pool("thread_pool", "my_func", my_function)

    # Run the function
    result = await manager.run(
        pool_id="thread_pool",
        worker_id=0,
        func_import_path_or_key="my_func",
        send_channel=False,
        func_args=(1, 2),
        func_kwargs={"x": 3},
    )

    print(result.result)  # Function return value
    print(result.print_buffer)  # Captured print statements
```

**ExecutionManager Protocol Keys**:
- `RUN` - Execute a function
- `SEND_FUNCTION` - Register a function by key
- `UP_RUN_STARTED` - Confirmation function started
- `UP_RUN_RESPONSE` - Return result
- `UP_PRINT_BUFFER` - Captured print statements

---

## Development Workflow

### Editing Code

1. Edit `.pct.py` files in `pts/netrun/` or `pts/tests/`
2. Export to notebooks: `nbl export --reverse`
3. Export to Python modules: `nbl export`

**Never edit files in `src/` directly** - they are auto-generated.

### Running Tests

```bash
cd netrun

# Run all tests
uv run pytest src/tests/

# Run specific test modules
uv run pytest src/tests/pool/test_thread.py -v
uv run pytest src/tests/execution_manager/ -v

# Run with output
uv run pytest src/tests/pool/test_multiprocess.py -v -s
```

### Building

```bash
cd netrun
uv sync  # Install dependencies
```

### Code Quality Guidelines

- **No hacks or workarounds**: If you find yourself writing code like `time.sleep(0.01)` to "get different timestamps" or similar workarounds, STOP and discuss with the user. There's likely a better design that captures the data properly at the source.
- **Capture data at the source**: Timestamps, metadata, and context should be captured when events occur, not approximated later. For example, `ctx.print()` should capture the timestamp when called, not when the buffer is flushed.
- **Ask before implementing workarounds**: If the current design doesn't support what you need, propose a design change rather than working around it.

---

# netrun-sim Documentation

## Overview

The `netrun-sim` library simulates packet flow through a network. It is designed to be used by external code (like `netrun`) that:

1. Defines the graph topology (nodes, ports, edges)
2. Handles actual node execution logic
3. Manages packet data/payloads
4. Responds to network events

## Core Concepts

### Graph (`graph.rs`)

The `Graph` represents the static topology of the network:

- **Nodes** (`Node`): Processing units with input and output ports
- **Ports** (`Port`): Connection points on nodes, either input or output
  - Each port has a `slots_spec` defining capacity (`Infinite` or `Finite(n)`)
- **Edges** (`Edge`): Connections between output ports of one node and input ports of another
- **Salvo Conditions** (`SalvoCondition`): Rules that define when packets can trigger an epoch or be sent

### Net (`net.rs`)

The `NetSim` represents the runtime state of a network:

- **Packets** (`Packet`): Units that flow through the network
  - Identified by `PacketID` (ULID)
  - Have a `location` tracking where they are
- **Epochs** (`Epoch`): Execution instances of a node
  - A single node can have multiple simultaneous epochs
  - Lifecycle: `Startable` → `Running` → `Finished`
- **Salvos** (`Salvo`): Collections of packets that enter or exit a node together

### Packet Locations

Packets can be in one of five locations:

```rust
enum PacketLocation {
    Node(EpochID),           // Inside a running/startable epoch
    InputPort(NodeName, PortName),  // Waiting at a node's input port
    OutputPort(EpochID, PortName),  // Loaded into an epoch's output port
    Edge(Edge),              // In transit between nodes
    OutsideNet,              // External to the network
}
```

### Salvo Conditions

Salvo conditions define when packets can trigger actions:

- **Input Salvo Conditions**: Define when packets at input ports can trigger a new epoch
- **Output Salvo Conditions**: Define when packets at output ports can be sent out

Each condition has:
- `term`: A boolean expression over port states (empty, full, equals N, etc.)
- `ports`: Which ports' packets are included when the condition triggers
- `max_salvos`: Maximum number of times this condition can trigger (must be 1 for input salvos)

## Flow Mechanics

### Automatic Flow (`run_step` / `run_until_blocked`)

When `RunStep` is called (via `do_action(NetAction::RunStep)` or the convenience method `net.run_step()`), the network automatically:

1. **Moves packets from edges to input ports**
   - Checks if the destination port has available slots
   - Respects port capacity limits

2. **Checks input salvo conditions**
   - After each packet arrives at an input port, checks all input salvo conditions
   - First satisfied condition wins (checked in order)
   - Creates a `Startable` epoch with the packets from the specified ports

3. **Repeats until blocked**
   - Blocked = no packets can move (either no packets on edges, or all destinations are full)

The convenience method `net.run_until_blocked()` repeatedly calls `RunStep` until no more progress can be made.

### Manual Actions (`NetAction`)

**Important**: All mutations to the `NetSim` state must go through `do_action(NetAction)`. This ensures:
- All operations return the list of `NetEvent`s that transpired
- External code can track exactly what operations have been performed
- Consistent event-driven architecture

External code controls the network through actions:

| Action | Description |
|--------|-------------|
| `RunStep` | Run automatic packet flow until no progress can be made |
| `CreatePacket(Option<EpochID>)` | Create a new packet (inside an epoch or outside the net) |
| `ConsumePacket(PacketID)` | Remove a packet from the network |
| `DestroyPacket(PacketID)` | Destroy a packet (abnormal removal, e.g., due to error) |
| `StartEpoch(EpochID)` | Transition a `Startable` epoch to `Running` |
| `FinishEpoch(EpochID)` | Complete a `Running` epoch (must be empty of packets) |
| `CancelEpoch(EpochID)` | Cancel an epoch and destroy its packets |
| `CreateEpoch(NodeName, Salvo)` | Manually create an epoch with specified packets |
| `LoadPacketIntoOutputPort(PacketID, PortName)` | Move a packet from inside an epoch to its output port |
| `SendOutputSalvo(EpochID, SalvoConditionName)` | Send packets from output ports onto edges |
| `TransportPacketToLocation(PacketID, PacketLocation)` | Move a packet to any location (with restrictions on running epochs) |

### Events (`NetEvent`)

Actions produce events that track what happened:

- `PacketCreated`, `PacketConsumed`
- `EpochCreated`, `EpochStarted`, `EpochFinished`, `EpochCancelled`
- `PacketMoved`
- `InputSalvoTriggered`, `OutputSalvoTriggered`

## Typical Usage Pattern

1. **Define the graph**: Create nodes with ports and salvo conditions, connect with edges
2. **Create a NetSim**: Initialize runtime state from the graph
3. **Inject packets**: Create packets and place them on edges or in input ports
4. **Run the network**: Call `run_step()` or `run_until_blocked()` to move packets and trigger epochs
5. **Handle startable epochs**: External code decides when to start each epoch
6. **Simulate node execution**: External code "runs" the node logic
7. **Output results**: Load packets into output ports and send output salvos
8. **Finish epochs**: Mark epochs as finished when done
9. **Repeat**: Continue running the network until processing is complete

## Example Workflow

```
1. Packets arrive on edges
2. run_until_blocked() moves them to input ports
3. Input salvo condition satisfied → Epoch created (Startable)
4. External code calls StartEpoch → Epoch now Running
5. External code "executes" the node (outside this library)
6. External code creates output packets, loads into output ports
7. External code calls SendOutputSalvo → Packets move to edges
8. External code calls FinishEpoch → Epoch complete
9. run_until_blocked() continues the flow to next nodes
```

## Design Philosophy

- **Separation of concerns**: The library handles flow mechanics; external code handles execution
- **Event-driven**: All state changes produce events for observability
- **Explicit control**: External code explicitly starts epochs and sends salvos
- **Deterministic**: Salvo conditions are checked in order; first match wins
- **Action-based mutations**: All `NetSim` state changes must go through `do_action(NetAction)` to ensure event tracking and auditability

## Building and Testing

### Rust Library

```bash
cd netrun-sim
cargo build -p netrun-sim
cargo test -p netrun-sim
cargo run -p netrun-sim --example linear_flow
```

### Python Bindings

```bash
cd netrun-sim/python
uv venv .venv && uv sync
uv run maturin develop
uv run python examples/linear_flow.py
```

See `netrun-sim/python/README.md` for full Python documentation.
