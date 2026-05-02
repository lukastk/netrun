# netrun - Flow-Based Development Runtime

This repository contains the **netrun** project, a flow-based development (FBD) runtime system.

## Project Structure

The project is split into the following components:

### netrun-sim (Simulation Engine)

`netrun-sim` is a Rust library that simulates the flow of packets through a network of interconnected nodes. It does **not** execute actual node logic or manage packet data—instead, it tracks packet locations, validates flow conditions, and manages the lifecycle of node executions (called "epochs").

This separation of concerns allows the actual execution and data storage to be implemented independently of the flow mechanics.

### netrun (Runtime)

`netrun` is a pure Python package built on top of `netrun-sim`. It provides:
- Flow-based network execution via the `Net` class (integrates with netrun-sim)
- Configuration system (`NetConfig`, `NodeConfig`, etc.) with JSON/TOML support
- RPC (Remote Procedure Call) communication primitives
- Worker pool management (threads, processes, remote)
- High-level execution orchestration via `ExecutionManager`
- Node factories for creating nodes from functions or broadcast patterns
- Tools for template resolution, action execution, and recipe management

**Important:** The `netrun` package uses **nblite** for literate programming. Before writing any code for `netrun`, you **must** read `netrun/NBLITE_INSTRUCTIONS.md` carefully. Key points:
- Source code lives in `.pct.py` files (percent notebooks), not in the exported Python modules
- Never edit files in `src/netrun/` directly - they are auto-generated
- After editing `.pct.py` files, run `nbl export --reverse` then `nbl export`
- **nblite does NOT auto-generate `__init__.py` files.** You must create `__init__.py` files manually in `src/` for any package that needs re-exports. These manual `__init__.py` files are not overwritten by `nbl export`.

### netrun-cli (CLI)

`netrun-cli` is a separate package that provides the `netrun` command-line tool. It depends on `netrun` for config models and tools, but is shipped independently so the runtime stays free of CLI dependencies (typer, tomli-w).

It uses the same nblite workflow as `netrun`. Source files live in `netrun-cli/pts/netrun_cli/`. The package installs the `netrun` command via `[project.scripts]`. See **netrun-cli specifics** below for details.

### netrun-ui (Visual Editor)

`netrun-ui` is a visual editor for creating and editing netrun flow configurations. The frontend is built with SvelteKit (Svelte 5) and SvelteFlow. The backend is a FastAPI Python app (`netrun_ui_backend/`) that handles file I/O, factory resolution, config validation, and action execution.

**Reference Documentation** (read these for context when working on netrun-ui):
- `netrun-ui/README.md` - Project overview, architecture, and development setup
- `netrun-ui/BACKEND_README.md` - Backend CLI usage and installation

**Important:** `netrun-ui/netrun_ui_backend/vis_assets/` is intentionally NOT gitignored (so hatchling includes it in the wheel), but the built assets inside it must NEVER be committed to git. They are generated at build time by `npm run build:app` in `netrun-ui-vis/`. The CI release workflow creates them before building the wheel.

## Development Notes (_dev_notes)

Each subproject may have a `_dev_notes/` directory containing implementation plans and development documentation. **These are critical for maintaining context across sessions.**

### Working with _dev_notes

- **Always check for existing plans**: Before starting work on a subproject, check its `_dev_notes/` directory for the latest plan (files are numbered with prefix like `00_`, `01_`, etc.)
- **Follow the current plan**: If there's an active plan for your task, follow it step by step
- **Create new plans for major features**: When starting a major new feature, create a new plan file in `_dev_notes/` with the next number prefix (e.g., `01_feature_name.md`)
- **Plans should include**: Overview, approach/strategy, implementation steps, files to modify, verification steps

Example structure:
```
netrun-ui/
├── _dev_notes/
│   ├── 00_multi_tab_support_plan.md
│   └── 01_next_feature_plan.md
```

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
│   │   │   ├── net_tests.rs    # Net tests (separate file)
│   │   │   └── test_fixtures.rs # Test helpers (feature-gated)
│   │   ├── tests/          # Integration tests
│   │   └── examples/       # Rust examples
│   └── python/             # Python bindings (PyO3)
│       ├── Cargo.toml      # PyO3 crate
│       ├── pyproject.toml  # Maturin config
│       ├── src/            # Rust binding code
│       ├── python/         # Python package
│       │   └── netrun_sim/
│       └── examples/       # Python examples
├── netrun/                 # Runtime (pure Python, nblite project)
│   ├── NBLITE_INSTRUCTIONS.md  # How to write code (READ THIS FIRST)
│   ├── nblite.toml         # nblite configuration
│   ├── nbs/                # Jupyter notebooks (.ipynb)
│   │   ├── netrun/         # Source notebooks
│   │   └── tests/          # Test notebooks
│   ├── pts/                # Percent notebooks (.pct.py) - EDIT THESE
│   │   ├── netrun/         # Source percent notebooks
│   │   └── tests/          # Test percent notebooks
│   └── src/                # Auto-generated code (DO NOT EDIT)
│       ├── netrun/         # Generated Python package
│       └── tests/          # Generated test files
├── netrun-cli/             # CLI (separate package, nblite project)
│   ├── nblite.toml         # nblite configuration
│   ├── pyproject.toml      # Defines `netrun` command via [project.scripts]
│   ├── pts/
│   │   ├── netrun_cli/     # CLI source percent notebooks
│   │   └── tests/          # CLI test percent notebooks
│   └── src/
│       ├── netrun_cli/     # Generated CLI package
│       └── tests/          # Generated CLI tests
└── netrun-ui/              # Visual editor (SvelteKit + FastAPI)
    ├── src/                # Frontend source (Svelte 5, SvelteFlow)
    └── netrun_ui_backend/  # Python backend (FastAPI)
```

---

# netrun Package Documentation

## Current Implementation Status

All major modules are fully implemented:

1. **Net Module** (`netrun.net`) - Flow-based execution with netrun-sim integration
2. **Packets** (`netrun.packets`) - Packet value storage (`PacketStore`, `LazyPacketValueSpec`)
3. **Storage** (`netrun.storage`) - Serialization, compression, backends, caching, file storage
4. **RPC Layer** (`netrun.rpc`) - Bidirectional message-passing channels
5. **Pool Layer** (`netrun.pool`) - Worker pool management
6. **ExecutionManager** (`netrun.execution_manager`) - High-level execution orchestration
7. **Node Factories** (`netrun.node_factories`) - Function, broadcast, join factories
8. **Tools** (`netrun.tools`) - Template resolution, action execution, recipes
9. **Core** (`netrun.core`) - Convenience re-exports of Net, NetConfig, etc.

The CLI lives in the separate **`netrun-cli`** package — see "netrun-cli specifics" below.

---

## Module Structure

### Internal Utilities (`netrun._iutils`)

- `_base` - Timestamp generation
- `hashing` - Hash computation utilities
- `pickling` - Pickling method handling
- `var_ref` - `VarRef`, `EnvVar` (alias), and `VarResolvableModel` for variable reference resolution

### Packets (`netrun.packets`)

- `PacketStore` - Thread-safe storage for packet values
- `LazyPacketValueSpec` - Lazy value specification for deferred evaluation (func_import_path, args, kwargs)

### Storage (`netrun.storage`)

Storage layer for packet data persistence, caching, and file storage:

- `config` - `StorageConfig`, `NodeStorageConfig`, `CacheConfig`, `NodeCacheConfig`, `NodeFileStorageConfig`, backend configs (`LocalBackendConfig`, `S3BackendConfig`, `SSHBackendConfig`, `RcloneBackendConfig`, `GCSBackendConfig`)
- `_serialization` - Serialization methods
- `_compression` - Compression methods
- `_backends` - Storage backend implementations
- `_retrieval` - Packet retrieval utilities
- `_cache` - Epoch caching logic
- `_file_storage` - File-based storage

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
        func_args=(1, 2),
        func_kwargs={"x": 3},
    )

    print(result.result)  # Function return value
```

**Async dispatch model**: Async functions dispatched to thread/multiprocess workers run on a single shared event loop owned by the manager (one daemon thread per process scope). Sync functions run directly on the worker. This makes `asyncio.create_subprocess_exec`, nested awaits, and cross-node `asyncio.Lock` work without per-worker loop hacks.

**Worker crash isolation**: A crashed worker only fails the jobs targeting it; the rest of the pool stays alive.

**ExecutionManager Protocol Keys**:
- `RUN` - Execute a function on a worker
- `SEND_FUNCTION` - Register a function by key on a worker
- `UP_RUN_RESPONSE` - Single terminal event with result and timestamps
- `UP_SEND_FUNCTION_RESPONSE` - Acknowledgment of SEND_FUNCTION

### Net Module (`netrun.net`)

The Net module provides flow-based network execution by integrating with netrun-sim.

**Key Classes**:
- `Net` - Main orchestrator: manages pools, executes epochs, routes packets
- `NetConfig` - Top-level configuration (pools, graph, output queues, storage, **`resources`**)
- `NodeConfig` - Node definition (ports, salvo conditions, factory, execution config)
- `NodeExecutionConfig` - Execution settings (pools, retries, type checking, **`depends_on`**, **`resources`**)
- `GraphConfig` - Graph topology (nodes, edges, output queues)
- `PortConfig` - Port definition with optional type annotation
- `EdgeConfig` - Connection between ports (`source_node`, `source_port`, `target_node`, `target_port`)
- `SalvoConditionConfig` - Rules for triggering epochs or sending output
- `OutputQueueConfig` - Collects packets from unconnected output ports
- `NodeExecutionContext` - Context passed to node functions (`ctx.create_packet`, `ctx.print`, **`ctx.state`**, etc.)
- `TargetInputSalvo` - Target-based execution support

**Net Lifecycle**:
```python
# Async usage
async with Net(config) as net:
    net.inject_data("Source", "in", [value1, value2])
    await net.run_until_blocked()
    results = net.flush_output_queue("results")

# Sync usage
with Net(config) as net:
    net.inject_data("Source", "in", [value1, value2])
    net.run_until_blocked_sync()
    results = net.flush_output_queue("results")
```

**Lifecycle methods**: `init()` / `close()` (async), `init_sync()` / `close_sync()` (sync). The context managers call these automatically.

**Sub-objects**:
- `net.cache` — Cache inspection and management (`net.cache.stats()`, `net.cache.entries(node)`, `net.cache.clear()`, etc.)
- `net.logs` — Log query and printing (`net.logs.print_all()`, `net.logs.for_node(name)`, `net.logs.for_epoch(id)`, etc.)

**Epoch Lifecycle Callbacks**: Register callbacks to observe epoch starts/ends in real time:
```python
# Net-level: fires for ALL nodes
remove = net.on_epoch_start(lambda node_name, epoch_id: print(f"{node_name} started"))
remove = net.on_epoch_end(lambda node_name, epoch_id, record: print(f"{node_name} ended"))
remove()  # deregister

# Node-scoped: fires only for THAT node
remove = net.on_epoch_start(callback, node="fetch")
remove = net.on_epoch_end(callback, node="fetch")
```

Both sync and async callbacks are supported. `on_epoch_end` receives the `EpochLog` (with `was_cancelled`, `ended_at`, `started_at`, `logs`, etc.).

**Features**: signals, controls, pause/resume, rate limiting, retries, type checking, print capture, output queues, caching, file storage, epoch lifecycle callbacks, scheduling constraints (`depends_on`, `resources`), retry-persistent state (`ctx.state`).

#### Scheduling: `depends_on` and `resources`

`NodeExecutionConfig.depends_on: list[str] | None` — Names of nodes that must complete at least one epoch before this node can fire. Provides directional ordering without requiring data edges. Validated at Net construction: cycles and references to non-existent nodes raise `ValueError`.

`NodeExecutionConfig.resources: dict[str, int] | None` — Named semaphore-style requirements. The node acquires the listed slots before its epoch starts and releases them when the epoch finishes, fails, or is cancelled. Resources must be declared at the net level via `NetConfig.resources: dict[str, int]` (resource name → total capacity).

```python
# Net-level capacities
NetConfig(
    resources={"gpu": 1, "http_conn": 4},
    graph=GraphConfig(nodes=[...]),
)

# Node-level requirements
NodeConfig(
    name="train",
    execution_config=NodeExecutionConfig(
        pools=["main"],
        depends_on=["preprocess"],   # waits for preprocess to complete first
        resources={"gpu": 1},        # holds the GPU slot during epoch
    ),
)
```

Common patterns:
- **Mutual exclusion** between two nodes: declare a 1-slot resource and have both nodes require 1 slot.
- **Bounded concurrency** (e.g. max 4 HTTP connections): declare a 4-slot resource and have N HTTP nodes each require 1 slot.

#### Retry-persistent state: `ctx.state`

`NodeExecutionContext.state: dict[str, Any]` — Mutable dict that persists across retry attempts of the same epoch. Use it to cache expensive precomputation so retries don't redo the work.

```python
async def main(ctx, ad_ids):
    if "texts_df" not in ctx.state:
        ctx.state["texts_df"] = pd.read_parquet(big_path)  # loaded once, even on retry
    texts_df = ctx.state["texts_df"]
    ...
```

Lifecycle: cleared when the epoch succeeds or permanently fails. The dict lives wherever the node runs — for multiprocess / remote pools, each worker process keeps its own copy and values must be picklable.

### Tools (`netrun.tools`)

Utilities for action execution and recipe management:

- `ActionConfig`, `ActionContext`, `ActionResult` - Action models
- `RecipeConfig` - Recipe configuration
- `execute_action()`, `execute_command()` - Execute shell commands with environment
- Template variable resolution

### Core (`netrun.core`)

Convenience re-exports of the most commonly used classes:
`Net`, `NetConfig`, `NodeConfig`, `NodeExecutionConfig`, `PoolConfig`, `PortConfig`, `EdgeConfig`, `GraphConfig`, `SalvoConditionConfig`, `OutputQueueConfig`, `TargetInputSalvo`, `EnvVar`.

---

## netrun-cli specifics

The CLI is a separate package at `netrun-cli/`. Source files live in `netrun-cli/pts/netrun_cli/`; tests live in `netrun-cli/pts/tests/`. It uses the same nblite workflow as `netrun`.

**Editing the CLI:**
1. Edit `.pct.py` files in `netrun-cli/pts/netrun_cli/` or `netrun-cli/pts/tests/`
2. From `netrun-cli/`: `nbl export --reverse && nbl export`
3. Run tests: `cd netrun-cli && uv run pytest src/tests/ -v`

**Installing the CLI command:**

```bash
cd netrun-cli && uv sync
.venv/bin/netrun --help
```

The `netrun` script is registered via `[project.scripts]` in `netrun-cli/pyproject.toml` and points to `netrun_cli._app:app_main`. `netrun-cli` depends on `netrun` (editable in dev) for config models and tools.

**Available commands:**

| Command | Description |
|---------|-------------|
| `validate` | Validate a config file (catches resolution failures) |
| `structure` | Output graph topology (`--format json` or `--format mermaid`) |
| `convert` | Convert between `.netrun.json` and `.netrun.toml` |
| `factory-info <path>` | Inspect a factory's parameters |
| `info` | Summary stats (nodes, edges, pools, factories, recipes) |
| `nodes` | List all nodes with port names |
| `node <name>` | Detailed node info; pass `--edges` to include incoming/outgoing edges |
| `dry-run` | Show topological execution order, source/sink nodes, and scheduling constraints |
| `add-node` / `remove-node` / `edit-node` | Mutate graph nodes |
| `add-edge` / `remove-edge` | Mutate edges |
| `actions list/run` | Inspect and run actions |
| `recipes list/run` | Inspect and run recipes |
| `download-agents` | Pull agent docs from the netrun GitHub repo |

**Adding a new command:**

1. Add a Typer command function to one of `netrun-cli/pts/netrun_cli/0X_*.pct.py` (or create a new module)
2. Register it in `00_app.pct.py` via `app.command("name")(fn)`
3. Add tests to `netrun-cli/pts/tests/test_cli.pct.py`
4. Re-export and run tests

The CLI imports from `netrun.net.config`, `netrun.tools`, etc. — these are already external dependencies through the `netrun` package, so no special setup is required.

---

### Node Variables (`NodeVariable`)

Node variables are typed key-value pairs accessible to nodes via `ctx.vars`. They support a two-level inheritance model:

- **Net-level** (`NetConfig.node_vars`): Global defaults applied to all nodes
- **Node-level** (`NodeExecutionConfig.node_vars`): Per-node overrides that take precedence

#### Value-optional design

`NodeVariable.value` is optional (`str | int | float | bool | VarRef | None`, default `None`). Variables without values are placeholders that can be filled in via `NetConfig.from_file()` overrides:

```python
NetConfig.from_file(
    "netrun.json",
    global_node_vars={"run_name": "my_run"},           # fills net-level placeholders
    node_vars={"my_node": {"model": "gpt-4"}},         # fills node-level placeholders
)
```

Calling `resolve_value()` on a variable with `value=None` raises `ValueError` — unless `optional=True` (see below). The UI frontend must handle `variable.value` being `undefined` (absent from JSON) gracefully.

#### Optional field

`NodeVariable.optional: bool = False` allows a variable to legitimately have no value. When `optional=True` and `value=None`, `resolve_value()` returns `None` (instead of raising). If `value` is set, it resolves normally (including options validation). When `optional=True` and `value=None`, options validation is skipped. For inherited vars, `optional` is inherited from the global variable (cannot be explicitly set with `inherit=True`).

#### Inherit field

`NodeVariable.inherit: bool = False` enables node-level vars to inherit from net-level vars of the same name. When `inherit=True` on a **node-level** var:

- Requires a net-level var of the same name (errors if missing at resolve time)
- `type`, `options`, and `optional` must not be explicitly set (they are inherited from the global var)
- `value` may optionally be set to override just the value; type/options still come from global
- If `value` is not set, the global value is used entirely

Net-level vars must **never** have `inherit=True` (error at resolve time).

The inherit validation uses `model_fields_set` but only rejects if the value differs from the default (to allow serialization round-trips where `model_dump()` includes `type: "str"`).

#### Key files

- Model definition: `pts/netrun/06_net/00_config/01_nodes.pct.py` (`class NodeVariable`)
- Variable merge logic (graph resolve): `pts/netrun/06_net/00_config/02_graph.pct.py`
- Variable merge logic (preprocessor): `pts/netrun/06_net/01_net/00_context.pct.py` (`create_net_func_preprocessor`)
- `from_file` overrides: `pts/netrun/06_net/00_config/03_net_config.pct.py` (`_set_node_vars_in_data`)
- UI store: `netrun-ui/src/lib/stores/variablesStore.ts`
- UI components: `netrun-ui/src/lib/components/NodeVariablesSection.svelte`, `AllNodeVariablesSection.svelte`

### Node Factories (`netrun.node_factories`)

Node factories provide a way to create `NodeConfig` objects dynamically. This enables reusable node templates, function-based nodes, and configuration-driven node creation.

#### Factory Protocol

A factory module must export exactly **two functions**:

**`get_node_config(_net_config=None, *, **factory_args) -> NodeConfig`**
- `_net_config` is a mandatory first parameter, always injected by the system (never by users)
- The `*` makes all factory_args keyword-only
- Returns the graph structure (ports, salvo conditions, metadata)
- **Must NOT include `execution_config`** (it will be stripped/ignored)
- Cannot return closures or unpickleable objects
- Subgraph factories may return `SubgraphConfig` instead of `NodeConfig`

**`get_node_funcs(_net_config=None, *, **factory_args) -> tuple[exec_func, init_func, close_func, on_failure_func]`**
- `_net_config` is injected the same way as above
- Returns the execution functions as a 4-tuple
- Typically: `(exec_func, None, None, None)` — (exec, init, close, on_failure)
- Can capture `factory_args` in closures (functions are resolved on each worker)
- Not needed for subgraph factories

The `_net_config` parameter provides access to the full `NetConfig`, enabling factories to resolve relative file paths against `project_root_path`. The underscore prefix signals it is internal and not user-facing. It is filtered from CLI `factory-info` output and UI parameter displays.

#### Resolution Lifecycle

```
1. CONFIG CREATION
   NodeConfig(factory="module.path", factory_args={...})
   → Factory and args stored, execution_config is None

2. RESOLUTION (in Net.__init__)
   NodeConfig.resolve(net_config=...) calls get_node_config(_net_config=net_config, **factory_args)
   → Returns config with ports/salvos, execution_config still None
   → Factory and factory_args preserved for worker-level resolution

3. WORKER EXECUTION
   NetFuncPreprocessor._resolve_factory(node_name)
   → Imports factory module on worker
   → Reconstructs NetConfig from serialized net_config_data
   → Calls get_node_funcs(_net_config=net_config, **factory_args)
   → Gets actual exec_func, caches for reuse
```

#### Using Factories in TOML

```toml
[[graph.nodes]]
factory = "netrun.node_factories.function"

[graph.nodes.factory_args]
func = "mymodule.my_function"

[graph.nodes.execution_config]
pools = ["main"]
type_checking_enabled = true  # Settings go here, NOT in factory_args
```

**Important**: Execution settings like `type_checking_enabled`, `pools`, etc. are configured in `execution_config`, NOT in `factory_args`. The factory only provides graph structure and execution functions.

#### Existing Factories

- `netrun.node_factories.function` - Creates nodes from regular Python functions
  - `factory_args`: `func` (callable or import path string)
  - Parses function signature for input/output ports
  - Handles special parameters (`ctx`, `print`)

- `netrun.node_factories.broadcast` - Fan-out replication node
  - `factory_args`: configurable input/output ports
  - `CopyMode` enum: `none`, `shallow`, `deep`
  - Replicates input packets to multiple output ports

#### Key Files

- Factory protocol implementation: `pts/netrun/06_net/00_config/01_nodes.pct.py`
- Function factory: `pts/netrun/07_node_factories/00_from_function.pct.py`
- Broadcast factory: `pts/netrun/07_node_factories/01_broadcast.pct.py`
- Factory resolution in Net: `pts/netrun/06_net/01_net/02_net.pct.py`
- Example usage: `sample_projects/` (see `07_node_factories`, `10_controls_and_signals`)

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

**CRITICAL — STOP AND ASK if something feels wrong:** If at any point — while implementing a feature, writing a test, building a sample project, or doing anything else — you find yourself writing code that works around a bug, feels like a hack, or is not in the spirit of the package, you **MUST stop immediately and ask the user for instructions**. Do NOT silently work around the problem. Do NOT introduce tech debt. Do NOT write "temporary" fixes. Instead, describe what you encountered, why the current approach feels wrong, and ask how to proceed. This applies to ALL work, not just core library code — tests, examples, and sample projects are equally important.

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

- `PacketCreated`, `PacketConsumed`, `PacketDestroyed`, `PacketMoved`, `PacketOrphaned`
- `EpochCreated`, `EpochStarted`, `EpochFinished`, `EpochCancelled`
- `InputSalvoTriggered`, `OutputSalvoTriggered`

### Undo Support

Actions can be reversed via `net.undo_action(action, events)`, enabling undo/redo workflows. Requires both the original action and the events it produced.

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
