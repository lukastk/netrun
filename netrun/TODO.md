# netrun Development Plan

This document outlines the development plan for the `netrun` Python package, which provides a high-level runtime for flow-based development graphs built on top of `netrun-sim`. Please see `PROJECT_SPEC.md` for further details on the package.

---

## Milestone 1: Core Foundation ✅ COMPLETED

**Goal:** Establish the basic Net class structure and packet value management.

### 1.1 Core Module Setup
- [x] Set up `00_core.pct.py` with proper nblite structure
- [x] Re-export `netrun_sim` types for user convenience (errors, graph types, etc.)
  - Use reflection/introspection where possible to avoid duplication
- [x] Define `NetrunRuntimeError` base class for netrun-specific errors
- [x] Define netrun-specific error types (inheriting from `NetrunRuntimeError`):
  - `PacketTypeMismatch`
  - `ValueFunctionFailed`
  - `NodeExecutionFailed`
  - `EpochTimeout`
  - `EpochCancelled`

### 1.2 Packet Value Storage
- [x] Implement `PacketValueStore` class:
  - Store packet values by PacketID
  - Support value functions (lazy evaluation)
  - Handle consumed packet storage with configurable limits
  - Support optional file-based storage (pickle)
- [x] Implement value function handling:
  - Sync value functions
  - Async value functions
  - Error handling for value function failures

### 1.3 Basic Net Class
- [x] Implement `Net.__init__()`:
  - Accept `Graph` from `netrun_sim`
  - Initialize internal `NetSim` (hidden from user)
  - Set up packet value store
  - Configuration options: `consumed_packet_storage`, `consumed_packet_storage_limit`
- [x] Implement `Net.set_node_exec()` for setting execution functions
- [x] Implement `Net.set_node_config()` for node configuration
- [x] Store node execution functions and configurations internally
- [x] Implement wrapper methods to hide NetSim:
  - `get_startable_epochs()` - list of startable epoch IDs
  - `get_startable_epochs_by_node(node_name)` - startable epochs for a specific node
  - `get_epoch(epoch_id)` - get epoch by ID
  - `get_packet(packet_id)` - get packet by ID

### 1.4 Tests for Milestone 1
- [x] Test error type definitions and hierarchy
- [x] Test `PacketValueStore`:
  - Direct value storage/retrieval
  - Value function evaluation (sync and async)
  - Storage limits and eviction
  - File-based storage
- [x] Test basic `Net` construction
- [x] Test `set_node_exec()` and `set_node_config()`

### 1.5 Examples for Milestone 1
- [x] `examples/00_basic_setup/` - Basic Net setup and configuration

---

## Milestone 2: Node Execution Contexts ✅ COMPLETED

**Goal:** Implement the execution contexts that nodes use to interact with the network.

### 2.1 NodeExecutionContext
- [x] Implement `NodeExecutionContext` class:
  - Properties: `epoch_id`, `node_name`, `retry_count`, `retry_timestamps`, `retry_exceptions`
  - Access to `_net` (private)
- [x] Implement context packet operations:
  - `create_packet(value)` - create packet with direct value
  - `create_packet_from_value_func(func)` - create packet with lazy value
  - `consume_packet(packet)` - consume and return value
  - `load_output_port(port_name, packet)` - load packet to output port
  - `send_output_salvo(salvo_condition_name)` - send output salvo
- [x] Implement `cancel_epoch()` - raises `EpochCancelled`
- [x] Support both sync and async variants of all methods

### 2.2 NodeFailureContext
- [x] Implement `NodeFailureContext` class:
  - Properties: `epoch_id`, `node_name`, `retry_count`, `retry_timestamps`, `retry_exceptions`
  - `input_salvo`: dict of input packets
  - `packet_values`: dict of consumed values

### 2.3 Deferred Actions System
- [x] Implement `DeferredPacket` class:
  - Behaves like `Packet` for all node operations
  - `id` property raises exception if accessed (ID not yet assigned)
  - Resolved to real `Packet` on commit
- [x] Implement action deferral for `defer_net_actions=True`:
  - Queue all packet operations (create, consume, load, send)
  - `create_packet()` returns `DeferredPacket` instead of `Packet`
  - Commit queue on successful completion (assign real PacketIDs)
  - Discard queue on failure (keep for logging)
- [x] Implement "unconsume" logic for retries:
  - Restore packet values on failure so they can be consumed again
  - Value functions may be called multiple times (document purity requirement)

### 2.4 Tests for Milestone 2
- [x] Test `NodeExecutionContext` operations (sync)
- [x] Test `NodeExecutionContext` operations (async)
- [x] Test `NodeFailureContext` creation and properties
- [x] Test `DeferredPacket`:
  - Behaves like Packet for load_output_port
  - `id` property raises exception
  - Resolved to real Packet on commit
- [x] Test deferred actions:
  - Successful commit
  - Discard on failure
  - Unconsume behavior (value functions called again on retry)
- [x] Test `cancel_epoch()` behavior

### 2.5 Examples for Milestone 2
- [x] (No standalone examples - context is used within execution, demonstrated in Milestone 3)

---

## Milestone 3: Single-Threaded Execution ✅ COMPLETED

**Goal:** Implement synchronous, single-threaded net execution.

### 3.1 Epoch Execution Loop
- [x] Implement internal `_execute_epoch()` method:
  - Create `NodeExecutionContext`
  - Call `exec_node_func` with context and packets
  - Handle success: commit deferred actions, finish epoch
  - Handle failure: call `exec_failed_node_func`, handle retries
- [x] Implement timeout handling:
  - Wall-clock timeout from epoch start
  - Cancel epoch on timeout

### 3.2 Run Methods
- [x] Implement `Net.run_step(start_epochs=True)`:
  - Call `RunNetUntilBlocked` on NetSim
  - If `start_epochs=True`: start and execute ready epochs
  - Wait for all started epochs to complete
- [x] Implement `Net.start()`:
  - Run `run_step()` in loop until fully blocked
  - No more startable epochs and no packets can move

### 3.3 Start/Stop Node Functions
- [x] Call `start_node_func` when net starts (in `Net.start()`)
- [x] Call `stop_node_func` when net stops
- [ ] Handle both sync and async variants (deferred to Milestone 5)

### 3.4 Tests for Milestone 3
- [x] Test simple linear flow execution
- [ ] Test branching flow execution (deferred to Milestone 4)
- [ ] Test epoch timeout handling (basic structure in place, full implementation in Milestone 4)
- [x] Test `start_node_func` and `stop_node_func` calls
- [x] Test `run_step()` with `start_epochs=True/False`
- [x] Test full `start()` execution

### 3.5 Examples for Milestone 3
- [x] `examples/01_simple_pipeline/` - Simple linear pipeline (Source -> Process -> Sink)
- [ ] `examples/02_branching_flow/` - Fan-out and fan-in patterns (deferred to Milestone 4)

---

## Milestone 4: Error Handling and Retries ✅ COMPLETED

**Goal:** Implement robust error handling, retries, and dead letter queue.

### 4.1 Retry System
- [x] Implement retry logic in `_execute_epoch()`:
  - Respect `retries` config (number of attempts)
  - Respect `retry_wait` config (delay between retries)
  - Track `retry_timestamps` and `retry_exceptions`
  - Enforce `defer_net_actions=True` when `retries > 0`
- [x] Call `exec_failed_node_func` after each failure (including final)

### 4.2 Dead Letter Queue
- [x] Implement `DeadLetterQueue` class:
  - Modes: "memory", "file", callback
  - Store failed packets with context (node, exception, timestamps)
- [x] Implement `Net.dead_letter_queue` property
- [x] Methods: `get_all()`, `get_by_node(node_name)`

### 4.3 Net-Level Error Handling
- [x] Implement `on_error` config with three modes:
  - `"continue"`: Net continues running other nodes; failed epoch is cancelled
  - `"pause"`: Net pauses (finishes running epochs, doesn't start new ones)
  - `"raise"`: Net pauses first, then raises the exception to the caller
- [x] Implement `error_callback` config

### 4.4 Tests for Milestone 4
- [x] Test retry execution (success on retry N)
- [x] Test retry wait timing
- [x] Test max retries exceeded behavior
- [x] Test `exec_failed_node_func` calls
- [x] Test dead letter queue (memory mode)
- [x] Test dead letter queue (file mode)
- [x] Test dead letter queue (callback mode)
- [x] Test `on_error="continue"` behavior
- [x] Test `on_error="pause"` behavior
- [x] Test `on_error="raise"` behavior
- [x] Test `error_callback` invocation

### 4.5 Examples for Milestone 4
- [x] `examples/02_error_handling/` - Retries and error recovery patterns

---

## Milestone 5: Async Execution ✅ COMPLETED

**Goal:** Support async node functions and async Net methods.

### 5.1 Async Node Functions
- [x] Detect if `exec_node_func` is async (via `asyncio.iscoroutinefunction`)
- [x] Execute async functions properly with `await`
- [x] Make `NodeExecutionContext` methods async when func is async

### 5.2 Async Net Methods
- [x] Implement `Net.async_run_step()`
- [x] Implement `Net.async_start()`
- [x] Implement `Net.async_pause()`
- [x] Implement `Net.async_stop()`
- [x] Implement `Net.async_wait_until_blocked()`

### 5.3 Mixed Sync/Async
- [x] Allow mixing sync and async nodes in same net
- [x] Proper event loop management

### 5.4 Tests for Milestone 5
- [x] Test async `exec_node_func` execution
- [x] Test async context methods
- [x] Test `async_run_step()` and `async_start()`
- [x] Test mixed sync/async nodes
- [x] Test async value functions

### 5.5 Examples for Milestone 5
- [x] `examples/03_async_nodes/` - Async node functions and mixed sync/async

---

## Milestone 6: Thread and Process Pools ✅ COMPLETED

**Goal:** Enable parallel execution via thread and process pools.

### 6.1 Pool Management
- [x] Implement pool configuration in `Net.__init__()`:
  - `thread_pools`: dict of pool configs `{"name": {"size": N}}`
  - `process_pools`: dict of pool configs
- [x] Create and manage `ThreadPoolExecutor` instances
- [x] Create and manage `ProcessPoolExecutor` instances
- [x] Process pool requirements:
  - Packet values must be picklable (raise clear error if not)
  - Each worker process maintains its own event loop for async functions
  - Ensure proper error propagation from worker processes

### 6.2 Node Pool Assignment
- [x] Support `pool` config: single name or list of names
- [x] Implement pool selection algorithm:
  - "least_busy" (default): pool with most available workers
- [x] Support `pool_init_mode`: "global" vs "per_worker"

### 6.3 Pool Lifecycle
- [x] Call `start_node_func` per worker (default) or globally
- [x] Call `stop_node_func` per worker or globally
- [x] Proper pool shutdown on net stop

### 6.4 Threaded Net Execution
- [x] Implement `Net.start(threaded=True)`:
  - Run net in background thread
- [x] Implement `Net.run_step(threaded=True)`
- [x] Implement control methods:
  - `wait_until_blocked()`
  - `poll()` - check status
  - `pause()` - finish current epochs, don't start new
  - `stop()` - stop entirely

### 6.5 Tests for Milestone 6
- [x] Test thread pool creation and configuration
- [x] Test process pool creation and configuration
- [x] Test node execution in thread pool
- [x] Test node execution in process pool
- [x] Test pool selection algorithm
- [x] Test per-worker vs global init mode
- [x] Test `threaded=True` execution
- [x] Test `wait_until_blocked()`, `poll()`, `pause()`, `stop()`

### 6.6 Examples for Milestone 6
- [x] `examples/04_background_runner/` - Background execution with thread pools

---

## Milestone 7: Rate Limiting and Parallel Epoch Control ✅ COMPLETED

**Goal:** Control execution rate and parallelism per node.

### 7.1 Rate Limiting
- [x] Implement `rate_limit_per_second` config
- [x] Track epoch start timestamps per node
- [x] Delay epoch starts to respect rate limit

### 7.2 Parallel Epoch Control
- [x] Implement `max_parallel_epochs` config
- [x] Track running epochs per node
- [x] Delay epoch starts when at max

### 7.3 Tests for Milestone 7
- [x] Test rate limiting accuracy
- [x] Test `max_parallel_epochs` enforcement
- [x] Test interaction between rate limit and max parallel

### 7.4 Examples for Milestone 7
- [x] `examples/05_rate_limiting/` - Rate limiting and parallel control

---

## Milestone 8: Logging and History ✅ COMPLETED

**Goal:** Comprehensive logging, history tracking, and stdout capture.

### 8.1 Event History
- [x] Implement history storage:
  - Record all `NetAction` and `NetEvent` with timestamps
  - Add unique IDs for traceability
  - Store action→events relationships
- [x] History configuration:
  - `history_max_size`: max events in memory
  - `history_file`: persist to JSONL file
  - `history_chunk_size`: write in chunks
  - `history_flush_on_pause`: flush when paused/blocked

### 8.2 Node-Level Logging
- [x] Implement stdout capture via custom `print` injection
- [x] Support `capture_stdout` config (per node)
- [x] Support `echo_stdout` config (also print to real stdout)
- [x] Store logs per node and per epoch
- [x] Methods: `get_node_log(node_name)`, `get_epoch_log(epoch_id)`

### 8.3 Tests for Milestone 8
- [x] Test history recording accuracy
- [x] Test history persistence to JSONL
- [x] Test history size limits
- [x] Test stdout capture
- [x] Test `echo_stdout` behavior
- [x] Test `get_node_log()` and `get_epoch_log()`

### 8.4 Examples for Milestone 8
- [x] `examples/06_logging_history/` - History tracking and log inspection

---

## Milestone 9: Port Types ✅ COMPLETED

**Goal:** Type checking for packet values on ports.

### 9.1 Port Type Definition
- [x] Support type specifications:
  - String class name: `"DataFrame"`
  - Actual class: `pandas.DataFrame`
  - Dict with options: `{"class": MyClass, "isinstance": True/False, "subclass": True}`

### 9.2 Type Checking
- [x] Check input port types when packet is consumed via `ctx.consume_packet()`
  - (Not at epoch creation, since value may not be known until consumption)
- [x] Check output port types when packet loaded via `ctx.load_output_port()`
- [x] Raise `PacketTypeMismatch` on mismatch

### 9.3 Tests for Milestone 9
- [x] Test string class name matching
- [x] Test actual class matching
- [x] Test isinstance checking
- [x] Test exact type checking
- [x] Test subclass checking
- [x] Test type mismatch error

### 9.4 Examples for Milestone 9
- [x] `examples/07_port_types/` - Port type checking demonstration

---

## Milestone 10: SIGINT Handling (DEFERRED)

**Goal:** Graceful shutdown on SIGINT.

**Note:** This milestone was deferred. The pause/stop functionality is already implemented
and can be used to handle SIGINT at the application level if needed.

### 10.1 Signal Handler
- [ ] Register SIGINT handler when net starts
- [ ] On SIGINT: equivalent to `net.pause()`
- [ ] Allow running epochs to finish
- [ ] Don't start new epochs
- [ ] Restore original handler on stop

### 10.2 Tests for Milestone 10
- [ ] Test graceful shutdown on SIGINT
- [ ] Test running epochs complete before shutdown

### 10.3 Examples for Milestone 10
- [ ] (Demonstrated in long-running examples from other milestones)

---

## Milestone 11: DSL Format (TOML) ✅ COMPLETED

**Goal:** Human-readable serialization format for nets.

### 11.1 TOML Parser
- [x] Parse `[net]` section:
  - Net-level config (on_error, history_file, etc.)
  - Thread/process pool definitions
  - Metadata
- [x] Parse `[nodes.X]` sections:
  - Port definitions (simple list or detailed config)
  - Salvo conditions with expression language
  - Node options
  - Exec function paths
  - Factory references
  - Metadata

### 11.2 Salvo Condition Expression Parser
- [x] Parse expressions:
  - `nonempty(port)`, `empty(port)`, `full(port)`
  - `count(port) >= N`, `count(port) == N`
  - `and`, `or`, `not`
  - Parentheses for grouping
- [x] Convert to `SalvoConditionTerm`
- [x] Handle `ports` field:
  - If omitted, include all input/output ports (depending on condition type)
- [x] Handle `max_salvos` field:
  - `"infinite"` maps to `MaxSalvos.Infinite`
  - Positive integer maps to `MaxSalvos.finite(n)`

### 11.3 Edge Definition Parser
- [x] Parse edge list format: `[{ from = "A.out", to = "B.in" }, ...]`
- [x] Parse detailed edge format with metadata

### 11.4 TOML Serializer
- [x] Serialize `Net` to TOML string
- [x] Preserve all configuration
- [x] Pretty-print salvo conditions

### 11.5 Tests for Milestone 11
- [x] Test parsing simple net definition
- [x] Test parsing complex salvo conditions
- [x] Test parsing edge definitions
- [x] Test round-trip: parse → serialize → parse
- [x] Test error messages for invalid TOML

### 11.6 Examples for Milestone 11
- [x] `examples/08_dsl_format/` - Defining nets in TOML format

---

## Milestone 12: Node Factories ✅ COMPLETED

**Goal:** Reusable node templates via factory pattern.

### 12.1 Factory Protocol
- [x] Define factory interface:
  - `get_node_spec(**args) -> dict` (Node kwargs)
  - `get_node_funcs(**args) -> tuple` (exec, start, stop, failed)
- [x] Support factory import from string path

### 12.2 Factory Usage
- [x] Implement `Net.from_factory()` class method
- [x] Support factory args in DSL:
  - `factory = "module.factory"`
  - `factory_args = { ... }`

### 12.3 Factory Serialization
- [x] Require JSON-serializable args for serialization
- [x] Support callable args as import paths

### 12.4 Tests for Milestone 12
- [x] Test factory `get_node_spec()`
- [x] Test factory `get_node_funcs()`
- [x] Test `Net.from_factory()`
- [x] Test factory in DSL
- [x] Test factory serialization

### 12.5 Examples for Milestone 12
- [x] `examples/09_node_factories/` - Creating reusable node templates

---

## Milestone 13: Checkpointing and State Serialization ✅ COMPLETED

**Goal:** Save and restore net state for resumability.

### 13.1 Full Checkpoint
- [x] Implement `Net.save_checkpoint(path)`:
  - **Require net to be paused first** (raise exception if not paused)
  - `net_definition.toml`: serialized net definition
  - `packet_states.json`: packet locations
  - `packet_values.pkl`: pickled packet values
  - `node_configs.json`: node configuration
  - `metadata.json`: checkpoint metadata
  - `history.jsonl`: event history (if configured)
- [x] Implement `Net.load_checkpoint(path)`:
  - Restore net definition
  - Restore packet values
  - Restore packet locations
  - Net starts in paused state, ready to resume

### 13.2 Definition-Only Serialization
- [x] Implement `Net.save_definition(path)`:
  - Save TOML without runtime state
- [x] Implement `Net.load_definition(path)`:
  - Load net from TOML

### 13.3 Tests for Milestone 13
- [x] Test checkpoint save/load
- [x] Test checkpoint requires paused net (error if not paused)
- [x] Test checkpoint with pending packets and startable epochs
- [x] Test definition save/load
- [x] Test resumption after load

### 13.4 Examples for Milestone 13
- [x] `examples/10_checkpointing/` - Saving and resuming net state

---

## Milestone 14: Integration and Polish (IN PROGRESS)

**Goal:** Final integration, documentation, and examples.

### 14.1 Integration Tests
- [x] End-to-end test: complex multi-node flow
- [x] Test with real-world patterns:
  - Producer-consumer
  - Fan-out/fan-in
  - Pipeline with error handling
  - Checkpoint and resume
  - TOML DSL integration
  - Combined features (pools, rate limiting, history)

### 14.2 Documentation
- [ ] Complete docstrings for all public APIs
- [x] Add example notebooks demonstrating:
  - Basic usage (00_basic_setup, 01_simple_pipeline)
  - Async nodes (03_async_nodes)
  - Thread pools (04_background_runner)
  - Error handling and retries (02_error_handling)
  - Checkpointing (10_checkpointing)
  - DSL usage (08_dsl_format)

### 14.3 Performance Tests
- [x] Benchmark packet throughput
- [x] Benchmark with thread/process pools
- [x] Memory usage under load

### 14.4 Examples for Milestone 14
- [x] `examples/11_complete_application/` - Full real-world ETL example combining all features

---

## Implementation Order Summary

1. **Milestone 1**: Core Foundation (errors, packet store, basic Net) ✅
2. **Milestone 2**: Execution Contexts (NodeExecutionContext, deferred actions) ✅
3. **Milestone 3**: Single-Threaded Execution (run_step, start) ✅
4. **Milestone 4**: Error Handling (retries, dead letter queue) ✅
5. **Milestone 5**: Async Execution ✅
6. **Milestone 6**: Thread and Process Pools ✅
7. **Milestone 7**: Rate Limiting and Parallel Control ✅
8. **Milestone 8**: Logging and History ✅
9. **Milestone 9**: Port Types ✅
10. **Milestone 10**: SIGINT Handling (DEFERRED)
11. **Milestone 11**: DSL Format ✅
12. **Milestone 12**: Node Factories ✅
13. **Milestone 13**: Checkpointing ✅
14. **Milestone 14**: Integration and Polish (MOSTLY COMPLETE - missing: complete docstrings)

---

## Examples Directory Structure

Examples use nblite's notebook format:

```
pts/examples/                       # Source notebooks (percent format)
├── 00_basic_setup.pct.py          # Milestone 1: Net setup and configuration
├── 01_simple_pipeline.pct.py      # Milestone 3: Linear pipeline
├── 02_error_handling.pct.py       # Milestone 4: Retries and recovery
├── 03_async_nodes.pct.py          # Milestone 5: Async execution
├── 04_background_runner.pct.py    # Milestone 6: Background execution with pools
├── 05_rate_limiting.pct.py        # Milestone 7: Rate limiting
├── 06_logging_history.pct.py      # Milestone 8: Logging
├── 07_port_types.pct.py           # Milestone 9: Port type checking
├── 08_dsl_format.pct.py           # Milestone 11: TOML format
├── 09_node_factories.pct.py       # Milestone 12: Factories
├── 10_checkpointing.pct.py        # Milestone 13: Checkpointing
└── 11_complete_application.pct.py # Milestone 14: Full example (TODO)

nbs/examples/                       # Generated Jupyter notebooks
src/examples/                       # Generated Python modules (runnable)
```

To export examples after editing:
```bash
nbl export --pipeline 'pts_examples->nbs_examples'
nbl export --pipeline 'nbs_examples->lib_examples'
```

---

## Notes

- Each milestone should be independently testable
- Run `nbl export --export-pipeline "pts->nbs"` after editing `.pct.py` files
- Run `nbl export` to generate Python modules
- Run `nbl test` to verify notebooks execute correctly
- Run examples with `./examples/XX_name/run_example.sh`
- The `netrun-sim` library handles flow mechanics; `netrun` handles execution
