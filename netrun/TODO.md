# netrun Development Plan

This document outlines the development plan for the `netrun` Python package, which provides a high-level runtime for flow-based development graphs built on top of `netrun-sim`.

---

## Milestone 1: Core Foundation

**Goal:** Establish the basic Net class structure and packet value management.

### 1.1 Core Module Setup
- [ ] Set up `00_core.pct.py` with proper nblite structure
- [ ] Re-export `netrun_sim` types for user convenience (errors, graph types, etc.)
  - Use reflection/introspection where possible to avoid duplication
- [ ] Define `NetrunError` base class for netrun-specific errors
- [ ] Define netrun-specific error types (inheriting from `NetrunError`):
  - `PacketTypeMismatch`
  - `ValueFunctionFailed`
  - `NodeExecutionFailed`
  - `EpochTimeout`
  - `EpochCancelled`

### 1.2 Packet Value Storage
- [ ] Implement `PacketValueStore` class:
  - Store packet values by PacketID
  - Support value functions (lazy evaluation)
  - Handle consumed packet storage with configurable limits
  - Support optional file-based storage (pickle)
- [ ] Implement value function handling:
  - Sync value functions
  - Async value functions
  - Error handling for value function failures

### 1.3 Basic Net Class
- [ ] Implement `Net.__init__()`:
  - Accept `Graph` from `netrun_sim`
  - Initialize internal `NetSim` (hidden from user)
  - Set up packet value store
  - Configuration options: `consumed_packet_storage`, `consumed_packet_storage_limit`
- [ ] Implement `Net.set_node_exec()` for setting execution functions
- [ ] Implement `Net.set_node_config()` for node configuration
- [ ] Store node execution functions and configurations internally
- [ ] Implement wrapper methods to hide NetSim:
  - `get_startable_epochs()` - list of startable epoch IDs
  - `get_startable_epochs_by_node(node_name)` - startable epochs for a specific node
  - `get_epoch(epoch_id)` - get epoch by ID
  - `get_packet(packet_id)` - get packet by ID

### 1.4 Tests for Milestone 1
- [ ] Test error type definitions and hierarchy
- [ ] Test `PacketValueStore`:
  - Direct value storage/retrieval
  - Value function evaluation (sync and async)
  - Storage limits and eviction
  - File-based storage
- [ ] Test basic `Net` construction
- [ ] Test `set_node_exec()` and `set_node_config()`

---

## Milestone 2: Node Execution Contexts

**Goal:** Implement the execution contexts that nodes use to interact with the network.

### 2.1 NodeExecutionContext
- [ ] Implement `NodeExecutionContext` class:
  - Properties: `epoch_id`, `node_name`, `retry_count`, `retry_timestamps`, `retry_exceptions`
  - Access to `_net` (private)
- [ ] Implement context packet operations:
  - `create_packet(value)` - create packet with direct value
  - `create_packet_from_value_func(func)` - create packet with lazy value
  - `consume_packet(packet)` - consume and return value
  - `load_output_port(port_name, packet)` - load packet to output port
  - `send_output_salvo(salvo_condition_name)` - send output salvo
- [ ] Implement `cancel_epoch()` - raises `EpochCancelled`
- [ ] Support both sync and async variants of all methods

### 2.2 NodeFailureContext
- [ ] Implement `NodeFailureContext` class:
  - Properties: `epoch_id`, `node_name`, `retry_count`, `retry_timestamps`, `retry_exceptions`
  - `input_salvo`: dict of input packets
  - `packet_values`: dict of consumed values

### 2.3 Deferred Actions System
- [ ] Implement `DeferredPacket` class:
  - Behaves like `Packet` for all node operations
  - `id` property raises exception if accessed (ID not yet assigned)
  - Resolved to real `Packet` on commit
- [ ] Implement action deferral for `defer_net_actions=True`:
  - Queue all packet operations (create, consume, load, send)
  - `create_packet()` returns `DeferredPacket` instead of `Packet`
  - Commit queue on successful completion (assign real PacketIDs)
  - Discard queue on failure (keep for logging)
- [ ] Implement "unconsume" logic for retries:
  - Restore packet values on failure so they can be consumed again
  - Value functions may be called multiple times (document purity requirement)

### 2.4 Tests for Milestone 2
- [ ] Test `NodeExecutionContext` operations (sync)
- [ ] Test `NodeExecutionContext` operations (async)
- [ ] Test `NodeFailureContext` creation and properties
- [ ] Test `DeferredPacket`:
  - Behaves like Packet for load_output_port
  - `id` property raises exception
  - Resolved to real Packet on commit
- [ ] Test deferred actions:
  - Successful commit
  - Discard on failure
  - Unconsume behavior (value functions called again on retry)
- [ ] Test `cancel_epoch()` behavior

---

## Milestone 3: Single-Threaded Execution

**Goal:** Implement synchronous, single-threaded net execution.

### 3.1 Epoch Execution Loop
- [ ] Implement internal `_execute_epoch()` method:
  - Create `NodeExecutionContext`
  - Call `exec_node_func` with context and packets
  - Handle success: commit deferred actions, finish epoch
  - Handle failure: call `exec_failed_node_func`, handle retries
- [ ] Implement timeout handling:
  - Wall-clock timeout from epoch start
  - Cancel epoch on timeout

### 3.2 Run Methods
- [ ] Implement `Net.run_step(start_epochs=True)`:
  - Call `RunNetUntilBlocked` on NetSim
  - If `start_epochs=True`: start and execute ready epochs
  - Wait for all started epochs to complete
- [ ] Implement `Net.start()`:
  - Run `run_step()` in loop until fully blocked
  - No more startable epochs and no packets can move

### 3.3 Start/Stop Node Functions
- [ ] Call `start_node_func` when net starts (in `Net.start()`)
- [ ] Call `stop_node_func` when net stops
- [ ] Handle both sync and async variants

### 3.4 Tests for Milestone 3
- [ ] Test simple linear flow execution
- [ ] Test branching flow execution
- [ ] Test epoch timeout handling
- [ ] Test `start_node_func` and `stop_node_func` calls
- [ ] Test `run_step()` with `start_epochs=True/False`
- [ ] Test full `start()` execution

---

## Milestone 4: Error Handling and Retries

**Goal:** Implement robust error handling, retries, and dead letter queue.

### 4.1 Retry System
- [ ] Implement retry logic in `_execute_epoch()`:
  - Respect `retries` config (number of attempts)
  - Respect `retry_wait` config (delay between retries)
  - Track `retry_timestamps` and `retry_exceptions`
  - Enforce `defer_net_actions=True` when `retries > 0`
- [ ] Call `exec_failed_node_func` after each failure (including final)

### 4.2 Dead Letter Queue
- [ ] Implement `DeadLetterQueue` class:
  - Modes: "memory", "file", callback
  - Store failed packets with context (node, exception, timestamps)
- [ ] Implement `Net.dead_letter_queue` property
- [ ] Methods: `get_all()`, `get_by_node(node_name)`

### 4.3 Net-Level Error Handling
- [ ] Implement `on_error` config with three modes:
  - `"continue"`: Net continues running other nodes; failed epoch is cancelled
  - `"pause"`: Net pauses (finishes running epochs, doesn't start new ones)
  - `"raise"`: Net pauses first, then raises the exception to the caller
- [ ] Implement `error_callback` config

### 4.4 Tests for Milestone 4
- [ ] Test retry execution (success on retry N)
- [ ] Test retry wait timing
- [ ] Test max retries exceeded behavior
- [ ] Test `exec_failed_node_func` calls
- [ ] Test dead letter queue (memory mode)
- [ ] Test dead letter queue (file mode)
- [ ] Test dead letter queue (callback mode)
- [ ] Test `on_error="continue"` behavior
- [ ] Test `on_error="pause"` behavior
- [ ] Test `on_error="raise"` behavior
- [ ] Test `error_callback` invocation

---

## Milestone 5: Async Execution

**Goal:** Support async node functions and async Net methods.

### 5.1 Async Node Functions
- [ ] Detect if `exec_node_func` is async (via `asyncio.iscoroutinefunction`)
- [ ] Execute async functions properly with `await`
- [ ] Make `NodeExecutionContext` methods async when func is async

### 5.2 Async Net Methods
- [ ] Implement `Net.async_run_step()`
- [ ] Implement `Net.async_start()`
- [ ] Implement `Net.async_pause()`
- [ ] Implement `Net.async_stop()`
- [ ] Implement `Net.async_wait_until_blocked()`

### 5.3 Mixed Sync/Async
- [ ] Allow mixing sync and async nodes in same net
- [ ] Proper event loop management

### 5.4 Tests for Milestone 5
- [ ] Test async `exec_node_func` execution
- [ ] Test async context methods
- [ ] Test `async_run_step()` and `async_start()`
- [ ] Test mixed sync/async nodes
- [ ] Test async value functions

---

## Milestone 6: Thread and Process Pools

**Goal:** Enable parallel execution via thread and process pools.

### 6.1 Pool Management
- [ ] Implement pool configuration in `Net.__init__()`:
  - `thread_pools`: dict of pool configs `{"name": {"size": N}}`
  - `process_pools`: dict of pool configs
- [ ] Create and manage `ThreadPoolExecutor` instances
- [ ] Create and manage `ProcessPoolExecutor` instances
- [ ] Process pool requirements:
  - Packet values must be picklable (raise clear error if not)
  - Each worker process maintains its own event loop for async functions
  - Ensure proper error propagation from worker processes

### 6.2 Node Pool Assignment
- [ ] Support `pool` config: single name or list of names
- [ ] Implement pool selection algorithm:
  - "least_busy" (default): pool with most available workers
- [ ] Support `pool_init_mode`: "global" vs "per_worker"

### 6.3 Pool Lifecycle
- [ ] Call `start_node_func` per worker (default) or globally
- [ ] Call `stop_node_func` per worker or globally
- [ ] Proper pool shutdown on net stop

### 6.4 Threaded Net Execution
- [ ] Implement `Net.start(threaded=True)`:
  - Run net in background thread
- [ ] Implement `Net.run_step(threaded=True)`
- [ ] Implement control methods:
  - `wait_until_blocked()`
  - `poll()` - check status
  - `pause()` - finish current epochs, don't start new
  - `stop()` - stop entirely

### 6.5 Tests for Milestone 6
- [ ] Test thread pool creation and configuration
- [ ] Test process pool creation and configuration
- [ ] Test node execution in thread pool
- [ ] Test node execution in process pool
- [ ] Test pool selection algorithm
- [ ] Test per-worker vs global init mode
- [ ] Test `threaded=True` execution
- [ ] Test `wait_until_blocked()`, `poll()`, `pause()`, `stop()`

---

## Milestone 7: Rate Limiting and Parallel Epoch Control

**Goal:** Control execution rate and parallelism per node.

### 7.1 Rate Limiting
- [ ] Implement `rate_limit_per_second` config
- [ ] Track epoch start timestamps per node
- [ ] Delay epoch starts to respect rate limit

### 7.2 Parallel Epoch Control
- [ ] Implement `max_parallel_epochs` config
- [ ] Track running epochs per node
- [ ] Delay epoch starts when at max

### 7.3 Tests for Milestone 7
- [ ] Test rate limiting accuracy
- [ ] Test `max_parallel_epochs` enforcement
- [ ] Test interaction between rate limit and max parallel

---

## Milestone 8: Logging and History

**Goal:** Comprehensive logging, history tracking, and stdout capture.

### 8.1 Event History
- [ ] Implement history storage:
  - Record all `NetAction` and `NetEvent` with timestamps
  - Add unique IDs for traceability
  - Store action→events relationships
- [ ] History configuration:
  - `history_max_size`: max events in memory
  - `history_file`: persist to JSONL file
  - `history_chunk_size`: write in chunks
  - `history_flush_on_pause`: flush when paused/blocked

### 8.2 Node-Level Logging
- [ ] Implement stdout capture via custom `print` injection
- [ ] Support `capture_stdout` config (per node)
- [ ] Support `echo_stdout` config (also print to real stdout)
- [ ] Store logs per node and per epoch
- [ ] Methods: `get_node_log(node_name)`, `get_epoch_log(epoch_id)`

### 8.3 Tests for Milestone 8
- [ ] Test history recording accuracy
- [ ] Test history persistence to JSONL
- [ ] Test history size limits
- [ ] Test stdout capture
- [ ] Test `echo_stdout` behavior
- [ ] Test `get_node_log()` and `get_epoch_log()`

---

## Milestone 9: Port Types

**Goal:** Type checking for packet values on ports.

### 9.1 Port Type Definition
- [ ] Support type specifications:
  - String class name: `"DataFrame"`
  - Actual class: `pandas.DataFrame`
  - Dict with options: `{"class": MyClass, "isinstance": True/False, "subclass": True}`

### 9.2 Type Checking
- [ ] Check input port types when packet is consumed via `ctx.consume_packet()`
  - (Not at epoch creation, since value may not be known until consumption)
- [ ] Check output port types when packet loaded via `ctx.load_output_port()`
- [ ] Raise `PacketTypeMismatch` on mismatch

### 9.3 Tests for Milestone 9
- [ ] Test string class name matching
- [ ] Test actual class matching
- [ ] Test isinstance checking
- [ ] Test exact type checking
- [ ] Test subclass checking
- [ ] Test type mismatch error

---

## Milestone 10: SIGINT Handling

**Goal:** Graceful shutdown on SIGINT.

### 10.1 Signal Handler
- [ ] Register SIGINT handler when net starts
- [ ] On SIGINT: equivalent to `net.pause()`
- [ ] Allow running epochs to finish
- [ ] Don't start new epochs
- [ ] Restore original handler on stop

### 10.2 Tests for Milestone 10
- [ ] Test graceful shutdown on SIGINT
- [ ] Test running epochs complete before shutdown

---

## Milestone 11: DSL Format (TOML)

**Goal:** Human-readable serialization format for nets.

### 11.1 TOML Parser
- [ ] Parse `[net]` section:
  - Net-level config (on_error, history_file, etc.)
  - Thread/process pool definitions
  - Metadata
- [ ] Parse `[nodes.X]` sections:
  - Port definitions (simple list or detailed config)
  - Salvo conditions with expression language
  - Node options
  - Exec function paths
  - Factory references
  - Metadata

### 11.2 Salvo Condition Expression Parser
- [ ] Parse expressions:
  - `nonempty(port)`, `empty(port)`, `full(port)`
  - `count(port) >= N`, `count(port) == N`
  - `and`, `or`, `not`
  - Parentheses for grouping
- [ ] Convert to `SalvoConditionTerm`
- [ ] Handle `ports` field:
  - If omitted, include all input/output ports (depending on condition type)
- [ ] Handle `max_salvos` field:
  - `"infinite"` maps to `MaxSalvos.Infinite`
  - Positive integer maps to `MaxSalvos.finite(n)`

### 11.3 Edge Definition Parser
- [ ] Parse edge list format: `[{ from = "A.out", to = "B.in" }, ...]`
- [ ] Parse detailed edge format with metadata

### 11.4 TOML Serializer
- [ ] Serialize `Net` to TOML string
- [ ] Preserve all configuration
- [ ] Pretty-print salvo conditions

### 11.5 Tests for Milestone 11
- [ ] Test parsing simple net definition
- [ ] Test parsing complex salvo conditions
- [ ] Test parsing edge definitions
- [ ] Test round-trip: parse → serialize → parse
- [ ] Test error messages for invalid TOML

---

## Milestone 12: Node Factories

**Goal:** Reusable node templates via factory pattern.

### 12.1 Factory Protocol
- [ ] Define factory interface:
  - `get_node_spec(**args) -> dict` (Node kwargs)
  - `get_node_funcs(**args) -> tuple` (exec, start, stop, failed)
- [ ] Support factory import from string path

### 12.2 Factory Usage
- [ ] Implement `Net.from_factory()` class method
- [ ] Support factory args in DSL:
  - `factory = "module.factory"`
  - `factory_args = { ... }`

### 12.3 Factory Serialization
- [ ] Require JSON-serializable args for serialization
- [ ] Support callable args as import paths

### 12.4 Tests for Milestone 12
- [ ] Test factory `get_node_spec()`
- [ ] Test factory `get_node_funcs()`
- [ ] Test `Net.from_factory()`
- [ ] Test factory in DSL
- [ ] Test factory serialization

---

## Milestone 13: Checkpointing and State Serialization

**Goal:** Save and restore net state for resumability.

### 13.1 Full Checkpoint
- [ ] Implement `Net.save_checkpoint(path)`:
  - **Require net to be paused first** (raise exception if not paused)
  - `net_state.json`: serialized NetSim state
  - `packets.pkl`: pickled packet values
  - `history.jsonl`: event history (if configured)
- [ ] Implement `Net.load_checkpoint(path)`:
  - Restore NetSim state
  - Restore packet values
  - Net starts in paused state, ready to resume

### 13.2 Definition-Only Serialization
- [ ] Implement `Net.save_definition(path)`:
  - Save TOML without runtime state
- [ ] Implement `Net.load_definition(path)`:
  - Load net from TOML

### 13.3 Tests for Milestone 13
- [ ] Test checkpoint save/load
- [ ] Test checkpoint requires paused net (error if not paused)
- [ ] Test checkpoint with pending packets and startable epochs
- [ ] Test definition save/load
- [ ] Test resumption after load

---

## Milestone 14: Integration and Polish

**Goal:** Final integration, documentation, and examples.

### 14.1 Integration Tests
- [ ] End-to-end test: complex multi-node flow
- [ ] Test with real-world patterns:
  - Producer-consumer
  - Fan-out/fan-in
  - Pipeline with error handling
  - Checkpoint and resume

### 14.2 Documentation
- [ ] Complete docstrings for all public APIs
- [ ] Add example notebooks demonstrating:
  - Basic usage
  - Async nodes
  - Thread pools
  - Error handling and retries
  - Checkpointing
  - DSL usage

### 14.3 Performance Tests
- [ ] Benchmark packet throughput
- [ ] Benchmark with thread/process pools
- [ ] Memory usage under load

---

## Implementation Order Summary

1. **Milestone 1**: Core Foundation (errors, packet store, basic Net)
2. **Milestone 2**: Execution Contexts (NodeExecutionContext, deferred actions)
3. **Milestone 3**: Single-Threaded Execution (run_step, start)
4. **Milestone 4**: Error Handling (retries, dead letter queue)
5. **Milestone 5**: Async Execution
6. **Milestone 6**: Thread and Process Pools
7. **Milestone 7**: Rate Limiting and Parallel Control
8. **Milestone 8**: Logging and History
9. **Milestone 9**: Port Types
10. **Milestone 10**: SIGINT Handling
11. **Milestone 11**: DSL Format
12. **Milestone 12**: Node Factories
13. **Milestone 13**: Checkpointing
14. **Milestone 14**: Integration and Polish

---

## Notes

- Each milestone should be independently testable
- Run `nbl export --export-pipeline "pts->nbs"` after editing `.pct.py` files
- Run `nbl export` to generate Python modules
- Run `nbl test` to verify notebooks execute correctly
- The `netrun-sim` library handles flow mechanics; `netrun` handles execution
