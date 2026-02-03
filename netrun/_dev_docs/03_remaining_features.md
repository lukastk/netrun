# Remaining Features for netrun

This document outlines the features that are not yet implemented in the `netrun` package, based on a comparison of `PROJECT_SPEC.md` against the current codebase.

## Table of Contents

1. [Implementation Status Overview](#implementation-status-overview)
2. [Partially Implemented Features](#partially-implemented-features)
3. [Not Implemented Features](#not-implemented-features)
4. [Implementation Plan](#implementation-plan)

---

## Implementation Status Overview

### Fully Implemented

The following features from `PROJECT_SPEC.md` are complete:

| Feature | Location |
|---------|----------|
| Internal Utilities | `netrun._iutils` |
| Storage (PacketStore) | `netrun.storage` |
| RPC Layer | `netrun.rpc` |
| Pool Layer | `netrun.pool` |
| ExecutionManager | `netrun.execution_manager` |
| Net Configuration | `netrun.net.config` |
| Node Execution | `netrun.net._net.NodeExecutionContext` |
| Packets and Values | `ctx.create_packet()`, `ctx.consume_packet()` |
| Error Handling and Retries | `_handle_epoch_failure()`, dead letter queue |
| Output Queues System | `OutputQueueConfig`, `get_output()`, etc. |
| Packet Creation & Injection | `inject_data()`, `create_external_packet()` |
| Log Access | `list_epoch_log_ids()`, `get_all_logs_chronological()` |
| Graph Queries | `get_edges_from_port()`, `has_downstream_connection()` |

### Partially Implemented

| Feature | What Works | What's Missing |
|---------|------------|----------------|
| Dead Letter Queue | In-memory storage | File persistence, callback |
| Net Error Handling | Re-raises errors | `on_error` config option |
| Logging | Print capture | Event history recording |

### Not Implemented

| Feature | Priority | Effort |
|---------|----------|--------|
| Event History | Medium | Medium |
| Net-Level Error Config | Medium | Small |
| Port Types | Low | Medium |
| ~~Node Factories~~ | ~~Low~~ | ~~Small~~ (DONE) |
| Checkpointing | Low | Large |

---

## Partially Implemented Features

### 1. Dead Letter Queue Enhancements

#### What is a Dead Letter Queue?

A **dead letter queue (DLQ)** is a holding area for failed work items that couldn't be processed successfully. When a node execution fails after all retry attempts are exhausted, the input packets and error information are stored in the DLQ instead of being lost.

**Why it matters:**
- **Debugging**: You can inspect what data caused failures
- **Recovery**: You can manually reprocess failed items after fixing bugs
- **Monitoring**: You can track failure rates and patterns
- **No data loss**: Failed packets aren't silently discarded

#### Current State

```python
# Works - in-memory storage
failed_items = net.dead_letter_queue  # list[dict] with epoch_id, error, packets, etc.
net.clear_dead_letter_queue()
```

The current implementation stores failed epochs in memory. When a node fails after max retries, the epoch info is added to `_dead_letter_queue`. However, this data is lost when the process exits.

#### Missing Features

**1. `dead_letter_file` - Persistent Storage**

Write failed items to disk so they survive process restarts:

```python
NetConfig(
    dead_letter_file="./dlq/",  # Directory to write failed items
)
```

Each failed epoch would be written as a JSON file:
```
./dlq/
  2024-01-15T10:30:45_epoch_01HQXYZ123.json
  2024-01-15T10:31:02_epoch_01HQXYZ456.json
```

**Use case**: Long-running production jobs where you need to review failures after the fact, or reprocess them with a separate tool.

**2. `dead_letter_callback` - Custom Handling**

Call a user-defined function when items enter the DLQ:

```python
def my_dlq_handler(failed_item: dict):
    # Send alert to Slack
    # Write to database
    # Trigger remediation workflow
    send_slack_alert(f"Node {failed_item['node_name']} failed: {failed_item['error']}")

NetConfig(
    dead_letter_callback=my_dlq_handler,
)
```

**Use case**: Integration with monitoring systems, alerting, or custom storage backends.

#### Implementation

```python
# In NetConfig
dead_letter_file: str | None = None
"""Directory path to write failed items as JSON files."""

dead_letter_callback: Callable[[dict], None] | str | None = None
"""Function to call when an item enters the DLQ. Can be a callable or import path."""
```

```python
# In _handle_epoch_failure(), after adding to in-memory queue:
if self._config.dead_letter_file:
    self._write_dead_letter_file(failed_item)

if self._config.dead_letter_callback:
    self._call_dead_letter_callback(failed_item)
```

**Effort:** Small

---

### 2. Net-Level Error Handling Config

#### What is this?

Controls how the Net responds when a node fails after exhausting all retries. Currently, the error is always re-raised, which stops the entire network. But sometimes you want different behavior.

#### Current State

```python
# Current behavior: always re-raises
async def _handle_epoch_failure(...):
    # ... retry logic ...
    # After max retries:
    raise error  # This propagates up and stops the network
```

#### Missing Feature: `on_error` Config

```python
NetConfig(
    on_error="pause",  # What to do when a node fails after max retries
)
```

**Options:**

1. **`"raise"` (current default)**: Re-raise the exception, stopping the network
   - Use when: Failures are critical and you want immediate attention
   - Effect: `run_loop()` exits with an exception

2. **`"pause"`**: Pause the network, allowing inspection
   - Use when: You want to debug or manually intervene
   - Effect: `net.paused` becomes True, no new epochs start, but running ones complete

3. **`"continue"`**: Log the error and keep processing other work
   - Use when: Some failures are acceptable (e.g., processing user-generated content where some items may be malformed)
   - Effect: Failed epoch goes to DLQ, network continues with other epochs

#### Example Use Cases

```python
# Production job - stop on any failure
NetConfig(on_error="raise")

# Development - pause so I can inspect state
NetConfig(on_error="pause")

# Batch processing - some failures expected, keep going
NetConfig(on_error="continue")
```

#### Implementation

```python
# In NetConfig
on_error: Literal["continue", "pause", "raise"] = "raise"
"""How to handle node failures after max retries exhausted."""
```

```python
# In _handle_epoch_failure(), after max retries:
match self._config.on_error:
    case "raise":
        raise error
    case "pause":
        await self.pause()
        # Don't raise - return None
    case "continue":
        # Log error, don't raise
        import sys
        print(f"Node {node_name} failed (continuing): {error}", file=sys.stderr)
```

**Effort:** Small

---

### 3. Event History Recording

#### What is this?

A log of every action taken on the network and every event that occurred. This provides a complete audit trail of network execution for debugging, testing, and replay.

#### Why it matters

- **Debugging**: "What happened right before this failure?"
- **Testing**: Assert that specific actions/events occurred
- **Replay**: Re-execute the same sequence of actions
- **Visualization**: Build tools that show network execution over time

#### Current State

Only print output is captured. There's no record of:
- When packets were created/consumed
- When epochs started/finished
- When salvos were sent
- What order things happened in

#### Missing Feature: Event History

```python
NetConfig(
    history_max_size=10000,      # Keep last N entries (None = unlimited)
    history_file="./history.jsonl",  # Optional: also write to file
)
```

**What gets recorded:**

Every call to `netsim.do_action()` produces:
- The action taken (e.g., `CreatePacket`, `StartEpoch`, `SendOutputSalvo`)
- The events that resulted (e.g., `PacketCreated`, `EpochStarted`, `PacketMoved`)

```python
# Example history entry
{
    "timestamp": "2024-01-15T10:30:45.123Z",
    "action": {"type": "StartEpoch", "epoch_id": "01HQXYZ..."},
    "events": [
        {"type": "EpochStarted", "epoch_id": "01HQXYZ...", "node_name": "Process"}
    ]
}
```

**API:**

```python
# Get history
history = net.get_history()  # list of (timestamp, action, events)

# Get history for specific node
history = net.get_history(node_name="Process")

# Get history in time range
history = net.get_history(since=datetime(...), until=datetime(...))
```

#### Implementation

```python
# In NetConfig
history_max_size: int | None = None
"""Max history entries to keep in memory. None = unlimited."""

history_file: str | None = None
"""Path to JSONL file for persistent history."""
```

```python
# In Net.__init__
from collections import deque
self._history: deque[tuple[datetime, Any, list]] = deque(maxlen=config.history_max_size)

# Wrap do_action calls
def _do_action_with_history(self, action):
    response, events = self._netsim.do_action(action)

    entry = (get_timestamp_utc(), action, list(events))
    self._history.append(entry)

    if self._config.history_file:
        self._write_history_entry(entry)

    return response, events
```

**Effort:** Medium

---

## Not Implemented Features

### 1. Port Types (Type Checking)

#### What is this?

Validation that packet values match expected types when flowing through ports. Catches type mismatches early instead of getting confusing errors deep in node logic.

#### Why it matters

Without type checking:
```python
def process_node(ctx, packets):
    df = ctx.consume_packet(packets["in"][0])
    df.groupby("category")  # AttributeError: 'str' object has no attribute 'groupby'
    # Confusing! Where did the string come from?
```

With type checking:
```python
# Port configured with port_type="DataFrame"
# Error at packet creation time:
# PacketTypeMismatch: Port 'out' expects DataFrame, got str
```

#### Proposed API

```python
from netrun.net.config import PortConfig

# By class name (string) - checked with type(value).__name__
PortConfig(port_type="DataFrame")

# By class - checked with isinstance()
import pandas as pd
PortConfig(port_type=pd.DataFrame)

# With explicit isinstance flag
PortConfig(port_type={"class": MyClass, "isinstance": True})
```

#### When validation happens

1. **`ctx.load_output_port()`**: Validate packet value matches output port type
2. **`ctx.consume_packet()`**: Optionally validate input port type (may be redundant if output was checked)

#### Implementation

```python
# In PortConfig
port_type: str | type | dict | None = None
"""Expected type for packets on this port. None = no validation."""

# New exception
class PacketTypeMismatch(NetrunError):
    def __init__(self, port_name: str, expected: str, actual: str, packet_id: str):
        self.port_name = port_name
        self.expected = expected
        self.actual = actual
        self.packet_id = packet_id
        super().__init__(
            f"Port '{port_name}' expects {expected}, got {actual} (packet {packet_id})"
        )
```

**Effort:** Medium

---

### 2. Node Factories ✅ IMPLEMENTED

Node factories are now fully implemented. See `_dev_docs/04_node_factories.md` for full documentation.

#### Summary

A factory is a Python module with two functions:
- `get_node_config(**args) -> NodeGraphConfig` - returns graph structure (without execution_config)
- `get_node_funcs(**args) -> tuple` - returns (exec_func, start_func, stop_func, on_failure_func)

#### Usage

```python
# Using from_factory() class method
config = NodeGraphConfig.from_factory(
    factory="myapp.nodes.worker",
    args={"name": "Worker1", "threshold": 0.5},
)

# Using factory field (auto-expands)
config = NodeGraphConfig(
    factory="myapp.nodes.worker",
    factory_args={"name": "Worker1", "threshold": 0.5},
)

# With overrides
config = NodeGraphConfig(
    factory="myapp.nodes.worker",
    factory_args={"name": "Worker1"},
    name="CustomName",  # Override factory's name
)
```

#### Tests

See `src/tests/net/test_node_factory.py` and `src/tests/net/sample_factory.py`.

---

### 3. Checkpointing and State Serialization

#### What is this?

Save the complete state of a running network to disk, and restore it later to continue execution.

#### Why it matters

- **Long-running jobs**: A 10-hour batch job crashes at hour 8. Without checkpointing, you restart from the beginning. With checkpointing, you resume from the last checkpoint.
- **Debugging**: Save state when an error occurs, load it later to investigate.
- **Testing**: Create a checkpoint at a known state, use it as a test fixture.

#### Proposed API

```python
# Save state (must be paused)
await net.pause()
await net.save_checkpoint("./checkpoint/")

# Load and resume
net = await Net.load_checkpoint("./checkpoint/")
await net.resume()
```

#### What gets saved

1. **NetConfig** (serialized to JSON)
2. **PacketStore state** (all packet values)
3. **NetSim state**:
   - All packets and their locations
   - All epochs and their states (Startable, Running, Finished)
   - Salvo condition counters
4. **Net state**:
   - Print logs
   - Dead letter queue
   - Rate limit tracking

#### Challenges

1. **Callable serialization**: `exec_node_func` is a Python function. Must store import path and restore via import.
2. **NetSim serialization**: Need to add serialization support to netrun-sim (Rust side).
3. **Running epochs**: Can't checkpoint while epochs are running (must be paused).
4. **Worker state**: If workers have internal state, it's lost.

#### Implementation

Requires changes to netrun-sim to support state serialization:
```rust
// In netrun-sim
impl NetSim {
    fn serialize(&self) -> Vec<u8> { ... }
    fn deserialize(data: &[u8]) -> Self { ... }
}
```

Then in Python:
```python
async def save_checkpoint(self, path: str) -> None:
    if not self._paused:
        raise RuntimeError("Must pause before checkpointing")

    os.makedirs(path, exist_ok=True)

    # Save config (with exec_node_func as import paths)
    config_data = self._serialize_config()
    write_json(f"{path}/config.json", config_data)

    # Save packet store
    self._packet_store.save(f"{path}/packets.pkl")

    # Save netsim state
    netsim_data = self._netsim.serialize()
    write_bytes(f"{path}/netsim.bin", netsim_data)

    # Save logs and DLQ
    write_json(f"{path}/logs.json", {
        "epoch_logs": self._epoch_print_logs,
        "node_logs": self._node_print_logs,
        "dead_letter": self._dead_letter_queue,
    })
```

**Effort:** Large (requires netrun-sim changes)

---

## Implementation Plan

### Phase 1: Quick Wins (Small Effort)

**1. Net-Level Error Config** (1-2 hours)
- Add `on_error` to `NetConfig`
- Update `_handle_epoch_failure()` with match statement
- Add tests for each mode

**2. Dead Letter Enhancements** (2-3 hours)
- Add `dead_letter_file` and `dead_letter_callback` to `NetConfig`
- Implement `_write_dead_letter_file()`
- Implement `_call_dead_letter_callback()`
- Add tests

### Phase 2: Medium Features

**3. Event History Recording** (4-6 hours)
- Add history config to `NetConfig`
- Add `_history` deque to `Net`
- Wrap `do_action` calls to record history
- Add `get_history()` method with filtering
- Optional: JSONL file writing
- Add tests

**4. Port Types** (4-6 hours)
- Add `port_type` to `PortConfig`
- Add `PacketTypeMismatch` exception
- Implement validation in `DeferredActionQueue` or commit phase
- Add tests

### Phase 3: Advanced Features

**5. Node Factories** ✅ COMPLETE
- Added `factory` and `factory_args` fields to `NodeGraphConfig`
- Added `from_factory()` class method
- Implemented `@model_validator` for automatic factory expansion
- Added `@field_serializer` for serialization of factory modules and function fields
- Removed redundant `node_name` from `NodeExecutionConfig`
- Tests: `src/tests/net/test_node_factory.py`

**6. Checkpointing** (1-2 days, requires netrun-sim work)
- Add serialization to netrun-sim (Rust)
- Implement `save_checkpoint()` and `load_checkpoint()`
- Handle callable serialization via import paths
- Add tests

---

## Notes

### On Priority

The current implementation covers all **core functionality** needed to run flow-based networks. The remaining features improve:

| Category | Features |
|----------|----------|
| **Debugging** | Event history, port type checking |
| **Reliability** | Error config, dead letter persistence |
| **Operations** | Checkpointing for long-running jobs |

None of these are blockers for using netrun in development or testing scenarios.

### On PROJECT_SPEC.md

The spec is significantly outdated. Consider either:
1. Updating it to reflect current state
2. Deprecating it in favor of these dev docs

### On Testing

Each feature should include:
1. Unit tests in `src/tests/net/`
2. Update to example notebook if user-facing
3. Update to this doc marking feature as complete
