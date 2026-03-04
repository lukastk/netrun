# Node Signals Implementation Plan

## 1. Overview

**Goal**: Enable nodes to emit "signal" packets on lifecycle events (finished, failed, etc.) that trigger downstream nodes through standard packet/port/edge mechanics.

**Primary Use Case**: Node B needs to start after Node A finishes, even if Node A produces no data output. Signals provide the triggering mechanism.

**Success Criteria**:
- Signal ports are auto-generated on nodes based on configuration
- Signal packets are emitted by the Net orchestrator (not user code) after epoch lifecycle events
- Downstream nodes receive signals via standard input ports and salvo conditions
- No changes required to `netrun-sim` — signals are regular packets/ports/edges
- Configuration is opt-in, both per-node and globally

**Scope**:
- In scope: `finished`, `failed` signal types; configuration; auto-generation of ports/salvos; emission logic in Net; function factory integration; TOML support
- Out of scope: `started`, `retrying` signals (can be added later); UI changes (separate task)

## 2. Design Philosophy

Signals are **not** a new primitive. They are regular packets emitted by the Net orchestrator at specific lifecycle points. This means:

- **Signal output ports** are regular `out_ports` on the emitting node (e.g., `__signal_finished__`)
- **Signal edges** are regular edges connecting signal output ports to downstream input ports
- **Signal packets** are regular packets with metadata as their value
- **Downstream nodes** receive signal packets via standard input salvo conditions
- **No netrun-sim changes** — the Rust layer is unaware of signals

The Net class is the sole emitter of signal packets. After an epoch finishes (or fails), the Net:
1. Creates a signal packet (with metadata value) inside the epoch
2. Loads it into the signal output port
3. Sends the signal output salvo
4. Then finishes/cancels the epoch

This requires **reordering** the current epoch finish flow: signal packets must be created and sent *before* `finish_epoch()` is called (because `finish_epoch` requires the epoch to be empty of packets).

## 3. Signal Types

There are two categories of lifecycle events, and the naming must distinguish them clearly:

- **Epoch signals**: Events related to individual epoch executions (a node can have many epochs)
- **Node signals**: Events related to the node's own startup/shutdown lifecycle (once per Net run)

The word "started" is ambiguous (does it mean the node initialized, or an epoch began?), so we use
distinct names: `node_started`/`node_stopped` for node lifecycle, `epoch_finished`/`epoch_failed`
for epoch lifecycle.

### Phase 1 (MVP)

#### Epoch Signals
| Signal | Port Name | Emitted When | Value |
|--------|-----------|--------------|-------|
| `epoch_finished` | `__signal_epoch_finished__` | Epoch finishes successfully | `SignalValue(signal="epoch_finished", node_name, epoch_id, timestamp)` |
| `epoch_failed` | `__signal_epoch_failed__` | Epoch fails (max retries exceeded, cancelled) | `SignalValue(signal="epoch_failed", node_name, epoch_id, timestamp, error_str)` |

#### Node Lifecycle Signals
| Signal | Port Name | Emitted When | Value |
|--------|-----------|--------------|-------|
| `node_started` | `__signal_node_started__` | After `start_node_func` completes (or node is marked started) | `SignalValue(signal="node_started", node_name, epoch_id=None, timestamp)` |
| `node_stopped` | `__signal_node_stopped__` | After `stop_node_func` completes (or node is marked stopped) | `SignalValue(signal="node_stopped", node_name, epoch_id=None, timestamp)` |

**Node lifecycle signal emission details:**
- `node_started`: Emitted at the end of `_start_node()`, after `start_node_func` has been called (or immediately if no start func). This happens during `Net.start()` for non-deferred nodes, or on first epoch for deferred nodes.
- `node_stopped`: Emitted at the end of `_stop_node()`, after `stop_node_func` has been called (or immediately if no stop func). This happens during `Net.stop()`.
- Since node start/stop happen outside any epoch, these signals use the create-outside-net + transport-to-edge approach (same as failure signals).

### Phase 2 (Future)
| Signal | Port Name | Emitted When |
|--------|-----------|--------------|
| `epoch_started` | `__signal_epoch_started__` | Epoch transitions to Running |
| `epoch_retrying` | `__signal_epoch_retrying__` | Retry attempt begins |
| `epoch_max_exceeded` | `__signal_epoch_max_exceeded__` | max_epochs limit hit |

### Signal Port Naming Convention
- All signal ports use the prefix `__signal_` and suffix `__` (dunder convention)
- This avoids collisions with user-defined port names
- Helper: `is_signal_port(name: str) -> bool` checks for this pattern

## 4. Signal Packet Value

```python
@dataclass
class SignalValue:
    """Value carried by a signal packet."""
    signal: str                # "epoch_finished", "epoch_failed", "node_started", "node_stopped"
    node_name: str             # Name of the emitting node
    epoch_id: str | None       # Epoch that triggered the signal (None for node lifecycle signals)
    timestamp: datetime        # When the signal was emitted
    error: str | None = None   # Error message (for "epoch_failed" signals)
```

`SignalValue` is a simple dataclass — it's the value stored in the PacketStore for signal packets, just like any other packet value.

## 5. Configuration

### 5.1 Per-Node Configuration (in `NodeExecutionConfig`)

```python
class NodeExecutionConfig(EnvVarResolvableModel):
    # ... existing fields ...
    signals: list[str] | VarRef = Field(
        default_factory=list,
        description="Signal types to emit. e.g., ['finished', 'failed']. Empty = no signals."
    )
```

- `signals: []` (default) — no signal ports, no signal packets
- `signals: ["epoch_finished"]` — adds `__signal_epoch_finished__` output port
- `signals: ["epoch_finished", "epoch_failed"]` — adds both epoch signal output ports
- `signals: ["node_started", "node_stopped"]` — adds node lifecycle signal output ports
- `signals: ["epoch_finished", "epoch_failed", "node_started", "node_stopped"]` — all MVP signals

### 5.2 Net-Level Default (in `NetConfig`)

```python
class NetConfig(EnvVarResolvableModel):
    # ... existing fields ...
    default_signals: list[str] | VarRef = Field(
        default_factory=list,
        description="Default signal types for all nodes. Can be overridden per-node."
    )
```

**Resolution**: Per-node `signals` overrides net-level `default_signals`. If a node sets `signals: []` explicitly, it opts out even if `default_signals` is set. If a node doesn't set `signals` (None/unset), it inherits from `default_signals`.

To support this override-vs-inherit semantic, use `None` as "inherit":
```python
signals: list[str] | VarRef | None = Field(
    default=None,
    description="Signal types to emit. None = inherit from NetConfig.default_signals. [] = no signals."
)
```

### 5.3 TOML Configuration

```toml
# Net-level default: all nodes emit "epoch_finished" signal
default_signals = ["epoch_finished"]

# Node with signals
[[graph.nodes]]
name = "processor"
factory = "netrun.node_factories.from_function"
factory_args = { func = "nodes.process" }

[graph.nodes.execution_config]
signals = ["epoch_finished", "epoch_failed"]

# Downstream node triggered by signal
[[graph.nodes]]
name = "notifier"
factory = "netrun.node_factories.from_function"
factory_args = { func = "nodes.notify" }

# Edge from signal port to downstream node
[[graph.edges]]
source_str = "processor.__signal_epoch_finished__"
target_str = "notifier.trigger"
```

## 6. Auto-Generation of Signal Ports and Salvo Conditions

When signals are configured, the system must auto-generate:

### 6.1 Output Ports
For each signal type in the effective signals list, add an output port:
```python
out_ports["__signal_epoch_finished__"] = PortConfig()  # infinite slots, no type constraint
```

### 6.2 Output Salvo Conditions
Each signal type gets its own output salvo condition:
```python
out_salvo_conditions["__signal_epoch_finished__"] = SalvoConditionConfig(
    max_salvos=MaxSalvosInfiniteConfig(),  # Can fire every epoch
    ports={"__signal_epoch_finished__": PacketCountAllConfig()},
    term=SalvoConditionTermPortConfig(
        port_name="__signal_epoch_finished__",
        state=PortStateNonEmptyConfig(),
    ),
)
```

**Key**: `max_salvos` must be `Infinite` (not `Finite(1)`) because signal ports fire on every epoch, not just once.

**Note on node lifecycle signal salvo conditions**: `node_started` and `node_stopped` signals also use `Infinite` max_salvos for consistency, even though they typically fire only once per Net run.

### 6.3 Where Auto-Generation Happens
Signal ports/salvos are injected during `NodeConfig.resolve()` or `GraphConfig.resolve()`, after factory resolution but before the graph is built. This is done by checking the effective signals list (from `execution_config.signals` or `NetConfig.default_signals`) and adding ports + salvo conditions that don't already exist.

## 7. Signal Emission in Net

### 7.1 Epoch Success Path (`_execute_epoch_with_retry`)

Current flow (after line ~1847):
```
1. _commit_epoch_result(epoch_id, result)  # commits user's deferred actions
2. snapshot epoch
3. cache/file storage
4. finish_epoch(epoch_id)                  # epoch must be empty
```

New flow:
```
1. _commit_epoch_result(epoch_id, result)
2. snapshot epoch
3. cache/file storage
4. _emit_signal(epoch_id, node_name, "epoch_finished")  # NEW: create + load + send signal
5. finish_epoch(epoch_id)
```

### 7.2 Epoch Failure Path (`_handle_epoch_failure`, max retries exceeded)

Current flow (after line ~2465):
```
1. cancel_epoch(epoch_id)  # destroys packets inside epoch
2. dead letter queue
3. raise or queue exception
```

**Key insight**: When `_handle_epoch_failure` is called (after max retries exceeded), the epoch is still in **Running** state. The failed execution's deferred actions were discarded, but the input packets are still inside the epoch (they were never consumed). This means we can use the **exact same in-epoch mechanism** as success signals — the epoch is Running, so `create_packet(epoch_id)`, `load_packet_into_output_port`, and `send_output_salvo` all work normally.

The signal packet is created inside the epoch, loaded into the signal output port, and sent onto the edge — all *before* `cancel_epoch` is called. By the time `cancel_epoch` runs, the signal packet has already left the epoch (it's on the edge), so it survives. `cancel_epoch` only destroys the remaining input packets that were never consumed.

New flow:
```
1. _emit_epoch_signal(epoch_id, node_name, "epoch_failed", error=str(error))  # NEW
2. cancel_epoch(epoch_id)  # destroys remaining input packets (signal already on edge)
3. dead letter queue
4. raise or queue exception
```

This is clean — failure signals use the identical mechanism as success signals. No special-casing, no transport-to-edge hack, no out-of-epoch packet creation. The only difference is *when* the signal is emitted (before cancel vs before finish).

### 7.3 Unified Epoch Signal Implementation

Both `epoch_finished` and `epoch_failed` use the same mechanism — the epoch is Running in both cases, so we use a single `_emit_epoch_signal` method:

```python
def _emit_epoch_signal(self, epoch_id: str, node_name: str, signal_type: str, error: str | None = None):
    """Emit an epoch signal while the epoch is still Running.

    Works for both success and failure signals because the epoch is Running
    in both cases — for failures, this is called before cancel_epoch().
    """
    signals = self._get_effective_signals(node_name)
    if signal_type not in signals:
        return
    signal_port = signal_port_name(signal_type)
    # Create packet inside the running epoch
    response, _ = self._netsim.do_action(netrun_sim.NetAction.create_packet(epoch_id))
    packet_id = str(response.packet_id)
    self._packet_store.register(packet_id, SignalValue(
        signal=signal_type, node_name=node_name,
        epoch_id=epoch_id, timestamp=get_timestamp_utc(), error=error,
    ))
    # Load into signal output port
    self._netsim.do_action(
        netrun_sim.NetAction.load_packet_into_output_port(packet_id, signal_port)
    )
    # Send signal salvo (salvo condition name = port name)
    self._netsim.do_action(
        netrun_sim.NetAction.send_output_salvo(epoch_id, signal_port)
    )
    # Handle orphaned packets (if signal port is unconnected)
    epoch = self._netsim.get_epoch(epoch_id)
    for orphaned_info in epoch.orphaned_packets:
        if orphaned_info.from_port == signal_port:
            self._route_orphaned_packet(
                packet_id=orphaned_info.packet_id,
                from_node=node_name,
                from_port=signal_port,
                epoch_id=epoch_id,
            )
```

This single method handles both cases:
- **Success**: called after `_commit_epoch_result()`, before `finish_epoch()`
- **Failure**: called before `cancel_epoch()` — the signal packet escapes onto the edge, then `cancel_epoch` destroys only the remaining input packets

### 7.4 Node Lifecycle Signal Emission

Node start/stop signals are emitted outside any epoch (node startup/shutdown is not an epoch operation). These use the create-outside-net + transport-to-edge approach, which is the only option since there's no epoch to create packets inside.

**Emission in `_start_node()`** (after `start_node_func` completes):
```python
async def _start_node(self, node_name: str) -> None:
    if node_name in self._started_nodes:
        return
    func = self._get_node_start_func(node_name)
    if func is not None:
        await self._call_lifecycle_func(func, node_name)
    self._started_nodes.add(node_name)
    # NEW: emit node_started signal
    self._emit_out_of_epoch_signal(node_name, "node_started")
```

**Emission in `_stop_node()`** (after `stop_node_func` completes):
```python
async def _stop_node(self, node_name: str) -> None:
    if node_name not in self._started_nodes:
        return
    func = self._get_node_stop_func(node_name)
    if func is not None:
        await self._call_lifecycle_func(func, node_name)
    self._started_nodes.discard(node_name)
    # NEW: emit node_stopped signal
    self._emit_out_of_epoch_signal(node_name, "node_stopped")
```

**Out-of-epoch signal helper** (only needed for `node_started` and `node_stopped`):
```python
def _emit_out_of_epoch_signal(self, node_name: str, signal_type: str):
    """Emit a signal outside any epoch by creating a packet and transporting it to the edge.

    Used only for node lifecycle signals (node_started, node_stopped) which
    happen outside any epoch context. Epoch signals use _emit_epoch_signal instead.
    """
    signals = self._get_effective_signals(node_name)
    if signal_type not in signals:
        return
    signal_port = signal_port_name(signal_type)
    # Create packet outside net
    response, _ = self._netsim.do_action(netrun_sim.NetAction.create_packet(None))
    packet_id = str(response.packet_id)
    self._packet_store.register(packet_id, SignalValue(
        signal=signal_type, node_name=node_name,
        epoch_id=None, timestamp=get_timestamp_utc(), error=None,
    ))
    # Find the edge from this signal port
    edge = self._find_edge_from_port(node_name, signal_port)
    if edge:
        self._netsim.do_action(
            netrun_sim.NetAction.transport_packet_to_location(
                packet_id, netrun_sim.PacketLocation.edge(edge)
            )
        )
    else:
        # No connected edge - silently consume
        self._packet_store.consume(packet_id)
        self._netsim.do_action(netrun_sim.NetAction.consume_packet(packet_id))
```

## 8. Impact on Function Factory

The function factory (`netrun.node_factories.from_function`) should **NOT** auto-generate signal ports. Signals are a runtime/orchestration concern configured via `execution_config.signals`, not a property of the function's signature.

However, the factory's `get_node_config()` must not strip signal ports if they were explicitly added. Since signal ports are added during `resolve()` (after factory resolution), there is no conflict.

**No changes needed to the function factory.**

## 9. Helper Utilities

```python
# In netrun.net.config._base or a new _signals module

SIGNAL_PORT_PREFIX = "__signal_"
SIGNAL_PORT_SUFFIX = "__"

VALID_SIGNAL_TYPES = {"epoch_finished", "epoch_failed", "node_started", "node_stopped"}  # Extend in Phase 2

def signal_port_name(signal_type: str) -> str:
    """Convert signal type to port name. e.g., 'finished' -> '__signal_finished__'"""
    return f"{SIGNAL_PORT_PREFIX}{signal_type}{SIGNAL_PORT_SUFFIX}"

def is_signal_port(port_name: str) -> bool:
    """Check if a port name is a signal port."""
    return port_name.startswith(SIGNAL_PORT_PREFIX) and port_name.endswith(SIGNAL_PORT_SUFFIX)

def signal_type_from_port(port_name: str) -> str | None:
    """Extract signal type from port name. Returns None if not a signal port."""
    if not is_signal_port(port_name):
        return None
    return port_name[len(SIGNAL_PORT_PREFIX):-len(SIGNAL_PORT_SUFFIX)]

def generate_signal_ports(signal_types: list[str]) -> dict[str, PortConfig]:
    """Generate output port configs for the given signal types."""
    return {signal_port_name(s): PortConfig() for s in signal_types}

def generate_signal_salvo_conditions(signal_types: list[str]) -> dict[str, SalvoConditionConfig]:
    """Generate output salvo conditions for signal ports."""
    conditions = {}
    for s in signal_types:
        port_name = signal_port_name(s)
        conditions[port_name] = SalvoConditionConfig(
            max_salvos=MaxSalvosInfiniteConfig(),
            ports={port_name: PacketCountAllConfig()},
            term=SalvoConditionTermPortConfig(
                port_name=port_name,
                state=PortStateNonEmptyConfig(),
            ),
        )
    return conditions
```

## 10. Implementation Steps

### Step 1: Add SignalValue dataclass
- **File**: `pts/netrun/06_net/00_config/00_base.pct.py` (or a new `pts/netrun/06_net/02_signals.pct.py`)
- Add `SignalValue` dataclass
- Add signal port naming utilities (`signal_port_name`, `is_signal_port`, etc.)
- Add `generate_signal_ports()` and `generate_signal_salvo_conditions()` helpers
- Export and run `nbl export --reverse && nbl export`

### Step 2: Add `signals` field to NodeExecutionConfig
- **File**: `pts/netrun/06_net/00_config/01_nodes.pct.py`
- Add `signals: list[str] | VarRef | None = Field(default=None, ...)`
- Add validation that signal types are in `VALID_SIGNAL_TYPES`
- Export and run `nbl export --reverse && nbl export`

### Step 3: Add `default_signals` field to NetConfig
- **File**: `pts/netrun/06_net/00_config/03_net_config.pct.py`
- Add `default_signals: list[str] | VarRef = Field(default_factory=list, ...)`
- Export and run `nbl export --reverse && nbl export`

### Step 4: Auto-generate signal ports during resolve
- **File**: `pts/netrun/06_net/00_config/01_nodes.pct.py` (in `NodeConfig.resolve()`)
- After factory resolution, check effective signals (node-level or inherited from net-level)
- Add signal output ports to `out_ports` (if not already present)
- Add signal output salvo conditions to `out_salvo_conditions` (if not already present)
- The `resolve()` method needs access to `NetConfig.default_signals` — this is already passed via `net_config` parameter
- **File**: `pts/netrun/06_net/00_config/02_graph.pct.py` (in `GraphConfig.resolve()`)
- Pass `default_signals` through to node resolution
- Export and run `nbl export --reverse && nbl export`

### Step 5: Add signal helpers to Net class
- **File**: `pts/netrun/06_net/01_net/02_net.pct.py`
- Add `_get_effective_signals(node_name)` — resolves effective signals for a node (node-level overrides net-level)
- Add `_find_edge_from_port(node_name, port_name)` — finds the edge connected to a given output port
- Add `_emit_epoch_signal(epoch_id, node_name, signal_type, error)` — unified helper for epoch signals (creates packet inside the running epoch, loads into signal output port, sends salvo)
- Add `_emit_out_of_epoch_signal(node_name, signal_type)` — helper for node lifecycle signals only (creates packet outside net, transports to edge)

### Step 6: Add epoch signal emission (both success and failure)
- **File**: `pts/netrun/06_net/01_net/02_net.pct.py`
- Implement `_emit_epoch_signal()` — single method for both `epoch_finished` and `epoch_failed`
- **Success path**: Insert `_emit_epoch_signal(epoch_id, node_name, "epoch_finished")` in `_execute_epoch_with_retry` after `_commit_epoch_result` but before `finish_epoch`
- **Failure path**: Insert `_emit_epoch_signal(epoch_id, node_name, "epoch_failed", error=str(error))` in `_handle_epoch_failure` **before** `cancel_epoch` (the epoch is still Running, so the signal packet is created inside the epoch, sent onto the edge, and escapes before cancellation destroys remaining input packets)
- **Cache/file storage replay**: Insert in `_replay_cached_epoch` and `_replay_file_storage_epoch` before `finish_epoch`
- Export and run `nbl export --reverse && nbl export`

### Step 7: Add node lifecycle signal emission
- **File**: `pts/netrun/06_net/01_net/02_net.pct.py`
- Add `_emit_out_of_epoch_signal(node_name, "node_started")` call at end of `_start_node()`
- Add `_emit_out_of_epoch_signal(node_name, "node_stopped")` call at end of `_stop_node()`
- Export and run `nbl export --reverse && nbl export`

### Step 8: Exclude signal ports from output queue auto-generation
- **File**: `pts/netrun/06_net/00_config/03_net_config.pct.py` (in `_generate_output_queues`)
- Signal ports that are unconnected should NOT auto-generate output queues (or should generate them — TBD based on desired behavior)
- Decision: **Do NOT** auto-generate output queues for unconnected signal ports. Signal packets to unconnected ports are silently consumed.

### Step 9: Write tests
- **File**: `pts/tests/06_net/test_signals.pct.py` (new file)
- Test signal port auto-generation from config
- Test `SignalValue` dataclass
- Test signal emission on success (A finishes → B gets signal packet)
- Test signal emission on failure
- Test signals with function factory
- Test net-level `default_signals` inheritance
- Test per-node override of signals
- Test no signals by default (backward compatibility)
- Test signal with unconnected port (no crash, packet silently consumed)
- Test TOML config loading with signals

### Step 10: Update TOML schema documentation
- Update any schema exports or documentation to reflect new fields
- Ensure the netrun-ui config schema endpoint includes `signals` and `default_signals`

## 11. File Changes Summary

### New Files
| File | Description |
|------|-------------|
| `pts/netrun/06_net/02_signals.pct.py` | SignalValue, signal utilities |
| `pts/tests/06_net/test_signals.pct.py` | Signal tests |

### Modified Files
| File | Change |
|------|--------|
| `pts/netrun/06_net/00_config/01_nodes.pct.py` | Add `signals` field to `NodeExecutionConfig` |
| `pts/netrun/06_net/00_config/03_net_config.pct.py` | Add `default_signals` field to `NetConfig` |
| `pts/netrun/06_net/00_config/01_nodes.pct.py` | Signal port auto-generation in `NodeConfig.resolve()` |
| `pts/netrun/06_net/00_config/02_graph.pct.py` | Pass `default_signals` through `GraphConfig.resolve()` |
| `pts/netrun/06_net/00_config/03_net_config.pct.py` | Exclude signal ports from `_generate_output_queues` |
| `pts/netrun/06_net/01_net/02_net.pct.py` | `_emit_finished_signal`, `_emit_failure_signal`, helper methods, insertion points |

### No Changes Needed
| Component | Reason |
|-----------|--------|
| `netrun-sim` (Rust) | Signals use existing packet/port/edge mechanics |
| Function factory | Signals are a config concern, not signature-derived |
| RPC/Pool layers | No impact |
| PacketStore | Signal values are regular packet values |

## 12. Testing Strategy

### Unit Tests
- `test_signal_port_name()` — naming convention
- `test_is_signal_port()` — detection
- `test_generate_signal_ports()` — port config generation
- `test_generate_signal_salvo_conditions()` — salvo config generation
- `test_signal_value_creation()` — dataclass

### Integration Tests
- `test_signal_epoch_finished_triggers_downstream()` — A(signals=["epoch_finished"]) → edge → B receives signal
- `test_signal_epoch_failed_triggers_downstream()` — A fails → B receives failure signal
- `test_signal_node_started_triggers_downstream()` — A starts → B receives node_started signal
- `test_signal_node_stopped_triggers_downstream()` — A stops → B receives node_stopped signal
- `test_no_signals_by_default()` — backward compat, no signal ports generated
- `test_net_level_default_signals()` — all nodes inherit signals
- `test_per_node_signal_override()` — node overrides net default
- `test_signal_unconnected_port()` — no crash when signal port has no edge
- `test_signal_with_function_factory()` — factory nodes + signals work together
- `test_signal_toml_config()` — TOML loading with signals
- `test_signal_node_started_deferred_startup()` — deferred startup emits node_started on first epoch

### Manual Testing
- Create a sample project with signal-based sequencing
- Verify in netrun-ui that signal ports appear on nodes

## 13. Edge Cases and Considerations

1. **Signal ports should not appear in function factory signature parsing** — they are auto-generated, not derived from the function
2. **Cache/file storage replay should also emit epoch signals** — when a cached epoch is replayed, the `epoch_finished` signal should still fire
3. **Disabled nodes** — if a node is disabled (`enabled: false`), its epochs are not executed, so no epoch signals are emitted. Node start/stop signals are also not emitted since the node is never started.
4. **No-exec-func nodes** — nodes without exec_func that finish immediately should still emit `epoch_finished` signals if configured
5. **Subgraphs** — signal ports on subgraph-internal nodes are internal; only exposed signal ports propagate out
6. **Fan-out constraint** — each signal port connects to at most 1 downstream edge. If multiple nodes need the same signal, the user can use a broadcast node factory.
7. **Signal packet consumption** — downstream nodes consume signal packets just like regular packets (via salvo conditions)
8. **Thread safety** — signal emission happens in the Net's main async context (same as `_commit_epoch_result`), so no additional synchronization needed
9. **Deferred startup and node_started** — for nodes with `defer_startup=True`, the `node_started` signal is emitted when the node is actually started (on first epoch), not during `Net.start()`. This is correct behavior since the node hasn't truly started until then.
10. **node_stopped during error** — if `stop_node_func` raises an exception, the `node_stopped` signal should still be emitted (the node is being stopped regardless of whether cleanup succeeded). This matches the existing behavior where `_started_nodes.discard()` happens regardless.
11. **Ordering of node_started signals** — during `Net.start()`, `_start_all_nodes()` starts nodes sequentially. Each `node_started` signal is emitted and placed on its edge immediately, but downstream epochs won't trigger until `run_step()` is called. This is correct — the signals queue up and flow naturally when the net starts running.

## 14. Estimated Effort

- **Complexity**: Medium
- **Estimated time**: 3-4 focused implementation sessions
  - Session 1: Steps 1-4 (config + auto-generation)
  - Session 2: Steps 5-7 (Net emission logic)
  - Session 3: Steps 8-9 (cleanup + tests)
  - Session 4: Step 10 (docs + UI schema updates)

