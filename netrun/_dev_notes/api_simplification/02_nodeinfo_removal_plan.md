# Plan: Remove NodeInfo, Preserve All Features

**Date:** 2026-03-18

## Approach

Remove NodeInfo and EdgeInfo entirely. Every feature they provide gets absorbed into Net (or its sub-objects) with no loss of functionality. Where NodeInfo provided a node-scoped convenience, Net methods gain an optional `node_name` parameter.

---

## Feature-by-feature migration

### 1. Node-scoped callbacks → add `node` parameter to existing methods

**Current:**
```python
net.nodes["X"].on_epoch_start(cb)   # fires only for node X
net.nodes["X"].on_epoch_end(cb)     # fires only for node X
```

**After:**
```python
net.on_epoch_start(cb, node="X")    # fires only for node X
net.on_epoch_end(cb, node="X")      # fires only for node X
```

The internal `_register_epoch_callback` already accepts `node_name` — just expose it on the public methods.

---

### 2. Packet inspection → add methods to Net

**Current (NodeInfo-only, no Net equivalent):**
```python
node.packets_at_input_port("data")      # -> list[Packet]
node.packets_at_all_input_ports()       # -> dict[str, list[Packet]]
node.packets_in_epoch(epoch_id)         # -> list[Packet]
```

**After (new Net methods):**
```python
net.get_packets_at_port(node_name, port_name)   # -> list[Packet]
net.get_packets_at_all_ports(node_name)          # -> dict[str, list[Packet]]
net.get_packets_in_epoch(epoch_id)               # -> list[Packet]
```

These are thin wrappers around netsim's `get_packets_at_location()`. ~15 lines of new code.

---

### 3. Data injection → already on Net

**Current:**
```python
node.inject_packets("port", [1, 2])     # -> net.inject_data(name, "port", [1, 2])
node.inject_packet("port", 1)           # -> net.inject_data(name, "port", [1])[0]
node.inject({"a": [1], "b": [2]}, plural=True)  # multi-port
```

**After:** Use `net.inject_data(name, port, values)` directly. The multi-port `inject()` convenience is only used in 3 test assertions — not worth preserving as its own method.

---

### 4. Enable/disable → already on Net

**Current:**
```python
node.enable()    # -> net.enable_node(name)
node.disable()   # -> net.disable_node(name)
node.enabled     # -> net.is_node_enabled(name)
```

**After:** Use `net.enable_node(name)`, `net.disable_node(name)`, `net.is_node_enabled(name)` directly. Already exist.

---

### 5. Filtered epoch views → add `node` parameter where useful

**Current:**
```python
node.epochs              # all epochs for this node
node.epoch_logs          # all EpochLogs for this node
node.running_epochs      # running epochs for this node
node.startable_epochs    # startable epochs for this node
node.epoch_count         # len of the above
node.is_busy             # any running?
```

**After:** The `epochs` property on Net already returns all epochs. Adding a `node` filter:

```python
net.epochs                    # dict of all epochs (existing)
net.get_epochs(node="X")     # filtered by node (new convenience, optional)
```

For `epoch_logs`, the property already exists and returns all. Filtering is a one-liner:
```python
[log for log in net.epoch_logs.values() if log.node_name == "X"]
```

`running_epochs`, `startable_epochs`, `is_busy` are rarely used (1 test each). Don't add new methods — users can filter the existing collections.

---

### 6. Config access → use config directly

**Current:**
```python
node.cfg                  # deep copy of NodeConfig
node.in_ports             # dict of input PortConfigs
node.out_ports            # dict of output PortConfigs
node.in_port_names        # list of input port names
node.out_port_names       # list of output port names
node.execution_config     # NodeExecutionConfig
node.pools                # list of pool names
```

**After:** Access through the config:
```python
# Users who need node config already know the node name
config = net.config  # or net._config_resolved for resolved version
node_cfg = next(n for n in config.graph.nodes if n.name == "X")
```

This is less convenient, but these properties are used in exactly 2 tests (checking execution_config and pools on 2 nodes). Not worth adding Net methods for.

If lookup-by-name becomes a common need, add a single helper:
```python
net.get_node_config(node_name) -> NodeConfig  # new convenience
```

---

### 7. Edge inspection → simplify

**Current (NodeInfo):**
```python
node.incoming_edges      # -> list[EdgeInfo]
node.outgoing_edges      # -> list[EdgeInfo]
```

**Current (Net):**
```python
net.edges                # -> list[EdgeInfo] (all edges)
net.get_edges_from_port(node, port)   # -> list (output edges from a port)
net.has_downstream_connection(node, port)  # -> bool
```

**After:** Remove EdgeInfo too. `net.edges` was only used in tests. The internal methods `get_edges_from_port` and `has_downstream_connection` are only used internally for signal emission — make them private.

If edge inspection is needed, users can access the graph:
```python
net.config.graph.edges  # list[EdgeConfig]
```

---

### 8. Cache methods → move to `net.cache` sub-object

**Current (3 layers):**
```python
net.get_cached_entries("X")          # Net method
net.nodes["X"].cached_entries        # NodeInfo proxy
net._cache_store.get_entries("X")    # internal
```

**After (1 layer):**
```python
net.cache.entries("X")
net.cache.input_salvos("X")
net.cache.output_salvos("X")
net.cache.output_for_input("X", input_values)
net.cache.stats()                    # all nodes
net.cache.stats("X")                 # single node
net.cache.is_enabled("X")
net.cache.clear()                    # all
net.cache.clear("X")                 # single node
net.cache.clear_for_input("X", input_values)
```

This replaces 10 Net methods + 8 NodeInfo methods with one sub-object.

---

### 9. Log query → move to `net.logs` sub-object

**Current:**
```python
net.get_node_logs("X")              # Net method
net.print_all_logs()                 # Net method (the one users call)
net.nodes["X"].print_all_logs()      # NodeInfo proxy
```

**After:**
```python
net.logs.for_epoch(epoch_id)         # -> list[tuple[datetime, str]]
net.logs.for_node(node_name)         # -> list[tuple[datetime, str]]
net.logs.all_chronological()         # -> list[tuple[datetime, str, str, str]]
net.logs.print_all(...)
net.logs.print_node(node_name, ...)
net.logs.print_epoch(epoch_id, ...)
```

Remove: `get_all_logs()` (zero usage), `list_epoch_log_ids()` (self-test only), `list_node_log_names()` (self-test only).

---

### 10. Structured logging access → already covered

**Current (sample 12):**
```python
net.nodes[name].epoch_logs  # list of EpochLog for that node
```

**After:**
```python
# Already exists:
[log for log in net.epoch_logs.values() if log.node_name == name]
```

Or if we want convenience:
```python
net.get_epoch_logs(node="X")  # optional node filter on the existing property
```

---

## Summary: what changes on Net's public API

### New methods (5)
| Method | Description |
|---|---|
| `get_packets_at_port(node, port)` | Packets waiting at an input port |
| `get_packets_at_all_ports(node)` | Packets at all input ports of a node |
| `get_packets_in_epoch(epoch_id)` | Packets inside a running epoch |
| `get_node_config(node_name)` | Convenience lookup for NodeConfig by name |
| `cache` property | NetCacheAPI sub-object |
| `logs` property | NetLogQuery sub-object |

### Modified methods (2)
| Method | Change |
|---|---|
| `on_epoch_start(cb, *, node=None)` | Add optional `node` parameter |
| `on_epoch_end(cb, *, node=None)` | Add optional `node` parameter |

### Removed from Net (16 methods — moved to sub-objects or deleted)
| Removed | Replacement |
|---|---|
| `get_cached_entries(name)` | `net.cache.entries(name)` |
| `get_cached_input_salvos(name)` | `net.cache.input_salvos(name)` |
| `get_cached_output_salvos(name)` | `net.cache.output_salvos(name)` |
| `get_cached_output_for_input(name, ...)` | `net.cache.output_for_input(name, ...)` |
| `cache_stats()` | `net.cache.stats()` |
| `clear_cache()` | `net.cache.clear()` |
| `clear_node_cache(name)` | `net.cache.clear(name)` |
| `clear_cache_by_version(...)` | Deleted (self-test only) |
| `clear_cached_output_for_input(name, ...)` | `net.cache.clear_for_input(name, ...)` |
| `clear_cached_inputs(name)` | Deleted (self-test only) |
| `get_all_logs()` | Deleted (zero usage) |
| `list_epoch_log_ids()` | Deleted (self-test only) |
| `list_node_log_names()` | Deleted (self-test only) |
| `get_epoch_log(id)` | `net.logs.for_epoch(id)` |
| `get_node_logs(name)` | `net.logs.for_node(name)` |
| `get_all_logs_chronological()` | `net.logs.all_chronological()` |
| `print_epoch_logs(id, ...)` | `net.logs.print_epoch(id, ...)` |
| `print_node_logs(name, ...)` | `net.logs.print_node(name, ...)` |
| `print_all_logs(...)` | `net.logs.print_all(...)` |

### Also removed from Net (from 00_bloat_analysis)
| Removed | Reason |
|---|---|
| `execute_startable_epochs()` | Redundant with `run_step(auto_start_epochs=True)` |
| `get_running_epochs()` | Self-test only |
| `has_output()` | Self-test only |
| `output_count()` | Self-test only |
| `list_output_queues()` | Self-test only |

### Privatized
| Method | Reason |
|---|---|
| `get_edges_from_port()` → `_get_edges_from_port()` | Internal only |
| `has_downstream_connection()` → `_has_downstream_connection()` | Internal only |
| `config_resolved` → `_config_resolved` | Internal only |
| `create_external_packet()` → `_create_external_packet()` | Low-level, wrapped by inject_data |
| `create_external_packets()` → `_create_external_packets()` | Same |
| `inject_packet()` → `_inject_packet()` | Same |
| `netsim` → keep as escape hatch but document as internal |
| `graph` → same |
| `pools` → same |
| `edges` → removed (use `config.graph.edges`) |

### Removed entirely
| Class | Lines | Reason |
|---|---|---|
| `NodeInfo` | ~370 | Replaced by Net methods + sub-objects |
| `EdgeInfo` | ~100 | Replaced by `config.graph.edges` |

---

## Resulting Net public API (~45 methods)

```
LIFECYCLE (9)
  __init__, from_file, start, stop, start_sync, stop_sync
  pause, resume, start_background, wait_until_done
  __aenter__, __aexit__

EXECUTION (7)
  run_until_blocked, run_step, execute_epoch, execute_node
  get_startable_epochs, is_blocked, run_to_targets

INJECTION (2)
  inject_data, request

OUTPUT (2)
  flush_output_queue, flush_all_output_queues
  get_output, try_get_output

ERRORS (4)
  dead_letter_queue, clear_dead_letter_queue
  exception_queue, propagate_exceptions

CONTROLS (4)
  send_control, enable_node, disable_node, is_node_enabled

CALLBACKS (3)
  on_epoch_start(cb, *, node=None)
  on_epoch_end(cb, *, node=None)
  on_net_actions(cb)

INSPECTION (4)
  get_node_config(node_name)
  get_packets_at_port(node_name, port_name)
  get_packets_at_all_ports(node_name)
  get_packets_in_epoch(epoch_id)

PROPERTIES (5)
  config, started, paused, epochs, epoch_logs, net_action_log

SUB-OBJECTS (2)
  cache: NetCacheAPI
  logs: NetLogQuery

REMOTE (2)
  serve_pool, request_pool_shutdown
```

---

## Impact on sample projects

Only 2 samples use `net.nodes[]`:

**08_caching (section 9: NodeInfo cache helpers):**
```python
# Before:
fetch = net.nodes["fetch_data"]
fetch.is_cache_enabled
fetch.cache_stats
fetch.cached_entries
fetch.get_cached_output_for_input(...)

# After:
net.cache.is_enabled("fetch_data")
net.cache.stats("fetch_data")
net.cache.entries("fetch_data")
net.cache.output_for_input("fetch_data", ...)
```

**12_structured_logging (section 4: NodeInfo epoch logs):**
```python
# Before:
node_logs = net.nodes[name].epoch_logs

# After:
node_logs = [log for log in net.epoch_logs.values() if log.node_name == name]
```

---

## Impact on tests

The main test file (`test_net.pct.py`) has a dedicated NodeInfo test section (~500 lines) that tests all NodeInfo features. This entire section gets replaced by tests for the new Net methods (`get_packets_at_port`, etc.) and updated calls. The cache test's NodeInfo section (~40 lines) migrates to `net.cache.*` calls.

---

## Implementation order

1. Add `node` parameter to `on_epoch_start`/`on_epoch_end` (trivial — already supported internally)
2. Add `get_packets_at_port`, `get_packets_at_all_ports`, `get_packets_in_epoch` to Net
3. Add `get_node_config` convenience method to Net
4. Create `NetCacheAPI` sub-object, move cache methods
5. Create `NetLogQuery` sub-object, move log methods
6. Remove dead methods from Net (`get_all_logs`, `list_epoch_log_ids`, etc.)
7. Privatize internal methods
8. Remove NodeInfo and EdgeInfo classes
9. Update all tests
10. Update sample projects 08 and 12
