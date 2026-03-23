# NodeInfo: Keep, Gut, or Remove?

**Date:** 2026-03-18

## What NodeInfo is

NodeInfo is a 370-line class with ~35 methods/properties. It holds a reference to the parent Net and a node name, pre-filling `node_name` into Net method calls. It's reconstructed on every `net.nodes` access (not cached).

Accessed via `net.nodes` which returns `dict[str, NodeInfo]`.

---

## Method-by-method classification

### Pure proxies (zero loss if removed)

These just call a Net method with the node name pre-filled:

| NodeInfo | Delegates to |
|---|---|
| `enable()` | `net.enable_node(name)` |
| `disable()` | `net.disable_node(name)` |
| `enabled` | `net.is_node_enabled(name)` |
| `inject_packets(port, values)` | `net.inject_data(name, port, values)` |
| `inject_packet(port, value)` | `net.inject_data(name, port, [value])[0]` |
| `inject(ports, plural)` | multi-port wrapper around inject_data |
| `cached_entries` | `net.get_cached_entries(name)` |
| `cached_input_salvos` | `net.get_cached_input_salvos(name)` |
| `cached_output_salvos` | `net.get_cached_output_salvos(name)` |
| `get_cached_output_for_input(...)` | `net.get_cached_output_for_input(name, ...)` |
| `cache_stats` | `net.cache_stats().get(name, ...)` |
| `is_cache_enabled` | `net._cache_store.is_cache_enabled(name)` |
| `clear_cache()` | `net.clear_node_cache(name)` |
| `clear_cached_output_for_input(...)` | `net.clear_cached_output_for_input(name, ...)` |
| `print_all_logs(...)` | `net.print_node_logs(name, ...)` |
| `print_epoch_logs(id, ...)` | reads from `net._epochs` directly |

**16 methods** that are pure delegation. These create the 3-layer duplication problem (NodeInfo → Net → internal store).

### Filtered views (trivially replaceable)

These filter Net-level collections by node name:

| NodeInfo | Equivalent without NodeInfo |
|---|---|
| `epochs` | `[e for e in net.epochs.values() if e.node_name == name]` |
| `epoch_logs` | `[l for l in net.epoch_logs.values() if l.node_name == name]` |
| `running_epochs` | filter `net._running_epochs` by node name |
| `startable_epochs` | filter `net.get_startable_epochs()` by node name |
| `epoch_count` | `len(node.epochs)` |
| `is_busy` | `len(node.running_epochs) > 0` |
| `incoming_edges` | filter `net.edges` by target node |
| `outgoing_edges` | filter `net.edges` by source node |

**8 properties** that are one-liner filters.

### Config access (available through config)

| NodeInfo | Available via |
|---|---|
| `cfg` | iterating `net.config_resolved.graph.nodes` |
| `in_ports` | `cfg.in_ports` |
| `out_ports` | `cfg.out_ports` |
| `in_port_names` | `list(cfg.in_ports.keys())` |
| `out_port_names` | `list(cfg.out_ports.keys())` |
| `execution_config` | `net._node_execution_configs.get(name)` |
| `pools` | `execution_config.pools` |

**7 properties** providing config access. Less convenient without NodeInfo but the data exists.

### Genuinely useful, harder to replace

| NodeInfo | What it does | Used where |
|---|---|---|
| `on_epoch_start(cb)` | Node-scoped callback — fires only for this node | Tests |
| `on_epoch_end(cb)` | Node-scoped callback — fires only for this node | Tests |
| `packets_at_input_port(port)` | Packet inspection using netsim directly | Tests |
| `packets_at_all_input_ports()` | All ports packet inspection | Tests |
| `packets_in_epoch(epoch_id)` | Packets inside a running epoch | Tests |

**5 methods** with unique capability. The node-scoped callbacks filter internally; without them you'd write `net.on_epoch_end(lambda n, id, log: cb(n, id, log) if n == name else None)`. The packet inspection methods use netsim's `get_packets_at_location()` — not exposed on Net.

---

## The case for removing entirely

- **370 lines eliminated.** Plus the tests that test NodeInfo itself.
- **API duplication gone.** Cache methods exist in 1 place, not 3. Log methods exist in 1 place, not 2.
- **Simpler mental model.** One object (Net) has all the methods. No question of "do I call it on net or on node?"
- **No caching trap.** `net.nodes` reconstructs NodeInfo objects on every access. Users who write `for node in net.nodes.values()` in a loop are creating objects needlessly.

**What you lose:**
- IDE discoverability of per-node operations (tab-complete on `net.nodes["X"].`)
- Clean node-scoped callbacks
- Packet-at-port inspection (could be added to Net)
- The `inject(ports, plural)` multi-port convenience

---

## The case for gutting (keep but slim)

Keep NodeInfo as a thin inspection object. Remove all proxy methods. Keep only what's genuinely node-scoped:

```python
class NodeInfo:
    # Identity
    name: str
    cfg: NodeConfig

    # Config access
    in_ports, out_ports, in_port_names, out_port_names
    execution_config, pools

    # Runtime state (filtered views)
    epochs, epoch_logs, running_epochs, startable_epochs
    epoch_count, is_busy, enabled

    # Unique capabilities
    packets_at_input_port(port_name)
    packets_at_all_input_ports()
    packets_in_epoch(epoch_id)
    on_epoch_start(callback)
    on_epoch_end(callback)

    # Edges
    incoming_edges, outgoing_edges
```

This is ~20 members instead of ~35. All cache proxy methods gone. All log proxy methods gone. All injection proxy methods gone. All enable/disable proxy methods gone.

Users who want cache operations use `net.cache.clear_node("X")` (or whatever the cache API becomes). Users who want to inject data use `net.inject_data("X", "port", values)`.

**Cuts:** 16 proxy methods removed from NodeInfo. Net-level cache/log methods become the single source of truth.

---

## The case for keeping as-is

- NodeInfo is already written and tested
- It's a convenient namespace for node operations
- The UI backend may rely on it
- Removing methods is a breaking change

---

## Recommendation

**Gut it.** The proxy methods are the source of the 3-layer duplication problem. Keeping the inspection/state/callback core preserves the genuine value (discoverability, node-scoped callbacks, packet inspection) while eliminating the bloat that makes the overall API feel redundant.

The ~16 proxy methods create the illusion of a rich per-node API, but they're just forwarding calls. Removing them makes it clear: NodeInfo is for *inspecting* a node. Net is for *operating* on the network.
