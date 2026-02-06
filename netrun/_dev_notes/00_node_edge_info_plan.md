# Plan: NodeInfo and EdgeInfo for Net.nodes and Net.edges

**Status: COMPLETED**

## Overview

Add `Net.nodes` and `Net.edges` properties that return dictionaries of `NodeInfo` and `EdgeInfo` objects. These provide convenient access to node/edge configuration and runtime state with helpful methods for inspection and manipulation.

## Design

### Net.nodes Property

```python
@property
def nodes(self) -> dict[str, NodeInfo]:
    """Return a dict of node_name -> NodeInfo for all nodes."""
```

### Net.edges Property

```python
@property
def edges(self) -> list[EdgeInfo]:
    """Return a list of EdgeInfo for all edges."""
```

---

## NodeInfo Class

### Core Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | `str` | Node name |
| `cfg` | `NodeConfig` | Copy of the node configuration |

### Port Information

| Property/Method | Type | Description |
|-----------------|------|-------------|
| `in_ports` | `dict[str, PortConfig]` | Input port configurations |
| `out_ports` | `dict[str, PortConfig]` | Output port configurations |
| `in_port_names` | `list[str]` | List of input port names |
| `out_port_names` | `list[str]` | List of output port names |

### Epoch Information

| Property/Method | Type | Description |
|-----------------|------|-------------|
| `epochs` | `list[Epoch]` | All epochs (running + startable) for this node |
| `running_epochs` | `list[Epoch]` | Epochs currently running |
| `startable_epochs` | `list[Epoch]` | Epochs waiting to be started |
| `epoch_count` | `int` | Total number of epochs |
| `is_busy` | `bool` | True if any epochs are running |

### Packet Information at Input Ports

| Method | Returns | Description |
|--------|---------|-------------|
| `packets_at_input_port(port_name)` | `list[Packet]` | Packets waiting at specific input port |
| `packets_at_all_input_ports()` | `dict[str, list[Packet]]` | All packets at all input ports |

### Packet Information Inside Epochs

| Method | Returns | Description |
|--------|---------|-------------|
| `packets_in_epoch(epoch_id)` | `list[Packet]` | Packets inside a specific epoch |

### Packet Injection (Mutating)

| Method | Returns | Description |
|--------|---------|-------------|
| `inject_packets(port_name, values)` | `list[str]` | Inject packets at input port, returns packet_ids |

### Edge Information

| Property | Type | Description |
|----------|------|-------------|
| `incoming_edges` | `list[EdgeInfo]` | Edges targeting this node's input ports |
| `outgoing_edges` | `list[EdgeInfo]` | Edges from this node's output ports |

### Execution Configuration

| Property | Type | Description |
|----------|------|-------------|
| `execution_config` | `NodeExecutionConfig | None` | Execution settings |
| `pools` | `list[str]` | Pool names this node can execute on |

---

## EdgeInfo Class

### Core Properties

| Property | Type | Description |
|----------|------|-------------|
| `cfg` | `EdgeConfig` | Copy of the edge configuration |
| `source_node` | `str` | Source node name |
| `source_port` | `str` | Source port name |
| `target_node` | `str` | Target node name |
| `target_port` | `str` | Target port name |

### Shorthand

| Property | Type | Description |
|----------|------|-------------|
| `source` | `tuple[str, str]` | (node_name, port_name) tuple |
| `target` | `tuple[str, str]` | (node_name, port_name) tuple |

### Packet Information

| Property/Method | Type | Description |
|-----------------|------|-------------|
| `packets_in_transit` | `list[Packet]` | Packets currently on this edge |
| `packet_count` | `int` | Number of packets in transit |
| `has_packets` | `bool` | True if any packets in transit |

### Related Nodes

| Method | Returns | Description |
|--------|---------|-------------|
| `source_node_info` | `NodeInfo` | NodeInfo for the source node |
| `target_node_info` | `NodeInfo` | NodeInfo for the target node |

---

## Implementation Details

### Internal References

Both `NodeInfo` and `EdgeInfo` will hold internal references to:
- `_net: Net` - Reference to parent Net (for accessing netsim, packet_store, etc.)
- The actual config object

### Lazy vs Eager

- Properties that query runtime state (epochs, packets) should be computed on access (lazy)
- This ensures they reflect current state, not stale snapshots
- Configuration copies should be made once on NodeInfo/EdgeInfo creation

### Thread Safety

- NodeInfo/EdgeInfo are not thread-safe
- They should only be used from the same context that owns the Net
- This matches the existing Net design

---

## Usage Examples

```python
net = Net(config)
await net.start()

# Access node info
node = net.nodes["my_node"]
print(f"Node: {node.name}")
print(f"Input ports: {node.in_port_names}")
print(f"Running epochs: {len(node.running_epochs)}")

# Check packets waiting
if node.has_input_packets("in"):
    packets = node.packets_at_input_port("in")
    print(f"Waiting packets: {len(packets)}")

# Inject test data
packet_id = node.inject_packet("in", {"test": "data"})

# Access edge info
for edge in net.edges:
    print(f"{edge.source_node}.{edge.source_port} -> {edge.target_node}.{edge.target_port}")
    if edge.has_packets:
        print(f"  {edge.packet_count} packets in transit")

# Get edges for a specific node
incoming = net.nodes["sink"].incoming_edges
outgoing = net.nodes["source"].outgoing_edges
```

---

## Files to Modify

| File | Changes |
|------|---------|
| `netrun/pts/netrun/05_net/01_net.pct.py` | Add NodeInfo, EdgeInfo classes and Net.nodes, Net.edges properties |

After editing:
```bash
cd netrun && nbl export --reverse && nbl export
```

---

## Testing

Add tests to `netrun/pts/tests/05_net/test_net.pct.py`:

1. Test `NodeInfo` properties (name, cfg, ports)
2. Test epoch-related helpers (running_epochs, startable_epochs)
3. Test packet inspection (packets_at_input_port, etc.)
4. Test packet injection (inject_packet, inject_packets)
5. Test `EdgeInfo` properties
6. Test edge packet inspection
7. Test integration with running Net
