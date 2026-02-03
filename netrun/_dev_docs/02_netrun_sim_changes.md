# Proposed netrun-sim Changes: Unconnected Output Ports

This document outlines a change to allow packets to flow through unconnected output ports.

**Decision:** Dangling edges will NOT be implemented (see analysis at bottom).

**Status:** Phase 1 (netrun-sim changes) COMPLETED. Phase 2 (netrun changes) pending.

---

## Current Behavior

When `send_output_salvo` is called and packets are in an output port that has no connected edge, netrun-sim raises `UnconnectedOutputPortError`:

```
output port '{port_name}' on node '{node_name}' is not connected to any edge
```

## Proposed Behavior

Instead of erroring, packets sent from unconnected output ports should:
1. Be moved to `PacketLocation::OutsideNet`
2. Be tracked so the caller knows which packets were "orphaned" and from which port

This enables "sink" nodes with outputs that drain to outside the network.

---

## Design

### New Event Type

Add a `PacketOrphaned` event (consistent with `PacketMoved`, `PacketConsumed`, etc.):

```rust
enum NetEvent {
    // ... existing events ...

    /// Packet was sent from an unconnected output port and moved to OutsideNet
    PacketOrphaned {
        packet_id: PacketId,
        epoch_id: EpochId,
        from_node: NodeName,
        from_port: PortName,
        salvo_condition: SalvoConditionName,
        timestamp: i64,
    },
}
```

### Updated FinishedEpoch Response

Add orphaned packet summary to `FinishedEpoch`:

```rust
struct FinishedEpoch {
    epoch: Epoch,
    /// Packets that went to OutsideNet from unconnected output ports during this epoch
    orphaned_packets: Vec<OrphanedPacketInfo>,
}

struct OrphanedPacketInfo {
    packet_id: PacketId,
    from_port: PortName,
    salvo_condition: SalvoConditionName,
}
```

### Implementation Changes

#### 1. Modify `send_output_salvo` (net.rs ~line 1261)

Current code:
```rust
// If no edge connected, return error
if let Some(edge_ref) = self.graph.get_edge_by_tail(&node.name, &port_name) {
    edge_ref.clone()
} else {
    return NetActionResponse::Error(
        NetActionError::CannotPutPacketIntoUnconnectedOutputPort { ... }
    );
}
```

New code:
```rust
if let Some(edge_ref) = self.graph.get_edge_by_tail(&node.name, &port_name) {
    // Normal flow: move packet to edge
    // ... existing code ...
} else {
    // Unconnected port: move packet to OutsideNet
    packet.location = PacketLocation::OutsideNet;
    events.push(NetEvent::PacketOrphaned {
        packet_id: packet.id,
        epoch_id,
        from_node: node.name.clone(),
        from_port: port_name.clone(),
        salvo_condition: salvo_condition_name.clone(),
        timestamp: now_utc_micros(),
    });
    orphaned_packets.push(OrphanedPacketInfo { ... });
}
```

#### 2. Track orphaned packets during epoch

Add to `Epoch` struct (or separate tracking):
```rust
struct Epoch {
    // ... existing fields ...
    orphaned_packets: Vec<OrphanedPacketInfo>,  // Accumulated during execution
}
```

#### 3. Update `finish_epoch`

Include orphaned packets in response:
```rust
NetActionResponse::FinishedEpoch {
    epoch,
    orphaned_packets: epoch.orphaned_packets.clone(),
}
```

#### 4. Update Python bindings

- Add `PacketOrphaned` to `NetEvent` enum
- Add `orphaned_packets` field to `FinishedEpoch` response class
- Add `OrphanedPacketInfo` class

---

## netrun Integration

After netrun-sim changes, update the `Net` class:

### 1. Handle orphaned packets in `_commit_epoch_result`

```python
async def _execute_epoch(self, epoch_id: str) -> NodeExecutionResult | None:
    # ... existing code ...

    # Finish the epoch
    response, events = self._netsim.do_action(
        netrun_sim.NetAction.finish_epoch(epoch_id)
    )

    # Track orphaned packets
    if response.orphaned_packets:
        for info in response.orphaned_packets:
            self._handle_orphaned_packet(epoch_id, info)
```

### 2. Add orphaned packet tracking

```python
class Net:
    def __init__(self, ...):
        # ... existing ...
        self._orphaned_packets: list[dict] = []

    def _handle_orphaned_packet(self, epoch_id: str, info) -> None:
        """Track a packet that was orphaned (sent to OutsideNet)."""
        self._orphaned_packets.append({
            "epoch_id": epoch_id,
            "packet_id": str(info.packet_id),
            "from_port": info.from_port,
            "salvo_condition": info.salvo_condition,
        })

    def get_orphaned_packets(self) -> list[dict]:
        """Get all packets that were sent to OutsideNet from unconnected ports."""
        return list(self._orphaned_packets)

    def extract_orphaned_packets(self) -> list[tuple[str, Any]]:
        """Remove orphaned packets from tracking and return with values."""
        result = []
        for info in self._orphaned_packets:
            packet_id = info["packet_id"]
            value = self._packet_store.consume(packet_id)
            result.append((packet_id, value))
        self._orphaned_packets.clear()
        return result
```

---

## Example Usage

```python
async with Net(config) as net:
    # Inject data
    net.inject_data("Source", "in", [{"id": 1}, {"id": 2}])

    # Run network (some outputs may be unconnected)
    await net.run_until_blocked()
    for epoch_id in net.get_startable_epochs():
        await net.execute_epoch(epoch_id)

    # Get results that flowed to unconnected outputs
    orphaned = net.extract_orphaned_packets()
    for packet_id, value in orphaned:
        print(f"Output: {value}")
```

---

## Implementation Plan

### Phase 1: netrun-sim changes ✅ COMPLETED

1. ✅ Add `PacketOrphaned` event type - Added to `NetEvent` enum in `net.rs`
2. ✅ Add `OrphanedPacketInfo` struct - Added in `net.rs`
3. ✅ Modify `send_output_salvo` to handle unconnected ports - Now moves packets to `OutsideNet`
4. ✅ Add orphaned tracking to `Epoch` - Added `orphaned_packets: Vec<OrphanedPacketInfo>`
5. ✅ Update `FinishedEpoch` response - Epoch now carries orphaned_packets
6. ✅ Update Python bindings - Added all new types to `python/src/net.rs`
7. ✅ Update Python type stubs - Added to `__init__.pyi`
8. ✅ Add undo support - Added `undo_packet_orphaned` function
9. ✅ Add tests - Added `test_send_output_salvo_unconnected_port`, `test_undo_send_output_salvo_with_orphaned_packets`, and `sink_graph` fixture

### Phase 2: netrun changes

1. Handle orphaned packets in `Net._execute_epoch`
2. Add orphaned packet tracking and retrieval methods
3. Update example notebook
4. Add tests

---

## Appendix: Why Not Dangling Edges?

Dangling edges (edges with missing source or target) were considered but rejected:

| Issue | Description |
|-------|-------------|
| **Weakens validation** | Typos in node/port names would be silently accepted |
| **Unclear intent** | Can't distinguish intentional dangling from mistakes |
| **Location ambiguity** | `PacketLocation::Edge(dangling)` has invalid endpoints |
| **Complex semantics** | What happens when packets reach a dangling target? |

The "unconnected output port → OutsideNet" solution covers the primary use case (draining packets to outside) without these complications.
