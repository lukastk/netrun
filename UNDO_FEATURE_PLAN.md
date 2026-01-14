# Undo NetAction Feature Plan

## Executive Summary

This document outlines the design and implementation plan for adding undo functionality to the `netrun-sim` library. The feature will allow users to reverse NetActions by passing undo information back to the library.

**Key Design Decisions:**
1. Replace `RunNetUntilBlocked` with `RunStep` (single-step execution)
2. `NetSim` does **not** maintain any history
3. `undo_action()` takes undo data returned from the original action

---

## Part 1: Replace `RunNetUntilBlocked` with `RunStep`

### Rationale

`RunNetUntilBlocked` is problematic for undo because:
1. It runs an unbounded loop, producing arbitrarily many events
2. The internal state changes are compound and hard to reverse atomically
3. Users lose fine-grained control over execution

`RunStep` provides:
1. Single-step execution (one "phase" of work)
2. Clear return value indicating whether progress was made
3. Easy undo (reverse a single step's events)

### New Action Design

```rust
/// An action that can be performed on the network.
#[derive(Debug, Clone)]
pub enum NetAction {
    /// Execute one step of automatic packet flow.
    /// Returns whether any progress was made (i.e., not blocked).
    RunStep,

    // ... all other actions unchanged ...
    CreatePacket(Option<EpochID>),
    ConsumePacket(PacketID),
    DestroyPacket(PacketID),
    StartEpoch(EpochID),
    FinishEpoch(EpochID),
    CancelEpoch(EpochID),
    CreateEpoch(NodeName, Salvo),
    LoadPacketIntoOutputPort(PacketID, PortName),
    SendOutputSalvo(EpochID, SalvoConditionName),
    TransportPacketToLocation(PacketID, PacketLocation),
}
```

### RunStep Semantics

A single step performs **one full iteration** of the flow loop:

1. **Phase 1: Move all movable packets from edges to input ports**
   - For each edge with packets, if the destination input port has capacity, move the first packet (FIFO)
   - Multiple packets can move in this phase (one per edge)

2. **Phase 2: Trigger all satisfied input salvo conditions**
   - For each node with packets at input ports, check salvo conditions
   - First satisfied condition per node triggers, creating an epoch
   - Multiple epochs can be created in this phase (one per node)

If neither phase makes progress (no packets moved, no salvos triggered), the network is "blocked" and `RunStep` returns indicating no progress.

**Rationale:** This matches one iteration of the current `run_until_blocked` loop. Finer granularity (single packet per step) would be inefficient due to action call overhead. Undo still works because all events are recorded and can be reversed in order.

```rust
/// Data returned by a successful network action.
#[derive(Debug, Clone)]
pub enum NetActionResponseData {
    /// Result of RunStep: whether progress was made
    StepResult { made_progress: bool },

    // ... existing variants ...
    Packet(PacketID),
    CreatedEpoch(Epoch),
    StartedEpoch(Epoch),
    FinishedEpoch(Epoch),
    CancelledEpoch(Epoch, Vec<PacketID>),
    None,
}
```

### Helper Method

```rust
impl NetSim {
    /// Check if the network is blocked (no progress can be made).
    ///
    /// Returns true if:
    /// - No packets can move from edges to input ports (all destinations full or no packets on edges)
    /// - No input salvo conditions can be triggered
    pub fn is_blocked(&self) -> bool;
}
```

### Migration Path

Users currently calling `RunNetUntilBlocked` can migrate to:

```rust
// Old code
net.do_action(&NetAction::RunNetUntilBlocked);

// New code - equivalent behavior
while !net.is_blocked() {
    net.do_action(&NetAction::RunStep);
}

// Or with response checking (doesn't need is_blocked)
loop {
    match net.do_action(&NetAction::RunStep) {
        NetActionResponse::Success(NetActionResponseData::StepResult { made_progress }, _) => {
            if !made_progress { break; }
        }
        _ => break,
    }
}
```

Since `RunStep` does one full loop iteration, the migration is straightforward - just loop until blocked.

---

## Part 2: Undo Design Without History

### Core Principle

The `NetSim` maintains **no history**. Instead:
1. `do_action()` returns all information needed for undo
2. `undo_action()` takes that information to reverse the action
3. The caller is responsible for storing undo data if needed

### Analysis: Is `NetAction` + `Vec<NetEvent>` Sufficient?

**Question:** Can we undo using just the original `NetAction` and the `Vec<NetEvent>` that were produced?

**Answer:** Almost, but **events need enhancement** for some actions.

#### Actions Where Events ARE Sufficient

| Action | Why Events Work |
|--------|-----------------|
| `CreatePacket` | `PacketCreated(time, id)` has packet ID to remove |
| `StartEpoch` | `EpochStarted(time, id)` - just revert state to Startable |
| `CreateEpoch` | `EpochCreated` + `PacketMoved(from, to)` - reverse moves, remove epoch |
| `LoadPacketIntoOutputPort` | `PacketMoved(from, to)` - reverse the move |
| `SendOutputSalvo` | `PacketMoved(from, to)` per packet - reverse moves, pop out_salvos |
| `TransportPacketToLocation` | `PacketMoved(from, to)` - reverse the move |
| `RunStep` | Same events as above - all reversible |

#### Actions Where Events Are NOT Sufficient

| Action | What's Missing | Solution |
|--------|---------------|----------|
| `ConsumePacket` | `PacketConsumed(time, id)` lacks **previous location** | Enhance event |
| `DestroyPacket` | `PacketDestroyed(time, id)` lacks **previous location** | Enhance event |
| `FinishEpoch` | Epoch is **removed** from `_epochs` - need full epoch data | Use response data |
| `CancelEpoch` | Epoch removed + packets destroyed - need packet locations | Enhance events + use response |

### Solution: Enhanced Events + Response Data

#### Option A: Enhance Events Only (Recommended)

Modify events to include all information needed for perfect undo:

```rust
pub enum NetEvent {
    // Unchanged
    PacketCreated(EventUTC, PacketID),
    EpochCreated(EventUTC, EpochID),
    EpochStarted(EventUTC, EpochID),
    InputSalvoTriggered(EventUTC, EpochID, SalvoConditionName),
    OutputSalvoTriggered(EventUTC, EpochID, SalvoConditionName),

    // Enhanced with location for undo
    PacketConsumed(EventUTC, PacketID, PacketLocation),  // +location
    PacketDestroyed(EventUTC, PacketID, PacketLocation), // +location

    // Enhanced with full Epoch for undo
    EpochFinished(EventUTC, Epoch),   // EpochID → Epoch
    EpochCancelled(EventUTC, Epoch),  // EpochID → Epoch

    // Enhanced with source index for perfect IndexSet restoration
    PacketMoved(EventUTC, PacketID, PacketLocation, PacketLocation, usize),  // +from_index
}
```

With these changes, `undo_action(action: &NetAction, events: &[NetEvent])` has all needed information for **perfect state restoration**.

**Pros:**
- Single source of truth (events contain everything)
- Clean API: `undo_action(&action, &events)`
- Events become more informative for debugging/logging

**Cons:**
- Events become larger (Epoch can be significant)
- Breaking change to event format

#### Option B: Separate Undo Data

Keep events minimal, return separate undo data:

```rust
/// Data needed to undo an action (returned alongside events)
#[derive(Debug, Clone)]
pub enum NetActionUndoData {
    /// No special data needed (can undo from events alone)
    None,
    /// Location of consumed/destroyed packet
    PacketLocation(PacketLocation),
    /// Epoch that was finished/cancelled
    Epoch(Epoch),
    /// Epoch + packet locations for CancelEpoch
    CancelledEpoch { epoch: Epoch, packet_locations: Vec<(PacketID, PacketLocation)> },
}

pub enum NetActionResponse {
    Success(NetActionResponseData, Vec<NetEvent>, NetActionUndoData),  // Added undo data
    Error(NetActionError),
}
```

**Pros:**
- Events stay minimal
- Clear separation of concerns

**Cons:**
- Redundant data structures
- More complex API

### Recommendation: Option A (Enhanced Events)

Events are already the "record of what happened" - they should contain enough information to understand (and reverse) what happened. Enhanced events are more useful for debugging, logging, and replay regardless of undo.

### Undo Function Signature

```rust
impl NetSim {
    /// Undo a previously executed action.
    ///
    /// Takes the original action and the events it produced.
    /// Returns Ok(()) on success, or an error if undo is not possible.
    ///
    /// # Restrictions
    /// - Actions must be undone in reverse order (LIFO)
    /// - Some state may have changed since the action (undo may fail)
    ///
    /// # Example
    /// ```
    /// let response = net.do_action(&NetAction::CreatePacket(None));
    /// if let NetActionResponse::Success(_, events) = response {
    ///     // Later, to undo:
    ///     net.undo_action(&NetAction::CreatePacket(None), &events)?;
    /// }
    /// ```
    pub fn undo_action(&mut self, action: &NetAction, events: &[NetEvent]) -> Result<(), UndoError>;
}

#[derive(Debug, Clone)]
pub enum UndoError {
    /// Cannot undo because state has changed
    StateChanged(String),
    /// Cannot undo this action type
    NotUndoable(String),
    /// Internal error during undo
    InternalError(String),
}
```

---

## Part 3: Undo Implementation Per Action

### RunStep

**Forward:** One iteration of flow loop - move all movable packets, trigger all satisfied salvos
**Events:** Multiple `PacketMoved` (phase 1) + multiple (`InputSalvoTriggered` + `EpochCreated` + `PacketMoved`) sequences (phase 2)

**Undo:**
1. Process events in **reverse** order
2. For each `PacketMoved(_, id, from, to, from_index)`: Remove packet from `to`, insert back into `from` at `from_index` using `shift_insert`
3. For each `EpochCreated(_, epoch_id)`: Remove epoch from `_epochs`, `_startable_epochs`, `_node_to_epochs`, and remove location entries
4. Ignore `InputSalvoTriggered` (informational only)

Note: Multiple events are reversed in LIFO order, and `from_index` ensures perfect IndexSet ordering restoration.

### CreatePacket

**Forward:** Create packet at location
**Events:** `PacketCreated(_, packet_id)`

**Undo:** Remove packet by ID

### ConsumePacket / DestroyPacket

**Forward:** Remove packet from network
**Events (enhanced):** `PacketConsumed(_, packet_id, location)` or `PacketDestroyed(_, packet_id, location)`

**Undo:** Recreate packet at the stored location

### StartEpoch

**Forward:** Change epoch state Startable → Running, remove from `_startable_epochs`
**Events:** `EpochStarted(_, epoch_id)`

**Undo:** Change state back to Startable, add to `_startable_epochs`

### FinishEpoch

**Forward:** Remove epoch from `_epochs`, clean up location entries
**Events (enhanced):** `EpochFinished(_, epoch)` (full Epoch struct)

**Undo:**
1. Restore epoch to `_epochs` with state = Running
2. Recreate location entries (`Node(epoch_id)` and `OutputPort(epoch_id, port)` for each out port)

### CancelEpoch

**Forward:** Destroy all packets in epoch, remove epoch
**Events (enhanced):** `PacketDestroyed(_, packet_id, location)` per packet, then `EpochCancelled(_, epoch)`

**Undo:**
1. Restore epoch to `_epochs` (state from the Epoch in event)
2. Recreate location entries
3. Recreate each packet at its stored location

### CreateEpoch

**Forward:** Create epoch, move packets from input ports into epoch
**Events:** `EpochCreated(_, epoch_id)` + `PacketMoved(_, packet_id, from, to, from_index)` per packet

**Undo:**
1. Move packets back using `shift_insert` at `from_index` (reverse each `PacketMoved`)
2. Remove epoch from all indices
3. Remove location entries for the epoch

### LoadPacketIntoOutputPort

**Forward:** Move packet from Node location to OutputPort location
**Events:** `PacketMoved(_, packet_id, from, to, from_index)`

**Undo:** Remove from `to`, insert back into `from` at `from_index`

### SendOutputSalvo

**Forward:** Move packets to edges, add salvo to `epoch.out_salvos`
**Events:** `PacketMoved(_, packet_id, from, to, from_index)` per packet, `OutputSalvoTriggered(_, epoch_id, condition)`

**Undo:**
1. Move packets back using `shift_insert` at `from_index` (reverse each `PacketMoved`)
2. Pop last entry from `epoch.out_salvos`

### TransportPacketToLocation

**Forward:** Move packet to new location
**Events:** `PacketMoved(_, packet_id, from, to, from_index)`

**Undo:** Remove from `to`, insert back into `from` at `from_index`

---

## Part 4: Implementation Plan

### Phase 1: Replace RunNetUntilBlocked with RunStep

1. Add `RunStep` variant to `NetAction`
2. Implement `run_step()` method (single iteration of current loop)
3. Add `is_blocked()` helper method
4. Update `NetActionResponseData` to include `StepResult { made_progress: bool }`
5. Deprecate/remove `RunNetUntilBlocked`
6. Update tests

### Phase 2: Enhance Events

1. Modify `PacketConsumed` to include location: `PacketConsumed(EventUTC, PacketID, PacketLocation)`
2. Modify `PacketDestroyed` to include location: `PacketDestroyed(EventUTC, PacketID, PacketLocation)`
3. Modify `EpochFinished` to include epoch: `EpochFinished(EventUTC, Epoch)`
4. Modify `EpochCancelled` to include epoch: `EpochCancelled(EventUTC, Epoch)`
5. Modify `PacketMoved` to include source index: `PacketMoved(EventUTC, PacketID, PacketLocation, PacketLocation, usize)`
6. Update all code that produces these events (capture index before removal)
7. Update all code that consumes/matches on these events
8. Update tests

### Phase 3: Implement Undo

1. Add `UndoError` enum
2. Implement `undo_action()` method
3. Implement undo logic for each action type:
   - Simple: `CreatePacket`, `StartEpoch`, `LoadPacketIntoOutputPort`, `TransportPacketToLocation`
   - Medium: `ConsumePacket`, `DestroyPacket`, `CreateEpoch`, `SendOutputSalvo`
   - Complex: `FinishEpoch`, `CancelEpoch`, `RunStep`
4. Add comprehensive tests

### Phase 4: Python Bindings

1. Expose `RunStep` action
2. Expose `is_blocked()` method
3. Expose `undo_action()` method
4. Update type stubs
5. Add Python tests

---

## Part 5: Edge Cases and Considerations

### IndexSet Ordering

The `_packets_by_location` uses `IndexSet` which maintains insertion order (FIFO). This ordering matters for salvo condition evaluation and packet flow.

**Solution:** Store the source index in `PacketMoved` events. On undo, use `IndexSet::shift_insert` to restore the packet to its exact original position.

```rust
// Enhanced PacketMoved event
PacketMoved(EventUTC, PacketID, PacketLocation, PacketLocation, usize)
//          time      packet_id from            to              from_index

// Undo implementation
fn undo_packet_move(&mut self, packet_id: PacketID, from: &PacketLocation, to: &PacketLocation, from_index: usize) {
    // Remove from destination
    self._packets_by_location
        .get_mut(to)
        .unwrap()
        .shift_remove(&packet_id);

    // Insert back into source at original index
    self._packets_by_location
        .get_mut(from)
        .unwrap()
        .shift_insert(from_index, packet_id);

    // Update packet's location field
    self._packets.get_mut(&packet_id).unwrap().location = from.clone();
}
```

This guarantees **perfect state restoration** - packet ordering is preserved exactly.

### Undo Validation

Undo can fail if state has changed since the original action:

```rust
// Create packet P1
let resp1 = net.do_action(&NetAction::CreatePacket(None));

// Consume packet P1
net.do_action(&NetAction::ConsumePacket(p1_id));

// Try to undo creation - FAILS because P1 no longer exists
net.undo_action(&NetAction::CreatePacket(None), &events1); // Error: packet not found
```

**Behavior:** `undo_action` validates preconditions and returns `UndoError::StateChanged` if undo is not possible.

### Undo Does Not Produce Events

Undo operations do **not** emit events. Rationale:
- Undo is a meta-operation restoring previous state
- Emitting events would create confusing audit trails (e.g., `PacketCreated` during undo of `ConsumePacket`)
- If event logging is needed for undo, the caller can track it externally

### Thread Safety

`NetSim` is not thread-safe. Undo assumes single-threaded access. The caller is responsible for synchronization if needed.

---

## Part 6: API Examples

### Basic Undo Flow

```rust
let mut net = NetSim::new(graph);

// Perform action
let action = NetAction::CreatePacket(None);
let response = net.do_action(&action);
let (packet_id, events) = match response {
    NetActionResponse::Success(NetActionResponseData::Packet(id), events) => (id, events),
    _ => panic!("unexpected"),
};

// ... later, undo the action ...
net.undo_action(&action, &events)?;
// Packet no longer exists
assert!(net.get_packet(&packet_id).is_none());
```

### RunStep Loop with Undo Stack

```rust
let mut net = NetSim::new(graph);
let mut undo_stack: Vec<(NetAction, Vec<NetEvent>)> = Vec::new();

// Run network, collecting undo info for each step
while !net.is_blocked() {
    let action = NetAction::RunStep;
    if let NetActionResponse::Success(NetActionResponseData::StepResult { made_progress: true }, events) =
        net.do_action(&action)
    {
        // Each step may produce many events (multiple packet moves + epoch creations)
        undo_stack.push((action, events));
    }
}

// Undo last 3 steps (each step may have moved multiple packets / created multiple epochs)
for _ in 0..3 {
    if let Some((action, events)) = undo_stack.pop() {
        net.undo_action(&action, &events)?;
    }
}
```

### Python Usage

```python
from netrun_sim import NetSim, NetAction, NetActionResponse

net = NetSim(graph)

# Check if blocked
if not net.is_blocked():
    response = net.do_action(NetAction.RunStep())

# Undo
action = NetAction.CreatePacket(None)
response = net.do_action(action)
if response.is_success():
    events = response.events
    net.undo_action(action, events)
```

---

## Summary

| Aspect | Decision |
|--------|----------|
| Replace `RunNetUntilBlocked` | Yes - new `RunStep` action |
| History in NetSim | No - caller manages undo data |
| Undo data format | Enhanced events (location, epoch, from_index) |
| Undo signature | `undo_action(&action, &events) -> Result<(), UndoError>` |
| IndexSet ordering | Perfect restoration via `shift_insert` with stored `from_index` |
| Undo events | None emitted |

**Event Enhancements:**
- `PacketConsumed`: +location
- `PacketDestroyed`: +location
- `EpochFinished`: EpochID → Epoch
- `EpochCancelled`: EpochID → Epoch
- `PacketMoved`: +from_index

**Implementation Order:**
1. RunStep + is_blocked()
2. Enhance events
3. Implement undo_action
4. Python bindings

**Estimated Scope:** ~600-800 lines of new Rust code, ~150 lines of Python bindings.

---

## Implementation Status: COMPLETED ✓

The undo feature has been fully implemented on branch `feat/undo-action`.

### Completed Items:

1. **Phase 1: RunStep + is_blocked()** ✓
   - Replaced `RunNetUntilBlocked` with `RunStep`
   - Implemented `run_step()` method
   - Added `is_blocked()` helper method
   - Added `run_until_blocked()` convenience method for easy migration
   - Updated `NetActionResponseData` with `StepResult { made_progress: bool }`

2. **Phase 2: Enhanced Events** ✓
   - `PacketConsumed(EventUTC, PacketID, PacketLocation)` - added location
   - `PacketDestroyed(EventUTC, PacketID, PacketLocation)` - added location
   - `EpochFinished(EventUTC, Epoch)` - changed from EpochID to full Epoch
   - `EpochCancelled(EventUTC, Epoch)` - changed from EpochID to full Epoch
   - `PacketMoved(EventUTC, PacketID, PacketLocation, PacketLocation, usize)` - added from_index
   - `SendOutputSalvo` now emits `OutputSalvoTriggered` event

3. **Phase 3: undo_action() Implementation** ✓
   - Added `UndoError` enum with variants: `StateMismatch`, `NotFound`, `NotUndoable`, `InternalError`
   - Implemented `undo_action(&mut self, action: &NetAction, events: &[NetEvent]) -> Result<(), UndoError>`
   - Undo handlers for all event types:
     - `undo_packet_created` - removes packet
     - `undo_packet_consumed` / `undo_packet_destroyed` - recreates packet at stored location
     - `undo_epoch_created` - removes epoch from all indices
     - `undo_epoch_started` - reverts state to Startable
     - `undo_epoch_finished` / `undo_epoch_cancelled` - restores epoch from event
     - `undo_packet_moved` - uses `shift_insert` for perfect ordering restoration
     - `undo_output_salvo_triggered` - pops from `out_salvos`

4. **Phase 4: Tests** ✓
   - `test_undo_create_packet`
   - `test_undo_consume_packet`
   - `test_undo_start_epoch`
   - `test_undo_finish_epoch`
   - `test_undo_transport_packet_to_location`
   - `test_undo_run_step`
   - `test_undo_preserves_packet_ordering`
   - `test_undo_cancel_epoch`
   - `test_undo_send_output_salvo`
   - `test_undo_create_epoch`

5. **Phase 5: Python Bindings** ✓
   - Updated `NetAction` with `run_step()` (replaces `run_net_until_blocked()`)
   - Added `is_blocked()` method to `NetSim`
   - Added `run_until_blocked()` method to `NetSim`
   - Added `undo_action()` method to `NetSim`
   - Updated `NetEvent` with new fields (location, epoch, from_index)
   - Added `UndoError` exception hierarchy
   - Updated `NetActionResponseData` with `StepResult`

### Test Results:
- 44 unit tests ✓
- 21 graph_api tests ✓
- 26 net_api tests ✓
- 9 workflow tests ✓
- Python bindings compile successfully ✓

### Files Modified:
- `netrun-sim/core/src/net.rs` - main implementation
- `netrun-sim/core/src/net_tests.rs` - undo tests
- `netrun-sim/core/tests/workflow.rs` - updated for new API
- `netrun-sim/core/tests/net_api.rs` - updated for new API
- `netrun-sim/core/examples/*.rs` - updated for new API
- `netrun-sim/python/src/net.rs` - Python bindings
- `netrun-sim/python/src/errors.rs` - UndoError exceptions
