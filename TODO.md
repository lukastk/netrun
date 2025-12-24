# TODO

This document tracks remaining tasks for the `netrun-core` library.

## High Priority

### Graph Construction & Validation

- [x] **Create `Graph` constructor**
  - Implement `Graph::new()` that takes nodes and edges
  - Removed redundant `nodes_by_name` field (nodes already keyed by name)
  - Populate `edges_by_tail` index in constructor
  - Added `edges_by_head` index for efficient lookup of edges targeting input ports

- [x] **Implement `Graph::validate()`**
  - Validate that all edges reference existing nodes and ports
  - Validate that edge sources are output ports and targets are input ports
  - Validate that `SalvoCondition.ports` only reference ports that exist on the node
  - Validate that `SalvoCondition.term` only references ports that exist on the node
  - Validate that `SalvoCondition.max_salvos == 1` for all input salvo conditions
  - Validate no duplicate edges (same source and target)
  - Note: Port name uniqueness within a node is inherently enforced by using HashMap<PortName, Port>

- [x] **Make `Graph` serializable**
  - Add `serde` dependency
  - Derive `Serialize` and `Deserialize` for all graph types
  - Graph serializes via GraphData helper (indexes are rebuilt on deserialize)
  - Note: Using ulid 1.1.0 due to compatibility issues with Rust 1.93 beta

### Net Construction

- [x] **Create `Net` constructor**
  - Implement `Net::new(graph: Graph)`
  - Initialize `_packets_by_location` with empty `IndexSet` for:
    - Every edge in the graph
    - Every input port of every node
    - Output port locations created per-epoch when epochs are created
  - Initialize `_packets`, `_epochs`, `_startable_epochs`, `_node_to_epochs` as empty
  - Removed unused `_ports` cache and lifetime parameter from Net

### Implement Missing NetActions

- [x] **Implement `cancel_epoch`**
  - Packets inside the epoch are destroyed (removed from network)
  - Collects packets from both Node location and output ports
  - Updates `_startable_epochs` if epoch was startable
  - Updates `_node_to_epochs`
  - Emits `EpochCancelled` and `PacketConsumed` events
  - Returns `CancelledEpoch(epoch, destroyed_packet_ids)`

- [x] **Implement `create_and_start_epoch`**
  - Allow external code to manually create an epoch with a specified salvo
  - Validates node exists
  - Validates all input ports in salvo exist on the node
  - Validates all packets exist and are at the correct input ports
  - Creates epoch in Running state (skips Startable)
  - Moves packets from input ports into the epoch
  - Creates output port location entries
  - Emits EpochCreated, PacketMoved, and EpochStarted events

## Medium Priority

### Testing

- [x] **Create unit tests for graph validation**
  - Test valid graphs pass validation
  - Test invalid port references are caught
  - Test invalid edge references are caught
  - Test max_salvos validation for input salvos
  - Test edge source/target port type validation
  - Test salvo condition port/term validation
  - Test complex nested salvo condition terms

- [x] **Create integration tests for Net operations**
  - Test packet creation and consumption
  - Test epoch lifecycle (create → start → finish)
  - Test `run_until_blocked` with simple linear graphs
  - Test `run_until_blocked` with port capacity limits (using node without salvo conditions)
  - Test output salvo sending (fixed `max_salvos=0` to mean "unlimited")
  - Test cancel epoch destroys packets
  - Test create_and_start_epoch validation
  - Test FIFO packet ordering (verified via PacketMoved events)
  - Note: 18 integration tests covering all Net operations

- [x] **Create test fixtures**
  - Helper functions to create common graph patterns (linear, branching, merging, diamond)
  - Helper functions for creating ports, nodes, edges, and salvo conditions
  - Self-tests to verify fixtures create valid graphs

### Code Quality

- [x] **Add `Clone` derive to `SalvoConditionTerm` and `PortState`**
  - Removed the manual `clone_salvo_condition_term` helper function in `net.rs`
  - Added Clone derive to all graph types as part of serde implementation

- [x] **Add `Clone` derive to `Port`**
  - Added Clone derive as part of serde implementation

- [ ] **Review error handling**
  - Consider using `thiserror` for better error types
  - Add more context to error messages
  - Consider whether panics should be errors instead

- [x] **Fix unused field warning**
  - `Graph.nodes_by_name` was defined but never read
  - Removed it (nodes already stored in HashMap keyed by name)

## Low Priority

### Features

- [ ] **Add `Net::get_packets_at_location()`**
  - Convenient accessor for querying packet locations

- [ ] **Add `Net::get_epoch()`**
  - Convenient accessor for querying epoch state

- [ ] **Add `Net::get_startable_epochs()`**
  - Return list of epochs ready to be started

- [ ] **Consider adding `NodeEnabled`/`NodeDisabled` events**
  - Events exist in `NetEvent` but are never emitted
  - Define semantics for node enabling/disabling

- [ ] **Add logging/tracing**
  - Consider adding `tracing` for debugging support

### Documentation

- [ ] **Add rustdoc comments to all public types and functions**

- [ ] **Add examples directory**
  - Simple linear flow example
  - Branching/merging flow example
  - Multi-epoch example

- [ ] **Consider adding architecture diagram**
  - Visual representation of packet flow
  - State machine diagram for epoch lifecycle

## Questions to Resolve

- [x] Should `OutsideNet` packets be tracked in `_packets_by_location`?
  - Yes, `OutsideNet` is initialized in `Net::new()` and packets created outside the net are tracked there
- [ ] What is the expected behavior when a graph is modified after a Net is created?
- [ ] Should there be a way to "inject" packets directly into input ports (bypassing edges)?
- [ ] How should the library handle infinite loops in the graph?
- [ ] Should there be rate limiting or cycle detection in `run_until_blocked`?
