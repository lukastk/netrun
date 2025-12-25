# TODO

This document tracks remaining tasks for the `netrun-sim` library.

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

### API Design

- [x] **Enforce all Net mutations go through NetAction/do_action**
  - Principle: All external operations on Net must use `do_action(NetAction)` so that:
    - External code can track exactly what operations have been performed
    - All operations return the list of `NetEvent`s that transpired
  - Audited public API and removed `place_packet_at_location()` method
  - Updated all tests to use `do_action(NetAction::TransportPacketToLocation(...))` instead

- [x] **Add `TransportPacketToLocation` NetAction**
  - Allows moving an existing packet to any location:
    - Input port (with capacity check - fail if port is full)
    - Output port of an epoch
    - Any edge
    - OutsideNet
  - Restrictions:
    - Cannot move packets into a Running epoch (only Startable epochs allowed)
    - Cannot move packets out of a Running epoch (only Startable epochs allowed)
  - This replaces direct injection and provides a unified way to set up packet locations
  - Emits `PacketMoved` event
  - Added new error variants: `InputPortFull`, `CannotMovePacketFromRunningEpoch`, `CannotMovePacketIntoRunningEpoch`, `EdgeNotFound`
  - Added `Display` impl for `EdgeRef`

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

- [x] **Create public API integration tests (tests/ directory)**
  - Test Graph construction, validation, and accessors (19 tests)
  - Test Net construction and all NetActions via do_action() (26 tests)
  - Test end-to-end workflows (packet flow through multi-node graphs) (9 tests)
  - Test error handling returns correct error types
  - Test serialization/deserialization of Graph types
  - Added public accessor methods: `get_packet()`, `get_epoch()`, `get_startable_epochs()`, `packet_count_at()`, `get_packets_at_location()`

### Code Quality

- [x] **Add `Clone` derive to `SalvoConditionTerm` and `PortState`**
  - Removed the manual `clone_salvo_condition_term` helper function in `net.rs`
  - Added Clone derive to all graph types as part of serde implementation

- [x] **Add `Clone` derive to `Port`**
  - Added Clone derive as part of serde implementation

- [x] **Review error handling**
  - Added `thiserror` dependency for implementing `std::error::Error` trait
  - Restructured `NetActionError` with structured data fields (IDs, names) instead of message strings
  - Added `#[error("...")]` annotations with descriptive error messages
  - Implemented `Display` for `PortRef` to support error formatting
  - Added `thiserror::Error` derive to `GraphValidationError`
  - Note: Panics are kept for internal invariant violations (e.g., inconsistent state) as this is idiomatic Rust

- [x] **Fix unused field warning**
  - `Graph.nodes_by_name` was defined but never read
  - Removed it (nodes already stored in HashMap keyed by name)

## Low Priority

### Features

- [x] **Add `Net::get_packets_at_location()`**
  - Convenient accessor for querying packet locations
  - Implemented along with `packet_count_at()`

- [x] **Add `Net::get_epoch()`**
  - Convenient accessor for querying epoch state

- [x] **Add `Net::get_startable_epochs()`**
  - Return list of epochs ready to be started

- [x] **Add `Net::get_packet()`**
  - Look up a packet by ID

- [x] **Consider adding `NodeEnabled`/`NodeDisabled` events**
  - Decided to remove them - they were dead code
  - External code can implement enable/disable logic by controlling when to call StartEpoch

- [x] **Add logging/tracing**
  - Decided: Not needed currently
  - NetEvent already provides complete observability for external consumers
  - Can be added later if internal debugging becomes an issue

### Documentation

- [x] **Add rustdoc comments to all public types and functions**
  - Added module-level docs to graph.rs and net.rs
  - Documented all public types, enums, and their variants
  - Added doc examples for Graph and Net::do_action

- [x] **Add examples directory**
  - `examples/linear_flow.rs` - Simple A -> B -> C flow demonstrating epoch lifecycle
  - `examples/diamond_flow.rs` - Branching/merging flow demonstrating synchronization
  - Fixed bug: output port locations weren't initialized when epochs created via run_until_blocked

## Outstanding Issues

### Testing

- [x] **Integration tests not running**
  - The `tests/` directory at the workspace root contained ~54 integration tests (graph_api.rs, net_api.rs, workflow.rs)
  - These were not being executed because the workspace root had no package that included them
  - Fixed: Moved `tests/` into `core/tests/` - now all 89 tests run (33 unit + 54 integration + 2 doc tests)

### Python Bindings

- [x] **`SalvoConditionNotMet` mapped to wrong exception**
  - In `python/src/errors.rs`, `NetActionError::SalvoConditionNotMet` was mapped to `SalvoConditionNotFoundError`
  - This conflated "condition not found" with "condition not met" - different error semantics
  - Fixed: Added dedicated `SalvoConditionNotMetError` exception class

## Questions to Resolve

- [x] Should `OutsideNet` packets be tracked in `_packets_by_location`?
  - Yes, `OutsideNet` is initialized in `Net::new()` and packets created outside the net are tracked there
- [x] What is the expected behavior when a graph is modified after a Net is created?
  - Graph is immutable by design (no mutation methods). If a different topology is needed, create a new Net.
- [x] Should there be a way to "inject" packets directly into input ports (bypassing edges)?
  - Yes, via the new `TransportPacketToLocation` NetAction (see High Priority > API Design)
- [x] How should the library handle infinite loops in the graph?
  - Not an issue. `run_until_blocked` only moves packets Edge→InputPort and triggers salvo conditions.
  - Completing a cycle requires external actions (StartEpoch, LoadPacketIntoOutputPort, SendOutputSalvo).
  - Therefore `run_until_blocked` is bounded by the number of packets on edges and is guaranteed to terminate.
- [x] Should there be rate limiting or cycle detection in `run_until_blocked`?
  - No, not needed. See above - the function always terminates.
