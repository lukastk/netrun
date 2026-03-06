# Packet Requests & Dependency Edges

## 1. Overview

**Goal**: Enable pull-based / demand-driven execution in netrun, where upstream computation is triggered automatically via dependency edges and packet requests. This gives netrun a dependency-graph execution model (similar to Apache Hamilton) alongside its existing push-based data-flow model.

**Motivation**: In a pure push-based data-flow system, all source nodes must be started externally and data flows forward. There is no mechanism to say "I need this node's output — go compute it." Packet requests add this pull-based capability, enabling:
- Lazy evaluation: only compute what is actually needed
- Dependency-driven execution: request a terminal output and let the system resolve the upstream computation graph
- Hamilton-style DAG execution within a flow-based runtime
- Hybrid push-pull: some inputs arrive via push, the rest are pulled via dependency edges

**Key Insight**: A "reverse data-flow" is the dual of the forward data-flow — it is a dependency graph. Packet requests traverse the same edges as regular packets, but in the opposite direction, to discover and activate the upstream computation needed to satisfy a downstream demand.

## 2. Dependency Edges

A **dependency edge** is a regular edge tagged with a `dependency: true` flag. It behaves identically to a regular edge for forward data flow (packets travel from output port to input port as normal). The difference is that dependency edges participate in the **automatic packet request** mechanism.

### Forward vs Backward Behavior

```
Forward (data):    A.out_port ==[packet]==> C.in_port   (same as regular edge)
Backward (request): A.out_port <==[request]== C.in_port   (dependency edges only)
```

A dependency edge is the bridge between push and pull: regular edges carry push data, dependency edges additionally enable pull requests.

### Graph Model

The `Edge` struct remains unchanged — it is a pure identity type representing a connection between two ports (`source` and `target`). Edge equality and hashing are based solely on these fields.

The dependency flag is stored as a **graph-level annotation**, not on the Edge itself. The `Graph` struct gains a `dependency_edges: HashSet<Edge>` field that tracks which edges are dependency edges (a subset of the full edge set).

```
Edge {
    source: PortRef,    // output port (unchanged)
    target: PortRef,    // input port (unchanged)
}

Graph {
    nodes: HashMap<NodeName, Node>,
    edges: HashSet<Edge>,
    edges_by_tail: HashMap<PortRef, Edge>,
    edges_by_head: HashMap<PortRef, Vec<Edge>>,
    dependency_edges: HashSet<Edge>,   // NEW: subset of edges
}
```

Helper: `graph.is_dependency_edge(&edge) -> bool`

**Rationale**: If `dependency` were on Edge and included in Hash/Eq, it would allow two edges between the same ports (one regular, one dependency), which is nonsensical. Keeping it as a graph-level annotation preserves Edge as a pure identity type.

A node may have a mix of regular and dependency edges on its input ports. This is the key hybrid pattern: some data is pushed (regular edges), the rest is pulled (dependency edges).

## 3. Automatic Request Triggers

Dependency edges on a node's input ports cause the node to **automatically emit packet requests** under configurable conditions. This is the mechanism that makes dependency edges practical — no external orchestration is needed.

### Trigger Conditions

Trigger configuration is set at the **node level** (not per-edge). Two trigger conditions exist, and both can be active simultaneously:

#### `on_startup`
- At net startup (during the first `RunStep`), the node sends a packet request backward through all its dependency edges
- This is a **one-shot** trigger: it fires once and never again
- **Default behavior**: this trigger is enabled by default on nodes with dependency edges
- Use case: Hamilton-style "resolve all dependencies eagerly at startup"

#### `on_no_salvo_triggered` (deferred)
- When a packet arrives at any of the node's input ports and **no input salvo condition is triggered**, the node sends a packet request backward through its dependency edges
- Rate-limited: the node has **one request token**. Sending a request spends it. Completing an epoch (finished or cancelled) replenishes it. Nodes start with one token.
- **Not enabled by default**: must be explicitly configured
- Use case: hybrid push-pull where push data arrives first, then missing dependency data is pulled on demand

#### Combined mode

Both triggers can be active simultaneously. In this case:
- `on_startup` fires once at the beginning
- `on_no_salvo_triggered` provides ongoing reactive pulling as new push data arrives

### Configuration

At the node level (in `NodeConfig` or similar):
```
dependency_request_triggers: ["on_startup"]              # default for nodes with dependency edges
dependency_request_triggers: ["on_no_salvo_triggered"]   # deferred only
dependency_request_triggers: ["on_startup", "on_no_salvo_triggered"]  # both
dependency_request_label: "batch_1"                      # REQUIRED
```

### Request Token Mechanics (for `on_no_salvo_triggered`)

1. Node starts with 1 request token
2. Push data arrives, salvo conditions fail → node spends token, sends request through dependency edges
3. Upstream data arrives via dependency edges, salvo triggers, epoch runs
4. Epoch completes (finished or cancelled) → token replenished
5. If more push data arrives and salvo fails again → repeat from step 2

If the token is spent and more push data arrives without a salvo triggering, no request is sent. This prevents request storms. The token is replenished only after the node completes an epoch, ensuring a 1:1 relationship between requests and epoch cycles.

**Note**: The rate limit only applies to `on_no_salvo_triggered`. The `on_startup` trigger is a one-shot that does not consume the token.

## 4. Core Concept: Packet Requests

A **packet request** is a lightweight demand signal that flows backward through the graph. Unlike regular packets, requests are **not** first-class simulation objects — they do not have IDs, locations, or slot semantics. Instead, a request cascade is resolved as an atomic graph traversal (BFS/DFS backward from the target node).

### Request Origin

Packet requests can originate from:
1. **Automatic triggers** on nodes with dependency edges (Section 3)
2. **External callers** via the `CreateRequest` action (e.g., `net.request("node_name")`)

Both feed into the same cascade and resolution system.

### Request Labels

Every packet request has a **label** (a required string identifier at the netrun-sim level). Labels enable batched deduplication:
- Requests with the same label that reach the same source node are merged into a single epoch
- Requests with different labels create separate epochs

Labels are required at the netrun-sim level. At the netrun (Python) level, a default label (e.g., `"main"`) may be provided for convenience.

For automatic triggers, the label is configured at the node level via `dependency_request_label`.

### Request Cascade Mechanics

1. A request is placed (automatically or externally) on a node's input port(s)
2. The request propagates backward along the connected edge to the upstream node's output port
3. At the upstream node, the request propagates through the node (from output ports to all input ports) and continues cascading backward
4. The cascade terminates at **source nodes** — nodes with no upstream connections
5. Epochs are created at the identified source nodes
6. Normal forward data-flow takes over, eventually delivering data downstream

### Request Cascade as Graph Traversal

Requests are NOT materialized as objects that "travel" along edges step by step. The cascade is computed as a single atomic BFS/DFS backward traversal. This is simpler and sufficient because:
- The logic is straightforward (follow edges backward until source nodes are found)
- No slot/capacity semantics needed for requests
- No need for request locations or lifecycle management

**Edge traversal during cascade**: The auto-request is **initiated** only through dependency edges (at the triggering node). Once the cascade is underway at upstream nodes, it traverses **all edges** backward (dependency and regular) to find source nodes. The edge type distinction only matters at the point of initiation.

However, observability is still important. The system should record:
- Which node/port originated the request
- The full cascade path (which nodes/edges were traversed)
- Which source nodes were identified
- Which epochs were created as a result

## 5. Parent Tracking

Each request knows its parent request, forming a **request tree**. When a request propagates through a node and fans out to multiple input ports (each connecting to different upstream nodes), all the child requests share the same parent. This tree structure enables:
- Tracing the full provenance of why a source node was activated
- Determining which downstream request caused which upstream computation
- Potential future use: cancellation propagation, cost attribution

All request cascades descend from a **single root request**. If a request targets node Z, and the cascade fans out through intermediate nodes to reach source nodes A, B, and C — all of these are part of one request tree rooted at the original request.

## 6. Source Node Activation

When a cascade reaches source nodes:
- An epoch is created on each identified source node
- The epoch is created by finding the first satisfiable input salvo condition on the source node (with an empty set of packets, since source nodes have no inputs). If no salvo condition is satisfiable with empty inputs, a `NetError` is raised
- After source epochs start, they produce output packets via normal execution
- These packets flow forward through the graph via standard push-based mechanics
- Intermediate nodes trigger via their normal input salvo conditions
- Eventually, data reaches the node that was originally requested

**Important**: The request cascade only creates epochs at source nodes. Intermediate nodes are activated by normal forward packet flow — they have no special awareness that they are running "because of a request." However, since epochs track their lineage and request-originated epochs are tagged, it is possible to trace backward from any intermediate epoch to the originating request.

## 7. Validation

The cascade performs the following validations, raising `NetError` on failure:

### 7.1 Source node startability

If the cascade reaches a source node that has no input salvo condition satisfiable with zero packets, a `NetError` is raised. This means the source node cannot be started without external input, and the request cannot be fulfilled.

### 7.2 Unconnected input ports

If the cascade reaches a node where an input port has no upstream edge, a `NetError` is raised by default. This means the cascade cannot propagate further, and data cannot flow forward through this port.

**Optional override**: This validation can be disabled for specific ports. The use case is nodes with optional input ports — where an input salvo condition does not require that port to have data. When disabled, the cascade simply does not propagate through the unconnected port, and the node is expected to trigger via a salvo condition that doesn't require it.

### 7.3 DAG check

If the upstream subgraph from the initial request contains cycles, a `NetError` is raised. Request cascades assume a DAG — cycles would cause infinite propagation.

## 8. Multiple Requests and the Diamond Problem

Consider a graph where nodes B and C both feed into node D, and both B and C have a shared upstream source node A:

```
A → B → D
A → C → D
```

If D is requested, the cascade reaches A via two paths (through B and through C). Within a single cascade, BFS naturally deduplicates — A is visited once, one epoch is created.

For **cross-request** deduplication (two independent requests reaching the same source), labels are used:

**Same label (merging happens)**:
```
Request X (label="batch_1") → cascade reaches source A
Request Y (label="batch_1") → cascade also reaches source A
Result: ONE epoch created in A (both requests share label "batch_1")
```

**Different labels (no merging)**:
```
Request X (label="batch_1") → cascade reaches source A
Request Y (label="batch_2") → cascade also reaches source A
Result: TWO epochs created in A (one for each label)
```

It is the user's responsibility to assign labels appropriately and to enable caching on nodes where redundant computation is a concern.

**Note**: In diagrams and informal discussion, we may use "color" as a visual metaphor for labels (e.g., "red request" and "green request"). In the implementation, the field is a string label.

## 9. Resolution Timing: RunStep Integration

Packet requests are accumulated as pending and resolved during `RunStep`. This is essential for label-based merging to work — all requests must be collected before resolution so that same-labeled requests can be deduplicated.

### Sequencing Within RunStep

```
RunStep:
  Phase 1: Move packets from edges to input ports (existing)
  Phase 2: Check input salvo conditions, create startable epochs (existing)
  Phase 2b: Auto-request generation (NEW)
    - For nodes where packets arrived but no salvo triggered:
      - If on_no_salvo_triggered is enabled AND node has request token:
        spend token, emit requests through dependency edges
    - On first RunStep only:
      - For nodes with on_startup trigger: emit requests through dependency edges
    - All generated requests join the pending queue
  Phase 3: Resolve pending packet requests (NEW)
    a. Cascade each pending request backward via BFS
    b. Validate (DAG check, startability, connectivity)
    c. At each source node, group requests by label
    d. Create one epoch per (source_node, label) pair
    e. Clear pending requests
    f. Emit request-specific events
```

Requests can also be submitted externally at any time via a `CreateRequest` action. They accumulate in the same pending queue and are resolved in Phase 3 alongside auto-generated requests.

Source epochs created in Phase 3 become `Startable`. In the Python `Net.run_step()`, these are picked up and executed either in the same step (if auto_start_epochs is true) or in the next step.

### Token Replenishment

Request tokens for `on_no_salvo_triggered` are replenished when an epoch on that node transitions to `Finished` or `Cancelled`. This happens outside `RunStep` (epoch completion is driven by external code via `FinishEpoch` or `CancelEpoch` actions).

## 10. Implementation Layer: netrun-sim

The entire request cascade mechanism is implemented in **netrun-sim** (Rust).

### Why netrun-sim

1. **Graph traversal is a graph operation.** netrun-sim owns the graph topology (nodes, ports, edges, connectivity). The `edges_by_head` index already supports efficient backward lookup from input ports to incoming edges.

2. **Epoch creation is a netrun-sim concern.** The cascade's result is "create epochs at these source nodes." Epoch lifecycle (`CreateEpoch`, `StartEpoch`) is core netrun-sim territory.

3. **Label-based merging needs centralized state.** Pending requests accumulate and are resolved together during `RunStep`. netrun-sim already manages step-based state.

4. **Avoids duplicating graph knowledge.** Everything the cascade needs (topology, port connectivity, salvo conditions) already lives in netrun-sim.

5. **Simplicity.** The cascade is a deterministic graph traversal + grouping + epoch creation. No Python-level policy is needed.

### New netrun-sim API

**Graph changes** (`graph.rs`):
- `Graph` gains `dependency_edges: HashSet<Edge>` — subset of edges that are dependency edges
- `Graph::new()` gains a `dependency_edges: Vec<Edge>` parameter
- `Graph::is_dependency_edge(&self, edge: &Edge) -> bool` helper
- `Graph` validation: dependency edges must be a subset of the full edge set
- `Node` gains optional dependency request configuration:
  ```rust
  pub struct DependencyRequestConfig {
      pub triggers: Vec<DependencyRequestTrigger>,
      pub label: String,  // required
  }

  pub enum DependencyRequestTrigger {
      OnStartup,
      OnNoSalvoTriggered,
  }
  ```
- `Node` validation: nodes with `DependencyRequestConfig` must have at least one dependency edge on their input ports

**NetSim changes** (`net.rs`):
- New state fields:
  ```rust
  _pending_requests: Vec<PendingRequest>,
  _request_tokens: HashMap<NodeName, bool>,
  _startup_requests_sent: bool,
  ```
- `NetSim::new()` initializes tokens for nodes with `on_no_salvo_triggered` trigger
- `PendingRequest` struct:
  ```rust
  pub struct PendingRequest {
      pub node_name: NodeName,
      pub port_name: PortName,
      pub label: String,
  }
  ```

**New action**:
- `CreateRequest(NodeName, PortName, String)` — registers a pending packet request (node, port, label)

**Modified actions**:
- `RunStep` — gains Phase 2b (auto-request generation) and Phase 3 (cascade resolution)
- `FinishEpoch` / `CancelEpoch` — replenish request token on the epoch's node

**New events**:
- `RequestCreated(EventUTC, NodeName, PortName, String)` — when a request is registered (node, port, label)
- `RequestCascadeResolved(EventUTC, Vec<NodeName>, String)` — cascade completed (source nodes found, label)
- `RequestEpochCreated(EventUTC, EpochID, NodeName, String)` — epoch created from request (epoch, source node, label)

**New errors** (added to `NetActionError`):
- `RequestCycleDetected { node_name }` — cycle found during backward BFS
- `RequestSourceNotStartable { node_name }` — source node has no satisfiable salvo with zero packets
- `RequestUnconnectedPort { node_name, port_name }` — cascade reached an unconnected input port
- `RequestNodeNotFound { node_name }` — request targets a non-existent node
- `RequestPortNotFound { node_name, port_name }` — request targets a non-existent port

### What belongs in netrun (Python)

- Higher-level API wrapping the netsim actions (e.g., `net.request("node_name")`)
- Default label (`"main"` or similar) for convenience when no explicit label is provided
- Integration with the execution manager for executing request-created epochs
- Configuration mapping from `NodeConfig` / `NodeExecutionConfig` to netsim node settings
- Future: caching-aware cascade short-circuiting

## 15. Implementation Plan (netrun-sim)

### Step 1: Add `DependencyRequestConfig` and `DependencyRequestTrigger` types

**File**: `netrun-sim/core/src/graph.rs`

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DependencyRequestTrigger {
    OnStartup,
    OnNoSalvoTriggered,
}

#[derive(Debug, Clone)]
pub struct DependencyRequestConfig {
    pub triggers: Vec<DependencyRequestTrigger>,
    pub label: String,
}
```

Add `pub dependency_request_config: Option<DependencyRequestConfig>` to `Node`.

### Step 2: Add `dependency_edges` to `Graph`

**File**: `netrun-sim/core/src/graph.rs`

- Add `dependency_edges: HashSet<Edge>` field to `Graph`
- Update `Graph::new()` signature: `new(nodes: Vec<Node>, edges: Vec<Edge>, dependency_edges: Vec<Edge>) -> Self`
- Add `is_dependency_edge(&self, edge: &Edge) -> bool`
- Add `get_dependency_edges(&self) -> &HashSet<Edge>`
- Add validation: every edge in `dependency_edges` must exist in `edges`

### Step 3: Add backward BFS to `Graph`

**File**: `netrun-sim/core/src/graph.rs`

```rust
/// Result of a backward cascade from a node.
pub struct CascadeResult {
    pub source_nodes: Vec<NodeName>,
    pub visited_nodes: Vec<NodeName>,
    pub visited_edges: Vec<Edge>,
}

impl Graph {
    /// Backward BFS from a set of input ports, following all edges upstream.
    /// Returns source nodes (nodes with no incoming edges in the traversed subgraph).
    /// Errors on cycles.
    pub fn cascade_backward(
        &self,
        start_ports: Vec<PortRef>,  // input ports to start from
    ) -> Result<CascadeResult, GraphValidationError> { ... }
}
```

The BFS:
1. Start with the given input ports
2. For each input port, find incoming edges via `edges_by_head`
3. Follow each edge to the upstream node's output port → the upstream node
4. For the upstream node, add ALL input ports to the BFS queue
5. Track visited nodes for cycle detection
6. A node is a "source" if none of its input ports have incoming edges

### Step 4: Add request state to `NetSim`

**File**: `netrun-sim/core/src/net.rs`

- Add `_pending_requests: Vec<PendingRequest>`, `_request_tokens: HashMap<NodeName, bool>`, `_startup_requests_sent: bool` to `NetSim`
- Initialize in `NetSim::new()`: tokens = `true` for all nodes with `on_no_salvo_triggered`, `_startup_requests_sent = false`

### Step 5: Add `CreateRequest` action

**File**: `netrun-sim/core/src/net.rs`

- Add `CreateRequest(NodeName, PortName, String)` variant to `NetAction`
- Implementation: validate node/port exist, push to `_pending_requests`, emit `RequestCreated` event

### Step 6: Add request events and errors

**File**: `netrun-sim/core/src/net.rs`

- Add `RequestCreated`, `RequestCascadeResolved`, `RequestEpochCreated` to `NetEvent`
- Add request-related errors to `NetActionError`

### Step 7: Implement Phase 2b in `RunStep` (auto-request generation)

**File**: `netrun-sim/core/src/net.rs`, inside `run_step()`

After Phase 2 (salvo condition checking):

1. **On first RunStep** (`!self._startup_requests_sent`):
   - For each node with `on_startup` in its triggers:
     - Find all dependency edges targeting this node's input ports
     - For each such port, push a `PendingRequest` with the node's label
   - Set `_startup_requests_sent = true`

2. **On every RunStep** (for `on_no_salvo_triggered`):
   - Track which nodes had packets arrive at input ports (from Phase 1) but no salvo triggered (from Phase 2)
   - For each such node:
     - Check if it has `on_no_salvo_triggered` trigger and a token (`_request_tokens[node] == true`)
     - If so: spend token, find dependency-edge input ports, push `PendingRequest` for each

### Step 8: Implement Phase 3 in `RunStep` (cascade resolution)

**File**: `netrun-sim/core/src/net.rs`, inside `run_step()`

After Phase 2b:

1. If `_pending_requests` is empty, skip
2. For each pending request, run `graph.cascade_backward(start_ports)`:
   - Validate: no cycles, all intermediate nodes have connected ports, source nodes are startable
3. Group results by `(source_node, label)`:
   - Deduplicate: one epoch per (source_node, label) pair
4. For each unique (source_node, label):
   - Find first satisfiable input salvo condition (with zero packets)
   - Create epoch via internal epoch creation logic (same as `try_trigger_input_salvo` but with empty salvo)
   - Emit `RequestEpochCreated` event
5. Clear `_pending_requests`
6. Emit `RequestCascadeResolved` events
7. Set `made_progress = true` if any epochs were created

### Step 9: Token replenishment in `FinishEpoch` / `CancelEpoch`

**File**: `netrun-sim/core/src/net.rs`

In both `finish_epoch()` and `cancel_epoch()`:
- After the existing logic, check if the epoch's node has `on_no_salvo_triggered` trigger
- If so, set `_request_tokens[node_name] = true`

### Step 10: Update Python bindings

**File**: `netrun-sim/python/src/graph.rs`

- Add `DependencyRequestTrigger` pyclass (enum)
- Add `DependencyRequestConfig` pyclass
- Update `Node.__init__` to accept optional `dependency_request_config`
- Update `Graph.__init__` to accept `dependency_edges` parameter
- Add `Graph.is_dependency_edge()` and `Graph.get_dependency_edges()` methods

**File**: `netrun-sim/python/src/net.rs`

- Add `CreateRequest` variant to `NetAction` Python binding
- Add new event types to Python event extraction
- Add new error types to Python error conversion

### Step 11: Write tests

**File**: `netrun-sim/core/src/net_tests.rs` (or new test file)

- Test backward BFS: linear chain, diamond, fan-in/fan-out
- Test cycle detection in backward BFS
- Test `CreateRequest` action and pending queue
- Test `on_startup` trigger: epochs created at source nodes on first RunStep
- Test `on_no_salvo_triggered` trigger: request sent when salvo fails, token mechanics
- Test label-based merging: same label → one epoch, different labels → separate epochs
- Test source node startability validation
- Test unconnected port validation
- Test combined triggers (on_startup + on_no_salvo_triggered)
- Test token replenishment on FinishEpoch and CancelEpoch
- Test full end-to-end: request → cascade → source epoch → forward flow → target epoch

### Step 12: Update Python binding tests

**File**: `netrun-sim/python/` tests

- Mirror the Rust tests at the Python binding level
- Test the `Graph` constructor with dependency edges
- Test `CreateRequest` action through Python API

## 11. Interaction with Existing Mechanisms

### Forward Flow

After source epochs produce output, normal forward flow handles everything. Intermediate nodes trigger via their existing input salvo conditions. No changes to forward flow mechanics are needed. Dependency edges carry forward data identically to regular edges.

### Epochs and Concurrency

netrun supports multiple running epochs per node. If a request cascade creates an epoch in a node that already has running epochs, this is fine — the new epoch is independent.

### Salvo Conditions

Request cascades bypass input salvo conditions during backward traversal — they are about dependency resolution, not data readiness. However, at source nodes, the cascade **does** check salvo conditions to find a startable condition (one satisfiable with zero packets). Intermediate nodes trigger via their normal salvo conditions when forward-flowing packets arrive.

### Signals

Signals (epoch_finished, epoch_failed, etc.) work orthogonally to requests. An epoch created by a request cascade emits signals just like any other epoch. Signals and requests are complementary: signals say "I finished," requests say "I need you to start."

### Existing Graph Capabilities

The `edges_by_head` index in the `Graph` struct maps each input port to its incoming edges. This is the exact primitive needed for backward BFS — no new indices are required. The `CreateEpoch` action already supports empty salvos (zero packets), which is what source node activation needs.

## 12. Worked Example: Hybrid Push-Pull

Consider the graph from the diagram:
```
A  ===dependency===>  C
B  ---regular------>  C
```

C has two input ports. One is connected to A via a dependency edge, the other to B via a regular edge. C is configured with `dependency_request_triggers: ["on_startup"]`.

### Startup flow:
1. Net starts, first `RunStep` begins
2. Phase 2b: C has `on_startup` trigger → emits request through dependency edge to A
3. Phase 3: Cascade from C's dependency port → reaches A (source node) → creates epoch at A
4. A's epoch becomes Startable, gets started, A runs
5. A produces output → packet flows forward through dependency edge → arrives at C's input port
6. Meanwhile, B also runs (started separately) → packet flows through regular edge → arrives at C's other input port
7. C's salvo condition: both ports have data → triggers → C runs

### Deferred flow (if configured with `on_no_salvo_triggered`):
1. B runs first, produces output → packet arrives at C's regular-edge port
2. Phase 2: C's salvo checked → fails (dependency port empty)
3. Phase 2b: C has `on_no_salvo_triggered`, has token → spends token, emits request through dependency edge
4. Phase 3: Cascade → A activated → A runs → data flows to C's dependency port
5. Next RunStep: C's salvo checks → both ports filled → triggers → C runs
6. C's epoch completes → token replenished

## 13. Open Questions

1. **Request-aware caching / cascade short-circuiting**: If an intermediate node already has cached output, should the cascade stop there instead of continuing to source nodes? This would be an optimization but requires netrun-sim to accept a "stop list" of nodes from the Python layer. Deferred to a future iteration.

2. **Port-specific requests**: Currently, a request is placed on a specific input port. Should there be a convenience to request "all input ports of a node" (i.e., cascade through all inputs)? Probably yes, as a higher-level API in the Python Net class.

3. **Request cancellation**: If a request is superseded or no longer needed, should pending requests be cancellable before resolution? Deferred.

4. **In-node requests**: Future iterations may allow executing nodes to emit requests via `ctx.request(...)`. This introduces complexity (the node runs twice — once to emit the request, once to process the response) and is deferred.

5. **Multi-packet salvo conditions on dependency ports**: If a salvo requires N packets from a dependency port, each request cycle only pulls one epoch's worth of output. The node would need N request-epoch cycles to accumulate enough data. This is correct behavior but could be slow for large N. It is the user's responsibility to design their graph appropriately for this.

## 14. Relationship to Existing Systems

### Apache Hamilton

Hamilton builds a DAG from function signatures (parameter names = node names = edges). You request terminal outputs, and Hamilton resolves the minimal upstream subgraph. Packet requests bring this same pull-based model to netrun, but within an explicit graph topology rather than implicit signature-based wiring.

### Push vs Pull Duality

| Aspect | Push (current netrun) | Pull (packet requests) |
|--------|----------------------|----------------------|
| Data flow | Forward: out_port → in_port | Backward: in_port → out_port (requests only) |
| Activation | Packets arrive → salvo triggers → epoch starts | Request sent → cascade finds sources → epochs start |
| Who initiates | Source nodes / external injection | Auto-trigger on dependency edges / external `CreateRequest` |
| Intermediate nodes | Triggered by arriving packets | Triggered by forward flow after sources run |
| Use case | Streaming, event-driven, continuous | On-demand, lazy, dependency-driven |

netrun with packet requests supports **both models simultaneously** on the same graph, with dependency edges as the composable bridge between them.
