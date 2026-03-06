# netrun-sim

A flow-based development (FBD) simulation engine that tracks packet flow through networks of interconnected nodes. It handles packet locations, validates flow conditions, and manages the lifecycle of node executions (epochs) — but does **not** execute actual node logic or manage packet data. This separation allows execution and data storage to be implemented independently (see [netrun](../netrun/)).

## Key Concepts

- **Graph** — Static topology: nodes with typed input/output ports, edges between ports, salvo conditions
- **NetSim** — Runtime state: packets, epochs, locations, event tracking
- **Packets** — Units that flow through the network, identified by ULID
- **Epochs** — Execution instances of a node (lifecycle: `Startable` → `Running` → `Finished`)
- **Salvo Conditions** — Boolean expressions over port states that trigger epoch creation or output sending
- **Actions & Events** — All mutations go through `do_action(NetAction)`, returning `NetEvent`s for auditability

## Architecture

```
External Code (netrun)          netrun-sim
┌──────────────────────┐    ┌──────────────────┐
│ • Defines graph      │───→│ Graph (topology)  │
│ • Executes node logic│    │                  │
│ • Manages data       │    │ NetSim (runtime)  │
│ • Responds to events │←───│ • Packet flow     │
│                      │    │ • Epoch lifecycle  │
│ do_action(action)    │───→│ • Salvo conditions │
│                      │←───│ → events[]        │
└──────────────────────┘    └──────────────────┘
```

## Packet Flow

1. Packets created at `OutsideNet`, transported to edges
2. `run_step()` moves packets from edges → input ports (respecting capacity)
3. Input salvo conditions checked; first match creates a `Startable` epoch
4. External code calls `StartEpoch` → epoch becomes `Running`
5. External code processes the node (outside this library)
6. Output packets loaded into output ports, `SendOutputSalvo` moves them to edges
7. `FinishEpoch` completes the epoch; cycle repeats

## Project Structure

```
netrun-sim/
├── Cargo.toml              # Workspace root (members: core, python)
├── core/                   # Rust library
│   ├── Cargo.toml          # Package: netrun-sim v0.1.4
│   ├── src/
│   │   ├── lib.rs          # Module exports (graph, net, test_fixtures)
│   │   ├── _utils.rs       # Utility functions
│   │   ├── graph.rs        # Graph topology types
│   │   ├── graph_tests.rs  # Graph unit tests
│   │   ├── net.rs          # NetSim runtime state + undo support
│   │   ├── net_tests.rs    # Net unit tests
│   │   └── test_fixtures.rs # Test helpers (feature-gated)
│   ├── tests/              # Integration tests
│   │   ├── graph_api.rs    # Graph construction and validation
│   │   ├── net_api.rs      # NetSim operations
│   │   ├── workflow.rs     # End-to-end workflows
│   │   └── common/         # Shared test utilities
│   └── examples/
│       ├── linear_flow.rs  # A → B → C workflow
│       └── diamond_flow.rs # A → B,C → D (branch/merge)
└── python/                 # Python bindings (PyO3 + Maturin)
    ├── Cargo.toml          # Package: netrun-sim-python
    ├── pyproject.toml      # Python build config
    ├── src/                # Rust binding code
    │   ├── lib.rs          # Module registration
    │   ├── errors.rs       # Exception hierarchy (23 exception types)
    │   ├── graph.rs        # Graph type wrappers
    │   └── net.rs          # NetSim type wrappers
    ├── python/netrun_sim/  # Python package
    │   ├── __init__.py     # Re-exports from native extension
    │   └── __init__.pyi    # Full type stubs for IDE support
    └── examples/           # Python examples and notebooks
```

## Rust API

### Graph Types

| Type | Description |
|------|-------------|
| `Graph` | Static topology with indexed edge lookups. Validates on construction |
| `Node` | Processing unit with named input/output ports and salvo conditions |
| `Port` | Connection point with capacity (`PortSlotSpec::Infinite` or `Finite(n)`) |
| `Edge` | Directed connection from output port to input port |
| `PortRef` | Reference to a specific port: `(NodeName, PortType, PortName)` |
| `SalvoCondition` | Rule with `term` (boolean expression), `ports` (which packets), `max_salvos` (trigger limit) |
| `SalvoConditionTerm` | Boolean expression tree: `True`, `False`, `Port{..}`, `And`, `Or`, `Not` |

**Graph rules**: Fan-in is allowed (multiple edges to one input port). Fan-out is not (one edge per output port).

### NetSim

```rust
let net = NetSim::new(graph);

// All mutations through do_action
let response = net.do_action(&NetAction::RunStep);
let response = net.do_action(&NetAction::CreatePacket(None));
let response = net.do_action(&NetAction::StartEpoch(epoch_id));
// ... etc.

// Convenience methods
let events = net.run_until_blocked();
let blocked = net.is_blocked();
let startable = net.get_startable_epochs();
```

### Actions

| Action | Description |
|--------|-------------|
| `RunStep` | Move packets from edges to input ports, trigger salvo conditions |
| `CreatePacket(Option<EpochID>)` | Create packet (outside net or inside epoch) |
| `ConsumePacket(PacketID)` | Normal removal |
| `DestroyPacket(PacketID)` | Abnormal removal (e.g., cancellation) |
| `StartEpoch(EpochID)` | Startable → Running |
| `FinishEpoch(EpochID)` | Running → Finished (epoch must be empty) |
| `CancelEpoch(EpochID)` | Cancel epoch, destroy all its packets |
| `CreateEpoch(NodeName, Salvo)` | Manually create epoch (bypasses salvo conditions) |
| `LoadPacketIntoOutputPort(PacketID, PortName)` | Move packet from inside epoch to output port |
| `SendOutputSalvo(EpochID, SalvoConditionName)` | Send output port packets onto edges |
| `TransportPacketToLocation(PacketID, PacketLocation)` | Move packet to any location |

### Events

All actions return `Vec<NetEvent>`:
- `PacketCreated`, `PacketConsumed`, `PacketDestroyed`, `PacketMoved`, `PacketOrphaned`
- `EpochCreated`, `EpochStarted`, `EpochFinished`, `EpochCancelled`
- `InputSalvoTriggered`, `OutputSalvoTriggered`

### Undo Support

Actions can be reversed via `undo_action()`, enabling undo/redo workflows.

## Python Bindings

Full Python API via PyO3 with type stubs for IDE autocompletion. See [`python/README.md`](python/README.md) for detailed documentation.

```python
from netrun_sim import (
    Graph, Node, Edge, NetSim, NetAction,
    Port, PortRef, PortType, PortSlotSpec, PortState,
    SalvoCondition, SalvoConditionTerm, PacketLocation,
)

# Create a simple A → B graph
node_a = Node(name="A", out_ports={"out": Port(PortSlotSpec.infinite())})
node_b = Node(
    name="B",
    in_ports={"in": Port(PortSlotSpec.infinite())},
    in_salvo_conditions={
        "default": SalvoCondition(
            max_salvos=1,
            ports=["in"],
            term=SalvoConditionTerm.port("in", PortState.non_empty()),
        ),
    },
)
edge = Edge(PortRef("A", PortType.Output, "out"), PortRef("B", PortType.Input, "in"))

graph = Graph([node_a, node_b], [edge])
net = NetSim(graph)

# Create packet, place on edge, run
response, events = net.do_action(NetAction.create_packet())
packet_id = response.packet_id
net.do_action(NetAction.transport_packet_to_location(
    packet_id, PacketLocation.edge(edge)
))
made_progress, events = net.run_step()
startable = net.get_startable_epochs()
```

## Building and Testing

### Rust

```bash
cd netrun-sim
cargo build -p netrun-sim
cargo test -p netrun-sim
cargo run -p netrun-sim --example linear_flow
cargo run -p netrun-sim --example diamond_flow
```

### Python

```bash
cd netrun-sim/python
uv venv .venv && uv sync
uv run maturin develop
uv run python examples/linear_flow.py
```
