# netrun-core

A Rust implementation of a flow-based development (FBD) runtime simulation engine.

## Overview

This library simulates the flow of packets through a network of interconnected nodes. It does **not** execute actual node logic or manage packet data—instead, it tracks packet locations, validates flow conditions, and manages the lifecycle of node executions (called "epochs").

The library is designed to be used by external code that:
- Defines the graph topology (nodes, ports, edges)
- Handles actual node execution logic
- Manages packet data/payloads
- Responds to network events

## Repository Structure

```
netrun-core/
├── Cargo.toml              # Workspace root
├── core/                   # Core Rust library
│   ├── Cargo.toml
│   ├── src/
│   ├── tests/              # Integration tests
│   └── examples/           # Rust examples
└── python/                 # Python bindings (PyO3)
    ├── Cargo.toml
    ├── pyproject.toml
    ├── src/
    ├── python/netrun_core/ # Python package
    └── examples/
```

## Requirements

- Rust 1.85+ (edition 2024)
- Python 3.8+ (for Python bindings)
- [uv](https://github.com/astral-sh/uv) (recommended for Python dependency management)

## Building

### Rust Library

```bash
# Build the core library
cargo build -p netrun-core

# Build in release mode
cargo build -p netrun-core --release
```

### Python Bindings

```bash
cd python

# Create virtual environment and install dependencies
uv venv .venv
uv sync

# Build and install in development mode
uv run maturin develop

# Or build a release wheel
uv run maturin build --release
```

## Running Tests

### All Tests

```bash
# Run all Rust tests (unit + integration + doc tests)
cargo test -p netrun-core

# Run Python examples
cd python
uv run python examples/linear_flow.py
uv run python examples/diamond_flow.py
```

### Rust Tests Only

```bash
# Unit tests only
cargo test -p netrun-core --lib

# Integration tests only
cargo test -p netrun-core --test '*'

# Doc tests only
cargo test -p netrun-core --doc

# Run with output
cargo test -p netrun-core -- --nocapture
```

### Python Tests

```bash
cd python

# Ensure bindings are built
uv run maturin develop

# Run examples
uv run python examples/linear_flow.py
uv run python examples/diamond_flow.py
```

## Running Examples

### Rust Examples

```bash
# Linear flow: A -> B -> C
cargo run -p netrun-core --example linear_flow

# Diamond flow: A -> B,C -> D (branching and synchronization)
cargo run -p netrun-core --example diamond_flow
```

### Python Examples

```bash
cd python
uv run python examples/linear_flow.py
uv run python examples/diamond_flow.py
```

## Quick Start (Rust)

```rust
use netrun_core::graph::*;
use netrun_core::net::*;

// Create a simple A -> B graph
let node_a = Node::new("A".into())
    .with_out_port("out".into(), Port::new(PortSlotSpec::Infinite));

let node_b = Node::new("B".into())
    .with_in_port("in".into(), Port::new(PortSlotSpec::Infinite))
    .with_in_salvo_condition(
        "default".into(),
        SalvoCondition::new(
            1,
            vec!["in".into()],
            SalvoConditionTerm::Port("in".into(), PortState::NonEmpty),
        ),
    );

let edge = (
    EdgeRef::new(
        PortRef::new("A".into(), PortType::Output, "out".into()),
        PortRef::new("B".into(), PortType::Input, "in".into()),
    ),
    Edge::new(),
);

let graph = Graph::new(vec![node_a, node_b], vec![edge]);
assert!(graph.validate().is_empty());

// Create and run the network
let mut net = Net::new(graph);

// Create a packet and place it on the edge
let (response, _) = net.do_action(NetAction::CreatePacket(None))?;
let packet_id = match response {
    NetActionResponseData::Packet { packet_id } => packet_id,
    _ => panic!("Expected packet response"),
};

let edge_loc = PacketLocation::Edge(EdgeRef::new(
    PortRef::new("A".into(), PortType::Output, "out".into()),
    PortRef::new("B".into(), PortType::Input, "in".into()),
));
net.do_action(NetAction::TransportPacketToLocation(packet_id, edge_loc))?;

// Run until blocked - packet moves to B's input, triggering an epoch
net.do_action(NetAction::RunNetUntilBlocked)?;

// Check for startable epochs
let startable = net.get_startable_epochs();
println!("Startable epochs: {}", startable.len());
```

## Quick Start (Python)

```python
from netrun_core import (
    Graph, Node, Edge, EdgeRef, Net, NetAction, NetActionResponseData,
    Port, PortRef, PortType, PortSlotSpec, PortState,
    SalvoCondition, SalvoConditionTerm, PacketLocation,
)

# Create a simple A -> B graph
node_a = Node(
    name="A",
    out_ports={"out": Port(PortSlotSpec.infinite())},
)
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

edge = (
    EdgeRef(
        PortRef("A", PortType.Output, "out"),
        PortRef("B", PortType.Input, "in"),
    ),
    Edge(),
)

graph = Graph([node_a, node_b], [edge])
assert len(graph.validate()) == 0

# Create and run the network
net = Net(graph)

# Create a packet and place it on the edge
response, events = net.do_action(NetAction.create_packet())
assert isinstance(response, NetActionResponseData.Packet)
packet_id = response.packet_id

edge_loc = PacketLocation.edge(
    EdgeRef(
        PortRef("A", PortType.Output, "out"),
        PortRef("B", PortType.Input, "in"),
    )
)
net.do_action(NetAction.transport_packet_to_location(packet_id, edge_loc))

# Run until blocked
net.do_action(NetAction.run_net_until_blocked())

# Check for startable epochs
startable = net.get_startable_epochs()
print(f"Startable epochs: {len(startable)}")
```

## Documentation

- [CLAUDE.md](CLAUDE.md) - Detailed architecture and concepts
- [python/README.md](python/README.md) - Python bindings documentation

## License

MIT
