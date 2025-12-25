# netrun-sim Python Bindings

Python bindings for `netrun-sim`, a flow-based development runtime simulation engine.

## Installation

### From Source (Development)

1. Create a virtual environment and install dependencies:

```bash
cd python
uv venv .venv
uv sync
```

2. Build and install the extension in development mode:

```bash
uv run maturin develop
```

### From Source (Release Build)

```bash
cd python
uv sync
uv run maturin build --release
pip install ../target/wheels/netrun_sim-*.whl
```

## Quick Start

```python
from netrun_sim import (
    Graph, Node, Edge, Net, NetAction, NetActionResponseData,
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

edge = Edge(
    PortRef("A", PortType.Output, "out"),
    PortRef("B", PortType.Input, "in"),
)

graph = Graph([node_a, node_b], [edge])
assert len(graph.validate()) == 0

# Create and run the network
net = Net(graph)

# Create a packet
response, events = net.do_action(NetAction.create_packet())
assert isinstance(response, NetActionResponseData.Packet)
packet_id = response.packet_id

# Place packet on edge A -> B
edge_loc = PacketLocation.edge(
    Edge(
        PortRef("A", PortType.Output, "out"),
        PortRef("B", PortType.Input, "in"),
    )
)
net.do_action(NetAction.transport_packet_to_location(packet_id, edge_loc))

# Run the network
net.do_action(NetAction.run_net_until_blocked())

# Check for startable epochs
startable = net.get_startable_epochs()
print(f"Startable epochs: {len(startable)}")
```

## Examples

See the `examples/` directory for complete examples:

### Python Scripts
- `linear_flow.py` - Simple A -> B -> C packet flow
- `diamond_flow.py` - Branching and merging with synchronization

Run examples:

```bash
python examples/linear_flow.py
python examples/diamond_flow.py
```

### Jupyter Notebooks
- `linear_flow.ipynb` - Interactive walkthrough of linear packet flow
- `diamond_flow.ipynb` - Interactive walkthrough of branching/merging with synchronization

Run notebooks:

```bash
jupyter notebook examples/linear_flow.ipynb
```

## API Overview

### Graph Types

| Type | Description |
|------|-------------|
| `Graph` | Static network topology |
| `Node` | Processing unit with ports and salvo conditions |
| `Port` | Connection point with capacity specification |
| `PortRef` | Reference to a specific port on a node |
| `Edge` | Connection between two ports (source and target) |
| `PortSlotSpec` | Port capacity (`.infinite()` or `.finite(n)`) |
| `PortState` | Predicate for salvo conditions |
| `SalvoCondition` | Rule for triggering epochs or sending packets |
| `SalvoConditionTerm` | Boolean expression over port states |

### Net Types

| Type | Description |
|------|-------------|
| `Net` | Runtime network state |
| `NetAction` | Action to perform on the network |
| `NetEvent` | Event that occurred during an action |
| `NetActionResponseData` | Response data from successful actions |
| `PacketLocation` | Where a packet is located |
| `Packet` | A packet in the network |
| `Epoch` | Execution instance of a node |
| `EpochState` | Lifecycle state (Startable, Running, Finished) |
| `Salvo` | Collection of packets entering/exiting a node |

### Exceptions

All exceptions inherit from `NetrunError`:

- `PacketNotFoundError`
- `EpochNotFoundError`
- `EpochNotRunningError`
- `EpochNotStartableError`
- `NodeNotFoundError`
- `InputPortNotFoundError`
- `InputPortFullError`
- `OutputPortNotFoundError`
- `OutputPortFullError`
- `EdgeNotFoundError`
- `GraphValidationError`
- And more...

## Type Hints

Full type stubs are provided in `__init__.pyi` for IDE autocompletion and type checking.

## License

Same license as the parent `netrun-sim` project.
