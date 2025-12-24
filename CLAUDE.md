# Flow-Based Development Runtime Core (netrun-core)

This document provides a comprehensive overview of the `netrun-core` library, a Rust implementation of a flow-based development (FBD) runtime simulation engine.

## Overview

This library simulates the flow of packets through a network of interconnected nodes. It does **not** execute actual node logic or manage packet data—instead, it tracks packet locations, validates flow conditions, and manages the lifecycle of node executions (called "epochs").

The library is designed to be used by external code that:
1. Defines the graph topology (nodes, ports, edges)
2. Handles actual node execution logic
3. Manages packet data/payloads
4. Responds to network events

## Core Concepts

### Graph (`graph.rs`)

The `Graph` represents the static topology of the network:

- **Nodes** (`Node`): Processing units with input and output ports
- **Ports** (`Port`): Connection points on nodes, either input or output
  - Each port has a `slots_spec` defining capacity (`Infinite` or `Finite(n)`)
- **Edges** (`Edge`): Connections between output ports of one node and input ports of another
- **Salvo Conditions** (`SalvoCondition`): Rules that define when packets can trigger an epoch or be sent

### Net (`net.rs`)

The `Net` represents the runtime state of a network:

- **Packets** (`Packet`): Units that flow through the network
  - Identified by `PacketID` (ULID)
  - Have a `location` tracking where they are
- **Epochs** (`Epoch`): Execution instances of a node
  - A single node can have multiple simultaneous epochs
  - Lifecycle: `Startable` → `Running` → `Finished`
- **Salvos** (`Salvo`): Collections of packets that enter or exit a node together

### Packet Locations

Packets can be in one of five locations:

```rust
enum PacketLocation {
    Node(EpochID),           // Inside a running/startable epoch
    InputPort(NodeName, PortName),  // Waiting at a node's input port
    OutputPort(EpochID, PortName),  // Loaded into an epoch's output port
    Edge(EdgeRef),           // In transit between nodes
    OutsideNet,              // External to the network
}
```

### Salvo Conditions

Salvo conditions define when packets can trigger actions:

- **Input Salvo Conditions**: Define when packets at input ports can trigger a new epoch
- **Output Salvo Conditions**: Define when packets at output ports can be sent out

Each condition has:
- `term`: A boolean expression over port states (empty, full, equals N, etc.)
- `ports`: Which ports' packets are included when the condition triggers
- `max_salvos`: Maximum number of times this condition can trigger (must be 1 for input salvos)

## Flow Mechanics

### Automatic Flow (`run_until_blocked`)

When `RunNetUntilBlocked` is called, the network automatically:

1. **Moves packets from edges to input ports**
   - Checks if the destination port has available slots
   - Respects port capacity limits

2. **Checks input salvo conditions**
   - After each packet arrives at an input port, checks all input salvo conditions
   - First satisfied condition wins (checked in order)
   - Creates a `Startable` epoch with the packets from the specified ports

3. **Repeats until blocked**
   - Blocked = no packets can move (either no packets on edges, or all destinations are full)

### Manual Actions (`NetAction`)

External code controls the network through actions:

| Action | Description |
|--------|-------------|
| `RunNetUntilBlocked` | Run automatic packet flow until no progress can be made |
| `CreatePacket(Option<EpochID>)` | Create a new packet (inside an epoch or outside the net) |
| `ConsumePacket(PacketID)` | Remove a packet from the network |
| `StartEpoch(EpochID)` | Transition a `Startable` epoch to `Running` |
| `FinishEpoch(EpochID)` | Complete a `Running` epoch (must be empty of packets) |
| `CancelEpoch(EpochID)` | Cancel an epoch (not yet implemented) |
| `CreateAndStartEpoch(NodeName, Salvo)` | Manually create and start an epoch (not yet implemented) |
| `LoadPacketIntoOutputPort(PacketID, PortName)` | Move a packet from inside an epoch to its output port |
| `SendOutputSalvo(EpochID, SalvoConditionName)` | Send packets from output ports onto edges |

### Events (`NetEvent`)

Actions produce events that track what happened:

- `PacketCreated`, `PacketConsumed`
- `EpochCreated`, `EpochStarted`, `EpochFinished`, `EpochCancelled`
- `PacketMoved`
- `InputSalvoTriggered`, `OutputSalvoTriggered`
- `NodeEnabled`, `NodeDisabled`

## Typical Usage Pattern

1. **Define the graph**: Create nodes with ports and salvo conditions, connect with edges
2. **Create a Net**: Initialize runtime state from the graph
3. **Inject packets**: Create packets and place them on edges or in input ports
4. **Run the network**: Call `RunNetUntilBlocked` to move packets and trigger epochs
5. **Handle startable epochs**: External code decides when to start each epoch
6. **Simulate node execution**: External code "runs" the node logic
7. **Output results**: Load packets into output ports and send output salvos
8. **Finish epochs**: Mark epochs as finished when done
9. **Repeat**: Continue running the network until processing is complete

## Example Workflow

```
1. Packets arrive on edges
2. run_until_blocked() moves them to input ports
3. Input salvo condition satisfied → Epoch created (Startable)
4. External code calls StartEpoch → Epoch now Running
5. External code "executes" the node (outside this library)
6. External code creates output packets, loads into output ports
7. External code calls SendOutputSalvo → Packets move to edges
8. External code calls FinishEpoch → Epoch complete
9. run_until_blocked() continues the flow to next nodes
```

## Design Philosophy

- **Separation of concerns**: The library handles flow mechanics; external code handles execution
- **Event-driven**: All state changes produce events for observability
- **Explicit control**: External code explicitly starts epochs and sends salvos
- **Deterministic**: Salvo conditions are checked in order; first match wins

## File Structure

```
src/
├── lib.rs       # Module exports
├── _utils.rs    # Utility functions (timestamps)
├── graph.rs     # Graph topology types and salvo condition evaluation
└── net.rs       # Network runtime state and actions
```

## IMPORTANT: Sound Notification

Run this command to notify the user by sound in these situations:
- After finishing responding to a request or running a command
- When asking questions or needing clarification
- When needing permission to proceed

```bash
afplay /System/Library/Sounds/Funk.aiff
```
