# Net Class Quality of Life Improvements

This document outlines helper methods and conveniences to make the `Net` class easier to use, based on pain points identified in `pts/examples/net/00_basic_net.pct.py`.

## Table of Contents

1. [Current Pain Points](#current-pain-points)
2. [Output Queues System](#output-queues-system) ⭐ **New**
3. [Proposed Helper Methods](#proposed-helper-methods)
4. [Implementation Plan](#implementation-plan)

---

## Current Pain Points

The example notebook reveals several ergonomic issues when working with the Net class:

### 1. Creating External Packets

Current code requires multiple steps and accessing private attributes:

```python
# Create packet outside any epoch (epoch_id=None)
response, _ = net._netsim.do_action(
    netrun_sim.NetAction.create_packet(None)
)
packet_id = response.packet_id

# Store the packet value in the Net's packet store
net._packet_store.register(packet_id, data)
```

**Problems:**
- Requires importing `netrun_sim`
- Accesses private `_netsim` and `_packet_store`
- Multiple lines for a common operation

### 2. Transporting Packets to Input Ports

```python
net._netsim.do_action(
    netrun_sim.NetAction.transport_packet_to_location(
        packet_id,
        netrun_sim.PacketLocation.input_port("Source", "in"),
    )
)
```

**Problems:**
- Verbose API
- Requires `netrun_sim` import
- Accesses private `_netsim`

### 3. Injecting Data into the Network

Users commonly want to create packets with data and send them to a node in one step:

```python
# Current: 10+ lines for this common operation
packet_ids = []
for data in external_data:
    response, _ = net._netsim.do_action(netrun_sim.NetAction.create_packet(None))
    packet_id = response.packet_id
    packet_ids.append(packet_id)
    net._packet_store.register(packet_id, data)

for packet_id in packet_ids:
    net._netsim.do_action(
        netrun_sim.NetAction.transport_packet_to_location(
            packet_id,
            netrun_sim.PacketLocation.input_port("Source", "in"),
        )
    )
```

### 4. Extracting Packets from the Network

No convenient way to:
- Get packets from an output port
- Get packets from an edge
- Get packets from anywhere and their values

### 5. Iterating Over Epoch Logs

```python
# Currently requires accessing private attribute
for epoch_id in net._epoch_print_logs.keys():
    epoch_log = net.get_epoch_log(epoch_id)
```

### 6. Manual Epoch Execution

The `_execute_epoch` method is private but commonly needed:

```python
startable = net.get_startable_epochs()
if startable:
    await net._execute_epoch(startable[0])  # Private method
```

### 7. Querying Network State

Limited ability to:
- Get all packets in the network
- Query packet locations
- Check if a port/edge has packets
- Get packet values without consuming

### 8. Collecting Output from DAG Networks

For DAG workflows, the final nodes produce output that needs to be collected. Currently there's no clean way to:
- Define which output ports are "network outputs"
- Collect packets that exit the network
- Await results from the network

---

## Output Queues System

For DAG-style workflows, packets flow from source nodes through processing nodes to sink nodes. The sink nodes' output ports are typically unconnected (no downstream edges). We need a clean way to collect these outputs.

### Design Overview

1. **Configuration**: Define named output queues that collect packets from specific output ports
2. **Automatic routing**: Packets sent from configured output ports go into queues (not error)
3. **Retrieval API**: Poll, await (async), or block (sync) for packets from queues
4. **Metadata**: Retrieved packets include metadata (source port, timestamp, etc.)

### Configuration

Add to `NetConfig`:

```python
class OutputQueueConfig(BaseModel):
    """Configuration for an output queue."""

    # List of (node_name, port_name) tuples that feed this queue
    ports: list[tuple[str, str]]


class NetConfig(BaseModel):
    # ... existing fields ...

    # Named output queues
    output_queues: dict[str, OutputQueueConfig] = {}
    """
    Map of queue_name -> OutputQueueConfig.

    Example:
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
            "logs": OutputQueueConfig(ports=[("Logger", "out"), ("ErrorLogger", "out")]),
        }
    """

    # Catch-all queue for undeclared unconnected output ports
    catch_all_output_queue: str | None = None
    """
    If set, packets from unconnected output ports that aren't in any
    configured queue go to this queue. If None, they are silently discarded.

    Example: catch_all_output_queue="_uncategorized"
    """

    # Behavior for undeclared ports when no catch-all is configured
    undeclared_output_behavior: Literal["discard", "error"] = "discard"
    """
    What to do with packets from unconnected ports not in any queue:
    - "discard": Silently discard (default)
    - "error": Raise UnconnectedOutputPortError (original behavior)
    """
```

**Configuration Examples:**

```python
# Simple: single output queue
NetConfig(
    output_queues={
        "results": OutputQueueConfig(ports=[("Sink", "out")]),
    },
    ...
)

# Multiple queues, with catch-all
NetConfig(
    output_queues={
        "results": OutputQueueConfig(ports=[("Sink", "out")]),
        "metrics": OutputQueueConfig(ports=[("MetricsNode", "metrics")]),
    },
    catch_all_output_queue="_other",  # Everything else goes here
    ...
)

# Strict mode: error on undeclared outputs
NetConfig(
    output_queues={
        "results": OutputQueueConfig(ports=[("Sink", "out")]),
    },
    undeclared_output_behavior="error",  # Fail if unexpected output port
    ...
)
```

### OutputPacket Data Class

```python
@dataclass
class OutputPacket:
    """A packet retrieved from an output queue."""

    packet_id: str
    """The packet's ULID."""

    value: Any
    """The packet's value."""

    from_node: str
    """The node that produced this packet."""

    from_port: str
    """The output port that produced this packet."""

    queue_name: str
    """The queue this packet was retrieved from."""

    timestamp: datetime
    """When the packet arrived in the queue."""

    epoch_id: str
    """The epoch that produced this packet."""
```

### Retrieval API

#### By Named Queue

```python
# Async: await next packet (with optional timeout)
packet: OutputPacket = await net.get_output("results")
packet: OutputPacket = await net.get_output("results", timeout=5.0)

# Sync: block until next packet (with optional timeout)
packet: OutputPacket = net.get_output_sync("results")
packet: OutputPacket = net.get_output_sync("results", timeout=5.0)

# Non-blocking: get if available, else None
packet: OutputPacket | None = net.try_get_output("results")

# Get all currently available packets
packets: list[OutputPacket] = net.get_all_outputs("results")
```

#### By Node/Port (Keyword-Only)

To avoid confusion with positional arguments, node/port lookup uses keyword-only args:

```python
# These are equivalent if "results" maps to ("Sink", "out")
packet = await net.get_output("results")
packet = await net.get_output(node="Sink", port="out")

# Sync versions
packet = net.get_output_sync(node="Sink", port="out")
packet = net.try_get_output(node="Sink", port="out")
```

#### Queue Inspection

```python
# Check if queue has packets
has_packets: bool = net.has_output("results")
has_packets: bool = net.has_output(node="Sink", port="out")

# Get queue depth
count: int = net.output_count("results")

# List all configured queue names
names: list[str] = net.list_output_queues()
```

### Method Signatures

```python
class Net:
    # Async retrieval
    async def get_output(
        self,
        queue_name: str | None = None,
        *,
        node: str | None = None,
        port: str | None = None,
        timeout: float | None = None,
    ) -> OutputPacket:
        """Get the next packet from an output queue.

        Args:
            queue_name: Name of the output queue. Mutually exclusive with node/port.
            node: Node name (keyword-only). Must be used with port.
            port: Port name (keyword-only). Must be used with node.
            timeout: Max seconds to wait. None = wait forever.

        Returns:
            The next OutputPacket from the queue.

        Raises:
            ValueError: If neither queue_name nor node/port specified.
            ValueError: If both queue_name and node/port specified.
            KeyError: If queue_name not found or node/port not configured.
            asyncio.TimeoutError: If timeout exceeded.
        """

    # Sync retrieval (blocking)
    def get_output_sync(
        self,
        queue_name: str | None = None,
        *,
        node: str | None = None,
        port: str | None = None,
        timeout: float | None = None,
    ) -> OutputPacket:
        """Blocking version of get_output().

        Blocks the current thread until a packet is available.

        Raises:
            TimeoutError: If timeout exceeded.
        """

    # Non-blocking retrieval
    def try_get_output(
        self,
        queue_name: str | None = None,
        *,
        node: str | None = None,
        port: str | None = None,
    ) -> OutputPacket | None:
        """Get a packet if available, otherwise return None.

        Non-blocking - returns immediately.
        """

    # Batch retrieval
    def get_all_outputs(
        self,
        queue_name: str | None = None,
        *,
        node: str | None = None,
        port: str | None = None,
    ) -> list[OutputPacket]:
        """Get all currently available packets from a queue.

        Non-blocking - returns whatever is currently in the queue.
        """

    # Inspection
    def has_output(
        self,
        queue_name: str | None = None,
        *,
        node: str | None = None,
        port: str | None = None,
    ) -> bool:
        """Check if the queue has any packets available."""

    def output_count(
        self,
        queue_name: str | None = None,
        *,
        node: str | None = None,
        port: str | None = None,
    ) -> int:
        """Get the number of packets in the queue."""

    def list_output_queues(self) -> list[str]:
        """List all configured output queue names."""
```

### Internal Implementation

```python
class Net:
    def __init__(self, config: NetConfig):
        # ... existing ...

        # Output queues: queue_name -> asyncio.Queue[OutputPacket]
        self._output_queues: dict[str, asyncio.Queue[OutputPacket]] = {}

        # Reverse mapping: (node_name, port_name) -> queue_name
        self._port_to_queue: dict[tuple[str, str], str] = {}

        # Initialize queues from config
        for queue_name, queue_config in config.output_queues.items():
            self._output_queues[queue_name] = asyncio.Queue()
            for node_name, port_name in queue_config.ports:
                self._port_to_queue[(node_name, port_name)] = queue_name

        # Initialize catch-all queue if configured
        if config.catch_all_output_queue:
            self._output_queues[config.catch_all_output_queue] = asyncio.Queue()

    def _route_orphaned_packet(
        self,
        packet_id: str,
        from_node: str,
        from_port: str,
        epoch_id: str,
    ) -> None:
        """Route an orphaned packet to the appropriate queue."""

        # Check if port is configured for a specific queue
        queue_name = self._port_to_queue.get((from_node, from_port))

        if queue_name is None:
            # Not in a specific queue - check catch-all
            if self._config.catch_all_output_queue:
                queue_name = self._config.catch_all_output_queue
            elif self._config.undeclared_output_behavior == "error":
                raise UnconnectedOutputPortError(...)
            else:
                # Discard: consume value and return
                self._packet_store.consume(packet_id)
                return

        # Get value and create OutputPacket
        value = self._packet_store.consume(packet_id)
        output_packet = OutputPacket(
            packet_id=packet_id,
            value=value,
            from_node=from_node,
            from_port=from_port,
            queue_name=queue_name,
            timestamp=get_timestamp_utc(),
            epoch_id=epoch_id,
        )

        # Add to queue
        self._output_queues[queue_name].put_nowait(output_packet)
```

### Example Usage

```python
# Configure a DAG network with output collection
config = NetConfig(
    pools={...},
    graph=GraphConfig(
        nodes=[
            NodeGraphConfig(name="Source", ...),
            NodeGraphConfig(name="Process", ...),
            NodeGraphConfig(name="Sink", out_ports={"out": PortConfig()}, ...),
        ],
        edges=[
            EdgeConfig(source_str="Source.out", target_str="Process.in"),
            EdgeConfig(source_str="Process.out", target_str="Sink.in"),
            # Note: Sink.out is unconnected - packets go to output queue
        ],
    ),
    output_queues={
        "results": OutputQueueConfig(ports=[("Sink", "out")]),
    },
)

async with Net(config) as net:
    # Inject input data
    net.inject_data("Source", "in", [
        {"id": 1, "data": "hello"},
        {"id": 2, "data": "world"},
    ])

    # Run network in background
    await net.start_background()

    # Collect results as they arrive
    while True:
        try:
            packet = await net.get_output("results", timeout=5.0)
            print(f"Got result: {packet.value}")
        except asyncio.TimeoutError:
            break  # No more results

    # Or collect all at once after network completes
    # results = net.get_all_outputs("results")
```

### Relationship to netrun-sim Changes

This feature depends on the netrun-sim change to allow unconnected output ports (see `02_netrun_sim_changes.md`):

1. **netrun-sim** sends `PacketOrphaned` events when packets go to unconnected ports
2. **Net** receives these events and routes packets to configured queues
3. The `FinishedEpoch` response includes orphaned packet info for tracking

---

## Proposed Helper Methods

### Category 1: Packet Creation

#### `create_external_packet(value) -> str`

Create a packet outside the network with a value.

```python
def create_external_packet(self, value: Any) -> str:
    """Create a packet outside the network.

    The packet is created in the OutsideNet location and can be
    transported to input ports using inject_packet().

    Args:
        value: The value to store in the packet.

    Returns:
        The packet ID (ULID string).
    """
```

#### `create_external_packets(values) -> list[str]`

Batch version for multiple packets.

```python
def create_external_packets(self, values: list[Any]) -> list[str]:
    """Create multiple packets outside the network.

    Args:
        values: List of values to store in packets.

    Returns:
        List of packet IDs in the same order as values.
    """
```

### Category 2: Packet Injection

#### `inject_packet(packet_id, node_name, port_name)`

Transport a packet to a node's input port.

```python
def inject_packet(self, packet_id: str, node_name: str, port_name: str) -> None:
    """Transport a packet to a node's input port.

    Args:
        packet_id: The packet ID to transport.
        node_name: Target node name.
        port_name: Target input port name.

    Raises:
        InputPortNotFoundError: If the port doesn't exist.
        InputPortFullError: If the port is at capacity.
    """
```

#### `inject_data(node_name, port_name, values) -> list[str]`

Combined: create packets with values and inject them.

```python
def inject_data(
    self,
    node_name: str,
    port_name: str,
    values: list[Any]
) -> list[str]:
    """Create packets with values and inject them into a node's input port.

    This is a convenience method that combines create_external_packets()
    and inject_packet() into a single call.

    Args:
        node_name: Target node name.
        port_name: Target input port name.
        values: List of values to create packets for.

    Returns:
        List of created packet IDs.

    Example:
        packet_ids = net.inject_data("Source", "in", [
            {"id": 1, "data": "hello"},
            {"id": 2, "data": "world"},
        ])
    """
```

### Category 3: Packet Extraction

#### `get_packets_at(node_name, port_name, port_type="input") -> list[str]`

Get packet IDs at a specific port.

```python
def get_packets_at_port(
    self,
    node_name: str,
    port_name: str,
    port_type: Literal["input", "output"] = "input",
    epoch_id: str | None = None,  # Required for output ports
) -> list[str]:
    """Get packet IDs at a port.

    Args:
        node_name: The node name.
        port_name: The port name.
        port_type: "input" or "output".
        epoch_id: Required for output ports (which belong to epochs).

    Returns:
        List of packet IDs at the port.
    """
```

#### `get_packets_on_edge(source_node, source_port, target_node, target_port) -> list[str]`

Get packet IDs on a specific edge.

```python
def get_packets_on_edge(
    self,
    source_node: str,
    source_port: str,
    target_node: str,
    target_port: str,
) -> list[str]:
    """Get packet IDs on an edge.

    Args:
        source_node: Source node name.
        source_port: Source output port name.
        target_node: Target node name.
        target_port: Target input port name.

    Returns:
        List of packet IDs on the edge.
    """
```

#### `extract_packet(packet_id) -> Any`

Remove a packet from the network and return its value.

```python
def extract_packet(self, packet_id: str) -> Any:
    """Remove a packet from the network and return its value.

    The packet is consumed from netrun-sim and its value is
    removed from the PacketStore.

    Args:
        packet_id: The packet to extract.

    Returns:
        The packet's value.

    Raises:
        PacketNotFoundError: If packet doesn't exist.
        CannotMovePacketFromRunningEpochError: If packet is in a running epoch.
    """
```

#### `extract_packets_from_port(node_name, port_name, ...) -> list[tuple[str, Any]]`

Extract all packets from a port.

```python
def extract_packets_from_port(
    self,
    node_name: str,
    port_name: str,
    port_type: Literal["input", "output"] = "input",
    epoch_id: str | None = None,
) -> list[tuple[str, Any]]:
    """Extract all packets from a port.

    Removes all packets from the port and returns them with their values.

    Returns:
        List of (packet_id, value) tuples.
    """
```

### Category 4: Packet Value Access

#### `peek_packet_value(packet_id) -> Any`

Get a packet's value without consuming it.

```python
def peek_packet_value(self, packet_id: str) -> Any:
    """Get a packet's value without consuming it.

    Useful for debugging and inspection.

    Args:
        packet_id: The packet ID.

    Returns:
        The packet's value.

    Raises:
        KeyError: If packet not found in PacketStore.
    """
```

#### `get_packet_location(packet_id) -> PacketLocation`

Get where a packet currently is.

```python
def get_packet_location(self, packet_id: str) -> "netrun_sim.PacketLocation":
    """Get the current location of a packet.

    Args:
        packet_id: The packet ID.

    Returns:
        The packet's location in the network.

    Raises:
        PacketNotFoundError: If packet doesn't exist.
    """
```

### Category 5: Epoch Execution

#### `execute_epoch(epoch_id) -> NodeExecutionResult | None`

Make `_execute_epoch` public.

```python
async def execute_epoch(self, epoch_id: str) -> "NodeExecutionResult | None":
    """Execute a single startable epoch.

    This method:
    1. Checks rate limiting
    2. Starts the epoch in netsim
    3. Dispatches the node function to a worker
    4. Waits for completion
    5. Commits deferred actions (on success) or handles failure with retries

    Args:
        epoch_id: The ID of the epoch to execute.

    Returns:
        The NodeExecutionResult if execution succeeded, None if skipped.
    """
```

### Category 6: Log Access

#### `list_epoch_log_ids() -> list[str]`

Get all epoch IDs that have logs.

```python
def list_epoch_log_ids(self) -> list[str]:
    """Get all epoch IDs that have print logs.

    Returns:
        List of epoch IDs with logs.
    """
```

#### `list_node_log_names() -> list[str]`

Get all node names that have logs.

```python
def list_node_log_names(self) -> list[str]:
    """Get all node names that have print logs.

    Returns:
        List of node names with logs.
    """
```

#### `get_all_logs_chronological() -> list[tuple[datetime, str, str, str]]`

Get all logs sorted by time.

```python
def get_all_logs_chronological(self) -> list[tuple[datetime, str, str, str]]:
    """Get all print logs across all epochs, sorted by timestamp.

    Returns:
        List of (timestamp, epoch_id, node_name, message) tuples.
    """
```

### Category 7: Network State Queries

#### `list_all_packets() -> list[str]`

Get all packet IDs in the network.

```python
def list_all_packets(self) -> list[str]:
    """Get all packet IDs currently in the network.

    Returns:
        List of all packet IDs.
    """
```

#### `packet_count() -> int`

Get total packet count.

```python
def packet_count(self) -> int:
    """Get the total number of packets in the network."""
```

#### `packet_count_at_port(node_name, port_name, ...) -> int`

Get count at a specific port.

```python
def packet_count_at_port(
    self,
    node_name: str,
    port_name: str,
    port_type: Literal["input", "output"] = "input",
    epoch_id: str | None = None,
) -> int:
    """Get the number of packets at a specific port."""
```

### Category 8: Graph Queries

#### `get_edges_from_port(node_name, port_name) -> list[Edge]`

Get edges connected to an output port.

```python
def get_edges_from_port(self, node_name: str, port_name: str) -> list:
    """Get all edges connected to an output port.

    Useful for checking if an output port has downstream connections.

    Args:
        node_name: The node name.
        port_name: The output port name.

    Returns:
        List of Edge objects connected to this port.
        Empty list if port is unconnected (dangling).
    """
```

#### `has_downstream_connection(node_name, port_name) -> bool`

Check if an output port is connected.

```python
def has_downstream_connection(self, node_name: str, port_name: str) -> bool:
    """Check if an output port has any downstream connections.

    Returns False if the port is "dangling" (no edges connected).
    """
```

---

## Implementation Plan

### Phase 0: netrun-sim Changes (Priority: Critical) ✅ COMPLETED

**Prerequisite for Output Queues.** See `02_netrun_sim_changes.md`.

1. ✅ Add `PacketOrphaned` event type
2. ✅ Modify `send_output_salvo` to allow unconnected ports
3. ✅ Update `FinishedEpoch` response with orphaned packet info
4. ✅ Update Python bindings

### Phase 1: Output Queues System (Priority: High) ✅ COMPLETED

The main feature for DAG-style workflows.

**Configuration:**
1. ✅ Add `OutputQueueConfig` class
2. ✅ Add `output_queues`, `catch_all_output_queue`, `undeclared_output_behavior` to `NetConfig`

**Internal Implementation:**
3. ✅ Add `_output_queues: dict[str, asyncio.Queue]` storage
4. ✅ Add `_port_to_queue` reverse mapping
5. ✅ Implement `_route_orphaned_packet()` method
6. ✅ Hook into epoch completion to route orphaned packets

**Retrieval API:**
7. ✅ `get_output()` - async await
8. ⬜ `get_output_sync()` - sync blocking (not implemented - use `try_get_output()` instead)
9. ✅ `try_get_output()` - non-blocking
10. ✅ `get_all_outputs()` - batch retrieval
11. ✅ `has_output()` - check availability
12. ✅ `output_count()` - queue depth
13. ✅ `list_output_queues()` - list configured queues

### Phase 2: Packet Creation & Injection (Priority: High) ✅ COMPLETED

These are the most commonly needed operations based on the example.

1. ✅ `create_external_packet(value) -> str`
2. ✅ `create_external_packets(values) -> list[str]`
3. ✅ `inject_packet(packet_id, node_name, port_name)`
4. ✅ `inject_data(node_name, port_name, values) -> list[str]`

### Phase 3: Packet Extraction (Priority: Medium) (SKIP)

Direct packet extraction (lower priority now that Output Queues exist).

1. `get_packets_at_port(node_name, port_name, port_type, epoch_id)`
2. `get_packets_on_edge(...)`
3. `extract_packet(packet_id) -> Any`
4. `extract_packets_from_port(...) -> list[tuple[str, Any]]`

**Estimated effort**: Small - uses existing `netsim.get_packets_at_location()`.

### Phase 4: Value Access & Location Queries (Priority: Medium)

Useful for debugging and inspection.

1. `peek_packet_value(packet_id) -> Any`
2. `get_packet_location(packet_id) -> PacketLocation`
3. `list_all_packets() -> list[str]`
4. `packet_count() -> int`
5. `packet_count_at_port(...) -> int`

**Estimated effort**: Small.

### Phase 5: Log Access Improvements (Priority: Medium)

Make log access more convenient.

1. `list_epoch_log_ids() -> list[str]`
2. `list_node_log_names() -> list[str]`
3. `get_all_logs_chronological() -> list[...]`

**Estimated effort**: Small.

### Phase 6: Public Epoch Execution (Priority: Medium)

Make `_execute_epoch` public.

1. `execute_epoch(epoch_id)` - rename `_execute_epoch` to public

**Estimated effort**: Trivial - just rename.

### Phase 7: Graph Queries (Priority: Low)

Nice-to-have for advanced use cases.

1. `get_edges_from_port(node_name, port_name) -> list[Edge]`
2. `has_downstream_connection(node_name, port_name) -> bool`

**Estimated effort**: Small.

---

## Example: Before and After

### Before (Current)

```python
# Inject 3 data items into Source node
import netrun_sim

packet_ids = []
for data in [{"id": 0}, {"id": 1}, {"id": 2}]:
    response, _ = net._netsim.do_action(
        netrun_sim.NetAction.create_packet(None)
    )
    packet_id = response.packet_id
    packet_ids.append(packet_id)
    net._packet_store.register(packet_id, data)

for packet_id in packet_ids:
    net._netsim.do_action(
        netrun_sim.NetAction.transport_packet_to_location(
            packet_id,
            netrun_sim.PacketLocation.input_port("Source", "in"),
        )
    )

# Run network and execute epochs
await net.run_until_blocked()
for epoch_id in net.get_startable_epochs():
    await net._execute_epoch(epoch_id)

# Get logs
for epoch_id in net._epoch_print_logs.keys():
    log = net.get_epoch_log(epoch_id)

# Getting output is awkward - need to manually track packets at unconnected ports
# (Currently raises UnconnectedOutputPortError)
```

### After (With Helpers + Output Queues)

```python
# Configuration includes output queue
config = NetConfig(
    pools={...},
    graph=graph_config,
    output_queues={
        "results": OutputQueueConfig(ports=[("Sink", "out")]),
    },
)

async with Net(config) as net:
    # Inject data (one line!)
    net.inject_data("Source", "in", [
        {"id": 0}, {"id": 1}, {"id": 2}
    ])

    # Run network in background
    await net.start_background()

    # Collect results as they arrive
    results = []
    while net.has_output("results"):
        packet = await net.get_output("results", timeout=1.0)
        results.append(packet.value)
        print(f"Got: {packet.value} from {packet.from_node}.{packet.from_port}")

    # Or: get all at once
    # results = [p.value for p in net.get_all_outputs("results")]

    # Logs still available
    for epoch_id in net.list_epoch_log_ids():
        log = net.get_epoch_log(epoch_id)
```

**Improvements:**
- ~25 lines → ~15 lines for equivalent functionality
- No need to import `netrun_sim` directly
- No access to private attributes
- Clean output collection via queues
- Can await results as they arrive

---

## Open Questions

1. **Return types**: Should extraction methods return ULID objects or strings?
   - Current: Mixed (netsim returns ULID, Net uses strings)
   - Recommendation: Always return strings for consistency with existing Net API

2. **Error handling**: Should helper methods wrap netrun-sim exceptions or pass them through?
   - Recommendation: Pass through - they're well-documented and informative

3. **Async vs sync**: Should packet creation/injection be async?
   - Recommendation: Sync - these are fast local operations

4. **Naming convention**: `extract_packet` vs `remove_packet` vs `consume_packet`?
   - `consume_packet` is used in NodeExecutionContext (removes from network)
   - `extract_packet` implies getting the value out (better for external use)
   - Recommendation: Use `extract_packet` for consistency with "external" operations

---

## Notes

### On "Dangling Edges" vs "Unconnected Ports"

After investigating netrun-sim's validation (in `graph.rs`):

- **Dangling edges are NOT possible**: Every edge must connect a valid output port to a valid input port. Graph validation enforces this - edges cannot have missing endpoints.

- **Unconnected ports ARE allowed**: An output port can exist without any edge connected to it. Similarly, an input port can have no incoming edges. This is valid and passes validation.

The `has_downstream_connection()` method checks for unconnected output ports. If you try to send packets from an unconnected output port via `send_output_salvo`, netrun-sim raises `UnconnectedOutputPortError`. This helper lets you check before attempting the send.

### Other Notes

- All these methods are thin wrappers and should have minimal performance impact.
- The example notebook should be updated after implementation to use the new helpers.
