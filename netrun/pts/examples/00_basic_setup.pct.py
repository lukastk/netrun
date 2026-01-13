# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Example 00: Basic Setup
#
# This example demonstrates the foundational components of `netrun`:
#
# - Creating a network graph with nodes and edges
# - Creating a `Net` instance
# - Configuring nodes with execution functions
# - Setting node configuration options
# - Using the `PacketValueStore` for packet value management
#
# ## What This Example Shows
#
# 1. **Graph Creation**: How to define nodes with input/output ports and salvo conditions
# 2. **Net Construction**: Creating a Net from a graph with various configuration options
# 3. **Node Configuration**: Using `set_node_exec()` and `set_node_config()` to configure nodes
# 4. **Value Storage**: Direct usage of `PacketValueStore` for storing and retrieving values
#
# ## Note
#
# This example only demonstrates setup and configuration. Actual execution of the network
# (running nodes, flowing packets) will be shown in later examples after Milestone 3 is complete.

# %%
#|default_exp 00_basic_setup

# %%
#|export
from netrun import (
    # Graph building
    Graph,
    Node,
    Edge,
    Port,
    PortType,
    PortRef,
    PortState,
    MaxSalvos,
    SalvoCondition,
    SalvoConditionTerm,
    # Net and configuration
    Net,
    NetState,
    NodeConfig,
    # Value storage
    PacketValueStore,
    # Error types
    NetrunRuntimeError,
    PacketTypeMismatch,
)

# %% [markdown]
# ## Part 1: Creating a Graph
#
# We'll create a simple three-node graph: Source -> Processor -> Sink

# %%
#|export
# Define a Source node with one output port
source_node = Node(
    name="Source",
    out_ports={"out": Port()},
    out_salvo_conditions={
        "send": SalvoCondition(
            MaxSalvos.infinite(),
            "out",
            SalvoConditionTerm.port("out", PortState.non_empty())
        )
    }
)

# Define a Processor node with input and output ports
processor_node = Node(
    name="Processor",
    in_ports={"in": Port()},
    out_ports={"out": Port()},
    in_salvo_conditions={
        "receive": SalvoCondition(
            MaxSalvos.finite(1),
            "in",
            SalvoConditionTerm.port("in", PortState.non_empty())
        )
    },
    out_salvo_conditions={
        "send": SalvoCondition(
            MaxSalvos.infinite(),
            "out",
            SalvoConditionTerm.port("out", PortState.non_empty())
        )
    }
)

# Define a Sink node with one input port
sink_node = Node(
    name="Sink",
    in_ports={"in": Port()},
    in_salvo_conditions={
        "receive": SalvoCondition(
            MaxSalvos.finite(1),
            "in",
            SalvoConditionTerm.port("in", PortState.non_empty())
        )
    }
)

# Connect the nodes with edges
edges = [
    Edge(
        PortRef("Source", PortType.Output, "out"),
        PortRef("Processor", PortType.Input, "in")
    ),
    Edge(
        PortRef("Processor", PortType.Output, "out"),
        PortRef("Sink", PortType.Input, "in")
    ),
]

# Create the graph
graph = Graph([source_node, processor_node, sink_node], edges)
print(f"Created graph: {graph}")
print(f"  Nodes: {list(graph.nodes().keys())}")

# %% [markdown]
# ## Part 2: Creating a Net
#
# The `Net` class wraps the graph and provides the runtime for executing nodes.

# %%
#|export
net = Net(
    graph,
    # Enable consumed packet storage (keep values after consumption)
    consumed_packet_storage=True,
    consumed_packet_storage_limit=100,
    # Error handling mode
    on_error="pause",
)

print(f"Created Net with state: {net.state}")
print(f"  on_error mode: {net._on_error}")

# %% [markdown]
# ## Part 3: Setting Node Execution Functions
#
# Each node can have execution functions that define its behavior.

# %%
#|export
# Define execution functions (these won't run until Milestone 3)
def source_exec(ctx, packets):
    """Source node: generates data."""
    print(f"[Source] Executing epoch {ctx.epoch_id}")

def processor_exec(ctx, packets):
    """Processor node: transforms data."""
    print(f"[Processor] Executing epoch {ctx.epoch_id}")

def sink_exec(ctx, packets):
    """Sink node: consumes data."""
    print(f"[Sink] Executing epoch {ctx.epoch_id}")

def processor_start(net):
    """Called when net starts - initialize processor state."""
    print("[Processor] Starting up...")

def processor_stop(net):
    """Called when net stops - cleanup processor state."""
    print("[Processor] Shutting down...")

# Register execution functions
net.set_node_exec("Source", source_exec)
net.set_node_exec(
    "Processor",
    processor_exec,
    start_func=processor_start,
    stop_func=processor_stop
)
net.set_node_exec("Sink", sink_exec)

print("Registered execution functions for all nodes")

# Verify registration
for node_name in ["Source", "Processor", "Sink"]:
    funcs = net.get_node_exec_funcs(node_name)
    print(f"  {node_name}: exec_func={funcs.exec_func.__name__ if funcs.exec_func else None}")

# %% [markdown]
# ## Part 4: Setting Node Configuration
#
# Nodes can be configured with various options for retries, timeouts, etc.

# %%
#|export
# Configure the Processor with retries and deferred actions
net.set_node_config(
    "Processor",
    retries=3,
    defer_net_actions=True,  # Required when retries > 0
    retry_wait=0.5,
    timeout=30.0,
)

# Configure the Sink
net.set_node_config(
    "Sink",
    capture_stdout=True,
    echo_stdout=True,
)

# Show configurations
for node_name in ["Source", "Processor", "Sink"]:
    config = net.get_node_config(node_name)
    print(f"{node_name} config:")
    print(f"  retries: {config.retries}")
    print(f"  defer_net_actions: {config.defer_net_actions}")
    print(f"  timeout: {config.timeout}")

# %% [markdown]
# ## Part 5: Using PacketValueStore
#
# The `PacketValueStore` manages packet values separately from packet tracking.

# %%
#|export
# Create a standalone value store for demonstration
store = PacketValueStore(
    consumed_storage=True,
    consumed_storage_limit=5
)

# Store direct values
store.store_value("packet-001", {"type": "data", "value": 42})
store.store_value("packet-002", [1, 2, 3, 4, 5])

# Store a value function (lazy evaluation)
computation_count = [0]

def compute_value():
    computation_count[0] += 1
    return f"computed-result-{computation_count[0]}"

store.store_value_func("packet-003", compute_value)

print("Stored values:")
print(f"  packet-001: {store.get_value('packet-001')}")
print(f"  packet-002: {store.get_value('packet-002')}")

# Value function is called each time
print(f"\nValue function (first call): {store.get_value('packet-003')}")
print(f"Value function (second call): {store.get_value('packet-003')}")
print(f"  (Function was called {computation_count[0]} times)")

# Consume a value
consumed = store.consume("packet-001")
print(f"\nConsumed packet-001: {consumed}")
print(f"  In consumed storage: {store.get_consumed_value('packet-001')}")
print(f"  Still in active storage: {store.has_value('packet-001')}")

# %% [markdown]
# ## Part 6: Net State and Wrapper Methods
#
# The Net provides wrapper methods to access network state without exposing NetSim.

# %%
#|export
print(f"Net state: {net.state}")

# These methods wrap the internal NetSim
print(f"Startable epochs: {net.get_startable_epochs()}")
print(f"Startable epochs for 'Processor': {net.get_startable_epochs_by_node('Processor')}")

# Demonstrate state transitions
net.pause()
print(f"After pause(): {net.state}")

net.stop()
print(f"After stop(): {net.state}")

# %% [markdown]
# ## Summary
#
# This example demonstrated:
# - Creating a graph with nodes, ports, and edges
# - Creating a Net from the graph
# - Setting node execution functions
# - Configuring node options
# - Using PacketValueStore for value management
# - Accessing net state and wrapper methods
#
# In later examples (after Milestone 3), we'll see how to actually run the network
# and flow packets through nodes.
