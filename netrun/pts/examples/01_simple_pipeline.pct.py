# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Example 01: Simple Pipeline Execution
#
# This example demonstrates how to execute a simple data pipeline in `netrun`:
#
# - Creating a Source -> Processor -> Sink pipeline
# - Setting up execution functions for each node
# - Running the network and observing packet flow
# - Using deferred actions in the Processor node
#
# ## The Pipeline
#
# ```
# Source -> Processor -> Sink
# ```
#
# - **Source**: Generates data packets
# - **Processor**: Transforms incoming data (uppercase)
# - **Sink**: Collects and stores the results

# %%
#|default_exp 01_simple_pipeline

# %%
#|export
from netrun.core import (
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
)

# %% [markdown]
# ## Part 1: Define the Graph
#
# We create a three-node pipeline: Source -> Processor -> Sink

# %%
#|export
# Define Source node - generates data
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

# Define Processor node - transforms data
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

# Define Sink node - collects results
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

# Connect the nodes
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
print(f"Created pipeline: {list(graph.nodes().keys())}")

# %% [markdown]
# ## Part 2: Create the Net and Set Up Execution Functions
#
# Each node needs an execution function that defines its behavior.

# %%
#|export
# Create the Net with consumed packet storage enabled
net = Net(
    graph,
    consumed_packet_storage=True,
    consumed_packet_storage_limit=100,
    on_error="raise",  # Raise exceptions on error
)

# Storage for results
results = []
execution_log = []

# Source: generates data packets
def source_exec(ctx, packets):
    """Generate data packets."""
    execution_log.append(f"Source executing (epoch {ctx.epoch_id[:8]}...)")

    # Create and send multiple data packets
    for i, message in enumerate(["hello", "world", "from", "netrun"]):
        pkt = ctx.create_packet({"index": i, "message": message})
        ctx.load_output_port("out", pkt)
        ctx.send_output_salvo("send")

    execution_log.append(f"Source sent 4 packets")

# Processor: transforms data (uppercase messages)
def processor_exec(ctx, packets):
    """Transform incoming data."""
    execution_log.append(f"Processor executing (epoch {ctx.epoch_id[:8]}...)")

    for port_name, pkts in packets.items():
        for pkt in pkts:
            # Get the input value
            value = ctx.consume_packet(pkt)

            # Transform: uppercase the message
            transformed = {
                "index": value["index"],
                "message": value["message"].upper(),
                "processed": True
            }

            execution_log.append(f"  Transformed: {value['message']} -> {transformed['message']}")

            # Create output packet and send
            out_pkt = ctx.create_packet(transformed)
            ctx.load_output_port("out", out_pkt)
            ctx.send_output_salvo("send")

# Sink: collects results
def sink_exec(ctx, packets):
    """Collect and store results."""
    execution_log.append(f"Sink executing (epoch {ctx.epoch_id[:8]}...)")

    for port_name, pkts in packets.items():
        for pkt in pkts:
            value = ctx.consume_packet(pkt)
            results.append(value)
            execution_log.append(f"  Collected: {value}")

# Register execution functions
net.set_node_exec("Source", source_exec)
net.set_node_exec("Processor", processor_exec)
net.set_node_exec("Sink", sink_exec)

print("Execution functions registered")

# %% [markdown]
# ## Part 3: Run the Pipeline
#
# We inject an epoch for the Source node (since it has no inputs),
# then run the network until completion.

# %%
#|export
# Clear any previous results
results.clear()
execution_log.clear()

# Inject a Source epoch to kick off the pipeline
source_epoch = net.inject_source_epoch("Source")
print(f"Injected Source epoch: {source_epoch[:8]}...")

# Run the network
print("\nStarting network execution...\n")
net.start()

print("\n--- Execution Log ---")
for entry in execution_log:
    print(entry)

print("\n--- Results ---")
for result in results:
    print(f"  {result}")

print(f"\nTotal results collected: {len(results)}")
print(f"Net state after execution: {net.state}")

# %% [markdown]
# ## Part 4: Verify the Results
#
# Let's verify that all data was processed correctly.

# %%
#|export
# Verify results
expected_messages = ["HELLO", "WORLD", "FROM", "NETRUN"]
actual_messages = [r["message"] for r in results]

print("Verification:")
print(f"  Expected: {expected_messages}")
print(f"  Actual:   {actual_messages}")
print(f"  Match: {expected_messages == actual_messages}")

# All results should have 'processed' flag
all_processed = all(r.get("processed", False) for r in results)
print(f"  All processed: {all_processed}")

# %% [markdown]
# ## Summary
#
# This example demonstrated:
#
# 1. **Pipeline Creation**: Building a Source -> Processor -> Sink graph
# 2. **Execution Functions**: Defining node behavior with `exec_func`
# 3. **Packet Creation**: Using `ctx.create_packet()` to create data
# 4. **Packet Consumption**: Using `ctx.consume_packet()` to read data
# 5. **Output Sending**: Using `ctx.load_output_port()` and `ctx.send_output_salvo()`
# 6. **Running the Network**: Using `inject_source_epoch()` and `start()`
#
# Key points:
# - Source nodes need manual epoch injection via `inject_source_epoch()`
# - The network runs until fully blocked (no more executable epochs)
# - Each packet flows through the pipeline: Source -> Processor -> Sink
