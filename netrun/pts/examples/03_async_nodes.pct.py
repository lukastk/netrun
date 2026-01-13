# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Example 03: Async Node Functions
#
# This example demonstrates async execution in `netrun`:
#
# - Using async `exec_func` for nodes
# - Mixing sync and async nodes in the same pipeline
# - Using async start/stop functions
# - Async value functions for lazy evaluation
#
# ## The Pipeline
#
# ```
# Source (async) -> Processor (sync) -> Sink (async)
# ```
#
# This demonstrates that sync and async nodes can be freely mixed.

# %%
#|default_exp 03_async_nodes

# %%
#|export
import asyncio
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
# We create a three-node pipeline with mixed sync/async nodes.

# %%
#|export
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

graph = Graph([source_node, processor_node, sink_node], edges)
print(f"Created pipeline: {list(graph.nodes().keys())}")

# %% [markdown]
# ## Part 2: Create Net with Async Node Functions
#
# We define a mix of sync and async execution functions.

# %%
#|export
net = Net(
    graph,
    consumed_packet_storage=True,
    on_error="raise",
)

# Storage for results and logs
results = []
execution_log = []

# Async Source: simulates fetching data from an async API
async def source_exec(ctx, packets):
    """Async source that simulates API calls."""
    execution_log.append("Source: starting async data fetch")

    # Simulate async operations (like HTTP requests)
    for i in range(3):
        await asyncio.sleep(0.01)  # Simulate network delay

        # Create packet with fetched data
        data = {
            "id": i,
            "fetched_at": f"timestamp_{i}",
            "value": f"async_data_{i}"
        }
        pkt = ctx.create_packet(data)
        ctx.load_output_port("out", pkt)
        ctx.send_output_salvo("send")

        execution_log.append(f"Source: fetched and sent item {i}")

    execution_log.append("Source: completed async fetch")

# Sync Processor: demonstrates mixing with sync nodes
def processor_exec(ctx, packets):
    """Sync processor that transforms data."""
    execution_log.append("Processor: starting sync processing")

    for port_name, pkts in packets.items():
        for pkt in pkts:
            value = ctx.consume_packet(pkt)

            # Transform the data
            transformed = {
                **value,
                "processed": True,
                "value": value["value"].upper()
            }

            execution_log.append(f"Processor: transformed {value['id']}")

            out_pkt = ctx.create_packet(transformed)
            ctx.load_output_port("out", out_pkt)
            ctx.send_output_salvo("send")

# Async Sink: simulates async storage
async def sink_exec(ctx, packets):
    """Async sink that simulates database writes."""
    execution_log.append("Sink: starting async storage")

    for port_name, pkts in packets.items():
        for pkt in pkts:
            value = ctx.consume_packet(pkt)

            # Simulate async database write
            await asyncio.sleep(0.01)

            results.append(value)
            execution_log.append(f"Sink: stored item {value['id']}")

    execution_log.append("Sink: completed async storage")

# Async start/stop functions
async def source_start(net):
    """Async initialization for Source node."""
    execution_log.append("Source: async start - initializing connection")
    await asyncio.sleep(0.01)  # Simulate connection setup

async def source_stop(net):
    """Async cleanup for Source node."""
    execution_log.append("Source: async stop - closing connection")
    await asyncio.sleep(0.01)  # Simulate connection cleanup

# Register execution functions
net.set_node_exec(
    "Source",
    source_exec,
    start_func=source_start,
    stop_func=source_stop
)
net.set_node_exec("Processor", processor_exec)
net.set_node_exec("Sink", sink_exec)

print("Registered mixed sync/async execution functions")

# %% [markdown]
# ## Part 3: Run the Pipeline Asynchronously
#
# We use `async_start()` to run the network with proper async support.

# %%
#|export
async def run_async_pipeline():
    """Run the pipeline asynchronously."""
    global results, execution_log

    # Clear previous results
    results.clear()
    execution_log.clear()

    # Inject source epoch
    source_epoch = net.inject_source_epoch("Source")
    print(f"Injected Source epoch: {source_epoch[:8]}...")

    # Run the network asynchronously
    print("\nStarting async network execution...\n")
    await net.async_start()

    print("\n--- Execution Log ---")
    for entry in execution_log:
        print(f"  {entry}")

    print("\n--- Results ---")
    for r in results:
        print(f"  {r}")

    print(f"\nTotal results: {len(results)}")
    print(f"Net state: {net.state}")

# Run the async pipeline
asyncio.run(run_async_pipeline())

# %% [markdown]
# ## Part 4: Demonstrate Async Value Functions
#
# Packets can have async value functions for lazy evaluation.

# %%
#|export
# Create a fresh net for this demonstration
net2 = Net(
    graph,
    consumed_packet_storage=True,
    on_error="raise",
)

results2 = []

async def source_with_async_value(ctx, packets):
    """Source that creates packets with async value functions."""
    for i in range(2):
        # Create an async value function
        async def make_async_value(idx=i):
            await asyncio.sleep(0.01)  # Simulate async computation
            return {"computed_async": True, "index": idx}

        pkt = ctx.create_packet_from_value_func(make_async_value)
        ctx.load_output_port("out", pkt)
        ctx.send_output_salvo("send")

async def processor2(ctx, packets):
    """Async processor for async value functions."""
    for port_name, pkts in packets.items():
        for pkt in pkts:
            # Must use async_consume_packet for async value functions
            value = await ctx.async_consume_packet(pkt)
            out_pkt = ctx.create_packet({**value, "processed": True})
            ctx.load_output_port("out", out_pkt)
            ctx.send_output_salvo("send")

async def sink_with_async_consume(ctx, packets):
    """Sink that uses async_consume_packet for async value functions."""
    for port_name, pkts in packets.items():
        for pkt in pkts:
            # Use async_consume_packet for async value functions
            value = await ctx.async_consume_packet(pkt)
            results2.append(value)

net2.set_node_exec("Source", source_with_async_value)
net2.set_node_exec("Processor", processor2)
net2.set_node_exec("Sink", sink_with_async_consume)

async def run_async_value_demo():
    """Demonstrate async value functions."""
    net2.inject_source_epoch("Source")
    await net2.async_start()

    print("\n--- Async Value Function Results ---")
    for r in results2:
        print(f"  {r}")

    assert all(r.get("computed_async") for r in results2)
    print("\nAll values were computed asynchronously!")

asyncio.run(run_async_value_demo())

# %% [markdown]
# ## Summary
#
# This example demonstrated:
#
# 1. **Async Execution Functions**: Using `async def` for node `exec_func`
# 2. **Mixed Sync/Async**: Combining sync and async nodes in the same pipeline
# 3. **Async Start/Stop**: Using async `start_func` and `stop_func`
# 4. **Async Net Methods**: Using `async_start()` instead of `start()`
# 5. **Async Value Functions**: Creating packets with lazy async computation
# 6. **Async Consume**: Using `ctx.async_consume_packet()` for async values
#
# Key points:
# - Use `async_start()` when any node has async functions
# - Sync and async nodes can be freely mixed
# - Async start/stop functions are properly awaited
# - Async value functions are awaited when consumed
