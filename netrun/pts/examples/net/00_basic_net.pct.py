# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %% [markdown]
# # Basic Net Example
#
# This notebook demonstrates how to use the `Net` class to orchestrate flow-based
# network execution. The Net class bridges `netrun-sim` (packet flow simulation)
# with actual node function execution via worker pools.
#
# We'll create a simple linear network: **Source → Process → Sink**

# %% [markdown]
# ## Setup
#
# First, let's import all the types we need.

# %%
import netrun_sim
from netrun.net.config import (
    # Net configuration
    NetConfig,
    GraphConfig,
    NodeGraphConfig,
    NodeExecutionConfig,
    PoolConfig,
    # Pool types
    MainPoolConfig,
    ThreadPoolConfig,
    MultiprocessPoolConfig,
    # Port and edge configuration
    PortConfig,
    EdgeConfig,
    # Salvo conditions
    SalvoConditionConfig,
    SalvoConditionTermTrueConfig,
    SalvoConditionTermPortConfig,
    MaxSalvosFiniteConfig,
    PacketCountAllConfig,
    PortStateNonEmptyConfig,
)
from netrun.net._net import Net, NodeExecutionContext
from netrun.execution_manager import RunAllocationMethod

# %% [markdown]
# ## Defining Node Functions
#
# Each node can have an `exec_node_func` that processes packets. The function
# receives a `NodeExecutionContext` and a dictionary of input packets.
#
# The context provides methods for:
# - `ctx.print()` - Captured print output (with automatic timestamps)
# - `ctx.consume_packet(packet_id)` - Get packet value and remove it
# - `ctx.create_packet(value)` - Create a new packet with a value
# - `ctx.load_output_port(port, packet_id)` - Load packet into output port
# - `ctx.send_output_salvo(condition)` - Send packets downstream

# %%
def source_node(ctx: NodeExecutionContext, packets: dict) -> None:
    """Source node that receives initial data and passes it downstream.

    This node consumes input packets and forwards them to the next node.
    """
    ctx.print(f"[{ctx.node_name}] Starting source node")

    # Get input packets from the "in" port
    input_packet_ids = packets.get("in", [])
    ctx.print(f"[{ctx.node_name}] Received {len(input_packet_ids)} input packets")

    for packet_id in input_packet_ids:
        # Consume the input packet to get its value
        value = ctx.consume_packet(packet_id)
        ctx.print(f"[{ctx.node_name}] Processing input: {value}")

        # Create output packet and forward downstream
        out_packet_id = ctx.create_packet(value)
        ctx.load_output_port("out", out_packet_id)

    # Send all packets downstream
    ctx.send_output_salvo("send")
    ctx.print(f"[{ctx.node_name}] Sent output salvo")


def process_node(ctx: NodeExecutionContext, packets: dict) -> None:
    """Processing node that transforms data.

    This node consumes input packets, processes them, and produces output.
    """
    ctx.print(f"[{ctx.node_name}] Processing started (retry #{ctx.retry_count})")

    # Get input packets from the "in" port
    input_packet_ids = packets.get("in", [])
    ctx.print(f"[{ctx.node_name}] Received {len(input_packet_ids)} packets")

    for packet_id in input_packet_ids:
        # Consume the input packet to get its value
        value = ctx.consume_packet(packet_id)
        ctx.print(f"[{ctx.node_name}] Processing: {value}")

        # Transform the data
        processed = {
            "id": value["id"],
            "data": value["data"].upper(),
            "processed": True,
        }

        # Create output packet and send downstream
        out_packet_id = ctx.create_packet(processed)
        ctx.load_output_port("out", out_packet_id)

    ctx.send_output_salvo("send")
    ctx.print(f"[{ctx.node_name}] Processing complete")


def sink_node(ctx: NodeExecutionContext, packets: dict) -> None:
    """Sink node that collects final results.

    This node consumes packets and stores the results.
    """
    ctx.print(f"[{ctx.node_name}] Collecting results")

    input_packet_ids = packets.get("in", [])
    results = []

    for packet_id in input_packet_ids:
        value = ctx.consume_packet(packet_id)
        results.append(value)
        ctx.print(f"[{ctx.node_name}] Collected: {value}")

    ctx.print(f"[{ctx.node_name}] Total collected: {len(results)} items")

# %% [markdown]
# ## Creating the Graph Configuration
#
# The graph defines:
# - **Nodes**: With input/output ports and salvo conditions
# - **Edges**: Connections between node ports
# - **Execution configs**: How each node should be executed

# %%
def create_graph_config() -> GraphConfig:
    """Create the graph configuration for our linear network."""

    return GraphConfig(
        nodes=[
            # Source node - input and output
            # Triggered when packets arrive at its input port
            NodeGraphConfig(
                name="Source",
                in_ports={"in": PortConfig()},
                out_ports={"out": PortConfig()},
                in_salvo_conditions={
                    # Trigger when input port has packets
                    "trigger": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"in": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(
                            port_name="in",
                            state=PortStateNonEmptyConfig(),
                        ),
                    ),
                },
                out_salvo_conditions={
                    "send": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"out": PacketCountAllConfig()},
                        term=SalvoConditionTermTrueConfig(),
                    ),
                },
                execution_config=NodeExecutionConfig(
                    node_name="Source",
                    pools=["main_pool"],
                    exec_node_func=source_node,
                ),
            ),

            # Process node - input and output
            NodeGraphConfig(
                name="Process",
                in_ports={"in": PortConfig()},
                out_ports={"out": PortConfig()},
                in_salvo_conditions={
                    "trigger": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"in": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(
                            port_name="in",
                            state=PortStateNonEmptyConfig(),
                        ),
                    ),
                },
                out_salvo_conditions={
                    "send": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"out": PacketCountAllConfig()},
                        term=SalvoConditionTermTrueConfig(),
                    ),
                },
                execution_config=NodeExecutionConfig(
                    node_name="Process",
                    pools=["thread_pool"],
                    exec_node_func=process_node,
                    retries=2,
                    retry_wait=0.1,
                    print_flush_interval=0.05,
                ),
            ),

            # Sink node - input only
            NodeGraphConfig(
                name="Sink",
                in_ports={"in": PortConfig()},
                in_salvo_conditions={
                    "trigger": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"in": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(
                            port_name="in",
                            state=PortStateNonEmptyConfig(),
                        ),
                    ),
                },
                execution_config=NodeExecutionConfig(
                    node_name="Sink",
                    pools=["main_pool"],
                    exec_node_func=sink_node,
                    print_echo_stdout=True,  # Echo prints to stdout for debugging
                ),
            ),
        ],
        edges=[
            EdgeConfig(source_str="Source.out", target_str="Process.in"),
            EdgeConfig(source_str="Process.out", target_str="Sink.in"),
        ],
    )

# %% [markdown]
# ## Configuring Pools
#
# The Net uses different pool types for executing node functions:
#
# - **MainPoolConfig**: Runs in the main async event loop (single worker)
# - **ThreadPoolConfig**: Multiple worker threads in the same process
# - **MultiprocessPoolConfig**: Multiple subprocesses with worker threads
# - **RemotePoolConfig**: Network-based workers via WebSockets

# %%
def create_net_config() -> NetConfig:
    """Create the complete Net configuration."""

    return NetConfig(
        pools={
            # Main pool - for lightweight async operations
            "main_pool": PoolConfig(
                id="main_pool",
                spec=MainPoolConfig(),
            ),

            # Thread pool - for CPU-bound work without GIL issues
            "thread_pool": PoolConfig(
                id="thread_pool",
                spec=ThreadPoolConfig(num_workers=4),
            ),

            # Multiprocess pool - for heavy CPU-bound work
            # Uncomment to use:
            # "mp_pool": PoolConfig(
            #     id="mp_pool",
            #     spec=MultiprocessPoolConfig(
            #         num_processes=2,
            #         threads_per_process=2,
            #     ),
            # ),
        },
        graph=create_graph_config(),
        default_pool_allocation_method=RunAllocationMethod.ROUND_ROBIN,
    )

# %% [markdown]
# ## Creating and Running the Net
#
# Now we can create the Net instance and explore its properties.

# %%
# Create the configuration
config = create_net_config()

# Create the Net instance
net = Net(config)

print("Net created successfully!")
print(f"Graph nodes: {list(net.graph.nodes().keys())}")
print(f"Graph edges: {len(net.graph.edges())}")

# %%
# Check the validation
errors = net.graph.validate()
if errors:
    print(f"Validation errors: {errors}")
else:
    print("Graph is valid!")

# %% [markdown]
# ## Net Lifecycle
#
# The Net supports the following lifecycle operations:
#
# - `start()` / `stop()` - Start and stop the execution pools
# - `pause()` / `resume()` - Pause/resume epoch processing
# - `run_step()` - Execute one simulation step
# - `run_until_blocked()` - Run until no more progress can be made

# %%
# Create a fresh Net and demonstrate lifecycle
config = create_net_config()

# Using context manager for automatic start/stop
async with Net(config) as net:
    print(f"Net started: {net.started}")
    print(f"Net paused: {net.paused}")

    # Run a simulation step
    made_progress, events = await net.run_step()
    print(f"Run step - made progress: {made_progress}, events: {len(events)}")

    # Check for startable epochs
    startable = net.get_startable_epochs()
    print(f"Startable epochs: {len(startable)}")

    # Pause and resume
    await net.pause()
    print(f"Paused: {net.paused}")

    await net.resume()
    print(f"Resumed: {net.paused}")

print(f"Net stopped: {not net.started}")

# %% [markdown]
# ## Full Network Execution with Print Capture
#
# Now let's run the complete network and see the print capture in action.
# We'll create packets outside the network and inject them into the Source node.
#
# **Key steps:**
# 1. Create packets outside the network using `NetAction.create_packet(None)`
# 2. Store packet values in the Net's PacketStore
# 3. Transport packets to Source's input port using `transport_packet_to_location`
# 4. Run simulation - this triggers the Source epoch automatically
# 5. Execute epochs and watch print capture in action

# %%
import asyncio

async def run_full_network():
    """Run the complete Source → Process → Sink network."""
    config = create_net_config()

    async with Net(config) as net:
        print("Starting network execution...")
        print(f"Nodes: {list(net.graph.nodes().keys())}")
        print()

        # Step 1: Create packets OUTSIDE the network
        # These represent external data being injected into the flow
        print("Creating external packets...")
        external_data = [
            {"id": 0, "data": "item_0"},
            {"id": 1, "data": "item_1"},
            {"id": 2, "data": "item_2"},
        ]

        packet_ids = []
        for data in external_data:
            # Create packet outside any epoch (epoch_id=None)
            response, _ = net._netsim.do_action(
                netrun_sim.NetAction.create_packet(None)
            )
            packet_id = response.packet_id
            packet_ids.append(packet_id)

            # Store the packet value in the Net's packet store
            net._packet_store.register(packet_id, data)
            print(f"  Created packet {packet_id[:12]}... with value: {data}")

        print()

        # Step 2: Transport packets to Source's input port
        # This simulates external data arriving at the network entry point
        print("Transporting packets to Source input port...")
        for packet_id in packet_ids:
            net._netsim.do_action(
                netrun_sim.NetAction.transport_packet_to_location(
                    packet_id,
                    netrun_sim.PacketLocation.input_port("Source", "in"),
                )
            )
        print(f"  Transported {len(packet_ids)} packets to Source.in")

        # Step 3: Run simulation - this triggers the Source epoch automatically
        # because Source's input salvo condition is satisfied (in port non-empty)
        print("Running simulation to trigger Source epoch...")
        await net.run_until_blocked()

        # Get and execute the Source epoch
        startable = net.get_startable_epochs()
        if startable:
            source_epoch_id = startable[0]
            print(f"Executing Source epoch: {str(source_epoch_id)[:12]}...")
            await net._execute_epoch(source_epoch_id)
            print("Source epoch executed")

        # Run simulation to move packets through the network
        await net.run_until_blocked()
        print("Packets moved to Process node")

        # Get and execute the Process epoch
        startable = net.get_startable_epochs()
        if startable:
            process_epoch_id = startable[0]
            print(f"Executing Process epoch: {str(process_epoch_id)[:12]}...")
            await net._execute_epoch(process_epoch_id)

        # Run simulation again
        await net.run_until_blocked()
        print("Packets moved to Sink node")

        # Get and execute the Sink epoch
        startable = net.get_startable_epochs()
        if startable:
            sink_epoch_id = startable[0]
            print(f"Executing Sink epoch: {str(sink_epoch_id)[:12]}...")
            await net._execute_epoch(sink_epoch_id)

        print()
        print("=" * 70)
        print("CAPTURED PRINT LOGS")
        print("=" * 70)

        # Display logs for each epoch
        for epoch_id in net._epoch_print_logs.keys():
            epoch_log = net.get_epoch_log(epoch_id)
            print(f"\n--- Epoch {str(epoch_id)[:12]}... ({len(epoch_log)} lines) ---")
            for timestamp, line in epoch_log:
                print(f"  [{timestamp.strftime('%H:%M:%S.%f')[:-3]}] {line.strip()}")

        return net

# Run the network
demo_net = await run_full_network()

# %% [markdown]
# ## Rate Limiting
#
# Nodes can be rate-limited to control how many epochs start per second.
# This is useful for controlling API calls or resource-intensive operations.

# %%
def create_rate_limited_config() -> NetConfig:
    """Create a config with rate-limited nodes."""

    return NetConfig(
        pools={
            "main_pool": PoolConfig(id="main_pool", spec=MainPoolConfig()),
        },
        graph=GraphConfig(
            nodes=[
                NodeGraphConfig(
                    name="RateLimited",
                    in_ports={"in": PortConfig()},
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={"in": PacketCountAllConfig()},
                            term=SalvoConditionTermTrueConfig(),
                        ),
                    },
                    execution_config=NodeExecutionConfig(
                        node_name="RateLimited",
                        rate_limit_per_second=5,  # Max 5 epochs per second
                    ),
                ),
            ],
            edges=[],
        ),
    )

# Demonstrate rate limiting
config = create_rate_limited_config()
net = Net(config)

# Check rate limiting behavior
results = []
for i in range(7):
    allowed = net._check_rate_limit("RateLimited")
    results.append(allowed)

print(f"Rate limit results (limit=5): {results}")
print(f"First 5 allowed: {all(results[:5])}")
print(f"6th and 7th blocked: {not any(results[5:])}")

# %% [markdown]
# ## Summary
#
# This example demonstrated:
#
# 1. **Node Functions**: How to define `exec_node_func` with `NodeExecutionContext`
# 2. **Graph Configuration**: Defining nodes, ports, edges, and salvo conditions
# 3. **Pool Configuration**: Setting up different pool types (Main, Thread, Multiprocess)
# 4. **Net Lifecycle**: Using `start()`, `stop()`, `pause()`, `resume()`, context manager
# 5. **Simulation**: Using `run_step()` and `run_until_blocked()`
# 6. **Print Capture**: How `ctx.print()` captures output with automatic timestamps
# 7. **Viewing Logs**: Retrieving print logs by epoch or chronologically
# 8. **Rate Limiting**: Controlling epoch start frequency
#
# The Net class orchestrates the flow-based execution by:
# - Using `netrun-sim` for packet flow simulation
# - Dispatching node functions to worker pools via `ExecutionManager`
# - Managing packet values in `PacketStore`
# - Handling errors, retries, and print capture
