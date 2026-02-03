# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

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
import asyncio
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
# - `ctx.print()` - Captured print output
# - `ctx.consume_packet(packet_id)` - Get packet value and remove it
# - `ctx.create_packet(value)` - Create a new packet with a value
# - `ctx.load_output_port(port, packet_id)` - Load packet into output port
# - `ctx.send_output_salvo(condition)` - Send packets downstream

# %%
def source_node(ctx: NodeExecutionContext, packets: dict) -> None:
    """Source node that generates initial data.

    This node creates output packets to start the flow.
    """
    ctx.print(f"[{ctx.node_name}] Starting source node")

    # Create some data to send downstream
    for i in range(3):
        value = {"id": i, "data": f"item_{i}"}
        packet_id = ctx.create_packet(value)
        ctx.print(f"[{ctx.node_name}] Created packet {i}: {value}")
        ctx.load_output_port("out", packet_id)

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
            # Source node - output only
            NodeGraphConfig(
                name="Source",
                out_ports={"out": PortConfig()},
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
async def demonstrate_lifecycle():
    """Demonstrate Net lifecycle operations."""

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

# Run the demonstration
asyncio.run(demonstrate_lifecycle())

# %% [markdown]
# ## Print Capture
#
# Node functions can use `ctx.print()` to capture output. This output is:
# - Buffered and periodically flushed back to the Net
# - Stored per-epoch and per-node for later retrieval
# - Optionally echoed to stdout for debugging

# %%
async def demonstrate_print_capture():
    """Demonstrate how print capture works."""

    config = create_net_config()

    async with Net(config) as net:
        # Manually call _handle_print_buffer to simulate receiving prints
        # (In normal operation, this happens automatically when workers send prints)
        net._handle_print_buffer("demo_epoch", [
            "Line 1: Starting processing\n",
            "Line 2: Processing data\n",
            "Line 3: Complete\n",
        ])

        # Retrieve the logs
        epoch_log = net.get_epoch_log("demo_epoch")
        print(f"Epoch log has {len(epoch_log)} entries:")
        for timestamp, line in epoch_log:
            print(f"  [{timestamp.strftime('%H:%M:%S')}] {line.strip()}")

asyncio.run(demonstrate_print_capture())

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
# 6. **Print Capture**: How `ctx.print()` output is captured and retrieved
# 7. **Rate Limiting**: Controlling epoch start frequency
#
# The Net class orchestrates the flow-based execution by:
# - Using `netrun-sim` for packet flow simulation
# - Dispatching node functions to worker pools via `ExecutionManager`
# - Managing packet values in `PacketStore`
# - Handling errors, retries, and print capture
