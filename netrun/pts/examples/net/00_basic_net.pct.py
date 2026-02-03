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
from datetime import datetime
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
# ## Executing Epochs
#
# The Net can execute startable epochs by dispatching node functions to worker
# pools. This section demonstrates the full execution flow.
#
# **How it works:**
# 1. `run_step()` moves packets through the network and triggers epochs
# 2. `execute_startable_epochs()` dispatches node functions to workers
# 3. Node functions use `ctx.print()` to capture timestamped output
# 4. Results (including prints) are returned and committed to the network

# %%
# Create a simple source-only config for demonstration
def create_source_only_config() -> NetConfig:
    """Create a config with just a source node for demonstration."""

    def demo_source_node(ctx: NodeExecutionContext, packets: dict) -> None:
        """Source node that creates output packets and prints progress."""
        ctx.print(f"[{ctx.node_name}] Starting execution")
        ctx.print(f"[{ctx.node_name}] Epoch ID: {ctx.epoch_id}")

        # Create some packets
        for i in range(3):
            value = {"id": i, "data": f"item_{i}"}
            packet_id = ctx.create_packet(value)
            ctx.print(f"[{ctx.node_name}] Created packet: {value}")
            ctx.load_output_port("out", packet_id)

        ctx.send_output_salvo("send")
        ctx.print(f"[{ctx.node_name}] Execution complete!")

    return NetConfig(
        pools={
            "main_pool": PoolConfig(id="main_pool", spec=MainPoolConfig()),
        },
        graph=GraphConfig(
            nodes=[
                NodeGraphConfig(
                    name="DemoSource",
                    out_ports={"out": PortConfig()},
                    out_salvo_conditions={
                        "send": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={"out": PacketCountAllConfig()},
                            term=SalvoConditionTermTrueConfig(),
                        ),
                    },
                    execution_config=NodeExecutionConfig(
                        node_name="DemoSource",
                        pools=["main_pool"],
                        exec_node_func=demo_source_node,
                    ),
                ),
            ],
            edges=[],
        ),
    )

# %% [markdown]
# ## Print Capture with Timestamps
#
# Node functions can use `ctx.print()` to capture output. Each print call is
# automatically timestamped when it is called. The captured output is:
# - Stored as `(timestamp, message)` tuples
# - Retrieved per-epoch using `net.get_epoch_log(epoch_id)`
# - Retrieved per-node using `net.get_node_log(node_name)`

# %% [markdown]
# ## Viewing Print Logs
#
# After executing epochs, you can retrieve the captured print logs.

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
# 2. **Context Operations**: Using `ctx.create_packet()`, `ctx.consume_packet()`,
#    `ctx.load_output_port()`, and `ctx.send_output_salvo()`
# 3. **Graph Configuration**: Defining nodes, ports, edges, and salvo conditions
# 4. **Pool Configuration**: Setting up different pool types (Main, Thread, Multiprocess)
# 5. **Net Lifecycle**: Using `start()`, `stop()`, `pause()`, `resume()`, context manager
# 6. **Simulation**: Using `run_step()` and `run_until_blocked()`
# 7. **Print Capture**: How `ctx.print()` captures output with automatic timestamps
# 8. **Rate Limiting**: Controlling epoch start frequency per node
#
# The Net class orchestrates the flow-based execution by:
# - Using `netrun-sim` for packet flow simulation
# - Dispatching node functions to worker pools via `ExecutionManager`
# - Managing packet values in `PacketStore`
# - Using deferred mode: all ctx operations are queued and committed atomically
# - Handling errors and capturing print output with timestamps
# 7. **Viewing Logs**: Retrieving print logs by epoch or chronologically
# 8. **Rate Limiting**: Controlling epoch start frequency
#
# The Net class orchestrates the flow-based execution by:
# - Using `netrun-sim` for packet flow simulation
# - Dispatching node functions to worker pools via `ExecutionManager`
# - Managing packet values in `PacketStore`
# - Handling errors, retries, and print capture
