# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Example 02: Error Handling and Retries
#
# This example demonstrates netrun's error handling capabilities:
#
# - Configuring retries for unreliable nodes
# - Using the dead letter queue (DLQ) for failed packets
# - Error callbacks and failure handlers
# - Different `on_error` modes
#
# ## Scenario
#
# We simulate a pipeline where the Processor node has a chance of failing.
# With retries configured, it will automatically retry on failure.

# %%
#|default_exp 02_error_handling

# %%
#|export
import random
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
    # DLQ
    DeadLetterEntry,
)

# %% [markdown]
# ## Part 1: Create a Simple Pipeline
#
# Source -> Processor (unreliable) -> Sink

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
# ## Part 2: Configure Net with Error Handling
#
# We configure:
# - `on_error="continue"` to keep processing other nodes on failure
# - An error callback to log errors
# - DLQ in memory mode

# %%
#|export
# Track errors for demonstration
error_log = []

def error_callback(exception, node_name, epoch_id):
    """Called when a node fails after all retries."""
    error_log.append({
        "node": node_name,
        "error": str(exception),
        "epoch_id": epoch_id[:8] + "...",
    })
    print(f"[ERROR] {node_name} failed: {exception}")

# Create net with error handling configured
net = Net(
    graph,
    consumed_packet_storage=True,
    on_error="continue",  # Keep running on errors
    error_callback=error_callback,
    dead_letter_queue="memory",
)

print(f"Created Net with on_error='{net._on_error}'")

# %% [markdown]
# ## Part 3: Define Node Functions with Retries
#
# The Processor has a 60% failure rate on first attempt.
# With retries, most packets should eventually succeed.

# %%
#|export
# Tracking for demonstration
execution_log = []
results = []

# Simulate unreliable processing
fail_rate = 0.6
random.seed(42)  # For reproducibility

def source_exec(ctx, packets):
    """Generate multiple data packets."""
    execution_log.append(f"Source: generating packets")
    for i in range(3):
        pkt = ctx.create_packet({"id": i, "data": f"item-{i}"})
        ctx.load_output_port("out", pkt)
        ctx.send_output_salvo("send")
    execution_log.append(f"Source: sent 3 packets")

def processor_exec(ctx, packets):
    """Process data with simulated failures."""
    for port_name, pkts in packets.items():
        for pkt in pkts:
            value = ctx.consume_packet(pkt)

            # Simulate unreliable processing
            # Fail more often on first attempts, less on retries
            adjusted_fail_rate = fail_rate / (ctx.retry_count + 1)
            if random.random() < adjusted_fail_rate:
                execution_log.append(
                    f"Processor: FAILED on {value['id']} (attempt {ctx.retry_count + 1})"
                )
                raise RuntimeError(f"Processing failed for item {value['id']}")

            execution_log.append(
                f"Processor: processed {value['id']} (attempt {ctx.retry_count + 1})"
            )

            # Create output
            output = {**value, "processed": True}
            out_pkt = ctx.create_packet(output)
            ctx.load_output_port("out", out_pkt)
            ctx.send_output_salvo("send")

def processor_failed(failure_ctx):
    """Called after each failure."""
    execution_log.append(
        f"Processor: failed_func called (retry {failure_ctx.retry_count})"
    )

def sink_exec(ctx, packets):
    """Collect processed results."""
    for port_name, pkts in packets.items():
        for pkt in pkts:
            value = ctx.consume_packet(pkt)
            results.append(value)
            execution_log.append(f"Sink: received {value['id']}")

# Register execution functions
net.set_node_exec("Source", source_exec)
net.set_node_exec("Processor", processor_exec, failed_func=processor_failed)
net.set_node_exec("Sink", sink_exec)

# Configure Processor with retries
net.set_node_config(
    "Processor",
    retries=3,               # Up to 3 retries (4 total attempts)
    retry_wait=0.01,         # Small delay between retries
    defer_net_actions=True,  # Required for retries
)

print("Node functions and retry configuration set up")

# %% [markdown]
# ## Part 4: Run the Pipeline

# %%
#|export
# Clear tracking
execution_log.clear()
results.clear()
error_log.clear()

# Inject source epoch
net.inject_source_epoch("Source")

print("Running pipeline with unreliable Processor...\n")
net.start()

print("\n--- Execution Log ---")
for entry in execution_log:
    print(f"  {entry}")

# %% [markdown]
# ## Part 5: Analyze Results

# %%
#|export
print("\n--- Results ---")
print(f"Successfully processed: {len(results)} items")
for r in results:
    print(f"  {r}")

print(f"\n--- Dead Letter Queue ---")
dlq_entries = net.dead_letter_queue.get_all()
print(f"Failed items in DLQ: {len(dlq_entries)}")
for entry in dlq_entries:
    print(f"  Node: {entry.node_name}")
    print(f"  Error: {entry.exception}")
    print(f"  Retries: {entry.retry_count}")
    print(f"  Input packets: {entry.input_packets}")

print(f"\n--- Error Log ---")
print(f"Total errors (after retries exhausted): {len(error_log)}")
for err in error_log:
    print(f"  {err}")

# %% [markdown]
# ## Part 6: Demonstrate Different on_error Modes
#
# Let's create a new net with `on_error="raise"` to see the difference.

# %%
#|export
print("\n" + "="*60)
print("Demonstrating on_error='raise' mode")
print("="*60 + "\n")

# Create a new net with raise mode
net_raise = Net(
    graph,
    on_error="raise",  # Raise exception on error
)

call_count = [0]

def source_exec_raise(ctx, packets):
    pkt = ctx.create_packet({"id": 0})
    ctx.load_output_port("out", pkt)
    ctx.send_output_salvo("send")

def processor_exec_raise(ctx, packets):
    call_count[0] += 1
    raise ValueError("Always fails!")

def sink_exec_raise(ctx, packets):
    pass

net_raise.set_node_exec("Source", source_exec_raise)
net_raise.set_node_exec("Processor", processor_exec_raise)
net_raise.set_node_exec("Sink", sink_exec_raise)

net_raise.inject_source_epoch("Source")

try:
    net_raise.start()
except Exception as e:
    print(f"Caught exception: {type(e).__name__}")
    print(f"  Message: {e}")
    print(f"  Net state: {net_raise.state}")

# %% [markdown]
# ## Summary
#
# This example demonstrated:
#
# 1. **Retry Configuration**: Using `retries` and `retry_wait` for unreliable nodes
# 2. **Deferred Actions**: Required (`defer_net_actions=True`) when using retries
# 3. **Failed Function**: `failed_func` is called after each failure
# 4. **Dead Letter Queue**: Failed packets (after all retries) go to the DLQ
# 5. **Error Callback**: `error_callback` is called after retries are exhausted
# 6. **on_error Modes**:
#    - `"continue"`: Keep running other nodes, put failed epoch in DLQ
#    - `"pause"`: Stop starting new epochs
#    - `"raise"`: Pause then raise exception
#
# Key points:
# - Retries require `defer_net_actions=True` so actions can be rolled back
# - The retry context provides info about previous attempts
# - DLQ entries contain full context for debugging and recovery
