# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Example 04: Background Runner and Pools
#
# This example demonstrates background execution and pool management in `netrun`:
#
# - Using `start(threaded=True)` to run the network in a background thread
# - Using `BackgroundNetRunner` to control execution
# - Configuring thread pools for parallel execution
# - Using `wait_until_blocked()` to wait for completion

# %%
#|default_exp 04_background_runner

# %%
#|export
import time
from netrun import (
    Graph, Node, Edge, Port, PortType, PortRef, PortState,
    MaxSalvos, SalvoCondition, SalvoConditionTerm,
    Net, NetState, BackgroundNetRunner, PoolManager,
)

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

# %%
#|export
def run_background_example():
    """Demonstrate background execution."""
    net = Net(
        graph,
        consumed_packet_storage=True,
        on_error="raise",
        thread_pools={"workers": {"size": 4}},
    )

    results = []

    def source_exec(ctx, packets):
        for i in range(3):
            pkt = ctx.create_packet({"id": i})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

    def processor_exec(ctx, packets):
        for port_name, pkts in packets.items():
            for pkt in pkts:
                value = ctx.consume_packet(pkt)
                time.sleep(0.01)
                out_pkt = ctx.create_packet({**value, "processed": True})
                ctx.load_output_port("out", out_pkt)
                ctx.send_output_salvo("send")

    def sink_exec(ctx, packets):
        for port_name, pkts in packets.items():
            for pkt in pkts:
                results.append(ctx.consume_packet(pkt))

    net.set_node_exec("Source", source_exec)
    net.set_node_exec("Processor", processor_exec)
    net.set_node_exec("Sink", sink_exec)
    net.inject_source_epoch("Source")

    # Run in background
    runner = net.start(threaded=True)

    # Main thread can do other work
    print("Main thread working while network runs...")
    for i in range(3):
        time.sleep(0.02)

    # Wait for completion
    runner.wait_until_blocked(timeout=10.0)
    runner.stop()

    print(f"Results: {results}")
    return results

# %%
if __name__ == "__main__":
    run_background_example()
