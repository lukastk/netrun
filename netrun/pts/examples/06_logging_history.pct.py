# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Example 06: Logging and History
#
# This example demonstrates logging and history features in `netrun`:
#
# - Capturing stdout from node execution
# - Accessing node and epoch logs
# - Event history recording
# - Persisting history to JSONL files

# %%
#|default_exp 06_logging_history

# %%
#|export
import tempfile
from pathlib import Path
from netrun import (
    Graph, Node, Edge, Port, PortType, PortRef, PortState,
    MaxSalvos, SalvoCondition, SalvoConditionTerm,
    Net, NetState,
)

# %%
#|export
# Create a simple pipeline: Source -> Processor -> Sink
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
def run_logging_example():
    """Demonstrate stdout capture and node logging."""
    print("=" * 60)
    print("Logging Example")
    print("=" * 60)

    net = Net(
        graph,
        consumed_packet_storage=True,
        on_error="raise",
    )

    # Track epoch IDs for later log access
    epoch_ids = {"Source": [], "Processor": [], "Sink": []}

    def source_exec(ctx, packets):
        """Source node that logs its activity."""
        epoch_ids["Source"].append(ctx.epoch_id)
        print(f"Source starting (epoch {ctx.epoch_id[:8]}...)")
        for i in range(3):
            pkt = ctx.create_packet({"id": i})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")
            print(f"  Created packet {i}")
        print("Source done")

    def processor_exec(ctx, packets):
        """Processor node that logs processing."""
        epoch_ids["Processor"].append(ctx.epoch_id)
        print(f"Processor starting (epoch {ctx.epoch_id[:8]}...)")
        for port_name, pkts in packets.items():
            for pkt in pkts:
                value = ctx.consume_packet(pkt)
                print(f"  Processing packet {value['id']}")
                out_pkt = ctx.create_packet({**value, "processed": True})
                ctx.load_output_port("out", out_pkt)
                ctx.send_output_salvo("send")
        print("Processor done")

    def sink_exec(ctx, packets):
        """Sink node that logs received packets."""
        epoch_ids["Sink"].append(ctx.epoch_id)
        print(f"Sink starting (epoch {ctx.epoch_id[:8]}...)")
        for port_name, pkts in packets.items():
            for pkt in pkts:
                value = ctx.consume_packet(pkt)
                print(f"  Received packet {value['id']}")
        print("Sink done")

    net.set_node_exec("Source", source_exec)
    net.set_node_exec("Processor", processor_exec)
    net.set_node_exec("Sink", sink_exec)

    # Enable stdout capture (default is True)
    net.set_node_config("Source", capture_stdout=True)
    net.set_node_config("Processor", capture_stdout=True)
    net.set_node_config("Sink", capture_stdout=True)

    net.inject_source_epoch("Source")
    net.start()

    # Access logs after execution
    print("\n" + "=" * 60)
    print("Accessing Logs")
    print("=" * 60)

    # Get all logs for a node
    print("\nSource node log entries:")
    for entry in net.get_node_log("Source"):
        print(f"  [{entry.timestamp.strftime('%H:%M:%S')}] {entry.message}")

    # Get logs for a specific epoch
    if epoch_ids["Processor"]:
        first_processor_epoch = epoch_ids["Processor"][0]
        print(f"\nProcessor epoch {first_processor_epoch[:8]}... logs:")
        for entry in net.get_epoch_log("Processor", first_processor_epoch):
            print(f"  {entry.message}")

    return epoch_ids

# %%
#|export
def run_history_example():
    """Demonstrate event history recording and persistence."""
    print("\n" + "=" * 60)
    print("History Example")
    print("=" * 60)

    # Create a temporary file for history
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        history_path = Path(f.name)

    try:
        net = Net(
            graph,
            consumed_packet_storage=True,
            on_error="raise",
            history_file=history_path,
            history_max_size=1000,
            history_chunk_size=10,
            history_flush_on_pause=True,
        )

        def source_exec(ctx, packets):
            for i in range(2):
                pkt = ctx.create_packet({"id": i})
                ctx.load_output_port("out", pkt)
                ctx.send_output_salvo("send")

        def processor_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    out_pkt = ctx.create_packet(value)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Processor", processor_exec)
        net.set_node_exec("Sink", sink_exec)

        # Record custom events
        net.event_history.record_action("CustomAction", {"info": "Starting net"})

        net.inject_source_epoch("Source")
        net.start()

        # Record more custom events
        net.event_history.record_action("CustomAction", {"info": "Net finished"})

        # Access history
        print(f"\nEvent history has {len(net.event_history)} entries")

        actions = net.event_history.get_actions(limit=5)
        print(f"\nLast 5 actions:")
        for action in actions:
            print(f"  {action.action_type}: {action.data}")

        # Pause to flush history
        net.pause()

        # Check history file
        print(f"\nHistory file: {history_path}")
        print(f"File size: {history_path.stat().st_size} bytes")

        # Read a few entries
        with open(history_path) as f:
            lines = f.readlines()
        print(f"Total entries in file: {len(lines)}")

    finally:
        # Cleanup
        history_path.unlink(missing_ok=True)

# %%
#|export
def run_echo_stdout_example():
    """Demonstrate echoing stdout while capturing."""
    print("\n" + "=" * 60)
    print("Echo Stdout Example")
    print("=" * 60)
    print("(stdout will be both captured and echoed)")

    net = Net(
        graph,
        consumed_packet_storage=True,
        on_error="raise",
    )

    def source_exec(ctx, packets):
        print("This message is echoed to stdout AND captured to logs")
        pkt = ctx.create_packet({"value": 1})
        ctx.load_output_port("out", pkt)
        ctx.send_output_salvo("send")

    def processor_exec(ctx, packets):
        print("Processor: echoed message")
        for port_name, pkts in packets.items():
            for pkt in pkts:
                value = ctx.consume_packet(pkt)
                out_pkt = ctx.create_packet(value)
                ctx.load_output_port("out", out_pkt)
                ctx.send_output_salvo("send")

    def sink_exec(ctx, packets):
        print("Sink received data")
        for port_name, pkts in packets.items():
            for pkt in pkts:
                ctx.consume_packet(pkt)

    net.set_node_exec("Source", source_exec)
    net.set_node_exec("Processor", processor_exec)
    net.set_node_exec("Sink", sink_exec)

    # Enable echo_stdout - messages go to both log and stdout
    net.set_node_config("Source", capture_stdout=True, echo_stdout=True)
    net.set_node_config("Processor", capture_stdout=True, echo_stdout=True)
    net.set_node_config("Sink", capture_stdout=True, echo_stdout=True)

    net.inject_source_epoch("Source")
    net.start()

    print("\nLog entries (also captured):")
    for entry in net.get_node_log("Source"):
        print(f"  Source: {entry.message}")

# %%
if __name__ == "__main__":
    run_logging_example()
    run_history_example()
    run_echo_stdout_example()
