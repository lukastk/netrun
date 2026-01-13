# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Example 08: DSL Format
#
# This example demonstrates the TOML-based DSL (Domain Specific Language) for
# defining and serializing netrun networks:
#
# - Creating a Net and exporting to TOML
# - Loading a Net from TOML
# - Understanding the TOML format structure
# - Expression syntax for salvo conditions

# %%
#|default_exp 08_dsl_format

# %%
#|export
import tempfile
from pathlib import Path

from netrun import (
    Graph, Node, Edge, Port, PortType, PortRef, PortState,
    MaxSalvos, SalvoCondition, SalvoConditionTerm,
    Net, NetState,
    parse_salvo_condition_expr,
    NetDSLConfig,
    parse_toml_string,
    net_config_to_toml,
)

# %%
#|export
def create_pipeline_graph():
    """Create a simple three-node pipeline."""
    source = Node(
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
    processor = Node(
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
    sink = Node(
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
    return Graph([source, processor, sink], edges)

# %%
#|export
def run_export_to_toml():
    """Demonstrate exporting a Net to TOML."""
    print("=" * 60)
    print("Exporting Net to TOML")
    print("=" * 60)

    graph = create_pipeline_graph()
    net = Net(
        graph,
        on_error="raise",
        consumed_packet_storage=True,
    )

    # Set some node configuration
    net.set_node_config("Processor", defer_net_actions=True, timeout=30.0)

    # Export to TOML string
    toml_str = net.to_toml()
    print("Generated TOML:")
    print("-" * 40)
    print(toml_str)
    print("-" * 40)

    return toml_str

# %%
#|export
def run_load_from_toml():
    """Demonstrate loading a Net from TOML."""
    print("\n" + "=" * 60)
    print("Loading Net from TOML")
    print("=" * 60)

    # Define a Net in TOML format
    toml_str = """
[net]
on_error = "raise"
consumed_packet_storage = true

[nodes.Source]
out_ports = { out = {} }

[nodes.Source.out_salvo_conditions.send]
max_salvos = "infinite"
ports = "out"
when = "nonempty(out)"

[nodes.Processor]
in_ports = { in = {} }
out_ports = { out = {} }
options = { defer_net_actions = true }

[nodes.Processor.in_salvo_conditions.receive]
max_salvos = 1
ports = "in"
when = "nonempty(in)"

[nodes.Processor.out_salvo_conditions.send]
max_salvos = "infinite"
ports = "out"
when = "nonempty(out)"

[nodes.Sink]
in_ports = { in = {} }

[nodes.Sink.in_salvo_conditions.receive]
max_salvos = 1
ports = "in"
when = "nonempty(in)"

[[edges]]
from = "Source.out"
to = "Processor.in"

[[edges]]
from = "Processor.out"
to = "Sink.in"
"""

    print("Input TOML:")
    print("-" * 40)
    print(toml_str.strip())
    print("-" * 40)

    # Load the Net (without resolving exec functions)
    net = Net.from_toml(toml_str, resolve_funcs=False)

    print(f"\nLoaded Net:")
    print(f"  on_error: {net._on_error}")
    print(f"  nodes: {list(net._graph.nodes().keys())}")
    print(f"  edges: {len(list(net._graph.edges()))} edges")

    # Now set the execution functions and run
    results = []

    def source_exec(ctx, packets):
        pkt = ctx.create_packet({"id": 1, "data": "hello"})
        ctx.load_output_port("out", pkt)
        ctx.send_output_salvo("send")

    def processor_exec(ctx, packets):
        for port_name, pkts in packets.items():
            for pkt in pkts:
                value = ctx.consume_packet(pkt)
                processed = {**value, "processed": True}
                out_pkt = ctx.create_packet(processed)
                ctx.load_output_port("out", out_pkt)
                ctx.send_output_salvo("send")

    def sink_exec(ctx, packets):
        for port_name, pkts in packets.items():
            for pkt in pkts:
                value = ctx.consume_packet(pkt)
                results.append(value)

    net.set_node_exec("Source", source_exec)
    net.set_node_exec("Processor", processor_exec)
    net.set_node_exec("Sink", sink_exec)

    net.inject_source_epoch("Source")
    net.start()

    print(f"\nExecution results: {results}")
    return results

# %%
#|export
def run_expression_parser():
    """Demonstrate the salvo condition expression parser."""
    print("\n" + "=" * 60)
    print("Salvo Condition Expression Parser")
    print("=" * 60)

    expressions = [
        # Simple port checks
        "nonempty(in)",
        "empty(buffer)",
        "full(queue)",

        # Count comparisons
        "count(in) >= 5",
        "count(in) == 3",
        "count(in) > 0",

        # Logical operators
        "nonempty(a) and nonempty(b)",
        "nonempty(a) or nonempty(b)",
        "not empty(in)",

        # Complex expressions
        "(nonempty(a) or nonempty(b)) and not full(out)",
    ]

    print("\nParsing expressions:")
    print("-" * 40)

    for expr in expressions:
        try:
            term = parse_salvo_condition_expr(expr)
            print(f"  '{expr}'")
            print(f"    -> {term}")
        except Exception as e:
            print(f"  '{expr}' -> Error: {e}")
        print()

# %%
#|export
def run_save_and_load_file():
    """Demonstrate saving and loading TOML files."""
    print("\n" + "=" * 60)
    print("Save and Load TOML Files")
    print("=" * 60)

    graph = create_pipeline_graph()
    net1 = Net(graph, on_error="pause")

    # Save to a temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
        path = Path(f.name)

    net1.save_toml(path)
    print(f"Saved Net to: {path}")

    # Read the file contents
    with open(path) as f:
        content = f.read()
    print(f"\nFile contents ({len(content)} bytes):")
    print("-" * 40)
    print(content[:500] + "..." if len(content) > 500 else content)
    print("-" * 40)

    # Load it back
    net2 = Net.from_toml_file(path, resolve_funcs=False)
    print(f"\nLoaded Net:")
    print(f"  on_error: {net2._on_error}")
    print(f"  nodes: {list(net2._graph.nodes().keys())}")

# %%
#|export
def run_toml_format_overview():
    """Show the TOML format structure."""
    print("\n" + "=" * 60)
    print("TOML Format Overview")
    print("=" * 60)

    print("""
The netrun TOML format has the following structure:

[net]
# Net-level configuration
on_error = "raise"           # "continue", "pause", or "raise"
consumed_packet_storage = true
history_max_size = 10000

[nodes.NodeName]
# Node definition
in_ports = { in = {}, other = { slots = 5 } }
out_ports = { out = {} }
options = { pool = "main", retries = 3, defer_net_actions = true }
exec_node_func = "mymodule.exec_func"    # Optional: import path

[nodes.NodeName.in_salvo_conditions.receive]
max_salvos = 1               # integer or "infinite"
ports = "in"                 # port name(s)
when = "nonempty(in)"        # expression

[nodes.NodeName.out_salvo_conditions.send]
max_salvos = "infinite"
ports = "out"
when = "nonempty(out)"

[[edges]]
from = "Source.out"
to = "Sink.in"

Expression Syntax:
- nonempty(port)       - port has packets
- empty(port)          - port is empty
- full(port)           - port is at capacity
- count(port) >= N     - port has at least N packets
- count(port) == N     - port has exactly N packets
- expr and expr        - both conditions
- expr or expr         - either condition
- not expr             - negation
- (expr)               - grouping
""")

# %%
if __name__ == "__main__":
    run_export_to_toml()
    run_load_from_toml()
    run_expression_parser()
    run_save_and_load_file()
    run_toml_format_overview()
