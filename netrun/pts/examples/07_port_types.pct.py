# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Example 07: Port Types
#
# This example demonstrates port type checking in `netrun`:
#
# - Defining type specifications for input and output ports
# - Type checking when consuming packets
# - Type checking when loading packets to output ports
# - Different type specification formats

# %%
#|default_exp 07_port_types

# %%
#|export
from netrun import (
    Graph, Node, Edge, Port, PortType, PortRef, PortState,
    MaxSalvos, SalvoCondition, SalvoConditionTerm,
    Net, NetState, PacketTypeMismatch,
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
def run_basic_type_checking():
    """Demonstrate basic port type checking."""
    print("=" * 60)
    print("Basic Port Type Checking")
    print("=" * 60)

    net = Net(
        graph,
        consumed_packet_storage=True,
        on_error="raise",
    )

    # Set type specifications for ports
    # Source outputs dict values
    net.set_output_port_type("Source", "out", dict)

    # Processor expects dict input and outputs dict
    net.set_input_port_type("Processor", "in", dict)
    net.set_output_port_type("Processor", "out", dict)

    # Sink expects dict input
    net.set_input_port_type("Sink", "in", dict)

    results = []

    def source_exec(ctx, packets):
        """Source node that produces dict values."""
        data = {"id": 1, "value": "hello"}
        pkt = ctx.create_packet(data)
        ctx.load_output_port("out", pkt)  # Type checked: dict matches dict
        ctx.send_output_salvo("send")
        print(f"Source: sent {data}")

    def processor_exec(ctx, packets):
        """Processor that transforms dict values."""
        for port_name, pkts in packets.items():
            for pkt in pkts:
                value = ctx.consume_packet(pkt)  # Type checked: dict matches dict
                print(f"Processor: received {value}")
                transformed = {**value, "processed": True}
                out_pkt = ctx.create_packet(transformed)
                ctx.load_output_port("out", out_pkt)  # Type checked
                ctx.send_output_salvo("send")
                print(f"Processor: sent {transformed}")

    def sink_exec(ctx, packets):
        """Sink node that collects results."""
        for port_name, pkts in packets.items():
            for pkt in pkts:
                value = ctx.consume_packet(pkt)  # Type checked
                results.append(value)
                print(f"Sink: received {value}")

    net.set_node_exec("Source", source_exec)
    net.set_node_exec("Processor", processor_exec)
    net.set_node_exec("Sink", sink_exec)

    net.inject_source_epoch("Source")
    net.start()

    print(f"\nFinal results: {results}")
    return results

# %%
#|export
def run_string_based_type_checking():
    """Demonstrate string-based type checking for external library types."""
    print("\n" + "=" * 60)
    print("String-Based Type Checking")
    print("=" * 60)
    print("(Useful for external library types like pandas DataFrame)")

    net = Net(
        graph,
        consumed_packet_storage=True,
        on_error="raise",
    )

    # Use string class names for type checking
    # This is useful for checking types from external libraries
    # without importing them
    net.set_output_port_type("Source", "out", "dict")
    net.set_input_port_type("Processor", "in", "dict")
    net.set_output_port_type("Processor", "out", "dict")
    net.set_input_port_type("Sink", "in", "dict")

    results = []

    def source_exec(ctx, packets):
        data = {"name": "test", "count": 42}
        pkt = ctx.create_packet(data)
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
                value = ctx.consume_packet(pkt)
                results.append(value)

    net.set_node_exec("Source", source_exec)
    net.set_node_exec("Processor", processor_exec)
    net.set_node_exec("Sink", sink_exec)

    net.inject_source_epoch("Source")
    net.start()

    print(f"Results: {results}")
    return results

# %%
#|export
def run_isinstance_vs_exact_type():
    """Demonstrate isinstance vs exact type matching."""
    print("\n" + "=" * 60)
    print("isinstance vs Exact Type Matching")
    print("=" * 60)

    class BaseData:
        def __init__(self, value):
            self.value = value

    class ExtendedData(BaseData):
        def __init__(self, value, extra):
            super().__init__(value)
            self.extra = extra

    net = Net(
        graph,
        consumed_packet_storage=True,
        on_error="raise",
    )

    # Use isinstance check (default) - allows subclasses
    net.set_output_port_type("Source", "out", BaseData)
    net.set_input_port_type("Processor", "in", BaseData)

    results = []

    def source_exec(ctx, packets):
        # Create an ExtendedData (subclass of BaseData)
        data = ExtendedData(value=100, extra="bonus")
        pkt = ctx.create_packet(data)
        ctx.load_output_port("out", pkt)  # Passes - ExtendedData is BaseData
        ctx.send_output_salvo("send")
        print(f"Source: sent ExtendedData with value={data.value}")

    def processor_exec(ctx, packets):
        for port_name, pkts in packets.items():
            for pkt in pkts:
                data = ctx.consume_packet(pkt)  # Passes isinstance check
                print(f"Processor: received {type(data).__name__}")
                out_pkt = ctx.create_packet(data)
                ctx.load_output_port("out", out_pkt)
                ctx.send_output_salvo("send")

    def sink_exec(ctx, packets):
        for port_name, pkts in packets.items():
            for pkt in pkts:
                data = ctx.consume_packet(pkt)
                results.append(data)

    net.set_node_exec("Source", source_exec)
    net.set_node_exec("Processor", processor_exec)
    net.set_node_exec("Sink", sink_exec)

    net.inject_source_epoch("Source")
    net.start()

    print(f"Result type: {type(results[0]).__name__}")
    return results

# %%
#|export
def run_type_mismatch_example():
    """Demonstrate what happens when type check fails."""
    print("\n" + "=" * 60)
    print("Type Mismatch Error Handling")
    print("=" * 60)

    net = Net(
        graph,
        consumed_packet_storage=True,
        on_error="raise",
    )

    # Expect int on output port, but we'll send a string
    net.set_output_port_type("Source", "out", int)

    def source_exec(ctx, packets):
        # This will cause a type mismatch error
        data = "not an int"
        pkt = ctx.create_packet(data)
        ctx.load_output_port("out", pkt)  # Should raise PacketTypeMismatch

    net.set_node_exec("Source", source_exec)

    net.inject_source_epoch("Source")

    try:
        net.start()
    except Exception as e:
        print(f"Caught error: {e}")
        print("Type checking prevented invalid data from flowing through the network!")

# %%
#|export
def run_dict_spec_options():
    """Demonstrate different dict spec options."""
    print("\n" + "=" * 60)
    print("Dict Specification Options")
    print("=" * 60)

    class Animal:
        pass

    class Dog(Animal):
        pass

    # Example 1: isinstance=False for exact type match
    net1 = Net(
        graph,
        consumed_packet_storage=True,
        on_error="raise",
    )

    # Only accept exactly Animal, not subclasses
    net1.set_output_port_type("Source", "out", {"class": Animal, "isinstance": False})

    def source_exact_check(ctx, packets):
        animal = Animal()
        pkt = ctx.create_packet(animal)
        ctx.load_output_port("out", pkt)  # Passes - exactly Animal
        ctx.send_output_salvo("send")
        print("Exact type match: Animal passed")

    net1.set_node_exec("Source", source_exact_check)
    net1.set_node_exec("Processor", lambda ctx, pkts: None)
    net1.set_node_exec("Sink", lambda ctx, pkts: None)
    net1.inject_source_epoch("Source")
    net1.start()

    # Example 2: subclass=True for explicit subclass check
    net2 = Net(
        graph,
        consumed_packet_storage=True,
        on_error="raise",
    )

    net2.set_output_port_type("Source", "out", {"class": Animal, "subclass": True})

    def source_subclass_check(ctx, packets):
        dog = Dog()
        pkt = ctx.create_packet(dog)
        ctx.load_output_port("out", pkt)  # Passes - Dog is subclass of Animal
        ctx.send_output_salvo("send")
        print("Subclass check: Dog passed as Animal subclass")

    net2.set_node_exec("Source", source_subclass_check)
    net2.set_node_exec("Processor", lambda ctx, pkts: None)
    net2.set_node_exec("Sink", lambda ctx, pkts: None)
    net2.inject_source_epoch("Source")
    net2.start()

# %%
if __name__ == "__main__":
    run_basic_type_checking()
    run_string_based_type_checking()
    run_isinstance_vs_exact_type()
    run_type_mismatch_example()
    run_dict_spec_options()
