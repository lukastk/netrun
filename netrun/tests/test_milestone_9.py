"""
Milestone 9 Tests: Port Types

This milestone adds:
- Type specifications for input and output ports
- Type checking when consuming input packets
- Type checking when loading packets to output ports
- PacketTypeMismatch error handling
"""

import pytest

from netrun import (
    Graph, Node, Edge, Port, PortType, PortRef, PortState,
    MaxSalvos, SalvoCondition, SalvoConditionTerm,
    Net, NetState, NodeConfig,
    PortTypeSpec, PortTypeRegistry, check_value_type,
    PacketTypeMismatch,
)


# ==============================================================================
# Fixtures
# ==============================================================================

@pytest.fixture
def simple_node():
    """A simple source node for testing."""
    return Node(
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


@pytest.fixture
def simple_graph(simple_node):
    """A simple one-node graph."""
    return Graph([simple_node], [])


@pytest.fixture
def pipeline_graph():
    """A Source -> Processor -> Sink pipeline."""
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


# ==============================================================================
# Test Classes for Type Specifications
# ==============================================================================

class TestPortTypeSpecFromString:
    """Test creating PortTypeSpec from string class names."""

    def test_from_string_creates_spec(self):
        """Test that a string creates a spec with type_name."""
        spec = PortTypeSpec.from_spec("DataFrame")
        assert spec is not None
        assert spec.type_name == "DataFrame"
        assert spec.type_class is None

    def test_string_check_matches_class_name(self):
        """Test string-based type checking matches __class__.__name__."""
        spec = PortTypeSpec.from_spec("dict")
        assert spec.check({"key": "value"}) is True
        assert spec.check([1, 2, 3]) is False
        assert spec.check("string") is False

    def test_string_check_custom_class(self):
        """Test string-based checking with custom class."""
        class MyCustomClass:
            pass
        spec = PortTypeSpec.from_spec("MyCustomClass")
        assert spec.check(MyCustomClass()) is True
        assert spec.check({}) is False


class TestPortTypeSpecFromType:
    """Test creating PortTypeSpec from type objects."""

    def test_from_type_creates_spec(self):
        """Test that a type creates a spec with isinstance check."""
        spec = PortTypeSpec.from_spec(dict)
        assert spec is not None
        assert spec.type_class == dict
        assert spec.use_isinstance is True

    def test_isinstance_check(self):
        """Test isinstance-based type checking."""
        spec = PortTypeSpec.from_spec(dict)
        assert spec.check({"key": "value"}) is True
        assert spec.check([1, 2, 3]) is False

    def test_isinstance_with_inheritance(self):
        """Test isinstance check works with inheritance."""
        class BaseClass:
            pass
        class DerivedClass(BaseClass):
            pass

        spec = PortTypeSpec.from_spec(BaseClass)
        assert spec.check(BaseClass()) is True
        assert spec.check(DerivedClass()) is True  # isinstance allows subclasses


class TestPortTypeSpecFromDict:
    """Test creating PortTypeSpec from dict configurations."""

    def test_from_dict_with_isinstance_true(self):
        """Test dict spec with isinstance=True (default)."""
        spec = PortTypeSpec.from_spec({"class": int, "isinstance": True})
        assert spec is not None
        assert spec.type_class == int
        assert spec.use_isinstance is True
        assert spec.check(42) is True
        assert spec.check("42") is False

    def test_from_dict_with_isinstance_false(self):
        """Test dict spec with isinstance=False for exact type match."""
        class Base:
            pass
        class Derived(Base):
            pass

        spec = PortTypeSpec.from_spec({"class": Base, "isinstance": False})
        assert spec.use_isinstance is False
        assert spec.check(Base()) is True
        assert spec.check(Derived()) is False  # Exact match fails for subclass

    def test_from_dict_with_subclass_true(self):
        """Test dict spec with subclass=True."""
        class Base:
            pass
        class Derived(Base):
            pass

        spec = PortTypeSpec.from_spec({"class": Base, "subclass": True})
        assert spec.use_subclass is True
        assert spec.check(Base()) is True
        assert spec.check(Derived()) is True

    def test_from_dict_without_class_returns_none(self):
        """Test dict without 'class' key returns None."""
        spec = PortTypeSpec.from_spec({"isinstance": True})
        assert spec is None


class TestPortTypeSpecNone:
    """Test PortTypeSpec with None (no type checking)."""

    def test_from_none_returns_none(self):
        """Test that None creates no spec."""
        spec = PortTypeSpec.from_spec(None)
        assert spec is None


class TestPortTypeSpecExpectedTypeStr:
    """Test get_expected_type_str method."""

    def test_string_type_str(self):
        """Test expected type string for string spec."""
        spec = PortTypeSpec.from_spec("DataFrame")
        assert spec.get_expected_type_str() == "DataFrame"

    def test_isinstance_type_str(self):
        """Test expected type string for isinstance spec."""
        spec = PortTypeSpec.from_spec(dict)
        assert spec.get_expected_type_str() == "instance of dict"

    def test_exact_type_str(self):
        """Test expected type string for exact match spec."""
        spec = PortTypeSpec.from_spec({"class": dict, "isinstance": False})
        assert spec.get_expected_type_str() == "exactly dict"

    def test_subclass_type_str(self):
        """Test expected type string for subclass spec."""
        spec = PortTypeSpec.from_spec({"class": dict, "subclass": True})
        assert spec.get_expected_type_str() == "subclass of dict"


# ==============================================================================
# Test check_value_type Function
# ==============================================================================

class TestCheckValueType:
    """Test the check_value_type helper function."""

    def test_check_with_no_type_spec(self):
        """Test checking with None spec always passes."""
        matches, expected, actual = check_value_type({"data": 1}, None)
        assert matches is True
        assert expected == "any"
        assert actual == "dict"

    def test_check_matching_type(self):
        """Test checking with matching type."""
        matches, expected, actual = check_value_type({"data": 1}, dict)
        assert matches is True
        assert expected == "instance of dict"
        assert actual == "dict"

    def test_check_mismatched_type(self):
        """Test checking with mismatched type."""
        matches, expected, actual = check_value_type("not a dict", dict)
        assert matches is False
        assert expected == "instance of dict"
        assert actual == "str"


# ==============================================================================
# Test PortTypeRegistry
# ==============================================================================

class TestPortTypeRegistry:
    """Test the PortTypeRegistry class."""

    def test_create_registry(self):
        """Test creating an empty registry."""
        registry = PortTypeRegistry()
        assert registry is not None

    def test_set_and_get_input_port_type(self):
        """Test setting and getting input port type."""
        registry = PortTypeRegistry()
        registry.set_input_port_type("Node1", "in", dict)
        assert registry.get_input_port_type("Node1", "in") == dict

    def test_set_and_get_output_port_type(self):
        """Test setting and getting output port type."""
        registry = PortTypeRegistry()
        registry.set_output_port_type("Node1", "out", "DataFrame")
        assert registry.get_output_port_type("Node1", "out") == "DataFrame"

    def test_get_nonexistent_returns_none(self):
        """Test getting nonexistent type returns None."""
        registry = PortTypeRegistry()
        assert registry.get_input_port_type("Node1", "in") is None
        assert registry.get_output_port_type("Node1", "out") is None

    def test_check_input_port_value_passes(self):
        """Test checking value against input port type."""
        registry = PortTypeRegistry()
        registry.set_input_port_type("Node1", "in", dict)
        matches, expected, actual = registry.check_input_port_value("Node1", "in", {"key": "value"})
        assert matches is True

    def test_check_input_port_value_fails(self):
        """Test checking mismatched value against input port type."""
        registry = PortTypeRegistry()
        registry.set_input_port_type("Node1", "in", dict)
        matches, expected, actual = registry.check_input_port_value("Node1", "in", "not a dict")
        assert matches is False
        assert expected == "instance of dict"
        assert actual == "str"

    def test_check_output_port_value_passes(self):
        """Test checking value against output port type."""
        registry = PortTypeRegistry()
        registry.set_output_port_type("Node1", "out", int)
        matches, expected, actual = registry.check_output_port_value("Node1", "out", 42)
        assert matches is True

    def test_check_output_port_value_fails(self):
        """Test checking mismatched value against output port type."""
        registry = PortTypeRegistry()
        registry.set_output_port_type("Node1", "out", int)
        matches, expected, actual = registry.check_output_port_value("Node1", "out", "not an int")
        assert matches is False

    def test_clear_registry(self):
        """Test clearing the registry."""
        registry = PortTypeRegistry()
        registry.set_input_port_type("Node1", "in", dict)
        registry.set_output_port_type("Node1", "out", int)
        registry.clear()
        assert registry.get_input_port_type("Node1", "in") is None
        assert registry.get_output_port_type("Node1", "out") is None


# ==============================================================================
# Test Net Port Type Integration
# ==============================================================================

class TestNetPortTypeConfiguration:
    """Test Net methods for configuring port types."""

    def test_net_has_port_type_registry(self, simple_graph):
        """Test that Net has a port type registry."""
        net = Net(simple_graph)
        assert net.port_type_registry is not None
        assert isinstance(net.port_type_registry, PortTypeRegistry)

    def test_set_input_port_type(self, pipeline_graph):
        """Test setting input port type via Net."""
        net = Net(pipeline_graph)
        net.set_input_port_type("Processor", "in", dict)
        assert net.get_input_port_type("Processor", "in") == dict

    def test_set_output_port_type(self, simple_graph):
        """Test setting output port type via Net."""
        net = Net(simple_graph)
        net.set_output_port_type("Source", "out", dict)
        assert net.get_output_port_type("Source", "out") == dict

    def test_set_port_type_invalid_node_raises(self, simple_graph):
        """Test that setting port type for invalid node raises error."""
        net = Net(simple_graph)
        with pytest.raises(Exception):  # NodeNotFoundError
            net.set_input_port_type("NonexistentNode", "in", dict)

    def test_set_multiple_port_types(self, pipeline_graph):
        """Test setting multiple port types."""
        net = Net(pipeline_graph)
        net.set_input_port_type("Processor", "in", dict)
        net.set_output_port_type("Processor", "out", dict)
        net.set_input_port_type("Sink", "in", dict)

        assert net.get_input_port_type("Processor", "in") == dict
        assert net.get_output_port_type("Processor", "out") == dict
        assert net.get_input_port_type("Sink", "in") == dict


# ==============================================================================
# Test Input Port Type Checking (consume_packet)
# ==============================================================================

class TestInputPortTypeChecking:
    """Test type checking when consuming packets from input ports."""

    def test_consume_packet_passes_type_check(self, pipeline_graph):
        """Test that consume_packet passes when type matches."""
        net = Net(pipeline_graph, consumed_packet_storage=True)

        # Set input port type
        net.set_input_port_type("Processor", "in", dict)

        results = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet({"data": 1})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def processor_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    results.append(value)
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

        net.inject_source_epoch("Source")
        net.start()

        assert len(results) == 1
        assert results[0] == {"data": 1}

    def test_consume_packet_fails_type_check(self, pipeline_graph):
        """Test that consume_packet raises PacketTypeMismatch when type doesn't match."""
        net = Net(pipeline_graph, consumed_packet_storage=True, on_error="raise")

        # Set input port type to expect int, but send dict
        net.set_input_port_type("Processor", "in", int)

        def source_exec(ctx, packets):
            pkt = ctx.create_packet({"data": 1})  # dict, not int
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def processor_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)  # Should raise

        def sink_exec(ctx, packets):
            pass

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Processor", processor_exec)
        net.set_node_exec("Sink", sink_exec)

        net.inject_source_epoch("Source")
        with pytest.raises(Exception) as exc_info:
            net.start()

        # The exception chain should include PacketTypeMismatch
        assert "expected type" in str(exc_info.value).lower() or "PacketTypeMismatch" in str(type(exc_info.value).__mro__)

    def test_consume_without_type_check(self, pipeline_graph):
        """Test that consume_packet works without type checking configured."""
        net = Net(pipeline_graph, consumed_packet_storage=True)

        # No type checking configured
        results = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet({"data": 1})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def processor_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    results.append(value)
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

        net.inject_source_epoch("Source")
        net.start()

        assert len(results) == 1


# ==============================================================================
# Test Output Port Type Checking (load_output_port)
# ==============================================================================

class TestOutputPortTypeChecking:
    """Test type checking when loading packets to output ports."""

    def test_load_output_port_passes_type_check(self, pipeline_graph):
        """Test that load_output_port passes when type matches."""
        net = Net(pipeline_graph, consumed_packet_storage=True)

        # Set output port type
        net.set_output_port_type("Source", "out", dict)

        results = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet({"data": 1})
            ctx.load_output_port("out", pkt)  # Should pass - dict matches
            ctx.send_output_salvo("send")
            results.append("success")

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

        net.inject_source_epoch("Source")
        net.start()

        assert len(results) == 1

    def test_load_output_port_fails_type_check(self, pipeline_graph):
        """Test that load_output_port raises PacketTypeMismatch when type doesn't match."""
        net = Net(pipeline_graph, consumed_packet_storage=True, on_error="raise")

        # Set output port type to expect int
        net.set_output_port_type("Source", "out", int)

        def source_exec(ctx, packets):
            pkt = ctx.create_packet({"data": 1})  # dict, not int
            ctx.load_output_port("out", pkt)  # Should raise

        net.set_node_exec("Source", source_exec)

        net.inject_source_epoch("Source")
        with pytest.raises(Exception) as exc_info:
            net.start()

        # Should fail due to type mismatch
        assert "expected type" in str(exc_info.value).lower() or "PacketTypeMismatch" in str(type(exc_info.value).__mro__)

    def test_load_output_port_without_type_check(self, pipeline_graph):
        """Test that load_output_port works without type checking configured."""
        net = Net(pipeline_graph, consumed_packet_storage=True)

        # No type checking configured
        results = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet({"data": 1})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")
            results.append("success")

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

        net.inject_source_epoch("Source")
        net.start()

        assert len(results) == 1


# ==============================================================================
# Test Deferred Mode Type Checking
# ==============================================================================

class TestDeferredModeTypeChecking:
    """Test type checking with deferred actions."""

    def test_deferred_load_output_port_passes_type_check(self, simple_graph):
        """Test deferred load_output_port passes type check."""
        net = Net(simple_graph, consumed_packet_storage=True)

        net.set_output_port_type("Source", "out", dict)
        net.set_node_config("Source", defer_net_actions=True)

        results = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet({"data": 1})
            ctx.load_output_port("out", pkt)  # Should pass
            ctx.send_output_salvo("send")
            results.append("success")

        net.set_node_exec("Source", source_exec)

        net.inject_source_epoch("Source")
        net.start()

        assert len(results) == 1

    def test_deferred_load_output_port_fails_type_check(self, simple_graph):
        """Test deferred load_output_port fails type check."""
        net = Net(simple_graph, consumed_packet_storage=True, on_error="raise")

        net.set_output_port_type("Source", "out", int)
        net.set_node_config("Source", defer_net_actions=True)

        def source_exec(ctx, packets):
            pkt = ctx.create_packet({"data": 1})  # dict, not int
            ctx.load_output_port("out", pkt)  # Should raise

        net.set_node_exec("Source", source_exec)

        net.inject_source_epoch("Source")
        with pytest.raises(Exception):
            net.start()


# ==============================================================================
# Test PacketTypeMismatch Error
# ==============================================================================

class TestPacketTypeMismatchError:
    """Test the PacketTypeMismatch error class."""

    def test_error_with_port_name(self):
        """Test error message includes port name."""
        error = PacketTypeMismatch("pkt-123", "int", "str", "in_port")
        assert "pkt-123" in str(error)
        assert "int" in str(error)
        assert "str" in str(error)
        assert "in_port" in str(error)

    def test_error_without_port_name(self):
        """Test error message without port name."""
        error = PacketTypeMismatch("pkt-123", "int", "str")
        assert "pkt-123" in str(error)
        assert "int" in str(error)
        assert "str" in str(error)

    def test_error_attributes(self):
        """Test error has expected attributes."""
        error = PacketTypeMismatch("pkt-123", "int", "str", "in_port")
        assert error.packet_id == "pkt-123"
        assert error.expected_type == "int"
        assert error.actual_type == "str"
        assert error.port_name == "in_port"


# ==============================================================================
# Test String-Based Type Checking (for external libraries)
# ==============================================================================

class TestStringBasedTypeChecking:
    """Test string-based type checking for external library types."""

    def test_string_type_check_passes(self, pipeline_graph):
        """Test string-based type checking passes."""
        net = Net(pipeline_graph, consumed_packet_storage=True)

        # Use string "dict" to match dict values
        net.set_output_port_type("Source", "out", "dict")

        results = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet({"data": 1})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")
            results.append("success")

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

        net.inject_source_epoch("Source")
        net.start()

        assert len(results) == 1

    def test_string_type_check_fails(self, pipeline_graph):
        """Test string-based type checking fails for mismatch."""
        net = Net(pipeline_graph, consumed_packet_storage=True, on_error="raise")

        # Use string "list" but send dict
        net.set_output_port_type("Source", "out", "list")

        def source_exec(ctx, packets):
            pkt = ctx.create_packet({"data": 1})  # dict, not list
            ctx.load_output_port("out", pkt)

        net.set_node_exec("Source", source_exec)

        net.inject_source_epoch("Source")
        with pytest.raises(Exception):
            net.start()
