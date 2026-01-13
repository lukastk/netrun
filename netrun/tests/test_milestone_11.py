"""
Milestone 11: Node Factories

Tests for node factory functionality including:
- NodeFactoryResult dataclass
- load_factory function
- create_node_from_factory function
- Net.from_factory static method
- DSL parsing/serialization of factory info
"""
import pytest
import sys
import types
from pathlib import Path

from netrun import (
    Net, Node, Graph, Edge, Port, PortType, PortRef,
    SalvoCondition, SalvoConditionTerm, MaxSalvos, PortState,
    NodeFactoryResult,
    FactoryError,
    FactoryNotFoundError,
    InvalidFactoryError,
    load_factory,
    create_node_from_factory,
    is_json_serializable,
    validate_factory_args,
    NetDSLConfig,
    parse_toml_string,
    net_config_to_toml,
)


# ============================================================================
# Test Factory Modules
# ============================================================================

def _create_test_factory_module():
    """Create a test factory module dynamically."""
    module = types.ModuleType("test_factory_module")

    def get_node_spec(name="TestNode", num_ports=1):
        in_ports = {f"in{i}": Port() for i in range(num_ports)}
        out_ports = {"out": Port()}

        in_salvo_conds = {}
        if num_ports > 0:
            in_salvo_conds["receive"] = SalvoCondition(
                MaxSalvos.finite(1),
                "in0",
                SalvoConditionTerm.port("in0", PortState.non_empty())
            )

        out_salvo_conds = {
            "send": SalvoCondition(
                MaxSalvos.infinite(),
                "out",
                SalvoConditionTerm.port("out", PortState.non_empty())
            )
        }

        return {
            "name": name,
            "in_ports": in_ports,
            "out_ports": out_ports,
            "in_salvo_conditions": in_salvo_conds,
            "out_salvo_conditions": out_salvo_conds,
        }

    def exec_func(ctx, packets):
        for port_name, pkts in packets.items():
            for pkt in pkts:
                value = ctx.consume_packet(pkt)
                out_pkt = ctx.create_packet(value)
                ctx.load_output_port("out", out_pkt)
                ctx.send_output_salvo("send")

    def start_func(net):
        pass

    def stop_func(net):
        pass

    def failed_func(ctx):
        pass

    def get_node_funcs(name="TestNode", num_ports=1):
        return (exec_func, start_func, stop_func, failed_func)

    module.get_node_spec = get_node_spec
    module.get_node_funcs = get_node_funcs
    module.exec_func = exec_func
    module.start_func = start_func
    module.stop_func = stop_func
    module.failed_func = failed_func

    return module


def _create_factory_function_module():
    """Create a module with a factory function instead of get_node_spec/get_node_funcs."""
    module = types.ModuleType("test_factory_func_module")

    def create_processor(prefix="proc"):
        """A factory function that returns a NodeFactoryResult."""
        def exec_func(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    out_pkt = ctx.create_packet({**value, "prefix": prefix})
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")

        spec = {
            "name": f"{prefix}_node",
            "in_ports": {"in": Port()},
            "out_ports": {"out": Port()},
            "in_salvo_conditions": {
                "receive": SalvoCondition(
                    MaxSalvos.finite(1),
                    "in",
                    SalvoConditionTerm.port("in", PortState.non_empty())
                )
            },
            "out_salvo_conditions": {
                "send": SalvoCondition(
                    MaxSalvos.infinite(),
                    "out",
                    SalvoConditionTerm.port("out", PortState.non_empty())
                )
            },
        }

        return NodeFactoryResult(
            node_spec=spec,
            exec_node_func=exec_func,
        )

    def create_node_tuple(name="TupleNode"):
        """A factory function that returns a 5-tuple."""
        def exec_func(ctx, packets):
            pass

        spec = {
            "name": name,
            "in_ports": {"in": Port()},
            "out_ports": {"out": Port()},
        }

        return (spec, exec_func, None, None, None)

    def create_node_pair(name="PairNode"):
        """A factory function that returns a 2-tuple of (spec, 4-tuple)."""
        def exec_func(ctx, packets):
            pass

        spec = {
            "name": name,
            "in_ports": {"in": Port()},
            "out_ports": {"out": Port()},
        }

        return (spec, (exec_func, None, None, None))

    module.create_processor = create_processor
    module.create_node_tuple = create_node_tuple
    module.create_node_pair = create_node_pair

    return module


def _create_invalid_factory_module():
    """Create a factory module with invalid structure."""
    module = types.ModuleType("test_invalid_factory")

    def get_node_spec():
        return {"name": "Test"}

    # Missing get_node_funcs!

    module.get_node_spec = get_node_spec

    return module


@pytest.fixture
def test_factory_module():
    """Register test factory module."""
    module = _create_test_factory_module()
    sys.modules["test_factory_module"] = module
    yield module
    del sys.modules["test_factory_module"]


@pytest.fixture
def factory_func_module():
    """Register factory function module."""
    module = _create_factory_function_module()
    sys.modules["test_factory_func_module"] = module
    yield module
    del sys.modules["test_factory_func_module"]


@pytest.fixture
def invalid_factory_module():
    """Register invalid factory module."""
    module = _create_invalid_factory_module()
    sys.modules["test_invalid_factory"] = module
    yield module
    del sys.modules["test_invalid_factory"]


# ============================================================================
# NodeFactoryResult Tests
# ============================================================================

class TestNodeFactoryResult:
    """Tests for NodeFactoryResult dataclass."""

    def test_create_with_defaults(self):
        """Test creating a result with default values."""
        spec = {"name": "Test", "in_ports": {}, "out_ports": {}}
        result = NodeFactoryResult(node_spec=spec)

        assert result.node_spec == spec
        assert result.exec_node_func is None
        assert result.start_node_func is None
        assert result.stop_node_func is None
        assert result.exec_failed_node_func is None
        assert result.factory_path is None
        assert result.factory_args is None

    def test_create_with_all_funcs(self):
        """Test creating a result with all functions."""
        spec = {"name": "Test"}
        exec_fn = lambda ctx, pkts: None
        start_fn = lambda net: None
        stop_fn = lambda net: None
        failed_fn = lambda ctx: None

        result = NodeFactoryResult(
            node_spec=spec,
            exec_node_func=exec_fn,
            start_node_func=start_fn,
            stop_node_func=stop_fn,
            exec_failed_node_func=failed_fn,
        )

        assert result.exec_node_func is exec_fn
        assert result.start_node_func is start_fn
        assert result.stop_node_func is stop_fn
        assert result.exec_failed_node_func is failed_fn

    def test_factory_info_stored(self):
        """Test that factory path and args are stored."""
        spec = {"name": "Test"}
        result = NodeFactoryResult(
            node_spec=spec,
            factory_path="my_module.my_factory",
            factory_args={"arg1": 1, "arg2": "value"},
        )

        assert result.factory_path == "my_module.my_factory"
        assert result.factory_args == {"arg1": 1, "arg2": "value"}


# ============================================================================
# load_factory Tests
# ============================================================================

class TestLoadFactory:
    """Tests for load_factory function."""

    def test_load_module_factory(self, test_factory_module):
        """Test loading a factory module with get_node_spec/get_node_funcs."""
        result = load_factory("test_factory_module", name="MyNode", num_ports=2)

        assert result.node_spec["name"] == "MyNode"
        assert "in0" in result.node_spec["in_ports"]
        assert "in1" in result.node_spec["in_ports"]
        assert result.exec_node_func is not None
        assert result.start_node_func is not None
        assert result.stop_node_func is not None
        assert result.exec_failed_node_func is not None
        assert result.factory_path == "test_factory_module"
        assert result.factory_args == {"name": "MyNode", "num_ports": 2}

    def test_load_factory_function_returning_result(self, factory_func_module):
        """Test loading a factory function that returns NodeFactoryResult."""
        result = load_factory("test_factory_func_module.create_processor", prefix="data")

        assert result.node_spec["name"] == "data_node"
        assert result.exec_node_func is not None
        assert result.factory_path == "test_factory_func_module.create_processor"
        assert result.factory_args == {"prefix": "data"}

    def test_load_factory_function_returning_tuple5(self, factory_func_module):
        """Test loading a factory function that returns a 5-tuple."""
        result = load_factory("test_factory_func_module.create_node_tuple", name="Custom")

        assert result.node_spec["name"] == "Custom"
        assert result.exec_node_func is not None
        assert result.start_node_func is None
        assert result.stop_node_func is None
        assert result.exec_failed_node_func is None

    def test_load_factory_function_returning_tuple2(self, factory_func_module):
        """Test loading a factory function that returns a 2-tuple."""
        result = load_factory("test_factory_func_module.create_node_pair", name="Pair")

        assert result.node_spec["name"] == "Pair"
        assert result.exec_node_func is not None

    def test_factory_not_found(self):
        """Test error when factory module doesn't exist."""
        with pytest.raises(FactoryNotFoundError, match="Cannot import"):
            load_factory("nonexistent_module.factory")

    def test_factory_missing_get_node_funcs(self, invalid_factory_module):
        """Test error when factory module is missing get_node_funcs."""
        with pytest.raises(InvalidFactoryError, match="missing required function"):
            load_factory("test_invalid_factory")


class TestLoadFactoryValidation:
    """Tests for factory validation."""

    def test_invalid_return_type(self):
        """Test error when factory function returns invalid type."""
        module = types.ModuleType("bad_factory")
        module.bad_func = lambda: "not a valid return"
        sys.modules["bad_factory"] = module

        try:
            with pytest.raises(InvalidFactoryError, match="must return"):
                load_factory("bad_factory.bad_func")
        finally:
            del sys.modules["bad_factory"]

    def test_invalid_tuple_length(self):
        """Test error when factory returns wrong tuple length."""
        module = types.ModuleType("bad_tuple_factory")
        module.bad_func = lambda: ({}, "extra", "args")
        sys.modules["bad_tuple_factory"] = module

        try:
            with pytest.raises(InvalidFactoryError, match="invalid tuple"):
                load_factory("bad_tuple_factory.bad_func")
        finally:
            del sys.modules["bad_tuple_factory"]


# ============================================================================
# create_node_from_factory Tests
# ============================================================================

class TestCreateNodeFromFactory:
    """Tests for create_node_from_factory function."""

    def test_create_node(self, test_factory_module):
        """Test creating a Node from a factory."""
        node, result = create_node_from_factory(
            "test_factory_module",
            name="CreatedNode",
            num_ports=1
        )

        assert isinstance(node, Node)
        assert node.name == "CreatedNode"
        assert result.exec_node_func is not None


# ============================================================================
# Utility Function Tests
# ============================================================================

class TestUtilityFunctions:
    """Tests for utility functions."""

    def test_is_json_serializable_primitives(self):
        """Test JSON serializable check with primitives."""
        assert is_json_serializable(1)
        assert is_json_serializable(1.5)
        assert is_json_serializable("string")
        assert is_json_serializable(True)
        assert is_json_serializable(None)

    def test_is_json_serializable_collections(self):
        """Test JSON serializable check with collections."""
        assert is_json_serializable([1, 2, 3])
        assert is_json_serializable({"key": "value"})
        assert is_json_serializable({"nested": {"list": [1, 2, 3]}})

    def test_is_json_serializable_non_serializable(self):
        """Test JSON serializable check with non-serializable values."""
        assert not is_json_serializable(lambda x: x)
        assert not is_json_serializable(object())

        class CustomClass:
            pass

        assert not is_json_serializable(CustomClass())

    def test_validate_factory_args_valid(self):
        """Test validate_factory_args with valid args."""
        # Should not raise
        validate_factory_args({"a": 1, "b": "string", "c": [1, 2, 3]})

    def test_validate_factory_args_callable(self):
        """Test validate_factory_args rejects callables."""
        with pytest.raises(ValueError, match="callable"):
            validate_factory_args({"func": lambda x: x})

    def test_validate_factory_args_non_serializable(self):
        """Test validate_factory_args rejects non-serializable."""
        with pytest.raises(ValueError, match="not JSON-serializable"):
            validate_factory_args({"obj": object()})


# ============================================================================
# Net.from_factory Tests
# ============================================================================

class TestNetFromFactory:
    """Tests for Net.from_factory static method."""

    def test_from_factory_returns_tuple(self, test_factory_module):
        """Test that from_factory returns a 5-tuple."""
        result = Net.from_factory("test_factory_module", name="FactoryNode")

        assert isinstance(result, tuple)
        assert len(result) == 5

        spec, exec_fn, start_fn, stop_fn, failed_fn = result
        assert isinstance(spec, dict)
        assert spec["name"] == "FactoryNode"
        assert callable(exec_fn)
        assert callable(start_fn)
        assert callable(stop_fn)
        assert callable(failed_fn)

    def test_from_factory_with_args(self, test_factory_module):
        """Test from_factory passes args correctly."""
        spec, _, _, _, _ = Net.from_factory(
            "test_factory_module",
            name="CustomNode",
            num_ports=3
        )

        assert spec["name"] == "CustomNode"
        assert "in0" in spec["in_ports"]
        assert "in1" in spec["in_ports"]
        assert "in2" in spec["in_ports"]

    def test_from_factory_create_and_use_node(self, test_factory_module):
        """Test creating a node from factory and using in a Net."""
        spec, exec_fn, start_fn, stop_fn, failed_fn = Net.from_factory(
            "test_factory_module",
            name="Source",
            num_ports=0
        )

        # Create source node
        source = Node(**spec)

        # Create sink node manually
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
                PortRef("Sink", PortType.Input, "in")
            )
        ]

        graph = Graph([source, sink], edges)
        net = Net(graph)

        # Set exec functions
        results = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet({"from": "factory"})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    results.append(ctx.consume_packet(pkt))

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)

        net.inject_source_epoch("Source")
        net.start()

        assert len(results) == 1
        assert results[0]["from"] == "factory"


# ============================================================================
# DSL Factory Integration Tests
# ============================================================================

class TestDSLFactoryIntegration:
    """Tests for factory info in DSL parsing and serialization."""

    def test_parse_toml_with_factory(self):
        """Test parsing TOML with factory definition."""
        toml_str = """
[nodes.Processor]
factory = "my_module.create_processor"
factory_args = { num_inputs = 3, timeout = 30 }
in_ports = { in = {} }
out_ports = { out = {} }

[[edges]]
from = "Source.out"
to = "Processor.in"

[nodes.Source]
out_ports = { out = {} }
"""
        config = parse_toml_string(toml_str)

        assert "Processor" in config.node_factories
        assert config.node_factories["Processor"]["factory"] == "my_module.create_processor"
        assert config.node_factories["Processor"]["factory_args"] == {
            "num_inputs": 3,
            "timeout": 30
        }

    def test_parse_toml_factory_only(self):
        """Test parsing TOML with factory but no factory_args."""
        toml_str = """
[nodes.Simple]
factory = "simple_factory"
in_ports = { in = {} }
out_ports = { out = {} }
"""
        config = parse_toml_string(toml_str)

        assert "Simple" in config.node_factories
        assert config.node_factories["Simple"]["factory"] == "simple_factory"
        assert "factory_args" not in config.node_factories["Simple"]

    def test_serialize_toml_with_factory(self):
        """Test serializing NetDSLConfig with factory info."""
        # Create a simple graph
        node = Node(name="Test", in_ports={"in": Port()}, out_ports={"out": Port()})
        graph = Graph([node], [])

        config = NetDSLConfig(
            graph=graph,
            node_factories={
                "Test": {
                    "factory": "my_module.my_factory",
                    "factory_args": {"size": 10, "mode": "fast"},
                }
            }
        )

        toml_str = net_config_to_toml(config)

        assert 'factory = "my_module.my_factory"' in toml_str
        assert "factory_args" in toml_str
        assert "size = 10" in toml_str
        assert 'mode = "fast"' in toml_str

    def test_roundtrip_factory_info(self):
        """Test that factory info survives TOML roundtrip."""
        # Create a simple graph
        node = Node(name="Test", in_ports={"in": Port()}, out_ports={"out": Port()})
        graph = Graph([node], [])

        config1 = NetDSLConfig(
            graph=graph,
            node_factories={
                "Test": {
                    "factory": "test_module.create_node",
                    "factory_args": {"count": 5},
                }
            }
        )

        # Serialize and parse
        toml_str = net_config_to_toml(config1)
        config2 = parse_toml_string(toml_str)

        assert config2.node_factories["Test"]["factory"] == "test_module.create_node"
        assert config2.node_factories["Test"]["factory_args"] == {"count": 5}


class TestNetFromDSLWithFactory:
    """Tests for Net creation from DSL with factory info."""

    def test_from_toml_with_factory(self, test_factory_module):
        """Test creating Net from TOML with factory, resolving funcs."""
        toml_str = """
[nodes.Source]
factory = "test_factory_module"
factory_args = { name = "Source", num_ports = 0 }
out_ports = { out = {} }

[nodes.Source.out_salvo_conditions.send]
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
to = "Sink.in"
"""
        net = Net.from_toml(toml_str, resolve_funcs=True)

        # Factory should have set the exec function
        assert "Source" in net._node_exec_funcs
        assert net._node_exec_funcs["Source"].exec_func is not None

    def test_from_toml_without_resolving(self, test_factory_module):
        """Test creating Net from TOML without resolving factory funcs."""
        toml_str = """
[nodes.Source]
factory = "test_factory_module"
factory_args = { name = "Source", num_ports = 0 }
out_ports = { out = {} }

[nodes.Source.out_salvo_conditions.send]
max_salvos = "infinite"
ports = "out"
when = "nonempty(out)"
"""
        net = Net.from_toml(toml_str, resolve_funcs=False)

        # Factory should NOT have set the exec function
        assert "Source" not in net._node_exec_funcs


# ============================================================================
# Integration Tests
# ============================================================================

class TestFactoryIntegration:
    """Integration tests for factory functionality."""

    def test_full_pipeline_with_factory(self, test_factory_module):
        """Test a complete pipeline using factory-created nodes."""
        # Create processor node using factory
        proc_spec, proc_exec, _, _, _ = Net.from_factory(
            "test_factory_module",
            name="Processor",
            num_ports=1
        )
        processor = Node(**proc_spec)

        # Create source and sink manually
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
                PortRef("Processor", PortType.Input, "in0")
            ),
            Edge(
                PortRef("Processor", PortType.Output, "out"),
                PortRef("Sink", PortType.Input, "in")
            ),
        ]

        graph = Graph([source, processor, sink], edges)
        net = Net(graph)

        results = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet({"value": 42})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    results.append(ctx.consume_packet(pkt))

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Processor", proc_exec)
        net.set_node_exec("Sink", sink_exec)

        net.inject_source_epoch("Source")
        net.start()

        assert len(results) == 1
        assert results[0]["value"] == 42

    def test_factory_with_dsl_roundtrip(self, test_factory_module):
        """Test that factory info is preserved through DSL roundtrip."""
        # Create nodes using factory
        source_spec, _, _, _, _ = Net.from_factory(
            "test_factory_module",
            name="Source",
            num_ports=0
        )
        source = Node(**source_spec)

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
                PortRef("Sink", PortType.Input, "in")
            ),
        ]

        graph = Graph([source, sink], edges)

        # Create DSL config with factory info
        config1 = NetDSLConfig(
            graph=graph,
            node_factories={
                "Source": {
                    "factory": "test_factory_module",
                    "factory_args": {"name": "Source", "num_ports": 0},
                }
            }
        )

        # Roundtrip through TOML
        toml_str = net_config_to_toml(config1)
        config2 = parse_toml_string(toml_str)

        # Factory info should be preserved
        assert "Source" in config2.node_factories
        assert config2.node_factories["Source"]["factory"] == "test_factory_module"
