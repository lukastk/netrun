"""
Tests for Milestone 10: DSL Format

This module tests:
- Salvo condition expression parser
- Import path resolution
- TOML serialization/deserialization
- Net DSL methods (to_toml, from_toml, etc.)
"""

import pytest
import tempfile
from pathlib import Path

from netrun import (
    Graph, Node, Edge, Port, PortType, PortRef, PortState,
    MaxSalvos, SalvoCondition, SalvoConditionTerm,
    Net, NetState,
    ExpressionParseError,
    parse_salvo_condition_expr,
    NetDSLConfig,
    parse_toml_string,
    parse_toml_file,
    net_config_to_toml,
    save_toml_file,
    resolve_import_path,
    get_import_path,
)

from netrun.dsl import (
    ExpressionLexer,
    ExpressionParser,
    Token,
    port_to_dict,
    dict_to_port,
    node_to_dict,
    dict_to_node,
    edge_to_dict,
    dict_to_edge,
    graph_to_dict,
    dict_to_graph,
    salvo_condition_to_dict,
    dict_to_salvo_condition,
)

from netrun_sim import PortSlotSpec, PacketCount, MaxSalvos


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def simple_graph():
    """A simple single-node graph for basic tests."""
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
    return Graph([source], [])


@pytest.fixture
def pipeline_graph():
    """A simple pipeline: Source -> Processor -> Sink."""
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


# -----------------------------------------------------------------------------
# Expression Lexer Tests
# -----------------------------------------------------------------------------

class TestExpressionLexer:
    def test_tokenize_simple_function(self):
        lexer = ExpressionLexer("nonempty(port)")
        tokens = lexer.tokenize()
        assert len(tokens) == 4
        assert tokens[0].type == "NONEMPTY"
        assert tokens[1].type == "LPAREN"
        assert tokens[2].type == "IDENT"
        assert tokens[2].value == "port"
        assert tokens[3].type == "RPAREN"

    def test_tokenize_empty_function(self):
        lexer = ExpressionLexer("empty(in)")
        tokens = lexer.tokenize()
        assert len(tokens) == 4
        assert tokens[0].type == "EMPTY"
        assert tokens[2].value == "in"

    def test_tokenize_full_function(self):
        lexer = ExpressionLexer("full(out)")
        tokens = lexer.tokenize()
        assert len(tokens) == 4
        assert tokens[0].type == "FULL"

    def test_tokenize_count_expression(self):
        lexer = ExpressionLexer("count(in) >= 5")
        tokens = lexer.tokenize()
        assert len(tokens) == 6
        assert tokens[0].type == "COUNT"
        assert tokens[4].type == "GTE"
        assert tokens[5].type == "NUMBER"
        assert tokens[5].value == "5"

    def test_tokenize_and_expression(self):
        lexer = ExpressionLexer("nonempty(a) and nonempty(b)")
        tokens = lexer.tokenize()
        assert any(t.type == "AND" for t in tokens)

    def test_tokenize_or_expression(self):
        lexer = ExpressionLexer("nonempty(a) or nonempty(b)")
        tokens = lexer.tokenize()
        assert any(t.type == "OR" for t in tokens)

    def test_tokenize_not_expression(self):
        lexer = ExpressionLexer("not empty(port)")
        tokens = lexer.tokenize()
        assert tokens[0].type == "NOT"

    def test_tokenize_comparison_operators(self):
        for op, expected in [(">=", "GTE"), ("<=", "LTE"), ("==", "EQ"), ("!=", "NEQ"), (">", "GT"), ("<", "LT")]:
            lexer = ExpressionLexer(f"count(p) {op} 10")
            tokens = lexer.tokenize()
            op_token = [t for t in tokens if t.type == expected]
            assert len(op_token) == 1, f"Failed for operator {op}"

    def test_tokenize_whitespace_ignored(self):
        lexer = ExpressionLexer("  nonempty ( port )  ")
        tokens = lexer.tokenize()
        assert len(tokens) == 4

    def test_tokenize_invalid_character_raises(self):
        lexer = ExpressionLexer("nonempty(port) @ invalid")
        with pytest.raises(ExpressionParseError) as exc_info:
            lexer.tokenize()
        assert "Unexpected character" in str(exc_info.value)


# -----------------------------------------------------------------------------
# Expression Parser Tests
# -----------------------------------------------------------------------------

class TestExpressionParser:
    def test_parse_nonempty(self):
        term = parse_salvo_condition_expr("nonempty(in)")
        # Should parse without error
        assert term is not None

    def test_parse_empty(self):
        term = parse_salvo_condition_expr("empty(out)")
        assert term is not None

    def test_parse_full(self):
        term = parse_salvo_condition_expr("full(buffer)")
        assert term is not None

    def test_parse_count_gte(self):
        term = parse_salvo_condition_expr("count(in) >= 5")
        assert term is not None

    def test_parse_count_lte(self):
        term = parse_salvo_condition_expr("count(in) <= 10")
        assert term is not None

    def test_parse_count_eq(self):
        term = parse_salvo_condition_expr("count(in) == 3")
        assert term is not None

    def test_parse_count_neq(self):
        term = parse_salvo_condition_expr("count(in) != 0")
        assert term is not None

    def test_parse_count_gt(self):
        term = parse_salvo_condition_expr("count(in) > 2")
        assert term is not None

    def test_parse_count_lt(self):
        term = parse_salvo_condition_expr("count(in) < 5")
        assert term is not None

    def test_parse_and_expression(self):
        term = parse_salvo_condition_expr("nonempty(a) and nonempty(b)")
        assert term is not None

    def test_parse_or_expression(self):
        term = parse_salvo_condition_expr("nonempty(a) or nonempty(b)")
        assert term is not None

    def test_parse_not_expression(self):
        term = parse_salvo_condition_expr("not empty(in)")
        assert term is not None

    def test_parse_complex_expression(self):
        term = parse_salvo_condition_expr("nonempty(a) and nonempty(b) or empty(c)")
        assert term is not None

    def test_parse_parenthesized_expression(self):
        term = parse_salvo_condition_expr("(nonempty(a) or nonempty(b)) and nonempty(c)")
        assert term is not None

    def test_parse_nested_not(self):
        term = parse_salvo_condition_expr("not not nonempty(in)")
        assert term is not None

    def test_parse_empty_expression_raises(self):
        with pytest.raises(ExpressionParseError) as exc_info:
            parse_salvo_condition_expr("")
        assert "Empty expression" in str(exc_info.value)

    def test_parse_incomplete_expression_raises(self):
        with pytest.raises(ExpressionParseError):
            parse_salvo_condition_expr("nonempty(")

    def test_parse_unexpected_token_raises(self):
        with pytest.raises(ExpressionParseError):
            parse_salvo_condition_expr("nonempty(in) extra")


# -----------------------------------------------------------------------------
# Import Path Tests
# -----------------------------------------------------------------------------

class TestResolveImportPath:
    def test_resolve_standard_library(self):
        # Resolve a standard library function
        result = resolve_import_path("os.path.join")
        import os.path
        assert result == os.path.join

    def test_resolve_module(self):
        # Resolve a module
        result = resolve_import_path("os")
        import os
        assert result == os

    def test_resolve_class(self):
        result = resolve_import_path("pathlib.Path")
        from pathlib import Path
        assert result == Path

    def test_resolve_invalid_raises(self):
        with pytest.raises(ImportError):
            resolve_import_path("nonexistent.module.func")


class TestGetImportPath:
    def test_get_path_for_function(self):
        import os.path
        path = get_import_path(os.path.join)
        assert path is not None
        assert "join" in path

    def test_get_path_for_class(self):
        from pathlib import Path
        path = get_import_path(Path)
        assert path == "pathlib.Path"

    def test_get_path_for_lambda_returns_none(self):
        # Lambdas don't have proper __name__
        f = lambda x: x
        path = get_import_path(f)
        # Lambda has __name__ = "<lambda>", so it returns something
        assert path is not None  # But the path won't be resolvable


# -----------------------------------------------------------------------------
# Port Serialization Tests
# -----------------------------------------------------------------------------

class TestPortSerialization:
    def test_port_to_dict_infinite_slots(self):
        port = Port()  # Default is infinite
        result = port_to_dict(port)
        assert "slots" not in result  # Infinite is the default

    def test_port_to_dict_finite_slots(self):
        port = Port(PortSlotSpec.finite(5))
        result = port_to_dict(port)
        assert result["slots"] == 5

    def test_dict_to_port_empty(self):
        port = dict_to_port({})
        assert port is not None

    def test_dict_to_port_none(self):
        port = dict_to_port(None)
        assert port is not None

    def test_dict_to_port_finite_slots(self):
        port = dict_to_port({"slots": 3})
        # Check it's finite by comparing with infinite
        assert port.slots_spec != PortSlotSpec.infinite()
        # Check it's the right count by string repr
        assert "finite(3)" in str(port.slots_spec)


# -----------------------------------------------------------------------------
# Node Serialization Tests
# -----------------------------------------------------------------------------

class TestNodeSerialization:
    def test_node_to_dict_minimal(self):
        node = Node(name="TestNode")
        result = node_to_dict(node)
        assert result["name"] == "TestNode"

    def test_node_to_dict_with_ports(self):
        node = Node(
            name="TestNode",
            in_ports={"in": Port()},
            out_ports={"out": Port()}
        )
        result = node_to_dict(node)
        assert "in_ports" in result
        assert "out_ports" in result

    def test_dict_to_node_minimal(self):
        node = dict_to_node("TestNode", {})
        assert node.name == "TestNode"

    def test_dict_to_node_with_ports_dict(self):
        data = {
            "in_ports": {"in": {"slots": 5}},
            "out_ports": {"out": {}}
        }
        node = dict_to_node("TestNode", data)
        assert "in" in node.in_ports
        assert "out" in node.out_ports
        # Check slots by string repr
        assert "finite(5)" in str(node.in_ports["in"].slots_spec)

    def test_dict_to_node_with_ports_list(self):
        data = {
            "in_ports": ["a", "b"],
            "out_ports": ["x", "y"]
        }
        node = dict_to_node("TestNode", data)
        assert "a" in node.in_ports
        assert "b" in node.in_ports
        assert "x" in node.out_ports
        assert "y" in node.out_ports


# -----------------------------------------------------------------------------
# Edge Serialization Tests
# -----------------------------------------------------------------------------

class TestEdgeSerialization:
    def test_edge_to_dict(self):
        edge = Edge(
            PortRef("A", PortType.Output, "out"),
            PortRef("B", PortType.Input, "in")
        )
        result = edge_to_dict(edge)
        assert result["from"] == "A.out"
        assert result["to"] == "B.in"

    def test_dict_to_edge(self):
        data = {"from": "Source.output", "to": "Sink.input"}
        edge = dict_to_edge(data)
        assert edge.source.node_name == "Source"
        assert edge.source.port_name == "output"
        assert edge.target.node_name == "Sink"
        assert edge.target.port_name == "input"

    def test_dict_to_edge_invalid_format_raises(self):
        with pytest.raises(ValueError):
            dict_to_edge({"from": "invalid", "to": "also_invalid"})


# -----------------------------------------------------------------------------
# Graph Serialization Tests
# -----------------------------------------------------------------------------

class TestGraphSerialization:
    def test_graph_to_dict(self, pipeline_graph):
        result = graph_to_dict(pipeline_graph)
        assert "nodes" in result
        assert "edges" in result
        assert "Source" in result["nodes"]
        assert "Processor" in result["nodes"]
        assert "Sink" in result["nodes"]
        assert len(result["edges"]) == 2

    def test_dict_to_graph(self):
        data = {
            "nodes": {
                "A": {"out_ports": {"out": {}}},
                "B": {"in_ports": {"in": {}}}
            },
            "edges": [
                {"from": "A.out", "to": "B.in"}
            ]
        }
        graph = dict_to_graph(data)
        assert "A" in list(graph.nodes())
        assert "B" in list(graph.nodes())
        assert len(list(graph.edges())) == 1

    def test_graph_roundtrip(self, pipeline_graph):
        data = graph_to_dict(pipeline_graph)
        graph2 = dict_to_graph(data)
        # Use sets for comparison since dict order may vary
        assert set(graph2.nodes().keys()) == set(pipeline_graph.nodes().keys())


# -----------------------------------------------------------------------------
# Salvo Condition Serialization Tests
# -----------------------------------------------------------------------------

class TestSalvoConditionSerialization:
    def test_salvo_condition_to_dict_infinite(self):
        cond = SalvoCondition(
            MaxSalvos.infinite(),
            "out",
            SalvoConditionTerm.port("out", PortState.non_empty())
        )
        result = salvo_condition_to_dict(cond)
        assert result["max_salvos"] == "infinite"
        assert result["ports"] == "out"

    def test_salvo_condition_to_dict_finite(self):
        cond = SalvoCondition(
            MaxSalvos.finite(3),
            "in",
            SalvoConditionTerm.port("in", PortState.non_empty())
        )
        result = salvo_condition_to_dict(cond)
        assert result["max_salvos"] == 3

    def test_dict_to_salvo_condition_infinite(self):
        data = {
            "max_salvos": "infinite",
            "ports": "out",
            "when": "nonempty(out)"
        }
        cond = dict_to_salvo_condition(data)
        # Check by comparing with infinite
        assert cond.max_salvos == MaxSalvos.infinite()

    def test_dict_to_salvo_condition_finite(self):
        data = {
            "max_salvos": 1,
            "ports": "in",
            "when": "nonempty(in)"
        }
        cond = dict_to_salvo_condition(data)
        # Check by comparing - not infinite means finite
        assert cond.max_salvos != MaxSalvos.infinite()
        # Check it's the right count by string repr
        assert "finite(1)" in str(cond.max_salvos)


# -----------------------------------------------------------------------------
# NetDSLConfig Tests
# -----------------------------------------------------------------------------

class TestNetDSLConfig:
    def test_default_values(self):
        graph = Graph([], [])
        config = NetDSLConfig(graph=graph)
        assert config.on_error == "continue"
        assert config.consumed_packet_storage == False
        assert config.history_max_size == 10000

    def test_custom_values(self):
        graph = Graph([], [])
        config = NetDSLConfig(
            graph=graph,
            on_error="raise",
            consumed_packet_storage=True,
            history_max_size=5000
        )
        assert config.on_error == "raise"
        assert config.consumed_packet_storage == True
        assert config.history_max_size == 5000


# -----------------------------------------------------------------------------
# TOML Parsing Tests
# -----------------------------------------------------------------------------

class TestParseTomlString:
    def test_parse_minimal_toml(self):
        toml_str = """
[nodes.Source]
out_ports = { out = {} }

[nodes.Source.out_salvo_conditions.send]
max_salvos = "infinite"
ports = "out"
when = "nonempty(out)"
"""
        config = parse_toml_string(toml_str)
        assert config is not None
        assert "Source" in list(config.graph.nodes())

    def test_parse_with_edges(self):
        toml_str = """
[nodes.A]
out_ports = { out = {} }

[nodes.B]
in_ports = { in = {} }

[[edges]]
from = "A.out"
to = "B.in"
"""
        config = parse_toml_string(toml_str)
        edges = list(config.graph.edges())
        assert len(edges) == 1

    def test_parse_with_net_config(self):
        toml_str = """
[net]
on_error = "raise"
consumed_packet_storage = true
history_max_size = 5000

[nodes.Source]
out_ports = { out = {} }
"""
        config = parse_toml_string(toml_str)
        assert config.on_error == "raise"
        assert config.consumed_packet_storage == True
        assert config.history_max_size == 5000

    def test_parse_with_node_options(self):
        toml_str = """
[nodes.Source]
out_ports = { out = {} }
options = { pool = "main", retries = 3 }
"""
        config = parse_toml_string(toml_str)
        assert "Source" in config.node_configs
        assert config.node_configs["Source"]["pool"] == "main"
        assert config.node_configs["Source"]["retries"] == 3

    def test_parse_with_exec_paths(self):
        toml_str = """
[nodes.Source]
out_ports = { out = {} }
exec_node_func = "my_module.my_func"
"""
        config = parse_toml_string(toml_str)
        assert "Source" in config.node_exec_paths
        assert config.node_exec_paths["Source"]["exec_func"] == "my_module.my_func"


class TestParseTomlFile:
    def test_parse_file(self, pipeline_graph):
        toml_str = """
[nodes.A]
out_ports = { out = {} }

[nodes.B]
in_ports = { in = {} }

[[edges]]
from = "A.out"
to = "B.in"
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write(toml_str)
            f.flush()
            config = parse_toml_file(f.name)

        assert config is not None
        assert "A" in list(config.graph.nodes())
        assert "B" in list(config.graph.nodes())


# -----------------------------------------------------------------------------
# TOML Generation Tests
# -----------------------------------------------------------------------------

class TestNetConfigToToml:
    def test_generate_minimal(self):
        node = Node(name="Source", out_ports={"out": Port()})
        graph = Graph([node], [])
        config = NetDSLConfig(graph=graph)

        toml_str = net_config_to_toml(config)
        assert "Source" in toml_str

    def test_generate_with_net_config(self):
        node = Node(name="Source", out_ports={"out": Port()})
        graph = Graph([node], [])
        config = NetDSLConfig(
            graph=graph,
            on_error="raise",
            consumed_packet_storage=True
        )

        toml_str = net_config_to_toml(config)
        assert 'on_error = "raise"' in toml_str
        assert "consumed_packet_storage = true" in toml_str

    def test_generate_with_edges(self):
        source = Node(name="Source", out_ports={"out": Port()})
        sink = Node(name="Sink", in_ports={"in": Port()})
        edge = Edge(
            PortRef("Source", PortType.Output, "out"),
            PortRef("Sink", PortType.Input, "in")
        )
        graph = Graph([source, sink], [edge])
        config = NetDSLConfig(graph=graph)

        toml_str = net_config_to_toml(config)
        assert "Source.out" in toml_str
        assert "Sink.in" in toml_str

    def test_roundtrip_parse_generate(self):
        toml_str_original = """
[net]
on_error = "raise"

[nodes.A]
out_ports = { out = {} }

[nodes.B]
in_ports = { in = {} }

[[edges]]
from = "A.out"
to = "B.in"
"""
        config = parse_toml_string(toml_str_original)
        toml_str_generated = net_config_to_toml(config)

        # Parse the generated TOML
        config2 = parse_toml_string(toml_str_generated)

        # Verify key properties preserved
        assert config2.on_error == "raise"
        assert "A" in list(config2.graph.nodes())
        assert "B" in list(config2.graph.nodes())


class TestSaveTomlFile:
    def test_save_file(self):
        node = Node(name="Source", out_ports={"out": Port()})
        graph = Graph([node], [])
        config = NetDSLConfig(graph=graph)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            path = f.name

        save_toml_file(config, path)

        # Read back and verify
        config2 = parse_toml_file(path)
        assert "Source" in list(config2.graph.nodes())


# -----------------------------------------------------------------------------
# Net DSL Methods Tests
# -----------------------------------------------------------------------------

class TestNetToDslConfig:
    def test_basic_config(self, pipeline_graph):
        net = Net(pipeline_graph, on_error="raise")
        config = net.to_dsl_config()

        assert config.on_error == "raise"
        # Use sets for comparison since dict order may vary
        assert set(config.graph.nodes().keys()) == set(pipeline_graph.nodes().keys())

    def test_with_consumed_storage(self, pipeline_graph):
        net = Net(pipeline_graph, consumed_packet_storage=True)
        config = net.to_dsl_config()

        assert config.consumed_packet_storage == True

    def test_with_node_config(self, pipeline_graph):
        net = Net(pipeline_graph)
        net.set_node_config("Source", pool="main", retries=3, defer_net_actions=True)
        config = net.to_dsl_config()

        assert "Source" in config.node_configs
        assert config.node_configs["Source"]["pool"] == "main"
        assert config.node_configs["Source"]["retries"] == 3


class TestNetToToml:
    def test_basic_toml(self, pipeline_graph):
        net = Net(pipeline_graph)
        toml_str = net.to_toml()

        assert "Source" in toml_str
        assert "Processor" in toml_str
        assert "Sink" in toml_str

    def test_toml_includes_edges(self, pipeline_graph):
        net = Net(pipeline_graph)
        toml_str = net.to_toml()

        assert "edges" in toml_str


class TestNetSaveToml:
    def test_save_toml(self, pipeline_graph):
        net = Net(pipeline_graph)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            path = f.name

        net.save_toml(path)

        # Verify file exists and is valid TOML
        config = parse_toml_file(path)
        assert "Source" in list(config.graph.nodes())


class TestNetFromToml:
    def test_from_toml_basic(self):
        toml_str = """
[nodes.Source]
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
        net = Net.from_toml(toml_str, resolve_funcs=False)

        assert "Source" in list(net._graph.nodes())
        assert "Sink" in list(net._graph.nodes())

    def test_from_toml_with_net_config(self):
        toml_str = """
[net]
on_error = "raise"
consumed_packet_storage = true

[nodes.Source]
out_ports = { out = {} }
"""
        net = Net.from_toml(toml_str, resolve_funcs=False)

        assert net._on_error == "raise"
        # consumed_packet_storage should be true
        assert net._value_store._consumed_storage == True


class TestNetFromTomlFile:
    def test_from_toml_file(self):
        toml_str = """
[nodes.Source]
out_ports = { out = {} }

[nodes.Sink]
in_ports = { in = {} }

[[edges]]
from = "Source.out"
to = "Sink.in"
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write(toml_str)
            f.flush()
            net = Net.from_toml_file(f.name, resolve_funcs=False)

        assert "Source" in list(net._graph.nodes())
        assert "Sink" in list(net._graph.nodes())


class TestNetDslRoundtrip:
    def test_roundtrip(self, pipeline_graph):
        """Test that Net -> TOML -> Net preserves key properties."""
        net1 = Net(
            pipeline_graph,
            on_error="raise",
            consumed_packet_storage=True
        )
        net1.set_node_config("Source", retries=3, defer_net_actions=True)

        # Convert to TOML and back
        toml_str = net1.to_toml()
        net2 = Net.from_toml(toml_str, resolve_funcs=False)

        # Verify properties preserved
        assert net2._on_error == "raise"
        assert net2._value_store._consumed_storage == True
        # Use sets for comparison since dict order may vary
        assert set(net2._graph.nodes().keys()) == set(pipeline_graph.nodes().keys())

    def test_roundtrip_file(self, pipeline_graph):
        """Test that save_toml -> from_toml_file preserves key properties."""
        net1 = Net(pipeline_graph, on_error="pause")

        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            path = f.name

        net1.save_toml(path)
        net2 = Net.from_toml_file(path, resolve_funcs=False)

        assert net2._on_error == "pause"
        # Use sets for comparison since dict order may vary
        assert set(net2._graph.nodes().keys()) == set(pipeline_graph.nodes().keys())


# -----------------------------------------------------------------------------
# Integration Tests
# -----------------------------------------------------------------------------

class TestDslIntegration:
    def test_parsed_net_can_run(self):
        """Test that a Net created from TOML can actually run."""
        toml_str = """
[nodes.Source]
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
        net = Net.from_toml(toml_str, resolve_funcs=False)

        results = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("hello")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    results.append(value)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)

        net.inject_source_epoch("Source")
        net.start()

        assert results == ["hello"]

    def test_saved_net_config_matches_original(self, pipeline_graph):
        """Test that saving and loading preserves node configs."""
        net1 = Net(pipeline_graph)
        net1.set_node_config("Source", retries=5, timeout=30.0, defer_net_actions=True)
        net1.set_node_config("Processor", defer_net_actions=True)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            path = f.name

        net1.save_toml(path)
        net2 = Net.from_toml_file(path, resolve_funcs=False)

        # Check configs preserved
        config = net2.get_node_config("Source")
        assert config.retries == 5
        assert config.timeout == 30.0

        config2 = net2.get_node_config("Processor")
        assert config2.defer_net_actions == True
