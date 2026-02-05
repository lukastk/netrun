"""Tests for fan-in (multiple edges to same input port) support."""

import pytest
from netrun_sim import Graph, Node, Edge, Port, PortRef, PortType


class TestFanIn:
    """Tests for fan-in: multiple edges connecting to the same input port."""

    def test_fan_in_is_valid(self):
        """Multiple edges can connect to the same input port (fan-in allowed)."""
        # Create three nodes: A and B both connect to C
        node_a = Node('A', out_ports={'out': Port()})
        node_b = Node('B', out_ports={'out': Port()})
        node_c = Node('C', in_ports={'in': Port()})

        # Create edges: A.out -> C.in and B.out -> C.in
        edge1 = Edge(
            PortRef('A', PortType.Output, 'out'),
            PortRef('C', PortType.Input, 'in')
        )
        edge2 = Edge(
            PortRef('B', PortType.Output, 'out'),
            PortRef('C', PortType.Input, 'in')
        )

        graph = Graph([node_a, node_b, node_c], [edge1, edge2])

        # Verify no validation errors (fan-in is allowed)
        errors = graph.validate()
        assert len(errors) == 0, f"Expected no errors for fan-in, got: {errors}"

    def test_get_edges_by_head_returns_all_incoming_edges(self):
        """get_edges_by_head should return all edges connecting to an input port."""
        # Create four nodes: A, B, C all connect to D
        node_a = Node('A', out_ports={'out': Port()})
        node_b = Node('B', out_ports={'out': Port()})
        node_c = Node('C', out_ports={'out': Port()})
        node_d = Node('D', in_ports={'in': Port()})

        edges = [
            Edge(PortRef('A', PortType.Output, 'out'), PortRef('D', PortType.Input, 'in')),
            Edge(PortRef('B', PortType.Output, 'out'), PortRef('D', PortType.Input, 'in')),
            Edge(PortRef('C', PortType.Output, 'out'), PortRef('D', PortType.Input, 'in')),
        ]

        graph = Graph([node_a, node_b, node_c, node_d], edges)

        # Get all incoming edges to D.in
        d_in_ref = PortRef('D', PortType.Input, 'in')
        incoming_edges = graph.get_edges_by_head(d_in_ref)

        assert len(incoming_edges) == 3, f"Expected 3 incoming edges, got {len(incoming_edges)}"

        # Verify all sources are present
        sources = {e.source.node_name for e in incoming_edges}
        assert sources == {'A', 'B', 'C'}

    def test_get_edges_by_head_returns_empty_for_unconnected_port(self):
        """get_edges_by_head should return empty list for ports with no incoming edges."""
        node_a = Node('A', in_ports={'in': Port()}, out_ports={'out': Port()})
        graph = Graph([node_a], [])

        a_in_ref = PortRef('A', PortType.Input, 'in')
        incoming_edges = graph.get_edges_by_head(a_in_ref)

        assert len(incoming_edges) == 0, "Unconnected port should have no incoming edges"

    def test_get_edges_by_head_single_edge(self):
        """get_edges_by_head should work correctly with a single incoming edge."""
        node_a = Node('A', out_ports={'out': Port()})
        node_b = Node('B', in_ports={'in': Port()})

        edge = Edge(
            PortRef('A', PortType.Output, 'out'),
            PortRef('B', PortType.Input, 'in')
        )

        graph = Graph([node_a, node_b], [edge])

        b_in_ref = PortRef('B', PortType.Input, 'in')
        incoming_edges = graph.get_edges_by_head(b_in_ref)

        assert len(incoming_edges) == 1
        assert incoming_edges[0].source.node_name == 'A'


class TestFanOut:
    """Tests for fan-out: only one edge allowed per output port."""

    def test_fan_out_is_invalid(self):
        """Multiple edges from the same output port should cause validation error."""
        # Create three nodes: A connects to both B and C (fan-out)
        node_a = Node('A', out_ports={'out': Port()})
        node_b = Node('B', in_ports={'in': Port()})
        node_c = Node('C', in_ports={'in': Port()})

        # Create edges: A.out -> B.in and A.out -> C.in (fan-out not allowed)
        edge1 = Edge(
            PortRef('A', PortType.Output, 'out'),
            PortRef('B', PortType.Input, 'in')
        )
        edge2 = Edge(
            PortRef('A', PortType.Output, 'out'),
            PortRef('C', PortType.Input, 'in')
        )

        graph = Graph([node_a, node_b, node_c], [edge1, edge2])

        # Verify validation error for fan-out
        errors = graph.validate()
        assert len(errors) == 1, f"Expected 1 error for fan-out, got: {len(errors)}"

        # Check error message contains the output port
        error_msg = str(errors[0])
        assert 'A.out.out' in error_msg, f"Error should mention output port, got: {error_msg}"
        assert '2' in error_msg, f"Error should mention 2 edges, got: {error_msg}"

    def test_different_output_ports_can_have_edges(self):
        """Different output ports on the same node can each have one edge."""
        # A has two output ports, each connecting to different nodes
        node_a = Node('A', out_ports={'out1': Port(), 'out2': Port()})
        node_b = Node('B', in_ports={'in': Port()})
        node_c = Node('C', in_ports={'in': Port()})

        edges = [
            Edge(PortRef('A', PortType.Output, 'out1'), PortRef('B', PortType.Input, 'in')),
            Edge(PortRef('A', PortType.Output, 'out2'), PortRef('C', PortType.Input, 'in')),
        ]

        graph = Graph([node_a, node_b, node_c], edges)

        # This should be valid - different output ports
        errors = graph.validate()
        assert len(errors) == 0, f"Expected no errors, got: {errors}"


class TestMixedTopology:
    """Tests for graphs with both fan-in and normal connections."""

    def test_complex_graph_with_fan_in(self):
        """Complex graph with multiple fan-in connections should be valid."""
        # Create a diamond-like graph:
        #   A -> C
        #   B -> C
        #   C -> D
        #   C -> E (using different port)
        # Wait, C -> D and C -> E from same port would be fan-out
        # Let me fix: C has two output ports

        node_a = Node('A', out_ports={'out': Port()})
        node_b = Node('B', out_ports={'out': Port()})
        node_c = Node('C', in_ports={'in': Port()}, out_ports={'out1': Port(), 'out2': Port()})
        node_d = Node('D', in_ports={'in': Port()})
        node_e = Node('E', in_ports={'in': Port()})

        edges = [
            # Fan-in: A and B both connect to C
            Edge(PortRef('A', PortType.Output, 'out'), PortRef('C', PortType.Input, 'in')),
            Edge(PortRef('B', PortType.Output, 'out'), PortRef('C', PortType.Input, 'in')),
            # C connects to D and E via different output ports
            Edge(PortRef('C', PortType.Output, 'out1'), PortRef('D', PortType.Input, 'in')),
            Edge(PortRef('C', PortType.Output, 'out2'), PortRef('E', PortType.Input, 'in')),
        ]

        graph = Graph([node_a, node_b, node_c, node_d, node_e], edges)

        errors = graph.validate()
        assert len(errors) == 0, f"Expected no errors, got: {errors}"

        # Verify fan-in to C
        c_in_ref = PortRef('C', PortType.Input, 'in')
        incoming = graph.get_edges_by_head(c_in_ref)
        assert len(incoming) == 2
        sources = {e.source.node_name for e in incoming}
        assert sources == {'A', 'B'}
