# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp net.config._graph

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
from pydantic import BaseModel, Field, BeforeValidator
from typing import Annotated, Any
from pathlib import Path

from netrun.net.config._nodes import (
    NodeConfig,
    SubgraphConfig,
    EdgeConfig,
    PortRefConfig,
    EnvVarResolvableModel,
)
import netrun_sim

# %% [markdown]
# # Graph Configuration
#
# The complete graph topology configuration.

# %%
#|export
def _parse_graph_node(value: Any) -> NodeConfig | SubgraphConfig:
    """Parse a graph node, defaulting to NodeConfig when type is not specified."""
    if isinstance(value, (NodeConfig, SubgraphConfig)):
        return value
    if isinstance(value, dict):
        # Check if it's explicitly a subgraph
        if value.get("type") == "subgraph":
            return SubgraphConfig.model_validate(value)
        # Default to NodeConfig (handles both "node" and missing type)
        return NodeConfig.model_validate(value)
    raise ValueError(f"Invalid node config: {value}")

# Type alias for nodes that can be either regular nodes or subgraphs
# Uses BeforeValidator to default to NodeConfig when type is not specified
GraphNodeConfig = Annotated[
    NodeConfig | SubgraphConfig,
    BeforeValidator(_parse_graph_node)
]

# %%
#|export
class GraphConfig(EnvVarResolvableModel):
    """Configuration for a complete flow-based network graph.

    Example:
        config = GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    out_ports={"out": PortConfig()},
                ),
                NodeConfig(
                    name="B",
                    in_ports={"in": PortConfig()},
                    in_salvo_conditions={
                        "default": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={"in": PacketCountAllConfig()},
                            term=SalvoConditionTermPortConfig(
                                port_name="in",
                                state=PortStateNonEmptyConfig(),
                            ),
                        ),
                    },
                ),
            ],
            edges=[
                EdgeConfig(source_str="A.out", target_str="B.in"),
            ],
        )
        graph = config.get_graph()
    """
    nodes: list[GraphNodeConfig]
    """List of nodes (NodeConfig) and/or subgraphs (SubgraphConfig)."""

    edges: list[EdgeConfig] = Field(default_factory=list)

    extra: dict[str, Any] = Field(default_factory=dict)
    """Arbitrary extra data for the graph.

    Can be used to store descriptions, UI viewport state, or any other
    tool-specific data that should be preserved across serialization.
    """

    def resolve(self, base_path: Path | None = None, net_config: 'Any | None' = None) -> "GraphConfig":
        """Return a resolved copy with all subgraphs flattened and nodes resolved.

        This method:
        1. Flattens all SubgraphConfig into NodeConfig with prefixed names
        2. Rewrites edges to use the prefixed names
        3. Adds edges connecting exposed ports to parent graph edges
        4. Resolves all node factories and import paths
        5. Validates no name collisions exist

        Args:
            base_path: Base path for resolving relative file paths in subgraphs.
            net_config: NetConfig instance for resolving project root and passed
                        to factory functions.

        Returns:
            A new GraphConfig with only NodeConfig (no SubgraphConfig) and all
            edges properly connected.

        Raises:
            ValueError: If name collisions are detected after resolution.
        """
        resolved_nodes: list[NodeConfig] = []
        resolved_edges: list[EdgeConfig] = list(self.edges)

        # Track subgraph port mappings for edge rewriting
        subgraph_in_mappings: dict[str, dict[str, str]] = {}
        subgraph_out_mappings: dict[str, dict[str, str]] = {}

        # First pass: resolve subgraphs and collect regular nodes
        for node in self.nodes:
            if isinstance(node, SubgraphConfig):
                # Resolve subgraph to flat nodes and edges
                sg_nodes, sg_edges, in_mapping, out_mapping = node.resolve(base_path=base_path)
                # Resolve factory nodes inside subgraphs (bug fix)
                resolved_nodes.extend(n.resolve(net_config=net_config) for n in sg_nodes)
                resolved_edges.extend(sg_edges)
                subgraph_in_mappings[node.name] = in_mapping
                subgraph_out_mappings[node.name] = out_mapping
            else:
                # Regular node - resolve factories
                resolved = node.resolve(net_config=net_config)
                if isinstance(resolved, SubgraphConfig):
                    # Factory returned a subgraph — flatten it
                    sg_nodes, sg_edges, in_mapping, out_mapping = resolved.resolve(base_path=base_path)
                    resolved_nodes.extend(n.resolve(net_config=net_config) for n in sg_nodes)
                    resolved_edges.extend(sg_edges)
                    subgraph_in_mappings[resolved.name] = in_mapping
                    subgraph_out_mappings[resolved.name] = out_mapping
                else:
                    resolved_nodes.append(resolved)

        # Second pass: rewrite edges that connect to subgraph exposed ports
        final_edges: list[EdgeConfig] = []
        for edge in resolved_edges:
            source = edge.get_source()
            target = edge.get_target()

            new_source = source
            new_target = target

            # Check if source refers to a subgraph's exposed output port
            if source.node_name in subgraph_out_mappings:
                mapping = subgraph_out_mappings[source.node_name]
                if source.port_name in mapping:
                    # Map to internal node.port
                    mapped = mapping[source.port_name]
                    internal_node, internal_port = mapped.rsplit(".", 1)
                    new_source = PortRefConfig(
                        node_name=internal_node,
                        port_type="output",
                        port_name=internal_port,
                    )

            # Check if target refers to a subgraph's exposed input port
            if target.node_name in subgraph_in_mappings:
                mapping = subgraph_in_mappings[target.node_name]
                if target.port_name in mapping:
                    # Map to internal node.port
                    mapped = mapping[target.port_name]
                    internal_node, internal_port = mapped.rsplit(".", 1)
                    new_target = PortRefConfig(
                        node_name=internal_node,
                        port_type="input",
                        port_name=internal_port,
                    )

            final_edges.append(EdgeConfig(source=new_source, target=new_target))

        # Validate no name collisions
        node_names = [n.name for n in resolved_nodes]
        seen_names: set[str] = set()
        for name in node_names:
            if name in seen_names:
                raise ValueError(f"Node name collision after subgraph resolution: '{name}'")
            # Also check for prefix conflicts
            for seen in seen_names:
                if name.startswith(f"{seen}.") or seen.startswith(f"{name}."):
                    raise ValueError(
                        f"Node name prefix collision: '{name}' conflicts with '{seen}'. "
                        "Node names cannot be prefixes of other node names when using subgraphs."
                    )
            seen_names.add(name)

        return GraphConfig(nodes=resolved_nodes, edges=final_edges, extra=self.extra)

    def has_subgraphs(self) -> bool:
        """Check if this graph contains any subgraphs."""
        return any(isinstance(node, SubgraphConfig) for node in self.nodes)

    def get_graph(self) -> netrun_sim.Graph:
        """Convert this config to a netrun_sim.Graph object.

        Raises:
            ValueError: If graph contains SubgraphConfig nodes. Call resolve() first,
                or if graph validation fails (e.g. fan-out from output ports).
        """
        if self.has_subgraphs():
            raise ValueError(
                "Cannot convert GraphConfig with subgraphs to netrun_sim.Graph. "
                "Call resolve() first to flatten subgraphs."
            )
        # At this point, all nodes are NodeConfig
        nodes = [node.to_netrun_sim() for node in self.nodes]  # type: ignore
        edges = [edge.to_netrun_sim() for edge in self.edges]
        graph = netrun_sim.Graph(nodes, edges)

        # Validate graph constraints (e.g. no fan-out from output ports)
        errors = graph.validate()
        if errors:
            msgs = [str(e) for e in errors]
            raise ValueError(
                f"Graph validation failed with {len(errors)} error(s):\n"
                + "\n".join(f"  - {m}" for m in msgs)
            )

        return graph
