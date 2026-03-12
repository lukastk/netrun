"""Subgraph factory that generates a multi-stage processing pipeline.

A factory module whose get_node_config() returns a SubgraphConfig instead
of a NodeConfig, enabling parameterized generation of node groups.
"""

from netrun.net.config import (
    SubgraphConfig,
    NodeConfig,
    PortConfig,
    EdgeConfig,
    ExposedPortConfig,
)


def get_node_config(_net_config=None, *, num_stages: int = 2) -> SubgraphConfig:
    """Create a linear processing pipeline with N stages.

    Each stage has an 'in' input port and an 'out' output port.
    Stages are connected sequentially: stage_0 -> stage_1 -> ... -> stage_N-1.
    The first stage's input and last stage's output are exposed.

    Args:
        num_stages: Number of stages in the pipeline (default 2).
    """
    nodes = [
        NodeConfig(
            name=f"stage_{i}",
            factory="netrun.node_factories.from_function",
            factory_args={"func": "nodes.transform"},
        )
        for i in range(num_stages)
    ]

    edges = [
        EdgeConfig(
            source_node=f"stage_{i}",
            source_port="out",
            target_node=f"stage_{i+1}",
            target_port="data",
        )
        for i in range(num_stages - 1)
    ]

    return SubgraphConfig(
        name="pipeline",
        nodes=nodes,
        edges=edges,
        exposed_in_ports={
            "in": ExposedPortConfig(
                internal_node="stage_0",
                internal_port="data",
            ),
        },
        exposed_out_ports={
            "out": ExposedPortConfig(
                internal_node=f"stage_{num_stages - 1}",
                internal_port="out",
            ),
        },
    )
