# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %% [markdown]
# # Subgraph Factory Example
#
# This notebook demonstrates how to use **subgraph factories** — factory modules
# whose `get_node_config()` returns a `SubgraphConfig` instead of a `NodeConfig`.
# This lets you programmatically generate groups of interconnected nodes from
# parameters.
#
# We'll cover:
# 1. Basic subgraph factories
# 2. Parameterized pipelines
# 3. Composing subgraph factories with regular nodes
# 4. Name and extra overrides
# 5. TOML configuration

# %% [markdown]
# ## Setup

# %%
from netrun.net.config import (
    NetConfig,
    NodeConfig,
    SubgraphConfig,
    GraphConfig,
    EdgeConfig,
    PortConfig,
    ExposedPortConfig,
    NodeExecutionConfig,
)

# %% [markdown]
# ## What is a Subgraph Factory?
#
# A regular node factory returns a `NodeConfig` — a single node with ports. A
# **subgraph factory** returns a `SubgraphConfig` — a group of nodes with
# internal edges and exposed ports. When `GraphConfig.resolve()` encounters one,
# it flattens the subgraph into the parent graph, just like an inline subgraph.
#
# The factory protocol is the same, except:
# - `get_node_config(**args)` returns `SubgraphConfig` instead of `NodeConfig`
# - `get_node_funcs()` is **not needed** (subgraphs don't execute directly)

# %% [markdown]
# ## Example 1: A Simple Pipeline Factory
#
# Let's create a factory that generates a linear pipeline of N stages.

# %%
# This is what a subgraph factory module looks like.
# In practice, this would be in its own .py file.

def make_pipeline_config(num_stages: int = 3) -> SubgraphConfig:
    """Create a linear pipeline subgraph with N stages."""
    nodes = [
        NodeConfig(
            name=f"stage_{i}",
            in_ports={"in": PortConfig()},
            out_ports={"out": PortConfig()},
        )
        for i in range(num_stages)
    ]

    edges = [
        EdgeConfig(
            source_str=f"stage_{i}.out",
            target_str=f"stage_{i+1}.in",
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
                internal_port="in",
            ),
        },
        exposed_out_ports={
            "out": ExposedPortConfig(
                internal_node=f"stage_{num_stages - 1}",
                internal_port="out",
            ),
        },
    )


# Create pipelines of different sizes
for n in [2, 4]:
    sg = make_pipeline_config(num_stages=n)
    print(f"Pipeline with {n} stages:")
    print(f"  Nodes: {[node.name for node in sg.nodes]}")
    print(f"  Edges: {len(sg.edges)}")
    print(f"  Exposed in:  {list(sg.exposed_in_ports.keys())}")
    print(f"  Exposed out: {list(sg.exposed_out_ports.keys())}")
    print()

# %% [markdown]
# ## Example 2: Using a Subgraph Factory in GraphConfig
#
# The factory is referenced via the `factory` field on a `NodeConfig`. When
# `GraphConfig.resolve()` is called, the NodeConfig is replaced by the
# flattened subgraph.

# %%
import textwrap, tempfile
from pathlib import Path

# Write the factory to a temporary file (simulating a real module)
tmp_dir = tempfile.mkdtemp()
factory_file = Path(tmp_dir) / "pipeline_factory.py"
factory_file.write_text(textwrap.dedent("""\
    from netrun.net.config import (
        NodeConfig, SubgraphConfig, EdgeConfig,
        PortConfig, ExposedPortConfig,
    )

    def get_node_config(_net_config=None, *, num_stages=3):
        nodes = [
            NodeConfig(
                name=f"stage_{i}",
                in_ports={"in": PortConfig()},
                out_ports={"out": PortConfig()},
            )
            for i in range(num_stages)
        ]
        edges = [
            EdgeConfig(
                source_str=f"stage_{i}.out",
                target_str=f"stage_{i+1}.in",
            )
            for i in range(num_stages - 1)
        ]
        return SubgraphConfig(
            name="pipeline",
            nodes=nodes,
            edges=edges,
            exposed_in_ports={"in": ExposedPortConfig(
                internal_node="stage_0", internal_port="in",
            )},
            exposed_out_ports={"out": ExposedPortConfig(
                internal_node=f"stage_{num_stages - 1}", internal_port="out",
            )},
        )
"""))

# Build a graph: Source -> pipeline (factory) -> Sink
graph = GraphConfig(
    nodes=[
        NodeConfig(name="Source", out_ports={"out": PortConfig()}),
        NodeConfig(
            name="my_pipeline",
            factory=str(factory_file),
            factory_args={"num_stages": 3},
        ),
        NodeConfig(name="Sink", in_ports={"in": PortConfig()}),
    ],
    edges=[
        EdgeConfig(source_str="Source.out", target_str="my_pipeline.in"),
        EdgeConfig(source_str="my_pipeline.out", target_str="Sink.in"),
    ],
)

# Before resolve: the factory node is still a NodeConfig
print("Before resolve:")
for node in graph.nodes:
    factory_info = f" (factory)" if isinstance(node, NodeConfig) and node.factory else ""
    print(f"  {node.name}{factory_info}")
print()

# After resolve: the factory is expanded into prefixed nodes
_nc = NetConfig(project_root=str(tmp_dir), graph=GraphConfig(nodes=[]))
resolved = graph.resolve(net_config=_nc)

print("After resolve:")
for node in resolved.nodes:
    print(f"  {node.name}")

print(f"\nEdges:")
for edge in resolved.edges:
    src = edge.get_source()
    tgt = edge.get_target()
    print(f"  {src.node_name}.{src.port_name} -> {tgt.node_name}.{tgt.port_name}")

# %% [markdown]
# Notice that:
# - The factory `NodeConfig` named `"my_pipeline"` was replaced by 3 nodes:
#   `my_pipeline.stage_0`, `my_pipeline.stage_1`, `my_pipeline.stage_2`
# - The parent edges (`Source.out -> my_pipeline.in`) were rewritten to
#   connect to the internal node (`Source.out -> my_pipeline.stage_0.in`)
# - The internal edges were prefixed and included

# %% [markdown]
# ## Example 3: Name and Extra Overrides
#
# When a `NodeConfig` references a subgraph factory:
# - The **NodeConfig's name** overrides the factory's default subgraph name
# - The **extra** dicts are merged, with the NodeConfig's values taking precedence

# %%
# The factory above returns name="pipeline" by default.
# But we set name="my_pipeline" on the NodeConfig, which takes precedence.

node_with_override = NodeConfig(
    name="custom_name",
    factory=str(factory_file),
    factory_args={"num_stages": 2},
    extra={"description": "My custom pipeline"},
)

_nc = NetConfig(project_root=str(tmp_dir), graph=GraphConfig(nodes=[]))
resolved_sg = node_with_override.resolve(net_config=_nc)
print(f"Subgraph name: {resolved_sg.name}")
print(f"Extra: {resolved_sg.extra}")
print(f"Type: {type(resolved_sg).__name__}")

# %% [markdown]
# If the NodeConfig's name is empty, the factory's default name is used:

# %%
node_no_name = NodeConfig(
    factory=str(factory_file),
    factory_args={"num_stages": 2},
)

resolved_sg2 = node_no_name.resolve(net_config=_nc)
print(f"Subgraph name (factory default): {resolved_sg2.name}")

# %% [markdown]
# ## Example 4: JSON Serialization
#
# A `NodeConfig` with a subgraph factory serializes to JSON like any other
# factory node. The factory and factory_args are preserved, and resolution
# happens on deserialization + resolve.

# %%
import json

node = NodeConfig(
    name="my_pipeline",
    factory=str(factory_file),
    factory_args={"num_stages": 2},
)

# Serialize
json_str = node.model_dump_json(indent=2)
print("Serialized NodeConfig:")
print(json_str)
print()

# Deserialize and resolve
loaded = NodeConfig.model_validate_json(json_str)
resolved = loaded.resolve(net_config=_nc)
print(f"After deserialize + resolve: {type(resolved).__name__} named '{resolved.name}'")

# %% [markdown]
# ## Example 5: from_factory() Does Not Support Subgraph Factories
#
# The `NodeConfig.from_factory()` class method is for eagerly creating
# node configs with execution functions. Since subgraph factories don't
# have `get_node_funcs()`, `from_factory()` raises a `ValueError`.

# %%
try:
    NodeConfig.from_factory(factory=str(factory_file), project_root=Path(tmp_dir))
except ValueError as e:
    print(f"Expected error: {e}")

# %% [markdown]
# Use the `factory` field on `NodeConfig` instead — it supports both
# node factories and subgraph factories via deferred resolution.

# %% [markdown]
# ## Example 6: TOML Configuration
#
# Subgraph factories work naturally in TOML configs. The `factory` field
# points to the factory module, and `factory_args` passes the parameters.
#
# ```toml
# # A subgraph factory node in TOML
# [[graph.nodes]]
# name = "my_pipeline"
# factory = "./pipeline_factory.py"
#
# [graph.nodes.factory_args]
# num_stages = 4
#
# # Edges connect to the subgraph's exposed ports
# [[graph.edges]]
# source_str = "Source.out"
# target_str = "my_pipeline.in"
#
# [[graph.edges]]
# source_str = "my_pipeline.out"
# target_str = "Sink.in"
# ```
#
# When `NetConfig.resolve()` (or `GraphConfig.resolve()`) is called, the
# factory is expanded and the subgraph is flattened — identical to inline
# subgraphs.

# %% [markdown]
# ## Summary
#
# Subgraph factories extend the factory protocol to generate groups of nodes:
#
# 1. **Same protocol**: `get_node_config(**args)` returns `SubgraphConfig`
#    instead of `NodeConfig`
#
# 2. **No get_node_funcs**: Subgraphs don't execute directly, so no
#    execution functions are needed
#
# 3. **Parameterized**: Factory args control the subgraph structure
#    (number of stages, topology, etc.)
#
# 4. **Transparent flattening**: `GraphConfig.resolve()` handles subgraph
#    factories the same way as inline subgraphs — prefixed names, rewritten
#    edges, exposed port mapping
#
# 5. **Composable**: Subgraph factory nodes can be used alongside regular
#    nodes, inline subgraphs, and other factory nodes
#
# 6. **Serializable**: The `factory` + `factory_args` fields serialize to
#    JSON/TOML cleanly; resolution happens at resolve time

# %%
print("Subgraph factory example complete!")
