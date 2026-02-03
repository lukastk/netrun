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
# # Function Node Factory Example
#
# This notebook demonstrates how to use the function node factory to create
# nodes from regular Python functions. The factory automatically:
#
# - Parses function signatures to determine input/output ports
# - Generates default salvo conditions
# - Creates wrapper functions that handle packet consumption and output
#
# We'll also show how to configure a network using TOML and serialize it
# to a `GraphConfig`.

# %% [markdown]
# ## The Function Node Factory
#
# The function node factory (`netrun.node_factories.function`) can create
# `NodeConfig` objects from regular Python functions. It inspects the
# function signature to determine:
#
# - **Input ports**: Each parameter becomes an input port
# - **Output ports**: Return annotation determines output port(s)
# - **Special parameters**: `ctx` and `print` are handled specially

# %%
from netrun.node_factories.function import from_function, parse_function_signature

# %% [markdown]
# ### Example 1: Simple Function
#
# Let's start with a simple function that doubles a number.

# %%
def double_number(x: int) -> int:
    """Double a number."""
    return x * 2

# Create a NodeConfig from the function
config = from_function(double_number)

print(f"Node name: {config.name}")
print(f"Input ports: {list(config.in_ports.keys())}")
print(f"Output ports: {list(config.out_ports.keys())}")
print(f"Input port 'x' type: {config.in_ports['x'].port_type}")
print(f"Output port 'out' type: {config.out_ports['out'].port_type}")

# %% [markdown]
# ### Example 2: Multiple Input Ports

# %%
def add_numbers(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b

config = from_function(add_numbers)
print(f"Input ports: {list(config.in_ports.keys())}")
print(f"In salvo condition 'trigger' ports: {list(config.in_salvo_conditions['trigger'].ports.keys())}")

# %% [markdown]
# ### Example 3: Special Parameters
#
# The `ctx` and `print` parameters are special - they don't become input ports
# but instead receive the execution context and print function.

# %%
def process_with_logging(data: str, print) -> str:
    """Process data with logging."""
    print(f"Processing: {data}")
    return data.upper()

parsed = parse_function_signature(process_with_logging)
print(f"Regular params (become ports): {parsed.regular_params}")
print(f"Special params (not ports): {parsed.special_params}")

# %% [markdown]
# ### Example 4: Config Override with `_node_config`
#
# You can attach a `_node_config` attribute to customize the generated config.

# %%
def custom_node(x: int) -> int:
    return x * 10

# Override with a dict
custom_node._node_config = {"name": "TenTimesMultiplier"}

config = from_function(custom_node)
print(f"Custom name: {config.name}")

# %%
# Override with TOML string
def toml_node(x: int) -> int:
    return x + 1

toml_node._node_config = '''
name = "IncrementNode"
'''

config = from_function(toml_node)
print(f"TOML-configured name: {config.name}")

# %% [markdown]
# ## Using TOML to Configure a Network
#
# Now let's create a complete network configuration using TOML. The network
# will use the function factory to create nodes from functions defined in
# an importable module.
#
# Our network will be:
# ```
# [Inject] -> [Double] -> [AddTen] -> [Output Queue]
# ```

# %%
import tomllib
from netrun.net.config import NetConfig, GraphConfig, NodeConfig, EdgeConfig, PortConfig
from netrun.net.config import (
    PoolConfig, MainPoolConfig, OutputQueueConfig,
    SalvoConditionConfig, SalvoConditionTermTrueConfig,
    MaxSalvosFiniteConfig, PacketCountAllConfig,
)

# Define the TOML configuration
NETWORK_TOML = '''
# Pool configuration
[pools.main]
id = "main"
print_flush_interval = 0.1
capture_prints = true

[pools.main.spec]
type = "main"

# Injection node (source) - doesn't use factory since it has no input ports
[[graph.nodes]]
name = "Inject"

[graph.nodes.out_ports.out]

[graph.nodes.out_salvo_conditions.send]
max_salvos = {type = "finite", max = 1}
term = {type = "true"}

[graph.nodes.out_salvo_conditions.send.ports]
out = {type = "all"}

# Double node - uses function factory
[[graph.nodes]]
factory = "netrun.node_factories.function"

[graph.nodes.factory_args]
func = "examples.net.function_factory_nodes.double_number"

[graph.nodes.execution_config]
pools = ["main"]

# AddTen node - manual node that adds 10
[[graph.nodes]]
name = "AddTen"

[graph.nodes.in_ports.x]
port_type = "int"

[graph.nodes.out_ports.out]
port_type = "int"

[graph.nodes.in_salvo_conditions.trigger]
max_salvos = {type = "finite", max = 1}
term = {type = "port", port_name = "x", state = {type = "non_empty"}}

[graph.nodes.in_salvo_conditions.trigger.ports]
x = {type = "all"}

[graph.nodes.out_salvo_conditions.send]
max_salvos = {type = "finite", max = 1}
term = {type = "true"}

[graph.nodes.out_salvo_conditions.send.ports]
out = {type = "all"}

# Edges
[[graph.edges]]
source_str = "Inject.out"
target_str = "double_number.x"

[[graph.edges]]
source_str = "double_number.out"
target_str = "AddTen.x"

# Output queue configuration
[output_queues.results]
ports = [["AddTen", "out"]]
'''

# Parse the TOML
config_dict = tomllib.loads(NETWORK_TOML)
print("Parsed TOML configuration:")
print(f"  Pools: {list(config_dict.get('pools', {}).keys())}")
print(f"  Nodes: {len(config_dict.get('graph', {}).get('nodes', []))}")
print(f"  Edges: {len(config_dict.get('graph', {}).get('edges', []))}")

# %% [markdown]
# ### Creating NetConfig from TOML
#
# Now we can create a `NetConfig` from the parsed TOML. The factory field
# will be expanded automatically when creating `NodeConfig` objects.

# %%
# Create GraphConfig from the TOML (nodes and edges)
graph_config = GraphConfig.model_validate(config_dict["graph"])

print(f"\nGraphConfig nodes:")
for node in graph_config.nodes:
    factory_info = f" (factory: {node.factory})" if node.factory else ""
    print(f"  - {node.name}{factory_info}")
    print(f"    In ports: {list(node.in_ports.keys())}")
    print(f"    Out ports: {list(node.out_ports.keys())}")

# %% [markdown]
# ### Building the Complete NetConfig
#
# We need to add execution configs for nodes that don't use factories
# and create the pool configurations.

# %%
from netrun.net._net import NodeExecutionContext

# Define the AddTen execution function manually
def add_ten_exec(ctx: NodeExecutionContext, packets: dict):
    """Add 10 to the input value."""
    for packet_id in packets.get("x", []):
        value = ctx.consume_packet(packet_id)
        ctx.print(f"AddTen: {value} + 10 = {value + 10}")
        out_id = ctx.create_packet(value + 10)
        ctx.load_output_port("out", out_id)
    ctx.send_output_salvo("send")

# Update the AddTen node with execution config
from netrun.net.config import NodeExecutionConfig

for node in graph_config.nodes:
    if node.name == "AddTen":
        node.execution_config = NodeExecutionConfig(
            exec_node_func=add_ten_exec,
            pools=["main"],
        )
    # Inject node doesn't need exec_func - we'll inject packets manually

# Create pool config
pools = {
    "main": PoolConfig(
        id="main",
        spec=MainPoolConfig(),
        capture_prints=True,
    )
}

# Create output queue config
output_queues = {
    "results": OutputQueueConfig(ports=[("AddTen", "out")]),
}

# Create the complete NetConfig
net_config = NetConfig(
    pools=pools,
    graph=graph_config,
    output_queues=output_queues,
)

print("NetConfig created successfully!")
print(f"  Pools: {list(net_config.pools.keys())}")
print(f"  Nodes: {[n.name for n in net_config.graph.nodes]}")
print(f"  Output queues: {list(net_config.output_queues.keys())}")

# %% [markdown]
# ### JSON Serialization
#
# The NetConfig can be serialized to JSON. Factory-created nodes will have
# their factory and factory_args preserved, while execution functions will
# be serialized to import paths (or error if they can't be serialized).

# %%
# For JSON serialization, we need to remove the closure-based exec funcs
# since the function factory creates closures that can't be serialized.

# Let's demonstrate serializing a graph config with a factory node
node_for_json = NodeConfig(
    factory="netrun.node_factories.function",
    factory_args={"func": "examples.net.function_factory_nodes.double_number"},
)

# The factory expansion creates execution_config with closures.
# We need to remove it before JSON serialization.
print(f"Node has execution_config: {node_for_json.execution_config is not None}")

# Remove execution_config for serialization (closures can't be serialized)
node_for_json = node_for_json.model_copy(update={"execution_config": None})

graph_for_json = GraphConfig(nodes=[node_for_json], edges=[])

# Serialize to JSON
import json
json_str = graph_for_json.model_dump_json(indent=2)
print("\nSerialized GraphConfig (factory node, without execution_config):")
print(json_str)

# Deserialize back - the factory will re-expand and recreate execution_config
loaded = GraphConfig.model_validate_json(json_str)
print(f"\nDeserialized node name: {loaded.nodes[0].name}")
print(f"Factory preserved: {loaded.nodes[0].factory}")
print(f"Execution config recreated: {loaded.nodes[0].execution_config is not None}")

# %% [markdown]
# ## Running the Network (Concept)
#
# To actually run the network, you would use the `Net` class. Here's how
# it would look conceptually (not executed here to avoid long-running code):
#
# ```python
# from netrun.net._net import Net
#
# async def run_network():
#     net = Net(net_config)
#
#     async with net:
#         # Inject initial packets
#         net.inject_packet("Inject", "out", 5)
#
#         # Run until all packets are processed
#         await net.run_until_idle()
#
#         # Get results from output queue
#         results = net.get_output_packets("results")
#         for result in results:
#             print(f"Result: {result.value}")
#             # Expected: 5 * 2 + 10 = 20
#
# asyncio.run(run_network())
# ```

# %% [markdown]
# ## Summary
#
# The function node factory provides a convenient way to create node
# configurations from regular Python functions:
#
# 1. **Automatic port generation**: Parameters become input ports, return
#    annotation becomes output port(s)
#
# 2. **Special parameters**: `ctx` and `print` receive the execution context
#    and captured print function
#
# 3. **Config override**: Use `_node_config` attribute to customize the
#    generated configuration (dict, NodeConfig, or TOML string)
#
# 4. **Factory protocol**: The module implements `get_node_config()` and
#    `get_node_funcs()` for use with the factory field in NodeConfig
#
# 5. **TOML configuration**: Networks can be configured entirely in TOML,
#    with factory nodes referencing importable functions by path

# %%
print("Function factory example complete!")
