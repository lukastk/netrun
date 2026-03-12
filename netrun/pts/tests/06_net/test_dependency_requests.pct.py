# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Dependency Edges and Packet Requests

# %%
#|default_exp net.test_dependency_requests

# %%
#|export
import pytest
import json

from netrun.net.config import (
    NetConfig,
    GraphConfig,
    NodeConfig,
    NodeExecutionConfig,
    PoolConfig,
    MainPoolConfig,
    PortConfig,
    EdgeConfig,
    SalvoConditionConfig,
    SalvoConditionTermTrueConfig,
    SalvoConditionTermPortConfig,
    MaxSalvosFiniteConfig,
    PacketCountAllConfig,
    PortStateNonEmptyConfig,
    DependencyRequestConfig,
    SalvoConditionTermAndConfig,
)
from netrun.net._net import Net
import netrun_sim

# %% [markdown]
# ## Config tests

# %%
#|export
def test_dependency_request_config_defaults():
    """DependencyRequestConfig has sensible defaults."""
    config = DependencyRequestConfig()
    assert config.triggers == ["on_startup"]
    assert config.label == "main"

# %%
#|export
def test_dependency_request_config_custom():
    """DependencyRequestConfig accepts custom values."""
    config = DependencyRequestConfig(
        triggers=["on_startup", "on_no_salvo_triggered"],
        label="my_label",
    )
    assert config.triggers == ["on_startup", "on_no_salvo_triggered"]
    assert config.label == "my_label"

# %%
#|export
def test_dependency_request_config_json_roundtrip():
    """DependencyRequestConfig serializes and deserializes via JSON."""
    config = DependencyRequestConfig(
        triggers=["on_startup", "on_no_salvo_triggered"],
        label="test",
    )
    data = json.loads(config.model_dump_json())
    restored = DependencyRequestConfig.model_validate(data)
    assert restored.triggers == config.triggers
    assert restored.label == config.label

# %%
#|export
def test_dependency_request_config_to_netrun_sim():
    """DependencyRequestConfig converts to netrun_sim correctly."""
    config = DependencyRequestConfig(
        triggers=["on_startup", "on_no_salvo_triggered"],
        label="test",
    )
    sim = config.to_netrun_sim()
    assert isinstance(sim, netrun_sim.DependencyRequestConfig)
    assert len(sim.triggers) == 2
    assert sim.label == "test"

# %%
#|export
def test_edge_config_dependency_flag_default():
    """EdgeConfig.dependency defaults to False."""
    edge = EdgeConfig(source_node="A", source_port="out", target_node="B", target_port="in")
    assert edge.dependency is False

# %%
#|export
def test_edge_config_dependency_flag_true():
    """EdgeConfig.dependency can be set to True."""
    edge = EdgeConfig(source_node="A", source_port="out", target_node="B", target_port="in", dependency=True)
    assert edge.dependency is True

# %%
#|export
def test_edge_config_dependency_json_roundtrip():
    """EdgeConfig with dependency=True round-trips through JSON."""
    edge = EdgeConfig(source_node="A", source_port="out", target_node="B", target_port="in", dependency=True)
    data = json.loads(edge.model_dump_json())
    assert data["dependency"] is True
    restored = EdgeConfig.model_validate(data)
    assert restored.dependency is True

# %%
#|export
def test_node_config_dependency_request_field():
    """NodeConfig accepts dependency_request field."""
    node = NodeConfig(
        name="Sink",
        in_ports={"in": PortConfig()},
        dependency_request=DependencyRequestConfig(triggers=["on_startup"], label="main"),
    )
    assert node.dependency_request is not None
    assert node.dependency_request.triggers == ["on_startup"]

# %%
#|export
def test_node_config_dependency_request_none_by_default():
    """NodeConfig.dependency_request defaults to None."""
    node = NodeConfig(name="Node")
    assert node.dependency_request is None

# %%
#|export
def test_node_config_to_netrun_sim_with_dependency_request():
    """NodeConfig.to_netrun_sim() passes dependency_request_config."""
    node = NodeConfig(
        name="Sink",
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
        dependency_request=DependencyRequestConfig(triggers=["on_startup"]),
    )
    sim_node = node.to_netrun_sim()
    assert sim_node.dependency_request_config is not None
    assert sim_node.dependency_request_config.label == "main"

# %%
#|export
def test_node_config_to_netrun_sim_without_dependency_request():
    """NodeConfig.to_netrun_sim() passes None when no dependency_request."""
    node = NodeConfig(
        name="Regular",
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
    )
    sim_node = node.to_netrun_sim()
    assert sim_node.dependency_request_config is None

# %% [markdown]
# ## Graph tests

# %%
#|export
def test_graph_config_passes_dependency_edges():
    """GraphConfig.get_graph() passes dependency edges to netrun_sim.Graph."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(
                name="Source",
                out_ports={"out": PortConfig()},
                in_salvo_conditions={
                    "trigger": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={},
                        term=SalvoConditionTermTrueConfig(),
                    ),
                },
                out_salvo_conditions={
                    "send": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"out": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(
                            port_name="out",
                            state=PortStateNonEmptyConfig(),
                        ),
                    ),
                },
            ),
            NodeConfig(
                name="Sink",
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
                dependency_request=DependencyRequestConfig(),
            ),
        ],
        edges=[
            EdgeConfig(source_node="Source", source_port="out", target_node="Sink", target_port="in", dependency=True),
        ],
    )
    graph = graph_config.get_graph()
    # Verify the edge is marked as a dependency edge
    edges = list(graph.edges())
    assert len(edges) == 1
    assert graph.is_dependency_edge(edges[0])

# %%
#|export
def test_graph_config_non_dependency_edge():
    """Non-dependency edges are not marked as dependency edges."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(
                name="A",
                out_ports={"out": PortConfig()},
                out_salvo_conditions={
                    "send": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"out": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(
                            port_name="out",
                            state=PortStateNonEmptyConfig(),
                        ),
                    ),
                },
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
            EdgeConfig(source_node="A", source_port="out", target_node="B", target_port="in"),
        ],
    )
    graph = graph_config.get_graph()
    edges = list(graph.edges())
    assert len(edges) == 1
    assert not graph.is_dependency_edge(edges[0])

# %%
#|export
def test_subgraph_resolve_preserves_dependency_flag():
    """Dependency flag is preserved through subgraph edge rewriting."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="A", out_ports={"out": PortConfig()}),
            NodeConfig(name="B", in_ports={"in": PortConfig()}),
        ],
        edges=[
            EdgeConfig(source_node="A", source_port="out", target_node="B", target_port="in", dependency=True),
        ],
    )
    # resolve() rewrites edges through subgraph mappings; with no subgraphs,
    # edges should pass through unchanged with the dependency flag preserved
    resolved = graph_config.resolve()
    assert len(resolved.edges) == 1
    assert resolved.edges[0].dependency is True

# %% [markdown]
# ## Auto-default DependencyRequestConfig tests

# %%
#|export
def test_auto_default_dependency_request_config():
    """A node targeted by a dependency edge gets DependencyRequestConfig() if none is set."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Source", out_ports={"out": PortConfig()}),
            NodeConfig(
                name="Sink",
                in_ports={"in": PortConfig()},
                # No dependency_request set
            ),
        ],
        edges=[
            EdgeConfig(source_node="Source", source_port="out", target_node="Sink", target_port="in", dependency=True),
        ],
    )

    resolved = graph_config.resolve()
    sink = [n for n in resolved.nodes if n.name == "Sink"][0]
    assert sink.dependency_request is not None
    assert sink.dependency_request.triggers == ["on_startup"]
    assert sink.dependency_request.label == "main"

# %%
#|export
def test_explicit_dependency_request_not_overridden():
    """A node with an explicit DependencyRequestConfig keeps it after resolve."""
    custom_config = DependencyRequestConfig(
        triggers=["on_startup", "on_no_salvo_triggered"],
        label="custom",
    )
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="Source", out_ports={"out": PortConfig()}),
            NodeConfig(
                name="Sink",
                in_ports={"in": PortConfig()},
                dependency_request=custom_config,
            ),
        ],
        edges=[
            EdgeConfig(source_node="Source", source_port="out", target_node="Sink", target_port="in", dependency=True),
        ],
    )

    resolved = graph_config.resolve()
    sink = [n for n in resolved.nodes if n.name == "Sink"][0]
    assert sink.dependency_request is not None
    assert sink.dependency_request.triggers == ["on_startup", "on_no_salvo_triggered"]
    assert sink.dependency_request.label == "custom"

# %%
#|export
def test_no_dependency_request_without_dependency_edges():
    """A node without dependency edges keeps dependency_request=None."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(name="A", out_ports={"out": PortConfig()}),
            NodeConfig(name="B", in_ports={"in": PortConfig()}),
        ],
        edges=[
            EdgeConfig(source_node="A", source_port="out", target_node="B", target_port="in"),
        ],
    )

    resolved = graph_config.resolve()
    b = [n for n in resolved.nodes if n.name == "B"][0]
    assert b.dependency_request is None

# %% [markdown]
# ## Net.request() tests

# %%
#|export
@pytest.mark.asyncio
async def test_net_request_creates_pending_request():
    """net.request() creates a pending request via CreateRequest action."""
    graph_config = GraphConfig(
        nodes=[
            NodeConfig(
                name="Source",
                out_ports={"out": PortConfig()},
                in_salvo_conditions={
                    "trigger": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={},
                        term=SalvoConditionTermTrueConfig(),
                    ),
                },
                out_salvo_conditions={
                    "send": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"out": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(
                            port_name="out",
                            state=PortStateNonEmptyConfig(),
                        ),
                    ),
                },
                execution_config=NodeExecutionConfig(
                    node_name="Source",
                    pools=["main"],
                    exec_node_func=lambda ctx, packets: None,
                ),
            ),
            NodeConfig(
                name="Sink",
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
                dependency_request=DependencyRequestConfig(),
            ),
        ],
        edges=[
            EdgeConfig(source_node="Source", source_port="out", target_node="Sink", target_port="in", dependency=True),
        ],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
    )

    async with Net(config) as net:
        events = net.request("Sink", "main")
        assert isinstance(events, list)

# %%
#|export
@pytest.mark.asyncio
async def test_end_to_end_on_startup_dependency_request():
    """End-to-end: on_startup trigger creates source epoch via dependency edges."""
    execution_log = []

    def source_node(ctx, packets):
        execution_log.append("source_executed")
        out_id = ctx.create_packet("from_source")
        ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    def sink_node(ctx, packets):
        for port_name, packet_ids in packets.items():
            for pid in packet_ids:
                value = ctx.consume_packet(pid)
                execution_log.append(f"sink_received:{value}")

    graph_config = GraphConfig(
        nodes=[
            NodeConfig(
                name="Source",
                out_ports={"out": PortConfig()},
                in_salvo_conditions={
                    "trigger": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={},
                        term=SalvoConditionTermTrueConfig(),
                    ),
                },
                out_salvo_conditions={
                    "send": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"out": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(
                            port_name="out",
                            state=PortStateNonEmptyConfig(),
                        ),
                    ),
                },
                execution_config=NodeExecutionConfig(
                    node_name="Source",
                    pools=["main"],
                    exec_node_func=source_node,
                ),
            ),
            NodeConfig(
                name="Sink",
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
                dependency_request=DependencyRequestConfig(
                    triggers=["on_startup"],
                    label="main",
                ),
                execution_config=NodeExecutionConfig(
                    node_name="Sink",
                    pools=["main"],
                    exec_node_func=sink_node,
                ),
            ),
        ],
        edges=[
            EdgeConfig(source_node="Source", source_port="out", target_node="Sink", target_port="in", dependency=True),
        ],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
    )

    async with Net(config) as net:
        # on_startup trigger fires during the first run_step,
        # creating a __request__ epoch at Source
        await net.run_until_blocked()

        # Source should have been executed (auto_start_epochs=True in run_until_blocked)
        assert "source_executed" in execution_log

        # Sink should have received the packet from Source
        assert any("sink_received:from_source" in entry for entry in execution_log)

# %% [markdown]
# ## Integration: manual net.request() triggers cascade and forward flow

# %%
#|export
@pytest.mark.asyncio
async def test_manual_request_triggers_cascade_and_forward_flow():
    """Calling net.request() cascades backward, activates source, data flows forward."""
    execution_log = []

    def source_node(ctx, packets):
        execution_log.append("source_executed")
        out_id = ctx.create_packet("requested_data")
        ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    def sink_node(ctx, packets):
        for port_name, packet_ids in packets.items():
            for pid in packet_ids:
                value = ctx.consume_packet(pid)
                execution_log.append(f"sink_received:{value}")

    graph_config = GraphConfig(
        nodes=[
            NodeConfig(
                name="Source",
                out_ports={"out": PortConfig()},
                in_salvo_conditions={
                    "trigger": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={},
                        term=SalvoConditionTermTrueConfig(),
                    ),
                },
                out_salvo_conditions={
                    "send": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"out": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(
                            port_name="out",
                            state=PortStateNonEmptyConfig(),
                        ),
                    ),
                },
                execution_config=NodeExecutionConfig(
                    node_name="Source",
                    pools=["main"],
                    exec_node_func=source_node,
                ),
            ),
            NodeConfig(
                name="Sink",
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
                dependency_request=DependencyRequestConfig(triggers=[]),
                execution_config=NodeExecutionConfig(
                    node_name="Sink",
                    pools=["main"],
                    exec_node_func=sink_node,
                ),
            ),
        ],
        edges=[
            EdgeConfig(source_node="Source", source_port="out", target_node="Sink", target_port="in", dependency=True),
        ],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
    )

    async with Net(config) as net:
        # Manual request (no on_startup trigger configured)
        net.request("Sink", "main")
        await net.run_until_blocked()

        assert "source_executed" in execution_log
        assert any("sink_received:requested_data" in entry for entry in execution_log)

# %% [markdown]
# ## Integration: multi-hop chain (Source → Middle → Sink)

# %%
#|export
@pytest.mark.asyncio
async def test_multi_hop_dependency_chain():
    """Request cascades through multiple hops: Sink → Middle → Source."""
    execution_log = []

    def source_node(ctx, packets):
        execution_log.append("source")
        out_id = ctx.create_packet("from_source")
        ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    def middle_node(ctx, packets):
        execution_log.append("middle")
        for port_name, packet_ids in packets.items():
            for pid in packet_ids:
                value = ctx.consume_packet(pid)
        out_id = ctx.create_packet("from_middle")
        ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    def sink_node(ctx, packets):
        for port_name, packet_ids in packets.items():
            for pid in packet_ids:
                value = ctx.consume_packet(pid)
                execution_log.append(f"sink:{value}")

    def _make_passthrough_node(name, exec_func):
        return NodeConfig(
            name=name,
            in_ports={"in": PortConfig()},
            out_ports={"out": PortConfig()},
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
            out_salvo_conditions={
                "send": SalvoConditionConfig(
                    max_salvos=MaxSalvosFiniteConfig(max=1),
                    ports={"out": PacketCountAllConfig()},
                    term=SalvoConditionTermPortConfig(
                        port_name="out",
                        state=PortStateNonEmptyConfig(),
                    ),
                ),
            },
            execution_config=NodeExecutionConfig(
                node_name=name,
                pools=["main"],
                exec_node_func=exec_func,
            ),
        )

    graph_config = GraphConfig(
        nodes=[
            NodeConfig(
                name="Source",
                out_ports={"out": PortConfig()},
                in_salvo_conditions={
                    "trigger": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={},
                        term=SalvoConditionTermTrueConfig(),
                    ),
                },
                out_salvo_conditions={
                    "send": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"out": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(
                            port_name="out",
                            state=PortStateNonEmptyConfig(),
                        ),
                    ),
                },
                execution_config=NodeExecutionConfig(
                    node_name="Source",
                    pools=["main"],
                    exec_node_func=source_node,
                ),
            ),
            _make_passthrough_node("Middle", middle_node),
            NodeConfig(
                name="Sink",
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
                dependency_request=DependencyRequestConfig(triggers=["on_startup"]),
                execution_config=NodeExecutionConfig(
                    node_name="Sink",
                    pools=["main"],
                    exec_node_func=sink_node,
                ),
            ),
        ],
        edges=[
            EdgeConfig(source_node="Source", source_port="out", target_node="Middle", target_port="in", dependency=True),
            EdgeConfig(source_node="Middle", source_port="out", target_node="Sink", target_port="in", dependency=True),
        ],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
    )

    async with Net(config) as net:
        await net.run_until_blocked()

        # All three nodes should have executed in order
        assert "source" in execution_log
        assert "middle" in execution_log
        assert any("sink:from_middle" in e for e in execution_log)

# %% [markdown]
# ## Integration: diamond graph deduplication

# %%
#|export
@pytest.mark.asyncio
async def test_diamond_graph_deduplication():
    """Diamond: Source feeds Left and Right which both feed Sink.
    One request from Sink should create only one epoch at Source."""
    source_exec_count = []

    def source_node(ctx, packets):
        source_exec_count.append(1)
        left_id = ctx.create_packet("data_left")
        ctx.load_output_port("out_left", left_id)
        ctx.send_output_salvo("send_out_left")
        right_id = ctx.create_packet("data_right")
        ctx.load_output_port("out_right", right_id)
        ctx.send_output_salvo("send_out_right")

    def passthrough(ctx, packets):
        for port_name, packet_ids in packets.items():
            for pid in packet_ids:
                ctx.consume_packet(pid)
        out_id = ctx.create_packet("passed")
        ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send_out")

    def sink_node(ctx, packets):
        for port_name, packet_ids in packets.items():
            for pid in packet_ids:
                ctx.consume_packet(pid)


    def _node(name, exec_func, in_ports=None, out_ports=None, dep_req=None):
        in_p = in_ports or {}
        out_p = out_ports or {}
        in_sc = {}
        out_sc = {}
        if in_p:
            terms = []
            for pn in in_p:
                terms.append(SalvoConditionTermPortConfig(port_name=pn, state=PortStateNonEmptyConfig()))
            if len(terms) == 1:
                term = terms[0]
            else:
                term = SalvoConditionTermAndConfig(terms=terms)
            ports = {pn: PacketCountAllConfig() for pn in in_p}
            in_sc["default"] = SalvoConditionConfig(
                max_salvos=MaxSalvosFiniteConfig(max=1), ports=ports, term=term,
            )
        else:
            in_sc["trigger"] = SalvoConditionConfig(
                max_salvos=MaxSalvosFiniteConfig(max=1), ports={}, term=SalvoConditionTermTrueConfig(),
            )
        if out_p:
            for pn in out_p:
                out_sc[f"send_{pn}"] = SalvoConditionConfig(
                    max_salvos=MaxSalvosFiniteConfig(max=1),
                    ports={pn: PacketCountAllConfig()},
                    term=SalvoConditionTermPortConfig(port_name=pn, state=PortStateNonEmptyConfig()),
                )
        return NodeConfig(
            name=name, in_ports=in_p, out_ports=out_p,
            in_salvo_conditions=in_sc, out_salvo_conditions=out_sc,
            dependency_request=dep_req,
            execution_config=NodeExecutionConfig(
                node_name=name, pools=["main"], exec_node_func=exec_func,
            ),
        )

    graph_config = GraphConfig(
        nodes=[
            # Source has two output ports to avoid fan-out
            _node("Source", source_node,
                  out_ports={"out_left": PortConfig(), "out_right": PortConfig()}),
            _node("Left", passthrough,
                  in_ports={"in": PortConfig()}, out_ports={"out": PortConfig()}),
            _node("Right", passthrough,
                  in_ports={"in": PortConfig()}, out_ports={"out": PortConfig()}),
            _node("Sink", sink_node,
                  in_ports={"left": PortConfig(), "right": PortConfig()},
                  dep_req=DependencyRequestConfig(triggers=["on_startup"])),
        ],
        edges=[
            EdgeConfig(source_node="Source", source_port="out_left", target_node="Left", target_port="in", dependency=True),
            EdgeConfig(source_node="Source", source_port="out_right", target_node="Right", target_port="in", dependency=True),
            EdgeConfig(source_node="Left", source_port="out", target_node="Sink", target_port="left", dependency=True),
            EdgeConfig(source_node="Right", source_port="out", target_node="Sink", target_port="right", dependency=True),
        ],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
    )

    async with Net(config) as net:
        await net.run_until_blocked()

        # Source should have been executed exactly once (BFS deduplication)
        assert len(source_exec_count) == 1

# %% [markdown]
# ## Integration: hybrid push-pull (mixed dependency + regular edges)

# %%
#|export
@pytest.mark.asyncio
async def test_hybrid_push_pull():
    """Node with one dependency edge and one regular edge.
    Push data on regular edge, pull data via dependency edge."""
    execution_log = []

    def dep_source(ctx, packets):
        execution_log.append("dep_source")
        out_id = ctx.create_packet("dep_data")
        ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    def sink_node(ctx, packets):
        values = []
        for port_name, packet_ids in packets.items():
            for pid in packet_ids:
                values.append(ctx.consume_packet(pid))
        execution_log.append(f"sink:{sorted(values)}")

    graph_config = GraphConfig(
        nodes=[
            NodeConfig(
                name="DepSource",
                out_ports={"out": PortConfig()},
                in_salvo_conditions={
                    "trigger": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={},
                        term=SalvoConditionTermTrueConfig(),
                    ),
                },
                out_salvo_conditions={
                    "send": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"out": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(
                            port_name="out",
                            state=PortStateNonEmptyConfig(),
                        ),
                    ),
                },
                execution_config=NodeExecutionConfig(
                    node_name="DepSource",
                    pools=["main"],
                    exec_node_func=dep_source,
                ),
            ),
            NodeConfig(
                name="Sink",
                in_ports={"dep_in": PortConfig(), "push_in": PortConfig()},
                in_salvo_conditions={
                    "default": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"dep_in": PacketCountAllConfig(), "push_in": PacketCountAllConfig()},
                        term=SalvoConditionTermAndConfig(terms=[
                            SalvoConditionTermPortConfig(port_name="dep_in", state=PortStateNonEmptyConfig()),
                            SalvoConditionTermPortConfig(port_name="push_in", state=PortStateNonEmptyConfig()),
                        ]),
                    ),
                },
                dependency_request=DependencyRequestConfig(triggers=["on_startup"]),
                execution_config=NodeExecutionConfig(
                    node_name="Sink",
                    pools=["main"],
                    exec_node_func=sink_node,
                ),
            ),
        ],
        edges=[
            EdgeConfig(source_node="DepSource", source_port="out", target_node="Sink", target_port="dep_in", dependency=True),
        ],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
    )

    async with Net(config) as net:
        # on_startup pulls dep_data from DepSource
        await net.run_until_blocked()
        assert "dep_source" in execution_log

        # Now push data on the regular input
        net.inject_data("Sink", "push_in", ["push_data"])
        await net.run_until_blocked()

        # Sink should have received both
        assert any("sink:" in e and "dep_data" in e and "push_data" in e for e in execution_log)

# %% [markdown]
# ## Integration: label deduplication

# %%
#|export
@pytest.mark.asyncio
async def test_label_deduplication_same_label():
    """Two requests with same label to same source → one source epoch."""
    source_exec_count = []

    def source_node(ctx, packets):
        source_exec_count.append(1)
        out_id = ctx.create_packet("data")
        ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    def sink_node(ctx, packets):
        for port_name, packet_ids in packets.items():
            for pid in packet_ids:
                ctx.consume_packet(pid)

    graph_config = GraphConfig(
        nodes=[
            NodeConfig(
                name="Source",
                out_ports={"out": PortConfig()},
                in_salvo_conditions={
                    "trigger": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={},
                        term=SalvoConditionTermTrueConfig(),
                    ),
                },
                out_salvo_conditions={
                    "send": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"out": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(
                            port_name="out",
                            state=PortStateNonEmptyConfig(),
                        ),
                    ),
                },
                execution_config=NodeExecutionConfig(
                    node_name="Source",
                    pools=["main"],
                    exec_node_func=source_node,
                ),
            ),
            NodeConfig(
                name="Sink",
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
                dependency_request=DependencyRequestConfig(triggers=[]),
                execution_config=NodeExecutionConfig(
                    node_name="Sink",
                    pools=["main"],
                    exec_node_func=sink_node,
                ),
            ),
        ],
        edges=[
            EdgeConfig(source_node="Source", source_port="out", target_node="Sink", target_port="in", dependency=True),
        ],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
    )

    async with Net(config) as net:
        # Two requests with the same label before run_step
        net.request("Sink", "same_label")
        net.request("Sink", "same_label")
        await net.run_until_blocked()

        # Source should execute only once (same label deduplication)
        assert len(source_exec_count) == 1

# %%
#|export
@pytest.mark.asyncio
async def test_label_deduplication_different_labels():
    """Two requests with different labels to same source → two source epochs."""
    source_exec_count = []

    def source_node(ctx, packets):
        source_exec_count.append(1)
        out_id = ctx.create_packet("data")
        ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    def sink_node(ctx, packets):
        for port_name, packet_ids in packets.items():
            for pid in packet_ids:
                ctx.consume_packet(pid)

    graph_config = GraphConfig(
        nodes=[
            NodeConfig(
                name="Source",
                out_ports={"out": PortConfig()},
                in_salvo_conditions={
                    "trigger": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={},
                        term=SalvoConditionTermTrueConfig(),
                    ),
                },
                out_salvo_conditions={
                    "send": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"out": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(
                            port_name="out",
                            state=PortStateNonEmptyConfig(),
                        ),
                    ),
                },
                execution_config=NodeExecutionConfig(
                    node_name="Source",
                    pools=["main"],
                    exec_node_func=source_node,
                ),
            ),
            NodeConfig(
                name="Sink",
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
                dependency_request=DependencyRequestConfig(triggers=[]),
                execution_config=NodeExecutionConfig(
                    node_name="Sink",
                    pools=["main"],
                    exec_node_func=sink_node,
                ),
            ),
        ],
        edges=[
            EdgeConfig(source_node="Source", source_port="out", target_node="Sink", target_port="in", dependency=True),
        ],
    )

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=graph_config,
    )

    async with Net(config) as net:
        # Two requests with different labels
        net.request("Sink", "label_a")
        net.request("Sink", "label_b")
        await net.run_until_blocked()

        # Source should execute twice (different labels)
        assert len(source_exec_count) == 2

# %% [markdown]
# ## Config: NetConfig JSON round-trip with dependency_request

# %%
#|export
def test_net_config_json_roundtrip_with_dependency_request():
    """Full NetConfig with dependency edges and request config survives JSON round-trip."""
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Source",
                    out_ports={"out": PortConfig()},
                ),
                NodeConfig(
                    name="Sink",
                    in_ports={"in": PortConfig()},
                    dependency_request=DependencyRequestConfig(
                        triggers=["on_startup", "on_no_salvo_triggered"],
                        label="batch_1",
                    ),
                ),
            ],
            edges=[
                EdgeConfig(source_node="Source", source_port="out", target_node="Sink", target_port="in", dependency=True),
            ],
        ),
    )

    data = json.loads(config.model_dump_json())

    # Verify dependency fields are in serialized form
    edge_data = data["graph"]["edges"][0]
    assert edge_data["dependency"] is True

    sink_data = [n for n in data["graph"]["nodes"] if n["name"] == "Sink"][0]
    assert sink_data["dependency_request"]["triggers"] == ["on_startup", "on_no_salvo_triggered"]
    assert sink_data["dependency_request"]["label"] == "batch_1"

    # Round-trip
    restored = NetConfig.model_validate(data)
    assert restored.graph.edges[0].dependency is True
    sink_node = [n for n in restored.graph.nodes if n.name == "Sink"][0]
    assert sink_node.dependency_request.triggers == ["on_startup", "on_no_salvo_triggered"]
    assert sink_node.dependency_request.label == "batch_1"
