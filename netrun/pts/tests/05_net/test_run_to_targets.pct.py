# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Net.run_to_targets

# %%
#|default_exp net.test_run_to_targets

# %%
#|export
import pytest
import asyncio
from typing import Any

from netrun.net._net import (
    Net,
    TargetInputSalvo,
)
from netrun.net._net._run_to_targets import (
    _compute_upstream_nodes,
    _classify_epoch,
)
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
    MaxSalvosInfiniteConfig,
    PacketCountAllConfig,
    PortStateNonEmptyConfig,
)

# %% [markdown]
# ## Helper: passthrough node function

# %%
#|export
def passthrough_func(ctx, packets):
    """Passthrough node: consume inputs, create outputs, send salvo."""
    for port_name, pkt_ids in packets.items():
        for pid in pkt_ids:
            val = ctx.consume_packet(pid)
            out_id = ctx.create_packet(val)
            ctx.load_output_port("out", out_id)
    ctx.send_output_salvo("default")

# %% [markdown]
# ## Helper: build node configs

# %%
#|export
def _make_passthrough_node(name: str, *, has_in: bool = True, has_out: bool = True) -> NodeConfig:
    """Create a passthrough node config with standard ports and salvo conditions."""
    in_ports = {"in": PortConfig()} if has_in else None
    out_ports = {"out": PortConfig()} if has_out else None

    in_salvo_conditions = None
    if has_in:
        in_salvo_conditions = {
            "default": SalvoConditionConfig(
                max_salvos=MaxSalvosInfiniteConfig(),
                ports={"in": PacketCountAllConfig()},
                term=SalvoConditionTermPortConfig(
                    port_name="in",
                    state=PortStateNonEmptyConfig(),
                ),
            ),
        }

    out_salvo_conditions = None
    if has_out:
        out_salvo_conditions = {
            "default": SalvoConditionConfig(
                max_salvos=MaxSalvosFiniteConfig(max=1),
                ports={"out": PacketCountAllConfig()},
                term=SalvoConditionTermPortConfig(
                    port_name="out",
                    state=PortStateNonEmptyConfig(),
                ),
            ),
        }

    return NodeConfig(
        name=name,
        in_ports=in_ports,
        out_ports=out_ports,
        in_salvo_conditions=in_salvo_conditions,
        out_salvo_conditions=out_salvo_conditions,
        execution_config=NodeExecutionConfig(
            pools=["main"],
            exec_node_func=passthrough_func,
        ),
    )


def _make_sink_node(name: str) -> NodeConfig:
    """Create a sink node config (input only, no output, no exec func)."""
    return NodeConfig(
        name=name,
        in_ports={"in": PortConfig()},
        in_salvo_conditions={
            "default": SalvoConditionConfig(
                max_salvos=MaxSalvosInfiniteConfig(),
                ports={"in": PacketCountAllConfig()},
                term=SalvoConditionTermPortConfig(
                    port_name="in",
                    state=PortStateNonEmptyConfig(),
                ),
            ),
        },
    )


def _make_net_config(nodes: list[NodeConfig], edges: list[EdgeConfig]) -> NetConfig:
    """Create a NetConfig with a main pool."""
    return NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(nodes=nodes, edges=edges),
    )

# %% [markdown]
# ## Test: linear pipeline (A -> B -> C)

# %%
#|export
@pytest.mark.asyncio
async def test_linear_pipeline():
    """A->B->C: run_to_targets('C') executes A and B, returns C's input salvo."""
    config = _make_net_config(
        nodes=[
            _make_passthrough_node("A"),
            _make_passthrough_node("B"),
            _make_sink_node("C"),
        ],
        edges=[
            EdgeConfig(source_str="A.out", target_str="B.in"),
            EdgeConfig(source_str="B.out", target_str="C.in"),
        ],
    )

    async with Net(config) as net:
        net.inject_data("A", "in", [42])
        salvos = await net.run_to_targets("C")

        assert len(salvos) == 1
        salvo = salvos[0]
        assert salvo.node_name == "C"
        assert salvo.salvo_condition == "default"
        assert "in" in salvo.packets
        assert salvo.packets["in"] == [42]

# %% [markdown]
# ## Test: diamond graph (A -> B, A -> C, B -> D, C -> D)

# %%
#|export
@pytest.mark.asyncio
async def test_diamond():
    """Diamond: A->B, A->C, B->D, C->D. Targeting D gets packets from both paths.

    A has two output ports (out_b and out_c) so each path gets its own packet.
    """
    def fork_func(ctx, packets):
        """Consume input, create separate outputs for each outgoing port."""
        for port_name, pkt_ids in packets.items():
            for pid in pkt_ids:
                val = ctx.consume_packet(pid)
                out_b = ctx.create_packet(val)
                ctx.load_output_port("out_b", out_b)
                out_c = ctx.create_packet(val)
                ctx.load_output_port("out_c", out_c)
        ctx.send_output_salvo("default")

    fork_node = NodeConfig(
        name="A",
        in_ports={"in": PortConfig()},
        out_ports={"out_b": PortConfig(), "out_c": PortConfig()},
        in_salvo_conditions={
            "default": SalvoConditionConfig(
                max_salvos=MaxSalvosInfiniteConfig(),
                ports={"in": PacketCountAllConfig()},
                term=SalvoConditionTermPortConfig(
                    port_name="in",
                    state=PortStateNonEmptyConfig(),
                ),
            ),
        },
        out_salvo_conditions={
            "default": SalvoConditionConfig(
                max_salvos=MaxSalvosFiniteConfig(max=1),
                ports={"out_b": PacketCountAllConfig(), "out_c": PacketCountAllConfig()},
                term=SalvoConditionTermTrueConfig(),
            ),
        },
        execution_config=NodeExecutionConfig(
            pools=["main"],
            exec_node_func=fork_func,
        ),
    )

    config = _make_net_config(
        nodes=[
            fork_node,
            _make_passthrough_node("B"),
            _make_passthrough_node("C"),
            _make_sink_node("D"),
        ],
        edges=[
            EdgeConfig(source_str="A.out_b", target_str="B.in"),
            EdgeConfig(source_str="A.out_c", target_str="C.in"),
            EdgeConfig(source_str="B.out", target_str="D.in"),
            EdgeConfig(source_str="C.out", target_str="D.in"),
        ],
    )

    async with Net(config) as net:
        net.inject_data("A", "in", [10])
        salvos = await net.run_to_targets("D")

        assert len(salvos) == 1
        salvo = salvos[0]
        assert salvo.node_name == "D"
        # D should receive packets from both B and C (both forwarding the value 10)
        assert sorted(salvo.packets["in"]) == [10, 10]

# %% [markdown]
# ## Test: specific salvo condition targeting

# %%
#|export
@pytest.mark.asyncio
async def test_specific_salvo_condition():
    """Target a specific salvo condition on B. Other conditions on B still execute."""
    executed_cond2 = []

    def cond2_func(ctx, packets):
        """Node func that fires when cond_2 triggers."""
        for port_name, pkt_ids in packets.items():
            for pid in pkt_ids:
                val = ctx.consume_packet(pid)
                executed_cond2.append(val)

    config = _make_net_config(
        nodes=[
            _make_passthrough_node("A"),
            NodeConfig(
                name="B",
                in_ports={"in1": PortConfig(), "in2": PortConfig()},
                in_salvo_conditions={
                    "cond_1": SalvoConditionConfig(
                        max_salvos=MaxSalvosInfiniteConfig(),
                        ports={"in1": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(
                            port_name="in1",
                            state=PortStateNonEmptyConfig(),
                        ),
                    ),
                    "cond_2": SalvoConditionConfig(
                        max_salvos=MaxSalvosInfiniteConfig(),
                        ports={"in2": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(
                            port_name="in2",
                            state=PortStateNonEmptyConfig(),
                        ),
                    ),
                },
                execution_config=NodeExecutionConfig(
                    pools=["main"],
                    exec_node_func=cond2_func,
                ),
            ),
        ],
        edges=[
            EdgeConfig(source_str="A.out", target_str="B.in1"),
        ],
    )

    async with Net(config) as net:
        # Inject data that will reach B.in1 (via A)
        net.inject_data("A", "in", [100])
        # Also inject directly into B.in2 to trigger cond_2
        net.inject_data("B", "in2", [200])

        salvos = await net.run_to_targets([("B", "cond_1")])

        # cond_1 should be collected as target
        assert len(salvos) == 1
        assert salvos[0].salvo_condition == "cond_1"
        assert salvos[0].packets["in1"] == [100]

        # cond_2 should have been executed (B is in upstream set for non-targeted salvos)
        assert executed_cond2 == [200]

# %% [markdown]
# ## Test: multiple target salvos

# %%
#|export
@pytest.mark.asyncio
async def test_multiple_target_salvos():
    """Injecting multiple packets produces multiple target salvos (one per epoch)."""
    config = _make_net_config(
        nodes=[
            _make_passthrough_node("A"),
            _make_sink_node("B"),
        ],
        edges=[
            EdgeConfig(source_str="A.out", target_str="B.in"),
        ],
    )

    async with Net(config) as net:
        net.inject_data("A", "in", [1])
        net.inject_data("A", "in", [2])
        salvos = await net.run_to_targets("B")

        assert len(salvos) == 2
        values = sorted([s.packets["in"][0] for s in salvos])
        assert values == [1, 2]
        for s in salvos:
            assert s.node_name == "B"
            assert s.salvo_condition == "default"

# %% [markdown]
# ## Test: source node as target (no upstream)

# %%
#|export
@pytest.mark.asyncio
async def test_source_node_target():
    """Target a source node with no upstream — salvo returned immediately."""
    config = _make_net_config(
        nodes=[
            _make_sink_node("A"),
        ],
        edges=[],
    )

    async with Net(config) as net:
        net.inject_data("A", "in", [99])
        salvos = await net.run_to_targets("A")

        assert len(salvos) == 1
        assert salvos[0].node_name == "A"
        assert salvos[0].packets["in"] == [99]

# %% [markdown]
# ## Test: irrelevant nodes not executed

# %%
#|export
@pytest.mark.asyncio
async def test_irrelevant_nodes_not_executed():
    """Nodes not upstream of targets should not be executed."""
    executed_nodes = []

    def tracking_passthrough(ctx, packets):
        executed_nodes.append(ctx.node_name)
        for port_name, pkt_ids in packets.items():
            for pid in pkt_ids:
                val = ctx.consume_packet(pid)
                out_id = ctx.create_packet(val)
                ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("default")

    def _make_tracked_node(name, *, has_in=True, has_out=True):
        in_ports = {"in": PortConfig()} if has_in else None
        out_ports = {"out": PortConfig()} if has_out else None
        in_salvo_conditions = None
        if has_in:
            in_salvo_conditions = {
                "default": SalvoConditionConfig(
                    max_salvos=MaxSalvosInfiniteConfig(),
                    ports={"in": PacketCountAllConfig()},
                    term=SalvoConditionTermPortConfig(
                        port_name="in",
                        state=PortStateNonEmptyConfig(),
                    ),
                ),
            }
        out_salvo_conditions = None
        if has_out:
            out_salvo_conditions = {
                "default": SalvoConditionConfig(
                    max_salvos=MaxSalvosFiniteConfig(max=1),
                    ports={"out": PacketCountAllConfig()},
                    term=SalvoConditionTermPortConfig(
                        port_name="out",
                        state=PortStateNonEmptyConfig(),
                    ),
                ),
            }
        return NodeConfig(
            name=name,
            in_ports=in_ports,
            out_ports=out_ports,
            in_salvo_conditions=in_salvo_conditions,
            out_salvo_conditions=out_salvo_conditions,
            execution_config=NodeExecutionConfig(
                pools=["main"],
                exec_node_func=tracking_passthrough,
            ),
        )

    config = _make_net_config(
        nodes=[
            _make_tracked_node("A"),
            _make_tracked_node("B"),
            _make_sink_node("C"),
            _make_tracked_node("D"),
            _make_sink_node("E"),
        ],
        edges=[
            EdgeConfig(source_str="A.out", target_str="B.in"),
            EdgeConfig(source_str="B.out", target_str="C.in"),
            EdgeConfig(source_str="D.out", target_str="E.in"),
        ],
    )

    async with Net(config) as net:
        net.inject_data("A", "in", [1])
        net.inject_data("D", "in", [2])

        salvos = await net.run_to_targets("C")

        # Only A and B should execute, not D
        assert "A" in executed_nodes
        assert "B" in executed_nodes
        assert "D" not in executed_nodes

        # C's salvo should be collected
        assert len(salvos) == 1
        assert salvos[0].node_name == "C"

# %% [markdown]
# ## Test: cycle handling

# %%
#|export
@pytest.mark.asyncio
async def test_cycle_handling():
    """A->B->C->A cycle: targeting C should not infinite-loop."""
    iteration_count = []

    def cycle_passthrough(ctx, packets):
        iteration_count.append(ctx.node_name)
        for port_name, pkt_ids in packets.items():
            for pid in pkt_ids:
                val = ctx.consume_packet(pid)
                out_id = ctx.create_packet(val)
                ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("default")

    def _make_cycle_node(name):
        return NodeConfig(
            name=name,
            in_ports={"in": PortConfig()},
            out_ports={"out": PortConfig()},
            in_salvo_conditions={
                "default": SalvoConditionConfig(
                    max_salvos=MaxSalvosInfiniteConfig(),
                    ports={"in": PacketCountAllConfig()},
                    term=SalvoConditionTermPortConfig(
                        port_name="in",
                        state=PortStateNonEmptyConfig(),
                    ),
                ),
            },
            out_salvo_conditions={
                "default": SalvoConditionConfig(
                    max_salvos=MaxSalvosInfiniteConfig(),
                    ports={"out": PacketCountAllConfig()},
                    term=SalvoConditionTermPortConfig(
                        port_name="out",
                        state=PortStateNonEmptyConfig(),
                    ),
                ),
            },
            execution_config=NodeExecutionConfig(
                pools=["main"],
                exec_node_func=cycle_passthrough,
            ),
        )

    config = _make_net_config(
        nodes=[
            _make_cycle_node("A"),
            _make_cycle_node("B"),
            _make_sink_node("C"),
        ],
        edges=[
            EdgeConfig(source_str="A.out", target_str="B.in"),
            EdgeConfig(source_str="B.out", target_str="C.in"),
        ],
    )

    async with Net(config) as net:
        net.inject_data("A", "in", [7])
        salvos = await net.run_to_targets("C", max_iterations=100)

        # Should get C's salvo without infinite loop
        assert len(salvos) == 1
        assert salvos[0].node_name == "C"
        assert salvos[0].packets["in"] == [7]
        # A and B should have executed
        assert "A" in iteration_count
        assert "B" in iteration_count

# %% [markdown]
# ## Test: empty result (no injection)

# %%
#|export
@pytest.mark.asyncio
async def test_empty_result():
    """No injection -> no salvos returned."""
    config = _make_net_config(
        nodes=[
            _make_passthrough_node("A"),
            _make_sink_node("B"),
        ],
        edges=[
            EdgeConfig(source_str="A.out", target_str="B.in"),
        ],
    )

    async with Net(config) as net:
        salvos = await net.run_to_targets("B")
        assert salvos == []

# %% [markdown]
# ## Test: invalid target node

# %%
#|export
@pytest.mark.asyncio
async def test_invalid_target_node():
    """Targeting a non-existent node raises ValueError."""
    config = _make_net_config(
        nodes=[
            _make_passthrough_node("A"),
            _make_sink_node("B"),
        ],
        edges=[
            EdgeConfig(source_str="A.out", target_str="B.in"),
        ],
    )

    async with Net(config) as net:
        with pytest.raises(ValueError, match="Target node 'Z' not found"):
            await net.run_to_targets("Z")

# %% [markdown]
# ## Test: upstream helper unit tests

# %%
#|export
def test_compute_upstream_nodes_linear():
    """Test _compute_upstream_nodes on a linear graph."""
    from dataclasses import dataclass

    @dataclass
    class _Port:
        node_name: str
        port_name: str

    @dataclass
    class _Edge:
        source: _Port
        target: _Port

    edges = [
        _Edge(_Port("A", "out"), _Port("B", "in")),
        _Edge(_Port("B", "out"), _Port("C", "in")),
        _Edge(_Port("C", "out"), _Port("D", "in")),
    ]

    upstream = _compute_upstream_nodes(edges, {"D"})
    assert upstream == {"A", "B", "C"}

    upstream = _compute_upstream_nodes(edges, {"C"})
    assert upstream == {"A", "B"}

    upstream = _compute_upstream_nodes(edges, {"A"})
    assert upstream == set()

# %%
test_compute_upstream_nodes_linear()

# %%
#|export
def test_compute_upstream_nodes_diamond():
    """Test _compute_upstream_nodes on a diamond graph."""
    from dataclasses import dataclass

    @dataclass
    class _Port:
        node_name: str
        port_name: str

    @dataclass
    class _Edge:
        source: _Port
        target: _Port

    edges = [
        _Edge(_Port("A", "out"), _Port("B", "in")),
        _Edge(_Port("A", "out"), _Port("C", "in")),
        _Edge(_Port("B", "out"), _Port("D", "in")),
        _Edge(_Port("C", "out"), _Port("D", "in")),
    ]

    upstream = _compute_upstream_nodes(edges, {"D"})
    assert upstream == {"A", "B", "C"}

# %%
test_compute_upstream_nodes_diamond()

# %%
#|export
def test_compute_upstream_nodes_disconnected():
    """Test _compute_upstream_nodes ignores disconnected nodes."""
    from dataclasses import dataclass

    @dataclass
    class _Port:
        node_name: str
        port_name: str

    @dataclass
    class _Edge:
        source: _Port
        target: _Port

    edges = [
        _Edge(_Port("A", "out"), _Port("B", "in")),
        _Edge(_Port("X", "out"), _Port("Y", "in")),
    ]

    upstream = _compute_upstream_nodes(edges, {"B"})
    assert upstream == {"A"}
    assert "X" not in upstream
    assert "Y" not in upstream

# %%
test_compute_upstream_nodes_disconnected()

# %%
#|export
def test_classify_epoch_target():
    """Test _classify_epoch returns 'target' for target nodes."""
    from dataclasses import dataclass

    @dataclass
    class _Salvo:
        salvo_condition: str

    @dataclass
    class _Epoch:
        node_name: str
        in_salvo: _Salvo

    epoch = _Epoch("B", _Salvo("default"))
    result = _classify_epoch(epoch, {("B", None)}, {"A"})
    assert result == "target"

# %%
test_classify_epoch_target()

# %%
#|export
def test_classify_epoch_target_specific_condition():
    """Test _classify_epoch matches specific salvo condition."""
    from dataclasses import dataclass

    @dataclass
    class _Salvo:
        salvo_condition: str

    @dataclass
    class _Epoch:
        node_name: str
        in_salvo: _Salvo

    epoch = _Epoch("B", _Salvo("cond_1"))
    # Match: specific condition
    assert _classify_epoch(epoch, {("B", "cond_1")}, set()) == "target"
    # No match: different condition
    assert _classify_epoch(epoch, {("B", "cond_2")}, set()) != "target"
    # If B is in upstream, non-matching condition -> upstream
    assert _classify_epoch(epoch, {("B", "cond_2")}, {"B"}) == "upstream"

# %%
test_classify_epoch_target_specific_condition()

# %%
#|export
def test_classify_epoch_upstream():
    """Test _classify_epoch returns 'upstream' for upstream nodes."""
    from dataclasses import dataclass

    @dataclass
    class _Salvo:
        salvo_condition: str

    @dataclass
    class _Epoch:
        node_name: str
        in_salvo: _Salvo

    epoch = _Epoch("A", _Salvo("default"))
    result = _classify_epoch(epoch, {("C", None)}, {"A", "B"})
    assert result == "upstream"

# %%
test_classify_epoch_upstream()

# %%
#|export
def test_classify_epoch_irrelevant():
    """Test _classify_epoch returns 'irrelevant' for unrelated nodes."""
    from dataclasses import dataclass

    @dataclass
    class _Salvo:
        salvo_condition: str

    @dataclass
    class _Epoch:
        node_name: str
        in_salvo: _Salvo

    epoch = _Epoch("X", _Salvo("default"))
    result = _classify_epoch(epoch, {("C", None)}, {"A", "B"})
    assert result == "irrelevant"

# %%
test_classify_epoch_irrelevant()

# %% [markdown]
# ## Test: multiple targets

# %%
#|export
@pytest.mark.asyncio
async def test_multiple_targets():
    """Target both B and C in A->B, A->C graph (A has separate output ports)."""
    def fork_func(ctx, packets):
        """Create separate output packets for each output port."""
        for port_name, pkt_ids in packets.items():
            for pid in pkt_ids:
                val = ctx.consume_packet(pid)
                out_b = ctx.create_packet(val)
                ctx.load_output_port("out_b", out_b)
                out_c = ctx.create_packet(val)
                ctx.load_output_port("out_c", out_c)
        ctx.send_output_salvo("default")

    fork_node = NodeConfig(
        name="A",
        in_ports={"in": PortConfig()},
        out_ports={"out_b": PortConfig(), "out_c": PortConfig()},
        in_salvo_conditions={
            "default": SalvoConditionConfig(
                max_salvos=MaxSalvosInfiniteConfig(),
                ports={"in": PacketCountAllConfig()},
                term=SalvoConditionTermPortConfig(
                    port_name="in",
                    state=PortStateNonEmptyConfig(),
                ),
            ),
        },
        out_salvo_conditions={
            "default": SalvoConditionConfig(
                max_salvos=MaxSalvosFiniteConfig(max=1),
                ports={"out_b": PacketCountAllConfig(), "out_c": PacketCountAllConfig()},
                term=SalvoConditionTermTrueConfig(),
            ),
        },
        execution_config=NodeExecutionConfig(
            pools=["main"],
            exec_node_func=fork_func,
        ),
    )

    config = _make_net_config(
        nodes=[
            fork_node,
            _make_sink_node("B"),
            _make_sink_node("C"),
        ],
        edges=[
            EdgeConfig(source_str="A.out_b", target_str="B.in"),
            EdgeConfig(source_str="A.out_c", target_str="C.in"),
        ],
    )

    async with Net(config) as net:
        net.inject_data("A", "in", [5])
        salvos = await net.run_to_targets(["B", "C"])

        assert len(salvos) == 2
        names = sorted([s.node_name for s in salvos])
        assert names == ["B", "C"]
        for s in salvos:
            assert s.packets["in"] == [5]
