# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for NetObserver

# %%
#|default_exp observe.test_observer

# %%
#|export
import pytest

from netrun.net._net._net import Net
from netrun.net.config import (
    NetConfig,
    GraphConfig,
    NodeConfig,
    PortConfig,
    EdgeConfig,
    PoolConfig,
    MainPoolConfig,
    NodeExecutionConfig,
)

from netrun_utils.observe.observer import NetObserver

# %% [markdown]
# ## Helpers

# %%
#|export
def _make_config() -> NetConfig:
    """Create a minimal 2-node net config for testing."""
    return NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="source",
                    out_ports={"out": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=lambda ctx, packets: None,
                    ),
                ),
                NodeConfig(
                    name="processor",
                    in_ports={"data": PortConfig()},
                    out_ports={"result": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_processor_func,
                    ),
                ),
            ],
            edges=[
                EdgeConfig(source_node="source", source_port="out", target_node="processor", target_port="data"),
            ],
        ),
        retain_epoch_logs=True,
    )


def _processor_func(ctx, packets):
    packet_id = list(packets["data"])[0]
    ctx.consume_packet(packet_id)

# %% [markdown]
# ## Status Tests

# %%
#|export
async def test_get_status_before_start():
    config = _make_config()
    net = Net(config, run_init_nodes=False)
    obs = NetObserver(net)
    status = obs.get_status()
    assert set(status.node_names) == {"source", "processor"}
    assert status.edge_count == 1
    assert status.initialized is False


async def test_get_status_after_start():
    config = _make_config()
    async with Net(config, run_init_nodes=False) as net:
        obs = NetObserver(net)
        status = obs.get_status()
        assert status.initialized is True
        assert status.paused is False

# %% [markdown]
# ## Node Tests

# %%
#|export
async def test_get_nodes():
    config = _make_config()
    net = Net(config, run_init_nodes=False)
    obs = NetObserver(net)
    nodes = obs.get_nodes()
    assert len(nodes) == 2
    names = {n.name for n in nodes}
    assert names == {"source", "processor"}

    proc = obs.get_node("processor")
    assert proc.in_port_names == ["data"]
    assert proc.out_port_names == ["result"]
    assert proc.enabled is True
    assert proc.is_busy is False

# %% [markdown]
# ## Edge Tests

# %%
#|export
async def test_get_edges():
    config = _make_config()
    net = Net(config, run_init_nodes=False)
    obs = NetObserver(net)
    edges = obs.get_edges()
    assert len(edges) == 1
    assert edges[0].source_node == "source"
    assert edges[0].target_node == "processor"
    assert edges[0].packet_count == 0

# %% [markdown]
# ## Control Tests

# %%
#|export
async def test_enable_disable_node():
    config = _make_config()
    async with Net(config, run_init_nodes=False) as net:
        obs = NetObserver(net)

        resp = obs.disable_node("processor")
        assert resp.ok is True
        assert obs.get_node("processor").enabled is False

        resp = obs.enable_node("processor")
        assert resp.ok is True
        assert obs.get_node("processor").enabled is True


async def test_inject_data():
    config = _make_config()
    async with Net(config, run_init_nodes=False) as net:
        obs = NetObserver(net)
        resp = obs.inject_data("processor", "data", [42])
        assert resp.ok is True

# %% [markdown]
# ## Epoch Log Tests

# %%
#|export
async def test_epoch_logs_after_execution():
    config = _make_config()
    async with Net(config, run_init_nodes=False) as net:
        net.inject_data("processor", "data", [10])
        # run_until_blocked auto-starts and executes the epoch
        await net.run_until_blocked()

        obs = NetObserver(net)
        epoch_logs = obs.get_epoch_logs()
        assert len(epoch_logs) >= 1
        finished = [e for e in epoch_logs if e.state == "finished"]
        assert len(finished) == 1
        assert finished[0].node_name == "processor"
        assert finished[0].outcome == "success"

# %% [markdown]
# ## Warning Test

# %%
#|export
def test_observer_with_retain_epoch_logs_false():
    """Observer works even when retain_epoch_logs is False."""
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(nodes=[
            NodeConfig(name="n", execution_config=NodeExecutionConfig(pools=["main"], exec_node_func=lambda ctx, packets: None)),
        ]),
        retain_epoch_logs=False,
    )
    net = Net(config, run_init_nodes=False)
    obs = NetObserver(net)
    status = obs.get_status()
    assert status.initialized is False
