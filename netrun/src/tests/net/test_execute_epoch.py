# Tests for public execute_epoch method

import pytest

from netrun.net._net import Net, NodeExecutionContext
from netrun.net.config import (
    NetConfig,
    GraphConfig,
    NodeGraphConfig,
    NodeExecutionConfig,
    PortConfig,
    PoolConfig,
    MainPoolConfig,
    SalvoConditionConfig,
    SalvoConditionTermPortConfig,
    MaxSalvosFiniteConfig,
    PacketCountAllConfig,
    PortStateNonEmptyConfig,
)


def _create_test_net(exec_func=None) -> Net:
    """Create a test Net with optional execution function."""
    graph_config = GraphConfig(
        nodes=[
            NodeGraphConfig(
                name="TestNode",
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
                execution_config=NodeExecutionConfig(
                    node_name="TestNode",
                    pools=["main"],
                    exec_node_func=exec_func,
                ) if exec_func else None,
            ),
        ],
        edges=[],
    )

    config = NetConfig(
        pools={"main": PoolConfig(id="main", spec=MainPoolConfig())},
        graph=graph_config,
    )

    return Net(config)


@pytest.mark.asyncio
async def test_execute_epoch_exists():
    """Test that execute_epoch method exists and is public."""
    net = _create_test_net()

    # Method should exist
    assert hasattr(net, 'execute_epoch')
    # Should be callable
    assert callable(net.execute_epoch)


@pytest.mark.asyncio
async def test_execute_epoch_no_exec_func():
    """Test execute_epoch with node that has no execution function.

    When a node has no exec_func, but has input packets, the epoch cannot
    be finished because the packets haven't been consumed. This test verifies
    the error is raised appropriately.
    """
    import netrun_sim

    net = _create_test_net(exec_func=None)

    async with net:
        # Inject data to create an epoch
        net.inject_data("TestNode", "in", [{"data": "test"}])
        await net.run_until_blocked()

        startable = net.get_startable_epochs()
        assert len(startable) == 1

        # Execute epoch - should raise because packets weren't consumed
        with pytest.raises(netrun_sim.CannotFinishNonEmptyEpochError):
            await net.execute_epoch(startable[0])


@pytest.mark.asyncio
async def test_execute_epoch_with_exec_func():
    """Test execute_epoch with a real execution function."""
    executed = []

    def test_func(ctx: NodeExecutionContext, packets):
        executed.append(ctx.epoch_id)
        for port, packet_ids in packets.items():
            for pid in packet_ids:
                ctx.consume_packet(pid)

    net = _create_test_net(exec_func=test_func)

    async with net:
        # Inject data
        net.inject_data("TestNode", "in", [{"data": "test"}])
        await net.run_until_blocked()

        startable = net.get_startable_epochs()
        assert len(startable) == 1
        epoch_id = startable[0]

        # Execute epoch
        result = await net.execute_epoch(epoch_id)

        # Verify execution happened
        assert epoch_id in executed
        assert result is not None


@pytest.mark.asyncio
async def test_execute_epoch_returns_result():
    """Test execute_epoch returns NodeExecutionResult."""
    def test_func(ctx: NodeExecutionContext, packets):
        ctx.print("Hello from test")
        for port, packet_ids in packets.items():
            for pid in packet_ids:
                ctx.consume_packet(pid)

    net = _create_test_net(exec_func=test_func)

    async with net:
        net.inject_data("TestNode", "in", [{"data": "test"}])
        await net.run_until_blocked()

        epoch_id = net.get_startable_epochs()[0]
        result = await net.execute_epoch(epoch_id)

        # Check result structure
        assert result is not None
        assert hasattr(result, 'print_buffer')
        assert hasattr(result, 'cancelled')
        assert result.cancelled is False
