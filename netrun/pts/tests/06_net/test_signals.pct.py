# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Node Signals
#
# Tests for signal utilities, signal port auto-generation, and signal emission.

# %%
#|default_exp net.test_signals

# %%
#|export
import pytest
import asyncio
from datetime import datetime

from netrun.net._net import (
    Net,
    NodeExecutionContext,
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
    SignalValue,
    EpochSignalValue,
    EpochFailedSignalValue,
    signal_port_name,
    is_signal_port,
    signal_type_from_port,
    VALID_SIGNAL_TYPES,
    validate_signal_types,
    generate_signal_ports,
    generate_signal_salvo_conditions,
)

# %% [markdown]
# ## Signal Utility Tests

# %%
#|export
def test_signal_port_name():
    """Test signal_port_name generates correct port names."""
    assert signal_port_name("epoch_finished") == "__signal_epoch_finished__"
    assert signal_port_name("epoch_failed") == "__signal_epoch_failed__"
    assert signal_port_name("node_started") == "__signal_node_started__"
    assert signal_port_name("node_stopped") == "__signal_node_stopped__"

# %%
#|export
def test_is_signal_port():
    """Test is_signal_port correctly identifies signal ports."""
    assert is_signal_port("__signal_epoch_finished__") is True
    assert is_signal_port("__signal_node_started__") is True
    assert is_signal_port("output") is False
    assert is_signal_port("__signal_") is False
    assert is_signal_port("__signal_foo") is False
    assert is_signal_port("signal_epoch_finished__") is False

# %%
#|export
def test_signal_type_from_port():
    """Test signal_type_from_port extracts signal type."""
    assert signal_type_from_port("__signal_epoch_finished__") == "epoch_finished"
    assert signal_type_from_port("__signal_node_started__") == "node_started"
    assert signal_type_from_port("output") is None
    assert signal_type_from_port("__signal_") is None

# %%
#|export
def test_validate_signal_types_valid():
    """Test validate_signal_types accepts valid types."""
    validate_signal_types(["epoch_finished"])
    validate_signal_types(["epoch_finished", "epoch_failed"])
    validate_signal_types(["node_started", "node_stopped"])
    validate_signal_types(list(VALID_SIGNAL_TYPES))
    validate_signal_types([])  # empty is valid

# %%
#|export
def test_validate_signal_types_invalid():
    """Test validate_signal_types rejects invalid types."""
    with pytest.raises(ValueError, match="Invalid signal type"):
        validate_signal_types(["invalid_signal"])
    with pytest.raises(ValueError, match="Invalid signal type"):
        validate_signal_types(["epoch_finished", "bogus"])

# %%
#|export
def test_generate_signal_ports():
    """Test generate_signal_ports creates correct port configs."""
    ports = generate_signal_ports(["epoch_finished", "epoch_failed"])
    assert "__signal_epoch_finished__" in ports
    assert "__signal_epoch_failed__" in ports
    assert len(ports) == 2
    # Each port should be a PortConfig (default infinite slots)
    assert isinstance(ports["__signal_epoch_finished__"], PortConfig)

# %%
#|export
def test_generate_signal_salvo_conditions():
    """Test generate_signal_salvo_conditions creates correct salvo configs."""
    salvos = generate_signal_salvo_conditions(["epoch_finished"])
    assert "__signal_epoch_finished__" in salvos
    salvo = salvos["__signal_epoch_finished__"]
    assert "__signal_epoch_finished__" in salvo.ports

# %%
#|export
def test_signal_value_dataclass():
    """Test SignalValue hierarchy can be created with all fields."""
    # Base SignalValue (node lifecycle)
    sv_base = SignalValue(
        signal="node_started",
        node_name="A",
        timestamp=datetime.now(),
    )
    assert sv_base.signal == "node_started"
    assert sv_base.node_name == "A"
    assert not isinstance(sv_base, EpochSignalValue)

    # EpochSignalValue (epoch lifecycle)
    sv_epoch = EpochSignalValue(
        signal="epoch_finished",
        node_name="A",
        epoch_id="epoch_123",
        timestamp=datetime.now(),
    )
    assert sv_epoch.signal == "epoch_finished"
    assert sv_epoch.epoch_id == "epoch_123"
    assert isinstance(sv_epoch, SignalValue)
    assert not isinstance(sv_epoch, EpochFailedSignalValue)

    # EpochFailedSignalValue
    sv_err = EpochFailedSignalValue(
        signal="epoch_failed",
        node_name="B",
        epoch_id="epoch_456",
        error="something went wrong",
    )
    assert sv_err.error == "something went wrong"
    assert sv_err.epoch_id == "epoch_456"
    assert isinstance(sv_err, EpochSignalValue)
    assert isinstance(sv_err, SignalValue)

# %% [markdown]
# ## Config Validation Tests

# %%
#|export
def test_node_execution_config_signals_validation():
    """Test that NodeExecutionConfig validates signal types."""
    # Valid
    config = NodeExecutionConfig(node_name="A", signals=["epoch_finished"])
    assert config.signals == ["epoch_finished"]

# %%
#|export
def test_net_config_default_signals_validation():
    """Test that NetConfig validates default_signals."""
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(nodes=[], edges=[]),
        default_signals=["epoch_finished"],
    )
    assert config.default_signals == ["epoch_finished"]

    with pytest.raises(Exception):
        NetConfig(
            pools={"main": PoolConfig(spec=MainPoolConfig())},
            graph=GraphConfig(nodes=[], edges=[]),
            default_signals=["invalid"],
        )

# %% [markdown]
# ## Signal Port Auto-Generation Tests

# %%
#|export
def test_signal_ports_auto_generated_on_resolve():
    """Test that signal ports are auto-generated during NodeConfig.resolve()."""
    node = NodeConfig(
        name="A",
        in_ports={"in": PortConfig()},
        out_ports={"out": PortConfig()},
        execution_config=NodeExecutionConfig(
            node_name="A",
            pools=["main"],
            signals=["epoch_finished", "epoch_failed"],
        ),
    )
    resolved = node.resolve()
    assert "__signal_epoch_finished__" in resolved.out_ports
    assert "__signal_epoch_failed__" in resolved.out_ports
    assert "out" in resolved.out_ports  # original port preserved
    # Signal salvo conditions should also be generated
    assert "__signal_epoch_finished__" in resolved.out_salvo_conditions
    assert "__signal_epoch_failed__" in resolved.out_salvo_conditions

# %%
#|export
def test_signal_ports_inherited_from_net_config():
    """Test that signal ports are inherited from NetConfig.default_signals."""
    net_config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        node_name="A",
                        pools=["main"],
                        # signals=None means inherit from net default
                    ),
                ),
            ],
            edges=[],
        ),
        default_signals=["epoch_finished"],
    )
    resolved = net_config.resolve()
    node_a = resolved.graph.nodes[0]
    assert "__signal_epoch_finished__" in node_a.out_ports

# %%
#|export
def test_signal_ports_opt_out_with_empty_list():
    """Test that signals=[] opts out even when net has default_signals."""
    net_config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        node_name="A",
                        pools=["main"],
                        signals=[],  # explicit opt-out
                    ),
                ),
            ],
            edges=[],
        ),
        default_signals=["epoch_finished"],
    )
    resolved = net_config.resolve()
    node_a = resolved.graph.nodes[0]
    assert "__signal_epoch_finished__" not in node_a.out_ports

# %%
#|export
def test_no_signal_ports_by_default():
    """Test backward compat: no signal ports when no signals configured."""
    node = NodeConfig(
        name="A",
        in_ports={"in": PortConfig()},
        out_ports={"out": PortConfig()},
        execution_config=NodeExecutionConfig(
            node_name="A",
            pools=["main"],
        ),
    )
    resolved = node.resolve()
    # No signal ports should exist
    for port_name in resolved.out_ports:
        assert not is_signal_port(port_name), f"Unexpected signal port: {port_name}"

# %% [markdown]
# ## Signal Emission Integration Tests
#
# Note: exec functions always receive `(ctx, packets)` where packets is `dict[str, list[str]]`.
# We use `run_until_blocked()` + `execute_startable_epochs()` loop for execution.

# %%
#|export
from netrun.net.config import (
    SalvoConditionConfig,
    SalvoConditionTermTrueConfig,
    SalvoConditionTermPortConfig,
    PortStateNonEmptyConfig,
    MaxSalvosFiniteConfig,
    MaxSalvosInfiniteConfig,
    PacketCountAllConfig,
)

async def _run_net_to_completion(net):
    """Helper: run a net until no more progress can be made."""
    while True:
        await net.run_until_blocked()
        executed = await net.execute_startable_epochs()
        if not executed:
            break

# %%
#|export
@pytest.mark.asyncio
async def test_epoch_finished_signal_triggers_downstream():
    """Test that epoch_finished signal triggers a downstream node."""
    results = []

    def node_a_exec(ctx, packets):
        """Source node - no output, but emits epoch_finished signal."""
        pass

    def node_b_exec(ctx, packets):
        """Downstream node triggered by signal."""
        for port_name, pkt_ids in packets.items():
            for pid in pkt_ids:
                value = ctx.consume_packet(pid)
                results.append(("B_triggered", value))

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={},
                            term=SalvoConditionTermTrueConfig(),
                        ),
                    },
                    execution_config=NodeExecutionConfig(
                        node_name="A",
                        pools=["main"],
                        exec_node_func=node_a_exec,
                        signals=["epoch_finished"],
                        run_on_startup=True,
                    ),
                ),
                NodeConfig(
                    name="B",
                    in_ports={"trigger": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        node_name="B",
                        pools=["main"],
                        exec_node_func=node_b_exec,
                    ),
                ),
            ],
            edges=[
                EdgeConfig(source_str="A.__signal_epoch_finished__", target_str="B.trigger"),
            ],
        ),
    )

    async with Net(config) as net:
        await _run_net_to_completion(net)

    assert len(results) == 1
    assert results[0][0] == "B_triggered"
    # The trigger value should be an EpochSignalValue
    signal_val = results[0][1]
    assert isinstance(signal_val, EpochSignalValue)
    assert not isinstance(signal_val, EpochFailedSignalValue)
    assert signal_val.signal == "epoch_finished"
    assert signal_val.node_name == "A"
    assert signal_val.epoch_id is not None

# %%
#|export
@pytest.mark.asyncio
async def test_epoch_failed_signal_triggers_downstream():
    """Test that epoch_failed signal triggers a downstream node when a node fails."""
    results = []

    def node_a_exec(ctx, packets):
        raise RuntimeError("intentional failure")

    def node_b_exec(ctx, packets):
        for port_name, pkt_ids in packets.items():
            for pid in pkt_ids:
                value = ctx.consume_packet(pid)
                results.append(("B_triggered", value))

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={},
                            term=SalvoConditionTermTrueConfig(),
                        ),
                    },
                    execution_config=NodeExecutionConfig(
                        node_name="A",
                        pools=["main"],
                        exec_node_func=node_a_exec,
                        signals=["epoch_failed"],
                        run_on_startup=True,
                        propagate_exceptions=False,
                        retries=0,
                    ),
                ),
                NodeConfig(
                    name="B",
                    in_ports={"trigger": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        node_name="B",
                        pools=["main"],
                        exec_node_func=node_b_exec,
                    ),
                ),
            ],
            edges=[
                EdgeConfig(source_str="A.__signal_epoch_failed__", target_str="B.trigger"),
            ],
        ),
    )

    async with Net(config) as net:
        await _run_net_to_completion(net)

    assert len(results) == 1
    signal_val = results[0][1]
    assert isinstance(signal_val, EpochFailedSignalValue)
    assert signal_val.signal == "epoch_failed"
    assert signal_val.node_name == "A"
    assert signal_val.epoch_id is not None
    assert "intentional failure" in signal_val.error

# %%
#|export
@pytest.mark.asyncio
async def test_node_started_signal():
    """Test that node_started signal is emitted when a node starts."""
    results = []

    def node_a_exec(ctx, packets):
        pass

    def node_b_exec(ctx, packets):
        for port_name, pkt_ids in packets.items():
            for pid in pkt_ids:
                value = ctx.consume_packet(pid)
                results.append(value)

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={},
                            term=SalvoConditionTermTrueConfig(),
                        ),
                    },
                    execution_config=NodeExecutionConfig(
                        node_name="A",
                        pools=["main"],
                        exec_node_func=node_a_exec,
                        signals=["node_started"],
                    ),
                ),
                NodeConfig(
                    name="B",
                    in_ports={"trigger": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        node_name="B",
                        pools=["main"],
                        exec_node_func=node_b_exec,
                    ),
                ),
            ],
            edges=[
                EdgeConfig(source_str="A.__signal_node_started__", target_str="B.trigger"),
            ],
        ),
    )

    async with Net(config) as net:
        # node_started signal is emitted during Net.start() → _start_all_nodes()
        # Run to let B pick up the signal
        await _run_net_to_completion(net)

    assert len(results) == 1
    signal_val = results[0]
    assert isinstance(signal_val, SignalValue)
    assert not isinstance(signal_val, EpochSignalValue)
    assert signal_val.signal == "node_started"
    assert signal_val.node_name == "A"

# %%
#|export
@pytest.mark.asyncio
async def test_node_stopped_signal():
    """Test that node_stopped signal doesn't crash (signal emitted during shutdown)."""
    def node_a_exec(ctx, packets):
        pass

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={},
                            term=SalvoConditionTermTrueConfig(),
                        ),
                    },
                    execution_config=NodeExecutionConfig(
                        node_name="A",
                        pools=["main"],
                        exec_node_func=node_a_exec,
                        signals=["node_stopped"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    # node_stopped signal is emitted during Net.stop() — B can't execute
    # because the net is already stopping. Just verify no crash.
    async with Net(config) as net:
        pass

# %%
#|export
@pytest.mark.asyncio
async def test_signal_unconnected_port_no_crash():
    """Test that signals on unconnected ports don't crash."""
    def node_a_exec(ctx, packets):
        pass

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={},
                            term=SalvoConditionTermTrueConfig(),
                        ),
                    },
                    execution_config=NodeExecutionConfig(
                        node_name="A",
                        pools=["main"],
                        exec_node_func=node_a_exec,
                        signals=["epoch_finished", "epoch_failed", "node_started", "node_stopped"],
                        run_on_startup=True,
                    ),
                ),
            ],
            edges=[],
        ),
    )

    # Should not crash even though no edges connect to signal ports
    async with Net(config) as net:
        await _run_net_to_completion(net)

# %%
#|export
def test_signal_no_output_queue_for_signal_ports():
    """Test that signal ports don't auto-generate output queues."""
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    out_ports={"real_out": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        node_name="A",
                        pools=["main"],
                        signals=["epoch_finished"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    resolved = config.resolve()
    # real_out should have an output queue, but signal port should not
    queue_names = list(resolved.output_queues.keys())
    assert "A::real_out" in queue_names
    assert not any("__signal_" in name for name in queue_names)

# %%
#|export
@pytest.mark.asyncio
async def test_default_signals_applied_to_all_nodes():
    """Test that default_signals applies to all nodes."""
    results = []

    def node_a_exec(ctx, packets):
        pass

    def node_b_exec(ctx, packets):
        for port_name, pkt_ids in packets.items():
            for pid in pkt_ids:
                value = ctx.consume_packet(pid)
                results.append(("B", value))

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        default_signals=["epoch_finished"],
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={},
                            term=SalvoConditionTermTrueConfig(),
                        ),
                    },
                    execution_config=NodeExecutionConfig(
                        node_name="A",
                        pools=["main"],
                        exec_node_func=node_a_exec,
                        run_on_startup=True,
                    ),
                ),
                NodeConfig(
                    name="B",
                    in_ports={"trigger": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        node_name="B",
                        pools=["main"],
                        exec_node_func=node_b_exec,
                    ),
                ),
            ],
            edges=[
                EdgeConfig(source_str="A.__signal_epoch_finished__", target_str="B.trigger"),
            ],
        ),
    )

    async with Net(config) as net:
        await _run_net_to_completion(net)

    assert len(results) == 1
    assert isinstance(results[0][1], SignalValue)

# %%
#|export
@pytest.mark.asyncio
async def test_signal_with_regular_output():
    """Test that signals work alongside regular output packets."""
    signal_results = []
    data_results = []

    def node_a_exec(ctx, packets):
        pkt = ctx.create_packet("hello")
        ctx.load_output_port("out", pkt)
        ctx.send_output_salvo("send_out")

    def signal_handler(ctx, packets):
        for pkt_ids in packets.values():
            for pid in pkt_ids:
                signal_results.append(ctx.consume_packet(pid))

    def data_handler(ctx, packets):
        for pkt_ids in packets.values():
            for pid in pkt_ids:
                data_results.append(ctx.consume_packet(pid))

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={},
                            term=SalvoConditionTermTrueConfig(),
                        ),
                    },
                    out_ports={"out": PortConfig()},
                    out_salvo_conditions={
                        "send_out": SalvoConditionConfig(
                            max_salvos=MaxSalvosInfiniteConfig(),
                            ports={"out": PacketCountAllConfig()},
                            term=SalvoConditionTermPortConfig(
                                port_name="out",
                                state=PortStateNonEmptyConfig(),
                            ),
                        ),
                    },
                    execution_config=NodeExecutionConfig(
                        node_name="A",
                        pools=["main"],
                        exec_node_func=node_a_exec,
                        signals=["epoch_finished"],
                        run_on_startup=True,
                    ),
                ),
                NodeConfig(
                    name="SignalHandler",
                    in_ports={"trigger": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        node_name="SignalHandler",
                        pools=["main"],
                        exec_node_func=signal_handler,
                    ),
                ),
                NodeConfig(
                    name="DataHandler",
                    in_ports={"data_in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        node_name="DataHandler",
                        pools=["main"],
                        exec_node_func=data_handler,
                    ),
                ),
            ],
            edges=[
                EdgeConfig(source_str="A.__signal_epoch_finished__", target_str="SignalHandler.trigger"),
                EdgeConfig(source_str="A.out", target_str="DataHandler.data_in"),
            ],
        ),
    )

    async with Net(config) as net:
        await _run_net_to_completion(net)

    # Both should have received their respective packets
    assert len(signal_results) == 1
    assert isinstance(signal_results[0], SignalValue)
    assert len(data_results) == 1
    assert data_results[0] == "hello"

# %%
#|export
@pytest.mark.asyncio
async def test_node_started_signal_deferred_startup():
    """Test that node_started signal fires on first epoch for deferred nodes."""
    results = []

    def node_a_exec(ctx, packets):
        pass

    def node_b_exec(ctx, packets):
        for pkt_ids in packets.values():
            for pid in pkt_ids:
                results.append(ctx.consume_packet(pid))

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={},
                            term=SalvoConditionTermTrueConfig(),
                        ),
                    },
                    execution_config=NodeExecutionConfig(
                        node_name="A",
                        pools=["main"],
                        exec_node_func=node_a_exec,
                        signals=["node_started"],
                        defer_startup=True,
                        run_on_startup=True,
                    ),
                ),
                NodeConfig(
                    name="B",
                    in_ports={"trigger": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        node_name="B",
                        pools=["main"],
                        exec_node_func=node_b_exec,
                    ),
                ),
            ],
            edges=[
                EdgeConfig(source_str="A.__signal_node_started__", target_str="B.trigger"),
            ],
        ),
    )

    async with Net(config) as net:
        await _run_net_to_completion(net)

    # node_started should fire when A's first epoch triggers deferred startup
    assert len(results) == 1
    assert isinstance(results[0], SignalValue)
    assert results[0].signal == "node_started"
    assert results[0].node_name == "A"

# %%
#|export
@pytest.mark.asyncio
async def test_per_node_signal_override():
    """Test that per-node signals override net default."""
    results_b = []

    def node_a_exec(ctx, packets):
        pass

    def node_b_exec(ctx, packets):
        pass

    def handler_b(ctx, packets):
        for pkt_ids in packets.values():
            for pid in pkt_ids:
                results_b.append(ctx.consume_packet(pid))

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        default_signals=["epoch_finished"],
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={},
                            term=SalvoConditionTermTrueConfig(),
                        ),
                    },
                    execution_config=NodeExecutionConfig(
                        node_name="A",
                        pools=["main"],
                        exec_node_func=node_a_exec,
                        run_on_startup=True,
                        signals=[],  # opt-out
                    ),
                ),
                NodeConfig(
                    name="B",
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={},
                            term=SalvoConditionTermTrueConfig(),
                        ),
                    },
                    execution_config=NodeExecutionConfig(
                        node_name="B",
                        pools=["main"],
                        exec_node_func=node_b_exec,
                        run_on_startup=True,
                        # signals=None → inherits default_signals=["epoch_finished"]
                    ),
                ),
                NodeConfig(
                    name="HandlerB",
                    in_ports={"trigger": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        node_name="HandlerB",
                        pools=["main"],
                        exec_node_func=handler_b,
                    ),
                ),
            ],
            edges=[
                # A has no signal ports (opted out), so no edge from A
                EdgeConfig(source_str="B.__signal_epoch_finished__", target_str="HandlerB.trigger"),
            ],
        ),
    )

    async with Net(config) as net:
        await _run_net_to_completion(net)

    # B inherited default, so HandlerB should have been triggered
    assert len(results_b) == 1
    assert isinstance(results_b[0], SignalValue)

# %% [markdown]
# ## New Signal Emission Tests

# %%
#|export
@pytest.mark.asyncio
async def test_epoch_started_signal_triggers_downstream():
    """Test that epoch_started signal triggers a downstream node with EpochSignalValue."""
    results = []

    def node_a_exec(ctx, packets):
        pass

    def node_b_exec(ctx, packets):
        for pkt_ids in packets.values():
            for pid in pkt_ids:
                results.append(ctx.consume_packet(pid))

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={},
                            term=SalvoConditionTermTrueConfig(),
                        ),
                    },
                    execution_config=NodeExecutionConfig(
                        node_name="A",
                        pools=["main"],
                        exec_node_func=node_a_exec,
                        signals=["epoch_started"],
                        run_on_startup=True,
                    ),
                ),
                NodeConfig(
                    name="B",
                    in_ports={"trigger": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        node_name="B",
                        pools=["main"],
                        exec_node_func=node_b_exec,
                    ),
                ),
            ],
            edges=[
                EdgeConfig(source_str="A.__signal_epoch_started__", target_str="B.trigger"),
            ],
        ),
    )

    async with Net(config) as net:
        await _run_net_to_completion(net)

    assert len(results) == 1
    signal_val = results[0]
    assert isinstance(signal_val, EpochSignalValue)
    assert not isinstance(signal_val, EpochFailedSignalValue)
    assert signal_val.signal == "epoch_started"
    assert signal_val.node_name == "A"
    assert signal_val.epoch_id is not None

# %%
#|export
@pytest.mark.asyncio
async def test_epoch_cancelled_signal_user_cancel():
    """Test that epoch_cancelled signal is emitted when ctx.cancel_epoch() is called."""
    results = []

    def node_a_exec(ctx, packets):
        ctx.cancel_epoch()

    def node_b_exec(ctx, packets):
        for pkt_ids in packets.values():
            for pid in pkt_ids:
                results.append(ctx.consume_packet(pid))

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={},
                            term=SalvoConditionTermTrueConfig(),
                        ),
                    },
                    execution_config=NodeExecutionConfig(
                        node_name="A",
                        pools=["main"],
                        exec_node_func=node_a_exec,
                        signals=["epoch_cancelled"],
                        run_on_startup=True,
                    ),
                ),
                NodeConfig(
                    name="B",
                    in_ports={"trigger": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        node_name="B",
                        pools=["main"],
                        exec_node_func=node_b_exec,
                    ),
                ),
            ],
            edges=[
                EdgeConfig(source_str="A.__signal_epoch_cancelled__", target_str="B.trigger"),
            ],
        ),
    )

    async with Net(config) as net:
        await _run_net_to_completion(net)

    assert len(results) == 1
    signal_val = results[0]
    assert isinstance(signal_val, EpochSignalValue)
    assert signal_val.signal == "epoch_cancelled"
    assert signal_val.node_name == "A"
    assert signal_val.epoch_id is not None

# %%
#|export
@pytest.mark.asyncio
async def test_epoch_cancelled_signal_max_epochs():
    """Test that epoch_cancelled signal includes epoch_id when triggered by max_epochs limit."""
    results = []

    call_count = 0
    def node_a_exec(ctx, packets):
        nonlocal call_count
        call_count += 1
        # Create an output that feeds back to self to trigger another epoch
        pkt = ctx.create_packet("data")
        ctx.load_output_port("out", pkt)
        ctx.send_output_salvo("send_out")

    def node_b_exec(ctx, packets):
        for pkt_ids in packets.values():
            for pid in pkt_ids:
                results.append(ctx.consume_packet(pid))

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_ports={"in": PortConfig()},
                    in_salvo_conditions={
                        # "loop" MUST come before "trigger" — netsim checks conditions
                        # in insertion order and first match wins. If "trigger" (term=True)
                        # is first, it fires on every run_step even when packets are waiting
                        # at "in", starving the "loop" condition.
                        "loop": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={"in": PacketCountAllConfig()},
                            term=SalvoConditionTermPortConfig(
                                port_name="in",
                                state=PortStateNonEmptyConfig(),
                            ),
                        ),
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={},
                            term=SalvoConditionTermTrueConfig(),
                        ),
                    },
                    out_ports={"out": PortConfig()},
                    out_salvo_conditions={
                        "send_out": SalvoConditionConfig(
                            max_salvos=MaxSalvosInfiniteConfig(),
                            ports={"out": PacketCountAllConfig()},
                            term=SalvoConditionTermPortConfig(
                                port_name="out",
                                state=PortStateNonEmptyConfig(),
                            ),
                        ),
                    },
                    execution_config=NodeExecutionConfig(
                        node_name="A",
                        pools=["main"],
                        exec_node_func=node_a_exec,
                        signals=["epoch_cancelled"],
                        run_on_startup=True,
                        max_epochs=1,
                        propagate_exceptions=False,
                    ),
                ),
                NodeConfig(
                    name="B",
                    in_ports={"trigger": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        node_name="B",
                        pools=["main"],
                        exec_node_func=node_b_exec,
                    ),
                ),
            ],
            edges=[
                EdgeConfig(source_str="A.out", target_str="A.in"),  # self-loop
                EdgeConfig(source_str="A.__signal_epoch_cancelled__", target_str="B.trigger"),
            ],
        ),
    )

    async with Net(config, run_startup_nodes=False) as net:
        await net.execute_node("A")
        # A's output is on the self-loop edge. Use auto_start_epochs=False
        # to avoid infinite loop (run_until_blocked with auto_start would
        # keep cycling through the max_epochs cancel + signal delivery).
        for _ in range(10):
            await net.run_until_blocked(auto_start_epochs=False)
            executed = await net.execute_startable_epochs()
            if not executed:
                break

    # A runs once (max_epochs=1), second epoch gets cancelled
    assert call_count == 1
    assert len(results) == 1
    signal_val = results[0]
    assert isinstance(signal_val, EpochSignalValue)
    assert signal_val.signal == "epoch_cancelled"
    assert signal_val.node_name == "A"
    # This was the bug: epoch_id should be set even for max_epochs cancellation
    assert signal_val.epoch_id is not None
