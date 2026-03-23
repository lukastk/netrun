# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Node Controls
#
# Tests for control utilities, control port auto-generation, and control epoch handling.

# %%
#|default_exp net.test_controls

# %%
#|export
import pytest
import asyncio

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
    SalvoConditionConfig,
    SalvoConditionTermTrueConfig,
    MaxSalvosFiniteConfig,
    PacketCountAllConfig,
    control_port_name,
    is_control_port,
    control_type_from_port,
    is_control_salvo_condition,
    VALID_CONTROL_TYPES,
    CONTROL_TYPES,
    ControlType,
    validate_control_types,
    generate_control_ports,
    generate_control_salvo_conditions,
)

# %% [markdown]
# ## Control Utility Tests

# %%
#|export
def test_control_port_name():
    """Test control_port_name generates correct port names."""
    assert control_port_name("enable") == "__control_enable__"
    assert control_port_name("disable") == "__control_disable__"
    assert control_port_name("start_epoch") == "__control_start_epoch__"
    assert control_port_name("cancel_epoch") == "__control_cancel_epoch__"
    assert control_port_name("set_epoch_count") == "__control_set_epoch_count__"

# %%
#|export
def test_is_control_port():
    """Test is_control_port correctly identifies control ports."""
    assert is_control_port("__control_enable__") is True
    assert is_control_port("__control_start_epoch__") is True
    assert is_control_port("in") is False
    assert is_control_port("__signal_epoch_finished__") is False
    assert is_control_port("__control_") is False
    assert is_control_port("__control_foo") is False
    assert is_control_port("control_enable__") is False

# %%
#|export
def test_control_type_from_port():
    """Test control_type_from_port extracts control type."""
    assert control_type_from_port("__control_enable__") == "enable"
    assert control_type_from_port("__control_start_epoch__") == "start_epoch"
    assert control_type_from_port("in") is None
    assert control_type_from_port("__control_") is None

# %%
#|export
def test_is_control_salvo_condition():
    """Test is_control_salvo_condition aliases is_control_port."""
    assert is_control_salvo_condition("__control_enable__") is True
    assert is_control_salvo_condition("default") is False

# %%
#|export
def test_validate_control_types_valid():
    """Test validate_control_types accepts valid types."""
    validate_control_types(["enable", "disable"])
    validate_control_types(["start_epoch", "cancel_epoch", "cancel_all_epochs"])
    validate_control_types(list(VALID_CONTROL_TYPES))
    validate_control_types([])  # empty is valid

# %%
#|export
def test_validate_control_types_invalid():
    """Test validate_control_types rejects invalid types."""
    with pytest.raises(ValueError, match="Invalid control type"):
        validate_control_types(["invalid_control"])
    with pytest.raises(ValueError, match="Invalid control type"):
        validate_control_types(["enable", "bogus"])

# %%
#|export
def test_generate_control_ports():
    """Test generate_control_ports creates correct port configs."""
    ports = generate_control_ports(["enable", "disable"])
    assert "__control_enable__" in ports
    assert "__control_disable__" in ports
    assert len(ports) == 2
    assert isinstance(ports["__control_enable__"], PortConfig)

# %%
#|export
def test_generate_control_salvo_conditions():
    """Test generate_control_salvo_conditions creates correct salvo configs."""
    salvos = generate_control_salvo_conditions(["enable"])
    assert "__control_enable__" in salvos
    salvo = salvos["__control_enable__"]
    assert "__control_enable__" in salvo.ports
    # Input salvo conditions must have finite(1) max_salvos
    assert salvo.max_salvos.type == "finite"
    assert salvo.max_salvos.max == 1

# %% [markdown]
# ## Config Tests

# %%
#|export
def test_control_ports_auto_generated_on_resolve():
    """Test that control ports are auto-generated during NodeConfig.resolve()."""
    node = NodeConfig(
        name="A",
        in_ports={"in": PortConfig()},
        out_ports={"out": PortConfig()},
        execution_config=NodeExecutionConfig(
            pools=["main"],
            controls=["enable", "disable"],
        ),
    )
    resolved = node.resolve()
    assert "__control_enable__" in resolved.in_ports
    assert "__control_disable__" in resolved.in_ports
    assert "in" in resolved.in_ports  # original port preserved
    # Control salvo conditions should also be generated
    assert "__control_enable__" in resolved.in_salvo_conditions
    assert "__control_disable__" in resolved.in_salvo_conditions

# %%
#|export
def test_control_ports_inherited_from_net_config():
    """Test that control ports are inherited from NetConfig.default_controls."""
    net_config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        # controls=None means inherit from net default
                    ),
                ),
            ],
            edges=[],
        ),
        default_controls=["enable", "disable"],
    )
    resolved = net_config.resolve()
    node_a = resolved.graph.nodes[0]
    assert "__control_enable__" in node_a.in_ports
    assert "__control_disable__" in node_a.in_ports

# %%
#|export
def test_control_ports_opt_out_with_empty_list():
    """Test that controls=[] opts out even when net has default_controls."""
    net_config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        controls=[],  # explicit opt-out
                    ),
                ),
            ],
            edges=[],
        ),
        default_controls=["enable", "disable"],
    )
    resolved = net_config.resolve()
    node_a = resolved.graph.nodes[0]
    assert "__control_enable__" not in node_a.in_ports
    assert "__control_disable__" not in node_a.in_ports

# %%
#|export
def test_control_ports_excluded_from_default_in_salvo():
    """Test that default in_salvo_conditions ignore control ports."""
    node = NodeConfig(
        name="A",
        in_ports={"in": PortConfig()},
        execution_config=NodeExecutionConfig(
            pools=["main"],
            controls=["enable"],
        ),
    )
    resolved = node.resolve()
    # The "default" salvo should only reference "in", not the control port
    default_salvo = resolved.in_salvo_conditions["default"]
    assert "in" in default_salvo.ports
    assert "__control_enable__" not in default_salvo.ports

# %% [markdown]
# ## Integration Tests

# %%
#|export
def _make_counting_exec_func(counter: dict):
    """Create an exec function that counts calls and consumes input packets."""
    def exec_func(ctx, packets):
        for pkt_ids in packets.values():
            for pid in pkt_ids:
                ctx.consume_packet(pid)
        counter["count"] = counter.get("count", 0) + 1
    return exec_func

# %%
#|export
@pytest.mark.asyncio
async def test_enable_control():
    """Test 'enable' control re-enables a disabled node."""
    counter = {}
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Worker",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_make_counting_exec_func(counter),
                        enabled=False,  # Start disabled
                        controls=["enable"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    async with Net(config) as net:
        # Inject data — won't be processed because disabled
        net.inject_data("Worker", "in", [1])
        await net.run_until_blocked()
        assert counter.get("count", 0) == 0

        # Send enable control
        net.send_control("Worker", "enable")
        await net.run_until_blocked()

        # Now Worker should have processed the data
        assert counter.get("count", 0) == 1

# %%
#|export
@pytest.mark.asyncio
async def test_disable_control():
    """Test 'disable' control disables a node."""
    counter = {"count": 0}
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Worker",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_make_counting_exec_func(counter),
                        controls=["disable"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    async with Net(config) as net:
        # First epoch should work
        net.inject_data("Worker", "in", [1])
        await net.run_until_blocked()
        assert counter["count"] == 1

        # Send disable control
        net.send_control("Worker", "disable")
        await net.run_until_blocked()

        # Now inject data — should NOT be processed
        net.inject_data("Worker", "in", [2])
        await net.run_until_blocked()
        assert counter["count"] == 1  # Still 1

# %%
#|export
@pytest.mark.asyncio
async def test_start_epoch_control_no_input_ports():
    """Test 'start_epoch' control on a source node (no regular input ports).

    The node has only control ports — no default term=True salvo is generated,
    so epoch creation is driven purely by the start_epoch control.
    """
    counter = {"count": 0}
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Source",
                    out_ports={"out": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_make_counting_exec_func(counter),
                        controls=["start_epoch"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    async with Net(config) as net:
        # Send start_epoch control
        net.send_control("Source", "start_epoch")
        await net.run_until_blocked()

        # Source should have executed once (from the control-triggered epoch)
        assert counter["count"] == 1

# %%
#|export
@pytest.mark.asyncio
async def test_start_epoch_control_with_input_ports_error():
    """Test 'start_epoch' control on a node with input ports but no startable epoch raises error."""
    from netrun.net._net._context import EpochError

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Worker",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_make_counting_exec_func({}),
                        controls=["start_epoch"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    async with Net(config) as net:
        net.send_control("Worker", "start_epoch")
        with pytest.raises(EpochError, match="has input ports but no startable epoch"):
            await net.run_until_blocked()

# %%
#|export
@pytest.mark.asyncio
async def test_cancel_all_epochs_control():
    """Test 'cancel_all_epochs' control cancels running epochs.

    Since we use a main pool (single async worker), we can't have a truly
    concurrent long-running epoch. Instead, we verify the control mechanism
    by checking it doesn't error when there are no running epochs, and that
    the control epoch itself completes successfully.
    """
    counter = {"count": 0}
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Worker",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_make_counting_exec_func(counter),
                        controls=["cancel_all_epochs"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    async with Net(config) as net:
        # Run a normal epoch first
        net.inject_data("Worker", "in", [1])
        await net.run_until_blocked()
        assert counter["count"] == 1

        # Send cancel_all_epochs — no running epochs, should complete silently
        net.send_control("Worker", "cancel_all_epochs")
        await net.run_until_blocked()

        # Verify no running epochs
        assert len(net._running_epochs) == 0

# %%
#|export
@pytest.mark.asyncio
async def test_init_node_control():
    """Test 'init_node' control initializes a deferred node."""
    started = {"value": False}

    def start_func(net_obj):
        started["value"] = True

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Worker",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_make_counting_exec_func({}),
                        init_node_func=start_func,
                        defer_init=True,
                        controls=["init_node"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    async with Net(config) as net:
        # Node should NOT be started yet (deferred)
        assert "Worker" not in net._initialized_nodes

        # Send init_node control
        net.send_control("Worker", "init_node")
        await net.run_until_blocked()

        # Node should now be started
        assert "Worker" in net._initialized_nodes
        assert started["value"] is True

# %%
#|export
@pytest.mark.asyncio
async def test_init_node_already_initialized_error():
    """Test 'init_node' control errors if node is already initialized."""
    from netrun.net._net._context import EpochError

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Worker",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_make_counting_exec_func({}),
                        controls=["init_node"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    async with Net(config) as net:
        # Node is already initialized during Net.init()
        assert "Worker" in net._initialized_nodes

        # Sending start_node should error
        net.send_control("Worker", "init_node")
        with pytest.raises(EpochError, match="already initialized"):
            await net.run_until_blocked()

# %%
#|export
@pytest.mark.asyncio
async def test_close_node_control():
    """Test 'close_node' control closes an initialized node."""
    stopped = {"value": False}

    def stop_func(net_obj):
        stopped["value"] = True

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Worker",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_make_counting_exec_func({}),
                        close_node_func=stop_func,
                        controls=["close_node"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    async with Net(config) as net:
        # Node should be started
        assert "Worker" in net._initialized_nodes

        # Send close_node control
        net.send_control("Worker", "close_node")
        await net.run_until_blocked()

        # Node should now be stopped
        assert "Worker" not in net._initialized_nodes
        assert stopped["value"] is True

# %%
#|export
@pytest.mark.asyncio
async def test_close_node_not_initialized_error():
    """Test 'close_node' control errors if node is not initialized."""
    from netrun.net._net._context import EpochError

    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Worker",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_make_counting_exec_func({}),
                        defer_init=True,
                        controls=["close_node"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    async with Net(config) as net:
        # Node NOT started (deferred)
        assert "Worker" not in net._initialized_nodes

        net.send_control("Worker", "close_node")
        with pytest.raises(EpochError, match="not initialized"):
            await net.run_until_blocked()

# %%
#|export
@pytest.mark.asyncio
async def test_set_epoch_count_control():
    """Test 'set_epoch_count' control sets the epoch count."""
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Worker",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_make_counting_exec_func({}),
                        controls=["set_epoch_count"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    async with Net(config) as net:
        # Set epoch count to 42
        net.send_control("Worker", "set_epoch_count", 42)
        await net.run_until_blocked()

        assert net._node_epoch_counts.get("Worker") == 42

# %%
#|export
@pytest.mark.asyncio
async def test_reset_epoch_count_control():
    """Test 'reset_epoch_count' control resets the epoch count to 0."""
    counter = {"count": 0}
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Worker",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_make_counting_exec_func(counter),
                        controls=["reset_epoch_count"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    async with Net(config) as net:
        # Run an epoch to increment the count
        net.inject_data("Worker", "in", [1])
        await net.run_until_blocked()
        assert counter["count"] == 1
        assert net._node_epoch_counts.get("Worker", 0) == 1

        # Reset
        net.send_control("Worker", "reset_epoch_count")
        await net.run_until_blocked()

        assert net._node_epoch_counts.get("Worker") == 0

# %%
#|export
@pytest.mark.asyncio
async def test_send_control_api():
    """Test net.send_control() convenience method."""
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Worker",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_make_counting_exec_func({}),
                        controls=["enable", "disable"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    async with Net(config) as net:
        # send_control returns a packet ID
        pid = net.send_control("Worker", "enable")
        assert isinstance(pid, str)
        assert len(pid) > 0

        # Invalid control port
        with pytest.raises(ValueError, match="does not have control port"):
            net.send_control("Worker", "start_epoch")

        # Invalid node
        with pytest.raises(ValueError, match="not found"):
            net.send_control("NonExistent", "enable")

# %%
#|export
@pytest.mark.asyncio
async def test_control_on_disabled_node():
    """Test that 'enable' control works even when node is disabled."""
    counter = {"count": 0}
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Worker",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_make_counting_exec_func(counter),
                        enabled=False,  # Start disabled
                        controls=["enable"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    async with Net(config) as net:
        assert not net.is_node_enabled("Worker")

        # Inject data — won't be processed because disabled
        net.inject_data("Worker", "in", [1])
        await net.run_until_blocked()
        assert counter["count"] == 0

        # Send enable control — should work even though node is disabled
        net.send_control("Worker", "enable")
        await net.run_until_blocked()

        # Node should now be enabled and the queued data processed
        assert net.is_node_enabled("Worker")
        assert counter["count"] == 1

# %% [markdown]
# ## ControlType Registry Tests

# %%
#|export
def test_control_type_registry_keys_match_valid_control_types():
    """CONTROL_TYPES keys must match VALID_CONTROL_TYPES exactly."""
    assert set(CONTROL_TYPES.keys()) == VALID_CONTROL_TYPES

# %%
#|export
def test_control_type_dataclass():
    """Test ControlType fields."""
    ct = CONTROL_TYPES["cancel_epoch"]
    assert isinstance(ct, ControlType)
    assert ct.name == "cancel_epoch"
    assert ct.value_type is str
    assert ct.description != ""

    ct_signal = CONTROL_TYPES["enable"]
    assert ct_signal.value_type is None

# %%
#|export
def test_control_type_value_types():
    """Test that value_type is set correctly for controls that require values."""
    assert CONTROL_TYPES["cancel_epoch"].value_type is str
    assert CONTROL_TYPES["set_epoch_count"].value_type is int
    # All others should be None
    for name, ct in CONTROL_TYPES.items():
        if name not in ("cancel_epoch", "set_epoch_count"):
            assert ct.value_type is None, f"{name} should be a signal (value_type=None)"

# %%
#|export
@pytest.mark.asyncio
async def test_send_control_rejects_value_for_signal_control():
    """send_control should raise if a value is passed to a signal-style control."""
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Worker",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_make_counting_exec_func({}),
                        controls=["enable"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    async with Net(config) as net:
        with pytest.raises(ValueError, match="does not accept a value"):
            net.send_control("Worker", "enable", value="unexpected")

# %%
#|export
@pytest.mark.asyncio
async def test_send_control_requires_value_for_valued_control():
    """send_control should raise if no value is passed to a valued control."""
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Worker",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_make_counting_exec_func({}),
                        controls=["cancel_epoch"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    async with Net(config) as net:
        with pytest.raises(ValueError, match="requires a str value"):
            net.send_control("Worker", "cancel_epoch")

# %%
#|export
@pytest.mark.asyncio
async def test_send_control_rejects_wrong_value_type():
    """send_control should raise if value has wrong type."""
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Worker",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_make_counting_exec_func({}),
                        controls=["set_epoch_count"],
                    ),
                ),
            ],
            edges=[],
        ),
    )

    async with Net(config) as net:
        with pytest.raises(ValueError, match="requires a int value, got str"):
            net.send_control("Worker", "set_epoch_count", value="not_an_int")

# %%
#|export
@pytest.mark.asyncio
async def test_send_control_rejects_unknown_control_type():
    """send_control should raise for unknown control types."""
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="Worker",
                    in_ports={"in": PortConfig()},
                    execution_config=NodeExecutionConfig(
                        pools=["main"],
                        exec_node_func=_make_counting_exec_func({}),
                    ),
                ),
            ],
            edges=[],
        ),
    )

    async with Net(config) as net:
        with pytest.raises(ValueError, match="Unknown control type"):
            net.send_control("Worker", "bogus_control")
