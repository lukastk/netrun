# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp net.test_structured_logging

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %% [markdown]
# # Structured Logging Tests

# %%
#|export
import asyncio
import json
import tempfile
from pathlib import Path
from datetime import datetime

import pytest

from netrun.net.config import (
    NetConfig, NodeConfig, NodeExecutionConfig, PortConfig, GraphConfig,
    EdgeConfig, SalvoConditionConfig, SalvoConditionTermTrueConfig,
    MaxSalvosFiniteConfig, PacketCountNConfig, OutputQueueConfig,
)
from netrun.net._net._context import (
    _EpochState, EpochLog, NodeLogEntry, SimActionLog, SimEventLog,
    NodeExecutionContext, _format_log_entry, _format_epoch_log,
)
from netrun.net._net._net import Net


def _simple_config(
    exec_func=None,
    *,
    retain_epoch_logs: bool = False,
    retain_sim_action_logs: bool = False,
    epoch_log_echo_stdout: bool = False,
    factory: str | None = None,
    factory_args: dict | None = None,
) -> NetConfig:
    """Create a minimal A -> B net config for testing."""
    node_a = NodeConfig(
        name="A",
        in_ports={"in": PortConfig()},
        out_ports={"out": PortConfig()},
        in_salvo_conditions={
            "trigger": SalvoConditionConfig(
                max_salvos=MaxSalvosFiniteConfig(max=1),
                ports={"in": PacketCountNConfig(count=1)},
                term=SalvoConditionTermTrueConfig(),
            )
        },
        out_salvo_conditions={
            "send": SalvoConditionConfig(
                max_salvos=MaxSalvosFiniteConfig(max=1),
                ports={"out": PacketCountNConfig(count=1)},
                term=SalvoConditionTermTrueConfig(),
            )
        },
        execution_config=NodeExecutionConfig(
            exec_node_func=exec_func,
            pools=["main"],
        ) if exec_func else None,
        factory=factory,
        factory_args=factory_args or {},
    )

    return NetConfig(
        graph=GraphConfig(
            nodes=[node_a],
            edges=[],
        ),
        retain_epoch_logs=retain_epoch_logs,
        retain_sim_action_logs=retain_sim_action_logs,
        epoch_log_echo_stdout=epoch_log_echo_stdout,
        print_echo_stdout=False,
    )

# %% [markdown]
# ## ctx.log() tests

# %%
#|export
@pytest.mark.asyncio
async def test_ctx_log_basic():
    """ctx.log() should add entries to structured_log_buffer."""
    def exec_func(ctx, packets):
        for port_name, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
        ctx.log("Fetched data", user_id="u123", subscription="premium")
        ctx.log("Done", tokens_used=1523)
        pid = ctx.create_packet("result")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    config = _simple_config(exec_func, retain_epoch_logs=True)
    async with Net(config) as net:
        net.inject_data("A", "in", ["hello"])
        await net.run_until_blocked()

    # Check retained epoch logs
    assert len(net.epoch_logs) == 1
    epoch_log = list(net.epoch_logs.values())[0]
    assert epoch_log.node_name == "A"
    assert epoch_log.outcome == "success"
    assert len(epoch_log.node_log_entries) == 2
    assert epoch_log.node_log_entries[0].message == "Fetched data"
    assert epoch_log.node_log_entries[0].fields == {"user_id": "u123", "subscription": "premium"}
    assert epoch_log.node_log_entries[1].message == "Done"
    assert epoch_log.node_log_entries[1].fields == {"tokens_used": 1523}

    # User fields should be merged
    assert epoch_log.user_fields == {"user_id": "u123", "subscription": "premium", "tokens_used": 1523}


@pytest.mark.asyncio
async def test_ctx_log_backward_compat():
    """ctx.log() should also add to print buffer for backward compat."""
    def exec_func(ctx, packets):
        for port_name, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
        ctx.log("Hello world", key="val")
        pid = ctx.create_packet("ok")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    config = _simple_config(exec_func)
    async with Net(config) as net:
        net.inject_data("A", "in", ["data"])
        await net.run_until_blocked()

    # Check that print buffer captured the log entry
    epoch_id = list(net.epochs.keys())[0]
    logs = net.get_epoch_log(epoch_id)
    assert len(logs) == 1
    ts, msg = logs[0]
    assert "Hello world" in msg
    assert "key=val" in msg

# %% [markdown]
# ## run_step() return value tests

# %%
#|export
@pytest.mark.asyncio
async def test_run_step_returns_sim_actions_and_epoch_logs():
    """run_step() should return (bool, sim_actions, epoch_logs)."""
    def exec_func(ctx, packets):
        for port_name, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
        pid = ctx.create_packet("result")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    config = _simple_config(exec_func)
    async with Net(config) as net:
        net.inject_data("A", "in", ["hello"])
        made_progress, sim_actions, epoch_logs = await net.run_step()

    assert made_progress is True
    assert isinstance(sim_actions, list)
    assert isinstance(epoch_logs, list)
    assert len(epoch_logs) == 1
    assert epoch_logs[0].node_name == "A"
    assert epoch_logs[0].outcome == "success"


@pytest.mark.asyncio
async def test_run_until_blocked_returns_sim_actions_and_epoch_logs():
    """run_until_blocked() should accumulate sim_actions and epoch_logs."""
    def exec_func(ctx, packets):
        for port_name, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
        pid = ctx.create_packet("result")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    config = _simple_config(exec_func)
    async with Net(config) as net:
        net.inject_data("A", "in", ["hello"])
        made_progress, sim_actions, epoch_logs = await net.run_until_blocked()

    assert made_progress is True
    assert len(epoch_logs) == 1

# %% [markdown]
# ## EpochLog tests

# %%
#|export
@pytest.mark.asyncio
async def test_epoch_log_success():
    """EpochLog should have correct fields for successful execution."""
    def exec_func(ctx, packets):
        for port_name, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
        ctx.print("hello")
        pid = ctx.create_packet("result")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    config = _simple_config(exec_func, retain_epoch_logs=True)
    async with Net(config) as net:
        net.inject_data("A", "in", ["input_val"])
        await net.run_until_blocked()

    epoch_log = list(net.epoch_logs.values())[0]
    assert epoch_log.outcome == "success"
    assert epoch_log.node_name == "A"
    assert epoch_log.started_at is not None
    assert epoch_log.ended_at is not None
    assert epoch_log.duration_ms is not None
    assert epoch_log.duration_ms >= 0
    assert epoch_log.in_salvo_ports == ["in"]
    assert epoch_log.in_salvo_packet_count == 1
    assert len(epoch_log.logs) == 1  # from ctx.print()


@pytest.mark.asyncio
async def test_epoch_log_cancelled():
    """EpochLog should reflect cancellation outcome."""
    def exec_func(ctx, packets):
        for port_name, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
        ctx.cancel_epoch()

    config = _simple_config(exec_func, retain_epoch_logs=True)
    async with Net(config) as net:
        net.inject_data("A", "in", ["data"])
        await net.run_until_blocked()

    epoch_log = list(net.epoch_logs.values())[0]
    assert epoch_log.outcome == "cancelled"


@pytest.mark.asyncio
async def test_epoch_log_error():
    """EpochLog should capture error information."""
    def exec_func(ctx, packets):
        for port_name, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
        raise ValueError("test error")

    config = _simple_config(exec_func, retain_epoch_logs=True)
    config = config.model_copy(update={"propagate_exceptions": False})
    async with Net(config) as net:
        net.inject_data("A", "in", ["data"])
        await net.run_until_blocked()

    epoch_log = list(net.epoch_logs.values())[0]
    assert epoch_log.outcome == "error"
    assert epoch_log.error == "test error"
    assert epoch_log.error_type == "ValueError"

# %% [markdown]
# ## on_epoch_end callback receives EpochLog

# %%
#|export
@pytest.mark.asyncio
async def test_on_epoch_end_receives_epoch_log():
    """on_epoch_end callback should receive EpochLog."""
    def exec_func(ctx, packets):
        for port_name, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
        pid = ctx.create_packet("result")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    received = []

    config = _simple_config(exec_func)
    async with Net(config) as net:
        net.on_epoch_end(lambda name, eid, epoch_log: received.append(epoch_log))
        net.inject_data("A", "in", ["data"])
        await net.run_until_blocked()

    assert len(received) == 1
    assert isinstance(received[0], EpochLog)
    assert received[0].node_name == "A"
    assert received[0].outcome == "success"

# %% [markdown]
# ## on_sim_actions callback tests

# %%
#|export
@pytest.mark.asyncio
async def test_on_sim_actions_callback():
    """on_sim_actions callback should fire with SimActionLog list."""
    def exec_func(ctx, packets):
        for port_name, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
        pid = ctx.create_packet("result")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    received = []

    config = _simple_config(exec_func)
    async with Net(config) as net:
        net.on_sim_actions(lambda actions: received.append(actions))
        net.inject_data("A", "in", ["data"])
        await net.run_step()

    assert len(received) == 1
    assert isinstance(received[0], list)
    assert all(isinstance(a, SimActionLog) for a in received[0])

# %% [markdown]
# ## Retention tests

# %%
#|export
@pytest.mark.asyncio
async def test_retain_epoch_logs():
    """retain_epoch_logs=True should accumulate EpochLogs."""
    def exec_func(ctx, packets):
        for port_name, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
        pid = ctx.create_packet("result")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    config = _simple_config(exec_func, retain_epoch_logs=True)
    async with Net(config) as net:
        net.inject_data("A", "in", ["v1"])
        await net.run_until_blocked()
        net.inject_data("A", "in", ["v2"])
        await net.run_until_blocked()

    assert len(net.epoch_logs) == 2


@pytest.mark.asyncio
async def test_retain_epoch_logs_false():
    """retain_epoch_logs=False should not accumulate EpochLogs."""
    def exec_func(ctx, packets):
        for port_name, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
        pid = ctx.create_packet("result")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    config = _simple_config(exec_func, retain_epoch_logs=False)
    async with Net(config) as net:
        net.inject_data("A", "in", ["v1"])
        await net.run_until_blocked()

    assert len(net.epoch_logs) == 0


@pytest.mark.asyncio
async def test_retain_sim_action_logs():
    """retain_sim_action_logs=True should accumulate SimActionLogs."""
    def exec_func(ctx, packets):
        for port_name, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
        pid = ctx.create_packet("result")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    config = _simple_config(exec_func, retain_sim_action_logs=True)
    async with Net(config) as net:
        net.inject_data("A", "in", ["v1"])
        await net.run_until_blocked()

    assert len(net.sim_action_log) > 0
    assert all(isinstance(a, SimActionLog) for a in net.sim_action_log)

# %% [markdown]
# ## EpochLog serialization tests

# %%
#|export
@pytest.mark.asyncio
async def test_epoch_log_to_dict_round_trip():
    """EpochLog.to_dict() and from_dict() should round-trip."""
    def exec_func(ctx, packets):
        for port_name, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
        ctx.log("hello", key="val")
        ctx.print("printed msg")
        pid = ctx.create_packet("result")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    config = _simple_config(exec_func, retain_epoch_logs=True)
    async with Net(config) as net:
        net.inject_data("A", "in", ["data"])
        await net.run_until_blocked()

    original = list(net.epoch_logs.values())[0]
    d = original.to_dict()
    restored = EpochLog.from_dict(d)

    assert restored.epoch_id == original.epoch_id
    assert restored.node_name == original.node_name
    assert restored.outcome == original.outcome
    assert restored.duration_ms == original.duration_ms
    assert len(restored.node_log_entries) == len(original.node_log_entries)
    assert len(restored.logs) == len(original.logs)
    assert restored.user_fields == original.user_fields

# %% [markdown]
# ## JSONL backend tests

# %%
#|export
@pytest.mark.asyncio
async def test_jsonl_epoch_logger_round_trip():
    """JsonlEpochLogger should write and load EpochLogs."""
    from netrun.logging._backends import JsonlEpochLogger

    def exec_func(ctx, packets):
        for port_name, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
        ctx.log("test", val=42)
        pid = ctx.create_packet("result")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        path = f.name

    try:
        logger = JsonlEpochLogger(path)
        config = _simple_config(exec_func)
        async with Net(config) as net:
            net.on_epoch_end(logger)
            net.inject_data("A", "in", ["data"])
            await net.run_until_blocked()
        logger.close()

        loaded = JsonlEpochLogger.load(path)
        assert len(loaded) == 1
        assert loaded[0].node_name == "A"
        assert loaded[0].outcome == "success"
        assert loaded[0].user_fields.get("val") == 42
    finally:
        Path(path).unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_jsonl_sim_action_logger_round_trip():
    """JsonlSimActionLogger should write and load SimActionLogs."""
    from netrun.logging._backends import JsonlSimActionLogger

    def exec_func(ctx, packets):
        for port_name, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
        pid = ctx.create_packet("result")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        path = f.name

    try:
        logger = JsonlSimActionLogger(path)
        config = _simple_config(exec_func)
        async with Net(config) as net:
            net.on_sim_actions(logger)
            net.inject_data("A", "in", ["data"])
            await net.run_until_blocked()
        logger.close()

        loaded = JsonlSimActionLogger.load(path)
        assert len(loaded) > 0
        assert all(isinstance(a, SimActionLog) for a in loaded)
    finally:
        Path(path).unlink(missing_ok=True)

# %% [markdown]
# ## SQLite backend tests

# %%
#|export
@pytest.mark.asyncio
async def test_sqlite_logger_round_trip():
    """SqliteLogger should write and load both log types."""
    from netrun.logging._backends import SqliteLogger

    def exec_func(ctx, packets):
        for port_name, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
        ctx.log("processed", count=5)
        pid = ctx.create_packet("result")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name

    try:
        logger = SqliteLogger(path)
        config = _simple_config(exec_func)
        async with Net(config) as net:
            net.on_epoch_end(logger.epoch_handler)
            net.on_sim_actions(logger.sim_action_handler)
            net.inject_data("A", "in", ["data"])
            await net.run_until_blocked()

        epoch_logs = logger.load_epoch_logs()
        assert len(epoch_logs) == 1
        assert epoch_logs[0].node_name == "A"
        assert epoch_logs[0].user_fields.get("count") == 5

        sim_actions = logger.load_sim_action_logs()
        assert len(sim_actions) > 0

        logger.close()
    finally:
        Path(path).unlink(missing_ok=True)

# %% [markdown]
# ## Function factory "log" parameter test

# %%
#|export
@pytest.mark.asyncio
async def test_function_factory_log_param():
    """Function factory should inject ctx.log as 'log' special param."""
    def my_node(data: str, log) -> str:
        log("Processing", input_len=len(data))
        return data.upper()

    config = NetConfig(
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="my_node",
                    factory="netrun.node_factories.from_function",
                    factory_args={"func": my_node},
                    execution_config=NodeExecutionConfig(pools=["main"]),
                ),
            ],
            edges=[],
        ),
        retain_epoch_logs=True,
        print_echo_stdout=False,
    )

    async with Net(config) as net:
        net.inject_data("my_node", "data", ["hello"])
        await net.run_until_blocked()

    epoch_log = list(net.epoch_logs.values())[0]
    assert epoch_log.outcome == "success"
    assert len(epoch_log.node_log_entries) == 1
    assert epoch_log.node_log_entries[0].message == "Processing"
    assert epoch_log.node_log_entries[0].fields == {"input_len": 5}

# %% [markdown]
# ## Format helper tests

# %%
#|export
def test_format_log_entry():
    """_format_log_entry should produce readable output."""
    entry = NodeLogEntry(
        timestamp=datetime.now(),
        fields={"user_id": "u123", "count": 42},
        message="Fetched data",
    )
    result = _format_log_entry("MyNode", entry)
    assert "Fetched data" in result
    assert "user_id=u123" in result
    assert "count=42" in result


def test_format_epoch_log():
    """_format_epoch_log should produce readable output."""
    epoch_log = EpochLog(
        epoch_id="test",
        node_name="MyNode",
        timestamp=datetime.now(),
        created_at=datetime.now(),
        started_at=datetime.now(),
        ended_at=datetime.now(),
        duration_ms=1247.5,
        queue_time_ms=0.5,
        outcome="success",
        pool_id="main",
        worker_id=0,
        user_fields={"tokens_used": 1523},
    )
    result = _format_epoch_log(epoch_log)
    assert "outcome=success" in result
    assert "duration=1248ms" in result
    assert "pool=main/0" in result
    assert "tokens_used=1523" in result

# %% [markdown]
# ## ctx.log() stdout echo

# %%
#|export
@pytest.mark.asyncio
async def test_ctx_log_stdout_echo(capsys):
    """ctx.log() should echo to stdout when print_echo_stdout=True."""
    def exec_func(ctx, packets):
        for pids in packets.values():
            for pid in pids:
                ctx.consume_packet(pid)
        ctx.log("Fetched data", user_id="u123")
        pid = ctx.create_packet("ok")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    config = _simple_config(exec_func)
    config = config.model_copy(update={"print_echo_stdout": True})
    async with Net(config) as net:
        net.inject_data("A", "in", ["data"])
        await net.run_until_blocked()

    captured = capsys.readouterr()
    assert "[A]" in captured.out
    assert "Fetched data" in captured.out
    assert "user_id=u123" in captured.out

# %% [markdown]
# ## epoch_log_echo_stdout

# %%
#|export
@pytest.mark.asyncio
async def test_epoch_log_echo_stdout(capsys):
    """epoch_log_echo_stdout=True should print epoch summary to stdout."""
    def exec_func(ctx, packets):
        for pids in packets.values():
            for pid in pids:
                ctx.consume_packet(pid)
        pid = ctx.create_packet("ok")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    config = _simple_config(exec_func, epoch_log_echo_stdout=True)
    async with Net(config) as net:
        net.inject_data("A", "in", ["data"])
        await net.run_until_blocked()

    captured = capsys.readouterr()
    assert "[A]" in captured.out
    assert "outcome=success" in captured.out

# %% [markdown]
# ## Multi-node flow with SimActionLog epoch attribution

# %%
#|export
@pytest.mark.asyncio
async def test_multi_node_sim_action_epoch_attribution():
    """SimActionLogs should be attributed to the correct epoch across nodes."""
    def node_a_func(ctx, packets):
        for pids in packets.values():
            for pid in pids:
                ctx.consume_packet(pid)
        pid = ctx.create_packet("from_a")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    def node_b_func(ctx, packets):
        for pids in packets.values():
            for pid in pids:
                ctx.consume_packet(pid)

    config = NetConfig(
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_ports={"in": PortConfig()},
                    out_ports={"out": PortConfig()},
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={"in": PacketCountNConfig(count=1)},
                            term=SalvoConditionTermTrueConfig(),
                        )
                    },
                    out_salvo_conditions={
                        "send": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={"out": PacketCountNConfig(count=1)},
                            term=SalvoConditionTermTrueConfig(),
                        )
                    },
                    execution_config=NodeExecutionConfig(
                        exec_node_func=node_a_func, pools=["main"],
                    ),
                ),
                NodeConfig(
                    name="B",
                    in_ports={"in": PortConfig()},
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={"in": PacketCountNConfig(count=1)},
                            term=SalvoConditionTermTrueConfig(),
                        )
                    },
                    execution_config=NodeExecutionConfig(
                        exec_node_func=node_b_func, pools=["main"],
                    ),
                ),
            ],
            edges=[
                EdgeConfig(source_node="A", source_port="out", target_node="B", target_port="in"),
            ],
        ),
        retain_epoch_logs=True,
        print_echo_stdout=False,
    )

    async with Net(config) as net:
        net.inject_data("A", "in", ["hello"])
        await net.run_until_blocked()

    assert len(net.epoch_logs) == 2
    epoch_logs_by_node = {el.node_name: el for el in net.epoch_logs.values()}
    assert "A" in epoch_logs_by_node
    assert "B" in epoch_logs_by_node

    # Each epoch's sim_actions should be attributed to the correct epoch_id
    for el in epoch_logs_by_node.values():
        for sa in el.sim_actions:
            assert sa.epoch_id == el.epoch_id

# %% [markdown]
# ## on_sim_actions deregistration

# %%
#|export
@pytest.mark.asyncio
async def test_on_sim_actions_deregistration():
    """Calling remove() from on_sim_actions should stop firing the callback."""
    def exec_func(ctx, packets):
        for pids in packets.values():
            for pid in pids:
                ctx.consume_packet(pid)
        pid = ctx.create_packet("ok")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    call_count = 0

    def on_actions(actions):
        nonlocal call_count
        call_count += 1

    config = _simple_config(exec_func)
    async with Net(config) as net:
        remove = net.on_sim_actions(on_actions)
        net.inject_data("A", "in", ["v1"])
        await net.run_until_blocked()
        assert call_count >= 1

        # Deregister
        remove()
        old_count = call_count

        net.inject_data("A", "in", ["v2"])
        await net.run_until_blocked()
        assert call_count == old_count  # Should not have incremented

# %% [markdown]
# ## Cache hit outcome in EpochLog

# %%
#|export
@pytest.mark.asyncio
async def test_epoch_log_cache_hit():
    """EpochLog should report outcome='cache_hit' on cache replay."""
    from netrun.storage.config import CacheConfig, NodeCacheConfig, StorageConfig, NodeStorageConfig

    call_count = 0

    def exec_func(ctx, packets):
        nonlocal call_count
        call_count += 1
        for pids in packets.values():
            for pid in pids:
                ctx.consume_packet(pid)
        pid = ctx.create_packet(100)
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    cache = CacheConfig(enabled=True, include_all_nodes=True)
    node_storage = NodeStorageConfig(cache=NodeCacheConfig())

    config = NetConfig(
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_ports={"in": PortConfig()},
                    out_ports={"out": PortConfig()},
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={"in": PacketCountNConfig(count=1)},
                            term=SalvoConditionTermTrueConfig(),
                        )
                    },
                    out_salvo_conditions={
                        "send": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={"out": PacketCountNConfig(count=1)},
                            term=SalvoConditionTermTrueConfig(),
                        )
                    },
                    execution_config=NodeExecutionConfig(
                        exec_node_func=exec_func,
                        pools=["main"],
                        storage=node_storage,
                    ),
                ),
            ],
            edges=[],
            output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
        ),
        storage=StorageConfig(cache=cache),
        retain_epoch_logs=True,
        print_echo_stdout=False,
    )

    async with Net(config) as net:
        # First run: actual execution
        net.inject_data("A", "in", [42])
        await net.run_until_blocked()

        # Second run: cache hit
        net.inject_data("A", "in", [42])
        await net.run_until_blocked()

    assert call_count == 1  # Only executed once

    epoch_logs = list(net.epoch_logs.values())
    assert len(epoch_logs) == 2
    outcomes = [el.outcome for el in epoch_logs]
    assert "success" in outcomes
    assert "cache_hit" in outcomes

    cache_hit_log = [el for el in epoch_logs if el.outcome == "cache_hit"][0]
    assert cache_hit_log.was_cache_hit is True

# %% [markdown]
# ## File storage hit outcome in EpochLog

# %%
#|export
@pytest.mark.asyncio
async def test_epoch_log_file_storage_hit():
    """EpochLog should report outcome='file_storage_hit' on file storage replay."""
    import tempfile
    from netrun.storage.config import StorageConfig, NodeStorageConfig, NodeFileStorageConfig, LocalBackendConfig

    call_count = 0

    def exec_func(ctx, packets):
        nonlocal call_count
        call_count += 1
        for pids in packets.values():
            for pid in pids:
                value = ctx.consume_packet(pid)
                pid_out = ctx.create_packet(value * 2)
                ctx.load_output_port("out", pid_out)
        ctx.send_output_salvo("send")

    with tempfile.TemporaryDirectory() as tmpdir:
        fs_config = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="pickle",
        )
        storage = StorageConfig(file_storage_metadata_path=tmpdir + "/meta")
        node_storage = NodeStorageConfig(file_storage=fs_config)

        node_cfg = NodeConfig(
            name="A",
            in_ports={"in": PortConfig()},
            out_ports={"out": PortConfig()},
            in_salvo_conditions={
                "trigger": SalvoConditionConfig(
                    max_salvos=MaxSalvosFiniteConfig(max=1),
                    ports={"in": PacketCountNConfig(count=1)},
                    term=SalvoConditionTermTrueConfig(),
                )
            },
            out_salvo_conditions={
                "send": SalvoConditionConfig(
                    max_salvos=MaxSalvosFiniteConfig(max=1),
                    ports={"out": PacketCountNConfig(count=1)},
                    term=SalvoConditionTermTrueConfig(),
                )
            },
            execution_config=NodeExecutionConfig(
                exec_node_func=exec_func,
                pools=["main"],
                storage=node_storage,
            ),
        )

        config1 = NetConfig(
            graph=GraphConfig(
                nodes=[node_cfg],
                edges=[],
                output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
            ),
            storage=storage,
            retain_epoch_logs=True,
            print_echo_stdout=False,
        )

        # First run: executes
        async with Net(config1) as net1:
            net1.inject_data("A", "in", [5])
            await net1.run_until_blocked()
        assert call_count == 1

        # Second run: file storage hit
        call_count = 0
        node_cfg2 = node_cfg.model_copy(deep=True)
        config2 = NetConfig(
            graph=GraphConfig(
                nodes=[node_cfg2],
                edges=[],
                output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
            ),
            storage=storage,
            retain_epoch_logs=True,
            print_echo_stdout=False,
        )

        async with Net(config2) as net2:
            net2.inject_data("A", "in", [5])
            await net2.run_until_blocked()

        assert call_count == 0  # Not called — replayed
        epoch_log = list(net2.epoch_logs.values())[0]
        assert epoch_log.outcome == "file_storage_hit"
        assert epoch_log.was_file_storage_hit is True

# %% [markdown]
# ## NodeInfo.epoch_logs property

# %%
#|export
@pytest.mark.asyncio
async def test_node_info_epoch_logs():
    """NodeInfo.epoch_logs should return filtered EpochLogs for that node."""
    def node_a_func(ctx, packets):
        for pids in packets.values():
            for pid in pids:
                ctx.consume_packet(pid)
        pid = ctx.create_packet("from_a")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    def node_b_func(ctx, packets):
        for pids in packets.values():
            for pid in pids:
                ctx.consume_packet(pid)

    config = NetConfig(
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    in_ports={"in": PortConfig()},
                    out_ports={"out": PortConfig()},
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={"in": PacketCountNConfig(count=1)},
                            term=SalvoConditionTermTrueConfig(),
                        )
                    },
                    out_salvo_conditions={
                        "send": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={"out": PacketCountNConfig(count=1)},
                            term=SalvoConditionTermTrueConfig(),
                        )
                    },
                    execution_config=NodeExecutionConfig(
                        exec_node_func=node_a_func, pools=["main"],
                    ),
                ),
                NodeConfig(
                    name="B",
                    in_ports={"in": PortConfig()},
                    in_salvo_conditions={
                        "trigger": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={"in": PacketCountNConfig(count=1)},
                            term=SalvoConditionTermTrueConfig(),
                        )
                    },
                    execution_config=NodeExecutionConfig(
                        exec_node_func=node_b_func, pools=["main"],
                    ),
                ),
            ],
            edges=[
                EdgeConfig(source_node="A", source_port="out", target_node="B", target_port="in"),
            ],
        ),
        retain_epoch_logs=True,
        print_echo_stdout=False,
    )

    async with Net(config) as net:
        net.inject_data("A", "in", ["hello"])
        await net.run_until_blocked()

        # NodeInfo should filter epoch logs to its own node
        a_logs = net.nodes["A"].epoch_logs
        b_logs = net.nodes["B"].epoch_logs

    assert len(a_logs) == 1
    assert a_logs[0].node_name == "A"
    assert len(b_logs) == 1
    assert b_logs[0].node_name == "B"

# %% [markdown]
# ## SimActionLog action_kind verification

# %%
#|export
@pytest.mark.asyncio
async def test_sim_action_log_action_kinds():
    """SimActionLogs should have correct action_kind values for specific actions."""
    def exec_func(ctx, packets):
        for pids in packets.values():
            for pid in pids:
                ctx.consume_packet(pid)
        pid = ctx.create_packet("result")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    config = _simple_config(exec_func, retain_epoch_logs=True)
    async with Net(config) as net:
        net.inject_data("A", "in", ["data"])
        made_progress, sim_actions, epoch_logs = await net.run_until_blocked()

    # Collect all action_kinds from the step
    action_kinds = [sa.action_kind for sa in sim_actions]

    # Should include RunStep (the automatic flow step)
    assert "RunStep" in action_kinds
    # Should include start_epoch
    assert any("start_epoch" in ak for ak in action_kinds)
    # Should include finish_epoch
    assert any("finish_epoch" in ak for ak in action_kinds)

    # Epoch-attributed actions should have matching epoch_id
    epoch_log = epoch_logs[0]
    epoch_actions = [sa for sa in sim_actions if sa.epoch_id == epoch_log.epoch_id]
    epoch_action_kinds = [sa.action_kind for sa in epoch_actions]
    assert any("start_epoch" in ak for ak in epoch_action_kinds)
    assert any("finish_epoch" in ak for ak in epoch_action_kinds)

# %% [markdown]
# ## SimEventLog detail fields

# %%
#|export
@pytest.mark.asyncio
async def test_sim_event_log_detail_fields():
    """SimEventLog detail dicts should contain event-specific string fields."""
    def exec_func(ctx, packets):
        for pids in packets.values():
            for pid in pids:
                ctx.consume_packet(pid)
        pid = ctx.create_packet("result")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    config = _simple_config(exec_func)
    async with Net(config) as net:
        net.inject_data("A", "in", ["data"])
        _, sim_actions, _ = await net.run_until_blocked()

    # Find a RunStep action — it should produce events with detail fields
    run_step_actions = [sa for sa in sim_actions if sa.action_kind == "RunStep"]
    assert len(run_step_actions) > 0

    # At least some events should have detail fields (e.g. packet_id, node_name)
    all_events = [ev for sa in sim_actions for ev in sa.events]
    events_with_details = [ev for ev in all_events if ev.detail]
    assert len(events_with_details) > 0

    # All detail values should be strings (ULID conversion)
    for ev in all_events:
        for key, val in ev.detail.items():
            assert isinstance(val, str), f"detail[{key!r}] = {val!r} is not a string"

# %% [markdown]
# ## ctx.log() with level="error"

# %%
#|export
@pytest.mark.asyncio
async def test_ctx_log_error_level():
    """ctx.log() with level='error' should set the level field correctly."""
    def exec_func(ctx, packets):
        for pids in packets.values():
            for pid in pids:
                ctx.consume_packet(pid)
        ctx.log("Something went wrong", level="error", code=500)
        pid = ctx.create_packet("ok")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    config = _simple_config(exec_func, retain_epoch_logs=True)
    async with Net(config) as net:
        net.inject_data("A", "in", ["data"])
        await net.run_until_blocked()

    epoch_log = list(net.epoch_logs.values())[0]
    assert len(epoch_log.node_log_entries) == 1
    entry = epoch_log.node_log_entries[0]
    assert entry.level == "error"
    assert entry.message == "Something went wrong"
    assert entry.fields == {"code": 500}

# %% [markdown]
# ## sim_action_log retention negative test

# %%
#|export
@pytest.mark.asyncio
async def test_retain_sim_action_logs_false():
    """retain_sim_action_logs=False should not accumulate SimActionLogs."""
    def exec_func(ctx, packets):
        for pids in packets.values():
            for pid in pids:
                ctx.consume_packet(pid)
        pid = ctx.create_packet("ok")
        ctx.load_output_port("out", pid)
        ctx.send_output_salvo("send")

    config = _simple_config(exec_func, retain_sim_action_logs=False)
    async with Net(config) as net:
        net.inject_data("A", "in", ["data"])
        await net.run_until_blocked()

    assert len(net.sim_action_log) == 0
