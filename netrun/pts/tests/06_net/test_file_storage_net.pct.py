# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for File Storage in Net Execution

# %%
#|default_exp net.test_file_storage_net

# %%
#|export
import tempfile
import pytest
import asyncio

from netrun.net._net import Net, NodeExecutionContext, EpochRecord
from netrun.net.config import (
    NetConfig,
    GraphConfig,
    NodeConfig,
    NodeExecutionConfig,
    PortConfig,
    EdgeConfig,
    PoolConfig,
    MainPoolConfig,
    SalvoConditionConfig,
    SalvoConditionTermPortConfig,
    SalvoConditionTermTrueConfig,
    MaxSalvosFiniteConfig,
    PacketCountAllConfig,
    PortStateNonEmptyConfig,
    OutputQueueConfig,
)
from netrun.storage.config import (
    StorageConfig,
    NodeStorageConfig,
    NodeFileStorageConfig,
    LocalBackendConfig,
    OnHashChange,
)

# %%
#|export
def _make_node(name, in_ports, out_ports, exec_func, *, pools=None, file_storage=None):
    """Helper to create a NodeConfig with salvo conditions and optional file storage."""
    in_port_cfgs = {p: PortConfig() for p in in_ports}
    out_port_cfgs = {p: PortConfig() for p in out_ports}

    in_salvo = {}
    if in_ports:
        in_salvo["trigger"] = SalvoConditionConfig(
            max_salvos=MaxSalvosFiniteConfig(max=1),
            ports={p: PacketCountAllConfig() for p in in_ports},
            term=SalvoConditionTermPortConfig(
                port_name=in_ports[0],
                state=PortStateNonEmptyConfig(),
            ),
        )

    out_salvo = {}
    if out_ports:
        out_salvo["send"] = SalvoConditionConfig(
            max_salvos=MaxSalvosFiniteConfig(max=1),
            ports={p: PacketCountAllConfig() for p in out_ports},
            term=SalvoConditionTermTrueConfig(),
        )

    storage = NodeStorageConfig(file_storage=file_storage) if file_storage is not None else None

    return NodeConfig(
        name=name,
        in_ports=in_port_cfgs,
        out_ports=out_port_cfgs,
        in_salvo_conditions=in_salvo,
        out_salvo_conditions=out_salvo,
        execution_config=NodeExecutionConfig(
            node_name=name,
            pools=pools or ["main"],
            exec_node_func=exec_func,
            storage=storage,
        ),
    )


def _make_net(*nodes, edges=None, storage=None, output_queues=None):
    """Helper to create a Net."""
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=list(nodes),
            edges=edges or [],
        ),
        storage=storage,
        output_queues=output_queues,
    )
    return Net(config)

# %% [markdown]
# ## Test: File storage disabled by default

# %%
#|export
@pytest.mark.asyncio
async def test_file_storage_disabled_by_default():
    """Without file_storage config, node executes every time."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)

    node = _make_node("A", ["in"], [], exec_fn)
    net = _make_net(node)

    async with net:
        net.inject_data("A", "in", [42])
        await net.run_until_blocked()

        net.inject_data("A", "in", [42])
        await net.run_until_blocked()

    assert call_count == 2

# %% [markdown]
# ## Test: File storage store and replay

# %%
#|export
@pytest.mark.asyncio
async def test_file_storage_store_and_replay():
    """File storage stores outputs on first run, replays from files on second."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                value = ctx.consume_packet(pid)
                out_id = ctx.create_packet(value * 2)
                ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    with tempfile.TemporaryDirectory() as tmpdir:
        fs_config = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="pickle",
        )
        storage = StorageConfig(
            file_storage_metadata_path=tmpdir + "/meta",
        )

        node = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_config)
        net = _make_net(
            node,
            storage=storage,
            output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
        )

        # First run: executes node function, stores to file storage
        async with net:
            net.inject_data("A", "in", [5])
            await net.run_until_blocked()
            results = net.flush_output_queue("results")

        assert call_count == 1
        assert len(results) == 1
        assert results[0] == 10  # 5 * 2

        # Second run: same inputs, should replay from file storage
        call_count = 0
        node2 = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_config)
        net2 = _make_net(
            node2,
            storage=storage,
            output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
        )

        async with net2:
            net2.inject_data("A", "in", [5])
            await net2.run_until_blocked()
            results2 = net2.flush_output_queue("results")

        assert call_count == 0  # Not called — replayed from file storage
        assert len(results2) == 1
        assert results2[0] == 10  # Same output

# %% [markdown]
# ## Test: File storage hash mismatch (error mode)

# %%
#|export
@pytest.mark.asyncio
async def test_file_storage_hash_mismatch_error():
    """When inputs change and on_hash_change='error', raises RuntimeError."""
    def exec_fn(ctx: NodeExecutionContext, packets):
        for port, pids in packets.items():
            for pid in pids:
                value = ctx.consume_packet(pid)
                out_id = ctx.create_packet(value * 2)
                ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    with tempfile.TemporaryDirectory() as tmpdir:
        fs_config = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="pickle",
            on_hash_change=OnHashChange.error,
        )
        storage = StorageConfig(
            file_storage_metadata_path=tmpdir + "/meta",
        )

        node = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_config)
        net = _make_net(node, storage=storage)

        # First run: store
        async with net:
            net.inject_data("A", "in", [5])
            await net.run_until_blocked()

        # Second run with different input: should error
        node2 = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_config)
        net2 = _make_net(node2, storage=storage)

        async with net2:
            net2.inject_data("A", "in", [99])  # Different input
            with pytest.raises(RuntimeError, match="hash mismatch"):
                await net2.run_until_blocked()

# %% [markdown]
# ## Test: File storage hash mismatch (overwrite mode)

# %%
#|export
@pytest.mark.asyncio
async def test_file_storage_hash_mismatch_overwrite():
    """When inputs change and on_hash_change='overwrite', re-stores the new output."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                value = ctx.consume_packet(pid)
                out_id = ctx.create_packet(value * 3)
                ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    with tempfile.TemporaryDirectory() as tmpdir:
        fs_config = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="pickle",
            on_hash_change=OnHashChange.overwrite,
        )
        storage = StorageConfig(
            file_storage_metadata_path=tmpdir + "/meta",
        )

        node = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_config)
        net = _make_net(
            node,
            storage=storage,
            output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
        )

        # First run
        async with net:
            net.inject_data("A", "in", [5])
            await net.run_until_blocked()
            results = net.flush_output_queue("results")
        assert call_count == 1
        assert results[0] == 15  # 5 * 3

        # Second run with different input: overwrites
        call_count = 0
        node2 = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_config)
        net2 = _make_net(
            node2,
            storage=storage,
            output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
        )

        async with net2:
            net2.inject_data("A", "in", [10])
            await net2.run_until_blocked()
            results2 = net2.flush_output_queue("results")
        assert call_count == 1  # Executed (not replayed)
        assert results2[0] == 30  # 10 * 3

        # Third run with same input as second: should replay
        call_count = 0
        node3 = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_config)
        net3 = _make_net(
            node3,
            storage=storage,
            output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
        )

        async with net3:
            net3.inject_data("A", "in", [10])
            await net3.run_until_blocked()
            results3 = net3.flush_output_queue("results")
        assert call_count == 0  # Replayed
        assert results3[0] == 30

# %% [markdown]
# ## Test: File storage version invalidation

# %%
#|export
@pytest.mark.asyncio
async def test_file_storage_version_invalidation():
    """Changing version forces re-execution even with same inputs."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                value = ctx.consume_packet(pid)
                out_id = ctx.create_packet(value + 1)
                ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    with tempfile.TemporaryDirectory() as tmpdir:
        fs_v0 = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="pickle",
            on_hash_change=OnHashChange.overwrite,
            version=0,
        )
        storage = StorageConfig(
            file_storage_metadata_path=tmpdir + "/meta",
        )

        # Run with version=0
        node = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_v0)
        net = _make_net(node, storage=storage)
        async with net:
            net.inject_data("A", "in", [42])
            await net.run_until_blocked()
        assert call_count == 1

        # Run again with version=0: should replay
        call_count = 0
        node2 = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_v0)
        net2 = _make_net(node2, storage=storage)
        async with net2:
            net2.inject_data("A", "in", [42])
            await net2.run_until_blocked()
        assert call_count == 0  # Replayed

        # Run with version=1: should re-execute
        call_count = 0
        fs_v1 = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="pickle",
            on_hash_change=OnHashChange.overwrite,
            version=1,
        )
        node3 = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_v1)
        net3 = _make_net(node3, storage=storage)
        async with net3:
            net3.inject_data("A", "in", [42])
            await net3.run_until_blocked()
        assert call_count == 1  # Re-executed due to version change

# %% [markdown]
# ## Test: File storage with JSON serialization

# %%
#|export
@pytest.mark.asyncio
async def test_file_storage_json_serialization():
    """File storage with JSON serialization round-trips correctly."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                value = ctx.consume_packet(pid)
                out_id = ctx.create_packet({"result": value * 2})
                ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    with tempfile.TemporaryDirectory() as tmpdir:
        fs_config = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="json",
        )
        storage = StorageConfig(
            file_storage_metadata_path=tmpdir + "/meta",
        )

        node = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_config)
        net = _make_net(
            node,
            storage=storage,
            output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
        )

        # First run
        async with net:
            net.inject_data("A", "in", [7])
            await net.run_until_blocked()
            results = net.flush_output_queue("results")
        assert results[0] == {"result": 14}
        assert call_count == 1

        # Replay
        call_count = 0
        node2 = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_config)
        net2 = _make_net(
            node2,
            storage=storage,
            output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
        )
        async with net2:
            net2.inject_data("A", "in", [7])
            await net2.run_until_blocked()
            results2 = net2.flush_output_queue("results")
        assert call_count == 0
        assert results2[0] == {"result": 14}

# %% [markdown]
# ## Test: File storage bundle mode (tar.gz)

# %%
#|export
@pytest.mark.asyncio
async def test_file_storage_bundle_tar_gz():
    """Bundle mode stores all outputs in a tar.gz archive and replays correctly."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                value = ctx.consume_packet(pid)
                out_id = ctx.create_packet(value * 2)
                ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    with tempfile.TemporaryDirectory() as tmpdir:
        fs_config = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="pickle",
            bundle=True,
            bundle_format="tar.gz",
        )
        storage = StorageConfig(
            file_storage_metadata_path=tmpdir + "/meta",
        )

        node = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_config)
        net = _make_net(
            node,
            storage=storage,
            output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
        )

        # First run: store in bundle
        async with net:
            net.inject_data("A", "in", [5])
            await net.run_until_blocked()
            results = net.flush_output_queue("results")

        assert call_count == 1
        assert len(results) == 1
        assert results[0] == 10

        # Verify archive was created
        import os
        archive_path = os.path.join(tmpdir, "data", "A", "outputs.tar.gz")
        assert os.path.exists(archive_path)

        # Second run: replay from bundle
        call_count = 0
        node2 = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_config)
        net2 = _make_net(
            node2,
            storage=storage,
            output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
        )

        async with net2:
            net2.inject_data("A", "in", [5])
            await net2.run_until_blocked()
            results2 = net2.flush_output_queue("results")

        assert call_count == 0  # Replayed from bundle
        assert results2[0] == 10

# %% [markdown]
# ## Test: File storage bundle mode (zip)

# %%
#|export
@pytest.mark.asyncio
async def test_file_storage_bundle_zip():
    """Bundle mode with zip format stores and replays correctly."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                value = ctx.consume_packet(pid)
                out_id = ctx.create_packet({"data": value + 100})
                ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    with tempfile.TemporaryDirectory() as tmpdir:
        fs_config = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="json",
            bundle=True,
            bundle_format="zip",
        )
        storage = StorageConfig(
            file_storage_metadata_path=tmpdir + "/meta",
        )

        node = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_config)
        net = _make_net(
            node,
            storage=storage,
            output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
        )

        # First run
        async with net:
            net.inject_data("A", "in", [7])
            await net.run_until_blocked()
            results = net.flush_output_queue("results")

        assert call_count == 1
        assert results[0] == {"data": 107}

        # Verify zip archive was created
        import os
        archive_path = os.path.join(tmpdir, "data", "A", "outputs.zip")
        assert os.path.exists(archive_path)

        # Replay
        call_count = 0
        node2 = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_config)
        net2 = _make_net(
            node2,
            storage=storage,
            output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
        )

        async with net2:
            net2.inject_data("A", "in", [7])
            await net2.run_until_blocked()
            results2 = net2.flush_output_queue("results")

        assert call_count == 0
        assert results2[0] == {"data": 107}

# %% [markdown]
# ## Test: Bundle mode with hash change overwrite

# %%
#|export
@pytest.mark.asyncio
async def test_file_storage_bundle_hash_change_overwrite():
    """Bundle mode re-stores when inputs change and on_hash_change='overwrite'."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                value = ctx.consume_packet(pid)
                out_id = ctx.create_packet(value * 5)
                ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    with tempfile.TemporaryDirectory() as tmpdir:
        fs_config = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="pickle",
            bundle=True,
            on_hash_change=OnHashChange.overwrite,
        )
        storage = StorageConfig(
            file_storage_metadata_path=tmpdir + "/meta",
        )

        # First run
        node = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_config)
        net = _make_net(
            node,
            storage=storage,
            output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
        )
        async with net:
            net.inject_data("A", "in", [3])
            await net.run_until_blocked()
            results = net.flush_output_queue("results")
        assert call_count == 1
        assert results[0] == 15

        # Second run with different input: re-executes
        call_count = 0
        node2 = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_config)
        net2 = _make_net(
            node2,
            storage=storage,
            output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
        )
        async with net2:
            net2.inject_data("A", "in", [4])
            await net2.run_until_blocked()
            results2 = net2.flush_output_queue("results")
        assert call_count == 1
        assert results2[0] == 20

        # Third run with same input as second: replays from bundle
        call_count = 0
        node3 = _make_node("A", ["in"], ["out"], exec_fn, file_storage=fs_config)
        net3 = _make_net(
            node3,
            storage=storage,
            output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
        )
        async with net3:
            net3.inject_data("A", "in", [4])
            await net3.run_until_blocked()
            results3 = net3.flush_output_queue("results")
        assert call_count == 0
        assert results3[0] == 20
