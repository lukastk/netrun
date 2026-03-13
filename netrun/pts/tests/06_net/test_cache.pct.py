# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Node Caching / Memoization

# %%
#|default_exp net.test_cache

# %%
#|export
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
    MaxSalvosInfiniteConfig,
    PacketCountAllConfig,
    PortStateNonEmptyConfig,
    OutputQueueConfig,
)
from netrun.storage.config import CacheConfig, CacheWhat, NodeCacheConfig, StorageConfig, NodeStorageConfig
from netrun.storage._cache import CachedEpochData

# %%
#|export
def _make_node(name, in_ports, out_ports, exec_func, *, pools=None, cache=None):
    """Helper to create a NodeConfig with salvo conditions."""
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

    storage = NodeStorageConfig(cache=cache) if cache is not None else None

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


def _make_net(*nodes, edges=None, cache=None, output_queues=None, retain_epoch_logs=False):
    """Helper to create a Net."""
    storage = StorageConfig(cache=cache) if cache is not None else None
    config = NetConfig(
        pools={"main": PoolConfig(spec=MainPoolConfig())},
        graph=GraphConfig(
            nodes=list(nodes),
            edges=edges or [],
        ),
        storage=storage,
        output_queues=output_queues,
        retain_epoch_logs=retain_epoch_logs,
    )
    return Net(config)

# %% [markdown]
# ## Test: Cache disabled by default

# %%
#|export
@pytest.mark.asyncio
async def test_cache_disabled_by_default():
    """Without a cache field, caching is disabled for all nodes."""
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
# ## Test: Cache hit on identical inputs (memoization)

# %%
#|export
@pytest.mark.asyncio
async def test_cache_hit_identical_inputs():
    """With cache enabled, the node function is called once for identical inputs."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
        out_id = ctx.create_packet(100)
        ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    node = _make_node("A", ["in"], ["out"], exec_fn)
    cache = CacheConfig(enabled=True, include_all_nodes=True)
    net = _make_net(
        node,
        cache=cache,
        output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
    )

    async with net:
        # First run: executes
        net.inject_data("A", "in", [42])
        await net.run_until_blocked()
        v1 = net.try_get_output("results")
        assert v1 == 100

        # Second run: cache hit
        net.inject_data("A", "in", [42])
        await net.run_until_blocked()
        v2 = net.try_get_output("results")
        assert v2 == 100

    assert call_count == 1

# %% [markdown]
# ## Test: Cache miss on different inputs

# %%
#|export
@pytest.mark.asyncio
async def test_cache_miss_different_inputs():
    """Different inputs produce a cache miss."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                val = ctx.consume_packet(pid)
                out_id = ctx.create_packet(val * 2)
                ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    node = _make_node("A", ["in"], ["out"], exec_fn)
    cache = CacheConfig(enabled=True, include_all_nodes=True)
    net = _make_net(
        node,
        cache=cache,
        output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
    )

    async with net:
        net.inject_data("A", "in", [10])
        await net.run_until_blocked()
        assert net.try_get_output("results") == 20

        net.inject_data("A", "in", [20])
        await net.run_until_blocked()
        assert net.try_get_output("results") == 40

    assert call_count == 2

# %% [markdown]
# ## Test: include_nodes glob matching

# %%
#|export
@pytest.mark.asyncio
async def test_include_nodes_glob():
    """Only nodes matching include_nodes patterns are cached."""
    calls = {"Proc": 0, "Other": 0}

    def exec_fn_proc(ctx: NodeExecutionContext, packets):
        calls["Proc"] += 1
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)

    def exec_fn_other(ctx: NodeExecutionContext, packets):
        calls["Other"] += 1
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)

    proc = _make_node("Processor", ["in"], [], exec_fn_proc)
    other = _make_node("OtherNode", ["in"], [], exec_fn_other)
    cache = CacheConfig(enabled=True, include_nodes=["Proc*"])
    net = _make_net(proc, other, cache=cache)

    async with net:
        # Run same input twice
        for _ in range(2):
            net.inject_data("Processor", "in", [1])
            net.inject_data("OtherNode", "in", [1])
            await net.run_until_blocked()

    # Processor: cached, so 1 call; OtherNode: not cached, so 2 calls
    assert calls["Proc"] == 1
    assert calls["Other"] == 2

# %% [markdown]
# ## Test: exclude_nodes glob matching

# %%
#|export
@pytest.mark.asyncio
async def test_exclude_nodes_glob():
    """Nodes matching exclude_nodes are not cached."""
    calls = {"A": 0, "B": 0}

    def exec_a(ctx: NodeExecutionContext, packets):
        calls["A"] += 1
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)

    def exec_b(ctx: NodeExecutionContext, packets):
        calls["B"] += 1
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)

    node_a = _make_node("NodeA", ["in"], [], exec_a)
    node_b = _make_node("NodeB", ["in"], [], exec_b)
    cache = CacheConfig(enabled=True, include_all_nodes=True, exclude_nodes=["NodeB"])
    net = _make_net(node_a, node_b, cache=cache)

    async with net:
        for _ in range(2):
            net.inject_data("NodeA", "in", [1])
            net.inject_data("NodeB", "in", [1])
            await net.run_until_blocked()

    assert calls["A"] == 1  # cached
    assert calls["B"] == 2  # excluded from cache

# %% [markdown]
# ## Test: Per-node cache enabled overrides net-level disabled

# %%
#|export
@pytest.mark.asyncio
async def test_per_node_enabled_override():
    """Per-node cache.enabled=True overrides net-level disabled."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)

    node = _make_node("A", ["in"], [], exec_fn,
                       cache=NodeCacheConfig(enabled=True))
    # Net-level: enabled=False (default)
    net = _make_net(node, cache=CacheConfig(enabled=False))

    async with net:
        for _ in range(2):
            net.inject_data("A", "in", [1])
            await net.run_until_blocked()

    assert call_count == 1

# %% [markdown]
# ## Test: Per-node cache disabled overrides net-level enabled

# %%
#|export
@pytest.mark.asyncio
async def test_per_node_disabled_override():
    """Per-node cache.enabled=False overrides net-level include_all_nodes."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)

    node = _make_node("A", ["in"], [], exec_fn,
                       cache=NodeCacheConfig(enabled=False))
    cache = CacheConfig(enabled=True, include_all_nodes=True)
    net = _make_net(node, cache=cache)

    async with net:
        for _ in range(2):
            net.inject_data("A", "in", [1])
            await net.run_until_blocked()

    assert call_count == 2

# %% [markdown]
# ## Test: OUTPUT_ONLY mode

# %%
#|export
@pytest.mark.asyncio
async def test_output_only_mode():
    """OUTPUT_ONLY: node always executes, but outputs are recorded."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                val = ctx.consume_packet(pid)
                out_id = ctx.create_packet(val * 3)
                ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    node = _make_node("A", ["in"], ["out"], exec_fn)
    cache = CacheConfig(enabled=True, include_all_nodes=True,
                        cache_what=CacheWhat.OUTPUT_ONLY)
    net = _make_net(
        node,
        cache=cache,
        output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
    )

    async with net:
        for _ in range(2):
            net.inject_data("A", "in", [5])
            await net.run_until_blocked()
            assert net.try_get_output("results") == 15

    # Always executes (no memoization)
    assert call_count == 2

    # But outputs are recorded
    entries = net.get_cached_entries("A")
    # Each run stores, but same hash means overwrite → 1 entry
    assert len(entries) >= 1
    assert entries[0].output_port_values["out"] == [15]

# %% [markdown]
# ## Test: INPUT_ONLY mode

# %%
#|export
@pytest.mark.asyncio
async def test_input_only_mode():
    """INPUT_ONLY: node always executes, inputs are recorded."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)

    node = _make_node("A", ["in"], [], exec_fn)
    cache = CacheConfig(enabled=True, include_all_nodes=True,
                        cache_what=CacheWhat.INPUT_ONLY)
    net = _make_net(node, cache=cache)

    async with net:
        net.inject_data("A", "in", [99])
        await net.run_until_blocked()

        net.inject_data("A", "in", [100])
        await net.run_until_blocked()

    # Always executes
    assert call_count == 2

    # Inputs recorded
    entries = net.get_cached_entries("A")
    assert len(entries) == 2
    # Check input values (sort by hash for deterministic order)
    input_vals = sorted([e.input_values_by_port["in"][0] for e in entries])
    assert input_vals == [99, 100]
    # No output data in INPUT_ONLY
    for e in entries:
        assert e.output_port_values == {}

# %% [markdown]
# ## Test: Retrieval API

# %%
#|export
@pytest.mark.asyncio
async def test_retrieval_api():
    """Test get_cached_input_salvos, get_cached_output_salvos, get_cached_output_for_input."""
    def exec_fn(ctx: NodeExecutionContext, packets):
        for port, pids in packets.items():
            for pid in pids:
                val = ctx.consume_packet(pid)
                out_id = ctx.create_packet(val + 1)
                ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    node = _make_node("A", ["in"], ["out"], exec_fn)
    cache = CacheConfig(enabled=True, include_all_nodes=True)
    net = _make_net(
        node,
        cache=cache,
        output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
    )

    async with net:
        net.inject_data("A", "in", [10])
        await net.run_until_blocked()
        net.try_get_output("results")  # drain

        net.inject_data("A", "in", [20])
        await net.run_until_blocked()
        net.try_get_output("results")  # drain

    # Input salvos
    inputs = net.get_cached_input_salvos("A")
    assert len(inputs) == 2
    in_vals = sorted([i["in"][0] for i in inputs])
    assert in_vals == [10, 20]

    # Output salvos
    outputs = net.get_cached_output_salvos("A")
    assert len(outputs) == 2
    out_vals = sorted([o["out"][0] for o in outputs])
    assert out_vals == [11, 21]

    # Lookup specific
    result = net.get_cached_output_for_input("A", {"in": [10]})
    assert result is not None
    assert result.output_port_values["out"] == [11]

    # Lookup miss
    result = net.get_cached_output_for_input("A", {"in": [999]})
    assert result is None

# %% [markdown]
# ## Test: cache_stats

# %%
#|export
@pytest.mark.asyncio
async def test_cache_stats():
    """cache_stats() returns correct counts."""
    def exec_fn(ctx: NodeExecutionContext, packets):
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)

    node = _make_node("A", ["in"], [], exec_fn)
    cache = CacheConfig(enabled=True, include_all_nodes=True)
    net = _make_net(node, cache=cache)

    async with net:
        for val in [1, 2, 3]:
            net.inject_data("A", "in", [val])
            await net.run_until_blocked()

        stats = net.cache_stats()
        assert "A" in stats
        assert stats["A"]["entry_count"] == 3
        assert stats["A"]["epoch_count"] == 3

# %% [markdown]
# ## Test: Reservoir sampling

# %%
#|export
@pytest.mark.asyncio
async def test_reservoir_sampling():
    """With sample_size=2, at most 2 entries are stored."""
    def exec_fn(ctx: NodeExecutionContext, packets):
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)

    node = _make_node("A", ["in"], [], exec_fn)
    cache = CacheConfig(enabled=True, include_all_nodes=True,
                        cache_what=CacheWhat.INPUT_ONLY, sample_size=2)
    net = _make_net(node, cache=cache)

    async with net:
        for val in range(10):
            net.inject_data("A", "in", [val])
            await net.run_until_blocked()

    entries = net.get_cached_entries("A")
    assert len(entries) <= 2

# %% [markdown]
# ## Test: Multiple input ports

# %%
#|export
@pytest.mark.asyncio
async def test_multiple_input_ports():
    """Cache correctly hashes and replays with multiple input ports."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        total = 0
        for port, pids in packets.items():
            for pid in pids:
                total += ctx.consume_packet(pid)
        out_id = ctx.create_packet(total)
        ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    in_port_cfgs = {"a": PortConfig(), "b": PortConfig()}
    out_port_cfgs = {"out": PortConfig()}

    node = NodeConfig(
        name="Add",
        in_ports=in_port_cfgs,
        out_ports=out_port_cfgs,
        in_salvo_conditions={
            "trigger": SalvoConditionConfig(
                max_salvos=MaxSalvosFiniteConfig(max=1),
                ports={"a": PacketCountAllConfig(), "b": PacketCountAllConfig()},
                term=SalvoConditionTermPortConfig(
                    port_name="a", state=PortStateNonEmptyConfig(),
                ),
            ),
        },
        out_salvo_conditions={
            "send": SalvoConditionConfig(
                max_salvos=MaxSalvosFiniteConfig(max=1),
                ports={"out": PacketCountAllConfig()},
                term=SalvoConditionTermTrueConfig(),
            ),
        },
        execution_config=NodeExecutionConfig(
            node_name="Add",
            pools=["main"],
            exec_node_func=exec_fn,
        ),
    )

    cache = CacheConfig(enabled=True, include_all_nodes=True)
    net = _make_net(
        node,
        cache=cache,
        output_queues={"results": OutputQueueConfig(ports=[("Add", "out")])},
    )

    async with net:
        net.inject_data("Add", "a", [3])
        net.inject_data("Add", "b", [7])
        await net.run_until_blocked()
        assert net.try_get_output("results") == 10

        # Same input → cache hit
        net.inject_data("Add", "a", [3])
        net.inject_data("Add", "b", [7])
        await net.run_until_blocked()
        assert net.try_get_output("results") == 10

    assert call_count == 1

# %% [markdown]
# ## Test: Version invalidation

# %%
#|export
@pytest.mark.asyncio
async def test_version_invalidation():
    """Changing cache version invalidates cached entries."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)
                out_id = ctx.create_packet(call_count)
                ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    import tempfile
    storage_path = tempfile.mkdtemp(prefix="netrun_test_cache_")

    # Run with version=0
    node = _make_node("A", ["in"], ["out"], exec_fn)
    cache = CacheConfig(enabled=True, include_all_nodes=True,
                        version=0, storage_path=storage_path)
    net = _make_net(
        node,
        cache=cache,
        output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
    )
    async with net:
        net.inject_data("A", "in", [42])
        await net.run_until_blocked()
        assert net.try_get_output("results") == 1

    assert call_count == 1

    # Run again with version=1 (same storage_path)
    call_count = 0
    node = _make_node("A", ["in"], ["out"], exec_fn)
    cache = CacheConfig(enabled=True, include_all_nodes=True,
                        version=1, storage_path=storage_path)
    net = _make_net(
        node,
        cache=cache,
        output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
    )
    async with net:
        net.inject_data("A", "in", [42])
        await net.run_until_blocked()
        # Version changed → cache miss → re-executes
        result = net.try_get_output("results")
        assert result == 1

    assert call_count == 1  # re-executed

# %% [markdown]
# ## Test: Per-node version override

# %%
#|export
@pytest.mark.asyncio
async def test_per_node_version():
    """NodeCacheConfig.version overrides net version."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)

    import tempfile
    storage_path = tempfile.mkdtemp(prefix="netrun_test_cache_")

    # Run with net version=0, node version=10
    node = _make_node("A", ["in"], [], exec_fn,
                       cache=NodeCacheConfig(version=10))
    cache = CacheConfig(enabled=True, include_all_nodes=True,
                        version=0, storage_path=storage_path)
    net = _make_net(node, cache=cache)
    async with net:
        net.inject_data("A", "in", [1])
        await net.run_until_blocked()
    assert call_count == 1

    # Same net version=0, node version=10 → cache hit
    call_count = 0
    node = _make_node("A", ["in"], [], exec_fn,
                       cache=NodeCacheConfig(version=10))
    cache = CacheConfig(enabled=True, include_all_nodes=True,
                        version=0, storage_path=storage_path)
    net = _make_net(node, cache=cache)
    async with net:
        net.inject_data("A", "in", [1])
        await net.run_until_blocked()
    assert call_count == 0  # cache hit

    # Change node version=11 → cache miss
    call_count = 0
    node = _make_node("A", ["in"], [], exec_fn,
                       cache=NodeCacheConfig(version=11))
    cache = CacheConfig(enabled=True, include_all_nodes=True,
                        version=0, storage_path=storage_path)
    net = _make_net(node, cache=cache)
    async with net:
        net.inject_data("A", "in", [1])
        await net.run_until_blocked()
    assert call_count == 1  # cache miss

# %% [markdown]
# ## Test: clear_cache

# %%
#|export
@pytest.mark.asyncio
async def test_clear_cache():
    """clear_cache() clears all entries."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)

    node = _make_node("A", ["in"], [], exec_fn)
    cache = CacheConfig(enabled=True, include_all_nodes=True)
    net = _make_net(node, cache=cache)

    async with net:
        net.inject_data("A", "in", [1])
        await net.run_until_blocked()
        assert call_count == 1

        # Cache hit
        net.inject_data("A", "in", [1])
        await net.run_until_blocked()
        assert call_count == 1

        # Clear
        net.clear_cache()

        # Cache miss → re-executes
        net.inject_data("A", "in", [1])
        await net.run_until_blocked()
        assert call_count == 2

# %% [markdown]
# ## Test: clear_node_cache

# %%
#|export
@pytest.mark.asyncio
async def test_clear_node_cache():
    """clear_node_cache() clears only the specified node's entries."""
    calls = {"A": 0, "B": 0}

    def exec_a(ctx: NodeExecutionContext, packets):
        calls["A"] += 1
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)

    def exec_b(ctx: NodeExecutionContext, packets):
        calls["B"] += 1
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)

    node_a = _make_node("A", ["in"], [], exec_a)
    node_b = _make_node("B", ["in"], [], exec_b)
    cache = CacheConfig(enabled=True, include_all_nodes=True)
    net = _make_net(node_a, node_b, cache=cache)

    async with net:
        # Populate cache
        net.inject_data("A", "in", [1])
        net.inject_data("B", "in", [1])
        await net.run_until_blocked()
        assert calls == {"A": 1, "B": 1}

        # Clear only A's cache
        net.clear_node_cache("A")

        # Re-run
        net.inject_data("A", "in", [1])
        net.inject_data("B", "in", [1])
        await net.run_until_blocked()

    assert calls["A"] == 2  # cleared, re-executed
    assert calls["B"] == 1  # still cached

# %% [markdown]
# ## Test: clear_cached_output_for_input

# %%
#|export
@pytest.mark.asyncio
async def test_clear_cached_output_for_input():
    """clear_cached_output_for_input() removes a specific cache entry."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)

    node = _make_node("A", ["in"], [], exec_fn)
    cache = CacheConfig(enabled=True, include_all_nodes=True)
    net = _make_net(node, cache=cache)

    async with net:
        net.inject_data("A", "in", [1])
        await net.run_until_blocked()

        net.inject_data("A", "in", [2])
        await net.run_until_blocked()
        assert call_count == 2

        # Clear only input=1
        net.clear_cached_output_for_input("A", {"in": [1]})

        # input=1 re-executes, input=2 still cached
        net.inject_data("A", "in", [1])
        await net.run_until_blocked()
        assert call_count == 3

        net.inject_data("A", "in", [2])
        await net.run_until_blocked()
        assert call_count == 3  # cache hit

# %% [markdown]
# ## Test: clear_cached_inputs

# %%
#|export
@pytest.mark.asyncio
async def test_clear_cached_inputs():
    """clear_cached_inputs() clears INPUT_ONLY entries."""
    def exec_fn(ctx: NodeExecutionContext, packets):
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)

    node = _make_node("A", ["in"], [], exec_fn)
    cache = CacheConfig(enabled=True, include_all_nodes=True,
                        cache_what=CacheWhat.INPUT_ONLY)
    net = _make_net(node, cache=cache)

    async with net:
        net.inject_data("A", "in", [1])
        await net.run_until_blocked()
        assert len(net.get_cached_entries("A")) == 1

        net.clear_cached_inputs()
        assert len(net.get_cached_entries("A")) == 0

# %% [markdown]
# ## Test: NodeInfo cache helpers

# %%
#|export
@pytest.mark.asyncio
async def test_node_info_cache_helpers():
    """NodeInfo provides cache helpers that delegate to Net."""
    def exec_fn(ctx: NodeExecutionContext, packets):
        for port, pids in packets.items():
            for pid in pids:
                val = ctx.consume_packet(pid)
                out_id = ctx.create_packet(val * 2)
                ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    node = _make_node("A", ["in"], ["out"], exec_fn)
    cache = CacheConfig(enabled=True, include_all_nodes=True)
    net = _make_net(
        node,
        cache=cache,
        output_queues={"results": OutputQueueConfig(ports=[("A", "out")])},
    )

    async with net:
        net.inject_data("A", "in", [5])
        await net.run_until_blocked()
        net.try_get_output("results")  # drain

    node_info = net.nodes["A"]

    # is_cache_enabled
    assert node_info.is_cache_enabled is True

    # cached_entries
    assert len(node_info.cached_entries) == 1

    # cached_input_salvos / cached_output_salvos
    assert node_info.cached_input_salvos == [{"in": [5]}]
    assert node_info.cached_output_salvos == [{"out": [10]}]

    # get_cached_output_for_input
    result = node_info.get_cached_output_for_input({"in": [5]})
    assert result is not None
    assert result.output_port_values["out"] == [10]

    # cache_stats
    stats = node_info.cache_stats
    assert stats["entry_count"] == 1

    # clear_cache
    node_info.clear_cache()
    assert len(node_info.cached_entries) == 0

# %% [markdown]
# ## Test: EpochRecord.was_cache_hit

# %%
#|export
@pytest.mark.asyncio
async def test_epoch_record_cache_hit_flag():
    """EpochRecord.was_cache_hit is True for cached replays."""
    call_count = 0

    def exec_fn(ctx: NodeExecutionContext, packets):
        nonlocal call_count
        call_count += 1
        for port, pids in packets.items():
            for pid in pids:
                ctx.consume_packet(pid)

    node = _make_node("A", ["in"], [], exec_fn)
    cache = CacheConfig(enabled=True, include_all_nodes=True)
    net = _make_net(node, cache=cache, retain_epoch_logs=True)

    async with net:
        net.inject_data("A", "in", [1])
        await net.run_until_blocked()

        net.inject_data("A", "in", [1])
        await net.run_until_blocked()

    epochs = list(net.epochs.values())
    assert len(epochs) == 2
    assert epochs[0].was_cache_hit is False
    assert epochs[1].was_cache_hit is True

# %% [markdown]
# ## Test: Two-node pipeline with caching

# %%
#|export
@pytest.mark.asyncio
async def test_two_node_pipeline_cache():
    """Caching works in a two-node pipeline: Source -> Processor."""
    proc_calls = 0

    def source_fn(ctx: NodeExecutionContext, packets):
        for port, pids in packets.items():
            for pid in pids:
                val = ctx.consume_packet(pid)
                out_id = ctx.create_packet(val)
                ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    def processor_fn(ctx: NodeExecutionContext, packets):
        nonlocal proc_calls
        proc_calls += 1
        for port, pids in packets.items():
            for pid in pids:
                val = ctx.consume_packet(pid)
                out_id = ctx.create_packet(val * 10)
                ctx.load_output_port("out", out_id)
        ctx.send_output_salvo("send")

    source = _make_node("Source", ["in"], ["out"], source_fn)
    processor = _make_node("Processor", ["in"], ["out"], processor_fn)

    cache = CacheConfig(enabled=True, include_nodes=["Processor"])
    net = _make_net(
        source, processor,
        edges=[EdgeConfig(source_node="Source", source_port="out", target_node="Processor", target_port="in")],
        cache=cache,
        output_queues={"results": OutputQueueConfig(ports=[("Processor", "out")])},
    )

    async with net:
        # First run
        net.inject_data("Source", "in", [5])
        await net.run_until_blocked()
        assert net.try_get_output("results") == 50

        # Second run with same value
        net.inject_data("Source", "in", [5])
        await net.run_until_blocked()
        assert net.try_get_output("results") == 50

    assert proc_calls == 1  # Processor cached
