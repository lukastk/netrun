# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for MultiprocessPool

# %%
#|default_exp pool.test_multiprocess

# %%
#|export
import pytest
import asyncio
from netrun.rpc.base import RecvTimeout
from netrun.pool.base import (
    PoolNotStarted,
    PoolAlreadyStarted,
)
from netrun.pool.multiprocess import MultiprocessPool

# %% [markdown]
# ## Import Worker Functions
#
# Worker functions are in an importable module for multiprocessing.

# %%
#|export
from .workers import echo_worker, compute_worker, pid_worker

# %% [markdown]
# ## Test Pool Creation

# %%
#|export
def test_pool_creation():
    """Test creating a MultiprocessPool."""
    pool = MultiprocessPool(echo_worker, num_processes=2, threads_per_process=2)
    assert pool.num_workers == 4
    assert pool.num_processes == 2
    assert pool.threads_per_process == 2
    assert not pool.is_running

# %%
test_pool_creation();

# %%
#|export
def test_pool_default_threads():
    """Test that threads_per_process defaults to 1."""
    pool = MultiprocessPool(echo_worker, num_processes=3)
    assert pool.num_workers == 3
    assert pool.threads_per_process == 1

# %%
test_pool_default_threads();

# %%
#|export
def test_pool_invalid_num_processes():
    """Test that invalid num_processes raises ValueError."""
    with pytest.raises(ValueError):
        MultiprocessPool(echo_worker, num_processes=0)

    with pytest.raises(ValueError):
        MultiprocessPool(echo_worker, num_processes=-1)

# %%
test_pool_invalid_num_processes();

# %%
#|export
def test_pool_invalid_threads_per_process():
    """Test that invalid threads_per_process raises ValueError."""
    with pytest.raises(ValueError):
        MultiprocessPool(echo_worker, num_processes=2, threads_per_process=0)

# %%
test_pool_invalid_threads_per_process();

# %% [markdown]
# ## Test Pool Lifecycle

# %%
#|export
@pytest.mark.asyncio
async def test_start_and_close():
    """Test starting and closing a pool."""
    pool = MultiprocessPool(echo_worker, num_processes=1, threads_per_process=1)

    assert not pool.is_running
    await pool.start()
    assert pool.is_running

    await pool.close()
    assert not pool.is_running

# %%
await test_start_and_close();

# %%
#|export
@pytest.mark.asyncio
async def test_double_start_raises():
    """Test that starting twice raises PoolAlreadyStarted."""
    pool = MultiprocessPool(echo_worker, num_processes=1)
    await pool.start()

    try:
        with pytest.raises(PoolAlreadyStarted):
            await pool.start()
    finally:
        await pool.close()

# %%
await test_double_start_raises();

# %%
#|export
@pytest.mark.asyncio
async def test_context_manager():
    """Test using pool as context manager."""
    async with MultiprocessPool(echo_worker, num_processes=1) as pool:
        assert pool.is_running

    assert not pool.is_running

# %%
await test_context_manager();

# %% [markdown]
# ## Test Send/Recv

# %%
#|export
@pytest.mark.asyncio
async def test_send_recv_single():
    """Test sending and receiving a single message."""
    async with MultiprocessPool(echo_worker, num_processes=1, threads_per_process=1) as pool:
        await pool.send(worker_id=0, key="test", data="hello")
        msg = await pool.recv(timeout=10.0)

        assert msg.worker_id == 0
        assert msg.key == "echo:test"
        assert msg.data == {"worker_id": 0, "data": "hello"}

# %%
await test_send_recv_single();

# %%
#|export
@pytest.mark.asyncio
async def test_send_recv_multiple_workers():
    """Test sending to multiple workers across processes."""
    async with MultiprocessPool(echo_worker, num_processes=2, threads_per_process=2) as pool:
        # Send to each worker
        for i in range(pool.num_workers):
            await pool.send(worker_id=i, key="ping", data=i)

        # Receive all responses
        responses = []
        for _ in range(pool.num_workers):
            msg = await pool.recv(timeout=10.0)
            responses.append(msg)

        assert len(responses) == 4
        worker_ids = {msg.worker_id for msg in responses}
        assert worker_ids == {0, 1, 2, 3}

# %%
await test_send_recv_multiple_workers();

# %% [markdown]
# ## Test Worker ID Mapping

# %%
#|export
@pytest.mark.asyncio
async def test_worker_id_mapping():
    """Test that worker IDs map correctly to processes and threads."""
    # 2 processes x 2 threads = 4 workers
    # Worker 0, 1 in process 0
    # Worker 2, 3 in process 1
    async with MultiprocessPool(pid_worker, num_processes=2, threads_per_process=2) as pool:
        for i in range(4):
            await pool.send(worker_id=i, key="get_pid", data=None)

        pids = {}
        for _ in range(4):
            msg = await pool.recv(timeout=10.0)
            pids[msg.data["worker_id"]] = msg.data["pid"]

        # Workers 0 and 1 should have same PID (same process)
        assert pids[0] == pids[1]
        # Workers 2 and 3 should have same PID (same process)
        assert pids[2] == pids[3]
        # Different processes should have different PIDs
        assert pids[0] != pids[2]

# %%
await test_worker_id_mapping();

# %% [markdown]
# ## Test try_recv

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_empty():
    """Test try_recv when no messages pending."""
    async with MultiprocessPool(echo_worker, num_processes=1) as pool:
        result = await pool.try_recv()
        assert result is None

# %%
await test_try_recv_empty();

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_with_message():
    """Test try_recv with pending message."""
    async with MultiprocessPool(echo_worker, num_processes=1) as pool:
        await pool.send(worker_id=0, key="test", data="data")
        await asyncio.sleep(0.5)  # Let worker process

        result = await pool.try_recv()
        assert result is not None
        assert result.key == "echo:test"

# %%
await test_try_recv_with_message();

# %% [markdown]
# ## Test Broadcast

# %%
#|export
@pytest.mark.asyncio
async def test_broadcast():
    """Test broadcasting to all workers."""
    async with MultiprocessPool(echo_worker, num_processes=2, threads_per_process=2) as pool:
        await pool.broadcast("config", {"setting": "value"})

        responses = []
        for _ in range(pool.num_workers):
            msg = await pool.recv(timeout=10.0)
            responses.append(msg)

        assert len(responses) == 4
        worker_ids = {msg.worker_id for msg in responses}
        assert worker_ids == {0, 1, 2, 3}

# %%
await test_broadcast();

# %% [markdown]
# ## Test Timeout

# %%
#|export
@pytest.mark.asyncio
async def test_recv_timeout():
    """Test recv timeout."""
    async with MultiprocessPool(echo_worker, num_processes=1) as pool:
        with pytest.raises(RecvTimeout):
            await pool.recv(timeout=0.5)

# %%
await test_recv_timeout();

# %% [markdown]
# ## Test Error Handling

# %%
#|export
@pytest.mark.asyncio
async def test_send_before_start_raises():
    """Test that sending before start raises PoolNotStarted."""
    pool = MultiprocessPool(echo_worker, num_processes=1)

    with pytest.raises(PoolNotStarted):
        await pool.send(worker_id=0, key="test", data="data")

# %%
await test_send_before_start_raises();

# %%
#|export
@pytest.mark.asyncio
async def test_recv_before_start_raises():
    """Test that receiving before start raises PoolNotStarted."""
    pool = MultiprocessPool(echo_worker, num_processes=1)

    with pytest.raises(PoolNotStarted):
        await pool.recv()

# %%
await test_recv_before_start_raises();

# %%
#|export
@pytest.mark.asyncio
async def test_invalid_worker_id():
    """Test that invalid worker_id raises ValueError."""
    async with MultiprocessPool(echo_worker, num_processes=2, threads_per_process=2) as pool:
        with pytest.raises(ValueError):
            await pool.send(worker_id=-1, key="test", data="data")

        with pytest.raises(ValueError):
            await pool.send(worker_id=4, key="test", data="data")

# %%
await test_invalid_worker_id();

# %% [markdown]
# ## Test Computation

# %%
#|export
@pytest.mark.asyncio
async def test_compute_workers():
    """Test compute workers with actual computation."""
    async with MultiprocessPool(compute_worker, num_processes=2, threads_per_process=1) as pool:
        await pool.send(worker_id=0, key="square", data=7)
        await pool.send(worker_id=1, key="double", data=21)

        results = []
        for _ in range(2):
            msg = await pool.recv(timeout=10.0)
            results.append((msg.worker_id, msg.data))

        results.sort()  # Sort by worker_id
        assert results == [(0, 49), (1, 42)]

# %%
await test_compute_workers();
