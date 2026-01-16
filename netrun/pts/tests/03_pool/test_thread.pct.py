# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for ThreadPool

# %%
#|default_exp pool.test_thread

# %%
#|export
import pytest
import asyncio
from netrun.rpc.base import ChannelClosed, RecvTimeout
from netrun.pool.base import (
    PoolNotStarted,
    PoolAlreadyStarted,
)
from netrun.pool.thread import ThreadPool

# %% [markdown]
# ## Worker Functions

# %%
#|export
def echo_worker(channel, worker_id):
    """Echo worker for testing."""
    try:
        while True:
            key, data = channel.recv()
            channel.send(f"echo:{key}", {"worker_id": worker_id, "data": data})
    except ChannelClosed:
        pass

# %%
#|export
def compute_worker(channel, worker_id):
    """Compute worker for testing."""
    try:
        while True:
            key, data = channel.recv()
            if key == "square":
                channel.send("result", data * data)
            elif key == "double":
                channel.send("result", data * 2)
    except ChannelClosed:
        pass

# %%
#|export
def slow_worker(channel, worker_id):
    """Slow worker that takes time to respond."""
    import time
    try:
        while True:
            key, data = channel.recv()
            time.sleep(0.1)
            channel.send("done", data)
    except ChannelClosed:
        pass

# %% [markdown]
# ## Test Pool Creation

# %%
#|export
def test_pool_creation():
    """Test creating a ThreadPool."""
    pool = ThreadPool(echo_worker, num_workers=3)
    assert pool.num_workers == 3
    assert not pool.is_running

# %%
test_pool_creation();

# %%
#|export
def test_pool_invalid_num_workers():
    """Test that invalid num_workers raises ValueError."""
    with pytest.raises(ValueError):
        ThreadPool(echo_worker, num_workers=0)

    with pytest.raises(ValueError):
        ThreadPool(echo_worker, num_workers=-1)

# %%
test_pool_invalid_num_workers();

# %% [markdown]
# ## Test Pool Lifecycle

# %%
#|export
@pytest.mark.asyncio
async def test_start_and_close():
    """Test starting and closing a pool."""
    pool = ThreadPool(echo_worker, num_workers=2)

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
    pool = ThreadPool(echo_worker, num_workers=2)
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
    async with ThreadPool(echo_worker, num_workers=2) as pool:
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
    async with ThreadPool(echo_worker, num_workers=1) as pool:
        await pool.send(worker_id=0, key="test", data="hello")
        msg = await pool.recv(timeout=5.0)

        assert msg.worker_id == 0
        assert msg.key == "echo:test"
        assert msg.data == {"worker_id": 0, "data": "hello"}

# %%
await test_send_recv_single();

# %%
#|export
@pytest.mark.asyncio
async def test_send_recv_multiple_workers():
    """Test sending to multiple workers."""
    async with ThreadPool(echo_worker, num_workers=3) as pool:
        # Send to each worker
        for i in range(3):
            await pool.send(worker_id=i, key="ping", data=i)

        # Receive all responses
        responses = []
        for _ in range(3):
            msg = await pool.recv(timeout=5.0)
            responses.append(msg)

        assert len(responses) == 3
        worker_ids = {msg.worker_id for msg in responses}
        assert worker_ids == {0, 1, 2}

# %%
await test_send_recv_multiple_workers();

# %%
#|export
@pytest.mark.asyncio
async def test_send_recv_multiple_messages_same_worker():
    """Test sending multiple messages to the same worker."""
    async with ThreadPool(echo_worker, num_workers=1) as pool:
        for i in range(5):
            await pool.send(worker_id=0, key=f"msg{i}", data=i)

        responses = []
        for _ in range(5):
            msg = await pool.recv(timeout=5.0)
            responses.append(msg)

        assert len(responses) == 5
        assert all(msg.worker_id == 0 for msg in responses)

# %%
await test_send_recv_multiple_messages_same_worker();

# %% [markdown]
# ## Test try_recv

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_empty():
    """Test try_recv when no messages pending."""
    async with ThreadPool(echo_worker, num_workers=1) as pool:
        result = await pool.try_recv()
        assert result is None

# %%
await test_try_recv_empty();

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_with_message():
    """Test try_recv with pending message."""
    async with ThreadPool(echo_worker, num_workers=1) as pool:
        await pool.send(worker_id=0, key="test", data="data")
        await asyncio.sleep(0.1)  # Let worker process

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
    async with ThreadPool(echo_worker, num_workers=3) as pool:
        await pool.broadcast("config", {"setting": "value"})

        responses = []
        for _ in range(3):
            msg = await pool.recv(timeout=5.0)
            responses.append(msg)

        assert len(responses) == 3
        worker_ids = {msg.worker_id for msg in responses}
        assert worker_ids == {0, 1, 2}
        assert all(msg.data["data"] == {"setting": "value"} for msg in responses)

# %%
await test_broadcast();

# %% [markdown]
# ## Test Timeout

# %%
#|export
@pytest.mark.asyncio
async def test_recv_timeout():
    """Test recv timeout."""
    async with ThreadPool(echo_worker, num_workers=1) as pool:
        with pytest.raises(RecvTimeout):
            await pool.recv(timeout=0.1)

# %%
await test_recv_timeout();

# %% [markdown]
# ## Test Error Handling

# %%
#|export
@pytest.mark.asyncio
async def test_send_before_start_raises():
    """Test that sending before start raises PoolNotStarted."""
    pool = ThreadPool(echo_worker, num_workers=1)

    with pytest.raises(PoolNotStarted):
        await pool.send(worker_id=0, key="test", data="data")

# %%
await test_send_before_start_raises();

# %%
#|export
@pytest.mark.asyncio
async def test_recv_before_start_raises():
    """Test that receiving before start raises PoolNotStarted."""
    pool = ThreadPool(echo_worker, num_workers=1)

    with pytest.raises(PoolNotStarted):
        await pool.recv()

# %%
await test_recv_before_start_raises();

# %%
#|export
@pytest.mark.asyncio
async def test_invalid_worker_id():
    """Test that invalid worker_id raises ValueError."""
    async with ThreadPool(echo_worker, num_workers=2) as pool:
        with pytest.raises(ValueError):
            await pool.send(worker_id=-1, key="test", data="data")

        with pytest.raises(ValueError):
            await pool.send(worker_id=2, key="test", data="data")

        with pytest.raises(ValueError):
            await pool.send(worker_id=10, key="test", data="data")

# %%
await test_invalid_worker_id();

# %% [markdown]
# ## Test Computation

# %%
#|export
@pytest.mark.asyncio
async def test_compute_workers():
    """Test compute workers with actual computation."""
    async with ThreadPool(compute_worker, num_workers=2) as pool:
        await pool.send(worker_id=0, key="square", data=7)
        await pool.send(worker_id=1, key="double", data=21)

        results = []
        for _ in range(2):
            msg = await pool.recv(timeout=5.0)
            results.append((msg.worker_id, msg.data))

        results.sort()  # Sort by worker_id
        assert results == [(0, 49), (1, 42)]

# %%
await test_compute_workers();

# %% [markdown]
# ## Test Concurrent Responses

# %%
#|export
@pytest.mark.asyncio
async def test_concurrent_responses():
    """Test that all concurrent responses are received."""
    async with ThreadPool(echo_worker, num_workers=4) as pool:
        # Send to all workers simultaneously
        for i in range(4):
            await pool.send(worker_id=i, key="concurrent", data=i)

        # All should respond
        responses = []
        for _ in range(4):
            msg = await pool.recv(timeout=5.0)
            responses.append(msg.worker_id)

        assert sorted(responses) == [0, 1, 2, 3]

# %%
await test_concurrent_responses();
