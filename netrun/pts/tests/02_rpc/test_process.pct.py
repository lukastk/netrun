# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for ProcessChannel (RPC Process Module)

# %%
#|default_exp rpc.test_process

# %%
#|export
import pytest
import multiprocessing as mp
from netrun.rpc.base import ChannelClosed, RecvTimeout
from netrun.rpc.process import (
    ProcessChannel,
    SyncProcessChannel,
    create_queue_pair,
)

# %% [markdown]
# ## Test Channel Creation

# %%
#|export
@pytest.mark.asyncio
async def test_create_queue_pair():
    """Test creating a queue pair."""
    ctx = mp.get_context("spawn")
    parent_channel, child_queues = create_queue_pair(ctx)
    assert isinstance(parent_channel, ProcessChannel)
    assert isinstance(child_queues, tuple)
    assert len(child_queues) == 2

# %%
await test_create_queue_pair();

# %%
#|export
def test_sync_channel_creation():
    """Test creating a SyncProcessChannel."""
    ctx = mp.get_context("spawn")
    parent_channel, child_queues = create_queue_pair(ctx)
    send_q, recv_q = child_queues
    child_channel = SyncProcessChannel(send_q, recv_q)
    assert isinstance(child_channel, SyncProcessChannel)
    assert not child_channel.is_closed

# %%
test_sync_channel_creation();

# %% [markdown]
# ## Test Basic Send/Recv (Same Process)
#
# These tests verify the channel works within the same process before
# testing cross-process communication.

# %%
#|export
@pytest.mark.asyncio
async def test_same_process_send_recv():
    """Test send/recv within same process using threads."""
    import threading

    ctx = mp.get_context("spawn")
    parent_channel, child_queues = create_queue_pair(ctx)
    send_q, recv_q = child_queues
    child_channel = SyncProcessChannel(send_q, recv_q)

    received = []

    def worker():
        key, data = child_channel.recv()
        received.append((key, data))
        child_channel.send("ack", data * 2)

    thread = threading.Thread(target=worker)
    thread.start()

    await parent_channel.send("test", 21)
    key, data = await parent_channel.recv(timeout=2.0)
    thread.join(timeout=2.0)

    assert received == [("test", 21)]
    assert key == "ack"
    assert data == 42

# %%
await test_same_process_send_recv();

# %% [markdown]
# ## Test Cross-Process Communication
#
# Worker functions are imported from the workers module.

# %%
#|export
from .workers import echo_worker, compute_worker

# %%
#|export
@pytest.mark.asyncio
async def test_cross_process_echo():
    """Test echo communication across processes."""
    ctx = mp.get_context("spawn")
    parent_channel, child_queues = create_queue_pair(ctx)

    proc = ctx.Process(target=echo_worker, args=child_queues)
    proc.start()

    try:
        await parent_channel.send("hello", "world")
        key, data = await parent_channel.recv(timeout=5.0)

        assert key == "echo:hello"
        assert data == "world"
    finally:
        await parent_channel.close()
        proc.join(timeout=2.0)
        if proc.is_alive():
            proc.terminate()

# %%
await test_cross_process_echo();

# %%
#|export
@pytest.mark.asyncio
async def test_cross_process_multiple_messages():
    """Test multiple messages across processes."""
    ctx = mp.get_context("spawn")
    parent_channel, child_queues = create_queue_pair(ctx)

    proc = ctx.Process(target=echo_worker, args=child_queues)
    proc.start()

    try:
        # Send multiple messages
        for i in range(5):
            await parent_channel.send(f"msg{i}", i)

        # Receive all responses
        responses = []
        for _ in range(5):
            key, data = await parent_channel.recv(timeout=5.0)
            responses.append((key, data))

        # Check all received (order may vary)
        assert len(responses) == 5
        for key, data in responses:
            assert key.startswith("echo:msg")
    finally:
        await parent_channel.close()
        proc.join(timeout=2.0)
        if proc.is_alive():
            proc.terminate()

# %%
await test_cross_process_multiple_messages();

# %%
#|export
@pytest.mark.asyncio
async def test_cross_process_compute():
    """Test computation across processes."""
    ctx = mp.get_context("spawn")
    parent_channel, child_queues = create_queue_pair(ctx)

    proc = ctx.Process(target=compute_worker, args=child_queues)
    proc.start()

    try:
        await parent_channel.send("square", 7)
        key, data = await parent_channel.recv(timeout=5.0)
        assert key == "result"
        assert data == 49

        await parent_channel.send("double", 21)
        key, data = await parent_channel.recv(timeout=5.0)
        assert key == "result"
        assert data == 42
    finally:
        await parent_channel.close()
        proc.join(timeout=2.0)
        if proc.is_alive():
            proc.terminate()

# %%
await test_cross_process_compute();

# %% [markdown]
# ## Test try_recv

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_empty():
    """Test try_recv on empty queue."""
    ctx = mp.get_context("spawn")
    parent_channel, _ = create_queue_pair(ctx)
    result = await parent_channel.try_recv()
    assert result is None

# %%
await test_try_recv_empty();

# %% [markdown]
# ## Test Timeout

# %%
#|export
@pytest.mark.asyncio
async def test_recv_timeout():
    """Test recv timeout."""
    ctx = mp.get_context("spawn")
    parent_channel, _ = create_queue_pair(ctx)

    with pytest.raises(RecvTimeout):
        await parent_channel.recv(timeout=0.1)

# %%
await test_recv_timeout();

# %% [markdown]
# ## Test Channel Close

# %%
#|export
@pytest.mark.asyncio
async def test_close():
    """Test closing channel."""
    ctx = mp.get_context("spawn")
    parent_channel, _ = create_queue_pair(ctx)

    assert not parent_channel.is_closed
    await parent_channel.close()
    assert parent_channel.is_closed

# %%
await test_close();

# %%
#|export
@pytest.mark.asyncio
async def test_send_on_closed_raises():
    """Test sending on closed channel raises ChannelClosed."""
    ctx = mp.get_context("spawn")
    parent_channel, _ = create_queue_pair(ctx)
    await parent_channel.close()

    with pytest.raises(ChannelClosed):
        await parent_channel.send("test", "data")

# %%
await test_send_on_closed_raises();
