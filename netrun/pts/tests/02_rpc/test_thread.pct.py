# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for ThreadChannel (RPC Thread Module)

# %%
#|default_exp rpc.test_thread

# %%
#|export
import pytest
import asyncio
import threading
from netrun.rpc.base import ChannelClosed, RecvTimeout
from netrun.rpc.thread import (
    ThreadChannel,
    SyncThreadChannel,
    create_thread_channel_pair,
)

# %% [markdown]
# ## Test Channel Creation

# %%
#|export
@pytest.mark.asyncio
async def test_create_channel_pair():
    """Test creating a thread channel pair."""
    parent_channel, child_queues = create_thread_channel_pair()
    assert isinstance(parent_channel, ThreadChannel)
    assert isinstance(child_queues, tuple)
    assert len(child_queues) == 2

# %%
await test_create_channel_pair();

# %%
#|export
def test_sync_channel_creation():
    """Test creating a SyncThreadChannel."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)
    assert isinstance(child_channel, SyncThreadChannel)
    assert not child_channel.is_closed

# %%
test_sync_channel_creation();

# %% [markdown]
# ## Test Basic Send/Recv Between Threads

# %%
#|export
@pytest.mark.asyncio
async def test_async_to_sync_communication():
    """Test sending from async (parent) to sync (child) thread."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    received = []

    def worker():
        key, data = child_channel.recv()
        received.append((key, data))

    thread = threading.Thread(target=worker)
    thread.start()

    await parent_channel.send("test", {"value": 42})
    thread.join(timeout=2.0)

    assert len(received) == 1
    assert received[0] == ("test", {"value": 42})

# %%
await test_async_to_sync_communication();

# %%
#|export
@pytest.mark.asyncio
async def test_sync_to_async_communication():
    """Test sending from sync (child) thread to async (parent)."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    def worker():
        child_channel.send("response", "hello from thread")

    thread = threading.Thread(target=worker)
    thread.start()

    key, data = await parent_channel.recv(timeout=2.0)
    thread.join(timeout=2.0)

    assert key == "response"
    assert data == "hello from thread"

# %%
await test_sync_to_async_communication();

# %%
#|export
@pytest.mark.asyncio
async def test_bidirectional_thread_communication():
    """Test bidirectional communication between threads."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    results = []

    def worker():
        # Receive from parent
        key, data = child_channel.recv()
        results.append(("received", key, data))
        # Send response
        child_channel.send("echo", data * 2)

    thread = threading.Thread(target=worker)
    thread.start()

    await parent_channel.send("number", 21)
    key, data = await parent_channel.recv(timeout=2.0)
    thread.join(timeout=2.0)

    assert results == [("received", "number", 21)]
    assert key == "echo"
    assert data == 42

# %%
await test_bidirectional_thread_communication();

# %% [markdown]
# ## Test Multiple Messages

# %%
#|export
@pytest.mark.asyncio
async def test_multiple_messages():
    """Test sending multiple messages between threads."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    received_in_thread = []

    def worker():
        for _ in range(3):
            key, data = child_channel.recv()
            received_in_thread.append((key, data))
            child_channel.send(f"ack:{key}", data)

    thread = threading.Thread(target=worker)
    thread.start()

    # Send from parent
    for i in range(3):
        await parent_channel.send(f"msg{i}", i)

    # Receive acks
    received_in_parent = []
    for _ in range(3):
        key, data = await parent_channel.recv(timeout=2.0)
        received_in_parent.append((key, data))

    thread.join(timeout=2.0)

    assert len(received_in_thread) == 3
    assert len(received_in_parent) == 3

# %%
await test_multiple_messages();

# %% [markdown]
# ## Test try_recv

# %%
#|export
@pytest.mark.asyncio
async def test_async_try_recv_empty():
    """Test async try_recv on empty queue."""
    parent_channel, _ = create_thread_channel_pair()
    result = await parent_channel.try_recv()
    assert result is None

# %%
await test_async_try_recv_empty();

# %%
#|export
def test_sync_try_recv_empty():
    """Test sync try_recv on empty queue."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)
    result = child_channel.try_recv()
    assert result is None

# %%
test_sync_try_recv_empty();

# %%
#|export
@pytest.mark.asyncio
async def test_async_try_recv_with_message():
    """Test async try_recv with pending message."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    child_channel.send("test", "data")
    # Small delay for queue
    await asyncio.sleep(0.01)
    result = await parent_channel.try_recv()

    assert result is not None
    key, data = result
    assert key == "test"
    assert data == "data"

# %%
await test_async_try_recv_with_message();

# %% [markdown]
# ## Test Timeout

# %%
#|export
@pytest.mark.asyncio
async def test_async_recv_timeout():
    """Test async recv timeout."""
    parent_channel, _ = create_thread_channel_pair()

    with pytest.raises(RecvTimeout):
        await parent_channel.recv(timeout=0.1)

# %%
await test_async_recv_timeout();

# %%
#|export
def test_sync_recv_timeout():
    """Test sync recv timeout."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    with pytest.raises(RecvTimeout):
        child_channel.recv(timeout=0.1)

# %%
test_sync_recv_timeout();

# %% [markdown]
# ## Test Channel Close

# %%
#|export
@pytest.mark.asyncio
async def test_async_close():
    """Test closing async channel."""
    parent_channel, _ = create_thread_channel_pair()

    assert not parent_channel.is_closed
    await parent_channel.close()
    assert parent_channel.is_closed

# %%
await test_async_close();

# %%
#|export
def test_sync_close():
    """Test closing sync channel."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    assert not child_channel.is_closed
    child_channel.close()
    assert child_channel.is_closed

# %%
test_sync_close();

# %%
#|export
@pytest.mark.asyncio
async def test_async_send_on_closed_raises():
    """Test sending on closed async channel raises ChannelClosed."""
    parent_channel, _ = create_thread_channel_pair()
    await parent_channel.close()

    with pytest.raises(ChannelClosed):
        await parent_channel.send("test", "data")

# %%
await test_async_send_on_closed_raises();

# %%
#|export
def test_sync_send_on_closed_raises():
    """Test sending on closed sync channel raises ChannelClosed."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)
    child_channel.close()

    with pytest.raises(ChannelClosed):
        child_channel.send("test", "data")

# %%
test_sync_send_on_closed_raises();

# %% [markdown]
# ## Test Shutdown Signal

# %%
#|export
@pytest.mark.asyncio
async def test_shutdown_signal_closes_sync_channel():
    """Test that shutdown signal from async side closes sync channel."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    closed_in_thread = []

    def worker():
        try:
            while True:
                child_channel.recv()
        except ChannelClosed:
            closed_in_thread.append(True)

    thread = threading.Thread(target=worker)
    thread.start()

    await asyncio.sleep(0.05)  # Let thread start
    await parent_channel.close()
    thread.join(timeout=2.0)

    assert closed_in_thread == [True]

# %%
await test_shutdown_signal_closes_sync_channel();
