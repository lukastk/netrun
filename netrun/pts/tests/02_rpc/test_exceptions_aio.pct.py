# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Async RPC Exception Tests
#
# This notebook provides comprehensive tests and examples for all exception types
# that can occur in the Async RPC layer. The Async RPC uses `asyncio.Queue` for
# communication between async tasks within the same event loop.
#
# ## Exception Types
#
# The Async RPC can raise two types of exceptions:
#
# 1. **ChannelClosed**: The channel has been explicitly closed or received a shutdown signal
# 2. **RecvTimeout**: A receive operation timed out waiting for a message
#
# **Note**: `ChannelBroken` is not typically raised by AsyncChannel since `asyncio.Queue`
# operations are very robust within a single process/event loop.

# %%
#|default_exp rpc.test_exceptions_aio

# %%
#|export
import pytest
import asyncio
import time
from netrun.rpc.base import (
    ChannelClosed,
    ChannelBroken,
    RecvTimeout,
    RPCError,
)
from netrun.rpc.aio import (
    create_async_channel_pair,
)

# %% [markdown]
# ---
# # ChannelClosed Exception
#
# `ChannelClosed` is raised in the following scenarios:
#
# 1. **Explicit close**: Calling `send()`, `recv()`, or `try_recv()` after `close()`
# 2. **Shutdown signal**: Receiving the `RPC_KEY_SHUTDOWN` message from the other end
# 3. **Propagated close**: When the other side closes, causing shutdown signal to be received
#
# This is the most common exception and represents graceful channel termination.

# %% [markdown]
# ## 1.1 ChannelClosed on Explicit Close

# %%
#|export
@pytest.mark.asyncio
async def test_send_after_close():
    """AsyncChannel.send() raises ChannelClosed after close()."""
    ch_a, ch_b = create_async_channel_pair()

    await ch_a.close()
    assert ch_a.is_closed

    with pytest.raises(ChannelClosed) as exc_info:
        await ch_a.send("test", "data")

    assert "closed" in str(exc_info.value).lower()

# %%
await test_send_after_close()
print("Send after close: raises ChannelClosed as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_recv_after_close():
    """AsyncChannel.recv() raises ChannelClosed after close()."""
    ch_a, ch_b = create_async_channel_pair()

    await ch_a.close()

    with pytest.raises(ChannelClosed):
        await ch_a.recv()

# %%
await test_recv_after_close()
print("Recv after close: raises ChannelClosed as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_after_close():
    """AsyncChannel.try_recv() raises ChannelClosed after close()."""
    ch_a, ch_b = create_async_channel_pair()

    await ch_a.close()

    with pytest.raises(ChannelClosed):
        await ch_a.try_recv()

# %%
await test_try_recv_after_close()
print("Try_recv after close: raises ChannelClosed as expected")

# %% [markdown]
# ## 1.2 ChannelClosed on Shutdown Signal
#
# When one side closes the channel, it sends `RPC_KEY_SHUTDOWN` to the other side,
# which then raises `ChannelClosed` on the next recv() call.

# %%
#|export
@pytest.mark.asyncio
async def test_recv_receives_shutdown_from_close():
    """When ch_b closes, ch_a.recv() raises ChannelClosed via shutdown signal."""
    ch_a, ch_b = create_async_channel_pair()

    # Close ch_b - this sends shutdown to ch_a
    await ch_b.close()

    # ch_a should receive the shutdown signal
    with pytest.raises(ChannelClosed) as exc_info:
        await ch_a.recv(timeout=1.0)

    assert "shut down" in str(exc_info.value).lower()
    assert ch_a.is_closed

# %%
await test_recv_receives_shutdown_from_close()
print("Recv receives shutdown from close: raises ChannelClosed as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_receives_shutdown_from_close():
    """When ch_b closes, ch_a.try_recv() raises ChannelClosed via shutdown signal."""
    ch_a, ch_b = create_async_channel_pair()

    # Close ch_b - this sends shutdown to ch_a
    await ch_b.close()

    # Give a moment for the message to be available
    await asyncio.sleep(0.01)

    with pytest.raises(ChannelClosed):
        await ch_a.try_recv()

    assert ch_a.is_closed

# %%
await test_try_recv_receives_shutdown_from_close()
print("Try_recv receives shutdown from close: raises ChannelClosed as expected")

# %% [markdown]
# ## 1.3 ChannelClosed Propagation
#
# When one channel closes, the other receives the shutdown signal.

# %%
#|export
@pytest.mark.asyncio
async def test_close_propagates_to_other_channel():
    """When one channel closes, the other's recv() raises ChannelClosed."""
    ch_a, ch_b = create_async_channel_pair()

    # Close channel A
    await ch_a.close()

    # Channel B should receive the shutdown signal
    with pytest.raises(ChannelClosed):
        await ch_b.recv(timeout=1.0)

# %%
await test_close_propagates_to_other_channel()
print("Close propagates: other channel receives ChannelClosed")

# %%
#|export
@pytest.mark.asyncio
async def test_close_in_worker_task():
    """Worker task receives ChannelClosed when main closes."""
    ch_a, ch_b = create_async_channel_pair()

    worker_exception = []

    async def worker():
        try:
            while True:
                await ch_b.recv()
        except ChannelClosed as e:
            worker_exception.append(e)

    task = asyncio.create_task(worker())

    # Give worker time to start
    await asyncio.sleep(0.01)

    # Close from main
    await ch_a.close()

    await asyncio.wait_for(task, timeout=1.0)

    assert len(worker_exception) == 1
    assert isinstance(worker_exception[0], ChannelClosed)

# %%
await test_close_in_worker_task()
print("Close in worker task: worker receives ChannelClosed")

# %% [markdown]
# ## 1.4 Multiple Close Calls Are Safe

# %%
#|export
@pytest.mark.asyncio
async def test_multiple_close_is_safe():
    """Multiple close() calls are safe."""
    ch_a, ch_b = create_async_channel_pair()

    await ch_a.close()
    await ch_a.close()
    await ch_a.close()

    assert ch_a.is_closed

# %%
await test_multiple_close_is_safe()
print("Multiple close calls: safe, no exceptions")

# %% [markdown]
# ---
# # RecvTimeout Exception
#
# `RecvTimeout` is raised when `recv()` with a timeout parameter does not
# receive a message within the specified time.
#
# **Note**: `try_recv()` does NOT raise `RecvTimeout` - it returns `None` immediately.

# %% [markdown]
# ## 2.1 RecvTimeout Basics

# %%
#|export
@pytest.mark.asyncio
async def test_recv_timeout():
    """AsyncChannel.recv() raises RecvTimeout when timeout expires."""
    ch_a, ch_b = create_async_channel_pair()

    start = time.time()
    with pytest.raises(RecvTimeout) as exc_info:
        await ch_a.recv(timeout=0.1)
    elapsed = time.time() - start

    assert elapsed >= 0.1
    assert elapsed < 0.5
    assert "timed out" in str(exc_info.value).lower()

# %%
await test_recv_timeout()
print("Recv timeout: raises RecvTimeout after specified duration")

# %%
#|export
@pytest.mark.asyncio
async def test_recv_timeout_preserves_channel():
    """After RecvTimeout, the channel is still usable."""
    ch_a, ch_b = create_async_channel_pair()

    # First recv times out
    with pytest.raises(RecvTimeout):
        await ch_a.recv(timeout=0.05)

    # Channel should still be open
    assert not ch_a.is_closed

    # Can still send and receive
    await ch_b.send("hello", "world")
    key, data = await ch_a.recv(timeout=1.0)
    assert key == "hello"
    assert data == "world"

# %%
await test_recv_timeout_preserves_channel()
print("Recv timeout: channel remains usable after timeout")

# %% [markdown]
# ## 2.2 try_recv Does NOT Raise RecvTimeout

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_returns_none_not_timeout():
    """AsyncChannel.try_recv() returns None, never raises RecvTimeout."""
    ch_a, ch_b = create_async_channel_pair()

    result = await ch_a.try_recv()
    assert result is None

# %%
await test_try_recv_returns_none_not_timeout()
print("Try_recv: returns None (no RecvTimeout)")

# %% [markdown]
# ## 2.3 Various Timeout Values

# %%
#|export
@pytest.mark.asyncio
async def test_recv_various_timeouts():
    """Test various timeout values."""
    ch_a, ch_b = create_async_channel_pair()

    # Very short timeout
    with pytest.raises(RecvTimeout):
        await ch_a.recv(timeout=0.01)

    # Zero-ish timeout
    with pytest.raises(RecvTimeout):
        await ch_a.recv(timeout=0.001)

# %%
await test_recv_various_timeouts()
print("Various timeouts: all raise RecvTimeout correctly")

# %%
#|export
@pytest.mark.asyncio
async def test_recv_no_timeout_blocks():
    """AsyncChannel.recv() without timeout blocks until message arrives."""
    ch_a, ch_b = create_async_channel_pair()

    async def delayed_send():
        await asyncio.sleep(0.1)
        await ch_b.send("delayed", "message")

    task = asyncio.create_task(delayed_send())

    start = time.time()
    key, data = await ch_a.recv()  # No timeout
    elapsed = time.time() - start

    await task

    assert key == "delayed"
    assert data == "message"
    assert elapsed >= 0.1

# %%
await test_recv_no_timeout_blocks()
print("Recv without timeout: blocks until message arrives")

# %% [markdown]
# ---
# # Exception Hierarchy
#
# All async RPC exceptions inherit from `RPCError`:
#
# ```
# Exception
# └── RPCError
#     ├── ChannelClosed  - Graceful shutdown
#     ├── ChannelBroken  - Unexpected failure (rare for async)
#     └── RecvTimeout    - Timeout waiting for message
# ```

# %%
#|export
def test_exception_hierarchy():
    """Verify exception hierarchy is correct."""
    assert issubclass(ChannelClosed, RPCError)
    assert issubclass(ChannelBroken, RPCError)
    assert issubclass(RecvTimeout, RPCError)
    assert issubclass(RPCError, Exception)

    # Can catch all with RPCError
    for exc_class in [ChannelClosed, ChannelBroken, RecvTimeout]:
        try:
            raise exc_class("test")
        except RPCError:
            pass  # All should be caught

# %%
test_exception_hierarchy()
print("Exception hierarchy: all RPC exceptions inherit from RPCError")

# %% [markdown]
# ---
# # Practical Examples

# %% [markdown]
# ## Example: Worker Task Pattern

# %%
@pytest.mark.asyncio
async def example_worker_pattern():
    """Example: Proper exception handling in worker tasks."""
    print("=" * 50)
    print("Example: Worker Task Pattern")
    print("=" * 50)

    ch_a, ch_b = create_async_channel_pair()
    processed = []

    async def worker():
        """Worker that processes until channel closes."""
        print("  [Worker] Started")
        try:
            while True:
                key, data = await ch_b.recv()
                print(f"  [Worker] Processing: {key}={data}")
                processed.append(data)
                await ch_b.send(f"done:{key}", data * 2)
        except ChannelClosed:
            print("  [Worker] Channel closed, shutting down gracefully")

    task = asyncio.create_task(worker())

    # Send some work
    for i in range(3):
        await ch_a.send("task", i + 1)
        key, result = await ch_a.recv(timeout=1.0)
        print(f"  [Main] Got result: {result}")

    # Graceful shutdown
    await ch_a.close()
    await task

    assert processed == [1, 2, 3]
    print("Done!")

# %%
await example_worker_pattern()

# %% [markdown]
# ## Example: Bidirectional Communication

# %%
@pytest.mark.asyncio
async def example_bidirectional():
    """Example: Two tasks communicating bidirectionally."""
    print("=" * 50)
    print("Example: Bidirectional Communication")
    print("=" * 50)

    ch_a, ch_b = create_async_channel_pair()

    async def ping(channel, count):
        for i in range(count):
            await channel.send("ping", i)
            key, data = await channel.recv(timeout=1.0)
            print(f"  [Ping] Sent {i}, got {key}={data}")
        await channel.close()

    async def pong(channel):
        try:
            while True:
                key, data = await channel.recv(timeout=1.0)
                print(f"  [Pong] Got {key}={data}")
                await channel.send("pong", data * 10)
        except ChannelClosed:
            print("  [Pong] Channel closed")

    await asyncio.gather(
        ping(ch_a, 3),
        pong(ch_b),
    )

    print("Done!")

# %%
await example_bidirectional()

# %% [markdown]
# ## Example: Timeout Handling

# %%
@pytest.mark.asyncio
async def example_timeout_handling():
    """Example: Handling timeouts gracefully."""
    print("=" * 50)
    print("Example: Timeout Handling")
    print("=" * 50)

    ch_a, ch_b = create_async_channel_pair()

    async def slow_responder():
        key, data = await ch_b.recv()
        await asyncio.sleep(0.2)  # Slow processing
        await ch_b.send("response", data)

    task = asyncio.create_task(slow_responder())

    await ch_a.send("request", "hello")

    # First attempt times out
    try:
        await ch_a.recv(timeout=0.05)
        print("  Unexpected success!")
    except RecvTimeout:
        print("  First attempt timed out (expected)")

    # Second attempt succeeds with longer timeout
    key, data = await ch_a.recv(timeout=1.0)
    print(f"  Second attempt succeeded: {key}={data}")

    await task
    await ch_a.close()
    print("Done!")

# %%
await example_timeout_handling()
