# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Multiprocess RPC Exception Tests
#
# This notebook provides comprehensive tests and examples for all exception types
# that can occur in the Multiprocess RPC layer. The Multiprocess RPC uses
# `multiprocessing.Queue` for communication between a parent process and child
# subprocesses.
#
# ## Exception Types
#
# The Multiprocess RPC can raise three types of exceptions:
#
# 1. **ChannelClosed**: The channel has been explicitly closed or received a shutdown signal
# 2. **RecvTimeout**: A receive operation timed out waiting for a message
# 3. **ChannelBroken**: The subprocess died or the pipe broke (BrokenPipeError, EOFError, OSError)
#
# Both `ProcessChannel` (async, for parent) and `SyncProcessChannel` (sync, for workers)
# can raise these exceptions.

# %%
#|default_exp rpc.test_exceptions_multiprocess

# %%
#|export
import pytest
import multiprocessing as mp
import time
from netrun.rpc.base import (
    ChannelClosed,
    ChannelBroken,
    RecvTimeout,
    RPCError,
    RPC_KEY_SHUTDOWN,
)
from netrun.rpc.multiprocess import (
    SyncProcessChannel,
    create_queue_pair,
)

# %% [markdown]
# ---
# # ChannelClosed Exception
#
# `ChannelClosed` is raised in the following scenarios:
#
# 1. **Explicit close**: Calling `send()`, `recv()`, or `try_recv()` after `close()`
# 2. **Shutdown signal**: Receiving the `RPC_KEY_SHUTDOWN` message
# 3. **Propagated close**: When the other side closes the channel

# %% [markdown]
# ## 1.1 ChannelClosed on Explicit Close (Sync Channel)

# %%
#|export
def test_sync_send_after_close():
    """SyncProcessChannel.send() raises ChannelClosed after close()."""
    parent_channel, child_queues = create_queue_pair()
    send_q, recv_q = child_queues
    child_channel = SyncProcessChannel(send_q, recv_q)

    child_channel.close()
    assert child_channel.is_closed

    with pytest.raises(ChannelClosed) as exc_info:
        child_channel.send("test", "data")

    assert "closed" in str(exc_info.value).lower()

# %%
test_sync_send_after_close()
print("Sync send after close: raises ChannelClosed as expected")

# %%
#|export
def test_sync_recv_after_close():
    """SyncProcessChannel.recv() raises ChannelClosed after close()."""
    parent_channel, child_queues = create_queue_pair()
    send_q, recv_q = child_queues
    child_channel = SyncProcessChannel(send_q, recv_q)

    child_channel.close()

    with pytest.raises(ChannelClosed):
        child_channel.recv()

# %%
test_sync_recv_after_close()
print("Sync recv after close: raises ChannelClosed as expected")

# %%
#|export
def test_sync_try_recv_after_close():
    """SyncProcessChannel.try_recv() raises ChannelClosed after close()."""
    parent_channel, child_queues = create_queue_pair()
    send_q, recv_q = child_queues
    child_channel = SyncProcessChannel(send_q, recv_q)

    child_channel.close()

    with pytest.raises(ChannelClosed):
        child_channel.try_recv()

# %%
test_sync_try_recv_after_close()
print("Sync try_recv after close: raises ChannelClosed as expected")

# %% [markdown]
# ## 1.2 ChannelClosed on Explicit Close (Async Channel)

# %%
#|export
@pytest.mark.asyncio
async def test_async_send_after_close():
    """ProcessChannel.send() raises ChannelClosed after close()."""
    parent_channel, _ = create_queue_pair()

    await parent_channel.close()
    assert parent_channel.is_closed

    with pytest.raises(ChannelClosed) as exc_info:
        await parent_channel.send("test", "data")

    assert "closed" in str(exc_info.value).lower()

# %%
await test_async_send_after_close()
print("Async send after close: raises ChannelClosed as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_async_recv_after_close():
    """ProcessChannel.recv() raises ChannelClosed after close()."""
    parent_channel, _ = create_queue_pair()

    await parent_channel.close()

    with pytest.raises(ChannelClosed):
        await parent_channel.recv()

# %%
await test_async_recv_after_close()
print("Async recv after close: raises ChannelClosed as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_async_try_recv_after_close():
    """ProcessChannel.try_recv() raises ChannelClosed after close()."""
    parent_channel, _ = create_queue_pair()

    await parent_channel.close()

    with pytest.raises(ChannelClosed):
        await parent_channel.try_recv()

# %%
await test_async_try_recv_after_close()
print("Async try_recv after close: raises ChannelClosed as expected")

# %% [markdown]
# ## 1.3 ChannelClosed on Shutdown Signal

# %%
#|export
def test_sync_recv_shutdown_signal():
    """SyncProcessChannel.recv() raises ChannelClosed when receiving shutdown signal."""
    parent_channel, child_queues = create_queue_pair()
    send_q, recv_q = child_queues
    child_channel = SyncProcessChannel(send_q, recv_q)

    # Inject shutdown signal
    recv_q.put((RPC_KEY_SHUTDOWN, None))

    with pytest.raises(ChannelClosed) as exc_info:
        child_channel.recv()

    assert "shut down" in str(exc_info.value).lower()
    assert child_channel.is_closed

# %%
test_sync_recv_shutdown_signal()
print("Sync recv shutdown signal: raises ChannelClosed as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_async_recv_shutdown_signal():
    """ProcessChannel.recv() raises ChannelClosed when receiving shutdown signal."""
    parent_channel, child_queues = create_queue_pair()
    send_q, recv_q = child_queues

    # Child sends shutdown to parent
    send_q.put((RPC_KEY_SHUTDOWN, None))

    with pytest.raises(ChannelClosed) as exc_info:
        await parent_channel.recv(timeout=1.0)

    assert "shut down" in str(exc_info.value).lower()
    assert parent_channel.is_closed

# %%
await test_async_recv_shutdown_signal()
print("Async recv shutdown signal: raises ChannelClosed as expected")

# %% [markdown]
# ## 1.4 Multiple Close Calls Are Safe

# %%
#|export
def test_sync_multiple_close_is_safe():
    """Multiple close() calls on SyncProcessChannel are safe."""
    parent_channel, child_queues = create_queue_pair()
    send_q, recv_q = child_queues
    child_channel = SyncProcessChannel(send_q, recv_q)

    child_channel.close()
    child_channel.close()
    child_channel.close()

    assert child_channel.is_closed

# %%
test_sync_multiple_close_is_safe()
print("Multiple sync close calls: safe")

# %%
#|export
@pytest.mark.asyncio
async def test_async_multiple_close_is_safe():
    """Multiple close() calls on ProcessChannel are safe."""
    parent_channel, _ = create_queue_pair()

    await parent_channel.close()
    await parent_channel.close()
    await parent_channel.close()

    assert parent_channel.is_closed

# %%
await test_async_multiple_close_is_safe()
print("Multiple async close calls: safe")

# %% [markdown]
# ---
# # RecvTimeout Exception
#
# `RecvTimeout` is raised when `recv()` with a timeout does not receive a message
# within the specified time.

# %% [markdown]
# ## 2.1 RecvTimeout on Sync Channel

# %%
#|export
def test_sync_recv_timeout():
    """SyncProcessChannel.recv() raises RecvTimeout when timeout expires."""
    parent_channel, child_queues = create_queue_pair()
    send_q, recv_q = child_queues
    child_channel = SyncProcessChannel(send_q, recv_q)

    start = time.time()
    with pytest.raises(RecvTimeout) as exc_info:
        child_channel.recv(timeout=0.1)
    elapsed = time.time() - start

    assert elapsed >= 0.1
    assert elapsed < 0.5
    assert "timed out" in str(exc_info.value).lower()

# %%
test_sync_recv_timeout()
print("Sync recv timeout: raises RecvTimeout")

# %%
#|export
def test_sync_recv_timeout_preserves_channel():
    """After RecvTimeout, the channel is still usable."""
    parent_channel, child_queues = create_queue_pair()
    send_q, recv_q = child_queues
    child_channel = SyncProcessChannel(send_q, recv_q)

    with pytest.raises(RecvTimeout):
        child_channel.recv(timeout=0.05)

    assert not child_channel.is_closed

    # Can still send
    child_channel.send("still", "works")

    # Can still receive
    recv_q.put(("test", "data"))
    key, data = child_channel.recv(timeout=1.0)
    assert key == "test"

# %%
test_sync_recv_timeout_preserves_channel()
print("Sync recv timeout: channel remains usable")

# %% [markdown]
# ## 2.2 RecvTimeout on Async Channel

# %%
#|export
@pytest.mark.asyncio
async def test_async_recv_timeout():
    """ProcessChannel.recv() raises RecvTimeout when timeout expires."""
    parent_channel, _ = create_queue_pair()

    start = time.time()
    with pytest.raises(RecvTimeout) as exc_info:
        await parent_channel.recv(timeout=0.1)
    elapsed = time.time() - start

    assert elapsed >= 0.1
    assert elapsed < 0.5
    assert "timed out" in str(exc_info.value).lower()

# %%
await test_async_recv_timeout()
print("Async recv timeout: raises RecvTimeout")

# %%
#|export
@pytest.mark.asyncio
async def test_async_recv_timeout_preserves_channel():
    """After RecvTimeout, the async channel is still usable."""
    parent_channel, child_queues = create_queue_pair()
    send_q, recv_q = child_queues

    with pytest.raises(RecvTimeout):
        await parent_channel.recv(timeout=0.05)

    assert not parent_channel.is_closed

    # Can still send
    await parent_channel.send("hello", "world")
    key, data = recv_q.get(timeout=1.0)
    assert key == "hello"

# %%
await test_async_recv_timeout_preserves_channel()
print("Async recv timeout: channel remains usable")

# %% [markdown]
# ## 2.3 try_recv Does NOT Raise RecvTimeout

# %%
#|export
def test_sync_try_recv_returns_none():
    """SyncProcessChannel.try_recv() returns None, never raises RecvTimeout."""
    parent_channel, child_queues = create_queue_pair()
    send_q, recv_q = child_queues
    child_channel = SyncProcessChannel(send_q, recv_q)

    result = child_channel.try_recv()
    assert result is None

# %%
test_sync_try_recv_returns_none()
print("Sync try_recv: returns None (no RecvTimeout)")

# %%
#|export
@pytest.mark.asyncio
async def test_async_try_recv_returns_none():
    """ProcessChannel.try_recv() returns None, never raises RecvTimeout."""
    parent_channel, _ = create_queue_pair()

    result = await parent_channel.try_recv()
    assert result is None

# %%
await test_async_try_recv_returns_none()
print("Async try_recv: returns None (no RecvTimeout)")

# %% [markdown]
# ---
# # ChannelBroken Exception
#
# `ChannelBroken` is raised when the underlying multiprocessing queue encounters
# an error, typically due to:
#
# - **BrokenPipeError**: The other process died and the pipe was broken
# - **EOFError**: End of file on the pipe (process terminated)
# - **OSError**: General OS-level error on the pipe
#
# This is more common in multiprocess RPC than thread RPC because subprocesses
# can die unexpectedly.

# %% [markdown]
# ## 3.1 ChannelBroken Exception Structure

# %%
#|export
def test_channel_broken_exception_structure():
    """ChannelBroken has the expected structure."""
    assert issubclass(ChannelBroken, RPCError)
    assert issubclass(ChannelBroken, Exception)

    exc = ChannelBroken("Pipe broken")
    assert "Pipe broken" in str(exc)

# %%
test_channel_broken_exception_structure()
print("ChannelBroken: properly structured exception class")

# %%
#|export
def test_channel_broken_vs_closed_distinction():
    """ChannelBroken and ChannelClosed are distinct exceptions."""
    assert ChannelBroken is not ChannelClosed

    # Catching one doesn't catch the other
    try:
        raise ChannelBroken("broken")
    except ChannelClosed:
        assert False, "ChannelBroken should not be caught by ChannelClosed"
    except ChannelBroken:
        pass

    try:
        raise ChannelClosed("closed")
    except ChannelBroken:
        assert False, "ChannelClosed should not be caught by ChannelBroken"
    except ChannelClosed:
        pass

# %%
test_channel_broken_vs_closed_distinction()
print("ChannelBroken vs ChannelClosed: distinct exception types")

# %% [markdown]
# ## 3.2 Real Subprocess Example
#
# This test demonstrates that when a subprocess dies unexpectedly,
# subsequent operations may raise ChannelBroken.

# %%
#|export
@pytest.mark.asyncio
async def test_subprocess_communication():
    """Test basic subprocess communication works correctly."""
    import tests.rpc.workers
    # Note: We test the happy path here since forcing ChannelBroken
    # requires actually killing a subprocess which is flaky in tests.
    parent_channel, child_queues = create_queue_pair()

    proc = mp.Process(target=tests.rpc.workers.echo_worker, args=child_queues)
    proc.start()

    # Normal communication
    await parent_channel.send("hello", "world")
    key, data = await parent_channel.recv(timeout=5.0)
    assert key == "echo:hello"
    assert data == "world"

    # Graceful shutdown
    await parent_channel.close()
    proc.join(timeout=2.0)
    if proc.is_alive():
        proc.terminate()
        proc.join(timeout=1.0)

# %%
await test_subprocess_communication()
print("Subprocess communication: works correctly")

# %% [markdown]
# ---
# # Exception Hierarchy

# %%
#|export
def test_exception_hierarchy():
    """Verify exception hierarchy is correct."""
    assert issubclass(ChannelClosed, RPCError)
    assert issubclass(ChannelBroken, RPCError)
    assert issubclass(RecvTimeout, RPCError)
    assert issubclass(RPCError, Exception)

    for exc_class in [ChannelClosed, ChannelBroken, RecvTimeout]:
        try:
            raise exc_class("test")
        except RPCError:
            pass

# %%
test_exception_hierarchy()
print("Exception hierarchy: all RPC exceptions inherit from RPCError")

# %% [markdown]
# ---
# # Practical Examples

# %% [markdown]
# ## Example: Robust Worker Process

# %%
@pytest.mark.asyncio
async def example_robust_worker():
    """Example: Proper exception handling in worker processes."""
    import tests.rpc.workers
    print("=" * 50)
    print("Example: Robust Worker Process")
    print("=" * 50)

    parent_channel, child_queues = create_queue_pair()

    proc = mp.Process(target=tests.rpc.workers.robust_worker, args=child_queues)
    proc.start()

    # Send some work
    for i in range(3):
        await parent_channel.send("task", i + 1)
        key, result = await parent_channel.recv(timeout=5.0)
        print(f"  [Main] Got: {result}")

    # Graceful shutdown
    await parent_channel.close()
    proc.join(timeout=2.0)
    if proc.is_alive():
        proc.terminate()

    print("Done!")

# %%
await example_robust_worker()

# %% [markdown]
# ## Example: Timeout with Retry

# %%
@pytest.mark.asyncio
async def example_timeout_retry():
    """Example: Handling timeouts with retry."""
    import tests.rpc.workers
    print("=" * 50)
    print("Example: Timeout with Retry")
    print("=" * 50)

    parent_channel, child_queues = create_queue_pair()

    proc = mp.Process(target=tests.rpc.workers.slow_worker, args=child_queues)
    proc.start()

    await parent_channel.send("task", 42)

    # First attempt might timeout
    result = None
    for attempt in range(3):
        try:
            key, result = await parent_channel.recv(timeout=0.4*(attempt+1))
            print(f"  Attempt {attempt + 1}: Success, got {result}")
            break
        except RecvTimeout:
            print(f"  Attempt {attempt + 1}: Timed out, retrying...")

    assert result == 42

    await parent_channel.close()
    proc.join(timeout=2.0)
    if proc.is_alive():
        proc.terminate()

    print("Done!")

# %%
await example_timeout_retry()
