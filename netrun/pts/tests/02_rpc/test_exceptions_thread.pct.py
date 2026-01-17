# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Thread RPC Exception Tests
#
# This notebook provides comprehensive tests and examples for all exception types
# that can occur in the Thread RPC layer. The Thread RPC uses `queue.Queue` for
# communication between an async parent (main thread) and sync workers (child threads).
#
# ## Exception Types
#
# The Thread RPC can raise three types of exceptions:
#
# 1. **ChannelClosed**: The channel has been explicitly closed or received a shutdown signal
# 2. **RecvTimeout**: A receive operation timed out waiting for a message
# 3. **ChannelBroken**: The underlying queue encountered an unexpected error
#
# Both `ThreadChannel` (async, for parent) and `SyncThreadChannel` (sync, for workers)
# can raise these exceptions.

# %%
#|default_exp rpc.test_exceptions_thread

# %%
#|export
import pytest
import asyncio
import threading
import time
from netrun.rpc.base import (
    ChannelClosed,
    ChannelBroken,
    RecvTimeout,
    RPC_KEY_SHUTDOWN,
)
from netrun.rpc.thread import (
    SyncThreadChannel,
    create_thread_channel_pair,
)

# %% [markdown]
# ---
# # ChannelClosed Exception
#
# `ChannelClosed` is raised in the following scenarios:
#
# 1. **Explicit close**: Calling `send()`, `recv()`, or `try_recv()` on a channel after `close()` was called
# 2. **Shutdown signal**: Receiving the `RPC_KEY_SHUTDOWN` message from the other end
# 3. **Propagated close**: When the other side closes the channel, causing shutdown signal to be received
#
# This is the most common exception and represents graceful channel termination.

# %% [markdown]
# ## 1.1 ChannelClosed on Explicit Close (Sync Channel)
#
# When `close()` is called on a `SyncThreadChannel`, subsequent operations raise `ChannelClosed`.

# %%
#|export
def test_sync_send_after_close():
    """SyncThreadChannel.send() raises ChannelClosed after close()."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    # Close the channel
    child_channel.close()
    assert child_channel.is_closed

    # Attempting to send raises ChannelClosed
    with pytest.raises(ChannelClosed) as exc_info:
        child_channel.send("test", "data")

    assert "closed" in str(exc_info.value).lower()

# %%
test_sync_send_after_close()
print("Sync send after close: raises ChannelClosed as expected")

# %%
#|export
def test_sync_recv_after_close():
    """SyncThreadChannel.recv() raises ChannelClosed after close()."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    child_channel.close()

    with pytest.raises(ChannelClosed):
        child_channel.recv()

# %%
test_sync_recv_after_close()
print("Sync recv after close: raises ChannelClosed as expected")

# %%
#|export
def test_sync_try_recv_after_close():
    """SyncThreadChannel.try_recv() raises ChannelClosed after close()."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    child_channel.close()

    with pytest.raises(ChannelClosed):
        child_channel.try_recv()

# %%
test_sync_try_recv_after_close()
print("Sync try_recv after close: raises ChannelClosed as expected")

# %% [markdown]
# ## 1.2 ChannelClosed on Explicit Close (Async Channel)
#
# The same behavior applies to `ThreadChannel` (async version).

# %%
#|export
@pytest.mark.asyncio
async def test_async_send_after_close():
    """ThreadChannel.send() raises ChannelClosed after close()."""
    parent_channel, _ = create_thread_channel_pair()

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
    """ThreadChannel.recv() raises ChannelClosed after close()."""
    parent_channel, _ = create_thread_channel_pair()

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
    """ThreadChannel.try_recv() raises ChannelClosed after close()."""
    parent_channel, _ = create_thread_channel_pair()

    await parent_channel.close()

    with pytest.raises(ChannelClosed):
        await parent_channel.try_recv()

# %%
await test_async_try_recv_after_close()
print("Async try_recv after close: raises ChannelClosed as expected")

# %% [markdown]
# ## 1.3 ChannelClosed on Shutdown Signal
#
# When one side sends `RPC_KEY_SHUTDOWN`, the receiving side raises `ChannelClosed`
# on the next `recv()` or `try_recv()` call. This enables graceful shutdown coordination.

# %%
#|export
def test_sync_recv_shutdown_signal():
    """SyncThreadChannel.recv() raises ChannelClosed when receiving shutdown signal."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    # Manually inject shutdown signal into the queue
    # (This is what happens internally when parent calls close())
    recv_q.put((RPC_KEY_SHUTDOWN, None))

    # Receiving the shutdown signal raises ChannelClosed
    with pytest.raises(ChannelClosed) as exc_info:
        child_channel.recv()

    assert "shut down" in str(exc_info.value).lower()
    assert child_channel.is_closed

# %%
test_sync_recv_shutdown_signal()
print("Sync recv shutdown signal: raises ChannelClosed as expected")

# %%
#|export
def test_sync_try_recv_shutdown_signal():
    """SyncThreadChannel.try_recv() raises ChannelClosed when receiving shutdown signal."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    recv_q.put((RPC_KEY_SHUTDOWN, None))

    with pytest.raises(ChannelClosed):
        child_channel.try_recv()

    assert child_channel.is_closed

# %%
test_sync_try_recv_shutdown_signal()
print("Sync try_recv shutdown signal: raises ChannelClosed as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_async_recv_shutdown_signal():
    """ThreadChannel.recv() raises ChannelClosed when receiving shutdown signal."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    # Child sends shutdown to parent
    child_channel._send_queue.put((RPC_KEY_SHUTDOWN, None))

    with pytest.raises(ChannelClosed) as exc_info:
        await parent_channel.recv()

    assert "shut down" in str(exc_info.value).lower()
    assert parent_channel.is_closed

# %%
await test_async_recv_shutdown_signal()
print("Async recv shutdown signal: raises ChannelClosed as expected")

# %% [markdown]
# ## 1.4 ChannelClosed Propagation Between Threads
#
# When the parent closes the channel, the worker thread receives the shutdown signal
# and raises `ChannelClosed`. This is the typical graceful shutdown pattern.

# %%
#|export
@pytest.mark.asyncio
async def test_parent_close_propagates_to_worker():
    """When parent closes channel, worker's recv() raises ChannelClosed."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    worker_exception = []

    def worker():
        try:
            # This will block until shutdown signal arrives
            child_channel.recv()
        except ChannelClosed as e:
            worker_exception.append(e)

    thread = threading.Thread(target=worker)
    thread.start()

    # Give thread time to start and block on recv
    await asyncio.sleep(0.05)

    # Close from parent side
    await parent_channel.close()

    thread.join(timeout=2.0)

    assert len(worker_exception) == 1
    assert isinstance(worker_exception[0], ChannelClosed)

# %%
await test_parent_close_propagates_to_worker()
print("Parent close propagates to worker: worker receives ChannelClosed")

# %%
#|export
@pytest.mark.asyncio
async def test_worker_close_propagates_to_parent():
    """When worker closes channel, parent's recv() raises ChannelClosed."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    def worker():
        # Close from worker side
        child_channel.close()

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(timeout=2.0)

    # Parent should receive the shutdown signal
    with pytest.raises(ChannelClosed):
        await parent_channel.recv(timeout=1.0)

# %%
await test_worker_close_propagates_to_parent()
print("Worker close propagates to parent: parent receives ChannelClosed")

# %% [markdown]
# ## 1.5 Multiple Close Calls Are Safe
#
# Calling `close()` multiple times is safe and does not raise exceptions.

# %%
#|export
def test_sync_multiple_close_is_safe():
    """Multiple close() calls on SyncThreadChannel are safe."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    # First close
    child_channel.close()
    assert child_channel.is_closed

    # Second close - should not raise
    child_channel.close()
    assert child_channel.is_closed

    # Third close - still safe
    child_channel.close()

# %%
test_sync_multiple_close_is_safe()
print("Multiple sync close calls: safe, no exceptions")

# %%
#|export
@pytest.mark.asyncio
async def test_async_multiple_close_is_safe():
    """Multiple close() calls on ThreadChannel are safe."""
    parent_channel, _ = create_thread_channel_pair()

    await parent_channel.close()
    await parent_channel.close()
    await parent_channel.close()

    assert parent_channel.is_closed

# %%
await test_async_multiple_close_is_safe()
print("Multiple async close calls: safe, no exceptions")

# %% [markdown]
# ---
# # RecvTimeout Exception
#
# `RecvTimeout` is raised when a `recv()` call with a timeout parameter
# does not receive a message within the specified time.
#
# **Note**: `try_recv()` does NOT raise `RecvTimeout` - it returns `None` immediately
# if no message is available.

# %% [markdown]
# ## 2.1 RecvTimeout on Sync Channel

# %%
#|export
def test_sync_recv_timeout():
    """SyncThreadChannel.recv() raises RecvTimeout when timeout expires."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    start = time.time()
    with pytest.raises(RecvTimeout) as exc_info:
        child_channel.recv(timeout=0.1)
    elapsed = time.time() - start

    # Should have waited approximately the timeout duration
    assert elapsed >= 0.1
    assert elapsed < 0.5  # But not too long
    assert "timed out" in str(exc_info.value).lower()

# %%
test_sync_recv_timeout()
print("Sync recv timeout: raises RecvTimeout after specified duration")

# %%
#|export
def test_sync_recv_timeout_preserves_channel():
    """After RecvTimeout, the channel is still usable."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    # First recv times out
    with pytest.raises(RecvTimeout):
        child_channel.recv(timeout=0.05)

    # Channel should still be open
    assert not child_channel.is_closed

    # Can still send
    child_channel.send("still", "works")

    # Can still receive (if message is available)
    recv_q.put(("test", "data"))
    key, data = child_channel.recv(timeout=1.0)
    assert key == "test"
    assert data == "data"

# %%
test_sync_recv_timeout_preserves_channel()
print("Sync recv timeout: channel remains usable after timeout")

# %% [markdown]
# ## 2.2 RecvTimeout on Async Channel

# %%
#|export
@pytest.mark.asyncio
async def test_async_recv_timeout():
    """ThreadChannel.recv() raises RecvTimeout when timeout expires."""
    parent_channel, _ = create_thread_channel_pair()

    start = time.time()
    with pytest.raises(RecvTimeout) as exc_info:
        await parent_channel.recv(timeout=0.1)
    elapsed = time.time() - start

    assert elapsed >= 0.1
    assert elapsed < 0.5
    assert "timed out" in str(exc_info.value).lower()

# %%
await test_async_recv_timeout()
print("Async recv timeout: raises RecvTimeout after specified duration")

# %%
#|export
@pytest.mark.asyncio
async def test_async_recv_timeout_preserves_channel():
    """After RecvTimeout, the async channel is still usable."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    # First recv times out
    with pytest.raises(RecvTimeout):
        await parent_channel.recv(timeout=0.05)

    # Channel should still be open
    assert not parent_channel.is_closed

    # Can still send and receive
    await parent_channel.send("hello", "world")
    key, data = child_channel.recv(timeout=1.0)
    assert key == "hello"

# %%
await test_async_recv_timeout_preserves_channel()
print("Async recv timeout: channel remains usable after timeout")

# %% [markdown]
# ## 2.3 try_recv Does NOT Raise RecvTimeout
#
# Unlike `recv()`, `try_recv()` is non-blocking and returns `None` immediately
# if no message is available.

# %%
#|export
def test_sync_try_recv_returns_none_not_timeout():
    """SyncThreadChannel.try_recv() returns None, never raises RecvTimeout."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    # Should return None immediately, not raise
    result = child_channel.try_recv()
    assert result is None

# %%
test_sync_try_recv_returns_none_not_timeout()
print("Sync try_recv: returns None (no RecvTimeout)")

# %%
#|export
@pytest.mark.asyncio
async def test_async_try_recv_returns_none_not_timeout():
    """ThreadChannel.try_recv() returns None, never raises RecvTimeout."""
    parent_channel, _ = create_thread_channel_pair()

    result = await parent_channel.try_recv()
    assert result is None

# %%
await test_async_try_recv_returns_none_not_timeout()
print("Async try_recv: returns None (no RecvTimeout)")

# %% [markdown]
# ## 2.4 Timeout Values

# %%
#|export
def test_sync_recv_various_timeouts():
    """Test various timeout values for SyncThreadChannel.recv()."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    # Very short timeout
    with pytest.raises(RecvTimeout):
        child_channel.recv(timeout=0.01)

    # Zero timeout - should timeout immediately
    with pytest.raises(RecvTimeout):
        child_channel.recv(timeout=0.001)

# %%
test_sync_recv_various_timeouts()
print("Sync recv various timeouts: all raise RecvTimeout correctly")

# %%
#|export
@pytest.mark.asyncio
async def test_async_recv_no_timeout_blocks():
    """ThreadChannel.recv() without timeout blocks until message arrives."""
    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    # Send a message after a delay
    def delayed_send():
        time.sleep(0.1)
        child_channel.send("delayed", "message")

    thread = threading.Thread(target=delayed_send)
    thread.start()

    # This should block until the message arrives (no timeout)
    start = time.time()
    key, data = await parent_channel.recv()  # No timeout = blocks indefinitely
    elapsed = time.time() - start

    thread.join()

    assert key == "delayed"
    assert data == "message"
    assert elapsed >= 0.1  # Should have waited for the delayed send

# %%
await test_async_recv_no_timeout_blocks()
print("Async recv without timeout: blocks until message arrives")

# %% [markdown]
# ---
# # ChannelBroken Exception
#
# `ChannelBroken` is raised when the underlying queue encounters an unexpected error.
# This is rare in practice since `queue.Queue` is very robust, but the exception
# exists to handle edge cases.
#
# **When ChannelBroken occurs:**
# - Unexpected exceptions during queue operations
# - Queue corruption (extremely rare)
#
# **Note**: `ChannelBroken` is distinct from `ChannelClosed`:
# - `ChannelClosed` = graceful shutdown (expected)
# - `ChannelBroken` = unexpected failure (error condition)

# %% [markdown]
# ## 3.1 Demonstrating ChannelBroken Concept
#
# Since `queue.Queue` rarely fails, we'll demonstrate the concept by showing
# how the exception is structured and when it would be raised.

# %%
#|export
def test_channel_broken_exception_structure():
    """ChannelBroken has the expected structure."""
    from netrun.rpc.base import RPCError

    # ChannelBroken is an RPCError
    assert issubclass(ChannelBroken, RPCError)
    assert issubclass(ChannelBroken, Exception)

    # Can be raised with a message
    exc = ChannelBroken("Connection lost unexpectedly")
    assert "Connection lost" in str(exc)

# %%
test_channel_broken_exception_structure()
print("ChannelBroken: properly structured exception class")

# %%
#|export
def test_channel_broken_vs_closed_distinction():
    """ChannelBroken and ChannelClosed are distinct exceptions."""
    from netrun.rpc.base import ChannelClosed

    # They are different exception types
    assert ChannelBroken is not ChannelClosed

    # Catching one doesn't catch the other
    try:
        raise ChannelBroken("broken")
    except ChannelClosed:
        assert False, "ChannelBroken should not be caught by ChannelClosed"
    except ChannelBroken:
        pass  # Expected

    try:
        raise ChannelClosed("closed")
    except ChannelBroken:
        assert False, "ChannelClosed should not be caught by ChannelBroken"
    except ChannelClosed:
        pass  # Expected

# %%
test_channel_broken_vs_closed_distinction()
print("ChannelBroken vs ChannelClosed: distinct exception types")

# %% [markdown]
# ## 3.2 Handling ChannelBroken in Worker Pattern
#
# When implementing workers, it's good practice to handle both `ChannelClosed`
# (graceful shutdown) and `ChannelBroken` (unexpected failure) separately.

# %%
#|export
@pytest.mark.asyncio
async def test_worker_exception_handling_pattern():
    """Demonstrate proper exception handling pattern for workers."""

    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    worker_status = {"shutdown_reason": None}

    def robust_worker():
        """Worker that handles all exception types properly."""
        try:
            while True:
                key, data = child_channel.recv()
                child_channel.send(f"echo:{key}", data)
        except ChannelClosed:
            worker_status["shutdown_reason"] = "graceful"
        except ChannelBroken as e:
            worker_status["shutdown_reason"] = f"broken: {e}"
        except Exception as e:
            worker_status["shutdown_reason"] = f"unexpected: {e}"

    thread = threading.Thread(target=robust_worker)
    thread.start()

    # Normal operation
    await parent_channel.send("hello", "world")
    key, data = await parent_channel.recv(timeout=1.0)
    assert key == "echo:hello"

    # Graceful shutdown
    await parent_channel.close()
    thread.join(timeout=2.0)

    assert worker_status["shutdown_reason"] == "graceful"

# %%
await test_worker_exception_handling_pattern()
print("Worker exception handling: properly distinguishes shutdown reasons")

# %% [markdown]
# ---
# # Exception Hierarchy Summary
#
# All thread RPC exceptions inherit from `RPCError`:
#
# ```
# Exception
# └── RPCError
#     ├── ChannelClosed  - Graceful shutdown
#     ├── ChannelBroken  - Unexpected failure
#     └── RecvTimeout    - Timeout waiting for message
# ```

# %%
#|export
def test_exception_hierarchy():
    """Verify exception hierarchy is correct."""
    from netrun.rpc.base import RPCError, ChannelClosed, RecvTimeout

    # All are RPCErrors
    assert issubclass(ChannelClosed, RPCError)
    assert issubclass(ChannelBroken, RPCError)
    assert issubclass(RecvTimeout, RPCError)

    # RPCError is an Exception
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
#
# These examples show real-world patterns for handling exceptions in thread RPC.

# %% [markdown]
# ## Example: Retry Pattern with Timeout

# %%
@pytest.mark.asyncio
async def example_retry_with_timeout():
    """Example: Implementing retry logic with timeouts."""
    print("=" * 50)
    print("Example: Retry Pattern with Timeout")
    print("=" * 50)

    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    def slow_worker():
        """Worker that sometimes responds slowly."""
        try:
            while True:
                key, data = child_channel.recv()
                if data.get("delay", 0) > 0:
                    time.sleep(data["delay"])
                child_channel.send("result", {"processed": data["value"]})
        except ChannelClosed:
            pass

    thread = threading.Thread(target=slow_worker)
    thread.start()

    async def send_with_retry(value, timeout=0.1, max_retries=3):
        """Send a request and retry on timeout."""
        for attempt in range(max_retries):
            try:
                await parent_channel.send("request", {"value": value, "delay": 0})
                key, data = await parent_channel.recv(timeout=timeout)
                return data
            except RecvTimeout:
                print(f"  Attempt {attempt + 1} timed out, retrying...")
        raise RecvTimeout(f"Failed after {max_retries} attempts")

    # This should succeed
    result = await send_with_retry("test", timeout=1.0)
    print(f"Success: {result}")

    await parent_channel.close()
    thread.join(timeout=2.0)
    print("Done!")

# %%
await example_retry_with_timeout()

# %% [markdown]
# ## Example: Graceful Shutdown with Pending Work

# %%
@pytest.mark.asyncio
async def example_graceful_shutdown():
    """Example: Graceful shutdown handling pending work."""
    print("=" * 50)
    print("Example: Graceful Shutdown")
    print("=" * 50)

    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    processed_items = []

    def worker():
        """Worker that processes items until shutdown."""
        try:
            while True:
                key, data = child_channel.recv()
                processed_items.append(data)
                child_channel.send("done", data)
        except ChannelClosed:
            print(f"  Worker: Received shutdown, processed {len(processed_items)} items")

    thread = threading.Thread(target=worker)
    thread.start()

    # Send some work
    for i in range(5):
        await parent_channel.send("work", f"item-{i}")

    # Collect responses
    for _ in range(5):
        await parent_channel.recv(timeout=1.0)

    print("  Main: Sent 5 items, received 5 responses")

    # Graceful shutdown
    await parent_channel.close()
    thread.join(timeout=2.0)

    assert len(processed_items) == 5
    print("Done!")

# %%
await example_graceful_shutdown()

# %% [markdown]
# ## Example: Distinguishing Exception Types

# %%
@pytest.mark.asyncio
async def example_exception_handling():
    """Example: Comprehensive exception handling."""
    print("=" * 50)
    print("Example: Exception Handling")
    print("=" * 50)

    from netrun.rpc.base import RPCError

    parent_channel, child_queues = create_thread_channel_pair()
    send_q, recv_q = child_queues
    child_channel = SyncThreadChannel(send_q, recv_q)

    def demo_exceptions():
        # Demonstrate different exception scenarios

        # 1. RecvTimeout
        print("  Testing RecvTimeout...")
        try:
            child_channel.recv(timeout=0.05)
        except RecvTimeout:
            print("    Caught RecvTimeout")

        # 2. ChannelClosed (via explicit close)
        print("  Testing ChannelClosed...")
        child_channel.close()
        try:
            child_channel.send("test", "data")
        except ChannelClosed:
            print("    Caught ChannelClosed")

        # 3. Catching any RPC error
        print("  Testing RPCError base class...")
        new_channel = SyncThreadChannel(send_q, recv_q)
        new_channel.close()
        try:
            new_channel.recv()
        except RPCError as e:
            print(f"    Caught RPCError: {type(e).__name__}")

    demo_exceptions()
    print("Done!")

# %%
await example_exception_handling()
