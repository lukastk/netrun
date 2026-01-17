# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Remote RPC Exception Tests
#
# This notebook provides comprehensive tests and examples for all exception types
# that can occur in the Remote RPC layer. The Remote RPC uses WebSockets for
# communication over the network.
#
# ## Exception Types
#
# The Remote RPC can raise four types of exceptions:
#
# 1. **ChannelClosed**: The channel has been explicitly closed or received a shutdown signal
# 2. **RecvTimeout**: A receive operation timed out waiting for a message
# 3. **ChannelBroken**: The WebSocket connection failed (send error, connection lost, parse error)
# 4. **ConnectionError**: Failed to establish the initial connection
#
# `WebSocketChannel` is async-only since WebSocket communication is inherently async.

# %%
#|default_exp rpc.test_exceptions_remote

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
    RPC_KEY_SHUTDOWN,
)
from netrun.rpc.remote import (
    WebSocketChannel,
    connect,
    serve_background,
)

# %% [markdown]
# ---
# # ChannelClosed Exception
#
# `ChannelClosed` is raised in the following scenarios:
#
# 1. **Explicit close**: Calling `send()`, `recv()`, or `try_recv()` after `close()`
# 2. **Shutdown signal**: Receiving the `RPC_KEY_SHUTDOWN` message from the other end
# 3. **Server/client disconnect**: When either end closes the WebSocket connection

# %% [markdown]
# ## 1.1 ChannelClosed on Explicit Close

# %%
#|export
@pytest.mark.asyncio
async def test_send_after_close():
    """WebSocketChannel.send() raises ChannelClosed after close()."""
    async def handler(channel):
        await asyncio.sleep(10)  # Keep server alive

    async with serve_background(handler, "127.0.0.1", 29801):
        async with connect("ws://127.0.0.1:29801") as channel:
            await channel.close()
            assert channel.is_closed

            with pytest.raises(ChannelClosed) as exc_info:
                await channel.send("test", "data")

            assert "closed" in str(exc_info.value).lower()

# %%
await test_send_after_close()
print("Send after close: raises ChannelClosed as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_recv_after_close():
    """WebSocketChannel.recv() raises ChannelClosed after close() and queue empty."""
    async def handler(channel):
        await asyncio.sleep(10)

    async with serve_background(handler, "127.0.0.1", 29802):
        async with connect("ws://127.0.0.1:29802") as channel:
            await channel.close()

            # After close with empty queue, should raise
            with pytest.raises(ChannelClosed):
                await channel.recv(timeout=0.1)

# %%
await test_recv_after_close()
print("Recv after close: raises ChannelClosed as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_after_close():
    """WebSocketChannel.try_recv() raises ChannelClosed after close() and queue empty."""
    async def handler(channel):
        await asyncio.sleep(10)

    async with serve_background(handler, "127.0.0.1", 29803):
        async with connect("ws://127.0.0.1:29803") as channel:
            await channel.close()

            with pytest.raises(ChannelClosed):
                await channel.try_recv()

# %%
await test_try_recv_after_close()
print("Try_recv after close: raises ChannelClosed as expected")

# %% [markdown]
# ## 1.2 ChannelClosed on Shutdown Signal

# %%
#|export
@pytest.mark.asyncio
async def test_recv_shutdown_from_server():
    """Client's recv() raises ChannelClosed when server closes."""
    async def handler(channel):
        # Immediately close
        await channel.close()

    async with serve_background(handler, "127.0.0.1", 29804):
        async with connect("ws://127.0.0.1:29804") as channel:
            # Server closed, so recv should get shutdown signal
            with pytest.raises(ChannelClosed):
                await channel.recv(timeout=1.0)

# %%
await test_recv_shutdown_from_server()
print("Recv shutdown from server: raises ChannelClosed")

# %%
#|export
@pytest.mark.asyncio
async def test_server_receives_client_close():
    """Server's recv() raises ChannelClosed when client closes."""
    server_exception = []

    async def handler(channel):
        try:
            await channel.recv(timeout=2.0)
        except ChannelClosed as e:
            server_exception.append(e)

    async with serve_background(handler, "127.0.0.1", 29805):
        async with connect("ws://127.0.0.1:29805") as channel:
            # Client closes immediately
            pass  # Context manager will close

        await asyncio.sleep(0.1)  # Let server process

    assert len(server_exception) == 1
    assert isinstance(server_exception[0], ChannelClosed)

# %%
await test_server_receives_client_close()
print("Server receives client close: raises ChannelClosed")

# %% [markdown]
# ## 1.3 Multiple Close Calls Are Safe

# %%
#|export
@pytest.mark.asyncio
async def test_multiple_close_is_safe():
    """Multiple close() calls are safe."""
    async def handler(channel):
        await asyncio.sleep(10)

    async with serve_background(handler, "127.0.0.1", 29806):
        async with connect("ws://127.0.0.1:29806") as channel:
            await channel.close()
            await channel.close()
            await channel.close()

            assert channel.is_closed

# %%
await test_multiple_close_is_safe()
print("Multiple close calls: safe, no exceptions")

# %% [markdown]
# ---
# # RecvTimeout Exception
#
# `RecvTimeout` is raised when `recv()` with a timeout does not receive
# a message within the specified time.

# %% [markdown]
# ## 2.1 RecvTimeout Basics

# %%
#|export
@pytest.mark.asyncio
async def test_recv_timeout():
    """WebSocketChannel.recv() raises RecvTimeout when timeout expires."""
    async def handler(channel):
        await asyncio.sleep(10)  # Don't send anything

    async with serve_background(handler, "127.0.0.1", 29807):
        async with connect("ws://127.0.0.1:29807") as channel:
            start = time.time()
            with pytest.raises(RecvTimeout) as exc_info:
                await channel.recv(timeout=0.1)
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
    async def handler(channel):
        # Wait a bit, then respond
        await asyncio.sleep(0.15)
        await channel.send("delayed", "response")

    async with serve_background(handler, "127.0.0.1", 29808):
        async with connect("ws://127.0.0.1:29808") as channel:
            # First recv times out
            with pytest.raises(RecvTimeout):
                await channel.recv(timeout=0.05)

            # Channel still usable
            assert not channel.is_closed

            # Can receive the delayed message
            key, data = await channel.recv(timeout=1.0)
            assert key == "delayed"
            assert data == "response"

# %%
await test_recv_timeout_preserves_channel()
print("Recv timeout: channel remains usable after timeout")

# %% [markdown]
# ## 2.2 try_recv Does NOT Raise RecvTimeout

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_returns_none():
    """WebSocketChannel.try_recv() returns None, never raises RecvTimeout."""
    async def handler(channel):
        await asyncio.sleep(10)

    async with serve_background(handler, "127.0.0.1", 29809):
        async with connect("ws://127.0.0.1:29809") as channel:
            # Start receiver to enable try_recv
            await channel._start_receiver()
            await asyncio.sleep(0.01)

            result = await channel.try_recv()
            assert result is None

# %%
await test_try_recv_returns_none()
print("Try_recv: returns None (no RecvTimeout)")

# %% [markdown]
# ---
# # ChannelBroken Exception
#
# `ChannelBroken` is raised when the WebSocket connection encounters an error:
#
# - **Send failure**: Failed to send over the WebSocket
# - **Connection lost**: The WebSocket connection was unexpectedly closed
# - **Parse error**: Failed to deserialize a received message
#
# This is more common in network communication due to connectivity issues.

# %% [markdown]
# ## 3.1 ChannelBroken Exception Structure

# %%
#|export
def test_channel_broken_exception_structure():
    """ChannelBroken has the expected structure."""
    assert issubclass(ChannelBroken, RPCError)
    assert issubclass(ChannelBroken, Exception)

    exc = ChannelBroken("Connection lost")
    assert "Connection lost" in str(exc)

# %%
test_channel_broken_exception_structure()
print("ChannelBroken: properly structured exception class")

# %%
#|export
def test_channel_broken_vs_closed_distinction():
    """ChannelBroken and ChannelClosed are distinct exceptions."""
    assert ChannelBroken is not ChannelClosed

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
# ---
# # ConnectionError Exception
#
# `ConnectionError` is raised when failing to establish the initial WebSocket connection.

# %%
#|export
@pytest.mark.asyncio
async def test_connection_error_no_server():
    """connect() raises ConnectionError when server is not available."""
    with pytest.raises(ConnectionError) as exc_info:
        async with connect("ws://127.0.0.1:29899"):  # No server on this port
            pass

    assert "Failed to connect" in str(exc_info.value)

# %%
await test_connection_error_no_server()
print("Connection to nonexistent server: raises ConnectionError")

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
# ## Example: Echo Server with Exception Handling

# %%
@pytest.mark.asyncio
async def example_echo_server():
    """Example: Echo server with proper exception handling."""
    print("=" * 50)
    print("Example: Echo Server")
    print("=" * 50)

    async def echo_handler(channel):
        print("  [Server] Client connected")
        try:
            while True:
                key, data = await channel.recv()
                print(f"  [Server] Received: {key}={data}")
                await channel.send(f"echo:{key}", data)
        except ChannelClosed:
            print("  [Server] Client disconnected (graceful)")
        except ChannelBroken as e:
            print(f"  [Server] Connection broken: {e}")

    async with serve_background(echo_handler, "127.0.0.1", 29810):
        async with connect("ws://127.0.0.1:29810") as channel:
            await channel.send("hello", "world")
            key, data = await channel.recv(timeout=1.0)
            print(f"  [Client] Got: {key}={data}")

            await channel.send("number", 42)
            key, data = await channel.recv(timeout=1.0)
            print(f"  [Client] Got: {key}={data}")

    print("Done!")

# %%
await example_echo_server()

# %% [markdown]
# ## Example: Retry on Timeout

# %%
@pytest.mark.asyncio
async def example_retry_on_timeout():
    """Example: Retrying requests on timeout."""
    print("=" * 50)
    print("Example: Retry on Timeout")
    print("=" * 50)

    request_count = [0]

    async def slow_handler(channel):
        try:
            while True:
                key, data = await channel.recv()
                request_count[0] += 1
                if request_count[0] == 1:
                    await asyncio.sleep(0.2)  # First request is slow
                await channel.send("result", f"processed-{data}")
        except ChannelClosed:
            pass

    async with serve_background(slow_handler, "127.0.0.1", 29811):
        async with connect("ws://127.0.0.1:29811") as channel:
            async def request_with_retry(data, timeout=0.1, max_retries=3):
                for attempt in range(max_retries):
                    await channel.send("request", data)
                    try:
                        key, result = await channel.recv(timeout=timeout)
                        return result
                    except RecvTimeout:
                        print(f"  Attempt {attempt + 1} timed out...")
                raise RecvTimeout(f"Failed after {max_retries} attempts")

            # First call may timeout and retry
            result = await request_with_retry("test", timeout=0.3)
            print(f"  Got result: {result}")

    print("Done!")

# %%
await example_retry_on_timeout()

# %% [markdown]
# ## Example: Connection Error Handling

# %%
@pytest.mark.asyncio
async def example_connection_error_handling():
    """Example: Handling connection errors gracefully."""
    print("=" * 50)
    print("Example: Connection Error Handling")
    print("=" * 50)

    async def try_connect(url, retries=3):
        for attempt in range(retries):
            try:
                channel = await asyncio.wait_for(
                    connect(url).__aenter__(),
                    timeout=1.0
                )
                print(f"  Connected on attempt {attempt + 1}")
                return channel
            except (ConnectionError, asyncio.TimeoutError) as e:
                print(f"  Attempt {attempt + 1} failed: {type(e).__name__}")
        return None

    # Try connecting to non-existent server
    channel = await try_connect("ws://127.0.0.1:29899")
    assert channel is None
    print("  All connection attempts failed (expected)")

    # Now try with a real server
    async def handler(ch):
        await asyncio.sleep(10)

    async with serve_background(handler, "127.0.0.1", 29812):
        channel = await try_connect("ws://127.0.0.1:29812")
        assert channel is not None
        await channel.close()

    print("Done!")

# %%
await example_connection_error_handling()
