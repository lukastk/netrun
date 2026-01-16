# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp rpc.remote

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();

# %% [markdown]
# # Remote RPC
#
# Channel for communication over the network using WebSockets.
# Provides async channel class and helper functions for server/client setup.
#
# **Requires**: `websockets` package (`pip install websockets`)
#
# ## Usage
#
# ### Server
#
# ```python
# from netrun.rpc.remote import WebSocketChannel, serve
#
# async def handle_client(channel: WebSocketChannel):
#     while True:
#         key, data = await channel.recv()
#         await channel.send("echo", data)
#
# # Run server (blocks until stopped)
# await serve(handle_client, host="0.0.0.0", port=8080)
# ```
#
# ### Client
#
# ```python
# from netrun.rpc.remote import connect
#
# async with connect("ws://server:8080") as channel:
#     await channel.send("hello", "world")
#     key, data = await channel.recv()
# ```

# %%
#|export
import asyncio
import pickle
import json
from typing import Any, Callable, Awaitable
from contextlib import asynccontextmanager

from netrun.rpc.base import (
    ChannelClosed,
    ChannelBroken,
    RecvTimeout,
    SHUTDOWN_KEY,
)

# %% [markdown]
# ## WebSocketChannel
#
# Async RPC channel over a WebSocket connection.

# %%
#|export
class WebSocketChannel:
    """Async RPC channel over a WebSocket connection.

    Messages are serialized using pickle (for data) wrapped in JSON (for metadata).
    Thread-safe for use from multiple coroutines.

    Wire format:
        {"key": str, "data_hex": str}
        where data_hex is hex-encoded pickled data
    """

    def __init__(self, websocket):
        """Create a channel from a websocket connection.

        Args:
            websocket: A websockets WebSocket connection
        """
        self._ws = websocket
        self._closed = False
        self._recv_queue: asyncio.Queue = asyncio.Queue()
        self._recv_task: asyncio.Task | None = None

    async def _start_receiver(self) -> None:
        """Start the background receiver task."""
        if self._recv_task is None:
            self._recv_task = asyncio.create_task(self._receiver_loop())

    async def _receiver_loop(self) -> None:
        """Background task that receives messages and queues them."""
        try:
            async for message in self._ws:
                try:
                    data = json.loads(message)
                    key = data["key"]
                    payload = pickle.loads(bytes.fromhex(data["data_hex"]))
                    await self._recv_queue.put((key, payload))
                except Exception as e:
                    await self._recv_queue.put(("__error__", str(e)))
        except Exception as e:
            if not self._closed:
                await self._recv_queue.put(("__broken__", str(e)))
        finally:
            self._closed = True

    async def send(self, key: str, data: Any) -> None:
        """Send a message."""
        if self._closed:
            raise ChannelClosed("Channel is closed")

        try:
            message = json.dumps({
                "key": key,
                "data_hex": pickle.dumps(data).hex(),
            })
            await self._ws.send(message)
        except Exception as e:
            self._closed = True
            raise ChannelBroken(f"Failed to send: {e}")

    async def recv(self, timeout: float | None = None) -> tuple[str, Any]:
        """Receive a message with optional timeout."""
        if self._closed and self._recv_queue.empty():
            raise ChannelClosed("Channel is closed")

        await self._start_receiver()

        try:
            if timeout is None:
                result = await self._recv_queue.get()
            else:
                result = await asyncio.wait_for(
                    self._recv_queue.get(),
                    timeout=timeout,
                )
        except asyncio.TimeoutError:
            raise RecvTimeout(f"Receive timed out after {timeout}s")

        key, data = result

        if key == SHUTDOWN_KEY:
            self._closed = True
            raise ChannelClosed("Channel was shut down")
        if key == "__broken__":
            self._closed = True
            raise ChannelBroken(f"Connection broken: {data}")
        if key == "__error__":
            raise ChannelBroken(f"Receive error: {data}")

        return key, data

    async def try_recv(self) -> tuple[str, Any] | None:
        """Non-blocking receive."""
        if self._closed and self._recv_queue.empty():
            raise ChannelClosed("Channel is closed")

        await self._start_receiver()

        try:
            result = self._recv_queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

        key, data = result

        if key == SHUTDOWN_KEY:
            self._closed = True
            raise ChannelClosed("Channel was shut down")
        if key == "__broken__":
            self._closed = True
            raise ChannelBroken(f"Connection broken: {data}")

        return key, data

    async def close(self) -> None:
        """Close the channel."""
        if not self._closed:
            self._closed = True
            try:
                await self._ws.send(json.dumps({
                    "key": SHUTDOWN_KEY,
                    "data_hex": pickle.dumps(None).hex(),
                }))
            except Exception:
                pass
            try:
                await self._ws.close()
            except Exception:
                pass
            if self._recv_task and not self._recv_task.done():
                self._recv_task.cancel()
                try:
                    await self._recv_task
                except asyncio.CancelledError:
                    pass

    @property
    def is_closed(self) -> bool:
        """Whether the channel is closed."""
        return self._closed

# %% [markdown]
# ## connect
#
# Helper function to connect to a WebSocket server.

# %%
#|export
@asynccontextmanager
async def connect(url: str):
    """Connect to a WebSocket RPC server.

    Args:
        url: WebSocket URL (e.g., "ws://host:port")

    Yields:
        WebSocketChannel for communication with the server.

    Example:
        ```python
        async with connect("ws://localhost:8080") as channel:
            await channel.send("hello", "world")
            key, data = await channel.recv()
        ```
    """
    try:
        import websockets
    except ImportError:
        raise ImportError(
            "websockets package required. Install with: pip install websockets"
        )

    try:
        websocket = await websockets.connect(url)
    except Exception as e:
        raise ConnectionError(f"Failed to connect to {url}: {e}")

    channel = WebSocketChannel(websocket)
    try:
        yield channel
    finally:
        await channel.close()

# %%
#|export
async def connect_channel(url: str) -> WebSocketChannel:
    """Connect to a WebSocket RPC server (non-context manager version).

    Args:
        url: WebSocket URL (e.g., "ws://host:port")

    Returns:
        WebSocketChannel for communication with the server.
        Caller is responsible for closing the channel.

    Example:
        ```python
        channel = await connect_channel("ws://localhost:8080")
        try:
            await channel.send("hello", "world")
            key, data = await channel.recv()
        finally:
            await channel.close()
        ```
    """
    try:
        import websockets
    except ImportError:
        raise ImportError(
            "websockets package required. Install with: pip install websockets"
        )

    try:
        websocket = await websockets.connect(url)
    except Exception as e:
        raise ConnectionError(f"Failed to connect to {url}: {e}")

    return WebSocketChannel(websocket)

# %% [markdown]
# ## serve
#
# Helper function to run a WebSocket server.

# %%
#|export
ConnectionHandler = Callable[[WebSocketChannel], Awaitable[None]]
"""Type for connection handler functions.""";

# %%
#|export
async def serve(
    handler: ConnectionHandler,
    host: str = "0.0.0.0",
    port: int = 8080,
) -> None:
    """Run a WebSocket RPC server.

    Accepts connections and runs the handler for each client.
    Blocks until the server is stopped (e.g., by KeyboardInterrupt).

    Args:
        handler: Async function called for each connection with a WebSocketChannel
        host: Host to bind to (default all interfaces)
        port: Port to listen on

    Example:
        ```python
        async def handle_client(channel: WebSocketChannel):
            while True:
                key, data = await channel.recv()
                result = process(data)
                await channel.send("result", result)

        # Run server (blocks until stopped)
        await serve(handle_client, port=8080)
        ```
    """
    try:
        import websockets
    except ImportError:
        raise ImportError(
            "websockets package required. Install with: pip install websockets"
        )

    async def handle_connection(websocket):
        channel = WebSocketChannel(websocket)
        try:
            await handler(channel)
        except ChannelClosed:
            pass
        finally:
            if not channel.is_closed:
                await channel.close()

    server = await websockets.serve(handle_connection, host, port)
    print(f"RPC server listening on ws://{host}:{port}")

    try:
        await server.wait_closed()
    except asyncio.CancelledError:
        pass
    finally:
        server.close()
        await server.wait_closed()

# %%
#|export
@asynccontextmanager
async def serve_background(
    handler: ConnectionHandler,
    host: str = "0.0.0.0",
    port: int = 8080,
):
    """Run a WebSocket RPC server in the background.

    Starts the server and yields control. Server stops when context exits.

    Args:
        handler: Async function called for each connection with a WebSocketChannel
        host: Host to bind to (default all interfaces)
        port: Port to listen on

    Yields:
        The websockets server object (for inspection).

    Example:
        ```python
        async def handle_client(channel):
            key, data = await channel.recv()
            await channel.send("echo", data)

        async with serve_background(handle_client, port=8080) as server:
            # Server is running, do other things
            async with connect("ws://localhost:8080") as client:
                await client.send("hello", "world")
                key, data = await client.recv()
        # Server stops when context exits
        ```
    """
    try:
        import websockets
    except ImportError:
        raise ImportError(
            "websockets package required. Install with: pip install websockets"
        )

    async def handle_connection(websocket):
        channel = WebSocketChannel(websocket)
        try:
            await handler(channel)
        except ChannelClosed:
            pass
        finally:
            if not channel.is_closed:
                await channel.close()

    server = await websockets.serve(handle_connection, host, port)
    try:
        yield server
    finally:
        server.close()
        await server.wait_closed()

# %% [markdown]
# ## Example: Echo Server and Client
#
# This example demonstrates a simple echo server and client using WebSockets.
# Note: Requires the `websockets` package.

# %%
import websockets

# %%
!uv pip install pandas

# %%
async def example_echo_server():
    """Example: echo server with a single client."""
    print("=" * 50)
    print("Example 1: Echo Server")
    print("=" * 50)

    async def echo_handler(channel: WebSocketChannel):
        """Echo back everything received."""
        print("[Server] Client connected")
        try:
            while True:
                key, data = await channel.recv()
                print(f"[Server] Received: {key} = {data}")
                await channel.send(f"echo:{key}", data)
        except ChannelClosed:
            print("[Server] Client disconnected")

    # Start server in background
    async with serve_background(echo_handler, "127.0.0.1", 19876):
        print("[Main] Server started")

        # Connect client
        async with connect("ws://127.0.0.1:19876") as channel:
            print("[Client] Connected")

            # Send some messages
            await channel.send("hello", "world")
            key, data = await channel.recv(timeout=5.0)
            print(f"[Client] Received: {key} = {data}")

            await channel.send("number", 42)
            key, data = await channel.recv(timeout=5.0)
            print(f"[Client] Received: {key} = {data}")

            await channel.send("data", {"list": [1, 2, 3], "nested": {"a": 1}})
            key, data = await channel.recv(timeout=5.0)
            print(f"[Client] Received: {key} = {data}")

    print("Done!\n")

await example_echo_server()

# %%
async def example_compute_server():
    """Example: compute server that processes requests."""
    print("=" * 50)
    print("Example 2: Compute Server")
    print("=" * 50)

    async def compute_handler(channel: WebSocketChannel):
        """Handle computation requests."""
        print("[Server] Client connected")
        try:
            while True:
                key, data = await channel.recv()
                print(f"[Server] Computing: {key}({data})")

                if key == "square":
                    result = data * data
                elif key == "factorial":
                    result = 1
                    for i in range(1, data + 1):
                        result *= i
                elif key == "sum":
                    result = sum(data)
                else:
                    result = f"unknown: {key}"

                await channel.send("result", result)
        except ChannelClosed:
            print("[Server] Client disconnected")

    async with serve_background(compute_handler, "127.0.0.1", 19877):
        async with connect("ws://127.0.0.1:19877") as channel:
            # Test various computations
            await channel.send("square", 7)
            _, result = await channel.recv(timeout=5.0)
            print(f"square(7) = {result}")

            await channel.send("factorial", 5)
            _, result = await channel.recv(timeout=5.0)
            print(f"factorial(5) = {result}")

            await channel.send("sum", [1, 2, 3, 4, 5])
            _, result = await channel.recv(timeout=5.0)
            print(f"sum([1,2,3,4,5]) = {result}")

    print("Done!\n")

await example_compute_server()

# %%
async def example_multiple_clients():
    """Example: server handling multiple clients."""
    print("=" * 50)
    print("Example 3: Multiple Clients")
    print("=" * 50)

    client_count = 0

    async def handler(channel: WebSocketChannel):
        """Handle each client with a unique ID."""
        nonlocal client_count
        client_count += 1
        client_id = client_count
        print(f"[Server] Client {client_id} connected")

        try:
            while True:
                key, data = await channel.recv()
                await channel.send("response", f"Client {client_id} received: {data}")
        except ChannelClosed:
            print(f"[Server] Client {client_id} disconnected")

    async with serve_background(handler, "127.0.0.1", 19878):
        # Create multiple clients
        async with connect("ws://127.0.0.1:19878") as client1:
            async with connect("ws://127.0.0.1:19878") as client2:
                async with connect("ws://127.0.0.1:19878") as client3:
                    # Each client sends a message
                    await client1.send("msg", "Hello from client 1")
                    await client2.send("msg", "Hello from client 2")
                    await client3.send("msg", "Hello from client 3")

                    # Receive responses
                    _, data = await client1.recv(timeout=5.0)
                    print(f"[Client 1] {data}")

                    _, data = await client2.recv(timeout=5.0)
                    print(f"[Client 2] {data}")

                    _, data = await client3.recv(timeout=5.0)
                    print(f"[Client 3] {data}")

    print("Done!\n")

await example_multiple_clients()
