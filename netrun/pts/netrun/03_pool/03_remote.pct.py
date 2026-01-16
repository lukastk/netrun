# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp pool.remote

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %% [markdown]
# # Remote Process Pool
#
# A pool of workers running on a remote server. The client connects via
# WebSocket and requests a pool with specific configuration. The server
# creates a MultiprocessPool and routes messages.
#
# ## Architecture
#
# ```
# Client                          Server
#   │                               │
#   │  ── WebSocket ──────────────► │
#   │                               │
#   │  send(worker_id, key, data)   │   MultiprocessPool
#   │  ─────────────────────────►   │   ├── Process 0
#   │                               │   │   ├── Thread 0
#   │                               │   │   └── Thread 1
#   │  ◄─────────────────────────   │   └── Process 1
#   │  recv() → WorkerMessage       │       ├── Thread 2
#   │                               │       └── Thread 3
# ```
#
# ## Usage
#
# ### Server
#
# ```python
# from netrun.pool.remote import RemotePoolServer
#
# def my_worker(channel, worker_id):
#     while True:
#         key, data = channel.recv()
#         channel.send("result", data * 2)
#
# server = RemotePoolServer()
# server.register_worker("my_worker", my_worker)
# await server.serve("0.0.0.0", 8080)
# ```
#
# ### Client
#
# ```python
# from netrun.pool.remote import RemotePoolClient
#
# async with RemotePoolClient("ws://server:8080") as client:
#     await client.create_pool(
#         worker_name="my_worker",
#         num_processes=2,
#         threads_per_process=2,
#     )
#
#     await client.send(worker_id=0, key="task", data=10)
#     msg = await client.recv()
# ```

# %%
#|export
import asyncio
from typing import Any
from contextlib import asynccontextmanager

from netrun.rpc.base import ChannelClosed, RecvTimeout
from netrun.rpc.remote import (
    WebSocketChannel,
    serve_background,
)
from netrun.pool.base import (
    WorkerId,
    WorkerFn,
    WorkerMessage,
    PoolError,
    PoolNotStarted,
)
from netrun.pool.multiprocess import MultiprocessPool

# %% [markdown]
# ## Protocol Messages
#
# Messages between client and server.

# %%
#|export
# Message types
MSG_CREATE_POOL = "create_pool"
MSG_POOL_CREATED = "pool_created"
MSG_SEND = "send"
MSG_RECV = "recv"
MSG_BROADCAST = "broadcast"
MSG_CLOSE = "close"
MSG_ERROR = "error"

# %% [markdown]
# ## RemotePoolServer

# %%
#|export
class RemotePoolServer:
    """Server that hosts remote worker pools.

    Clients connect, request a pool configuration, and the server
    creates a MultiprocessPool to handle their requests.
    """

    def __init__(self):
        """Create a remote pool server."""
        self._workers: dict[str, WorkerFn] = {}
        self._running = False

    def register_worker(self, name: str, worker_fn: WorkerFn) -> None:
        """Register a worker function by name.

        Args:
            name: Name clients will use to request this worker
            worker_fn: The worker function (must be importable for multiprocessing)
        """
        self._workers[name] = worker_fn

    @property
    def registered_workers(self) -> list[str]:
        """List of registered worker names."""
        return list(self._workers.keys())

    async def serve(self, host: str = "0.0.0.0", port: int = 8080) -> None:
        """Start the server and handle connections.

        This blocks until the server is stopped.

        Args:
            host: Host to bind to
            port: Port to listen on
        """
        from netrun.rpc.remote import serve

        async def handle_client(channel: WebSocketChannel):
            await self._handle_client(channel)

        await serve(handle_client, host, port)

    @asynccontextmanager
    async def serve_background(self, host: str = "0.0.0.0", port: int = 8080):
        """Start the server in the background.

        Args:
            host: Host to bind to
            port: Port to listen on

        Yields:
            The server object
        """
        async def handle_client(channel: WebSocketChannel):
            await self._handle_client(channel)

        async with serve_background(handle_client, host, port) as server:
            yield server

    async def _handle_client(self, channel: WebSocketChannel) -> None:
        """Handle a single client connection."""
        pool: MultiprocessPool | None = None
        forward_task: asyncio.Task | None = None

        try:
            while True:
                key, data = await channel.recv()

                if key == MSG_CREATE_POOL:
                    # Create pool
                    worker_name = data["worker_name"]
                    num_processes = data["num_processes"]
                    threads_per_process = data.get("threads_per_process", 1)

                    if worker_name not in self._workers:
                        await channel.send(MSG_ERROR, f"Unknown worker: {worker_name}")
                        continue

                    # Clean up existing pool and forward task if any
                    if forward_task is not None:
                        forward_task.cancel()
                        try:
                            await forward_task
                        except asyncio.CancelledError:
                            pass
                        forward_task = None

                    if pool is not None:
                        await pool.close()

                    pool = MultiprocessPool(
                        worker_fn=self._workers[worker_name],
                        num_processes=num_processes,
                        threads_per_process=threads_per_process,
                    )
                    await pool.start()

                    await channel.send(MSG_POOL_CREATED, {
                        "num_workers": pool.num_workers,
                        "num_processes": pool.num_processes,
                        "threads_per_process": pool.threads_per_process,
                    })

                    # Start forwarding responses from pool to client
                    forward_task = asyncio.create_task(self._forward_responses(pool, channel))

                elif key == MSG_SEND:
                    if pool is None:
                        await channel.send(MSG_ERROR, "No pool created")
                        continue

                    worker_id = data["worker_id"]
                    msg_key = data["key"]
                    msg_data = data["data"]
                    await pool.send(worker_id, msg_key, msg_data)

                elif key == MSG_BROADCAST:
                    if pool is None:
                        await channel.send(MSG_ERROR, "No pool created")
                        continue

                    msg_key = data["key"]
                    msg_data = data["data"]
                    await pool.broadcast(msg_key, msg_data)

                elif key == MSG_CLOSE:
                    break

        except ChannelClosed:
            pass
        finally:
            # Cancel forward task first
            if forward_task is not None:
                forward_task.cancel()
                try:
                    await forward_task
                except asyncio.CancelledError:
                    pass

            if pool is not None:
                await pool.close()

    async def _forward_responses(self, pool: MultiprocessPool, channel: WebSocketChannel) -> None:
        """Forward responses from pool workers to the client."""
        try:
            while pool.is_running and not channel.is_closed:
                try:
                    # Use a timeout so we can periodically check if we should stop
                    msg = await pool.recv(timeout=0.5)
                    await channel.send(MSG_RECV, {
                        "worker_id": msg.worker_id,
                        "key": msg.key,
                        "data": msg.data,
                    })
                except RecvTimeout:
                    continue
                except ChannelClosed:
                    break
        except asyncio.CancelledError:
            raise  # Re-raise to properly signal cancellation
        except Exception:
            pass

# %% [markdown]
# ## RemotePoolClient

# %%
#|export
class RemotePoolClient:
    """Client for connecting to a remote pool server.

    Provides the same interface as local pools (send, recv, etc.)
    but workers run on the remote server.
    """

    def __init__(self, url: str):
        """Create a client.

        Args:
            url: WebSocket URL of the server (e.g., "ws://server:8080")
        """
        self._url = url
        self._channel: WebSocketChannel | None = None
        self._num_workers = 0
        self._num_processes = 0
        self._threads_per_process = 0
        self._running = False
        self._recv_queue: asyncio.Queue = asyncio.Queue()
        self._recv_task: asyncio.Task | None = None

    @property
    def num_workers(self) -> int:
        """Total number of workers in the remote pool."""
        return self._num_workers

    @property
    def num_processes(self) -> int:
        """Number of processes on the server."""
        return self._num_processes

    @property
    def threads_per_process(self) -> int:
        """Number of threads per process."""
        return self._threads_per_process

    @property
    def is_running(self) -> bool:
        """Whether the client is connected and pool is created."""
        return self._running

    async def connect(self) -> None:
        """Connect to the server."""
        from netrun.rpc.remote import connect_channel
        self._channel = await connect_channel(self._url)

    async def create_pool(
        self,
        worker_name: str,
        num_processes: int,
        threads_per_process: int = 1,
    ) -> None:
        """Create a pool on the server.

        Args:
            worker_name: Name of registered worker function on server
            num_processes: Number of processes
            threads_per_process: Threads per process
        """
        if self._channel is None:
            raise PoolNotStarted("Not connected to server")

        await self._channel.send(MSG_CREATE_POOL, {
            "worker_name": worker_name,
            "num_processes": num_processes,
            "threads_per_process": threads_per_process,
        })

        # Wait for response
        key, data = await self._channel.recv(timeout=None)

        if key == MSG_ERROR:
            raise PoolError(f"Server error: {data}")

        if key != MSG_POOL_CREATED:
            raise PoolError(f"Unexpected response: {key}")

        self._num_workers = data["num_workers"]
        self._num_processes = data["num_processes"]
        self._threads_per_process = data["threads_per_process"]
        self._running = True

        # Start receiving messages from server
        self._recv_task = asyncio.create_task(self._receive_loop())

    async def _receive_loop(self) -> None:
        """Receive messages from server and queue them."""
        try:
            while self._running and self._channel and not self._channel.is_closed:
                try:
                    key, data = await self._channel.recv(timeout=None)
                    if key == MSG_RECV:
                        await self._recv_queue.put(data)
                    elif key == MSG_ERROR:
                        print(f"Server error: {data}")
                except RecvTimeout:
                    continue
                except ChannelClosed:
                    break
        except Exception:
            pass

    async def close(self, timeout: float | None = None) -> None:
        """Close the connection and remote pool.

        Args:
            timeout: Not used for RemotePoolClient (included for protocol compatibility).
        """
        self._running = False

        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass

        if self._channel and not self._channel.is_closed:
            try:
                await self._channel.send(MSG_CLOSE, None)
            except Exception:
                pass
            await self._channel.close()

        self._channel = None

    async def send(self, worker_id: WorkerId, key: str, data: Any) -> None:
        """Send a message to a specific worker on the server."""
        if not self._running or self._channel is None:
            raise PoolNotStarted("Pool not created")

        if worker_id < 0 or worker_id >= self._num_workers:
            raise ValueError(f"worker_id {worker_id} out of range [0, {self._num_workers})")

        await self._channel.send(MSG_SEND, {
            "worker_id": worker_id,
            "key": key,
            "data": data,
        })

    async def recv(self, timeout: float | None = None) -> WorkerMessage:
        """Receive a message from any worker."""
        if not self._running:
            raise PoolNotStarted("Pool not created")

        try:
            if timeout is None:
                data = await self._recv_queue.get()
            else:
                data = await asyncio.wait_for(
                    self._recv_queue.get(),
                    timeout=timeout,
                )
            return WorkerMessage(
                worker_id=data["worker_id"],
                key=data["key"],
                data=data["data"],
            )
        except TimeoutError:
            raise RecvTimeout(f"Receive timed out after {timeout}s")

    async def try_recv(self) -> WorkerMessage | None:
        """Non-blocking receive from any worker."""
        if not self._running:
            raise PoolNotStarted("Pool not created")

        try:
            data = self._recv_queue.get_nowait()
            return WorkerMessage(
                worker_id=data["worker_id"],
                key=data["key"],
                data=data["data"],
            )
        except asyncio.QueueEmpty:
            return None

    async def broadcast(self, key: str, data: Any) -> None:
        """Send a message to all workers."""
        if not self._running or self._channel is None:
            raise PoolNotStarted("Pool not created")

        await self._channel.send(MSG_BROADCAST, {
            "key": key,
            "data": data,
        })

    async def __aenter__(self) -> "RemotePoolClient":
        """Context manager entry - connects to server."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - closes connection."""
        await self.close()

# %% [markdown]
# ## Example
#
# Demonstrates a server and client communicating.

# %%
import tempfile
import sys
from pathlib import Path

# Create temp module with worker function
_temp_dir = tempfile.mkdtemp(prefix="remote_pool_example_")
_worker_code = '''
"""Worker for remote pool example."""
from netrun.rpc.base import ChannelClosed

def remote_echo_worker(channel, worker_id):
"""Echo worker for remote pool."""
import os
print(f"[Remote Worker {worker_id}] Started in process {os.getpid()}")
try:
    while True:
        key, data = channel.recv()
        print(f"[Remote Worker {worker_id}] Received: {key}={data}")
        channel.send(f"echo:{key}", {"worker_id": worker_id, "data": data})
except ChannelClosed:
    print(f"[Remote Worker {worker_id}] Stopping")
'''

_worker_path = Path(_temp_dir) / "remote_workers.py"
_worker_path.write_text(_worker_code)
print(f"Created worker module at: {_worker_path}")

if _temp_dir not in sys.path:
    sys.path.insert(0, _temp_dir)

# %%
from remote_workers import remote_echo_worker
# Import from installed module to avoid pickling issues with __main__
from netrun.pool.remote import RemotePoolServer as _RemotePoolServer
from netrun.pool.remote import RemotePoolClient as _RemotePoolClient

async def example_remote_pool():
    """Example: remote pool server and client."""
    print("=" * 50)
    print("Example: Remote Pool")
    print("=" * 50)

    # Create server
    server = _RemotePoolServer()
    server.register_worker("echo", remote_echo_worker)

    async with server.serve_background("127.0.0.1", 19999):
        print("[Main] Server started")

        # Connect client
        async with _RemotePoolClient("ws://127.0.0.1:19999") as client:
            print("[Main] Client connected")

            # Create pool
            await client.create_pool(
                worker_name="echo",
                num_processes=2,
                threads_per_process=2,
            )
            print(f"[Main] Pool created with {client.num_workers} workers")

            # Send to each worker
            for worker_id in range(client.num_workers):
                await client.send(worker_id, "hello", f"message-{worker_id}")

            # Receive responses
            for _ in range(client.num_workers):
                msg = await client.recv(timeout=10.0)
                print(f"[Main] Got from worker {msg.worker_id}: {msg.key}={msg.data}")

    print("Done!\n")

await example_remote_pool()

# %%
# Clean up
import shutil
shutil.rmtree(_temp_dir, ignore_errors=True)
print(f"Cleaned up: {_temp_dir}")
