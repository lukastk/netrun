# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp pool.aio

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %% [markdown]
# # Async Pool
#
# A single-worker pool for async coroutines in the main thread. Designed for
# use with netrun's main execution loop where the "worker" is an async
# coroutine running in the same event loop.
#
# ## Usage
#
# ```python
# from netrun.pool.aio import SingleWorkerPool
# from netrun.rpc.base import ChannelClosed
#
# async def my_worker(channel, worker_id):
#     print(f"Worker {worker_id} started")
#     try:
#         while True:
#             key, data = await channel.recv()
#             result = data * 2
#             await channel.send("result", result)
#     except ChannelClosed:
#         print(f"Worker {worker_id} stopping")
#
# async with SingleWorkerPool(my_worker) as pool:
#     await pool.send(worker_id=0, key="task", data=10)
#     msg = await pool.recv()
#     print(f"Worker {msg.worker_id} returned: {msg.data}")
# ```

# %%
#|export
import asyncio
from typing import Any
from collections.abc import Callable, Awaitable

from netrun.rpc.base import ChannelClosed, RecvTimeout
from netrun.rpc.aio import (
    AsyncChannel,
    create_async_channel_pair,
)
from netrun.pool.base import (
    WorkerId,
    WorkerMessage,
    PoolNotStarted,
    PoolAlreadyStarted,
    POOL_UP_ERROR_EXCEPTION,
    POOL_UP_ERROR_CRASHED,
    _check_error_and_raise,
)

# %% [markdown]
# ## AsyncWorkerFn Type

# %%
#|export
AsyncWorkerFn = Callable[[AsyncChannel, WorkerId], Awaitable[None]]
"""Type for async worker functions: async def worker(channel, worker_id) -> None"""

# %% [markdown]
# ## SingleWorkerPool

# %%
#|export
class SingleWorkerPool:
    """A pool with a single async worker coroutine.

    Designed for the main thread of netrun where the "worker" is
    an async coroutine running in the same event loop. Unlike ThreadPool
    or MultiprocessPool, this does not spawn threads or processes.
    """

    def __init__(
        self,
        worker_fn: AsyncWorkerFn,
    ):
        """Create a single-worker async pool.

        Args:
            worker_fn: Async function to run as the worker.
                       Signature: async def worker(channel: AsyncChannel, worker_id: int) -> None
        """
        self._worker_fn = worker_fn
        self._running = False

        # Will be populated on start()
        self._channel: AsyncChannel | None = None
        self._worker_channel: AsyncChannel | None = None
        self._worker_task: asyncio.Task | None = None
        self._recv_queue: asyncio.Queue = asyncio.Queue()
        self._recv_task: asyncio.Task | None = None
        self._monitor_task: asyncio.Task | None = None

    @property
    def num_workers(self) -> int:
        """Total number of workers in the pool. Always 1."""
        return 1

    @property
    def is_running(self) -> bool:
        """Whether the pool has been started."""
        return self._running

    async def start(self) -> None:
        """Start the worker."""
        if self._running:
            raise PoolAlreadyStarted("Pool is already running")

        self._channel, self._worker_channel = create_async_channel_pair()

        # Start worker as an async task
        self._worker_task = asyncio.create_task(
            self._run_worker()
        )
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_worker())

    async def _monitor_worker(self) -> None:
        """Detect if worker task dies unexpectedly."""
        try:
            await self._worker_task
        except asyncio.CancelledError:
            return
        except Exception:
            pass

        if self._running:
            # Task ended while pool still running
            await self._recv_queue.put(WorkerMessage(
                worker_id=0,
                key=POOL_UP_ERROR_CRASHED,
                data={"reason": "Worker task ended unexpectedly"}
            ))

    async def _run_worker(self) -> None:
        """Run the worker function."""
        try:
            await self._worker_fn(self._worker_channel, 0)
        except ChannelClosed:
            pass
        except Exception as e:
            # Try to send exception object back (no serialization needed for async pool)
            try:
                await self._worker_channel.send(POOL_UP_ERROR_EXCEPTION, e)
            except Exception:
                pass

    async def close(self, timeout: float | None = None) -> None:
        """Shut down the worker and clean up resources.

        Args:
            timeout: Max seconds to wait for the worker task to finish.
                     If None, wait indefinitely. Note: asyncio tasks are
                     cancelled, so timeout mainly affects graceful shutdown.
        """
        if not self._running:
            return

        self._running = False

        # Cancel monitor task first
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

        # Close the channel to signal worker to stop
        if self._channel:
            await self._channel.close()

        # Cancel recv task if running
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass

        # Wait for worker task to finish
        if self._worker_task and not self._worker_task.done():
            if timeout is not None:
                try:
                    await asyncio.wait_for(self._worker_task, timeout=timeout)
                except TimeoutError:
                    self._worker_task.cancel()
                    try:
                        await self._worker_task
                    except asyncio.CancelledError:
                        pass
            else:
                # Wait indefinitely for graceful shutdown
                try:
                    await self._worker_task
                except asyncio.CancelledError:
                    pass

        self._channel = None
        self._worker_channel = None
        self._worker_task = None
        self._recv_queue = asyncio.Queue()
        self._recv_task = None
        self._monitor_task = None

    async def send(self, worker_id: WorkerId, key: str, data: Any) -> None:
        """Send a message to the worker."""
        if not self._running:
            raise PoolNotStarted("Pool has not been started")

        if worker_id != 0:
            raise ValueError(f"worker_id must be 0, got {worker_id}")

        await self._channel.send(key, data)

    def _start_recv_task(self) -> None:
        """Start background task that forwards messages to the queue."""
        if self._recv_task is not None:
            return

        async def recv_loop():
            try:
                while self._running:
                    key, data = await self._channel.recv()
                    msg = WorkerMessage(worker_id=0, key=key, data=data)
                    await self._recv_queue.put(msg)
            except (ChannelClosed, asyncio.CancelledError):
                pass
            except Exception:
                pass

        self._recv_task = asyncio.create_task(recv_loop())

    async def recv(self, timeout: float | None = None) -> WorkerMessage:
        """Receive a message from the worker.

        Raises:
            WorkerException: If the worker raised an exception
            WorkerCrashed: If the worker died unexpectedly
            WorkerTimeout: If the worker timed out
            RecvTimeout: If this recv() call times out
        """
        if not self._running:
            raise PoolNotStarted("Pool has not been started")

        self._start_recv_task()

        try:
            if timeout is None:
                msg = await self._recv_queue.get()
            else:
                msg = await asyncio.wait_for(
                    self._recv_queue.get(),
                    timeout=timeout,
                )
        except TimeoutError:
            raise RecvTimeout(f"Receive timed out after {timeout}s")

        _check_error_and_raise(msg)
        return msg

    async def try_recv(self) -> WorkerMessage | None:
        """Non-blocking receive from the worker.

        Raises:
            WorkerException: If the worker raised an exception
            WorkerCrashed: If the worker died unexpectedly
            WorkerTimeout: If the worker timed out
        """
        if not self._running:
            raise PoolNotStarted("Pool has not been started")

        # If recv task is running, check the queue first
        if self._recv_task is not None:
            try:
                msg = self._recv_queue.get_nowait()
                _check_error_and_raise(msg)
                return msg
            except asyncio.QueueEmpty:
                return None

        # Otherwise, read directly from channel
        result = await self._channel.try_recv()
        if result is not None:
            key, data = result
            msg = WorkerMessage(worker_id=0, key=key, data=data)
            _check_error_and_raise(msg)
            return msg

        return None

    async def broadcast(self, key: str, data: Any) -> None:
        """Send a message to the worker (same as send for single worker)."""
        if not self._running:
            raise PoolNotStarted("Pool has not been started")

        await self._channel.send(key, data)

    async def __aenter__(self) -> "SingleWorkerPool":
        """Context manager entry - starts the pool."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - closes the pool."""
        await self.close()

# %% [markdown]
# ## Example: Echo Worker

# %%
async def echo_worker(channel: AsyncChannel, worker_id: int):
    """Simple async worker that echoes messages back."""
    print(f"[Worker {worker_id}] Started")
    try:
        while True:
            key, data = await channel.recv()
            print(f"[Worker {worker_id}] Received: {key}={data}")
            await channel.send(f"echo:{key}", {"worker_id": worker_id, "data": data})
    except ChannelClosed:
        print(f"[Worker {worker_id}] Stopping")

# %%
async def example_echo_pool():
    """Example: basic echo pool with async worker."""
    print("=" * 50)
    print("Example 1: Async Echo Pool")
    print("=" * 50)

    async with SingleWorkerPool(echo_worker) as pool:
        # Send messages to the worker
        await pool.send(worker_id=0, key="hello", data="world")
        await pool.send(worker_id=0, key="number", data=42)
        await pool.send(worker_id=0, key="list", data=[1, 2, 3])

        # Receive all responses
        for _ in range(3):
            msg = await pool.recv(timeout=5.0)
            print(f"[Main] Got from worker {msg.worker_id}: {msg.key}={msg.data}")

    print("Done!\n")

# %%
await example_echo_pool()

# %% [markdown]
# ## Example: Compute Worker

# %%
async def compute_worker(channel: AsyncChannel, worker_id: int):
    """Async worker that performs computations."""
    print(f"[Worker {worker_id}] Started")
    try:
        while True:
            key, data = await channel.recv()
            print(f"[Worker {worker_id}] Computing: {key}({data})")

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

            await channel.send("result", {"input": data, "output": result})
    except ChannelClosed:
        print(f"[Worker {worker_id}] Stopping")

# %%
async def example_compute_pool():
    """Example: async compute worker."""
    print("=" * 50)
    print("Example 2: Async Compute Pool")
    print("=" * 50)

    async with SingleWorkerPool(compute_worker) as pool:
        # Send computation requests
        await pool.send(0, "square", 7)
        msg = await pool.recv(timeout=5.0)
        print(f"[Main] square(7) = {msg.data['output']}")

        await pool.send(0, "factorial", 5)
        msg = await pool.recv(timeout=5.0)
        print(f"[Main] factorial(5) = {msg.data['output']}")

        await pool.send(0, "sum", [1, 2, 3, 4, 5])
        msg = await pool.recv(timeout=5.0)
        print(f"[Main] sum([1,2,3,4,5]) = {msg.data['output']}")

    print("Done!\n")

# %%
await example_compute_pool()

# %% [markdown]
# ## Example: Broadcast
#
# For a single-worker pool, broadcast is equivalent to send.

# %%
async def example_broadcast():
    """Example: broadcasting (same as send for single worker)."""
    print("=" * 50)
    print("Example 3: Broadcast")
    print("=" * 50)

    async def config_worker(channel: AsyncChannel, worker_id: int):
        try:
            while True:
                key, data = await channel.recv()
                print(f"[Worker {worker_id}] Got broadcast: {key}={data}")
                await channel.send("ack", f"worker-{worker_id} received {key}")
        except ChannelClosed:
            pass

    async with SingleWorkerPool(config_worker) as pool:
        # Broadcast config to worker
        await pool.broadcast("config", {"setting": "value"})
        msg = await pool.recv(timeout=5.0)
        print(f"[Main] {msg.data}")

    print("Done!\n")

# %%
await example_broadcast()
