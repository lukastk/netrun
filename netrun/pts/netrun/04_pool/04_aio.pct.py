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
import logging
from typing import Any
from collections.abc import Callable, Awaitable

from netrun.rpc.base import ChannelClosed, RecvTimeout
from netrun.rpc.aio import (
    AsyncChannel,
    create_async_channel_pair,
)
from netrun.pool.base import (
    BasePool,
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
class SingleWorkerPool(BasePool):
    """A pool with a single async worker coroutine.

    Designed for the main thread of netrun where the "worker" is
    an async coroutine running in the same event loop. Unlike ThreadPool
    or MultiprocessPool, this does not spawn threads or processes.
    """

    def __init__(self, worker_fn: AsyncWorkerFn, **kwargs):
        """Create a single-worker async pool.

        Args:
            worker_fn: Async function to run as the worker.
                       Signature: async def worker(channel: AsyncChannel, worker_id: int) -> None
        """
        super().__init__(num_workers=1, **kwargs)
        self._worker_fn = worker_fn
        self._channel: AsyncChannel | None = None
        self._worker_channel: AsyncChannel | None = None
        self._worker_task: asyncio.Task | None = None

    async def _do_start(self) -> None:
        self._channel, self._worker_channel = create_async_channel_pair()
        self._worker_task = asyncio.create_task(self._run_worker())

    async def _run_worker(self) -> None:
        """Run the worker function."""
        try:
            await self._worker_fn(self._worker_channel, 0)
        except ChannelClosed:
            pass
        except Exception as e:
            try:
                await self._worker_channel.send(POOL_UP_ERROR_EXCEPTION, e)
            except Exception:
                pass

    async def _do_close(self, timeout: float | None = None) -> None:
        # Close channel to signal worker to stop
        if self._channel:
            await self._channel.close()

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
                try:
                    await self._worker_task
                except asyncio.CancelledError:
                    pass

        self._channel = None
        self._worker_channel = None
        self._worker_task = None

    def _create_recv_loops(self) -> list:
        async def recv_loop():
            try:
                while self._running:
                    key, data = await self._channel.recv()
                    await self._recv_queue.put(WorkerMessage(worker_id=0, key=key, data=data))
            except (ChannelClosed, asyncio.CancelledError):
                pass
            except Exception as e:
                logging.getLogger("netrun.pool").error(
                    f"recv_loop for worker 0 crashed: {e}", exc_info=True
                )
                await self._recv_queue.put(WorkerMessage(
                    worker_id=0, key=POOL_UP_ERROR_CRASHED,
                    data={"reason": f"recv_loop exception: {e}"},
                ))
        return [recv_loop()]

    async def _check_worker_health(self) -> None:
        if self._worker_task and self._worker_task.done() and self._running:
            await self._recv_queue.put(WorkerMessage(
                worker_id=0, key=POOL_UP_ERROR_CRASHED,
                data={"reason": "Worker task ended unexpectedly"},
            ))

    async def _try_recv_direct(self) -> WorkerMessage | None:
        """Read directly from channel (when recv tasks aren't running)."""
        result = await self._channel.try_recv()
        if result is not None:
            key, data = result
            return WorkerMessage(worker_id=0, key=key, data=data)
        return None

    async def send(self, worker_id: WorkerId, key: str, data: Any) -> None:
        if not self._running:
            raise PoolNotStarted("Pool has not been started")
        if worker_id != 0:
            raise ValueError(f"worker_id must be 0, got {worker_id}")
        await self._channel.send(key, data)

    async def broadcast(self, key: str, data: Any) -> None:
        if not self._running:
            raise PoolNotStarted("Pool has not been started")
        await self._channel.send(key, data)

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
