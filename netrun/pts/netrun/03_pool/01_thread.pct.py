# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp pool.thread

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %% [markdown]
# # Thread Pool
#
# A pool of worker threads within the same process. Uses `queue.Queue` for
# communication (via `rpc.thread`).
#
# ## Usage
#
# ```python
# from netrun.pool.thread import ThreadPool
# from netrun.rpc.base import ChannelClosed
#
# def my_worker(channel, worker_id):
#     print(f"Worker {worker_id} started")
#     try:
#         while True:
#             key, data = channel.recv()
#             result = data * 2
#             channel.send("result", result)
#     except ChannelClosed:
#         print(f"Worker {worker_id} stopping")
#
# pool = ThreadPool(my_worker, num_workers=4)
# await pool.start()
#
# # Send to specific worker
# await pool.send(worker_id=0, key="task", data=10)
# msg = await pool.recv()
# print(f"Worker {msg.worker_id} returned: {msg.data}")
#
# await pool.close()
# ```

# %%
#|export
import asyncio
import threading
from typing import Any

from netrun.rpc.base import ChannelClosed, RecvTimeout
from netrun.rpc.thread import (
    ThreadChannel,
    SyncThreadChannel,
    create_thread_channel_pair,
)
from netrun.pool.base import (
    WorkerId,
    WorkerFn,
    WorkerMessage,
    PoolNotStarted,
    PoolAlreadyStarted,
)

# %% [markdown]
# ## ThreadPool

# %%
#|export
class ThreadPool:
    """A pool of worker threads.

    Each worker runs a user-provided function that receives messages
    via a sync channel and can send responses back.
    """

    def __init__(
        self,
        worker_fn: WorkerFn,
        num_workers: int,
    ):
        """Create a thread pool.

        Args:
            worker_fn: Function to run in each worker thread
            num_workers: Number of worker threads to create
        """
        if num_workers < 1:
            raise ValueError("num_workers must be at least 1")

        self._worker_fn = worker_fn
        self._num_workers = num_workers
        self._running = False

        # Will be populated on start()
        self._channels: list[ThreadChannel] = []
        self._threads: list[threading.Thread] = []
        self._recv_queue: asyncio.Queue = asyncio.Queue()
        self._recv_tasks: list[asyncio.Task] = []

    @property
    def num_workers(self) -> int:
        """Total number of workers in the pool."""
        return self._num_workers

    @property
    def is_running(self) -> bool:
        """Whether the pool has been started."""
        return self._running

    async def start(self) -> None:
        """Start all workers in the pool."""
        if self._running:
            raise PoolAlreadyStarted("Pool is already running")

        self._channels = []
        self._threads = []

        for worker_id in range(self._num_workers):
            # Create channel pair
            parent_channel, child_queues = create_thread_channel_pair()
            self._channels.append(parent_channel)

            # Create and start worker thread
            thread = threading.Thread(
                target=self._run_worker,
                args=(child_queues, worker_id),
                name=f"PoolWorker-{worker_id}",
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)

        self._running = True

    def _run_worker(self, child_queues: tuple, worker_id: WorkerId) -> None:
        """Run the worker function in a thread."""
        send_q, recv_q = child_queues
        channel = SyncThreadChannel(send_q, recv_q)

        try:
            self._worker_fn(channel, worker_id)
        except ChannelClosed:
            pass
        except Exception as e:
            # Try to send error back
            try:
                channel.send("__error__", str(e))
            except Exception:
                pass

    async def close(self) -> None:
        """Shut down all workers and clean up resources."""
        if not self._running:
            return

        self._running = False

        # Close channels first - this unblocks any recv() calls
        for channel in self._channels:
            await channel.close()

        # Now cancel recv tasks (they should exit quickly since channels are closed)
        for task in self._recv_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Wait for threads to finish
        for thread in self._threads:
            thread.join(timeout=2.0)

        self._channels = []
        self._threads = []
        self._recv_queue = asyncio.Queue()
        self._recv_tasks = []

    async def send(self, worker_id: WorkerId, key: str, data: Any) -> None:
        """Send a message to a specific worker."""
        if not self._running:
            raise PoolNotStarted("Pool has not been started")

        if worker_id < 0 or worker_id >= self._num_workers:
            raise ValueError(f"worker_id {worker_id} out of range [0, {self._num_workers})")

        await self._channels[worker_id].send(key, data)

    def _start_recv_tasks(self) -> None:
        """Start background tasks that forward messages to the queue."""
        if self._recv_tasks:
            return

        async def recv_loop(worker_id: WorkerId, channel: ThreadChannel):
            try:
                while self._running:
                    key, data = await channel.recv()
                    msg = WorkerMessage(worker_id=worker_id, key=key, data=data)
                    await self._recv_queue.put(msg)
            except (ChannelClosed, asyncio.CancelledError):
                pass
            except Exception:
                pass

        for worker_id, channel in enumerate(self._channels):
            task = asyncio.create_task(recv_loop(worker_id, channel))
            self._recv_tasks.append(task)

    async def recv(self, timeout: float | None = None) -> WorkerMessage:
        """Receive a message from any worker."""
        if not self._running:
            raise PoolNotStarted("Pool has not been started")

        self._start_recv_tasks()

        try:
            if timeout is None:
                return await self._recv_queue.get()
            else:
                return await asyncio.wait_for(
                    self._recv_queue.get(),
                    timeout=timeout,
                )
        except TimeoutError:
            raise RecvTimeout(f"Receive timed out after {timeout}s")

    async def try_recv(self) -> WorkerMessage | None:
        """Non-blocking receive from any worker."""
        if not self._running:
            raise PoolNotStarted("Pool has not been started")

        for worker_id, channel in enumerate(self._channels):
            result = await channel.try_recv()
            if result is not None:
                key, data = result
                return WorkerMessage(worker_id=worker_id, key=key, data=data)

        return None

    async def broadcast(self, key: str, data: Any) -> None:
        """Send a message to all workers."""
        if not self._running:
            raise PoolNotStarted("Pool has not been started")

        for worker_id in range(self._num_workers):
            await self._channels[worker_id].send(key, data)

    async def __aenter__(self) -> "ThreadPool":
        """Context manager entry - starts the pool."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - closes the pool."""
        await self.close()

# %% [markdown]
# ## Example: Echo Pool

# %%

def echo_worker(channel, worker_id):
    """Simple worker that echoes messages back."""
    print(f"[Worker {worker_id}] Started")
    try:
        while True:
            key, data = channel.recv()
            print(f"[Worker {worker_id}] Received: {key}={data}")
            channel.send(f"echo:{key}", {"worker_id": worker_id, "data": data})
    except ChannelClosed:
        print(f"[Worker {worker_id}] Stopping")

# %%
async def example_echo_pool():
    """Example: basic echo pool."""
    print("=" * 50)
    print("Example 1: Echo Pool")
    print("=" * 50)

    async with ThreadPool(echo_worker, num_workers=3) as pool:
        # Send to each worker
        for i in range(3):
            await pool.send(worker_id=i, key="hello", data=f"message-{i}")

        # Receive all responses
        for _ in range(3):
            msg = await pool.recv(timeout=5.0)
            print(f"[Main] Got from worker {msg.worker_id}: {msg.key}={msg.data}")

    print("Done!\n")

# %%
await example_echo_pool()

# %% [markdown]
# ## Example: Compute Pool

# %%
def compute_worker(channel, worker_id):
    """Worker that performs computations."""
    print(f"[Worker {worker_id}] Started")
    try:
        while True:
            key, data = channel.recv()

            if key == "square":
                result = data * data
            elif key == "factorial":
                result = 1
                for i in range(1, data + 1):
                    result *= i
            else:
                result = f"unknown: {key}"

            channel.send("result", {"worker_id": worker_id, "input": data, "output": result})
    except ChannelClosed:
        pass

# %%
async def example_compute_pool():
    """Example: compute pool with task distribution."""
    print("=" * 50)
    print("Example 2: Compute Pool")
    print("=" * 50)

    async with ThreadPool(compute_worker, num_workers=2) as pool:
        # Distribute tasks round-robin
        tasks = [
            ("square", 5),
            ("factorial", 6),
            ("square", 10),
            ("factorial", 4),
        ]

        for i, (key, data) in enumerate(tasks):
            worker_id = i % pool.num_workers
            await pool.send(worker_id, key, data)

        # Collect results
        for _ in range(len(tasks)):
            msg = await pool.recv(timeout=5.0)
            print(f"[Main] {msg.data}")

    print("Done!\n")

# %%
await example_compute_pool()

# %% [markdown]
# ## Example: Broadcast

# %%
async def example_broadcast():
    """Example: broadcasting to all workers."""
    print("=" * 50)
    print("Example 3: Broadcast")
    print("=" * 50)

    def broadcast_worker(channel, worker_id):
        try:
            while True:
                key, data = channel.recv()
                print(f"[Worker {worker_id}] Got broadcast: {data}")
                channel.send("ack", f"worker-{worker_id} received")
        except ChannelClosed:
            pass

    async with ThreadPool(broadcast_worker, num_workers=4) as pool:
        # Broadcast to all
        await pool.broadcast("config", {"setting": "value"})

        # Collect acks
        for _ in range(pool.num_workers):
            msg = await pool.recv(timeout=5.0)
            print(f"[Main] {msg.data}")

    print("Done!\n")

# %%
await example_broadcast()
