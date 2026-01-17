# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp pool.multiprocess

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %% [markdown]
# # Multiprocess Pool
#
# A pool of worker processes, each running multiple worker threads.
# Uses `multiprocessing.Queue` for communication between the parent
# and subprocesses.
#
# ## Architecture
#
# ```
# Parent Process
#     │
#     ├── ProcessChannel ──► Subprocess 0
#     │                         ├── Thread 0 (worker_id=0)
#     │                         ├── Thread 1 (worker_id=1)
#     │                         └── Thread 2 (worker_id=2)
#     │
#     └── ProcessChannel ──► Subprocess 1
#                             ├── Thread 0 (worker_id=3)
#                             ├── Thread 1 (worker_id=4)
#                             └── Thread 2 (worker_id=5)
# ```
#
# Worker IDs are flat integers: `worker_id = process_idx * threads_per_process + thread_idx`
#
# ## Usage
#
# ```python
# from netrun.pool.multiprocess import MultiprocessPool
#
# # Worker function must be importable (defined at module level)
# def my_worker(channel, worker_id):
#     while True:
#         key, data = channel.recv()
#         channel.send("result", data * 2)
#
# pool = MultiprocessPool(
#     worker_fn=my_worker,
#     num_processes=2,
#     threads_per_process=3,
# )
# await pool.start()
# await pool.send(worker_id=0, key="task", data=10)
# msg = await pool.recv()
# await pool.close()
# ```

# %%
#|export
import asyncio
import queue
import threading
import multiprocessing as mp
from typing import Any

from netrun.rpc.base import ChannelClosed, RecvTimeout, SHUTDOWN_KEY
from netrun.rpc.process import (
    ProcessChannel,
    SyncProcessChannel,
    create_queue_pair,
)
from netrun.pool.base import (
    WorkerId,
    WorkerFn,
    WorkerMessage,
    PoolNotStarted,
    PoolAlreadyStarted,
)

# %% [markdown]
# ## Subprocess Worker Entry Point
#
# This function runs in each subprocess. It creates worker threads and
# routes messages between the parent and workers.

# %%
#|export
def _subprocess_main(
    parent_send_q: mp.Queue,
    parent_recv_q: mp.Queue,
    worker_fn: WorkerFn,
    num_threads: int,
    process_idx: int,
    threads_per_process: int,
):
    """Entry point for subprocess. Routes messages to/from worker threads."""
    # Create channel to parent
    parent_channel = SyncProcessChannel(parent_send_q, parent_recv_q)

    # Create thread-safe queues for each worker
    # worker_queues[thread_idx] = (send_to_worker, recv_from_worker)
    worker_send_queues: list[queue.Queue] = [queue.Queue() for _ in range(num_threads)]
    response_queue: queue.Queue = queue.Queue()  # All workers send responses here

    # Start worker threads
    threads = []
    for thread_idx in range(num_threads):
        worker_id = process_idx * threads_per_process + thread_idx
        t = threading.Thread(
            target=_thread_worker,
            args=(worker_fn, worker_send_queues[thread_idx], response_queue, worker_id),
            daemon=True,
        )
        t.start()
        threads.append(t)

    # Router loop: handle messages from parent and responses from workers
    shutdown = False

    def response_forwarder():
        """Forward responses from workers to parent."""
        while not shutdown:
            try:
                msg = response_queue.get(timeout=None)
                if msg is None:
                    break
                worker_id, key, data = msg
                parent_channel.send("response", (worker_id, key, data))
            except queue.Empty:
                continue
            except Exception:
                break

        # Drain any remaining messages before exiting
        while True:
            try:
                msg = response_queue.get_nowait()
                if msg is None:
                    break
                worker_id, key, data = msg
                parent_channel.send("response", (worker_id, key, data))
            except queue.Empty:
                break
            except Exception:
                break

    # Start response forwarder thread
    forwarder = threading.Thread(target=response_forwarder, daemon=True)
    forwarder.start()

    # Main loop: receive from parent and dispatch to workers
    try:
        while True:
            key, data = parent_channel.recv()

            if key == "__dispatch__":
                thread_idx, msg_key, msg_data = data
                worker_send_queues[thread_idx].put((msg_key, msg_data))
            elif key == "__broadcast__":
                msg_key, msg_data = data
                for q in worker_send_queues:
                    q.put((msg_key, msg_data))
            elif key == SHUTDOWN_KEY:
                break
    except ChannelClosed:
        pass
    finally:
        # Signal workers to stop
        for q in worker_send_queues:
            q.put((SHUTDOWN_KEY, None))

        # Wait for worker threads to finish sending their responses
        for t in threads:
            t.join()

        # Now signal forwarder to stop (after workers are done)
        shutdown = True
        response_queue.put(None)

        # Wait for forwarder to finish draining
        forwarder.join()

# %%
#|export
def _thread_worker(
    worker_fn: WorkerFn,
    recv_queue: queue.Queue,
    response_queue: queue.Queue,
    worker_id: WorkerId,
):
    """Run worker function in a thread within subprocess."""

    class _WorkerChannel:
        """Adapter that looks like SyncRPCChannel for the worker."""
        def __init__(self):
            self._closed = False

        def send(self, key: str, data: Any) -> None:
            if self._closed:
                raise ChannelClosed("Channel is closed")
            response_queue.put((worker_id, key, data))

        def recv(self, timeout: float | None = None) -> tuple[str, Any]:
            if self._closed:
                raise ChannelClosed("Channel is closed")
            try:
                key, data = recv_queue.get(timeout=timeout)
                if key == SHUTDOWN_KEY:
                    self._closed = True
                    raise ChannelClosed("Channel was shut down")
                return key, data
            except queue.Empty:
                from netrun.rpc.base import RecvTimeout
                raise RecvTimeout(f"Receive timed out after {timeout}s")

        def try_recv(self) -> tuple[str, Any] | None:
            if self._closed:
                raise ChannelClosed("Channel is closed")
            try:
                key, data = recv_queue.get_nowait()
                if key == SHUTDOWN_KEY:
                    self._closed = True
                    raise ChannelClosed("Channel was shut down")
                return key, data
            except queue.Empty:
                return None

        def close(self) -> None:
            self._closed = True

        @property
        def is_closed(self) -> bool:
            return self._closed

    channel = _WorkerChannel()
    try:
        worker_fn(channel, worker_id)
    except ChannelClosed:
        pass
    except Exception as e:
        # Send error back - try exception object first, fallback to dict if unpickleable
        import pickle
        import traceback
        try:
            pickle.dumps(e)  # Test if pickleable
            response_queue.put((worker_id, "__error__", e))
        except Exception:
            # Fallback to dict with error info
            response_queue.put((worker_id, "__error__", {
                "type": type(e).__name__,
                "message": str(e),
                "traceback": traceback.format_exc(),
            }))

# %% [markdown]
# ## MultiprocessPool

# %%
#|export
class MultiprocessPool:
    """A pool of worker processes, each running multiple threads.

    Messages are routed to specific workers via their worker_id.
    Worker IDs are flat integers from 0 to (num_processes * threads_per_process - 1).
    """

    def __init__(
        self,
        worker_fn: WorkerFn,
        num_processes: int,
        threads_per_process: int = 1,
    ):
        """Create a multiprocess pool.

        Args:
            worker_fn: Function to run in each worker thread (must be importable)
            num_processes: Number of subprocesses to create
            threads_per_process: Number of worker threads per subprocess
        """
        if num_processes < 1:
            raise ValueError("num_processes must be at least 1")
        if threads_per_process < 1:
            raise ValueError("threads_per_process must be at least 1")

        self._worker_fn = worker_fn
        self._num_processes = num_processes
        self._threads_per_process = threads_per_process
        self._num_workers = num_processes * threads_per_process
        self._running = False

        # Will be populated on start()
        self._channels: list[ProcessChannel] = []
        self._processes: list[mp.Process] = []
        self._recv_queue: asyncio.Queue = asyncio.Queue()
        self._recv_tasks: list[asyncio.Task] = []

    @property
    def num_workers(self) -> int:
        """Total number of workers in the pool."""
        return self._num_workers

    @property
    def num_processes(self) -> int:
        """Number of subprocesses."""
        return self._num_processes

    @property
    def threads_per_process(self) -> int:
        """Number of threads per subprocess."""
        return self._threads_per_process

    @property
    def is_running(self) -> bool:
        """Whether the pool has been started."""
        return self._running

    def _worker_id_to_process_thread(self, worker_id: WorkerId) -> tuple[int, int]:
        """Convert flat worker_id to (process_idx, thread_idx)."""
        process_idx = worker_id // self._threads_per_process
        thread_idx = worker_id % self._threads_per_process
        return process_idx, thread_idx

    async def start(self) -> None:
        """Start all processes and workers."""
        if self._running:
            raise PoolAlreadyStarted("Pool is already running")

        ctx = mp.get_context("spawn")
        self._channels = []
        self._processes = []

        for process_idx in range(self._num_processes):
            # Create channel pair
            parent_channel, child_queues = create_queue_pair(ctx)
            self._channels.append(parent_channel)

            # Create and start subprocess
            proc = ctx.Process(
                target=_subprocess_main,
                args=(
                    child_queues[0],  # send_q
                    child_queues[1],  # recv_q
                    self._worker_fn,
                    self._threads_per_process,
                    process_idx,
                    self._threads_per_process,
                ),
            )
            proc.start()
            self._processes.append(proc)

        self._running = True

    async def close(self, timeout: float | None = None) -> None:
        """Shut down all processes and clean up resources.

        Args:
            timeout: Max seconds to wait for each process to finish gracefully.
                     If None, wait indefinitely. If timeout expires, processes
                     are forcefully terminated.
        """
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

        # Wait for processes to finish
        for proc in self._processes:
            proc.join(timeout=timeout)
            if proc.is_alive():
                proc.terminate()

        self._channels = []
        self._processes = []
        self._recv_queue = asyncio.Queue()
        self._recv_tasks = []

    async def send(self, worker_id: WorkerId, key: str, data: Any) -> None:
        """Send a message to a specific worker."""
        if not self._running:
            raise PoolNotStarted("Pool has not been started")

        if worker_id < 0 or worker_id >= self._num_workers:
            raise ValueError(f"worker_id {worker_id} out of range [0, {self._num_workers})")

        process_idx, thread_idx = self._worker_id_to_process_thread(worker_id)
        await self._channels[process_idx].send("__dispatch__", (thread_idx, key, data))

    def _start_recv_tasks(self) -> None:
        """Start background tasks that forward messages to the queue."""
        if self._recv_tasks:
            return

        async def recv_loop(process_idx: int, channel: ProcessChannel):
            try:
                while self._running:
                    key, data = await channel.recv()
                    if key == "response":
                        worker_id, msg_key, msg_data = data
                        msg = WorkerMessage(worker_id=worker_id, key=msg_key, data=msg_data)
                        await self._recv_queue.put(msg)
            except (ChannelClosed, asyncio.CancelledError):
                pass
            except Exception:
                pass

        for process_idx, channel in enumerate(self._channels):
            task = asyncio.create_task(recv_loop(process_idx, channel))
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

        for process_idx, channel in enumerate(self._channels):
            result = await channel.try_recv()
            if result is not None:
                key, data = result
                if key == "response":
                    worker_id, msg_key, msg_data = data
                    return WorkerMessage(worker_id=worker_id, key=msg_key, data=msg_data)

        return None

    async def broadcast(self, key: str, data: Any) -> None:
        """Send a message to all workers."""
        if not self._running:
            raise PoolNotStarted("Pool has not been started")

        for channel in self._channels:
            await channel.send("__broadcast__", (key, data))

    async def __aenter__(self) -> "MultiprocessPool":
        """Context manager entry - starts the pool."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - closes the pool."""
        await self.close()

# %% [markdown]
# ## Example
#
# Note: For multiprocessing with spawn context, the worker function must be
# defined at module level (importable). We'll create a temp module for the example.

# %%
import tempfile
import sys
from pathlib import Path

# Create temp module with worker function
_temp_dir = tempfile.mkdtemp(prefix="pool_example_")
_worker_code = '''
"""Worker functions for multiprocess pool example."""
from netrun.rpc.base import ChannelClosed

def echo_worker(channel, worker_id):
    """Echo worker that runs in subprocess."""
    import os
    print(f"[Worker {worker_id}] Started process")
    try:
        while True:
            key, data = channel.recv()
            print(f"[Worker {worker_id}] Received: {key}={data}")
            channel.send(f"echo:{key}", {"worker_id": worker_id, "data": data})
    except ChannelClosed:
        print(f"[Worker {worker_id}] Stopping")

def compute_worker(channel, worker_id):
    """Compute worker that runs in subprocess."""
    try:
        while True:
            key, data = channel.recv()
            if key == "square":
                result = data * data
            elif key == "double":
                result = data * 2
            else:
                result = f"unknown: {key}"
            channel.send("result", {"worker_id": worker_id, "result": result})
    except ChannelClosed:
        pass
'''

_worker_path = Path(_temp_dir) / "mp_workers.py"
_worker_path.write_text(_worker_code)
print(f"Created worker module at: {_worker_path}")

if _temp_dir not in sys.path:
    sys.path.insert(0, _temp_dir)

# %%
from mp_workers import echo_worker, compute_worker
from netrun.pool.multiprocess import MultiprocessPool as _MultiprocessPool

async def example_multiprocess_pool():
    """Example: multiprocess pool with 2 processes, 2 threads each."""
    print("=" * 50)
    print("Example 1: Multiprocess Pool")
    print("=" * 50)

    async with _MultiprocessPool(echo_worker, num_processes=2, threads_per_process=2) as pool:
        print(f"Pool has {pool.num_workers} workers across {pool.num_processes} processes")

        # Send to each worker
        for worker_id in range(pool.num_workers):
            await pool.send(worker_id, "hello", f"message-{worker_id}")

        # Receive all responses
        for _ in range(pool.num_workers):
            msg = await pool.recv(timeout=5.0)
            print(f"[Main] Got from worker {msg.worker_id}: {msg.key}={msg.data}")

    print("Done!\n")

# %%
await example_multiprocess_pool()

# %%
async def example_compute_multiprocess():
    """Example: compute tasks distributed across processes."""
    print("=" * 50)
    print("Example 2: Distributed Compute")
    print("=" * 50)

    async with _MultiprocessPool(compute_worker, num_processes=2, threads_per_process=2) as pool:
        # Distribute tasks
        tasks = [("square", 5), ("double", 10), ("square", 7), ("double", 3)]

        for i, (key, data) in enumerate(tasks):
            worker_id = i % pool.num_workers
            await pool.send(worker_id, key, data)

        # Collect results
        for _ in range(len(tasks)):
            msg = await pool.recv(timeout=5.0)
            print(f"[Main] Worker {msg.worker_id}: {msg.data}")

    print("Done!\n")

# %%
await example_compute_multiprocess()

# %%
# Clean up temp module
import shutil
shutil.rmtree(_temp_dir, ignore_errors=True)
print(f"Cleaned up: {_temp_dir}")
