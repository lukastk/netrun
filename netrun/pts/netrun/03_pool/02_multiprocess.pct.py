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
import datetime
import queue
import sys
import threading
import multiprocessing as mp
from typing import Any

from netrun.rpc.base import ChannelClosed, RecvTimeout, RPC_KEY_SHUTDOWN
from netrun.rpc.multiprocess import (
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
    POOL_UP_ERROR_EXCEPTION,
    POOL_UP_ERROR_CRASHED,
    _check_error_and_raise,
)
from netrun._iutils import get_timestamp_utc

# %% [markdown]
# ## MultiprocessPool Keys
#
# Keys for internal routing in multiprocess pools.
# Downstream: parent → subprocess, Upstream: subprocess → parent

# %%
#|export
MP_DOWN_DISPATCH = "__pool-mp-down:dispatch"
"""Route message to specific worker thread. Data: (thread_idx, key, data)"""

# %%
#|export
MP_DOWN_BROADCAST = "__pool-mp-down:broadcast"
"""Broadcast to all workers in subprocess. Data: (key, data)"""

# %%
#|export
MP_UP_RESPONSE = "__pool-mp-up:response"
"""Wrapper for worker responses. Data: (worker_id, key, data)"""

# %%
#|export
MP_DOWN_FLUSH_STDOUT = "__pool-mp-down:flush-stdout"
"""Request to flush stdout buffer and send it back. Data: None"""

# %%
#|export
MP_UP_STDOUT_BUFFER = "__pool-mp-up:stdout-buffer"
"""Stdout buffer contents. Data: list[tuple[datetime, bool, str]]"""

# %%
#|export
OutputBuffer = list[tuple[datetime.datetime, bool, str]]
"""Type alias for stdout/stderr buffer. Each entry is (timestamp, is_stdout, text)."""

# %% [markdown]
# ## Output Capture
#
# A file-like object that captures writes and stores them in a shared buffer
# with timestamps. Used to redirect stdout/stderr in subprocesses.

# %%
#|export
class _OutputCapture:
    """Captures writes to stdout/stderr with timestamps.

    Args:
        buffer: Shared list to append captured output to
        is_stdout: True for stdout, False for stderr
        buffer_output: If False, discard output instead of buffering
    """

    def __init__(
        self,
        buffer: OutputBuffer,
        is_stdout: bool,
        buffer_output: bool = True,
    ):
        self._buffer = buffer
        self._is_stdout = is_stdout
        self._buffer_output = buffer_output
        self._lock = threading.Lock()

    def write(self, text: str) -> int:
        """Capture a write with timestamp."""
        if text and self._buffer_output:
            with self._lock:
                self._buffer.append((get_timestamp_utc(), self._is_stdout, text))
        return len(text)

    def flush(self) -> None:
        """No-op for compatibility."""
        pass

    def fileno(self) -> int:
        """Return invalid fd - not connected to real file."""
        raise OSError("_OutputCapture does not have a file descriptor")

    def isatty(self) -> bool:
        """Not a TTY."""
        return False

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
    redirect_output: bool = True,
    buffer_output: bool = True,
):
    """Entry point for subprocess. Routes messages to/from worker threads.

    Args:
        parent_send_q: Queue for sending to parent
        parent_recv_q: Queue for receiving from parent
        worker_fn: Worker function to run in threads
        num_threads: Number of worker threads to create
        process_idx: Index of this process
        threads_per_process: Total threads per process (for worker_id calculation)
        redirect_output: If True, capture stdout/stderr
        buffer_output: If True, buffer captured output; if False, discard it
    """
    # Set up stdout/stderr capture if requested
    output_buffer: OutputBuffer = []
    output_buffer_lock = threading.Lock()

    if redirect_output:
        stdout_capture = _OutputCapture(output_buffer, is_stdout=True, buffer_output=buffer_output)
        stderr_capture = _OutputCapture(output_buffer, is_stdout=False, buffer_output=buffer_output)
        sys.stdout = stdout_capture  # type: ignore
        sys.stderr = stderr_capture  # type: ignore

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
                parent_channel.send(MP_UP_RESPONSE, (worker_id, key, data))
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
                parent_channel.send(MP_UP_RESPONSE, (worker_id, key, data))
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

            if key == MP_DOWN_DISPATCH:
                thread_idx, msg_key, msg_data = data
                worker_send_queues[thread_idx].put((msg_key, msg_data))
            elif key == MP_DOWN_BROADCAST:
                msg_key, msg_data = data
                for q in worker_send_queues:
                    q.put((msg_key, msg_data))
            elif key == MP_DOWN_FLUSH_STDOUT:
                # Flush and send back the output buffer
                with output_buffer_lock:
                    buffer_copy = list(output_buffer)
                    output_buffer.clear()
                parent_channel.send(MP_UP_STDOUT_BUFFER, buffer_copy)
            elif key == RPC_KEY_SHUTDOWN:
                break
    except ChannelClosed:
        pass
    finally:
        # Signal workers to stop
        for q in worker_send_queues:
            q.put((RPC_KEY_SHUTDOWN, None))

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
                if key == RPC_KEY_SHUTDOWN:
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
                if key == RPC_KEY_SHUTDOWN:
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
            response_queue.put((worker_id, POOL_UP_ERROR_EXCEPTION, e))
        except Exception:
            # Fallback to dict with error info
            response_queue.put((worker_id, POOL_UP_ERROR_EXCEPTION, {
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
        redirect_output: bool = True,
        buffer_output: bool = True,
    ):
        """Create a multiprocess pool.

        Args:
            worker_fn: Function to run in each worker thread (must be importable)
            num_processes: Number of subprocesses to create
            threads_per_process: Number of worker threads per subprocess
            redirect_output: If True, capture stdout/stderr from subprocesses
            buffer_output: If True, buffer captured output for retrieval.
                          If False, discard captured output (silent mode).
                          Only applies when redirect_output=True.
        """
        if num_processes < 1:
            raise ValueError("num_processes must be at least 1")
        if threads_per_process < 1:
            raise ValueError("threads_per_process must be at least 1")

        self._worker_fn = worker_fn
        self._num_processes = num_processes
        self._threads_per_process = threads_per_process
        self._num_workers = num_processes * threads_per_process
        self._redirect_output = redirect_output
        self._buffer_output = buffer_output
        self._running = False

        # Will be populated on start()
        self._channels: list[ProcessChannel] = []
        self._processes: list[mp.Process] = []
        self._recv_queue: asyncio.Queue = asyncio.Queue()
        self._recv_tasks: list[asyncio.Task] = []
        self._monitor_task: asyncio.Task | None = None
        self._dead_processes: set[int] = set()  # Track processes we've already reported as dead
        self._stdout_buffers: dict[int, OutputBuffer] = {}  # process_idx -> buffer
        self._flush_events: dict[int, asyncio.Event] = {}  # process_idx -> event for flush response

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
        self._stdout_buffers = {}
        self._flush_events = {}

        for process_idx in range(self._num_processes):
            # Create channel pair
            parent_channel, child_queues = create_queue_pair(ctx)
            self._channels.append(parent_channel)

            # Initialize stdout buffer for this process
            self._stdout_buffers[process_idx] = []

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
                    self._redirect_output,
                    self._buffer_output,
                ),
            )
            proc.start()
            self._processes.append(proc)

        self._running = True
        self._dead_processes = set()
        self._monitor_task = asyncio.create_task(self._monitor_processes())

    async def _monitor_processes(self) -> None:
        """Background task to detect dead subprocesses."""
        while self._running:
            for proc_idx, proc in enumerate(self._processes):
                if proc_idx not in self._dead_processes and proc.exitcode is not None:
                    # Process died
                    self._dead_processes.add(proc_idx)
                    exit_info = {
                        "exit_code": proc.exitcode,
                        "reason": f"Process exited with code {proc.exitcode}",
                    }
                    # Signal death for all workers in this process
                    for thread_idx in range(self._threads_per_process):
                        worker_id = proc_idx * self._threads_per_process + thread_idx
                        await self._recv_queue.put(WorkerMessage(
                            worker_id=worker_id,
                            key=POOL_UP_ERROR_CRASHED,
                            data=exit_info
                        ))
            await asyncio.sleep(0.5)  # Check every 500ms

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

        # Cancel monitor task
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

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
        self._monitor_task = None
        self._dead_processes = set()
        self._stdout_buffers = {}
        self._flush_events = {}

    async def send(self, worker_id: WorkerId, key: str, data: Any) -> None:
        """Send a message to a specific worker."""
        if not self._running:
            raise PoolNotStarted("Pool has not been started")

        if worker_id < 0 or worker_id >= self._num_workers:
            raise ValueError(f"worker_id {worker_id} out of range [0, {self._num_workers})")

        process_idx, thread_idx = self._worker_id_to_process_thread(worker_id)
        await self._channels[process_idx].send(MP_DOWN_DISPATCH, (thread_idx, key, data))

    def _start_recv_tasks(self) -> None:
        """Start background tasks that forward messages to the queue."""
        if self._recv_tasks:
            return

        async def recv_loop(process_idx: int, channel: ProcessChannel):
            try:
                while self._running:
                    key, data = await channel.recv()
                    if key == MP_UP_RESPONSE:
                        worker_id, msg_key, msg_data = data
                        msg = WorkerMessage(worker_id=worker_id, key=msg_key, data=msg_data)
                        await self._recv_queue.put(msg)
                    elif key == MP_UP_STDOUT_BUFFER:
                        # Append received buffer to our local buffer for this process
                        self._stdout_buffers[process_idx].extend(data)
                        # Signal that flush response was received
                        if process_idx in self._flush_events:
                            self._flush_events[process_idx].set()
            except (ChannelClosed, asyncio.CancelledError):
                pass
            except Exception:
                pass

        for process_idx, channel in enumerate(self._channels):
            task = asyncio.create_task(recv_loop(process_idx, channel))
            self._recv_tasks.append(task)

    async def recv(self, timeout: float | None = None) -> WorkerMessage:
        """Receive a message from any worker.

        Raises:
            WorkerException: If the worker raised an exception
            WorkerCrashed: If the worker died unexpectedly
            WorkerTimeout: If the worker timed out
            RecvTimeout: If this recv() call times out
        """
        if not self._running:
            raise PoolNotStarted("Pool has not been started")

        self._start_recv_tasks()

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
        """Non-blocking receive from any worker.

        Raises:
            WorkerException: If the worker raised an exception
            WorkerCrashed: If the worker died unexpectedly
            WorkerTimeout: If the worker timed out
        """
        if not self._running:
            raise PoolNotStarted("Pool has not been started")

        # If recv tasks are running, check the queue first
        if self._recv_tasks:
            try:
                msg = self._recv_queue.get_nowait()
                _check_error_and_raise(msg)
                return msg
            except asyncio.QueueEmpty:
                return None

        # Otherwise, read directly from channels
        for process_idx, channel in enumerate(self._channels):
            result = await channel.try_recv()
            if result is not None:
                key, data = result
                if key == MP_UP_RESPONSE:
                    worker_id, msg_key, msg_data = data
                    msg = WorkerMessage(worker_id=worker_id, key=msg_key, data=msg_data)
                    _check_error_and_raise(msg)
                    return msg

        return None

    async def broadcast(self, key: str, data: Any) -> None:
        """Send a message to all workers."""
        if not self._running:
            raise PoolNotStarted("Pool has not been started")

        for channel in self._channels:
            await channel.send(MP_DOWN_BROADCAST, (key, data))

    async def flush_stdout(self, process_idx: int, timeout: float = 5.0) -> OutputBuffer:
        """Flush and retrieve stdout/stderr buffer from a specific process.

        Sends a flush request to the subprocess, waits for the response,
        and returns the buffer contents. The subprocess buffer is cleared.

        Args:
            process_idx: Index of the process (0 to num_processes-1)
            timeout: Maximum time to wait for response in seconds

        Returns:
            List of (timestamp, is_stdout, text) tuples.
            is_stdout is True for stdout, False for stderr.

        Raises:
            PoolNotStarted: If the pool is not running
            ValueError: If process_idx is out of range
            RecvTimeout: If the subprocess doesn't respond in time
        """
        if not self._running:
            raise PoolNotStarted("Pool has not been started")

        if process_idx < 0 or process_idx >= self._num_processes:
            raise ValueError(f"process_idx {process_idx} out of range [0, {self._num_processes})")

        # Ensure recv tasks are running to capture the response
        self._start_recv_tasks()

        # Create an event to wait for the flush response
        event = asyncio.Event()
        self._flush_events[process_idx] = event

        # Send flush request
        await self._channels[process_idx].send(MP_DOWN_FLUSH_STDOUT, None)

        # Wait for response
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except TimeoutError:
            raise RecvTimeout(f"flush_stdout timed out after {timeout}s")
        finally:
            # Clean up event
            self._flush_events.pop(process_idx, None)

        # Extract buffer and clear it
        result = self._stdout_buffers[process_idx]
        self._stdout_buffers[process_idx] = []
        return result

    async def flush_all_stdout(self, timeout: float = 5.0) -> dict[int, OutputBuffer]:
        """Flush and retrieve stdout/stderr buffers from all processes.

        Args:
            timeout: Maximum time to wait for each process response

        Returns:
            Dictionary mapping process_idx to buffer contents.
            Each buffer is a list of (timestamp, is_stdout, text) tuples.

        Raises:
            PoolNotStarted: If the pool is not running
        """
        if not self._running:
            raise PoolNotStarted("Pool has not been started")

        # Flush all processes concurrently
        tasks = [
            self.flush_stdout(process_idx, timeout=timeout)
            for process_idx in range(self._num_processes)
        ]
        results = await asyncio.gather(*tasks)

        return {i: result for i, result in enumerate(results)}

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
