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
import logging
import queue
import sys
import threading
import multiprocessing as mp
from typing import Any
from collections.abc import Callable

from netrun.rpc.base import ChannelClosed, RecvTimeout, RPC_KEY_SHUTDOWN
from netrun.rpc.multiprocess import (
    ProcessChannel,
    SyncProcessChannel,
    create_queue_pair,
)
from netrun.pool.base import (
    BasePool,
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
MP_DOWN_SHUTDOWN = "__pool-mp-down:shutdown"
"""Signal subprocess to shut down gracefully. Data: None.

Note: This is different from RPC_KEY_SHUTDOWN which triggers immediate channel close.
This key allows the subprocess to complete its shutdown sequence (including final
flush and SHUTDOWN_COMPLETE signal) before the channel is closed."""

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
MP_UP_SHUTDOWN_COMPLETE = "__pool-mp-up:shutdown-complete"
"""Signal from subprocess that shutdown is complete and final buffer has been sent. Data: None"""

# %%
#|export
MP_UP_SUBPROCESS_ERROR = "__pool-mp-up:subprocess-error"
"""Signal from subprocess that a fatal error occurred. Data: dict with error info."""

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
    output_flush_interval: float = 0.1,
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
        output_flush_interval: Interval in seconds between automatic output buffer flushes
    """
    # Top-level error handling - catch any fatal errors and report back to parent
    import traceback
    parent_channel_for_errors = SyncProcessChannel(parent_send_q, parent_recv_q)
    try:
        _subprocess_main_inner(
            parent_send_q, parent_recv_q, worker_fn, num_threads,
            process_idx, threads_per_process, redirect_output,
            buffer_output, output_flush_interval
        )
    except Exception as e:
        # Send error back to parent
        try:
            parent_channel_for_errors.send(MP_UP_SUBPROCESS_ERROR, {
                "type": type(e).__name__,
                "message": str(e),
                "traceback": traceback.format_exc(),
            })
        except Exception:
            pass  # Channel might be closed
        raise  # Re-raise to ensure non-zero exit code

# %%
#|export
def _subprocess_main_inner(
    parent_send_q: mp.Queue,
    parent_recv_q: mp.Queue,
    worker_fn: WorkerFn,
    num_threads: int,
    process_idx: int,
    threads_per_process: int,
    redirect_output: bool = True,
    buffer_output: bool = True,
    output_flush_interval: float = 0.1,
):
    """Inner implementation of subprocess main. Wrapped by _subprocess_main for error handling."""
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

    # Create process-local shared event loop for async function dispatch.
    # Each subprocess gets its own loop (event loops can't cross process boundaries).
    shared_loop = asyncio.new_event_loop()
    shared_loop_thread = threading.Thread(
        target=shared_loop.run_forever,
        daemon=True,
        name=f"netrun-subprocess-{process_idx}-async-loop",
    )
    shared_loop_thread.start()

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
            kwargs={"shared_loop": shared_loop},
            daemon=True,
        )
        t.start()
        threads.append(t)

    # Router loop: handle messages from parent and responses from workers
    shutdown = False
    shutdown_event = threading.Event()

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

    def output_flusher():
        """Periodically flush output buffer to parent."""
        while not shutdown:
            # Wait for shutdown_event or timeout - allows fast wakeup on shutdown
            shutdown_event.wait(timeout=output_flush_interval)
            if shutdown:
                break
            with output_buffer_lock:
                if output_buffer:
                    buffer_copy = list(output_buffer)
                    output_buffer.clear()
                    try:
                        parent_channel.send(MP_UP_STDOUT_BUFFER, buffer_copy)
                    except Exception:
                        # Channel might be closed during shutdown
                        break

    # Start response forwarder thread
    forwarder = threading.Thread(target=response_forwarder, daemon=True)
    forwarder.start()

    # Start output flusher thread if output is being captured and buffered
    flusher = None
    if redirect_output and buffer_output:
        flusher = threading.Thread(target=output_flusher, daemon=True)
        flusher.start()

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
            elif key == MP_DOWN_SHUTDOWN:
                # Graceful shutdown - allows us to send SHUTDOWN_COMPLETE before channel closes
                break
    except ChannelClosed:
        pass
    finally:
        # Signal shutdown to background threads
        shutdown = True
        shutdown_event.set()  # Wake up flusher immediately

        # Signal workers to stop
        for q in worker_send_queues:
            q.put((RPC_KEY_SHUTDOWN, None))

        # Wait for worker threads to finish sending their responses
        for t in threads:
            t.join()

        # Now signal forwarder to stop (after workers are done)
        response_queue.put(None)

        # Wait for forwarder to finish draining
        forwarder.join()

        # Flusher will exit quickly due to shutdown_event being set
        if flusher is not None:
            flusher.join(timeout=1.0)  # Short timeout since we signaled shutdown_event

        # Final flush of any remaining output
        if redirect_output and buffer_output:
            with output_buffer_lock:
                if output_buffer:
                    try:
                        parent_channel.send(MP_UP_STDOUT_BUFFER, list(output_buffer))
                        output_buffer.clear()
                    except Exception:
                        pass

        # Stop the process-local shared event loop
        shared_loop.call_soon_threadsafe(shared_loop.stop)
        shared_loop_thread.join(timeout=5.0)
        shared_loop.close()

        # Signal that shutdown is complete
        try:
            parent_channel.send(MP_UP_SHUTDOWN_COMPLETE, None)
        except Exception:
            pass

# %%
#|export
def _thread_worker(
    worker_fn: WorkerFn,
    recv_queue: queue.Queue,
    response_queue: queue.Queue,
    worker_id: WorkerId,
    shared_loop: asyncio.AbstractEventLoop | None = None,
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
        # Only pass shared_loop if the worker function accepts it.
        # ExecutionManager worker functions accept it; raw pool worker functions don't.
        import inspect
        _accepts_shared_loop = False
        if shared_loop is not None:
            try:
                sig = inspect.signature(worker_fn)
                _accepts_shared_loop = "shared_loop" in sig.parameters
            except (ValueError, TypeError):
                pass
        if _accepts_shared_loop:
            worker_fn(channel, worker_id, shared_loop=shared_loop)
        else:
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
class MultiprocessPool(BasePool):
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
        output_flush_interval: float = 0.1,
        on_output: Callable[[int, OutputBuffer], None] | None = None,
        **kwargs,
    ):
        if num_processes < 1:
            raise ValueError("num_processes must be at least 1")
        if threads_per_process < 1:
            raise ValueError("threads_per_process must be at least 1")

        super().__init__(num_workers=num_processes * threads_per_process, **kwargs)
        self._worker_fn = worker_fn
        self._num_processes = num_processes
        self._threads_per_process = threads_per_process
        self._redirect_output = redirect_output
        self._buffer_output = buffer_output
        self._output_flush_interval = output_flush_interval
        self._on_output = on_output

        self._channels: list[ProcessChannel] = []
        self._processes: list[mp.Process] = []
        self._dead_processes: set[int] = set()
        self._stdout_buffers: dict[int, OutputBuffer] = {}
        self._flush_events: dict[int, asyncio.Event] = {}
        self._shutdown_complete_events: dict[int, asyncio.Event] = {}

    @property
    def num_processes(self) -> int:
        return self._num_processes

    @property
    def threads_per_process(self) -> int:
        return self._threads_per_process

    def _worker_id_to_process_thread(self, worker_id: WorkerId) -> tuple[int, int]:
        process_idx = worker_id // self._threads_per_process
        thread_idx = worker_id % self._threads_per_process
        return process_idx, thread_idx

    async def _do_start(self) -> None:
        ctx = mp.get_context("spawn")
        self._channels = []
        self._processes = []
        self._stdout_buffers = {}
        self._flush_events = {}
        self._shutdown_complete_events = {i: asyncio.Event() for i in range(self._num_processes)}
        self._dead_processes = set()

        for process_idx in range(self._num_processes):
            parent_channel, child_queues = create_queue_pair(ctx)
            self._channels.append(parent_channel)
            self._stdout_buffers[process_idx] = []

            proc = ctx.Process(
                target=_subprocess_main,
                args=(
                    child_queues[0], child_queues[1],
                    self._worker_fn, self._threads_per_process,
                    process_idx, self._threads_per_process,
                    self._redirect_output, self._buffer_output,
                    self._output_flush_interval,
                ),
            )
            proc.start()
            self._processes.append(proc)

    async def close(self, timeout: float | None = None) -> None:
        """Shut down all processes and clean up resources.

        Overrides BasePool.close() because MultiprocessPool needs to:
        1. Start recv tasks BEFORE setting _running=False (to receive SHUTDOWN_COMPLETE)
        2. Send MP_DOWN_SHUTDOWN and wait for SHUTDOWN_COMPLETE
        3. Close channels, then cancel recv tasks, then join processes
        """
        if not self._running:
            return
        self._start_recv_tasks()
        self._running = False

        # Cancel monitor
        if self._monitor_task is not None and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        self._monitor_task = None

        # Send shutdown to each subprocess
        for channel in self._channels:
            try:
                await channel.send(MP_DOWN_SHUTDOWN, None)
            except Exception:
                pass

        # Wait for SHUTDOWN_COMPLETE from all processes
        shutdown_timeout = timeout if timeout is not None else 30.0
        try:
            await asyncio.wait_for(
                asyncio.gather(*[
                    self._shutdown_complete_events[i].wait()
                    for i in range(self._num_processes)
                ]),
                timeout=shutdown_timeout,
            )
        except TimeoutError:
            pass

        # Close channels
        for channel in self._channels:
            await channel.close()

        # Cancel recv tasks
        for task in self._recv_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._recv_tasks.clear()

        # Join/terminate processes
        for proc in self._processes:
            proc.join(timeout=timeout)
            if proc.is_alive():
                proc.terminate()

        self._channels = []
        self._processes = []
        self._recv_queue = asyncio.Queue()
        self._dead_processes = set()
        self._stdout_buffers = {}
        self._flush_events = {}
        self._shutdown_complete_events = {}

    async def _do_close(self, timeout: float | None = None) -> None:
        # Not used — close() is overridden entirely
        pass

    def _create_recv_loops(self) -> list:
        loops = []
        for process_idx, channel in enumerate(self._channels):
            async def recv_loop(_proc_idx=process_idx, _channel=channel):
                try:
                    while True:
                        try:
                            key, data = await _channel.recv(timeout=1.0)
                        except RecvTimeout:
                            if _proc_idx in self._dead_processes:
                                break
                            continue

                        if key == MP_UP_RESPONSE:
                            worker_id, msg_key, msg_data = data
                            await self._recv_queue.put(
                                WorkerMessage(worker_id=worker_id, key=msg_key, data=msg_data)
                            )
                        elif key == MP_UP_SUBPROCESS_ERROR:
                            for thread_idx in range(self._threads_per_process):
                                worker_id = _proc_idx * self._threads_per_process + thread_idx
                                await self._recv_queue.put(WorkerMessage(
                                    worker_id=worker_id, key=POOL_UP_ERROR_EXCEPTION, data=data,
                                ))
                        elif key == MP_UP_STDOUT_BUFFER:
                            self._stdout_buffers[_proc_idx].extend(data)
                            if self._on_output is not None and data:
                                self._on_output(_proc_idx, data)
                            if _proc_idx in self._flush_events:
                                self._flush_events[_proc_idx].set()
                        elif key == MP_UP_SHUTDOWN_COMPLETE:
                            if _proc_idx in self._shutdown_complete_events:
                                self._shutdown_complete_events[_proc_idx].set()
                            break
                except (ChannelClosed, asyncio.CancelledError):
                    pass
                except Exception as e:
                    logging.getLogger("netrun.pool").error(
                        f"recv_loop for process {_proc_idx} crashed: {e}", exc_info=True
                    )
                    for worker_id in range(
                        _proc_idx * self._threads_per_process,
                        (_proc_idx + 1) * self._threads_per_process,
                    ):
                        await self._recv_queue.put(WorkerMessage(
                            worker_id=worker_id, key=POOL_UP_ERROR_CRASHED,
                            data={"reason": f"recv_loop exception: {e}"},
                        ))
            loops.append(recv_loop())
        return loops

    async def _check_worker_health(self) -> None:
        for proc_idx, proc in enumerate(self._processes):
            if proc_idx not in self._dead_processes and proc.exitcode is not None:
                self._dead_processes.add(proc_idx)
                exit_info = {
                    "exit_code": proc.exitcode,
                    "reason": f"Process exited with code {proc.exitcode}",
                }
                for thread_idx in range(self._threads_per_process):
                    worker_id = proc_idx * self._threads_per_process + thread_idx
                    await self._recv_queue.put(WorkerMessage(
                        worker_id=worker_id, key=POOL_UP_ERROR_CRASHED, data=exit_info,
                    ))
                try:
                    await self._channels[proc_idx].close()
                except Exception:
                    pass
                if proc_idx in self._shutdown_complete_events:
                    self._shutdown_complete_events[proc_idx].set()

    async def _try_recv_direct(self) -> WorkerMessage | None:
        """Direct non-blocking receive (when recv tasks aren't running)."""
        for process_idx, channel in enumerate(self._channels):
            result = await channel.try_recv()
            if result is not None:
                key, data = result
                if key == MP_UP_RESPONSE:
                    worker_id, msg_key, msg_data = data
                    return WorkerMessage(worker_id=worker_id, key=msg_key, data=msg_data)
        return None

    async def send(self, worker_id: WorkerId, key: str, data: Any) -> None:
        if not self._running:
            raise PoolNotStarted("Pool has not been started")
        if worker_id < 0 or worker_id >= self._num_workers:
            raise ValueError(f"worker_id {worker_id} out of range [0, {self._num_workers})")
        process_idx, thread_idx = self._worker_id_to_process_thread(worker_id)
        await self._channels[process_idx].send(MP_DOWN_DISPATCH, (thread_idx, key, data))

    async def broadcast(self, key: str, data: Any) -> None:
        if not self._running:
            raise PoolNotStarted("Pool has not been started")
        for channel in self._channels:
            await channel.send(MP_DOWN_BROADCAST, (key, data))

    async def flush_stdout(self, process_idx: int, timeout: float | None = None) -> OutputBuffer:
        """Flush and retrieve stdout/stderr buffer from a specific process."""
        if not self._running:
            raise PoolNotStarted("Pool has not been started")
        if process_idx < 0 or process_idx >= self._num_processes:
            raise ValueError(f"process_idx {process_idx} out of range [0, {self._num_processes})")

        self._start_recv_tasks()
        event = asyncio.Event()
        self._flush_events[process_idx] = event
        await self._channels[process_idx].send(MP_DOWN_FLUSH_STDOUT, None)
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except TimeoutError:
            raise RecvTimeout(f"flush_stdout timed out after {timeout}s")
        finally:
            self._flush_events.pop(process_idx, None)
        result = self._stdout_buffers[process_idx]
        self._stdout_buffers[process_idx] = []
        return result

    async def flush_all_stdout(self, timeout: float | None = None) -> dict[int, OutputBuffer]:
        """Flush and retrieve stdout/stderr buffers from all processes."""
        if not self._running:
            raise PoolNotStarted("Pool has not been started")
        tasks = [self.flush_stdout(i, timeout=timeout) for i in range(self._num_processes)]
        results = await asyncio.gather(*tasks)
        return {i: result for i, result in enumerate(results)}

# %% [markdown]
# ## Example
#
# Note: For multiprocessing with spawn context, the worker function must be
# defined at module level (importable). We'll create a temp module for the example.

# %%
import tempfile
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
