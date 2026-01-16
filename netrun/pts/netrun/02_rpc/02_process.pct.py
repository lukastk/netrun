# ---
# jupyter:
#   kernelspec:
#     display_name: netrun
#     language: python
#     name: netrun
# ---

# %%
#|default_exp rpc.process

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %% [markdown]
# # Process RPC
#
# Channel for communication with subprocesses using `multiprocessing.Queue`.
# Provides both async (for parent) and sync (for worker) channel classes.
#
# ## Usage
#
# ```python
# import multiprocessing as mp
# from netrun.rpc.process import create_queue_pair, SyncProcessChannel
#
# def worker(send_q, recv_q):
#     channel = SyncProcessChannel(send_q, recv_q)
#     while True:
#         key, data = channel.recv()
#         channel.send("result", process(data))
#
# # Create queues and parent channel
# parent_channel, child_queues = create_queue_pair()
#
# # Start subprocess
# proc = mp.Process(target=worker, args=child_queues)
# proc.start()
#
# # Parent uses async channel
# await parent_channel.send("task", data)
# key, result = await parent_channel.recv()
# ```

# %%
#|export
import asyncio
import multiprocessing as mp
import queue
import threading
from typing import Any
from concurrent.futures import ThreadPoolExecutor

from netrun.rpc.base import (
    ChannelClosed,
    ChannelBroken,
    RecvTimeout,
    SHUTDOWN_KEY,
)

# %% [markdown]
# ## SyncProcessChannel
#
# Synchronous channel for use in worker subprocesses.

# %%
#|export
class SyncProcessChannel:
    """Synchronous RPC channel over multiprocessing queues.

    For use in worker subprocesses. Thread-safe.
    """

    def __init__(
        self,
        send_queue: mp.Queue,
        recv_queue: mp.Queue,
    ):
        """Create a channel from multiprocessing queues.

        Args:
            send_queue: Queue for outgoing messages (to parent)
            recv_queue: Queue for incoming messages (from parent)
        """
        self._send_queue = send_queue
        self._recv_queue = recv_queue
        self._closed = False
        self._lock = threading.Lock()

    def send(self, key: str, data: Any) -> None:
        """Send a message."""
        if self._closed:
            raise ChannelClosed("Channel is closed")

        try:
            self._send_queue.put((key, data))
        except (BrokenPipeError, EOFError, OSError) as e:
            self._closed = True
            raise ChannelBroken(f"Channel broken: {e}")

    def recv(self, timeout: float | None = None) -> tuple[str, Any]:
        """Receive a message with optional timeout."""
        if self._closed:
            raise ChannelClosed("Channel is closed")

        try:
            result = self._recv_queue.get(timeout=timeout)
        except queue.Empty:
            raise RecvTimeout(f"Receive timed out after {timeout}s")
        except (BrokenPipeError, EOFError, OSError) as e:
            self._closed = True
            raise ChannelBroken(f"Channel broken: {e}")

        if result[0] == SHUTDOWN_KEY:
            self._closed = True
            raise ChannelClosed("Channel was shut down")

        return result

    def try_recv(self) -> tuple[str, Any] | None:
        """Non-blocking receive."""
        if self._closed:
            raise ChannelClosed("Channel is closed")

        try:
            result = self._recv_queue.get_nowait()
        except queue.Empty:
            return None
        except (BrokenPipeError, EOFError, OSError) as e:
            self._closed = True
            raise ChannelBroken(f"Channel broken: {e}")

        if result[0] == SHUTDOWN_KEY:
            self._closed = True
            raise ChannelClosed("Channel was shut down")

        return result

    def close(self) -> None:
        """Close the channel."""
        with self._lock:
            if not self._closed:
                self._closed = True
                try:
                    self._send_queue.put_nowait((SHUTDOWN_KEY, None))
                except Exception:
                    pass

    @property
    def is_closed(self) -> bool:
        return self._closed

# %% [markdown]
# ## ProcessChannel
#
# Async channel for use in the parent process.

# %%
#|export
class ProcessChannel:
    """Async RPC channel over multiprocessing queues.

    For use in the parent process. Thread-safe.
    """

    def __init__(
        self,
        send_queue: mp.Queue,
        recv_queue: mp.Queue,
        executor: ThreadPoolExecutor | None = None,
    ):
        """Create a channel from multiprocessing queues.

        Args:
            send_queue: Queue for outgoing messages (to child)
            recv_queue: Queue for incoming messages (from child)
            executor: Thread pool for async operations (created if None)
        """
        self._send_queue = send_queue
        self._recv_queue = recv_queue
        self._executor = executor or ThreadPoolExecutor(max_workers=2)
        self._owns_executor = executor is None
        self._closed = False
        self._lock = threading.Lock()

    async def send(self, key: str, data: Any) -> None:
        """Send a message."""
        if self._closed:
            raise ChannelClosed("Channel is closed")

        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                self._executor,
                self._send_queue.put,
                (key, data),
            )
        except (BrokenPipeError, EOFError, OSError) as e:
            self._closed = True
            raise ChannelBroken(f"Channel broken: {e}")

    async def recv(self, timeout: float | None = None) -> tuple[str, Any]:
        """Receive a message with optional timeout."""
        if self._closed:
            raise ChannelClosed("Channel is closed")

        loop = asyncio.get_running_loop()

        def blocking_recv():
            try:
                return self._recv_queue.get(timeout=timeout)
            except queue.Empty:
                raise RecvTimeout(f"Receive timed out after {timeout}s")

        try:
            result = await loop.run_in_executor(self._executor, blocking_recv)
        except RecvTimeout:
            raise
        except (BrokenPipeError, EOFError, OSError) as e:
            self._closed = True
            raise ChannelBroken(f"Channel broken: {e}")

        if result[0] == SHUTDOWN_KEY:
            self._closed = True
            raise ChannelClosed("Channel was shut down")

        return result

    async def try_recv(self) -> tuple[str, Any] | None:
        """Non-blocking receive."""
        if self._closed:
            raise ChannelClosed("Channel is closed")

        try:
            result = self._recv_queue.get_nowait()
        except queue.Empty:
            return None
        except (BrokenPipeError, EOFError, OSError) as e:
            self._closed = True
            raise ChannelBroken(f"Channel broken: {e}")

        if result[0] == SHUTDOWN_KEY:
            self._closed = True
            raise ChannelClosed("Channel was shut down")

        return result

    async def close(self) -> None:
        """Close the channel."""
        with self._lock:
            if not self._closed:
                self._closed = True
                try:
                    self._send_queue.put_nowait((SHUTDOWN_KEY, None))
                except Exception:
                    pass

                if self._owns_executor:
                    self._executor.shutdown(wait=False)

    @property
    def is_closed(self) -> bool:
        return self._closed

# %% [markdown]
# ## create_queue_pair

# %%
#|export
def create_queue_pair(
    ctx: mp.context.BaseContext | None = None,
) -> tuple[ProcessChannel, tuple[mp.Queue, mp.Queue]]:
    """Create queues and a parent channel for subprocess communication.

    Args:
        ctx: Multiprocessing context (uses spawn by default)

    Returns:
        (parent_channel, (child_send_queue, child_recv_queue))

    Example:
        ```python
        import multiprocessing as mp
        from netrun.rpc.process import create_queue_pair, SyncProcessChannel

        def worker(send_q, recv_q):
            channel = SyncProcessChannel(send_q, recv_q)
            key, data = channel.recv()
            channel.send("echo", data)

        parent_channel, child_queues = create_queue_pair()
        proc = mp.Process(target=worker, args=child_queues)
        proc.start()

        await parent_channel.send("hello", "world")
        key, data = await parent_channel.recv()
        ```
    """
    if ctx is None:
        ctx = mp.get_context("spawn")

    parent_to_child = ctx.Queue()
    child_to_parent = ctx.Queue()

    parent_channel = ProcessChannel(
        send_queue=parent_to_child,
        recv_queue=child_to_parent,
    )

    # Child gets queues in opposite order
    child_queues = (child_to_parent, parent_to_child)

    return parent_channel, child_queues

# %% [markdown]
# ## Example

# %%
import tempfile
import sys
import os
from pathlib import Path

# Create a temporary directory for our worker module
_temp_dir = tempfile.mkdtemp(prefix="rpc_example_")

# Write a simple worker script
_worker_code = '''
"""Dynamically generated worker module."""
import os
from netrun.rpc.process import SyncProcessChannel
from netrun.rpc.base import ChannelClosed

def echo_worker(send_q, recv_q):
    """Simple echo worker."""
    channel = SyncProcessChannel(send_q, recv_q)
    print(f"[Worker] Started in process {os.getpid()}")

    try:
        while True:
            key, data = channel.recv()
            print(f"[Worker] Received: {key}={data}")
            channel.send(f"echo:{key}", data)
    except ChannelClosed:
        print("[Worker] Channel closed, exiting")
'''

_worker_path = Path(_temp_dir) / "dynamic_worker.py"
_worker_path.write_text(_worker_code)

# Add temp dir to sys.path so the subprocess can import it
if _temp_dir not in sys.path:
    sys.path.insert(0, _temp_dir)

# %%
# Now import and use the dynamically created worker
from dynamic_worker import echo_worker
from multiprocessing import Process

async def run_dynamic_example():
    """Run the dynamic worker example."""
    print("=" * 50)
    print("Dynamic Worker Example")
    print("=" * 50)

    # Create queue pair
    parent_channel, child_queues = create_queue_pair()

    # Start subprocess with our dynamic worker
    proc = Process(target=echo_worker, args=child_queues)
    proc.start()
    print(f"[Parent] Started worker process {proc.pid}")

    # Send some messages
    await parent_channel.send("hello", "world")
    await parent_channel.send("number", 42)
    await parent_channel.send("data", {"key": "value"})

    # Receive responses
    for _ in range(3):
        key, data = await parent_channel.recv(timeout=5.0)
        print(f"[Parent] Received: {key}={data}")

    # Clean up
    await parent_channel.close()
    proc.join(timeout=2.0)
    if proc.is_alive():
        proc.terminate()

    print("Done!")

# %%
await run_dynamic_example()
