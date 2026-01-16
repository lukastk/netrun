# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp rpc.thread

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %% [markdown]
# # Thread RPC
#
# Channel for communication between OS threads using `queue.Queue`.
# Provides both async (for main thread) and sync (for worker threads) channel classes.
#
# ## Usage
#
# ```python
# import threading
# from netrun.rpc.thread import create_thread_channel_pair, SyncThreadChannel
#
# def worker(send_q, recv_q):
#     channel = SyncThreadChannel(send_q, recv_q)
#     while True:
#         key, data = channel.recv()
#         channel.send("result", process(data))
#
# # Create queues and parent channel
# parent_channel, child_queues = create_thread_channel_pair()
#
# # Start thread
# thread = threading.Thread(target=worker, args=child_queues)
# thread.start()
#
# # Parent uses async channel
# await parent_channel.send("task", data)
# key, result = await parent_channel.recv()
# ```

# %%
#|export
import asyncio
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
# ## SyncThreadChannel
#
# Synchronous channel for use in worker threads.

# %%
#|export
class SyncThreadChannel:
    """Synchronous RPC channel over thread-safe queues.

    For use in worker threads. Thread-safe.
    """

    def __init__(
        self,
        send_queue: queue.Queue,
        recv_queue: queue.Queue,
    ):
        """Create a channel from thread-safe queues.

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
        except Exception as e:
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
        except Exception as e:
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
        except Exception as e:
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
# ## ThreadChannel
#
# Async channel for use in the main thread with an event loop.

# %%
#|export
class ThreadChannel:
    """Async RPC channel over thread-safe queues.

    For use in the main thread with an asyncio event loop. Thread-safe.
    """

    def __init__(
        self,
        send_queue: queue.Queue,
        recv_queue: queue.Queue,
        executor: ThreadPoolExecutor | None = None,
    ):
        """Create a channel from thread-safe queues.

        Args:
            send_queue: Queue for outgoing messages (to worker)
            recv_queue: Queue for incoming messages (from worker)
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
        except Exception as e:
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
        except Exception as e:
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
        except Exception as e:
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
                # Send shutdown to worker
                try:
                    self._send_queue.put_nowait((SHUTDOWN_KEY, None))
                except Exception:
                    pass
                # Also put shutdown on recv queue to unblock any recv() calls
                try:
                    self._recv_queue.put_nowait((SHUTDOWN_KEY, None))
                except Exception:
                    pass

                if self._owns_executor:
                    self._executor.shutdown(wait=False)

    @property
    def is_closed(self) -> bool:
        return self._closed

# %% [markdown]
# ## create_thread_channel_pair

# %%
#|export
def create_thread_channel_pair() -> tuple[ThreadChannel, tuple[queue.Queue, queue.Queue]]:
    """Create queues and a parent channel for thread communication.

    Returns:
        (parent_channel, (child_send_queue, child_recv_queue))

    Example:
        ```python
        import threading
        from netrun.rpc.thread import create_thread_channel_pair, SyncThreadChannel

        def worker(send_q, recv_q):
            channel = SyncThreadChannel(send_q, recv_q)
            key, data = channel.recv()
            channel.send("echo", data)

        parent_channel, child_queues = create_thread_channel_pair()
        thread = threading.Thread(target=worker, args=child_queues)
        thread.start()

        await parent_channel.send("hello", "world")
        key, data = await parent_channel.recv()
        ```
    """
    parent_to_child: queue.Queue = queue.Queue()
    child_to_parent: queue.Queue = queue.Queue()

    parent_channel = ThreadChannel(
        send_queue=parent_to_child,
        recv_queue=child_to_parent,
    )

    # Child gets queues in opposite order
    child_queues = (child_to_parent, parent_to_child)

    return parent_channel, child_queues

# %% [markdown]
# ## Example: Worker Thread
#
# This example demonstrates communication with a worker thread.

# %%

async def example_echo_worker():
    """Example: simple echo worker thread."""
    print("=" * 50)
    print("Example 1: Echo Worker Thread")
    print("=" * 50)

    def echo_worker(send_q, recv_q):
        """Worker that echoes back messages."""
        channel = SyncThreadChannel(send_q, recv_q)
        thread_id = threading.current_thread().name
        print(f"[Worker {thread_id}] Started")

        try:
            while True:
                key, data = channel.recv()
                print(f"[Worker {thread_id}] Received: {key}={data}")
                channel.send(f"echo:{key}", data)
        except ChannelClosed:
            print(f"[Worker {thread_id}] Channel closed, exiting")

    # Create channel pair
    parent_channel, child_queues = create_thread_channel_pair()

    # Start worker thread
    thread = threading.Thread(target=echo_worker, args=child_queues, name="EchoWorker")
    thread.start()

    # Send some messages
    await parent_channel.send("hello", "world")
    await parent_channel.send("number", 42)
    await parent_channel.send("data", {"key": "value"})

    # Receive responses
    for _ in range(3):
        key, data = await parent_channel.recv(timeout=5.0)
        print(f"[Main] Received: {key}={data}")

    # Clean up
    await parent_channel.close()
    thread.join(timeout=2.0)

    print("Done!\n")

# %%
await example_echo_worker()

# %%
async def example_compute_worker():
    """Example: compute worker thread."""
    print("=" * 50)
    print("Example 2: Compute Worker Thread")
    print("=" * 50)

    def compute_worker(send_q, recv_q):
        """Worker that performs computations."""
        channel = SyncThreadChannel(send_q, recv_q)
        print("[Worker] Started")

        try:
            while True:
                key, data = channel.recv()
                print(f"[Worker] Computing: {key}({data})")

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

                channel.send("result", result)
        except ChannelClosed:
            print("[Worker] Channel closed, exiting")

    parent_channel, child_queues = create_thread_channel_pair()
    thread = threading.Thread(target=compute_worker, args=child_queues)
    thread.start()

    # Test various computations
    await parent_channel.send("square", 7)
    _, result = await parent_channel.recv(timeout=5.0)
    print(f"square(7) = {result}")

    await parent_channel.send("factorial", 5)
    _, result = await parent_channel.recv(timeout=5.0)
    print(f"factorial(5) = {result}")

    await parent_channel.send("sum", [1, 2, 3, 4, 5])
    _, result = await parent_channel.recv(timeout=5.0)
    print(f"sum([1,2,3,4,5]) = {result}")

    await parent_channel.close()
    thread.join(timeout=2.0)

    print("Done!\n")

# %%
await example_compute_worker()

# %%
async def example_multiple_workers():
    """Example: multiple worker threads."""
    print("=" * 50)
    print("Example 3: Multiple Worker Threads")
    print("=" * 50)

    def worker(send_q, recv_q, worker_id):
        """Worker that processes messages with an ID."""
        channel = SyncThreadChannel(send_q, recv_q)
        print(f"[Worker {worker_id}] Started")

        try:
            while True:
                key, data = channel.recv()
                print(f"[Worker {worker_id}] Processing: {key}={data}")
                channel.send("result", f"Worker {worker_id} processed: {data}")
        except ChannelClosed:
            print(f"[Worker {worker_id}] Channel closed, exiting")

    # Start multiple workers
    workers = []
    for i in range(3):
        parent_channel, child_queues = create_thread_channel_pair()
        thread = threading.Thread(target=worker, args=(*child_queues, i))
        thread.start()
        workers.append((parent_channel, thread))
        print(f"[Main] Started worker {i}")

    # Send a message to each worker
    for i, (channel, _) in enumerate(workers):
        await channel.send("task", f"message {i}")

    # Receive responses
    for i, (channel, _) in enumerate(workers):
        _, data = await channel.recv(timeout=5.0)
        print(f"[Main] From worker {i}: {data}")

    # Clean up
    for channel, thread in workers:
        await channel.close()
        thread.join(timeout=2.0)

    print("Done!\n")

# %%
await example_multiple_workers()
