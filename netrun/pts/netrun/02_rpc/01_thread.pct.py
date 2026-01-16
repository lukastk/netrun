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
# Channel for communication between async tasks within the same event loop.
# Uses `asyncio.Queue` for message passing.

# %%
#|export
import asyncio
from typing import Any

from netrun.rpc.base import (
    ChannelClosed,
    RecvTimeout,
    SHUTDOWN_KEY,
)

# %% [markdown]
# ## ThreadChannel

# %%
#|export
class ThreadChannel:
    """Async RPC channel using asyncio queues.

    For communication between async tasks in the same event loop.
    Thread-safe for use from multiple coroutines.
    """

    def __init__(
        self,
        send_queue: asyncio.Queue,
        recv_queue: asyncio.Queue,
    ):
        """Create a channel from two queues.

        Args:
            send_queue: Queue for outgoing messages
            recv_queue: Queue for incoming messages
        """
        self._send_queue = send_queue
        self._recv_queue = recv_queue
        self._closed = False

    async def send(self, key: str, data: Any) -> None:
        """Send a message."""
        if self._closed:
            raise ChannelClosed("Channel is closed")
        await self._send_queue.put((key, data))

    async def recv(self, timeout: float | None = None) -> tuple[str, Any]:
        """Receive a message with optional timeout."""
        if self._closed:
            raise ChannelClosed("Channel is closed")

        try:
            if timeout is None:
                result = await self._recv_queue.get()
            else:
                result = await asyncio.wait_for(
                    self._recv_queue.get(),
                    timeout=timeout,
                )
        except TimeoutError:
            raise RecvTimeout(f"Receive timed out after {timeout}s")

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
        except asyncio.QueueEmpty:
            return None

        if result[0] == SHUTDOWN_KEY:
            self._closed = True
            raise ChannelClosed("Channel was shut down")

        return result

    async def close(self) -> None:
        """Close the channel."""
        if not self._closed:
            self._closed = True
            try:
                await self._send_queue.put((SHUTDOWN_KEY, None))
            except Exception:
                pass

    @property
    def is_closed(self) -> bool:
        """Whether the channel is closed."""
        return self._closed

# %% [markdown]
# ## create_channel_pair

# %%
#|export
def create_channel_pair() -> tuple[ThreadChannel, ThreadChannel]:
    """Create a pair of connected ThreadChannels.

    Returns:
        (channel_a, channel_b) where messages sent on one are received on the other.

    Example:
        ```python
        ch_a, ch_b = create_channel_pair()

        await ch_a.send("hello", "world")
        key, data = await ch_b.recv()  # ("hello", "world")

        await ch_b.send("reply", 42)
        key, data = await ch_a.recv()  # ("reply", 42)
        ```
    """
    queue_a_to_b: asyncio.Queue = asyncio.Queue()
    queue_b_to_a: asyncio.Queue = asyncio.Queue()

    channel_a = ThreadChannel(send_queue=queue_a_to_b, recv_queue=queue_b_to_a)
    channel_b = ThreadChannel(send_queue=queue_b_to_a, recv_queue=queue_a_to_b)

    return channel_a, channel_b

# %% [markdown]
# ## Example: Two Async Tasks Communicating
#
# This example demonstrates two async tasks communicating via a channel pair.

# %%
async def example_basic_communication():
    """Basic example: two tasks exchanging messages."""
    print("=" * 50)
    print("Example 1: Basic Communication")
    print("=" * 50)

    # Create a pair of connected channels
    channel_a, channel_b = create_channel_pair()

    # Send from A, receive on B
    await channel_a.send("greeting", "Hello from A!")
    key, data = await channel_b.recv()
    print(f"B received: {key} = {data}")

    # Send from B, receive on A
    await channel_b.send("response", "Hello back from B!")
    key, data = await channel_a.recv()
    print(f"A received: {key} = {data}")

    # Clean up
    await channel_a.close()
    await channel_b.close()

    print("Done!\n")

# %%
await example_basic_communication()

# %%
async def example_worker_task():
    """Example: worker task pattern with async tasks."""
    print("=" * 50)
    print("Example 2: Worker Task Pattern")
    print("=" * 50)

    channel_a, channel_b = create_channel_pair()

    async def worker(channel: ThreadChannel):
        """Worker that processes requests and sends responses."""
        print("[Worker] Started")
        try:
            while True:
                key, data = await channel.recv()
                print(f"[Worker] Processing: {key} = {data}")

                # Simulate some computation
                if key == "square":
                    result = data * data
                elif key == "double":
                    result = data * 2
                else:
                    result = f"unknown command: {key}"

                await channel.send(f"result:{key}", result)
        except ChannelClosed:
            print("[Worker] Channel closed, stopping")

    # Start worker as a background task
    worker_task = asyncio.create_task(worker(channel_b))

    # Send some requests
    await channel_a.send("square", 5)
    key, data = await channel_a.recv()
    print(f"[Main] Got: {key} = {data}")

    await channel_a.send("double", 7)
    key, data = await channel_a.recv()
    print(f"[Main] Got: {key} = {data}")

    await channel_a.send("square", 10)
    key, data = await channel_a.recv()
    print(f"[Main] Got: {key} = {data}")

    # Close the channel to stop the worker
    await channel_a.close()

    # Wait for worker to finish
    await worker_task

    print("Done!\n")

# %%
await example_worker_task()

# %%
async def example_bidirectional_workers():
    """Example: two workers communicating bidirectionally."""
    print("=" * 50)
    print("Example 3: Bidirectional Workers")
    print("=" * 50)

    channel_a, channel_b = create_channel_pair()
    results = []

    async def ping_worker(channel: ThreadChannel, name: str, count: int):
        """Worker that sends pings and waits for pongs."""
        print(f"[{name}] Started")
        try:
            for i in range(count):
                await channel.send("ping", f"{name}-{i}")
                key, data = await channel.recv(timeout=1.0)
                results.append(f"{name} got {key}={data}")
                print(f"[{name}] Received: {key} = {data}")
        except ChannelClosed:
            print(f"[{name}] Channel closed")
        print(f"[{name}] Done")

    async def pong_worker(channel: ThreadChannel, name: str, count: int):
        """Worker that receives pings and sends pongs."""
        print(f"[{name}] Started")
        try:
            for _ in range(count):
                key, data = await channel.recv(timeout=1.0)
                print(f"[{name}] Received: {key} = {data}")
                await channel.send("pong", f"reply-to-{data}")
        except ChannelClosed:
            print(f"[{name}] Channel closed")
        print(f"[{name}] Done")

    # Run both workers concurrently
    await asyncio.gather(
        ping_worker(channel_a, "Ping", 3),
        pong_worker(channel_b, "Pong", 3),
    )

    await channel_a.close()
    await channel_b.close()

    print(f"Results: {results}")
    print("Done!\n")

# %%
await example_bidirectional_workers()
