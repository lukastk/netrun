# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp pool.base

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %% [markdown]
# # Pool Base
#
# Base protocol and types for worker pools. A pool manages multiple workers
# (threads) that can receive and send messages via RPC channels.
#
# ## Concepts
#
# - **Worker**: A thread that processes messages. Each worker has a unique ID.
# - **Pool**: Manages a collection of workers, routing messages to/from them.
# - **WorkerFn**: A function that runs in each worker, receiving messages via a channel.

# %%
#|export
from typing import Any, Protocol, runtime_checkable
from collections.abc import Callable
from dataclasses import dataclass

from netrun.rpc.base import SyncRPCChannel

# %% [markdown]
# ## Types

# %%
#|export
WorkerId = int
"""Unique identifier for a worker within a pool."""

# %%
#|export
WorkerFn = Callable[[SyncRPCChannel, WorkerId], None]
"""Worker function signature.

Args:
    channel: Sync channel for sending/receiving messages
    worker_id: This worker's unique ID

The function should loop receiving messages until the channel is closed.

Example:
    ```python
    def my_worker(channel: SyncRPCChannel, worker_id: WorkerId):
        while True:
            try:
                key, data = channel.recv()
                result = process(data)
                channel.send("result", result)
            except ChannelClosed:
                break
    ```
"""

# %%
#|export
@dataclass
class WorkerMessage:
    """A message from a worker."""
    worker_id: WorkerId
    key: str
    data: Any

# %% [markdown]
# ## Pool Protocol

# %%
#|export
@runtime_checkable
class Pool(Protocol):
    """Protocol for worker pools.

    A pool manages multiple workers and provides a unified interface
    for sending messages to specific workers and receiving messages
    from any worker.
    """

    @property
    def num_workers(self) -> int:
        """Total number of workers in the pool."""
        ...

    @property
    def is_running(self) -> bool:
        """Whether the pool has been started."""
        ...

    async def start(self) -> None:
        """Start all workers in the pool."""
        ...

    async def close(self, timeout: float | None = None) -> None:
        """Shut down all workers and clean up resources.

        Args:
            timeout: Max seconds to wait for each worker to finish gracefully.
                     If None, wait indefinitely.
        """
        ...

    async def send(self, worker_id: WorkerId, key: str, data: Any) -> None:
        """Send a message to a specific worker.

        Args:
            worker_id: ID of the worker to send to (0 to num_workers-1)
            key: Message key
            data: Message data (must be pickleable for process/remote pools)

        Raises:
            ValueError: If worker_id is out of range
            ChannelClosed: If the worker's channel is closed
        """
        ...

    async def recv(self, timeout: float | None = None) -> WorkerMessage:
        """Receive a message from any worker.

        Args:
            timeout: Optional timeout in seconds

        Returns:
            WorkerMessage with worker_id, key, and data

        Raises:
            RecvTimeout: If timeout expires
            ChannelClosed: If all workers are closed
        """
        ...

    async def try_recv(self) -> WorkerMessage | None:
        """Non-blocking receive from any worker.

        Returns:
            WorkerMessage if available, None otherwise
        """
        ...

    async def broadcast(self, key: str, data: Any) -> None:
        """Send a message to all workers.

        Args:
            key: Message key
            data: Message data
        """
        ...

# %% [markdown]
# ## Exceptions

# %%
#|export
class PoolError(Exception):
    """Base exception for pool errors."""
    pass

# %%
#|export
class PoolNotStarted(PoolError):
    """Raised when trying to use a pool that hasn't been started."""
    pass

# %%
#|export
class PoolAlreadyStarted(PoolError):
    """Raised when trying to start a pool that's already running."""
    pass

# %%
#|export
class WorkerError(PoolError):
    """Raised when a worker encounters an error."""
    def __init__(self, worker_id: WorkerId, message: str):
        self.worker_id = worker_id
        super().__init__(f"Worker {worker_id}: {message}")

# %%
#|export
class WorkerException(PoolError):
    """Raised when a worker's code raised an exception."""
    def __init__(self, worker_id: WorkerId, exception: Exception | dict):
        self.worker_id = worker_id
        self.original_exception = exception
        if isinstance(exception, Exception):
            super().__init__(f"Worker {worker_id} raised {type(exception).__name__}: {exception}")
        else:
            exc_type = exception.get("type", "Exception")
            exc_msg = exception.get("message", "")
            super().__init__(f"Worker {worker_id} raised {exc_type}: {exc_msg}")

# %%
#|export
class WorkerCrashed(PoolError):
    """Raised when a worker process/thread died unexpectedly."""
    def __init__(self, worker_id: WorkerId, details: dict):
        self.worker_id = worker_id
        self.details = details
        reason = details.get("reason", "unknown")
        super().__init__(f"Worker {worker_id} crashed: {reason}")

# %%
#|export
class WorkerTimeout(PoolError):
    """Raised when a worker didn't respond within timeout."""
    def __init__(self, worker_id: WorkerId, timeout: float):
        self.worker_id = worker_id
        self.timeout = timeout
        super().__init__(f"Worker {worker_id} timed out after {timeout}s")

# %% [markdown]
# ## Pool Error Codes
#
# Standard keys used by pools for error messages from workers.
# Format: `"__pool-up:error-name"` (upstream: worker → parent)

# %%
#|export
POOL_UP_ERROR_EXCEPTION = "__pool-up:error-exception"
"""Worker code raised an exception. Data: exception object or error dict."""

# %%
#|export
POOL_UP_ERROR_CRASHED = "__pool-up:error-crashed"
"""Worker process/thread died unexpectedly. Data: {exit_code?, signal?, reason}"""

# %%
#|export
POOL_UP_ERROR_TIMEOUT = "__pool-up:error-timeout"
"""Worker didn't respond within timeout. Data: {timeout_seconds, operation}"""

# %%
#|export
POOL_UP_ERROR_KEYS = [POOL_UP_ERROR_EXCEPTION, POOL_UP_ERROR_CRASHED, POOL_UP_ERROR_TIMEOUT]
"""All pool error codes."""

# %%
#|export
def _check_error_and_raise(msg: WorkerMessage) -> None:
    """Check if message is an error code and raise appropriate exception.

    Call this in recv() before returning the message to auto-raise for errors.
    """
    if msg.key == POOL_UP_ERROR_EXCEPTION:
        raise WorkerException(msg.worker_id, msg.data)
    elif msg.key == POOL_UP_ERROR_CRASHED:
        raise WorkerCrashed(msg.worker_id, msg.data)
    elif msg.key == POOL_UP_ERROR_TIMEOUT:
        raise WorkerTimeout(msg.worker_id, msg.data["timeout_seconds"])

# %% [markdown]
# ## BasePool
#
# Abstract base class that provides the common lifecycle, recv/try_recv,
# monitor, and silence detection. Subclasses implement pool-specific
# worker creation, shutdown, message routing, and health checking.

# %%
#|export
import asyncio
import time
import logging
from abc import ABC, abstractmethod
from collections.abc import Coroutine

class BasePool(ABC):
    """Abstract base class for worker pools.

    Provides concrete implementations of recv(), try_recv(), start(), close(),
    and worker health monitoring with optional silence detection. Subclasses
    implement the pool-specific worker lifecycle and message routing.

    Subclass contract:
        _do_start()            — Create workers and channels
        _do_close(timeout)     — Shut down workers, clean up resources
        _create_recv_loops()   — Return coroutines that forward messages to _recv_queue
        _check_worker_health() — Detect dead workers, put errors on _recv_queue
        send(worker_id, ...)   — Send to a specific worker
        broadcast(key, data)   — Send to all workers
    """

    def __init__(self, num_workers: int, *, silence_timeout: float | None = None):
        """Initialize the base pool.

        Args:
            num_workers: Number of workers in this pool.
            silence_timeout: If set, log a warning when any worker has been
                silent for this many seconds. None disables silence detection.
        """
        self._num_workers = num_workers
        self._running = False
        self._recv_queue: asyncio.Queue[WorkerMessage] = asyncio.Queue()
        self._recv_tasks: list[asyncio.Task] = []
        self._monitor_task: asyncio.Task | None = None
        self._silence_timeout = silence_timeout
        self._last_message_time: dict[int, float] = {}

    @property
    def num_workers(self) -> int:
        return self._num_workers

    @property
    def is_running(self) -> bool:
        return self._running

    async def start(self) -> None:
        """Start the pool: create workers, begin health monitoring."""
        if self._running:
            raise PoolAlreadyStarted("Pool is already running")
        await self._do_start()
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())

    async def close(self, timeout: float | None = None) -> None:
        """Shut down the pool: stop workers, cancel tasks, clean up."""
        if not self._running:
            return
        # Ensure recv tasks are running before setting _running=False.
        # MultiprocessPool needs recv tasks to receive SHUTDOWN_COMPLETE.
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
        # Subclass shutdown (send shutdown signals, wait, close channels, join)
        await self._do_close(timeout)
        # Cancel recv tasks
        for task in self._recv_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._recv_tasks.clear()
        self._recv_queue = asyncio.Queue()

    def _start_recv_tasks(self) -> None:
        """Start background tasks that forward worker messages to _recv_queue.

        Idempotent — only creates tasks on first call.
        """
        if self._recv_tasks:
            return
        for coro in self._create_recv_loops():
            self._recv_tasks.append(asyncio.create_task(coro))

    async def recv(self, timeout: float | None = None) -> WorkerMessage:
        """Receive a message from any worker.

        Raises:
            PoolNotStarted: If pool hasn't been started
            RecvTimeout: If timeout expires
            WorkerException: If a worker raised an exception
            WorkerCrashed: If a worker died unexpectedly
            WorkerTimeout: If a worker timed out
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
        except (TimeoutError, asyncio.TimeoutError):
            from netrun.rpc.base import RecvTimeout
            raise RecvTimeout(f"Receive timed out after {timeout}s")
        self._last_message_time[msg.worker_id] = time.monotonic()
        _check_error_and_raise(msg)
        return msg

    async def recv_raw(self, timeout: float | None = None) -> WorkerMessage:
        """Like recv() but returns error messages instead of raising.

        Used by ExecutionManager to handle worker errors gracefully
        without killing the pool's central message receiver. Error messages
        (WorkerCrashed, WorkerException, etc.) are returned as normal
        WorkerMessage objects for the caller to route appropriately.
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
        except (TimeoutError, asyncio.TimeoutError):
            from netrun.rpc.base import RecvTimeout
            raise RecvTimeout(f"Receive timed out after {timeout}s")
        self._last_message_time[msg.worker_id] = time.monotonic()
        return msg

    async def try_recv(self) -> WorkerMessage | None:
        """Non-blocking receive from any worker.

        If recv tasks are running (started by a previous recv() call),
        checks the centralized queue. Otherwise, delegates to
        _try_recv_direct() which subclasses can override to read
        directly from channels.

        Raises:
            PoolNotStarted: If pool hasn't been started
            WorkerException: If a worker raised an exception
            WorkerCrashed: If a worker died unexpectedly
            WorkerTimeout: If a worker timed out
        """
        if not self._running:
            raise PoolNotStarted("Pool has not been started")
        if self._recv_tasks:
            try:
                msg = self._recv_queue.get_nowait()
            except asyncio.QueueEmpty:
                return None
        else:
            msg = await self._try_recv_direct()
            if msg is None:
                return None
        self._last_message_time[msg.worker_id] = time.monotonic()
        _check_error_and_raise(msg)
        return msg

    async def _monitor_loop(self) -> None:
        """Background task: check worker health and silence detection."""
        _logger = logging.getLogger("netrun.pool")
        while self._running:
            await self._check_worker_health()
            # Silence detection
            if self._silence_timeout is not None:
                now = time.monotonic()
                for worker_id in range(self._num_workers):
                    last = self._last_message_time.get(worker_id)
                    if last is not None and (now - last) > self._silence_timeout:
                        _logger.warning(
                            "Worker %d has been silent for %.0fs (threshold: %.0fs)",
                            worker_id, now - last, self._silence_timeout,
                        )
                        self._last_message_time[worker_id] = now  # Reset to avoid spam
            await asyncio.sleep(0.5)

    # --- Subclass contract ---

    @abstractmethod
    async def _do_start(self) -> None:
        """Create workers and channels. Called by start()."""
        ...

    @abstractmethod
    async def _do_close(self, timeout: float | None = None) -> None:
        """Shut down workers and clean up. Called by close() after monitor is cancelled."""
        ...

    @abstractmethod
    def _create_recv_loops(self) -> list[Coroutine]:
        """Return coroutines that read from workers and put WorkerMessages on _recv_queue.

        Each coroutine should loop reading from its worker channel and forwarding
        to self._recv_queue. It should catch ChannelClosed and CancelledError
        and exit gracefully.
        """
        ...

    @abstractmethod
    async def _check_worker_health(self) -> None:
        """Check if any workers have died. Put error WorkerMessages on _recv_queue for dead workers."""
        ...

    async def _try_recv_direct(self) -> WorkerMessage | None:
        """Direct non-blocking receive (when recv tasks aren't running).

        Default implementation returns None. Subclasses that support direct
        channel reads (ThreadPool, SingleWorkerPool) can override.
        """
        return None

    @abstractmethod
    async def send(self, worker_id: WorkerId, key: str, data: Any) -> None:
        """Send a message to a specific worker."""
        ...

    @abstractmethod
    async def broadcast(self, key: str, data: Any) -> None:
        """Send a message to all workers."""
        ...

    # --- Context manager ---

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.close()
