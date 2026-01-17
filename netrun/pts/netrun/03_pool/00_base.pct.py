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
