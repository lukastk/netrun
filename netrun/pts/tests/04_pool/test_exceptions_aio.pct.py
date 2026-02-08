# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Async Pool (SingleWorkerPool) Exception Tests
#
# This notebook provides comprehensive tests and examples for all exception types
# that can occur in the Async Pool layer. The SingleWorkerPool uses `asyncio`
# tasks within the same event loop, with `asyncio.Queue` for communication.
#
# ## Exception Types
#
# The SingleWorkerPool can raise the following exceptions:
#
# 1. **PoolNotStarted**: Trying to use the pool before calling `start()`
# 2. **PoolAlreadyStarted**: Calling `start()` on a running pool
# 3. **RecvTimeout**: A receive operation timed out waiting for a message
# 4. **WorkerException**: The worker function raised an exception
# 5. **WorkerCrashed**: The worker task ended unexpectedly
# 6. **ValueError**: Invalid worker_id (must be 0 for single worker)

# %%
#|default_exp pool.test_exceptions_aio

# %%
#|export
import pytest
import asyncio
import time
from netrun.rpc.base import ChannelClosed, RecvTimeout
from netrun.rpc.aio import AsyncChannel
from netrun.pool.base import (
    PoolError,
    PoolNotStarted,
    PoolAlreadyStarted,
    WorkerException,
    WorkerCrashed,
)
from netrun.pool.aio import SingleWorkerPool

# %% [markdown]
# ## Async Worker Functions for Testing

# %%
#|export
async def echo_worker(channel: AsyncChannel, worker_id: int):
    """Echo worker for testing."""
    try:
        while True:
            key, data = await channel.recv()
            await channel.send(f"echo:{key}", {"worker_id": worker_id, "data": data})
    except ChannelClosed:
        pass

# %%
#|export
async def raising_worker(channel: AsyncChannel, worker_id: int):
    """Worker that raises an exception on specific message."""
    try:
        while True:
            key, data = await channel.recv()
            if key == "raise":
                raise ValueError(f"Intentional error: {data}")
            await channel.send("ok", data)
    except ChannelClosed:
        pass

# %%
#|export
async def immediate_exit_worker(channel: AsyncChannel, worker_id: int):
    """Worker that exits immediately without processing."""
    return  # Exit immediately

# %%
#|export
async def slow_worker(channel: AsyncChannel, worker_id: int):
    """Worker that takes time to respond."""
    try:
        while True:
            key, data = await channel.recv()
            await asyncio.sleep(0.1)
            await channel.send("done", data)
    except ChannelClosed:
        pass

# %% [markdown]
# ---
# # PoolNotStarted Exception
#
# `PoolNotStarted` is raised when trying to use the pool before calling `start()`.

# %% [markdown]
# ## 1.1 PoolNotStarted on send()

# %%
#|export
@pytest.mark.asyncio
async def test_send_before_start():
    """SingleWorkerPool.send() raises PoolNotStarted before start()."""
    pool = SingleWorkerPool(echo_worker)

    with pytest.raises(PoolNotStarted) as exc_info:
        await pool.send(0, "hello", "world")

    assert "not been started" in str(exc_info.value).lower()

# %%
await test_send_before_start()
print("Send before start: raises PoolNotStarted as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_recv_before_start():
    """SingleWorkerPool.recv() raises PoolNotStarted before start()."""
    pool = SingleWorkerPool(echo_worker)

    with pytest.raises(PoolNotStarted):
        await pool.recv(timeout=0.1)

# %%
await test_recv_before_start()
print("Recv before start: raises PoolNotStarted as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_before_start():
    """SingleWorkerPool.try_recv() raises PoolNotStarted before start()."""
    pool = SingleWorkerPool(echo_worker)

    with pytest.raises(PoolNotStarted):
        await pool.try_recv()

# %%
await test_try_recv_before_start()
print("Try_recv before start: raises PoolNotStarted as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_broadcast_before_start():
    """SingleWorkerPool.broadcast() raises PoolNotStarted before start()."""
    pool = SingleWorkerPool(echo_worker)

    with pytest.raises(PoolNotStarted):
        await pool.broadcast("hello", "world")

# %%
await test_broadcast_before_start()
print("Broadcast before start: raises PoolNotStarted as expected")

# %% [markdown]
# ---
# # PoolAlreadyStarted Exception
#
# `PoolAlreadyStarted` is raised when calling `start()` on a pool that's already running.

# %%
#|export
@pytest.mark.asyncio
async def test_start_twice():
    """SingleWorkerPool.start() raises PoolAlreadyStarted if already running."""
    pool = SingleWorkerPool(echo_worker)

    await pool.start()
    try:
        assert pool.is_running

        with pytest.raises(PoolAlreadyStarted) as exc_info:
            await pool.start()

        assert "already running" in str(exc_info.value).lower()
    finally:
        await pool.close()

# %%
await test_start_twice()
print("Start twice: raises PoolAlreadyStarted as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_close_allows_restart():
    """After close(), the pool can be started again."""
    pool = SingleWorkerPool(echo_worker)

    # First start
    await pool.start()
    await pool.close()
    assert not pool.is_running

    # Second start should work
    await pool.start()
    assert pool.is_running
    await pool.close()

# %%
await test_close_allows_restart()
print("Close allows restart: pool can be restarted after close")

# %% [markdown]
# ---
# # RecvTimeout Exception
#
# `RecvTimeout` is raised when `recv()` times out waiting for a message.

# %% [markdown]
# ## 3.1 RecvTimeout Basics

# %%
#|export
@pytest.mark.asyncio
async def test_recv_timeout():
    """SingleWorkerPool.recv() raises RecvTimeout when timeout expires."""
    pool = SingleWorkerPool(echo_worker)
    await pool.start()

    try:
        start = time.time()
        with pytest.raises(RecvTimeout) as exc_info:
            await pool.recv(timeout=0.1)
        elapsed = time.time() - start

        assert elapsed >= 0.1
        assert elapsed < 0.5
        assert "timed out" in str(exc_info.value).lower()
    finally:
        await pool.close()

# %%
await test_recv_timeout()
print("Recv timeout: raises RecvTimeout after specified duration")

# %%
#|export
@pytest.mark.asyncio
async def test_recv_timeout_preserves_pool():
    """After RecvTimeout, the pool is still usable."""
    pool = SingleWorkerPool(echo_worker)
    await pool.start()

    try:
        # First recv times out
        with pytest.raises(RecvTimeout):
            await pool.recv(timeout=0.05)

        # Pool should still be running
        assert pool.is_running

        # Can still send and receive
        await pool.send(0, "hello", "world")
        msg = await pool.recv(timeout=1.0)
        assert msg.key == "echo:hello"
        assert msg.data["data"] == "world"
    finally:
        await pool.close()

# %%
await test_recv_timeout_preserves_pool()
print("Recv timeout: pool remains usable after timeout")

# %% [markdown]
# ## 3.2 try_recv Does NOT Raise RecvTimeout

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_returns_none():
    """SingleWorkerPool.try_recv() returns None, never raises RecvTimeout."""
    pool = SingleWorkerPool(echo_worker)
    await pool.start()

    try:
        result = await pool.try_recv()
        assert result is None
    finally:
        await pool.close()

# %%
await test_try_recv_returns_none()
print("Try_recv: returns None (no RecvTimeout)")

# %% [markdown]
# ---
# # WorkerException
#
# `WorkerException` is raised when a worker's code raises an exception.
# Since this is in the same process, the actual exception object is captured.

# %%
#|export
@pytest.mark.asyncio
async def test_worker_exception():
    """WorkerException is raised when worker raises an exception."""
    pool = SingleWorkerPool(raising_worker)
    await pool.start()

    try:
        await pool.send(0, "raise", "test error")

        # Worker exception and crash detection can race - accept either
        with pytest.raises((WorkerException, WorkerCrashed)) as exc_info:
            await pool.recv(timeout=2.0)

        exc = exc_info.value
        assert exc.worker_id == 0

        if isinstance(exc, WorkerException):
            assert isinstance(exc.original_exception, ValueError)
            assert "Intentional error" in str(exc.original_exception)
    finally:
        await pool.close()

# %%
await test_worker_exception()
print("Worker exception: raises WorkerException with original exception")

# %%
#|export
def test_worker_exception_structure():
    """WorkerException has the expected structure."""
    exc = WorkerException(0, ValueError("test"))

    assert exc.worker_id == 0
    assert isinstance(exc.original_exception, ValueError)
    assert "Worker 0" in str(exc)
    assert "ValueError" in str(exc)

# %%
test_worker_exception_structure()
print("WorkerException: has expected structure")

# %% [markdown]
# ---
# # WorkerCrashed Exception
#
# `WorkerCrashed` is raised when a worker task ends unexpectedly.

# %%
#|export
def test_worker_crashed_structure():
    """WorkerCrashed has the expected structure."""
    details = {"reason": "Worker task ended unexpectedly"}
    exc = WorkerCrashed(0, details)

    assert exc.worker_id == 0
    assert exc.details == details
    assert "Worker 0" in str(exc)
    assert "crashed" in str(exc).lower()

# %%
test_worker_crashed_structure()
print("WorkerCrashed: has expected structure")

# %%
#|export
@pytest.mark.asyncio
async def test_worker_crash_detected():
    """When a worker task exits unexpectedly, WorkerCrashed is raised."""
    pool = SingleWorkerPool(immediate_exit_worker)
    await pool.start()

    try:
        # Worker exits immediately
        # The monitor task should detect it
        await asyncio.sleep(0.1)  # Give monitor time to detect

        with pytest.raises(WorkerCrashed) as exc_info:
            await pool.recv(timeout=1.0)

        assert exc_info.value.worker_id == 0
        assert "unexpectedly" in str(exc_info.value).lower()
    finally:
        await pool.close()

# %%
await test_worker_crash_detected()
print("Worker crash: detected and raises WorkerCrashed")

# %% [markdown]
# ---
# # ValueError (Invalid worker_id)
#
# `ValueError` is raised when passing a worker_id other than 0 to `send()`.
# SingleWorkerPool always has exactly one worker with ID 0.

# %%
#|export
@pytest.mark.asyncio
async def test_send_invalid_worker_id():
    """send() raises ValueError for worker_id != 0."""
    pool = SingleWorkerPool(echo_worker)
    await pool.start()

    try:
        with pytest.raises(ValueError) as exc_info:
            await pool.send(1, "hello", "world")

        assert "must be 0" in str(exc_info.value)

        with pytest.raises(ValueError):
            await pool.send(-1, "hello", "world")

        with pytest.raises(ValueError):
            await pool.send(100, "hello", "world")
    finally:
        await pool.close()

# %%
await test_send_invalid_worker_id()
print("Invalid worker_id: raises ValueError")

# %%
#|export
@pytest.mark.asyncio
async def test_num_workers_always_one():
    """SingleWorkerPool always has exactly 1 worker."""
    pool = SingleWorkerPool(echo_worker)
    assert pool.num_workers == 1

    await pool.start()
    try:
        assert pool.num_workers == 1
    finally:
        await pool.close()

# %%
await test_num_workers_always_one()
print("Num workers: always 1")

# %% [markdown]
# ---
# # Exception Hierarchy
#
# All pool-specific exceptions inherit from `PoolError`:

# %%
#|export
def test_exception_hierarchy():
    """Verify exception hierarchy is correct."""
    assert issubclass(PoolNotStarted, PoolError)
    assert issubclass(PoolAlreadyStarted, PoolError)
    assert issubclass(WorkerException, PoolError)
    assert issubclass(WorkerCrashed, PoolError)
    assert issubclass(PoolError, Exception)

    # RecvTimeout is from RPC layer, not PoolError
    assert not issubclass(RecvTimeout, PoolError)

# %%
test_exception_hierarchy()
print("Exception hierarchy: verified")

# %% [markdown]
# ---
# # Practical Examples

# %% [markdown]
# ## Example: Graceful Error Handling

# %%
@pytest.mark.asyncio
async def example_graceful_error_handling():
    """Example: Handling all exception types gracefully."""
    print("=" * 50)
    print("Example: Graceful Error Handling")
    print("=" * 50)

    async def mixed_worker(channel: AsyncChannel, worker_id: int):
        try:
            while True:
                key, data = await channel.recv()
                if key == "fail":
                    raise ValueError("Worker failed!")
                await channel.send("ok", f"processed {data}")
        except ChannelClosed:
            pass

    async with SingleWorkerPool(mixed_worker) as pool:
        # Normal operation
        await pool.send(0, "task", "data1")
        try:
            msg = await pool.recv(timeout=1.0)
            print(f"  Got: {msg.data}")
        except RecvTimeout:
            print("  Timed out waiting for response")

        # Trigger worker exception
        await pool.send(0, "fail", "data2")
        try:
            msg = await pool.recv(timeout=1.0)
            print(f"  Got: {msg.data}")
        except WorkerException as e:
            print(f"  Worker {e.worker_id} failed: {e.original_exception}")
        except RecvTimeout:
            print("  Timed out (exception may not have been sent)")

    print("Done!")

# %%
await example_graceful_error_handling()

# %% [markdown]
# ## Example: Async Compute Pattern

# %%
@pytest.mark.asyncio
async def example_async_compute():
    """Example: Using SingleWorkerPool for async computation."""
    print("=" * 50)
    print("Example: Async Compute")
    print("=" * 50)

    async def compute_worker(channel: AsyncChannel, worker_id: int):
        try:
            while True:
                key, data = await channel.recv()
                if key == "factorial":
                    result = 1
                    for i in range(1, data + 1):
                        result *= i
                    await channel.send("result", result)
                elif key == "fib":
                    # Async-friendly fibonacci
                    if data <= 1:
                        await channel.send("result", data)
                    else:
                        a, b = 0, 1
                        for _ in range(data - 1):
                            a, b = b, a + b
                            await asyncio.sleep(0)  # Yield
                        await channel.send("result", b)
        except ChannelClosed:
            pass

    async with SingleWorkerPool(compute_worker) as pool:
        await pool.send(0, "factorial", 5)
        msg = await pool.recv(timeout=1.0)
        print(f"  factorial(5) = {msg.data}")

        await pool.send(0, "fib", 10)
        msg = await pool.recv(timeout=1.0)
        print(f"  fib(10) = {msg.data}")

    print("Done!")

# %%
await example_async_compute()

# %% [markdown]
# ## Example: Broadcast (Same as Send)

# %%
@pytest.mark.asyncio
async def example_broadcast():
    """Example: Broadcast is same as send for single worker."""
    print("=" * 50)
    print("Example: Broadcast")
    print("=" * 50)

    async with SingleWorkerPool(echo_worker) as pool:
        # broadcast() is equivalent to send() for single worker
        await pool.broadcast("config", {"setting": "value"})
        msg = await pool.recv(timeout=1.0)
        print(f"  Broadcast result: {msg.data}")

    print("Done!")

# %%
await example_broadcast()
