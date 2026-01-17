# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Thread Pool Exception Tests
#
# This notebook provides comprehensive tests and examples for all exception types
# that can occur in the Thread Pool layer. The ThreadPool uses threads within the
# same process with `queue.Queue` for communication.
#
# ## Exception Types
#
# The ThreadPool can raise the following exceptions:
#
# 1. **PoolNotStarted**: Trying to use the pool before calling `start()`
# 2. **PoolAlreadyStarted**: Calling `start()` on a running pool
# 3. **RecvTimeout**: A receive operation timed out waiting for a message
# 4. **WorkerException**: The worker function raised an exception
# 5. **WorkerCrashed**: The worker thread died unexpectedly
# 6. **ValueError**: Invalid worker_id passed to `send()`

# %%
#|default_exp pool.test_exceptions_thread

# %%
#|export
import pytest
import asyncio
import time
from netrun.rpc.base import ChannelClosed, RecvTimeout
from netrun.pool.base import (
    PoolError,
    PoolNotStarted,
    PoolAlreadyStarted,
    WorkerException,
    WorkerCrashed,
)
from netrun.pool.thread import ThreadPool

# %% [markdown]
# ## Worker Functions for Testing

# %%
#|export
def echo_worker(channel, worker_id):
    """Echo worker for testing."""
    try:
        while True:
            key, data = channel.recv()
            channel.send(f"echo:{key}", {"worker_id": worker_id, "data": data})
    except ChannelClosed:
        pass

# %%
#|export
def raising_worker(channel, worker_id):
    """Worker that raises an exception on specific message."""
    try:
        while True:
            key, data = channel.recv()
            if key == "raise":
                raise ValueError(f"Intentional error: {data}")
            channel.send("ok", data)
    except ChannelClosed:
        pass

# %%
#|export
def immediate_exit_worker(channel, worker_id):
    """Worker that exits immediately without processing."""
    return  # Exit immediately

# %%
#|export
def crash_after_one_worker(channel, worker_id):
    """Worker that processes one message then crashes."""
    try:
        key, data = channel.recv()
        channel.send("got", data)
        # Simulate crash by raising uncaught exception
        raise RuntimeError("Simulated crash")
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
    """ThreadPool.send() raises PoolNotStarted before start()."""
    pool = ThreadPool(echo_worker, num_workers=2)

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
    """ThreadPool.recv() raises PoolNotStarted before start()."""
    pool = ThreadPool(echo_worker, num_workers=2)

    with pytest.raises(PoolNotStarted):
        await pool.recv(timeout=0.1)

# %%
await test_recv_before_start()
print("Recv before start: raises PoolNotStarted as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_before_start():
    """ThreadPool.try_recv() raises PoolNotStarted before start()."""
    pool = ThreadPool(echo_worker, num_workers=2)

    with pytest.raises(PoolNotStarted):
        await pool.try_recv()

# %%
await test_try_recv_before_start()
print("Try_recv before start: raises PoolNotStarted as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_broadcast_before_start():
    """ThreadPool.broadcast() raises PoolNotStarted before start()."""
    pool = ThreadPool(echo_worker, num_workers=2)

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
    """ThreadPool.start() raises PoolAlreadyStarted if already running."""
    pool = ThreadPool(echo_worker, num_workers=2)

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
    pool = ThreadPool(echo_worker, num_workers=2)

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
    """ThreadPool.recv() raises RecvTimeout when timeout expires."""
    pool = ThreadPool(echo_worker, num_workers=2)
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
    pool = ThreadPool(echo_worker, num_workers=2)
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
    """ThreadPool.try_recv() returns None, never raises RecvTimeout."""
    pool = ThreadPool(echo_worker, num_workers=2)
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
# The original exception is captured and can be inspected.

# %%
#|export
@pytest.mark.asyncio
async def test_worker_exception():
    """WorkerException is raised when worker raises an exception."""
    pool = ThreadPool(raising_worker, num_workers=1)
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
@pytest.mark.asyncio
async def test_worker_exception_structure():
    """WorkerException has the expected structure."""
    exc = WorkerException(42, ValueError("test"))

    assert exc.worker_id == 42
    assert isinstance(exc.original_exception, ValueError)
    assert "Worker 42" in str(exc)
    assert "ValueError" in str(exc)
    assert "test" in str(exc)

# %%
test_worker_exception_structure()
print("WorkerException: has expected structure")

# %%
#|export
@pytest.mark.asyncio
async def test_worker_exception_dict_form():
    """WorkerException can also hold error dict (for unpickleable exceptions)."""
    error_dict = {
        "type": "CustomError",
        "message": "Something went wrong",
    }
    exc = WorkerException(0, error_dict)

    assert exc.worker_id == 0
    assert exc.original_exception == error_dict
    assert "CustomError" in str(exc)
    assert "Something went wrong" in str(exc)

# %%
test_worker_exception_dict_form()
print("WorkerException: handles error dict form")

# %% [markdown]
# ---
# # WorkerCrashed Exception
#
# `WorkerCrashed` is raised when a worker thread dies unexpectedly.

# %%
#|export
@pytest.mark.asyncio
async def test_worker_crashed_exception_structure():
    """WorkerCrashed has the expected structure."""
    details = {"reason": "Thread exited unexpectedly", "exit_code": 1}
    exc = WorkerCrashed(5, details)

    assert exc.worker_id == 5
    assert exc.details == details
    assert "Worker 5" in str(exc)
    assert "crashed" in str(exc).lower()

# %%
test_worker_crashed_exception_structure()
print("WorkerCrashed: has expected structure")

# %%
#|export
@pytest.mark.asyncio
async def test_worker_crash_detected():
    """When a worker thread exits unexpectedly, WorkerCrashed is raised."""
    pool = ThreadPool(immediate_exit_worker, num_workers=1)
    await pool.start()

    try:
        # Worker exits immediately
        # The monitor task should detect it and send crash notification
        await asyncio.sleep(1.0)  # Wait for monitor to detect

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
# `ValueError` is raised when passing an invalid `worker_id` to `send()`.

# %%
#|export
@pytest.mark.asyncio
async def test_send_invalid_worker_id_negative():
    """send() raises ValueError for negative worker_id."""
    pool = ThreadPool(echo_worker, num_workers=3)
    await pool.start()

    try:
        with pytest.raises(ValueError) as exc_info:
            await pool.send(-1, "hello", "world")

        assert "out of range" in str(exc_info.value)
    finally:
        await pool.close()

# %%
await test_send_invalid_worker_id_negative()
print("Invalid worker_id (negative): raises ValueError")

# %%
#|export
@pytest.mark.asyncio
async def test_send_invalid_worker_id_too_large():
    """send() raises ValueError for worker_id >= num_workers."""
    pool = ThreadPool(echo_worker, num_workers=3)
    await pool.start()

    try:
        with pytest.raises(ValueError) as exc_info:
            await pool.send(3, "hello", "world")  # Valid are 0, 1, 2

        assert "out of range" in str(exc_info.value)

        with pytest.raises(ValueError):
            await pool.send(100, "hello", "world")
    finally:
        await pool.close()

# %%
await test_send_invalid_worker_id_too_large()
print("Invalid worker_id (too large): raises ValueError")

# %% [markdown]
# ---
# # Exception Hierarchy
#
# All pool-specific exceptions inherit from `PoolError`:
#
# ```
# Exception
# └── PoolError
#     ├── PoolNotStarted
#     ├── PoolAlreadyStarted
#     ├── WorkerError
#     ├── WorkerException
#     ├── WorkerCrashed
#     └── WorkerTimeout
# ```

# %%
#|export
def test_exception_hierarchy():
    """Verify exception hierarchy is correct."""
    assert issubclass(PoolNotStarted, PoolError)
    assert issubclass(PoolAlreadyStarted, PoolError)
    assert issubclass(WorkerException, PoolError)
    assert issubclass(WorkerCrashed, PoolError)
    assert issubclass(PoolError, Exception)

    # Can catch all pool errors with PoolError
    for exc_class in [PoolNotStarted, PoolAlreadyStarted]:
        try:
            raise exc_class("test")
        except PoolError:
            pass  # All should be caught

# %%
test_exception_hierarchy()
print("Exception hierarchy: all pool exceptions inherit from PoolError")

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

    def mixed_worker(channel, worker_id):
        try:
            while True:
                key, data = channel.recv()
                if key == "fail":
                    raise ValueError("Worker failed!")
                channel.send("ok", f"processed {data}")
        except ChannelClosed:
            pass

    async with ThreadPool(mixed_worker, num_workers=2) as pool:
        # Normal operation
        await pool.send(0, "task", "data1")
        try:
            msg = await pool.recv(timeout=1.0)
            print(f"  Got: {msg.data}")
        except RecvTimeout:
            print("  Timed out waiting for response")

        # Trigger worker exception
        await pool.send(1, "fail", "data2")
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
# ## Example: Retry Pattern

# %%
@pytest.mark.asyncio
async def example_retry_pattern():
    """Example: Retrying on timeout."""
    print("=" * 50)
    print("Example: Retry Pattern")
    print("=" * 50)

    def slow_worker(channel, worker_id):
        import time
        try:
            while True:
                key, data = channel.recv()
                time.sleep(0.15)  # Slow processing
                channel.send("done", data)
        except ChannelClosed:
            pass

    async with ThreadPool(slow_worker, num_workers=1) as pool:
        await pool.send(0, "task", "important data")

        for attempt in range(3):
            try:
                msg = await pool.recv(timeout=0.1 * (attempt + 1))
                print(f"  Attempt {attempt + 1}: Got {msg.data}")
                break
            except RecvTimeout:
                print(f"  Attempt {attempt + 1}: Timed out, retrying...")

    print("Done!")

# %%
await example_retry_pattern()
