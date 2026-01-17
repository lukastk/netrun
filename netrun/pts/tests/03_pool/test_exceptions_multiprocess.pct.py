# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Multiprocess Pool Exception Tests
#
# This notebook provides comprehensive tests and examples for all exception types
# that can occur in the Multiprocess Pool layer. The MultiprocessPool uses
# subprocesses with threads, and `multiprocessing.Queue` for communication.
#
# ## Exception Types
#
# The MultiprocessPool can raise the following exceptions:
#
# 1. **PoolNotStarted**: Trying to use the pool before calling `start()`
# 2. **PoolAlreadyStarted**: Calling `start()` on a running pool
# 3. **RecvTimeout**: A receive operation timed out waiting for a message
# 4. **WorkerException**: The worker function raised an exception
# 5. **WorkerCrashed**: The worker process died unexpectedly
# 6. **ValueError**: Invalid worker_id or configuration

# %%
#|default_exp pool.test_exceptions_multiprocess

# %%
#|export
import pytest
import time
from netrun.rpc.base import RecvTimeout
from netrun.pool.base import (
    PoolError,
    PoolNotStarted,
    PoolAlreadyStarted,
    WorkerException,
    WorkerCrashed,
)
from netrun.pool.multiprocess import MultiprocessPool

# %% [markdown]
# ## Worker Functions
#
# For multiprocessing with spawn context, workers must be importable
# (defined at module level in an importable module).
# We use the workers from tests.pool.workers.

# %%
#|export
from tests.pool.workers import echo_worker, compute_worker

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
    """MultiprocessPool.send() raises PoolNotStarted before start()."""
    pool = MultiprocessPool(echo_worker, num_processes=1, threads_per_process=1)

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
    """MultiprocessPool.recv() raises PoolNotStarted before start()."""
    pool = MultiprocessPool(echo_worker, num_processes=1, threads_per_process=1)

    with pytest.raises(PoolNotStarted):
        await pool.recv(timeout=0.1)

# %%
await test_recv_before_start()
print("Recv before start: raises PoolNotStarted as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_before_start():
    """MultiprocessPool.try_recv() raises PoolNotStarted before start()."""
    pool = MultiprocessPool(echo_worker, num_processes=1, threads_per_process=1)

    with pytest.raises(PoolNotStarted):
        await pool.try_recv()

# %%
await test_try_recv_before_start()
print("Try_recv before start: raises PoolNotStarted as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_broadcast_before_start():
    """MultiprocessPool.broadcast() raises PoolNotStarted before start()."""
    pool = MultiprocessPool(echo_worker, num_processes=1, threads_per_process=1)

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
    """MultiprocessPool.start() raises PoolAlreadyStarted if already running."""
    pool = MultiprocessPool(echo_worker, num_processes=1, threads_per_process=1)

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
    pool = MultiprocessPool(echo_worker, num_processes=1, threads_per_process=1)

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
    """MultiprocessPool.recv() raises RecvTimeout when timeout expires."""
    pool = MultiprocessPool(echo_worker, num_processes=1, threads_per_process=1)
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
    pool = MultiprocessPool(echo_worker, num_processes=1, threads_per_process=1)
    await pool.start()

    try:
        # First recv times out
        with pytest.raises(RecvTimeout):
            await pool.recv(timeout=0.05)

        # Pool should still be running
        assert pool.is_running

        # Can still send and receive
        await pool.send(0, "hello", "world")
        msg = await pool.recv(timeout=5.0)
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
    """MultiprocessPool.try_recv() returns None, never raises RecvTimeout."""
    pool = MultiprocessPool(echo_worker, num_processes=1, threads_per_process=1)
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
# The exception info is serialized and sent back to the parent.

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

# %%
test_worker_exception_structure()
print("WorkerException: has expected structure")

# %%
#|export
@pytest.mark.asyncio
async def test_worker_exception_dict_form():
    """WorkerException can hold error dict (for unpickleable exceptions)."""
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
# `WorkerCrashed` is raised when a worker process dies unexpectedly.

# %%
#|export
@pytest.mark.asyncio
async def test_worker_crashed_structure():
    """WorkerCrashed has the expected structure."""
    details = {"exit_code": -9, "reason": "Process killed"}
    exc = WorkerCrashed(3, details)

    assert exc.worker_id == 3
    assert exc.details == details
    assert "Worker 3" in str(exc)
    assert "crashed" in str(exc).lower()

# %%
test_worker_crashed_structure()
print("WorkerCrashed: has expected structure")

# %% [markdown]
# ---
# # ValueError (Invalid Configuration)
#
# `ValueError` is raised for invalid configuration or worker_id.

# %%
#|export
def test_invalid_num_processes():
    """MultiprocessPool raises ValueError for invalid num_processes."""
    with pytest.raises(ValueError) as exc_info:
        MultiprocessPool(echo_worker, num_processes=0)

    assert "num_processes" in str(exc_info.value).lower()

    with pytest.raises(ValueError):
        MultiprocessPool(echo_worker, num_processes=-1)

# %%
test_invalid_num_processes()
print("Invalid num_processes: raises ValueError")

# %%
#|export
def test_invalid_threads_per_process():
    """MultiprocessPool raises ValueError for invalid threads_per_process."""
    with pytest.raises(ValueError) as exc_info:
        MultiprocessPool(echo_worker, num_processes=1, threads_per_process=0)

    assert "threads_per_process" in str(exc_info.value).lower()

    with pytest.raises(ValueError):
        MultiprocessPool(echo_worker, num_processes=1, threads_per_process=-1)

# %%
test_invalid_threads_per_process()
print("Invalid threads_per_process: raises ValueError")

# %%
#|export
@pytest.mark.asyncio
async def test_send_invalid_worker_id_negative():
    """send() raises ValueError for negative worker_id."""
    pool = MultiprocessPool(echo_worker, num_processes=2, threads_per_process=2)
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
    pool = MultiprocessPool(echo_worker, num_processes=2, threads_per_process=2)
    # Total workers = 2 * 2 = 4, valid IDs are 0, 1, 2, 3
    await pool.start()

    try:
        with pytest.raises(ValueError) as exc_info:
            await pool.send(4, "hello", "world")

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
# # Worker ID Mapping
#
# MultiprocessPool uses flat worker IDs across processes and threads.
# worker_id = process_idx * threads_per_process + thread_idx

# %%
#|export
@pytest.mark.asyncio
async def test_worker_id_mapping():
    """Worker IDs are correctly mapped across processes."""
    pool = MultiprocessPool(echo_worker, num_processes=2, threads_per_process=2)
    await pool.start()

    try:
        assert pool.num_workers == 4
        assert pool.num_processes == 2
        assert pool.threads_per_process == 2

        # Send to each worker and verify responses
        for worker_id in range(4):
            await pool.send(worker_id, "test", worker_id)

        responses = []
        for _ in range(4):
            msg = await pool.recv(timeout=5.0)
            responses.append((msg.worker_id, msg.data["worker_id"]))

        # Each worker should report their correct ID
        for worker_id, reported_id in responses:
            assert worker_id == reported_id
    finally:
        await pool.close()

# %%
await test_worker_id_mapping()
print("Worker ID mapping: correct across processes and threads")

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
# ## Example: Error Handling with Multiple Workers

# %%
@pytest.mark.asyncio
async def example_error_handling():
    """Example: Error handling with multiprocess pool."""
    print("=" * 50)
    print("Example: Error Handling")
    print("=" * 50)

    async with MultiprocessPool(compute_worker, num_processes=2, threads_per_process=1) as pool:
        print(f"  Pool has {pool.num_workers} workers")

        # Send tasks to workers
        await pool.send(0, "square", 5)
        await pool.send(1, "double", 10)

        # Receive results with error handling
        for _ in range(2):
            try:
                msg = await pool.recv(timeout=5.0)
                print(f"  Worker {msg.worker_id}: {msg.key} = {msg.data}")
            except RecvTimeout:
                print("  Timeout waiting for response")
            except WorkerException as e:
                print(f"  Worker {e.worker_id} failed: {e}")
            except WorkerCrashed as e:
                print(f"  Worker {e.worker_id} crashed: {e.details}")

    print("Done!")

# %%
await example_error_handling()
