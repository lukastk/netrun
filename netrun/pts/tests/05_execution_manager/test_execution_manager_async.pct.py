# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for ExecutionManager with SingleWorkerPool (Async/Main Pool)
#
# SingleWorkerPool runs tasks in the main process using asyncio, which is useful for
# I/O-bound tasks and when you don't need process isolation.

# %%
#|default_exp execution_manager.test_execution_manager_async

# %%
#|export
import pytest
import asyncio

from netrun.pool.aio import SingleWorkerPool

from netrun.execution_manager import (
    ExecutionManager,
    RunAllocationMethod,
)

# Import worker functions from the workers module
from tests.execution_manager.workers import (
    add_numbers,
    multiply_numbers,
    slow_function,
    function_with_error,
    function_returns_non_serializable,
    function_with_kwargs,
    async_add,
)

# %% [markdown]
# ## Test Starting and Closing

# %%
#|export
@pytest.mark.asyncio
async def test_start_and_close():
    """Test starting and closing the manager with SingleWorkerPool."""
    manager = ExecutionManager({
        "pool": (SingleWorkerPool, {}),
    })

    await manager.start()
    assert manager._started is True
    assert "pool" in manager._pools
    await manager.close()

# %%
await test_start_and_close();

# %%
#|export
@pytest.mark.asyncio
async def test_context_manager():
    """Test using ExecutionManager as async context manager."""
    manager = ExecutionManager({
        "pool": (SingleWorkerPool, {}),
    })

    async with manager:
        assert manager._started is True

    # After exit, pools should be closed

# %%
await test_context_manager();

# %%
#|export
@pytest.mark.asyncio
async def test_immediate_close():
    """Test that immediate close after start doesn't raise errors."""
    # Run multiple times to catch race conditions
    for _ in range(10):
        manager = ExecutionManager({
            "pool": (SingleWorkerPool, {}),
        })
        async with manager:
            pass  # Immediately close without doing anything

# %%
await test_immediate_close();

# %%
#|export
@pytest.mark.asyncio
async def test_double_start_raises():
    """Test that starting twice raises an error."""
    manager = ExecutionManager({
        "pool": (SingleWorkerPool, {}),
    })

    await manager.start()
    try:
        with pytest.raises(RuntimeError, match="already started"):
            await manager.start()
    finally:
        await manager.close()

# %%
await test_double_start_raises();

# %% [markdown]
# ## Test pool_ids and get_num_workers

# %%
#|export
@pytest.mark.asyncio
async def test_pool_ids():
    """Test getting pool IDs."""
    manager = ExecutionManager({
        "pool_a": (SingleWorkerPool, {}),
        "pool_b": (SingleWorkerPool, {}),
    })

    async with manager:
        pool_ids = [pool_id for pool_id, _ in manager.pools]
        assert "pool_a" in pool_ids
        assert "pool_b" in pool_ids
        assert len(pool_ids) == 2

# %%
await test_pool_ids();

# %%
#|export
@pytest.mark.asyncio
async def test_get_num_workers():
    """Test getting number of workers in a pool."""
    manager = ExecutionManager({
        "pool": (SingleWorkerPool, {}),
    })

    async with manager:
        # SingleWorkerPool always has 1 worker
        assert manager.get_num_workers("pool") == 1

# %%
await test_get_num_workers();

# %% [markdown]
# ## Test send_function and run

# %%
#|export
@pytest.mark.asyncio
async def test_send_function_and_run():
    """Test sending a function and running it."""
    manager = ExecutionManager({
        "pool": (SingleWorkerPool, {}),
    })

    async with manager:
        # Send the function to the worker
        await manager.send_function("pool", 0, "add", add_numbers)

        # Run the function
        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="add",

            func_args=(3, 4),
            func_kwargs={},
        )

        assert result.result == 7
        assert result.pool_id == "pool"
        assert result.worker_id == 0
        assert result.converted_to_str is False

# %%
await test_send_function_and_run();

# %%
#|export
@pytest.mark.asyncio
async def test_send_function_to_pool():
    """Test sending a function to all workers in a pool."""
    manager = ExecutionManager({
        "pool": (SingleWorkerPool, {}),
    })

    async with manager:
        # Send the function to all workers (just 1 for SingleWorkerPool)
        await manager.send_function_to_pool("pool", "multiply", multiply_numbers)

        # Run on the single worker
        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="multiply",

            func_args=(5, 10),
            func_kwargs={},
        )

        assert result.result == 50

# %%
await test_send_function_to_pool();

# %% [markdown]
# ## Test JobResult

# %%
#|export
@pytest.mark.asyncio
async def test_job_result_timestamps():
    """Test that JobResult has correct timestamps."""
    manager = ExecutionManager({
        "pool": (SingleWorkerPool, {}),
    })

    async with manager:
        await manager.send_function("pool", 0, "slow", slow_function)

        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="slow",

            func_args=(0.1,),
            func_kwargs={},
        )

        # Check timestamps are in correct order
        assert result.timestamp_utc_submitted <= result.timestamp_utc_started
        assert result.timestamp_utc_started <= result.timestamp_utc_completed

        # Check result
        assert result.result == "done"

# %%
await test_job_result_timestamps();

# %%
#|export
@pytest.mark.asyncio
async def test_non_serializable_result():
    """Test that non-serializable results work in SingleWorkerPool (same process)."""
    manager = ExecutionManager({
        "pool": (SingleWorkerPool, {}),
    })

    async with manager:
        await manager.send_function("pool", 0, "nonserialized", function_returns_non_serializable)

        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="nonserialized",

            func_args=(),
            func_kwargs={},
        )

        # SingleWorkerPool runs in the same process, so non-serializable results work
        assert result.converted_to_str is False
        # The result should be a lambda function
        assert callable(result.result)
        assert result.result(5) == 5

# %%
await test_non_serializable_result();

# %% [markdown]
# ## Test Function with kwargs

# %%
#|export
@pytest.mark.asyncio
async def test_function_with_kwargs():
    """Test running a function with keyword arguments."""
    manager = ExecutionManager({
        "pool": (SingleWorkerPool, {}),
    })

    async with manager:
        await manager.send_function("pool", 0, "kwargs_fn", function_with_kwargs)

        # Test with only positional arg
        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="kwargs_fn",

            func_args=(1,),
            func_kwargs={},
        )
        assert result.result == 111  # 1 + 10 + 100

        # Test with kwargs
        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="kwargs_fn",

            func_args=(5,),
            func_kwargs={"b": 20, "c": 200},
        )
        assert result.result == 225  # 5 + 20 + 200

# %%
await test_function_with_kwargs();

# %% [markdown]
# ## Test Allocation Methods

# %%
#|export
@pytest.mark.asyncio
async def test_round_robin_allocation():
    """Test round-robin job allocation with single worker."""
    manager = ExecutionManager({
        "pool": (SingleWorkerPool, {}),
    })

    async with manager:
        await manager.send_function_to_pool("pool", "add", add_numbers)

        # Run 3 jobs sequentially with round-robin
        worker_ids = []
        for i in range(3):
            result = await manager.run_allocate(
                pool_worker_ids=["pool"],
                allocation_method=RunAllocationMethod.ROUND_ROBIN,
                func_import_path_or_key="add",
    
                func_args=(i, 1),
                func_kwargs={},
            )
            worker_ids.append(result.worker_id)

        # With only 1 worker, we should always see worker 0
        assert worker_ids == [0, 0, 0]

# %%
await test_round_robin_allocation();

# %%
#|export
@pytest.mark.asyncio
async def test_empty_workers_raises():
    """Test that empty worker list raises error."""
    manager = ExecutionManager({
        "pool": (SingleWorkerPool, {}),
    })

    async with manager:
        await manager.send_function("pool", 0, "add", add_numbers)

        with pytest.raises(ValueError, match="No workers available"):
            await manager.run_allocate(
                pool_worker_ids=[],
                allocation_method=RunAllocationMethod.ROUND_ROBIN,
                func_import_path_or_key="add",
    
                func_args=(1, 2),
                func_kwargs={},
            )

# %%
await test_empty_workers_raises();

# %% [markdown]
# ## Test get_worker_jobs

# %%
#|export
@pytest.mark.asyncio
async def test_get_worker_jobs_empty():
    """Test get_worker_jobs when no jobs are running."""
    manager = ExecutionManager({
        "pool": (SingleWorkerPool, {}),
    })

    async with manager:
        jobs = manager.get_worker_jobs("pool", 0)
        assert jobs == []

# %%
await test_get_worker_jobs_empty();

# %% [markdown]
# ## Test Multiple Pools

# %%
#|export
@pytest.mark.asyncio
async def test_multiple_pools():
    """Test running jobs on multiple SingleWorkerPools."""
    manager = ExecutionManager({
        "pool_a": (SingleWorkerPool, {}),
        "pool_b": (SingleWorkerPool, {}),
    })

    async with manager:
        await manager.send_function_to_pool("pool_a", "add", add_numbers)
        await manager.send_function_to_pool("pool_b", "multiply", multiply_numbers)

        # Run on pool_a
        result1 = await manager.run(
            pool_id="pool_a",
            worker_id=0,
            func_import_path_or_key="add",

            func_args=(5, 3),
            func_kwargs={},
        )

        # Run on pool_b
        result2 = await manager.run(
            pool_id="pool_b",
            worker_id=0,
            func_import_path_or_key="multiply",

            func_args=(4, 7),
            func_kwargs={},
        )

        assert result1.result == 8
        assert result1.pool_id == "pool_a"
        assert result2.result == 28
        assert result2.pool_id == "pool_b"

# %%
await test_multiple_pools();

# %% [markdown]
# ## Test Async Functions

# %%
#|export
@pytest.mark.asyncio
async def test_async_function():
    """Test running an async function."""
    manager = ExecutionManager({
        "pool": (SingleWorkerPool, {}),
    })

    async with manager:
        await manager.send_function("pool", 0, "async_add", async_add)

        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="async_add",

            func_args=(10, 20),
            func_kwargs={},
        )

        assert result.result == 30

# %%
await test_async_function();

# %% [markdown]
# ## Test using SingleWorkerPool class directly

# %%
#|export
@pytest.mark.asyncio
async def test_pool_class_directly():
    """Test using SingleWorkerPool class directly (not a string alias)."""
    manager = ExecutionManager({
        "pool": (SingleWorkerPool, {}),
    })

    async with manager:
        await manager.send_function("pool", 0, "add", add_numbers)

        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="add",

            func_args=(100, 200),
            func_kwargs={},
        )

        assert result.result == 300
        assert manager.get_num_workers("pool") == 1

# %%
await test_pool_class_directly();

# %% [markdown]
# ## Test Concurrent Async Execution

# %%
#|export
@pytest.mark.asyncio
async def test_concurrent_async_execution():
    """Test that multiple async RUN requests execute concurrently on SingleWorkerPool.

    Dispatches 3 async functions with asyncio.sleep(0.2) each to the same
    SingleWorkerPool. With serialization, total time >= 0.6s. With concurrency,
    total time ~= 0.2s.
    """
    import time

    async def slow_async_func(delay: float) -> str:
        await asyncio.sleep(delay)
        return "done"

    manager = ExecutionManager({
        "pool": (SingleWorkerPool, {}),
    })

    async with manager:
        await manager.send_function_to_pool("pool", "slow", slow_async_func)

        start = time.monotonic()

        # Dispatch 3 concurrent runs
        tasks = [
            asyncio.create_task(
                manager.run(
                    pool_id="pool",
                    worker_id=0,
                    func_import_path_or_key="slow",
        
                    func_args=(0.2,),
                    func_kwargs={},
                )
            )
            for _ in range(3)
        ]

        results = await asyncio.gather(*tasks)
        elapsed = time.monotonic() - start

        # All should succeed
        for r in results:
            assert r.result == "done"

        # If serialized, would take >= 0.6s. With concurrency, should be ~0.2s.
        assert elapsed < 0.5, (
            f"Took {elapsed:.2f}s — expected < 0.5s for concurrent execution of 3x0.2s tasks"
        )

# %%
await test_concurrent_async_execution();

# %% [markdown]
# ## Regression: Async worker exception does not hang

# %%
#|export
@pytest.mark.asyncio
async def test_worker_exception_does_not_hang():
    """Test that worker-level exceptions result in an error, not a hang.

    Regression test: async worker swallows exceptions in _handle_run,
    causing run() to hang forever waiting for UP_RUN_RESPONSE.
    """
    manager = ExecutionManager({
        "pool": (SingleWorkerPool, {}),
    })
    async with manager:
        await manager.send_function("pool", 0, "error_fn", function_with_error)

        # This should NOT hang. Before the fix, it would hang forever.
        with pytest.raises(Exception):
            await asyncio.wait_for(
                manager.run(
                    pool_id="pool",
                    worker_id=0,
                    func_import_path_or_key="error_fn",
        
                    func_args=(),
                    func_kwargs={},
                ),
                timeout=5.0,
            )

        # Worker should still be functional after the error
        await manager.send_function("pool", 0, "add", add_numbers)
        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="add",

            func_args=(10, 20),
            func_kwargs={},
        )
        assert result.result == 30

# %%
await test_worker_exception_does_not_hang();
