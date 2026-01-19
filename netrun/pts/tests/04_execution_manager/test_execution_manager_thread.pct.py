# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for ExecutionManager with ThreadPools

# %%
#|default_exp execution_manager.test_execution_manager_thread

# %%
#|export
import pytest
import asyncio
from datetime import datetime

from netrun.pool.thread import ThreadPool
from netrun.pool.aio import SingleWorkerPool

from netrun.execution_manager import (
    ExecutionManager,
    RunAllocationMethod,
)

# Import worker functions from the workers module
from tests.execution_manager.workers import (
    add_numbers,
    multiply_numbers,
    function_with_print,
    slow_function,
    function_returns_non_serializable,
    async_add,
    function_with_kwargs,
    function_with_multiple_prints,
    slow_printing_function,
)

# %% [markdown]
# ## Test ExecutionManager Creation

# %%
#|export
def test_create_execution_manager():
    """Test creating an ExecutionManager."""
    manager = ExecutionManager({
        "pool1": ("thread", {"num_workers": 2}),
    })
    assert manager._started is False
    assert "pool1" in manager._pool_configs

# %%
test_create_execution_manager();

# %%
#|export
def test_create_multiple_pools():
    """Test creating ExecutionManager with multiple pools."""
    manager = ExecutionManager({
        "thread_pool": ("thread", {"num_workers": 2}),
        "main_pool": ("main", {}),
    })
    assert "thread_pool" in manager._pool_configs
    assert "main_pool" in manager._pool_configs

# %%
test_create_multiple_pools();

# %%
#|export
@pytest.mark.asyncio
async def test_invalid_pool_type():
    """Test that invalid pool type raises error."""
    manager = ExecutionManager({
        "pool": ("invalid_type", {}),
    })
    with pytest.raises(ValueError, match="Unknown pool type"):
        await manager.start()

# %%
await test_invalid_pool_type();

# %% [markdown]
# ## Test Starting and Closing

# %%
#|export
@pytest.mark.asyncio
async def test_start_and_close():
    """Test starting and closing the manager."""
    manager = ExecutionManager({
        "pool": (ThreadPool, {"num_workers": 1}),
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
        "pool": (ThreadPool, {"num_workers": 1}),
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
    """Test that immediate close after start doesn't raise errors.

    This verifies the fix for the race condition where recv tasks
    might try to recv from a closed pool.
    """
    # Run multiple times to catch race conditions
    for _ in range(10):
        manager = ExecutionManager({
            "pool": (ThreadPool, {"num_workers": 2}),
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
        "pool": (ThreadPool, {"num_workers": 1}),
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
        "pool_a": (ThreadPool, {"num_workers": 1}),
        "pool_b": (ThreadPool, {"num_workers": 2}),
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
        "pool_a": (ThreadPool, {"num_workers": 3}),
        "pool_b": (ThreadPool, {"num_workers": 1}),
    })

    async with manager:
        assert manager.get_num_workers("pool_a") == 3
        assert manager.get_num_workers("pool_b") == 1

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
        "pool": (ThreadPool, {"num_workers": 1}),
    })

    async with manager:
        # Send the function to the worker
        await manager.send_function("pool", 0, "add", add_numbers)

        # Run the function
        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="add",
            send_channel=False,
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
        "pool": (ThreadPool, {"num_workers": 3}),
    })

    async with manager:
        # Send the function to all workers
        await manager.send_function_to_pool("pool", "multiply", multiply_numbers)

        # Run on each worker
        results = []
        for worker_id in range(3):
            result = await manager.run(
                pool_id="pool",
                worker_id=worker_id,
                func_import_path_or_key="multiply",
                send_channel=False,
                func_args=(worker_id + 1, 10),
                func_kwargs={},
            )
            results.append(result.result)

        assert results == [10, 20, 30]

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
        "pool": (ThreadPool, {"num_workers": 1}),
    })

    async with manager:
        await manager.send_function("pool", 0, "slow", slow_function)

        before = datetime.utcnow()
        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="slow",
            send_channel=False,
            func_args=(0.1,),
            func_kwargs={},
        )
        after = datetime.utcnow()

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
async def test_non_serializable_result_for_main_process():
    """Test that non-serializable results are not converted to string if the worker is in the main process."""
    manager = ExecutionManager({
        "pool": (ThreadPool, {"num_workers": 1}),
    })

    async with manager:
        await manager.send_function("pool", 0, "nonserialized", function_returns_non_serializable)

        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="nonserialized",
            send_channel=False,
            func_args=(),
            func_kwargs={},
        )

        assert result.converted_to_str is False
        assert not isinstance(result.result, str)

# %%
await test_non_serializable_result_for_main_process();

# %% [markdown]
# ## Test Function with kwargs

# %%
#|export
@pytest.mark.asyncio
async def test_function_with_kwargs():
    """Test running a function with keyword arguments."""
    manager = ExecutionManager({
        "pool": (ThreadPool, {"num_workers": 1}),
    })

    async with manager:
        await manager.send_function("pool", 0, "kwargs_fn", function_with_kwargs)

        # Test with only positional arg
        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="kwargs_fn",
            send_channel=False,
            func_args=(1,),
            func_kwargs={},
        )
        assert result.result == 111  # 1 + 10 + 100

        # Test with kwargs
        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="kwargs_fn",
            send_channel=False,
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
    """Test round-robin job allocation."""
    manager = ExecutionManager({
        "pool": (ThreadPool, {"num_workers": 3}),
    })

    async with manager:
        await manager.send_function_to_pool("pool", "add", add_numbers)

        # Run 6 jobs sequentially with round-robin
        worker_ids = []
        for i in range(6):
            result = await manager.run_allocate(
                pool_worker_ids=["pool"],
                allocation_method=RunAllocationMethod.ROUND_ROBIN,
                func_import_path_or_key="add",
                send_channel=False,
                func_args=(i, 1),
                func_kwargs={},
            )
            worker_ids.append(result.worker_id)

        # With round-robin, we should see workers 0, 1, 2, 0, 1, 2
        assert worker_ids == [0, 1, 2, 0, 1, 2]

# %%
await test_round_robin_allocation();

# %%
#|export
@pytest.mark.asyncio
async def test_random_allocation():
    """Test random job allocation."""
    manager = ExecutionManager({
        "pool": (ThreadPool, {"num_workers": 3}),
    })

    async with manager:
        await manager.send_function_to_pool("pool", "add", add_numbers)

        # Run many jobs with random allocation
        worker_ids = set()
        for i in range(20):
            result = await manager.run_allocate(
                pool_worker_ids=["pool"],
                allocation_method=RunAllocationMethod.RANDOM,
                func_import_path_or_key="add",
                send_channel=False,
                func_args=(i, 1),
                func_kwargs={},
            )
            worker_ids.add(result.worker_id)

        # With 20 jobs and 3 workers, we should see all workers
        assert len(worker_ids) == 3

# %%
await test_random_allocation();

# %%
#|export
@pytest.mark.asyncio
async def test_allocation_with_specific_workers():
    """Test allocation with specific pool/worker pairs."""
    manager = ExecutionManager({
        "pool": (ThreadPool, {"num_workers": 3}),
    })

    async with manager:
        await manager.send_function_to_pool("pool", "add", add_numbers)

        # Only allow workers 0 and 2
        worker_ids = set()
        for i in range(10):
            result = await manager.run_allocate(
                pool_worker_ids=[("pool", 0), ("pool", 2)],
                allocation_method=RunAllocationMethod.ROUND_ROBIN,
                func_import_path_or_key="add",
                send_channel=False,
                func_args=(i, 1),
                func_kwargs={},
            )
            worker_ids.add(result.worker_id)

        # Should only see workers 0 and 2
        assert worker_ids == {0, 2}

# %%
await test_allocation_with_specific_workers();

# %%
#|export
@pytest.mark.asyncio
async def test_empty_workers_raises():
    """Test that empty worker list raises error."""
    manager = ExecutionManager({
        "pool": (ThreadPool, {"num_workers": 1}),
    })

    async with manager:
        await manager.send_function("pool", 0, "add", add_numbers)

        with pytest.raises(ValueError, match="No workers available"):
            await manager.run_allocate(
                pool_worker_ids=[],
                allocation_method=RunAllocationMethod.ROUND_ROBIN,
                func_import_path_or_key="add",
                send_channel=False,
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
        "pool": (ThreadPool, {"num_workers": 1}),
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
    """Test running jobs on multiple pools."""
    manager = ExecutionManager({
        "fast": (ThreadPool, {"num_workers": 2}),
        "slow": (ThreadPool, {"num_workers": 1}),
    })

    async with manager:
        await manager.send_function_to_pool("fast", "add", add_numbers)
        await manager.send_function_to_pool("slow", "multiply", multiply_numbers)

        # Run on fast pool
        result1 = await manager.run(
            pool_id="fast",
            worker_id=0,
            func_import_path_or_key="add",
            send_channel=False,
            func_args=(5, 3),
            func_kwargs={},
        )

        # Run on slow pool
        result2 = await manager.run(
            pool_id="slow",
            worker_id=0,
            func_import_path_or_key="multiply",
            send_channel=False,
            func_args=(4, 7),
            func_kwargs={},
        )

        assert result1.result == 8
        assert result1.pool_id == "fast"
        assert result2.result == 28
        assert result2.pool_id == "slow"

# %%
await test_multiple_pools();

# %% [markdown]
# ## Test Concurrent Jobs

# %%
#|export
@pytest.mark.asyncio
async def test_concurrent_jobs():
    """Test running multiple jobs concurrently."""
    manager = ExecutionManager({
        "pool": (ThreadPool, {"num_workers": 3}),
    })

    async with manager:
        await manager.send_function_to_pool("pool", "add", add_numbers)

        # Run multiple jobs concurrently
        tasks = []
        for i in range(10):
            task = asyncio.create_task(
                manager.run_allocate(
                    pool_worker_ids=["pool"],
                    allocation_method=RunAllocationMethod.ROUND_ROBIN,
                    func_import_path_or_key="add",
                    send_channel=False,
                    func_args=(i, i),
                    func_kwargs={},
                )
            )
            tasks.append(task)

        results = await asyncio.gather(*tasks)

        # Check all results are correct
        for i, result in enumerate(results):
            assert result.result == i + i

# %%
await test_concurrent_jobs();

# %% [markdown]
# ## Test Async Functions

# %%
#|export
@pytest.mark.asyncio
async def test_async_function():
    """Test running an async function."""
    manager = ExecutionManager({
        "pool": (ThreadPool, {"num_workers": 1}),
    })

    async with manager:
        await manager.send_function("pool", 0, "async_add", async_add)

        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="async_add",
            send_channel=False,
            func_args=(10, 20),
            func_kwargs={},
        )

        assert result.result == 30

# %%
await test_async_function();

# %% [markdown]
# ## Test Main Pool (SingleWorkerPool)

# %%
#|export
@pytest.mark.asyncio
async def test_main_pool():
    """Test using the 'main' pool type (SingleWorkerPool)."""
    manager = ExecutionManager({
        "main": (SingleWorkerPool, {}),
    })

    async with manager:
        await manager.send_function("main", 0, "add", add_numbers)

        result = await manager.run(
            pool_id="main",
            worker_id=0,
            func_import_path_or_key="add",
            send_channel=False,
            func_args=(100, 200),
            func_kwargs={},
        )

        assert result.result == 300

# %%
await test_main_pool();

# %% [markdown]
# ## Test Print Buffer

# %%
#|export
@pytest.mark.asyncio
async def test_print_buffer_in_result():
    """Test that JobResult contains the print buffer."""
    manager = ExecutionManager({
        "pool": (ThreadPool, {"num_workers": 1}),
    })

    async with manager:
        await manager.send_function("pool", 0, "print_fn", function_with_print)

        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="print_fn",
            send_channel=False,
            func_args=("World",),
            func_kwargs={},
        )

        assert result.result == "greeted World"
        # Check print buffer contains the captured print
        assert len(result.print_buffer) == 1
        timestamp, text = result.print_buffer[0]
        assert "Hello, World!" in text
        assert isinstance(timestamp, datetime)

# %%
await test_print_buffer_in_result();

# %%
#|export
@pytest.mark.asyncio
async def test_print_buffer_multiple_prints():
    """Test that JobResult captures multiple print statements."""
    manager = ExecutionManager({
        "pool": (ThreadPool, {"num_workers": 1}),
    })

    async with manager:
        await manager.send_function("pool", 0, "multi_print", function_with_multiple_prints)

        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="multi_print",
            send_channel=False,
            func_args=(5,),
            func_kwargs={},
        )

        assert result.result == "printed 5 lines"
        assert len(result.print_buffer) == 5
        for i, (timestamp, text) in enumerate(result.print_buffer):
            assert f"Line {i}" in text

# %%
await test_print_buffer_multiple_prints();

# %%
#|export
@pytest.mark.asyncio
async def test_on_print_callback():
    """Test the on_print callback receives print buffers during execution."""
    # Use a short flush interval to get multiple callbacks
    manager = ExecutionManager({
        "pool": (ThreadPool, {"num_workers": 1, "print_flush_interval": 0.05}),
    })

    received_buffers: list[list[tuple[datetime, str]]] = []

    def on_print(buffer):
        received_buffers.append(buffer)

    async with manager:
        await manager.send_function("pool", 0, "slow_print", slow_printing_function)

        # Run function that prints 5 times with delays
        result = await manager.run(
            pool_id="pool",
            worker_id=0,
            func_import_path_or_key="slow_print",
            send_channel=False,
            func_args=(5, 0.08),  # 5 prints, 80ms delay each
            func_kwargs={},
            on_print=on_print,
        )

        assert result.result == "done"
        # All prints should be in the final result
        assert len(result.print_buffer) == 5

        # The on_print callback should have been called at least once
        # (since we have 5 prints with 80ms delay and 50ms flush interval)
        assert len(received_buffers) >= 1

        # All received buffers combined should equal the final print_buffer
        all_received = []
        for buf in received_buffers:
            all_received.extend(buf)
        assert len(all_received) == len(result.print_buffer)

# %%
await test_on_print_callback();
