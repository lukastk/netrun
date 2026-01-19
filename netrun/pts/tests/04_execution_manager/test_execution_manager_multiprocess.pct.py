# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for ExecutionManager with MultiprocessPools

# %%
#|default_exp execution_manager.test_execution_manager_multiprocess

# %%
#|export
import pytest
import asyncio
from datetime import datetime

from netrun.pool.thread import ThreadPool
from netrun.pool.multiprocess import MultiprocessPool
from netrun.pool.aio import SingleWorkerPool
from netrun.pool.remote import RemotePoolClient

from netrun.execution_manager import (
    ExecutionManager,
    RunAllocationMethod,
)

# %% [markdown]
# ## Test Helper Functions
#
# Simple functions for testing.

# %%
#|export
def add_numbers(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b

def multiply_numbers(x: int, y: int) -> int:
    """Multiply two numbers."""
    return x * y

def function_with_print(name: str) -> str:
    """A function that prints."""
    print(f"Hello, {name}!")
    return f"greeted {name}"

def slow_function(delay: float) -> str:
    """A function that takes some time."""
    import time
    time.sleep(delay)
    return "done"

def function_with_error() -> None:
    """A function that raises an error."""
    raise ValueError("Intentional error")

def function_returns_non_serializable():
    """A function that returns something non-serializable."""
    return lambda x: x  # Lambdas can't be pickled

async def async_add(a: int, b: int) -> int:
    """Async function that adds two numbers."""
    await asyncio.sleep(0.01)
    return a + b

def function_with_kwargs(a: int, b: int = 10, c: int = 100) -> int:
    """Function with keyword arguments."""
    return a + b + c

# %% [markdown]
# ## Test Multiprocess Pool Stdout Helper Methods

# %%
#|export
import sys

def mp_stdout_function(message: str) -> str:
    """A function that writes directly to stdout to test subprocess output capture.

    This uses sys.stdout.write which bypasses the ExecutionManager's print capture
    and goes directly to the subprocess stdout, which is captured by the MultiprocessPool.
    """
    sys.stdout.write(f"MP Output: {message}\n")
    sys.stdout.flush()
    return f"printed {message}"

# %%
#|export
@pytest.mark.asyncio
async def test_get_process_ids():
    """Test get_process_ids returns correct process indices."""
    manager = ExecutionManager({
        "mp_pool": (MultiprocessPool, {"num_processes": 3, "threads_per_process": 2}),
    })

    async with manager:
        process_ids = manager.get_process_ids("mp_pool")
        assert process_ids == [0, 1, 2]

# %%
await test_get_process_ids();

# %%
#|export
@pytest.mark.asyncio
async def test_get_process_ids_raises_for_non_multiprocess():
    """Test that get_process_ids raises ValueError for non-MultiprocessPool."""
    manager = ExecutionManager({
        "thread_pool": (ThreadPool, {"num_workers": 2}),
    })

    async with manager:
        with pytest.raises(ValueError, match="not a MultiprocessPool"):
            manager.get_process_ids("thread_pool")

# %%
await test_get_process_ids_raises_for_non_multiprocess();

# %%
#|export
@pytest.mark.asyncio
async def test_flush_pool_stdout():
    """Test flush_pool_stdout for a specific process."""
    manager = ExecutionManager({
        "mp_pool": (MultiprocessPool, {
            "num_processes": 2,
            "threads_per_process": 1,
            "redirect_output": True,
            "buffer_output": True,
        }),
    })

    async with manager:
        await manager.send_function_to_pool("mp_pool", "mp_print", mp_stdout_function)

        # Run on process 0
        result = await manager.run(
            pool_id="mp_pool",
            worker_id=0,
            func_import_path_or_key="mp_print",
            send_channel=False,
            func_args=("hello",),
            func_kwargs={},
        )

        assert result.result == "printed hello"

        # Flush stdout from process 0
        buffer = await manager.flush_pool_stdout("mp_pool", 0)

        # Buffer should contain the print output
        stdout_texts = [text for _, is_stdout, text in buffer if is_stdout]
        combined = "".join(stdout_texts)
        assert "MP Output: hello" in combined

# %%
await test_flush_pool_stdout();

# %%
#|export
@pytest.mark.asyncio
async def test_flush_pool_stdout_raises_for_non_multiprocess():
    """Test that flush_pool_stdout raises ValueError for non-MultiprocessPool."""
    manager = ExecutionManager({
        "thread_pool": (ThreadPool, {"num_workers": 2}),
    })

    async with manager:
        with pytest.raises(ValueError, match="not a MultiprocessPool"):
            await manager.flush_pool_stdout("thread_pool", 0)

# %%
await test_flush_pool_stdout_raises_for_non_multiprocess();

# %%
#|export
@pytest.mark.asyncio
async def test_flush_all_pool_stdout():
    """Test flush_all_pool_stdout for all processes."""
    manager = ExecutionManager({
        "mp_pool": (MultiprocessPool, {
            "num_processes": 2,
            "threads_per_process": 1,
            "redirect_output": True,
            "buffer_output": True,
        }),
    })

    async with manager:
        await manager.send_function_to_pool("mp_pool", "mp_print", mp_stdout_function)

        # Run on both workers (process 0 and process 1)
        result0 = await manager.run(
            pool_id="mp_pool",
            worker_id=0,
            func_import_path_or_key="mp_print",
            send_channel=False,
            func_args=("proc0",),
            func_kwargs={},
        )
        result1 = await manager.run(
            pool_id="mp_pool",
            worker_id=1,
            func_import_path_or_key="mp_print",
            send_channel=False,
            func_args=("proc1",),
            func_kwargs={},
        )

        assert result0.result == "printed proc0"
        assert result1.result == "printed proc1"

        # Flush stdout from all processes
        buffers = await manager.flush_all_pool_stdout("mp_pool")

        assert len(buffers) == 2
        assert 0 in buffers
        assert 1 in buffers

        # Check each process has captured its output
        for process_idx, expected_msg in [(0, "proc0"), (1, "proc1")]:
            stdout_texts = [text for _, is_stdout, text in buffers[process_idx] if is_stdout]
            combined = "".join(stdout_texts)
            assert f"MP Output: {expected_msg}" in combined

# %%
await test_flush_all_pool_stdout();

# %%
#|export
@pytest.mark.asyncio
async def test_flush_all_pool_stdout_raises_for_non_multiprocess():
    """Test that flush_all_pool_stdout raises ValueError for non-MultiprocessPool."""
    manager = ExecutionManager({
        "thread_pool": (ThreadPool, {"num_workers": 2}),
    })

    async with manager:
        with pytest.raises(ValueError, match="not a MultiprocessPool"):
            await manager.flush_all_pool_stdout("thread_pool")

# %%
await test_flush_all_pool_stdout_raises_for_non_multiprocess();
