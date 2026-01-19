# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for ExecutionManager with RemotePoolClient
#
# RemotePoolClient connects to a RemotePoolServer over WebSockets to run tasks
# on a remote machine. This is useful for distributed computing scenarios.
#
# **Note:** RemotePoolClient requires a server to connect to. These tests spin up
# a local server for testing purposes.

# %%
#|default_exp execution_manager.test_execution_manager_remote

# %%
#|export
import pytest
import asyncio
from datetime import datetime

# Check if websockets is available
try:
    import websockets
    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False

pytestmark = pytest.mark.skipif(not HAS_WEBSOCKETS, reason="websockets not installed")

# %%
#|export
from netrun.pool.remote import RemotePoolServer, RemotePoolClient

from netrun.execution_manager import (
    ExecutionManager,
    RunAllocationMethod,
    ExecutionManagerProtocolKeys,
    remote_execution_manager_worker,
)

# Import worker functions from the workers module
from tests.execution_manager.workers import (
    add_numbers,
    multiply_numbers,
    function_with_print,
    slow_function,
    function_with_kwargs,
    async_add,
    function_with_multiple_prints,
)

# %% [markdown]
# ## Test Helpers
#
# Helper context managers to set up server and clients/managers for testing.

# %%
#|export
from contextlib import asynccontextmanager

# Port counter to avoid conflicts between tests
_test_port = 19100

def _get_next_port() -> int:
    global _test_port
    _test_port += 1
    return _test_port

@asynccontextmanager
async def create_remote_client(num_processes: int = 1, threads_per_process: int = 1):
    """Create a remote server and client for testing (low-level tests).

    This sets up a RemotePoolServer with the remote_execution_manager_worker,
    and creates a RemotePoolClient that connects to it.
    """
    port = _get_next_port()
    server = RemotePoolServer()
    server.register_worker("em_worker", remote_execution_manager_worker)

    async with server.serve_background("127.0.0.1", port):
        # Create client and connect
        client = RemotePoolClient(f"ws://127.0.0.1:{port}")
        await client.connect()
        await client.create_pool("em_worker", num_processes=num_processes, threads_per_process=threads_per_process)

        try:
            yield client
        finally:
            await client.close()

@asynccontextmanager
async def create_remote_execution_manager(num_processes: int = 1, threads_per_process: int = 1):
    """Create a remote server and ExecutionManager for testing.

    This sets up a RemotePoolServer with the remote_execution_manager_worker,
    and creates an ExecutionManager with a RemotePoolClient that connects to it.
    """
    port = _get_next_port()
    server = RemotePoolServer()
    server.register_worker("em_worker", remote_execution_manager_worker)

    async with server.serve_background("127.0.0.1", port):
        manager = ExecutionManager({
            "remote": (RemotePoolClient, {
                "url": f"ws://127.0.0.1:{port}",
                "worker_name": "em_worker",
                "num_processes": num_processes,
                "threads_per_process": threads_per_process,
            }),
        })

        async with manager:
            yield manager

# %% [markdown]
# ## Test Basic Remote Pool Operations
#
# These tests verify that RemotePoolClient works correctly with the
# ExecutionManager protocol at the low level.

# %%
#|export
@pytest.mark.asyncio
async def test_remote_pool_creation():
    """Test creating a remote pool."""
    async with create_remote_client(num_processes=2, threads_per_process=1) as client:
        assert client.is_running
        assert client.num_workers == 2
        assert client.num_processes == 2
        assert client.threads_per_process == 1

# %%
await test_remote_pool_creation();

# %%
#|export
@pytest.mark.asyncio
async def test_remote_pool_send_function():
    """Test sending a function to a remote pool using ExecutionManager protocol."""
    async with create_remote_client(num_processes=1, threads_per_process=1) as client:
        # Send function using ExecutionManager protocol
        await client.send(
            worker_id=0,
            key=ExecutionManagerProtocolKeys.SEND_FUNCTION.value,
            data=("msg_1", "add", add_numbers)
        )

        # Wait for acknowledgment
        msg = await client.recv(timeout=10.0)
        assert msg.key == ExecutionManagerProtocolKeys.UP_SEND_FUNCTION_RESPONSE.value
        assert msg.data[0] == "msg_1"

# %%
await test_remote_pool_send_function();

# %%
#|export
@pytest.mark.asyncio
async def test_remote_pool_run_function():
    """Test running a function on a remote pool using ExecutionManager protocol."""
    async with create_remote_client(num_processes=1, threads_per_process=1) as client:
        # Send function using ExecutionManager protocol
        await client.send(
            worker_id=0,
            key=ExecutionManagerProtocolKeys.SEND_FUNCTION.value,
            data=("msg_1", "add", add_numbers)
        )

        # Wait for acknowledgment
        msg = await client.recv(timeout=10.0)
        assert msg.key == ExecutionManagerProtocolKeys.UP_SEND_FUNCTION_RESPONSE.value

        # Run function using ExecutionManager protocol
        await client.send(
            worker_id=0,
            key=ExecutionManagerProtocolKeys.RUN.value,
            data=("msg_2", "add", "run_1", False, (3, 4), {})
        )

        # Get RUN_STARTED
        msg = await client.recv(timeout=10.0)
        assert msg.key == ExecutionManagerProtocolKeys.UP_RUN_STARTED.value

        # Get result
        msg = await client.recv(timeout=10.0)
        assert msg.key == ExecutionManagerProtocolKeys.UP_RUN_RESPONSE.value
        # Data format: (msg_id, timestamp_utc_started, timestamp_utc_completed, converted_to_str, result)
        msg_id, ts_started, ts_completed, converted, result = msg.data
        assert msg_id == "msg_2"
        assert result == 7

# %%
await test_remote_pool_run_function();

# %%
#|export
@pytest.mark.asyncio
async def test_remote_pool_multiple_workers():
    """Test running functions on multiple remote workers."""
    async with create_remote_client(num_processes=2, threads_per_process=1) as client:
        # Send function to all workers
        for worker_id in range(2):
            await client.send(
                worker_id=worker_id,
                key=ExecutionManagerProtocolKeys.SEND_FUNCTION.value,
                data=(f"msg_send_{worker_id}", "multiply", multiply_numbers)
            )

        # Wait for acknowledgments
        for _ in range(2):
            msg = await client.recv(timeout=10.0)
            assert msg.key == ExecutionManagerProtocolKeys.UP_SEND_FUNCTION_RESPONSE.value

        # Run on each worker
        for worker_id in range(2):
            await client.send(
                worker_id=worker_id,
                key=ExecutionManagerProtocolKeys.RUN.value,
                data=(f"msg_run_{worker_id}", "multiply", f"run_{worker_id}", False, (worker_id + 1, 10), {})
            )

        # Collect results (2 RUN_STARTED + 2 RUN_RESPONSE)
        started_count = 0
        results = {}
        for _ in range(4):
            msg = await client.recv(timeout=10.0)
            if msg.key == ExecutionManagerProtocolKeys.UP_RUN_STARTED.value:
                started_count += 1
            elif msg.key == ExecutionManagerProtocolKeys.UP_RUN_RESPONSE.value:
                msg_id = msg.data[0]
                result = msg.data[4]
                results[msg_id] = result

        assert started_count == 2
        assert results["msg_run_0"] == 10  # 1 * 10
        assert results["msg_run_1"] == 20  # 2 * 10

# %%
await test_remote_pool_multiple_workers();

# %%
#|export
@pytest.mark.asyncio
async def test_remote_pool_function_with_kwargs():
    """Test running a function with keyword arguments on remote pool."""
    async with create_remote_client(num_processes=1, threads_per_process=1) as client:
        # Send function
        await client.send(
            worker_id=0,
            key=ExecutionManagerProtocolKeys.SEND_FUNCTION.value,
            data=("msg_1", "kwargs_fn", function_with_kwargs)
        )

        # Wait for acknowledgment
        msg = await client.recv(timeout=10.0)
        assert msg.key == ExecutionManagerProtocolKeys.UP_SEND_FUNCTION_RESPONSE.value

        # Run with kwargs
        await client.send(
            worker_id=0,
            key=ExecutionManagerProtocolKeys.RUN.value,
            data=("msg_2", "kwargs_fn", "run_1", False, (5,), {"b": 20, "c": 200})
        )

        # Get RUN_STARTED
        msg = await client.recv(timeout=10.0)
        assert msg.key == ExecutionManagerProtocolKeys.UP_RUN_STARTED.value

        # Get result
        msg = await client.recv(timeout=10.0)
        assert msg.key == ExecutionManagerProtocolKeys.UP_RUN_RESPONSE.value
        result = msg.data[4]
        assert result == 225  # 5 + 20 + 200

# %%
await test_remote_pool_function_with_kwargs();

# %%
#|export
@pytest.mark.asyncio
async def test_remote_pool_concurrent_runs():
    """Test running multiple functions concurrently on remote pool."""
    async with create_remote_client(num_processes=2, threads_per_process=2) as client:
        # Send function to all workers
        for worker_id in range(4):
            await client.send(
                worker_id=worker_id,
                key=ExecutionManagerProtocolKeys.SEND_FUNCTION.value,
                data=(f"msg_send_{worker_id}", "add", add_numbers)
            )

        # Wait for acknowledgments
        for _ in range(4):
            msg = await client.recv(timeout=10.0)
            assert msg.key == ExecutionManagerProtocolKeys.UP_SEND_FUNCTION_RESPONSE.value

        # Run on all workers concurrently
        for i in range(4):
            await client.send(
                worker_id=i,
                key=ExecutionManagerProtocolKeys.RUN.value,
                data=(f"msg_run_{i}", "add", f"run_{i}", False, (i, i), {})
            )

        # Collect all results (4 RUN_STARTED + 4 RUN_RESPONSE)
        started_count = 0
        results = {}
        for _ in range(8):
            msg = await client.recv(timeout=10.0)
            if msg.key == ExecutionManagerProtocolKeys.UP_RUN_STARTED.value:
                started_count += 1
            elif msg.key == ExecutionManagerProtocolKeys.UP_RUN_RESPONSE.value:
                msg_id = msg.data[0]
                result = msg.data[4]
                results[msg_id] = result

        assert started_count == 4
        # Verify results
        for i in range(4):
            assert results[f"msg_run_{i}"] == i + i

# %%
await test_remote_pool_concurrent_runs();

# %% [markdown]
# ## Tests for ExecutionManager Integration
#
# These tests verify that ExecutionManager works correctly with RemotePoolClient.
# The RemotePoolClient.start() method handles both connection and pool creation,
# making it compatible with ExecutionManager's startup protocol.

# %%
#|export
@pytest.mark.asyncio
async def test_execution_manager_with_remote_pool():
    """Test ExecutionManager with RemotePoolClient."""
    async with create_remote_execution_manager(num_processes=1, threads_per_process=1) as manager:
        await manager.send_function_to_pool("remote", "add", add_numbers)

        result = await manager.run(
            pool_id="remote",
            worker_id=0,
            func_import_path_or_key="add",
            send_channel=False,
            func_args=(3, 4),
            func_kwargs={},
        )

        assert result.result == 7
        assert result.pool_id == "remote"
        assert result.worker_id == 0

# %%
await test_execution_manager_with_remote_pool();

# %%
#|export
@pytest.mark.asyncio
async def test_execution_manager_remote_multiple_workers():
    """Test ExecutionManager with multiple remote workers."""
    async with create_remote_execution_manager(num_processes=2, threads_per_process=2) as manager:
        await manager.send_function_to_pool("remote", "multiply", multiply_numbers)

        # Run on each worker
        results = []
        for worker_id in range(4):
            result = await manager.run(
                pool_id="remote",
                worker_id=worker_id,
                func_import_path_or_key="multiply",
                send_channel=False,
                func_args=(worker_id + 1, 10),
                func_kwargs={},
            )
            results.append(result.result)

        assert results == [10, 20, 30, 40]

# %%
await test_execution_manager_remote_multiple_workers();

# %%
#|export
@pytest.mark.asyncio
async def test_execution_manager_remote_context_manager():
    """Test using ExecutionManager as async context manager with RemotePoolClient."""
    async with create_remote_execution_manager() as manager:
        assert manager._started is True
        pool_ids = [pool_id for pool_id, _ in manager.pools]
        assert "remote" in pool_ids

# %%
await test_execution_manager_remote_context_manager();

# %%
#|export
@pytest.mark.asyncio
async def test_execution_manager_remote_kwargs():
    """Test running a function with keyword arguments on remote pool via ExecutionManager."""
    async with create_remote_execution_manager() as manager:
        await manager.send_function_to_pool("remote", "kwargs_fn", function_with_kwargs)

        result = await manager.run(
            pool_id="remote",
            worker_id=0,
            func_import_path_or_key="kwargs_fn",
            send_channel=False,
            func_args=(5,),
            func_kwargs={"b": 20, "c": 200},
        )

        assert result.result == 225  # 5 + 20 + 200

# %%
await test_execution_manager_remote_kwargs();

# %%
#|export
@pytest.mark.asyncio
async def test_execution_manager_remote_timestamps():
    """Test that JobResult from remote pool has correct timestamps."""
    async with create_remote_execution_manager() as manager:
        await manager.send_function_to_pool("remote", "slow", slow_function)

        result = await manager.run(
            pool_id="remote",
            worker_id=0,
            func_import_path_or_key="slow",
            send_channel=False,
            func_args=(0.05,),
            func_kwargs={},
        )

        # Check timestamps are in correct order
        assert result.timestamp_utc_submitted <= result.timestamp_utc_started
        assert result.timestamp_utc_started <= result.timestamp_utc_completed
        assert result.result == "done"

# %%
await test_execution_manager_remote_timestamps();
