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
# ## Define Remote Worker Function
#
# The remote worker function needs to be registered on the server.
# It uses the same protocol as other pool workers.

# %%
#|export
from netrun.rpc.base import ChannelClosed

def execution_manager_worker(channel, worker_id: int) -> None:
    """Worker function for ExecutionManager remote tests.

    This worker handles the ExecutionManager protocol:
    - SEND_FUNC: Store a function by key
    - RUN: Execute a stored function
    """
    import pickle
    from datetime import datetime

    funcs = {}

    try:
        while True:
            key, data = channel.recv()

            if key == "SEND_FUNC":
                # Store function
                func_key = data["func_key"]
                func_bytes = data["func_bytes"]
                func = pickle.loads(func_bytes)
                funcs[func_key] = func
                channel.send("SEND_FUNC_ACK", {"func_key": func_key})

            elif key == "RUN":
                # Execute function
                run_id = data["run_id"]
                func_key = data["func_import_path_or_key"]
                func_args = data["func_args"]
                func_kwargs = data["func_kwargs"]

                timestamp_started = datetime.utcnow()

                try:
                    if func_key in funcs:
                        func = funcs[func_key]
                    else:
                        # Try to import the function
                        import importlib
                        module_path, func_name = func_key.rsplit(".", 1)
                        module = importlib.import_module(module_path)
                        func = getattr(module, func_name)

                    # Execute the function
                    if asyncio.iscoroutinefunction(func):
                        result = asyncio.get_event_loop().run_until_complete(
                            func(*func_args, **func_kwargs)
                        )
                    else:
                        result = func(*func_args, **func_kwargs)

                    timestamp_completed = datetime.utcnow()

                    channel.send("RUN_RESULT", {
                        "run_id": run_id,
                        "result": result,
                        "timestamp_utc_started": timestamp_started.isoformat(),
                        "timestamp_utc_completed": timestamp_completed.isoformat(),
                        "print_buffer": [],
                        "converted_to_str": False,
                    })

                except Exception as e:
                    timestamp_completed = datetime.utcnow()
                    channel.send("RUN_ERROR", {
                        "run_id": run_id,
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "timestamp_utc_started": timestamp_started.isoformat(),
                        "timestamp_utc_completed": timestamp_completed.isoformat(),
                    })

    except ChannelClosed:
        pass

# %% [markdown]
# ## Test Helper: Create Server and Manager
#
# Helper context manager to set up server and execution manager together.

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
async def create_remote_manager(num_processes: int = 1, threads_per_process: int = 1):
    """Create a remote server and execution manager for testing.

    This sets up a RemotePoolServer with the execution_manager_worker,
    and creates an ExecutionManager that connects to it.
    """
    port = _get_next_port()
    server = RemotePoolServer()
    server.register_worker("em_worker", execution_manager_worker)

    async with server.serve_background("127.0.0.1", port):
        # Create client and connect
        client = RemotePoolClient(f"ws://127.0.0.1:{port}")
        await client.connect()
        await client.create_pool("em_worker", num_processes=num_processes, threads_per_process=threads_per_process)

        try:
            yield client
        finally:
            await client.close()

# %% [markdown]
# ## Test Basic Remote Pool Operations
#
# These tests verify that RemotePoolClient works correctly outside of ExecutionManager.
# Full ExecutionManager integration with RemotePoolClient would require additional work
# to handle the different startup protocol (connect + create_pool vs start).

# %%
#|export
@pytest.mark.asyncio
async def test_remote_pool_creation():
    """Test creating a remote pool."""
    async with create_remote_manager(num_processes=2, threads_per_process=1) as client:
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
    """Test sending a function to a remote pool."""
    import pickle

    async with create_remote_manager(num_processes=1, threads_per_process=1) as client:
        # Send function
        func_bytes = pickle.dumps(add_numbers)
        await client.send(
            worker_id=0,
            key="SEND_FUNC",
            data={"func_key": "add", "func_bytes": func_bytes}
        )

        # Wait for acknowledgment
        msg = await client.recv(timeout=10.0)
        assert msg.key == "SEND_FUNC_ACK"
        assert msg.data["func_key"] == "add"

# %%
await test_remote_pool_send_function();

# %%
#|export
@pytest.mark.asyncio
async def test_remote_pool_run_function():
    """Test running a function on a remote pool."""
    import pickle

    async with create_remote_manager(num_processes=1, threads_per_process=1) as client:
        # Send function
        func_bytes = pickle.dumps(add_numbers)
        await client.send(
            worker_id=0,
            key="SEND_FUNC",
            data={"func_key": "add", "func_bytes": func_bytes}
        )

        # Wait for acknowledgment
        msg = await client.recv(timeout=10.0)
        assert msg.key == "SEND_FUNC_ACK"

        # Run function
        await client.send(
            worker_id=0,
            key="RUN",
            data={
                "run_id": "test_run_1",
                "func_import_path_or_key": "add",
                "func_args": (3, 4),
                "func_kwargs": {},
            }
        )

        # Get result
        msg = await client.recv(timeout=10.0)
        assert msg.key == "RUN_RESULT"
        assert msg.data["run_id"] == "test_run_1"
        assert msg.data["result"] == 7

# %%
await test_remote_pool_run_function();

# %%
#|export
@pytest.mark.asyncio
async def test_remote_pool_multiple_workers():
    """Test running functions on multiple remote workers."""
    import pickle

    async with create_remote_manager(num_processes=2, threads_per_process=1) as client:
        # Send function to all workers
        func_bytes = pickle.dumps(multiply_numbers)
        for worker_id in range(2):
            await client.send(
                worker_id=worker_id,
                key="SEND_FUNC",
                data={"func_key": "multiply", "func_bytes": func_bytes}
            )

        # Wait for acknowledgments
        for _ in range(2):
            msg = await client.recv(timeout=10.0)
            assert msg.key == "SEND_FUNC_ACK"

        # Run on each worker
        for worker_id in range(2):
            await client.send(
                worker_id=worker_id,
                key="RUN",
                data={
                    "run_id": f"run_{worker_id}",
                    "func_import_path_or_key": "multiply",
                    "func_args": (worker_id + 1, 10),
                    "func_kwargs": {},
                }
            )

        # Collect results
        results = {}
        for _ in range(2):
            msg = await client.recv(timeout=10.0)
            assert msg.key == "RUN_RESULT"
            results[msg.data["run_id"]] = msg.data["result"]

        assert results["run_0"] == 10  # 1 * 10
        assert results["run_1"] == 20  # 2 * 10

# %%
await test_remote_pool_multiple_workers();

# %%
#|export
@pytest.mark.asyncio
async def test_remote_pool_function_with_kwargs():
    """Test running a function with keyword arguments on remote pool."""
    import pickle

    async with create_remote_manager(num_processes=1, threads_per_process=1) as client:
        # Send function
        func_bytes = pickle.dumps(function_with_kwargs)
        await client.send(
            worker_id=0,
            key="SEND_FUNC",
            data={"func_key": "kwargs_fn", "func_bytes": func_bytes}
        )

        # Wait for acknowledgment
        msg = await client.recv(timeout=10.0)
        assert msg.key == "SEND_FUNC_ACK"

        # Run with kwargs
        await client.send(
            worker_id=0,
            key="RUN",
            data={
                "run_id": "test_kwargs",
                "func_import_path_or_key": "kwargs_fn",
                "func_args": (5,),
                "func_kwargs": {"b": 20, "c": 200},
            }
        )

        # Get result
        msg = await client.recv(timeout=10.0)
        assert msg.key == "RUN_RESULT"
        assert msg.data["result"] == 225  # 5 + 20 + 200

# %%
await test_remote_pool_function_with_kwargs();

# %%
#|export
@pytest.mark.asyncio
async def test_remote_pool_concurrent_runs():
    """Test running multiple functions concurrently on remote pool."""
    import pickle

    async with create_remote_manager(num_processes=2, threads_per_process=2) as client:
        # Send function to all workers
        func_bytes = pickle.dumps(add_numbers)
        for worker_id in range(4):
            await client.send(
                worker_id=worker_id,
                key="SEND_FUNC",
                data={"func_key": "add", "func_bytes": func_bytes}
            )

        # Wait for acknowledgments
        for _ in range(4):
            msg = await client.recv(timeout=10.0)
            assert msg.key == "SEND_FUNC_ACK"

        # Run on all workers concurrently
        for i in range(4):
            await client.send(
                worker_id=i,
                key="RUN",
                data={
                    "run_id": f"run_{i}",
                    "func_import_path_or_key": "add",
                    "func_args": (i, i),
                    "func_kwargs": {},
                }
            )

        # Collect all results
        results = {}
        for _ in range(4):
            msg = await client.recv(timeout=10.0)
            assert msg.key == "RUN_RESULT"
            results[msg.data["run_id"]] = msg.data["result"]

        # Verify results
        for i in range(4):
            assert results[f"run_{i}"] == i + i

# %%
await test_remote_pool_concurrent_runs();

# %% [markdown]
# ## Tests for ExecutionManager Integration (Skipped)
#
# The following tests are skipped because ExecutionManager.start() calls pool.start()
# but RemotePoolClient uses connect() + create_pool() instead.
# Full integration would require either:
# 1. Adding a start() method to RemotePoolClient that handles connection and pool creation
# 2. Modifying ExecutionManager to handle RemotePoolClient specially
#
# These tests demonstrate the intended usage once integration is complete.

# %%
#|export
@pytest.mark.skip(reason="RemotePoolClient integration with ExecutionManager not yet complete")
@pytest.mark.asyncio
async def test_execution_manager_with_remote_pool():
    """Test ExecutionManager with RemotePoolClient (pending integration)."""
    port = _get_next_port()
    server = RemotePoolServer()
    server.register_worker("em_worker", execution_manager_worker)

    async with server.serve_background("127.0.0.1", port):
        manager = ExecutionManager({
            "remote": (RemotePoolClient, {"url": f"ws://127.0.0.1:{port}"}),
        })

        async with manager:
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

# %%
# Skipped: await test_execution_manager_with_remote_pool();

# %%
#|export
@pytest.mark.skip(reason="RemotePoolClient integration with ExecutionManager not yet complete")
@pytest.mark.asyncio
async def test_execution_manager_remote_multiple_workers():
    """Test ExecutionManager with multiple remote workers (pending integration)."""
    port = _get_next_port()
    server = RemotePoolServer()
    server.register_worker("em_worker", execution_manager_worker)

    async with server.serve_background("127.0.0.1", port):
        manager = ExecutionManager({
            "remote": (RemotePoolClient, {
                "url": f"ws://127.0.0.1:{port}",
                "num_processes": 2,
                "threads_per_process": 2,
            }),
        })

        async with manager:
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
# Skipped: await test_execution_manager_remote_multiple_workers();
