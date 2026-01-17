# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Remote Pool Exception Tests
#
# This notebook provides comprehensive tests and examples for all exception types
# that can occur in the Remote Pool layer. The RemotePool uses WebSockets for
# communication between a client and server, with the server running a MultiprocessPool.
#
# ## Exception Types
#
# The RemotePoolClient can raise the following exceptions:
#
# 1. **PoolNotStarted**: Trying to use the pool before creating/connecting
# 2. **PoolError**: Server-side pool errors
# 3. **RecvTimeout**: A receive operation timed out waiting for a message
# 4. **WorkerException**: The worker function raised an exception
# 5. **WorkerCrashed**: The worker process died unexpectedly
# 6. **ValueError**: Invalid worker_id
# 7. **ConnectionError**: Failed to connect to server

# %%
#|default_exp pool.test_exceptions_remote

# %%
#|export
import pytest
import time
from netrun.rpc.base import RecvTimeout
from netrun.pool.base import (
    PoolError,
    PoolNotStarted,
    WorkerException,
    WorkerCrashed,
)
from netrun.pool.remote import RemotePoolServer, RemotePoolClient

# %% [markdown]
# ## Worker Functions
#
# Workers must be importable for multiprocessing.

# %%
#|export
from tests.pool.workers import echo_worker, compute_worker, raising_worker

# %% [markdown]
# ---
# # PoolNotStarted Exception
#
# `PoolNotStarted` is raised when trying to use the client before connecting
# or creating a pool.

# %% [markdown]
# ## 1.1 PoolNotStarted on send()

# %%
#|export
@pytest.mark.asyncio
async def test_send_before_create_pool():
    """RemotePoolClient.send() raises PoolNotStarted before create_pool()."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 29901):
        async with RemotePoolClient("ws://127.0.0.1:29901") as client:
            # Connected but no pool created yet
            with pytest.raises(PoolNotStarted) as exc_info:
                await client.send(0, "hello", "world")

            assert "not created" in str(exc_info.value).lower()

# %%
await test_send_before_create_pool()
print("Send before create_pool: raises PoolNotStarted as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_recv_before_create_pool():
    """RemotePoolClient.recv() raises PoolNotStarted before create_pool()."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 29902):
        async with RemotePoolClient("ws://127.0.0.1:29902") as client:
            with pytest.raises(PoolNotStarted):
                await client.recv(timeout=0.1)

# %%
await test_recv_before_create_pool()
print("Recv before create_pool: raises PoolNotStarted as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_before_create_pool():
    """RemotePoolClient.try_recv() raises PoolNotStarted before create_pool()."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 29903):
        async with RemotePoolClient("ws://127.0.0.1:29903") as client:
            with pytest.raises(PoolNotStarted):
                await client.try_recv()

# %%
await test_try_recv_before_create_pool()
print("Try_recv before create_pool: raises PoolNotStarted as expected")

# %%
#|export
@pytest.mark.asyncio
async def test_broadcast_before_create_pool():
    """RemotePoolClient.broadcast() raises PoolNotStarted before create_pool()."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 29904):
        async with RemotePoolClient("ws://127.0.0.1:29904") as client:
            with pytest.raises(PoolNotStarted):
                await client.broadcast("hello", "world")

# %%
await test_broadcast_before_create_pool()
print("Broadcast before create_pool: raises PoolNotStarted as expected")

# %% [markdown]
# ---
# # PoolError (Server-side Errors)
#
# `PoolError` is raised when the server encounters an error, such as
# requesting an unknown worker.

# %%
#|export
@pytest.mark.asyncio
async def test_unknown_worker_error():
    """create_pool() raises PoolError for unknown worker name."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 29905):
        async with RemotePoolClient("ws://127.0.0.1:29905") as client:
            with pytest.raises(PoolError) as exc_info:
                await client.create_pool(
                    worker_name="nonexistent",
                    num_processes=1,
                )

            assert "unknown worker" in str(exc_info.value).lower()

# %%
await test_unknown_worker_error()
print("Unknown worker: raises PoolError as expected")

# %% [markdown]
# ---
# # RecvTimeout Exception
#
# `RecvTimeout` is raised when `recv()` times out waiting for a message.

# %%
#|export
@pytest.mark.asyncio
async def test_recv_timeout():
    """RemotePoolClient.recv() raises RecvTimeout when timeout expires."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 29906):
        async with RemotePoolClient("ws://127.0.0.1:29906") as client:
            await client.create_pool(worker_name="echo", num_processes=1)

            start = time.time()
            with pytest.raises(RecvTimeout) as exc_info:
                await client.recv(timeout=0.1)
            elapsed = time.time() - start

            assert elapsed >= 0.1
            assert elapsed < 0.5
            assert "timed out" in str(exc_info.value).lower()

# %%
await test_recv_timeout()
print("Recv timeout: raises RecvTimeout after specified duration")

# %%
#|export
@pytest.mark.asyncio
async def test_recv_timeout_preserves_client():
    """After RecvTimeout, the client is still usable."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 29907):
        async with RemotePoolClient("ws://127.0.0.1:29907") as client:
            await client.create_pool(worker_name="echo", num_processes=1)

            # First recv times out
            with pytest.raises(RecvTimeout):
                await client.recv(timeout=0.05)

            # Client should still be running
            assert client.is_running

            # Can still send and receive
            await client.send(0, "hello", "world")
            msg = await client.recv(timeout=5.0)
            assert msg.key == "echo:hello"
            assert msg.data["data"] == "world"

# %%
await test_recv_timeout_preserves_client()
print("Recv timeout: client remains usable after timeout")

# %% [markdown]
# ## try_recv Does NOT Raise RecvTimeout

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_returns_none():
    """RemotePoolClient.try_recv() returns None, never raises RecvTimeout."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 29908):
        async with RemotePoolClient("ws://127.0.0.1:29908") as client:
            await client.create_pool(worker_name="echo", num_processes=1)

            result = await client.try_recv()
            assert result is None

# %%
await test_try_recv_returns_none()
print("Try_recv: returns None (no RecvTimeout)")

# %% [markdown]
# ---
# # WorkerException
#
# `WorkerException` is raised when a worker's code raises an exception.
# The error info is serialized and sent from server to client.

# %%
#|export
@pytest.mark.asyncio
async def test_worker_exception():
    """WorkerException is raised when remote worker raises exception."""
    server = RemotePoolServer()
    server.register_worker("raising", raising_worker)

    async with server.serve_background("127.0.0.1", 29909):
        async with RemotePoolClient("ws://127.0.0.1:29909") as client:
            await client.create_pool(worker_name="raising", num_processes=1)

            await client.send(0, "raise", "test error")

            with pytest.raises(WorkerException) as exc_info:
                await client.recv(timeout=5.0)

            exc = exc_info.value
            assert exc.worker_id == 0
            # Remote exceptions come as dict
            assert "ValueError" in str(exc)

# %%
await test_worker_exception()
print("Worker exception: raises WorkerException with error info")

# %%
#|export
def test_worker_exception_structure():
    """WorkerException has the expected structure."""
    error_dict = {
        "type": "ValueError",
        "message": "Test error",
    }
    exc = WorkerException(0, error_dict)

    assert exc.worker_id == 0
    assert "ValueError" in str(exc)
    assert "Test error" in str(exc)

# %%
test_worker_exception_structure()
print("WorkerException: has expected structure")

# %% [markdown]
# ---
# # WorkerCrashed Exception
#
# `WorkerCrashed` is raised when a worker process dies unexpectedly.

# %%
#|export
def test_worker_crashed_structure():
    """WorkerCrashed has the expected structure."""
    details = {"exit_code": -9, "reason": "Process killed"}
    exc = WorkerCrashed(2, details)

    assert exc.worker_id == 2
    assert exc.details == details
    assert "Worker 2" in str(exc)
    assert "crashed" in str(exc).lower()

# %%
test_worker_crashed_structure()
print("WorkerCrashed: has expected structure")

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
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 29910):
        async with RemotePoolClient("ws://127.0.0.1:29910") as client:
            await client.create_pool(worker_name="echo", num_processes=2)

            with pytest.raises(ValueError) as exc_info:
                await client.send(-1, "hello", "world")

            assert "out of range" in str(exc_info.value)

# %%
await test_send_invalid_worker_id_negative()
print("Invalid worker_id (negative): raises ValueError")

# %%
#|export
@pytest.mark.asyncio
async def test_send_invalid_worker_id_too_large():
    """send() raises ValueError for worker_id >= num_workers."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 29911):
        async with RemotePoolClient("ws://127.0.0.1:29911") as client:
            await client.create_pool(
                worker_name="echo",
                num_processes=2,
                threads_per_process=2,
            )
            # Total workers = 4, valid IDs are 0, 1, 2, 3

            with pytest.raises(ValueError) as exc_info:
                await client.send(4, "hello", "world")

            assert "out of range" in str(exc_info.value)

# %%
await test_send_invalid_worker_id_too_large()
print("Invalid worker_id (too large): raises ValueError")

# %% [markdown]
# ---
# # ConnectionError
#
# `ConnectionError` is raised when failing to connect to the server.

# %%
#|export
@pytest.mark.asyncio
async def test_connection_error_no_server():
    """connect() raises error when server is not available."""
    client = RemotePoolClient("ws://127.0.0.1:29999")  # No server

    with pytest.raises(Exception):  # May be ConnectionError or OSError
        await client.connect()

# %%
await test_connection_error_no_server()
print("Connection to nonexistent server: raises error as expected")

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
# ## Example: Robust Client with Error Handling

# %%
@pytest.mark.asyncio
async def example_robust_client():
    """Example: Client with comprehensive error handling."""
    print("=" * 50)
    print("Example: Robust Client")
    print("=" * 50)

    server = RemotePoolServer()
    server.register_worker("compute", compute_worker)

    async with server.serve_background("127.0.0.1", 29912):
        async with RemotePoolClient("ws://127.0.0.1:29912") as client:
            await client.create_pool(
                worker_name="compute",
                num_processes=2,
                threads_per_process=1,
            )
            print(f"  Connected with {client.num_workers} workers")

            # Send tasks
            await client.send(0, "square", 5)
            await client.send(1, "double", 10)

            # Receive with error handling
            for _ in range(2):
                try:
                    msg = await client.recv(timeout=5.0)
                    print(f"  Worker {msg.worker_id}: {msg.key} = {msg.data}")
                except RecvTimeout:
                    print("  Timeout!")
                except WorkerException as e:
                    print(f"  Worker error: {e}")
                except WorkerCrashed as e:
                    print(f"  Worker crashed: {e}")

    print("Done!")

# %%
await example_robust_client()

# %% [markdown]
# ## Example: Server Registration

# %%
@pytest.mark.asyncio
async def example_server_registration():
    """Example: Multiple workers registered on server."""
    print("=" * 50)
    print("Example: Server Registration")
    print("=" * 50)

    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)
    server.register_worker("compute", compute_worker)

    print(f"  Registered workers: {server.registered_workers}")

    async with server.serve_background("127.0.0.1", 29913):
        # Client 1 uses echo worker
        async with RemotePoolClient("ws://127.0.0.1:29913") as client1:
            await client1.create_pool(worker_name="echo", num_processes=1)
            await client1.send(0, "test", "hello")
            msg = await client1.recv(timeout=5.0)
            print(f"  Echo: {msg.data}")

        # Client 2 uses compute worker
        async with RemotePoolClient("ws://127.0.0.1:29913") as client2:
            await client2.create_pool(worker_name="compute", num_processes=1)
            await client2.send(0, "square", 7)
            msg = await client2.recv(timeout=5.0)
            print(f"  Compute: {msg.data}")

    print("Done!")

# %%
await example_server_registration()
