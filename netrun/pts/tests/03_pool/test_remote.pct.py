# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for RemotePool (Server and Client)

# %%
#|default_exp pool.test_remote

# %%
#|export
import pytest
import asyncio

# Check if websockets is available
try:
    import websockets
    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False

pytestmark = pytest.mark.skipif(not HAS_WEBSOCKETS, reason="websockets not installed")

# %%
#|export
from netrun.rpc.base import RecvTimeout
from netrun.pool.base import (
    PoolNotStarted,
    PoolError,
)
from netrun.pool.remote import RemotePoolServer, RemotePoolClient

# %% [markdown]
# ## Import Worker Functions
#
# Worker functions are in an importable module for multiprocessing.

# %%
#|export
from .workers import echo_worker, compute_worker

# %% [markdown]
# ## Test Server Creation

# %%
#|export
def test_server_creation():
    """Test creating a RemotePoolServer."""
    server = RemotePoolServer()
    assert server.registered_workers == []

# %%
test_server_creation();

# %%
#|export
def test_register_worker():
    """Test registering workers on server."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)
    server.register_worker("compute", compute_worker)

    assert "echo" in server.registered_workers
    assert "compute" in server.registered_workers
    assert len(server.registered_workers) == 2

# %%
test_register_worker();

# %% [markdown]
# ## Test Client Creation

# %%
#|export
def test_client_creation():
    """Test creating a RemotePoolClient."""
    client = RemotePoolClient("ws://localhost:8080")
    assert client.num_workers == 0
    assert not client.is_running

# %%
test_client_creation();

# %% [markdown]
# ## Test Server-Client Communication

# %%
#|export
@pytest.mark.asyncio
async def test_connect_and_create_pool():
    """Test connecting client and creating a pool."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 19001):
        async with RemotePoolClient("ws://127.0.0.1:19001") as client:
            await client.create_pool("echo", num_processes=1, threads_per_process=1)

            assert client.is_running
            assert client.num_workers == 1
            assert client.num_processes == 1
            assert client.threads_per_process == 1

# %%
await test_connect_and_create_pool();

# %%
#|export
@pytest.mark.asyncio
async def test_send_recv():
    """Test sending and receiving messages."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 19002):
        async with RemotePoolClient("ws://127.0.0.1:19002") as client:
            await client.create_pool("echo", num_processes=1, threads_per_process=1)

            await client.send(worker_id=0, key="test", data="hello")
            msg = await client.recv(timeout=10.0)

            assert msg.worker_id == 0
            assert msg.key == "echo:test"
            assert msg.data == {"worker_id": 0, "data": "hello"}

# %%
await test_send_recv();

# %%
#|export
@pytest.mark.asyncio
async def test_multiple_workers():
    """Test with multiple workers."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 19003):
        async with RemotePoolClient("ws://127.0.0.1:19003") as client:
            await client.create_pool("echo", num_processes=2, threads_per_process=2)

            assert client.num_workers == 4

            # Send to each worker
            for i in range(4):
                await client.send(worker_id=i, key="ping", data=i)

            # Receive all responses
            responses = []
            for _ in range(4):
                msg = await client.recv(timeout=10.0)
                responses.append(msg)

            assert len(responses) == 4
            worker_ids = {msg.worker_id for msg in responses}
            assert worker_ids == {0, 1, 2, 3}

# %%
await test_multiple_workers();

# %% [markdown]
# ## Test try_recv

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_empty():
    """Test try_recv when no messages pending."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 19004):
        async with RemotePoolClient("ws://127.0.0.1:19004") as client:
            await client.create_pool("echo", num_processes=1)

            result = await client.try_recv()
            assert result is None

# %%
await test_try_recv_empty();

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_with_message():
    """Test try_recv with pending message."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 19005):
        async with RemotePoolClient("ws://127.0.0.1:19005") as client:
            await client.create_pool("echo", num_processes=1)

            await client.send(worker_id=0, key="test", data="data")
            await asyncio.sleep(0.5)  # Let message arrive

            result = await client.try_recv()
            assert result is not None
            assert result.key == "echo:test"

# %%
await test_try_recv_with_message();

# %% [markdown]
# ## Test Broadcast

# %%
#|export
@pytest.mark.asyncio
async def test_broadcast():
    """Test broadcasting to all workers."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 19006):
        async with RemotePoolClient("ws://127.0.0.1:19006") as client:
            await client.create_pool("echo", num_processes=2, threads_per_process=1)

            await client.broadcast("config", {"setting": "value"})

            responses = []
            for _ in range(client.num_workers):
                msg = await client.recv(timeout=10.0)
                responses.append(msg)

            assert len(responses) == 2
            worker_ids = {msg.worker_id for msg in responses}
            assert worker_ids == {0, 1}

# %%
await test_broadcast();

# %% [markdown]
# ## Test Timeout

# %%
#|export
@pytest.mark.asyncio
async def test_recv_timeout():
    """Test recv timeout."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 19007):
        async with RemotePoolClient("ws://127.0.0.1:19007") as client:
            await client.create_pool("echo", num_processes=1)

            with pytest.raises(RecvTimeout):
                await client.recv(timeout=0.5)

# %%
await test_recv_timeout();

# %% [markdown]
# ## Test Error Handling

# %%
#|export
@pytest.mark.asyncio
async def test_unknown_worker_raises():
    """Test that requesting unknown worker raises PoolError."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 19008):
        async with RemotePoolClient("ws://127.0.0.1:19008") as client:
            with pytest.raises(PoolError):
                await client.create_pool("nonexistent", num_processes=1)

# %%
await test_unknown_worker_raises();

# %%
#|export
@pytest.mark.asyncio
async def test_send_before_create_raises():
    """Test that sending before create_pool raises PoolNotStarted."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 19009):
        async with RemotePoolClient("ws://127.0.0.1:19009") as client:
            with pytest.raises(PoolNotStarted):
                await client.send(worker_id=0, key="test", data="data")

# %%
await test_send_before_create_raises();

# %%
#|export
@pytest.mark.asyncio
async def test_invalid_worker_id():
    """Test that invalid worker_id raises ValueError."""
    server = RemotePoolServer()
    server.register_worker("echo", echo_worker)

    async with server.serve_background("127.0.0.1", 19010):
        async with RemotePoolClient("ws://127.0.0.1:19010") as client:
            await client.create_pool("echo", num_processes=1, threads_per_process=1)

            with pytest.raises(ValueError):
                await client.send(worker_id=-1, key="test", data="data")

            with pytest.raises(ValueError):
                await client.send(worker_id=1, key="test", data="data")

# %%
await test_invalid_worker_id();

# %% [markdown]
# ## Test Computation

# %%
#|export
@pytest.mark.asyncio
async def test_compute_workers():
    """Test compute workers with actual computation."""
    server = RemotePoolServer()
    server.register_worker("compute", compute_worker)

    async with server.serve_background("127.0.0.1", 19011):
        async with RemotePoolClient("ws://127.0.0.1:19011") as client:
            await client.create_pool("compute", num_processes=2, threads_per_process=1)

            await client.send(worker_id=0, key="square", data=7)
            await client.send(worker_id=1, key="double", data=21)

            results = []
            for _ in range(2):
                msg = await client.recv(timeout=10.0)
                results.append((msg.worker_id, msg.data))

            results.sort()  # Sort by worker_id
            assert results == [(0, 49), (1, 42)]

# %%
await test_compute_workers();
