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
from tests.pool.workers import echo_worker, compute_worker, printing_worker

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

# %% [markdown]
# ## Test Stdout Redirection

# %%
#|export
@pytest.mark.asyncio
async def test_stdout_redirect_default():
    """Test that stdout redirection is enabled by default."""
    server = RemotePoolServer()
    server.register_worker("printing", printing_worker)

    async with server.serve_background("127.0.0.1", 19020):
        async with RemotePoolClient("ws://127.0.0.1:19020") as client:
            await client.create_pool("printing", num_processes=1, threads_per_process=1)

            # Give time for startup prints
            await asyncio.sleep(0.3)

            # Flush stdout
            buffer = await client.flush_stdout(0)

            # Should have captured startup output
            assert len(buffer) > 0
            texts = [text for _, _, text in buffer]
            combined = "".join(texts)
            assert "Worker 0 starting" in combined

# %%
await test_stdout_redirect_default();

# %%
#|export
@pytest.mark.asyncio
async def test_flush_stdout_single_process():
    """Test flush_stdout for a single process."""
    server = RemotePoolServer()
    server.register_worker("printing", printing_worker)

    async with server.serve_background("127.0.0.1", 19021):
        async with RemotePoolClient("ws://127.0.0.1:19021") as client:
            await client.create_pool("printing", num_processes=2, threads_per_process=1)
            await asyncio.sleep(0.3)

            # Flush process 0
            buffer0 = await client.flush_stdout(0)
            assert len(buffer0) > 0

            # Each entry should have (timestamp, is_stdout, text)
            for ts, is_stdout, text in buffer0:
                assert hasattr(ts, 'isoformat')  # Is datetime
                assert isinstance(is_stdout, bool)
                assert isinstance(text, str)

# %%
await test_flush_stdout_single_process();

# %%
#|export
@pytest.mark.asyncio
async def test_flush_stdout_clears_buffer():
    """Test that flush_stdout clears the buffer."""
    server = RemotePoolServer()
    server.register_worker("printing", printing_worker)

    async with server.serve_background("127.0.0.1", 19022):
        async with RemotePoolClient("ws://127.0.0.1:19022") as client:
            await client.create_pool("printing", num_processes=1)
            await asyncio.sleep(0.3)

            # Flush once
            buffer1 = await client.flush_stdout(0)
            assert len(buffer1) > 0

            # Flush again - should be empty
            buffer2 = await client.flush_stdout(0)
            assert len(buffer2) == 0

# %%
await test_flush_stdout_clears_buffer();

# %%
#|export
@pytest.mark.asyncio
async def test_flush_all_stdout():
    """Test flush_all_stdout for all processes."""
    server = RemotePoolServer()
    server.register_worker("printing", printing_worker)

    async with server.serve_background("127.0.0.1", 19023):
        async with RemotePoolClient("ws://127.0.0.1:19023") as client:
            await client.create_pool("printing", num_processes=2, threads_per_process=1)
            await asyncio.sleep(0.3)

            # Flush all
            buffers = await client.flush_all_stdout()

            assert len(buffers) == 2
            assert 0 in buffers
            assert 1 in buffers

            # Each process should have captured something
            for idx, buffer in buffers.items():
                texts = [text for _, _, text in buffer]
                combined = "".join(texts)
                assert f"Worker {idx} starting" in combined

# %%
await test_flush_all_stdout();

# %%
#|export
@pytest.mark.asyncio
async def test_stdout_content():
    """Test that stdout content is correctly captured."""
    server = RemotePoolServer()
    server.register_worker("printing", printing_worker)

    async with server.serve_background("127.0.0.1", 19024):
        async with RemotePoolClient("ws://127.0.0.1:19024") as client:
            await client.create_pool("printing", num_processes=1)
            await asyncio.sleep(0.2)

            # Send a message to trigger more printing
            await client.send(0, "test", "hello")
            await client.recv(timeout=5.0)
            await asyncio.sleep(0.2)

            # Flush and check content
            buffer = await client.flush_stdout(0)
            texts = [text for _, _, text in buffer]
            combined = "".join(texts)

            assert "Worker 0 starting" in combined
            assert "Worker 0 got: test=hello" in combined

# %%
await test_stdout_content();

# %%
#|export
@pytest.mark.asyncio
async def test_stdout_invalid_process_idx():
    """Test that invalid process_idx raises ValueError."""
    server = RemotePoolServer()
    server.register_worker("printing", printing_worker)

    async with server.serve_background("127.0.0.1", 19025):
        async with RemotePoolClient("ws://127.0.0.1:19025") as client:
            await client.create_pool("printing", num_processes=2)

            with pytest.raises(ValueError):
                await client.flush_stdout(-1)

            with pytest.raises(ValueError):
                await client.flush_stdout(2)  # Only 0 and 1 valid

# %%
await test_stdout_invalid_process_idx();

# %%
#|export
@pytest.mark.asyncio
async def test_stdout_before_create():
    """Test that flush_stdout before create_pool raises PoolNotStarted."""
    server = RemotePoolServer()
    server.register_worker("printing", printing_worker)

    async with server.serve_background("127.0.0.1", 19026):
        async with RemotePoolClient("ws://127.0.0.1:19026") as client:
            with pytest.raises(PoolNotStarted):
                await client.flush_stdout(0)

            with pytest.raises(PoolNotStarted):
                await client.flush_all_stdout()

# %%
await test_stdout_before_create();

# %%
#|export
@pytest.mark.asyncio
async def test_stdout_stderr_distinction():
    """Test that stdout and stderr are distinguished."""
    server = RemotePoolServer()
    server.register_worker("printing", printing_worker)

    async with server.serve_background("127.0.0.1", 19027):
        async with RemotePoolClient("ws://127.0.0.1:19027") as client:
            await client.create_pool("printing", num_processes=1)
            await asyncio.sleep(0.3)

            buffer = await client.flush_stdout(0)

            stdout_entries = [(ts, text) for ts, is_stdout, text in buffer if is_stdout]
            stderr_entries = [(ts, text) for ts, is_stdout, text in buffer if not is_stdout]

            # Both stdout and stderr should have content
            stdout_text = "".join(text for _, text in stdout_entries)
            stderr_text = "".join(text for _, text in stderr_entries)

            assert "Worker 0 starting" in stdout_text
            assert "Worker 0 stderr" in stderr_text

# %%
await test_stdout_stderr_distinction();
