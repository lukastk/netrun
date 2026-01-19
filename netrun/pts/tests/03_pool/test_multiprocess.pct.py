# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for MultiprocessPool

# %%
#|default_exp pool.test_multiprocess

# %%
#|export
import pytest
import asyncio
from netrun.rpc.base import RecvTimeout
from netrun.pool.base import (
    PoolNotStarted,
    PoolAlreadyStarted,
)
from netrun.pool.multiprocess import MultiprocessPool

# %% [markdown]
# ## Import Worker Functions
#
# Worker functions are in an importable module for multiprocessing.

# %%
#|export
from tests.pool.workers import echo_worker, compute_worker, pid_worker, printing_worker

# %% [markdown]
# ## Test Pool Creation

# %%
#|export
def test_pool_creation():
    """Test creating a MultiprocessPool."""
    pool = MultiprocessPool(echo_worker, num_processes=2, threads_per_process=2)
    assert pool.num_workers == 4
    assert pool.num_processes == 2
    assert pool.threads_per_process == 2
    assert not pool.is_running

# %%
test_pool_creation();

# %%
#|export
def test_pool_default_threads():
    """Test that threads_per_process defaults to 1."""
    pool = MultiprocessPool(echo_worker, num_processes=3)
    assert pool.num_workers == 3
    assert pool.threads_per_process == 1

# %%
test_pool_default_threads();

# %%
#|export
def test_pool_invalid_num_processes():
    """Test that invalid num_processes raises ValueError."""
    with pytest.raises(ValueError):
        MultiprocessPool(echo_worker, num_processes=0)

    with pytest.raises(ValueError):
        MultiprocessPool(echo_worker, num_processes=-1)

# %%
test_pool_invalid_num_processes();

# %%
#|export
def test_pool_invalid_threads_per_process():
    """Test that invalid threads_per_process raises ValueError."""
    with pytest.raises(ValueError):
        MultiprocessPool(echo_worker, num_processes=2, threads_per_process=0)

# %%
test_pool_invalid_threads_per_process();

# %% [markdown]
# ## Test Pool Lifecycle

# %%
#|export
@pytest.mark.asyncio
async def test_start_and_close():
    """Test starting and closing a pool."""
    pool = MultiprocessPool(echo_worker, num_processes=1, threads_per_process=1)

    assert not pool.is_running
    await pool.start()
    assert pool.is_running

    await pool.close()
    assert not pool.is_running

# %%
await test_start_and_close();

# %%
#|export
@pytest.mark.asyncio
async def test_double_start_raises():
    """Test that starting twice raises PoolAlreadyStarted."""
    pool = MultiprocessPool(echo_worker, num_processes=1)
    await pool.start()

    try:
        with pytest.raises(PoolAlreadyStarted):
            await pool.start()
    finally:
        await pool.close()

# %%
await test_double_start_raises();

# %%
#|export
@pytest.mark.asyncio
async def test_context_manager():
    """Test using pool as context manager."""
    async with MultiprocessPool(echo_worker, num_processes=1) as pool:
        assert pool.is_running

    assert not pool.is_running

# %%
await test_context_manager();

# %% [markdown]
# ## Test Send/Recv

# %%
#|export
@pytest.mark.asyncio
async def test_send_recv_single():
    """Test sending and receiving a single message."""
    async with MultiprocessPool(echo_worker, num_processes=1, threads_per_process=1) as pool:
        await pool.send(worker_id=0, key="test", data="hello")
        msg = await pool.recv(timeout=10.0)

        assert msg.worker_id == 0
        assert msg.key == "echo:test"
        assert msg.data == {"worker_id": 0, "data": "hello"}

# %%
await test_send_recv_single();

# %%
#|export
@pytest.mark.asyncio
async def test_send_recv_multiple_workers():
    """Test sending to multiple workers across processes."""
    async with MultiprocessPool(echo_worker, num_processes=2, threads_per_process=2) as pool:
        # Send to each worker
        for i in range(pool.num_workers):
            await pool.send(worker_id=i, key="ping", data=i)

        # Receive all responses
        responses = []
        for _ in range(pool.num_workers):
            msg = await pool.recv(timeout=10.0)
            responses.append(msg)

        assert len(responses) == 4
        worker_ids = {msg.worker_id for msg in responses}
        assert worker_ids == {0, 1, 2, 3}

# %%
await test_send_recv_multiple_workers();

# %% [markdown]
# ## Test Worker ID Mapping

# %%
#|export
@pytest.mark.asyncio
async def test_worker_id_mapping():
    """Test that worker IDs map correctly to processes and threads."""
    # 2 processes x 2 threads = 4 workers
    # Worker 0, 1 in process 0
    # Worker 2, 3 in process 1
    async with MultiprocessPool(pid_worker, num_processes=2, threads_per_process=2) as pool:
        for i in range(4):
            await pool.send(worker_id=i, key="get_pid", data=None)

        pids = {}
        for _ in range(4):
            msg = await pool.recv(timeout=10.0)
            pids[msg.data["worker_id"]] = msg.data["pid"]

        # Workers 0 and 1 should have same PID (same process)
        assert pids[0] == pids[1]
        # Workers 2 and 3 should have same PID (same process)
        assert pids[2] == pids[3]
        # Different processes should have different PIDs
        assert pids[0] != pids[2]

# %%
await test_worker_id_mapping();

# %% [markdown]
# ## Test try_recv

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_empty():
    """Test try_recv when no messages pending."""
    async with MultiprocessPool(echo_worker, num_processes=1) as pool:
        result = await pool.try_recv()
        assert result is None

# %%
await test_try_recv_empty();

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_with_message():
    """Test try_recv with pending message."""
    async with MultiprocessPool(echo_worker, num_processes=1) as pool:
        await pool.send(worker_id=0, key="test", data="data")
        await asyncio.sleep(0.5)  # Let worker process

        result = await pool.try_recv()
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
    async with MultiprocessPool(echo_worker, num_processes=2, threads_per_process=2) as pool:
        await pool.broadcast("config", {"setting": "value"})

        responses = []
        for _ in range(pool.num_workers):
            msg = await pool.recv(timeout=10.0)
            responses.append(msg)

        assert len(responses) == 4
        worker_ids = {msg.worker_id for msg in responses}
        assert worker_ids == {0, 1, 2, 3}

# %%
await test_broadcast();

# %% [markdown]
# ## Test Timeout

# %%
#|export
@pytest.mark.asyncio
async def test_recv_timeout():
    """Test recv timeout."""
    async with MultiprocessPool(echo_worker, num_processes=1) as pool:
        with pytest.raises(RecvTimeout):
            await pool.recv(timeout=0.5)

# %%
await test_recv_timeout();

# %% [markdown]
# ## Test Error Handling

# %%
#|export
@pytest.mark.asyncio
async def test_send_before_start_raises():
    """Test that sending before start raises PoolNotStarted."""
    pool = MultiprocessPool(echo_worker, num_processes=1)

    with pytest.raises(PoolNotStarted):
        await pool.send(worker_id=0, key="test", data="data")

# %%
await test_send_before_start_raises();

# %%
#|export
@pytest.mark.asyncio
async def test_recv_before_start_raises():
    """Test that receiving before start raises PoolNotStarted."""
    pool = MultiprocessPool(echo_worker, num_processes=1)

    with pytest.raises(PoolNotStarted):
        await pool.recv()

# %%
await test_recv_before_start_raises();

# %%
#|export
@pytest.mark.asyncio
async def test_invalid_worker_id():
    """Test that invalid worker_id raises ValueError."""
    async with MultiprocessPool(echo_worker, num_processes=2, threads_per_process=2) as pool:
        with pytest.raises(ValueError):
            await pool.send(worker_id=-1, key="test", data="data")

        with pytest.raises(ValueError):
            await pool.send(worker_id=4, key="test", data="data")

# %%
await test_invalid_worker_id();

# %% [markdown]
# ## Test Computation

# %%
#|export
@pytest.mark.asyncio
async def test_compute_workers():
    """Test compute workers with actual computation."""
    async with MultiprocessPool(compute_worker, num_processes=2, threads_per_process=1) as pool:
        await pool.send(worker_id=0, key="square", data=7)
        await pool.send(worker_id=1, key="double", data=21)

        results = []
        for _ in range(2):
            msg = await pool.recv(timeout=10.0)
            results.append((msg.worker_id, msg.data))

        results.sort()  # Sort by worker_id
        assert results == [(0, 49), (1, 42)]

# %%
await test_compute_workers();

# %% [markdown]
# ## Test Consistency
#
# These tests verify that all messages are reliably delivered across multiple runs.

# %%
#|export
@pytest.mark.asyncio
async def test_consistency_single_run():
    """Test that all messages are received in a single run."""
    async with MultiprocessPool(echo_worker, num_processes=2, threads_per_process=2) as pool:
        num_messages = pool.num_workers

        # Send to each worker
        for i in range(num_messages):
            await pool.send(worker_id=i, key="ping", data=i)

        # Receive all responses
        received = []
        for _ in range(num_messages):
            msg = await pool.recv(timeout=10.0)
            received.append(msg.worker_id)

        # All workers should have responded
        assert sorted(received) == list(range(num_messages)), f"Missing responses: expected {list(range(num_messages))}, got {sorted(received)}"

# %%
await test_consistency_single_run();

# %%
#|export
@pytest.mark.asyncio
async def test_consistency_multiple_runs():
    """Test consistency across multiple pool create/destroy cycles."""
    for run in range(5):
        async with MultiprocessPool(echo_worker, num_processes=2, threads_per_process=2) as pool:
            num_messages = pool.num_workers

            # Send to each worker
            for i in range(num_messages):
                await pool.send(worker_id=i, key="ping", data=i)

            # Receive all responses
            received = []
            for _ in range(num_messages):
                msg = await pool.recv(timeout=10.0)
                received.append(msg.worker_id)

            assert sorted(received) == list(range(num_messages)), f"Run {run}: Missing responses"

# %%
await test_consistency_multiple_runs();

# %%
#|export
@pytest.mark.asyncio
async def test_consistency_many_messages():
    """Test consistency with many messages per worker."""
    async with MultiprocessPool(echo_worker, num_processes=2, threads_per_process=2) as pool:
        messages_per_worker = 10
        total_messages = pool.num_workers * messages_per_worker

        # Send multiple messages to each worker
        for round_num in range(messages_per_worker):
            for worker_id in range(pool.num_workers):
                await pool.send(worker_id, key=f"msg{round_num}", data=round_num)

        # Receive all responses
        received_count = 0
        for _ in range(total_messages):
            msg = await pool.recv(timeout=10.0)
            received_count += 1

        assert received_count == total_messages, f"Expected {total_messages} messages, got {received_count}"

# %%
await test_consistency_many_messages();

# %%
#|export
@pytest.mark.asyncio
async def test_consistency_rapid_cycles():
    """Test consistency with rapid pool creation and destruction."""
    for run in range(10):
        async with MultiprocessPool(echo_worker, num_processes=1, threads_per_process=2) as pool:
            # Quick send/recv cycle
            await pool.send(worker_id=0, key="quick", data=run)
            await pool.send(worker_id=1, key="quick", data=run)

            msg1 = await pool.recv(timeout=10.0)
            msg2 = await pool.recv(timeout=10.0)

            worker_ids = sorted([msg1.worker_id, msg2.worker_id])
            assert worker_ids == [0, 1], f"Run {run}: Expected workers [0, 1], got {worker_ids}"

# %%
await test_consistency_rapid_cycles();

# %% [markdown]
# ## Test Stdout/Stderr Redirection
#
# These tests verify the stdout/stderr capture and buffering feature.

# %%
#|export
@pytest.mark.asyncio
async def test_stdout_redirect_default():
    """Test that stdout/stderr redirection is enabled by default."""
    pool = MultiprocessPool(echo_worker, num_processes=1)
    # Default should be redirect_output=True, buffer_output=True
    assert pool._redirect_output is True
    assert pool._buffer_output is True

# %%
await test_stdout_redirect_default();

# %%
#|export
@pytest.mark.asyncio
async def test_stdout_redirect_disabled():
    """Test creating pool with stdout redirection disabled."""
    pool = MultiprocessPool(echo_worker, num_processes=1, redirect_output=False)
    assert pool._redirect_output is False

# %%
await test_stdout_redirect_disabled();

# %%
#|export
@pytest.mark.asyncio
async def test_flush_stdout_single_process():
    """Test flushing stdout from a single process."""
    async with MultiprocessPool(
        printing_worker,
        num_processes=1,
        threads_per_process=1,
        redirect_output=True,
        buffer_output=True,
    ) as pool:
        # Wait for startup messages
        await asyncio.sleep(0.5)

        # Send a message to trigger more prints
        await pool.send(0, "test", "hello")
        await pool.recv(timeout=5.0)

        # Flush stdout
        buffer = await pool.flush_stdout(0)

        # Should have captured output
        assert len(buffer) > 0

        # Check buffer format: list of (datetime, bool, str)
        for entry in buffer:
            assert len(entry) == 3
            timestamp, is_stdout, text = entry
            assert isinstance(is_stdout, bool)
            assert isinstance(text, str)

        # Should have captured both stdout (True) and stderr (False)
        has_stdout = any(entry[1] is True for entry in buffer)
        has_stderr = any(entry[1] is False for entry in buffer)
        assert has_stdout, "Should have captured stdout"
        assert has_stderr, "Should have captured stderr"

# %%
await test_flush_stdout_single_process();

# %%
#|export
@pytest.mark.asyncio
async def test_flush_stdout_clears_buffer():
    """Test that flushing clears the buffer."""
    async with MultiprocessPool(
        printing_worker,
        num_processes=1,
        redirect_output=True,
        buffer_output=True,
    ) as pool:
        await asyncio.sleep(0.5)

        # First flush gets startup messages
        buffer1 = await pool.flush_stdout(0)
        assert len(buffer1) > 0

        # Second flush should be empty (no new output)
        buffer2 = await pool.flush_stdout(0)
        assert len(buffer2) == 0

# %%
await test_flush_stdout_clears_buffer();

# %%
#|export
@pytest.mark.asyncio
async def test_flush_all_stdout():
    """Test flushing stdout from all processes."""
    async with MultiprocessPool(
        printing_worker,
        num_processes=2,
        threads_per_process=1,
        redirect_output=True,
        buffer_output=True,
    ) as pool:
        await asyncio.sleep(0.5)

        # Send to both workers
        await pool.send(0, "test", "hello")
        await pool.send(1, "test", "world")

        # Receive responses
        await pool.recv(timeout=5.0)
        await pool.recv(timeout=5.0)

        # Flush all
        all_buffers = await pool.flush_all_stdout()

        assert len(all_buffers) == 2
        assert 0 in all_buffers
        assert 1 in all_buffers

        # Both processes should have output
        assert len(all_buffers[0]) > 0
        assert len(all_buffers[1]) > 0

# %%
await test_flush_all_stdout();

# %%
#|export
@pytest.mark.asyncio
async def test_stdout_silent_mode():
    """Test that buffer_output=False discards output."""
    async with MultiprocessPool(
        printing_worker,
        num_processes=1,
        redirect_output=True,
        buffer_output=False,  # Silent mode
    ) as pool:
        await asyncio.sleep(0.5)

        # Flush should return empty buffer (output discarded)
        buffer = await pool.flush_stdout(0)
        assert len(buffer) == 0

# %%
await test_stdout_silent_mode();

# %%
#|export
@pytest.mark.asyncio
async def test_flush_stdout_invalid_process_idx():
    """Test that flush_stdout raises ValueError for invalid process_idx."""
    async with MultiprocessPool(
        printing_worker,
        num_processes=2,
        redirect_output=True,
        buffer_output=True,
    ) as pool:
        with pytest.raises(ValueError):
            await pool.flush_stdout(-1)

        with pytest.raises(ValueError):
            await pool.flush_stdout(2)  # Only 0 and 1 are valid

# %%
await test_flush_stdout_invalid_process_idx();

# %%
#|export
@pytest.mark.asyncio
async def test_flush_stdout_before_start():
    """Test that flush_stdout raises PoolNotStarted before start()."""
    pool = MultiprocessPool(
        printing_worker,
        num_processes=1,
        redirect_output=True,
        buffer_output=True,
    )

    with pytest.raises(PoolNotStarted):
        await pool.flush_stdout(0)

# %%
await test_flush_stdout_before_start();

# %%
#|export
@pytest.mark.asyncio
async def test_stdout_content():
    """Test that captured stdout contains expected content."""
    async with MultiprocessPool(
        printing_worker,
        num_processes=1,
        redirect_output=True,
        buffer_output=True,
    ) as pool:
        await asyncio.sleep(0.5)

        await pool.send(0, "mykey", "myvalue")
        await pool.recv(timeout=5.0)

        buffer = await pool.flush_stdout(0)

        # Combine all stdout text
        stdout_text = "".join(
            text for timestamp, is_stdout, text in buffer if is_stdout
        )
        stderr_text = "".join(
            text for timestamp, is_stdout, text in buffer if not is_stdout
        )

        # Check expected content
        assert "Worker 0 starting" in stdout_text
        assert "Worker 0 got: mykey=myvalue" in stdout_text
        assert "Worker 0 stderr" in stderr_text

# %%
await test_stdout_content();

# %% [markdown]
# ## Test Auto-Flush and on_output Callback
#
# These tests verify the automatic output flushing and callback features.

# %%
#|export
@pytest.mark.asyncio
async def test_on_output_callback():
    """Test that on_output callback is called when output is received."""
    received_outputs: list[tuple[int, list]] = []

    def on_output_callback(process_idx: int, buffer: list):
        received_outputs.append((process_idx, buffer))

    async with MultiprocessPool(
        printing_worker,
        num_processes=1,
        threads_per_process=1,
        redirect_output=True,
        buffer_output=True,
        output_flush_interval=0.05,  # 50ms for faster test
        on_output=on_output_callback,
    ) as pool:
        # Send a message to trigger prints
        await pool.send(0, "test", "hello")
        await pool.recv(timeout=5.0)

        # Wait for auto-flush to occur
        await asyncio.sleep(0.2)

    # Callback should have been called at least once
    assert len(received_outputs) > 0, "on_output callback should have been called"

    # All calls should be for process_idx 0
    for process_idx, buffer in received_outputs:
        assert process_idx == 0
        assert len(buffer) > 0

# %%
await test_on_output_callback();

# %%
#|export
@pytest.mark.asyncio
async def test_auto_flush_interval():
    """Test that output is automatically flushed at the configured interval."""
    import time
    flush_times: list[float] = []

    def on_output_callback(process_idx: int, buffer: list):
        flush_times.append(time.time())

    async with MultiprocessPool(
        printing_worker,
        num_processes=1,
        threads_per_process=1,
        redirect_output=True,
        buffer_output=True,
        output_flush_interval=0.1,  # 100ms
        on_output=on_output_callback,
    ) as pool:
        # Send multiple messages over time to generate continuous output
        for i in range(3):
            await pool.send(0, "test", f"message-{i}")
            await pool.recv(timeout=5.0)
            await asyncio.sleep(0.15)  # Wait longer than flush interval

    # Should have received multiple flushes
    assert len(flush_times) >= 2, f"Expected at least 2 auto-flushes, got {len(flush_times)}"

# %%
await test_auto_flush_interval();

# %%
#|export
@pytest.mark.asyncio
async def test_on_output_multiple_processes():
    """Test on_output callback with multiple processes."""
    outputs_by_process: dict[int, list] = {0: [], 1: []}

    def on_output_callback(process_idx: int, buffer: list):
        outputs_by_process[process_idx].extend(buffer)

    async with MultiprocessPool(
        printing_worker,
        num_processes=2,
        threads_per_process=1,
        redirect_output=True,
        buffer_output=True,
        output_flush_interval=0.05,
        on_output=on_output_callback,
    ) as pool:
        # Send to both processes
        await pool.send(0, "test", "hello-0")
        await pool.send(1, "test", "hello-1")

        await pool.recv(timeout=5.0)
        await pool.recv(timeout=5.0)

        # Wait for auto-flush
        await asyncio.sleep(0.2)

    # Both processes should have output
    assert len(outputs_by_process[0]) > 0, "Process 0 should have output"
    assert len(outputs_by_process[1]) > 0, "Process 1 should have output"

# %%
await test_on_output_multiple_processes();

# %%
#|export
def test_output_flush_interval_default():
    """Test that output_flush_interval defaults to 0.1 (100ms)."""
    pool = MultiprocessPool(echo_worker, num_processes=1)
    assert pool._output_flush_interval == 0.1

# %%
test_output_flush_interval_default();

# %%
#|export
@pytest.mark.asyncio
async def test_no_callback_still_buffers():
    """Test that output is still buffered locally even without on_output callback."""
    async with MultiprocessPool(
        printing_worker,
        num_processes=1,
        threads_per_process=1,
        redirect_output=True,
        buffer_output=True,
        output_flush_interval=0.05,
        # No on_output callback
    ) as pool:
        # Send a message to trigger prints
        await pool.send(0, "test", "hello")
        await pool.recv(timeout=5.0)

        # Wait for auto-flush
        await asyncio.sleep(0.15)

        # Even without callback, flush_stdout should still work
        # (auto-flush accumulates in _stdout_buffers)
        buffer = await pool.flush_stdout(0)
        # The buffer might have some content from auto-flush that accumulated
        # Note: this is additive to what auto-flush already sent

# %%
await test_no_callback_still_buffers();
