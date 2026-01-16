# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for AsyncChannel (RPC Aio Module)

# %%
#|default_exp rpc.test_aio

# %%
#|export
import pytest
import asyncio
from netrun.rpc.base import ChannelClosed, RecvTimeout
from netrun.rpc.aio import AsyncChannel, create_async_channel_pair

# %% [markdown]
# ## Test Channel Creation

# %%
#|export
@pytest.mark.asyncio
async def test_create_channel_pair():
    """Test creating a channel pair."""
    ch1, ch2 = create_async_channel_pair()
    assert isinstance(ch1, AsyncChannel)
    assert isinstance(ch2, AsyncChannel)
    assert not ch1.is_closed
    assert not ch2.is_closed

# %%
await test_create_channel_pair();

# %% [markdown]
# ## Test Basic Send/Recv

# %%
#|export
@pytest.mark.asyncio
async def test_send_recv():
    """Test basic send and receive."""
    ch1, ch2 = create_async_channel_pair()

    await ch1.send("test", {"value": 42})
    key, data = await ch2.recv()

    assert key == "test"
    assert data == {"value": 42}

# %%
await test_send_recv();

# %%
#|export
@pytest.mark.asyncio
async def test_send_recv_multiple():
    """Test multiple send/recv operations."""
    ch1, ch2 = create_async_channel_pair()

    # Send multiple messages
    await ch1.send("msg1", "data1")
    await ch1.send("msg2", "data2")
    await ch1.send("msg3", "data3")

    # Receive in order
    key1, data1 = await ch2.recv()
    key2, data2 = await ch2.recv()
    key3, data3 = await ch2.recv()

    assert (key1, data1) == ("msg1", "data1")
    assert (key2, data2) == ("msg2", "data2")
    assert (key3, data3) == ("msg3", "data3")

# %%
await test_send_recv_multiple();

# %%
#|export
@pytest.mark.asyncio
async def test_bidirectional():
    """Test bidirectional communication."""
    ch1, ch2 = create_async_channel_pair()

    # ch1 -> ch2
    await ch1.send("ping", None)
    key, _ = await ch2.recv()
    assert key == "ping"

    # ch2 -> ch1
    await ch2.send("pong", None)
    key, _ = await ch1.recv()
    assert key == "pong"

# %%
await test_bidirectional();

# %% [markdown]
# ## Test try_recv

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_empty():
    """Test try_recv on empty queue returns None."""
    ch1, ch2 = create_async_channel_pair()
    result = await ch2.try_recv()
    assert result is None

# %%
await test_try_recv_empty();

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_with_message():
    """Test try_recv with pending message."""
    ch1, ch2 = create_async_channel_pair()

    await ch1.send("test", "data")
    result = await ch2.try_recv()

    assert result is not None
    key, data = result
    assert key == "test"
    assert data == "data"

# %%
await test_try_recv_with_message();

# %% [markdown]
# ## Test Timeout

# %%
#|export
@pytest.mark.asyncio
async def test_recv_timeout():
    """Test recv timeout raises RecvTimeout."""
    ch1, ch2 = create_async_channel_pair()

    with pytest.raises(RecvTimeout):
        await ch2.recv(timeout=0.1)

# %%
await test_recv_timeout();

# %% [markdown]
# ## Test Channel Close

# %%
#|export
@pytest.mark.asyncio
async def test_close():
    """Test closing a channel."""
    ch1, ch2 = create_async_channel_pair()

    assert not ch1.is_closed
    await ch1.close()
    assert ch1.is_closed

# %%
await test_close();

# %%
#|export
@pytest.mark.asyncio
async def test_send_on_closed_raises():
    """Test sending on closed channel raises ChannelClosed."""
    ch1, ch2 = create_async_channel_pair()
    await ch1.close()

    with pytest.raises(ChannelClosed):
        await ch1.send("test", "data")

# %%
await test_send_on_closed_raises();

# %%
#|export
@pytest.mark.asyncio
async def test_recv_on_closed_raises():
    """Test receiving on closed channel raises ChannelClosed."""
    ch1, ch2 = create_async_channel_pair()
    await ch1.close()

    with pytest.raises(ChannelClosed):
        await ch1.recv()

# %%
await test_recv_on_closed_raises();

# %%
#|export
@pytest.mark.asyncio
async def test_try_recv_on_closed_raises():
    """Test try_recv on closed channel raises ChannelClosed."""
    ch1, ch2 = create_async_channel_pair()
    await ch1.close()

    with pytest.raises(ChannelClosed):
        await ch1.try_recv()

# %%
await test_try_recv_on_closed_raises();

# %% [markdown]
# ## Test Data Types

# %%
#|export
@pytest.mark.asyncio
async def test_send_various_types():
    """Test sending various data types."""
    ch1, ch2 = create_async_channel_pair()

    test_data = [
        ("str", "hello"),
        ("int", 42),
        ("float", 3.14),
        ("list", [1, 2, 3]),
        ("dict", {"a": 1, "b": 2}),
        ("none", None),
        ("bool", True),
        ("nested", {"list": [1, {"nested": True}]}),
    ]

    for key, data in test_data:
        await ch1.send(key, data)

    for expected_key, expected_data in test_data:
        key, data = await ch2.recv()
        assert key == expected_key
        assert data == expected_data

# %%
await test_send_various_types();
