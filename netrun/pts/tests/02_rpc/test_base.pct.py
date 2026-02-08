# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for RPC Base Module

# %%
#|default_exp rpc.test_base

# %%
#|export
import pytest
from netrun.rpc.base import (
    ChannelClosed,
    RecvTimeout,
    RPC_KEY_SHUTDOWN,
)

# %% [markdown]
# ## Test Exceptions

# %%
#|export
def test_channel_closed_exception():
    """Test ChannelClosed exception."""
    with pytest.raises(ChannelClosed):
        raise ChannelClosed("Test channel closed")

# %%
test_channel_closed_exception();

# %%
#|export
def test_recv_timeout_exception():
    """Test RecvTimeout exception."""
    with pytest.raises(RecvTimeout):
        raise RecvTimeout("Test timeout")

# %%
test_recv_timeout_exception();

# %%
#|export
def test_channel_closed_is_exception():
    """ChannelClosed should be an Exception subclass."""
    assert issubclass(ChannelClosed, Exception)

# %%
test_channel_closed_is_exception();

# %%
#|export
def test_recv_timeout_is_exception():
    """RecvTimeout should be an Exception subclass."""
    assert issubclass(RecvTimeout, Exception)

# %%
test_recv_timeout_is_exception();

# %% [markdown]
# ## Test Constants

# %%
#|export
def test_shutdown_key_is_string():
    """RPC_KEY_SHUTDOWN should be a string."""
    assert isinstance(RPC_KEY_SHUTDOWN, str)

# %%
test_shutdown_key_is_string();

# %%
#|export
def test_shutdown_key_follows_convention():
    """RPC_KEY_SHUTDOWN should follow __layer:name convention to avoid collisions."""
    assert RPC_KEY_SHUTDOWN.startswith("__")
    assert ":" in RPC_KEY_SHUTDOWN

# %%
test_shutdown_key_follows_convention();
