# ---
# jupyter:
#   kernelspec:
#     display_name: netrun
#     language: python
#     name: netrun
# ---

# %%
#|default_exp rpc.base

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %% [markdown]
# # RPC Base
#
# Core abstractions for the RPC layer. Defines the channel protocols
# for bidirectional `(key, data)` message passing.

# %%
#|export
from typing import Any, Protocol, runtime_checkable

# %% [markdown]
# ## Exceptions

# %%
#|export
class RPCError(Exception):
    """Base exception for RPC errors."""
    pass

# %%
#|export
class ChannelClosed(RPCError):
    """Raised when attempting to use a closed channel."""
    pass

# %%
#|export
class ChannelBroken(RPCError):
    """Raised when the channel is unexpectedly broken (e.g., other end died)."""
    pass

# %%
#|export
class RecvTimeout(RPCError):
    """Raised when recv times out."""
    pass

# %% [markdown]
# ## RPCChannel Protocol (Async)

# %%
#|export
@runtime_checkable
class RPCChannel(Protocol):
    """Async protocol for bidirectional (key, data) message passing.

    Thread-safe - can be shared among multiple coroutines.
    """

    async def send(self, key: str, data: Any) -> None:
        """Send a message."""
        ...

    async def recv(self, timeout: float | None = None) -> tuple[str, Any]:
        """Receive a message. Blocks until available or timeout."""
        ...

    async def try_recv(self) -> tuple[str, Any] | None:
        """Non-blocking receive. Returns None if no message."""
        ...

    async def close(self) -> None:
        """Close the channel."""
        ...

    @property
    def is_closed(self) -> bool:
        """Whether the channel is closed."""
        ...

# %% [markdown]
# ## SyncRPCChannel Protocol
#
# For use in contexts without async (e.g., subprocess workers).

# %%
#|export
@runtime_checkable
class SyncRPCChannel(Protocol):
    """Sync protocol for bidirectional (key, data) message passing.

    Thread-safe - can be shared among multiple threads.
    """

    def send(self, key: str, data: Any) -> None:
        """Send a message."""
        ...

    def recv(self, timeout: float | None = None) -> tuple[str, Any]:
        """Receive a message. Blocks until available or timeout."""
        ...

    def try_recv(self) -> tuple[str, Any] | None:
        """Non-blocking receive. Returns None if no message."""
        ...

    def close(self) -> None:
        """Close the channel."""
        ...

    @property
    def is_closed(self) -> bool:
        """Whether the channel is closed."""
        ...

# %% [markdown]
# ## RPC Layer Keys
#
# Standard keys used by the RPC layer for control messages.
# Format: `"__rpc:name"`

# %%
#|export
RPC_KEY_SHUTDOWN = "__rpc:shutdown"
"""Signal graceful channel shutdown. Either side can send."""

# %%
#|export
RPC_KEY_ERROR = "__rpc:error"
"""Channel-level error (e.g., parse error, deserialization failure)."""

# %%
#|export
RPC_KEY_BROKEN = "__rpc:broken"
"""Connection broken unexpectedly."""

# %%
#|export
RPC_KEYS = [RPC_KEY_SHUTDOWN, RPC_KEY_ERROR, RPC_KEY_BROKEN]
"""All RPC layer keys."""

# %%
#|export
# Backwards compatibility alias
SHUTDOWN_KEY = RPC_KEY_SHUTDOWN
"""Deprecated: Use RPC_KEY_SHUTDOWN instead."""
