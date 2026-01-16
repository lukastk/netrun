# RPC module - bidirectional (key, data) message passing channels
from netrun.rpc.aio import (
    AsyncChannel,
    create_async_channel_pair,
)
from netrun.rpc.base import (
    SHUTDOWN_KEY,
    ChannelBroken,
    ChannelClosed,
    RecvTimeout,
    RPCChannel,
    RPCError,
    SyncRPCChannel,
)
from netrun.rpc.process import (
    ProcessChannel,
    SyncProcessChannel,
    create_queue_pair,
)
from netrun.rpc.remote import (
    ConnectionHandler,
    WebSocketChannel,
    connect,
    connect_channel,
    serve,
    serve_background,
)
from netrun.rpc.thread import (
    SyncThreadChannel,
    ThreadChannel,
    create_thread_channel_pair,
)

__all__ = [
    # Base
    "RPCError",
    "ChannelClosed",
    "ChannelBroken",
    "RecvTimeout",
    "RPCChannel",
    "SyncRPCChannel",
    "SHUTDOWN_KEY",
    # Async (coroutine-to-coroutine, same event loop)
    "AsyncChannel",
    "create_async_channel_pair",
    # Thread (OS threads, same process)
    "SyncThreadChannel",
    "ThreadChannel",
    "create_thread_channel_pair",
    # Process (subprocesses)
    "SyncProcessChannel",
    "ProcessChannel",
    "create_queue_pair",
    # Remote (network)
    "WebSocketChannel",
    "ConnectionHandler",
    "connect",
    "connect_channel",
    "serve",
    "serve_background",
]
