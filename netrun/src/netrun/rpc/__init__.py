# RPC module - bidirectional (key, data) message passing channels
from netrun.rpc.base import (
    RPCError,
    ChannelClosed,
    ChannelBroken,
    RecvTimeout,
    RPCChannel,
    SyncRPCChannel,
    SHUTDOWN_KEY,
)

from netrun.rpc.thread import (
    ThreadChannel,
    create_channel_pair,
)

from netrun.rpc.process import (
    SyncProcessChannel,
    ProcessChannel,
    create_queue_pair,
)

from netrun.rpc.remote import (
    WebSocketChannel,
    ConnectionHandler,
    connect,
    connect_channel,
    serve,
    serve_background,
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
    # Thread
    "ThreadChannel",
    "create_channel_pair",
    # Process
    "SyncProcessChannel",
    "ProcessChannel",
    "create_queue_pair",
    # Remote
    "WebSocketChannel",
    "ConnectionHandler",
    "connect",
    "connect_channel",
    "serve",
    "serve_background",
]
