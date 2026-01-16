# RPC module - bidirectional (key, data) message passing channels
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
    ThreadChannel,
    create_channel_pair,
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
