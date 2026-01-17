# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # RPC Test Workers
#
# Worker functions for RPC cross-process tests. These must be in an importable
# module for multiprocessing with spawn context.

# %%
#|default_exp rpc.workers

# %%
#|export
from netrun.rpc.base import ChannelClosed
from netrun.rpc.multiprocess import SyncProcessChannel

# %%
#|export
def echo_worker(send_q, recv_q):
    """Echo worker that runs in subprocess."""
    channel = SyncProcessChannel(send_q, recv_q)
    try:
        while True:
            key, data = channel.recv()
            channel.send(f"echo:{key}", data)
    except ChannelClosed:
        pass

# %%
#|export
def compute_worker(send_q, recv_q):
    """Compute worker that runs in subprocess."""
    channel = SyncProcessChannel(send_q, recv_q)
    try:
        while True:
            key, data = channel.recv()
            if key == "square":
                channel.send("result", data * data)
            elif key == "double":
                channel.send("result", data * 2)
    except ChannelClosed:
        pass

# %%
#|export
def robust_worker(send_q, recv_q):
    """used in test_exceptions_multiprocess"""
    channel = SyncProcessChannel(send_q, recv_q)
    print("  [Worker] Started")
    try:
        while True:
            key, data = channel.recv()
            print(f"  [Worker] Processing: {key}={data}")
            channel.send("result", data * 2)
    except ChannelClosed:
        print("  [Worker] Graceful shutdown")
    except ChannelBroken as e:
        print(f"  [Worker] Channel broken: {e}")

# %%
#|export
def slow_worker(send_q, recv_q):
    """used in test_exceptions_multiprocess"""
    import time
    channel = SyncProcessChannel(send_q, recv_q)
    try:
        while True:
            key, data = channel.recv()
            time.sleep(1)  # Simulate slow processing
            channel.send("result", data)
    except ChannelClosed:
        pass
