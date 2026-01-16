# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Pool Test Workers
#
# Worker functions for pool tests. These must be in an importable
# module for multiprocessing with spawn context.

# %%
#|default_exp pool.workers

# %%
#|export
from netrun.rpc.base import ChannelClosed

# %%
#|export
def echo_worker(channel, worker_id):
    """Echo worker for pool testing."""
    try:
        while True:
            key, data = channel.recv()
            channel.send(f"echo:{key}", {"worker_id": worker_id, "data": data})
    except ChannelClosed:
        pass

# %%
#|export
def compute_worker(channel, worker_id):
    """Compute worker for pool testing."""
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
def pid_worker(channel, worker_id):
    """Worker that reports its process ID."""
    import os
    try:
        while True:
            key, data = channel.recv()
            channel.send("pid", {"worker_id": worker_id, "pid": os.getpid()})
    except ChannelClosed:
        pass
