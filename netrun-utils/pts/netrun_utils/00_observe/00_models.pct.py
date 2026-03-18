# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Observe Models
#
# Pydantic data models for the observe API. These are pure data transfer objects
# with no dependency on the Net runtime.

# %%
#|default_exp observe.models

# %%
#|export
from typing import Any

from pydantic import BaseModel

# %% [markdown]
# ## Status Models

# %%
#|export
class NodeStatus(BaseModel):
    """Status of a single node."""
    name: str
    enabled: bool
    epoch_count: int
    is_busy: bool
    running_epoch_ids: list[str] = []
    startable_epoch_ids: list[str] = []
    in_port_names: list[str] = []
    out_port_names: list[str] = []


class EdgeStatus(BaseModel):
    """Status of a single edge."""
    source_node: str
    source_port: str
    target_node: str
    target_port: str
    packet_count: int = 0


class EpochInfo(BaseModel):
    """Information about an epoch (running, completed, or cancelled)."""
    epoch_id: str
    node_name: str
    state: str  # "startable", "running", "finished", "cancelled"
    created_at: str  # ISO format
    started_at: str | None = None
    ended_at: str | None = None
    duration_ms: float | None = None
    outcome: str | None = None  # "success", "error", "cancelled", "cache_hit", "file_storage_hit"
    error: str | None = None
    error_type: str | None = None
    error_traceback: str | None = None
    pool_id: str | None = None
    worker_id: int | None = None
    was_cache_hit: bool = False
    was_file_storage_hit: bool = False
    retry_count: int = 0
    factory: str | None = None


class LogEntry(BaseModel):
    """A single log entry."""
    timestamp: str  # ISO format
    message: str
    node_name: str | None = None
    epoch_id: str | None = None


class NetStatus(BaseModel):
    """Overall status of the Net."""
    started: bool
    paused: bool
    node_names: list[str] = []
    edge_count: int = 0
    total_epochs: int = 0
    busy_nodes: list[str] = []
    idle_nodes: list[str] = []
    startable_epoch_count: int = 0
    running_epoch_count: int = 0

# %% [markdown]
# ## Control Models

# %%
#|export
class ControlResponse(BaseModel):
    """Response from a control action."""
    ok: bool
    message: str


class SendControlRequest(BaseModel):
    """Request to send a control signal to a node."""
    node_name: str
    control_type: str
    value: Any = None


class InjectDataRequest(BaseModel):
    """Request to inject data into a node's input port."""
    node_name: str
    port_name: str
    values: list[Any]
