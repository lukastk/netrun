# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # NetObserver
#
# Reads live status from a `Net` instance and returns Pydantic models.
# All methods are synchronous (reading state snapshots).

# %%
#|default_exp observe.observer

# %%
#|export
import warnings
from typing import Any

from netrun.net._net._net import Net
from netrun.net._net._context import EpochLog

from netrun_utils.observe.models import (
    NetStatus,
    NodeStatus,
    EdgeStatus,
    EpochInfo,
    LogEntry,
    ControlResponse,
)

# %%
#|export
def _epoch_state_to_str(state) -> str:
    """Convert a netrun_sim EpochState enum to a string."""
    s = str(state).lower()
    if "startable" in s:
        return "startable"
    if "running" in s:
        return "running"
    if "finished" in s:
        return "finished"
    if "cancelled" in s:
        return "cancelled"
    return s


def _epoch_log_to_info(log: EpochLog) -> EpochInfo:
    """Convert an EpochLog dataclass to an EpochInfo model."""
    state = "finished"
    if log.outcome == "cancelled":
        state = "cancelled"
    return EpochInfo(
        epoch_id=log.epoch_id,
        node_name=log.node_name,
        state=state,
        created_at=log.created_at.isoformat(),
        started_at=log.started_at.isoformat() if log.started_at else None,
        ended_at=log.ended_at.isoformat() if log.ended_at else None,
        duration_ms=log.duration_ms,
        outcome=log.outcome,
        error=log.error,
        error_type=log.error_type,
        error_traceback=log.error_traceback,
        pool_id=log.pool_id,
        worker_id=log.worker_id,
        was_cache_hit=log.was_cache_hit,
        was_file_storage_hit=log.was_file_storage_hit,
        retry_count=log.retry_count,
        factory=log.factory,
    )


def _epoch_state_to_info(epoch_state) -> EpochInfo:
    """Convert an _EpochState to an EpochInfo model."""
    return EpochInfo(
        epoch_id=epoch_state.id,
        node_name=epoch_state.node_name,
        state=_epoch_state_to_str(epoch_state.state),
        created_at=epoch_state.created_at.isoformat(),
        started_at=epoch_state.started_at.isoformat() if epoch_state.started_at else None,
        ended_at=epoch_state.ended_at.isoformat() if epoch_state.ended_at else None,
        duration_ms=None,
        outcome=None,
        pool_id=epoch_state.pool_id,
        worker_id=epoch_state.worker_id,
        was_cache_hit=epoch_state.was_cache_hit,
        was_file_storage_hit=epoch_state.was_file_storage_hit,
    )

# %% [markdown]
# ## NetObserver

# %%
#|export
class NetObserver:
    """Reads live status from a Net instance.

    Provides query methods for inspecting node status, edges, epochs, and logs,
    plus control methods for enabling/disabling nodes and injecting data.
    """

    def __init__(self, net: Net):
        self._net = net
        if not net.config_resolved.retain_epoch_logs:
            warnings.warn(
                "Net config has retain_epoch_logs=False. Epoch log data will not be "
                "available through the observe API. Set retain_epoch_logs=True in your "
                "NetConfig for full observability.",
                UserWarning,
                stacklevel=2,
            )

    # --- Query methods ---

    def get_status(self) -> NetStatus:
        """Get overall net status."""
        nodes = self._net.nodes
        node_names = list(nodes.keys())
        busy = [name for name, info in nodes.items() if info.is_busy]
        idle = [name for name in node_names if name not in busy]

        return NetStatus(
            started=self._net.started,
            paused=self._net.paused,
            node_names=node_names,
            edge_count=len(self._net.edges),
            total_epochs=len(self._net.epochs),
            busy_nodes=busy,
            idle_nodes=idle,
            startable_epoch_count=len(self._net.get_startable_epochs()),
            running_epoch_count=len(self._net.get_running_epochs()),
        )

    def get_nodes(self) -> list[NodeStatus]:
        """Get status of all nodes."""
        return [self._node_to_status(info) for info in self._net.nodes.values()]

    def get_node(self, name: str) -> NodeStatus:
        """Get status of a single node."""
        return self._node_to_status(self._net.nodes[name])

    def get_edges(self) -> list[EdgeStatus]:
        """Get status of all edges."""
        return [
            EdgeStatus(
                source_node=edge.source_node,
                source_port=edge.source_port,
                target_node=edge.target_node,
                target_port=edge.target_port,
                packet_count=edge.packet_count,
            )
            for edge in self._net.edges
        ]

    def get_epoch_logs(self) -> list[EpochInfo]:
        """Get all epoch information.

        Combines retained EpochLog objects (for completed epochs) with
        live _EpochState objects (for running/startable epochs).
        """
        result = []

        # Completed epochs from retained logs
        for log in self._net.epoch_logs.values():
            result.append(_epoch_log_to_info(log))

        # Running and startable epochs from live state
        completed_ids = {info.epoch_id for info in result}
        for epoch_state in self._net.epochs.values():
            if epoch_state.id not in completed_ids:
                result.append(_epoch_state_to_info(epoch_state))

        return result

    def get_all_logs(self) -> list[LogEntry]:
        """Get all logs in chronological order."""
        return [
            LogEntry(
                timestamp=ts.isoformat(),
                message=message,
                node_name=node_name,
                epoch_id=str(epoch_id),
            )
            for ts, epoch_id, node_name, message in self._net.get_all_logs_chronological()
        ]

    def get_node_logs(self, node_name: str) -> list[LogEntry]:
        """Get logs for a specific node."""
        return [
            LogEntry(
                timestamp=ts.isoformat(),
                message=message,
                node_name=node_name,
            )
            for ts, message in self._net.get_node_logs(node_name)
        ]

    # --- Control methods ---

    def enable_node(self, name: str) -> ControlResponse:
        """Enable a node."""
        try:
            self._net.enable_node(name)
            return ControlResponse(ok=True, message=f"Node '{name}' enabled")
        except Exception as e:
            return ControlResponse(ok=False, message=str(e))

    def disable_node(self, name: str) -> ControlResponse:
        """Disable a node."""
        try:
            self._net.disable_node(name)
            return ControlResponse(ok=True, message=f"Node '{name}' disabled")
        except Exception as e:
            return ControlResponse(ok=False, message=str(e))

    def send_control(self, node_name: str, control_type: str, value: Any = None) -> ControlResponse:
        """Send a control signal to a node."""
        try:
            self._net.send_control(node_name, control_type, value)
            return ControlResponse(ok=True, message=f"Control '{control_type}' sent to '{node_name}'")
        except Exception as e:
            return ControlResponse(ok=False, message=str(e))

    def inject_data(self, node_name: str, port_name: str, values: list[Any]) -> ControlResponse:
        """Inject data into a node's input port."""
        try:
            packet_ids = self._net.inject_data(node_name, port_name, values)
            return ControlResponse(ok=True, message=f"Injected {len(packet_ids)} packets into '{node_name}.{port_name}'")
        except Exception as e:
            return ControlResponse(ok=False, message=str(e))

    # --- Config ---

    def get_config(self) -> dict:
        """Get the resolved net config as a JSON-serializable dict."""
        return self._net.config_resolved.model_dump(mode="json")

    # --- Private helpers ---

    def _node_to_status(self, info) -> NodeStatus:
        """Convert a NodeInfo to a NodeStatus model."""
        return NodeStatus(
            name=info.name,
            enabled=info.enabled,
            epoch_count=info.epoch_count,
            is_busy=info.is_busy,
            running_epoch_ids=[str(e.id) for e in info.running_epochs],
            startable_epoch_ids=[str(e.id) for e in info.startable_epochs],
            in_port_names=info.in_port_names,
            out_port_names=info.out_port_names,
        )
