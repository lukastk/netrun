# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Observe Models

# %%
#|default_exp observe.test_models

# %%
#|export
import pytest

from netrun_utils.observe.models import (
    NetStatus,
    NodeStatus,
    EdgeStatus,
    EpochInfo,
    LogEntry,
    ControlResponse,
    SendControlRequest,
    InjectDataRequest,
)

# %%
#|export
def test_net_status_construction():
    status = NetStatus(started=True, paused=False, node_names=["a", "b"], edge_count=1)
    assert status.started is True
    assert status.paused is False
    assert status.node_names == ["a", "b"]
    assert status.edge_count == 1
    assert status.total_epochs == 0
    assert status.busy_nodes == []
    assert status.idle_nodes == []


def test_net_status_serialization():
    status = NetStatus(started=True, paused=False, busy_nodes=["a"])
    d = status.model_dump()
    assert d["started"] is True
    assert d["busy_nodes"] == ["a"]
    # Round-trip
    status2 = NetStatus.model_validate(d)
    assert status2 == status


def test_node_status_defaults():
    ns = NodeStatus(name="test", enabled=True, epoch_count=0, is_busy=False)
    assert ns.running_epoch_ids == []
    assert ns.startable_epoch_ids == []
    assert ns.in_port_names == []
    assert ns.out_port_names == []


def test_edge_status():
    es = EdgeStatus(source_node="a", source_port="out", target_node="b", target_port="in", packet_count=3)
    assert es.packet_count == 3
    d = es.model_dump()
    assert d["source_node"] == "a"


def test_epoch_info_minimal():
    ei = EpochInfo(epoch_id="ep1", node_name="n", state="running", created_at="2024-01-01T00:00:00")
    assert ei.state == "running"
    assert ei.outcome is None
    assert ei.was_cache_hit is False


def test_log_entry():
    le = LogEntry(timestamp="2024-01-01T00:00:00", message="hello")
    assert le.node_name is None
    assert le.epoch_id is None


def test_control_response():
    cr = ControlResponse(ok=True, message="done")
    assert cr.ok is True


def test_send_control_request():
    req = SendControlRequest(node_name="n", control_type="enable")
    assert req.value is None


def test_inject_data_request():
    req = InjectDataRequest(node_name="n", port_name="in", values=[1, 2, 3])
    assert len(req.values) == 3
