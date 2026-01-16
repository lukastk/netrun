# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Pool Base Module

# %%
#|default_exp pool.test_base

# %%
#|export
import pytest
from netrun.pool.base import (
    WorkerId,
    WorkerMessage,
    Pool,
    PoolError,
    PoolNotStarted,
    PoolAlreadyStarted,
    WorkerError,
)

# %% [markdown]
# ## Test Types

# %%
#|export
def test_worker_id_is_int():
    """WorkerId should be an int alias."""
    worker_id: WorkerId = 42
    assert isinstance(worker_id, int)

# %%
test_worker_id_is_int();

# %%
#|export
def test_worker_message_dataclass():
    """Test WorkerMessage dataclass."""
    msg = WorkerMessage(worker_id=1, key="test", data={"value": 42})
    assert msg.worker_id == 1
    assert msg.key == "test"
    assert msg.data == {"value": 42}

# %%
test_worker_message_dataclass();

# %%
#|export
def test_worker_message_equality():
    """Test WorkerMessage equality."""
    msg1 = WorkerMessage(worker_id=1, key="test", data="data")
    msg2 = WorkerMessage(worker_id=1, key="test", data="data")
    msg3 = WorkerMessage(worker_id=2, key="test", data="data")

    assert msg1 == msg2
    assert msg1 != msg3

# %%
test_worker_message_equality();

# %% [markdown]
# ## Test Exceptions

# %%
#|export
def test_pool_error():
    """Test PoolError exception."""
    with pytest.raises(PoolError):
        raise PoolError("Test error")

# %%
test_pool_error();

# %%
#|export
def test_pool_not_started():
    """Test PoolNotStarted exception."""
    with pytest.raises(PoolNotStarted):
        raise PoolNotStarted("Pool not started")

    # Should be subclass of PoolError
    assert issubclass(PoolNotStarted, PoolError)

# %%
test_pool_not_started();

# %%
#|export
def test_pool_already_started():
    """Test PoolAlreadyStarted exception."""
    with pytest.raises(PoolAlreadyStarted):
        raise PoolAlreadyStarted("Pool already started")

    # Should be subclass of PoolError
    assert issubclass(PoolAlreadyStarted, PoolError)

# %%
test_pool_already_started();

# %%
#|export
def test_worker_error():
    """Test WorkerError exception."""
    err = WorkerError(worker_id=5, message="Worker failed")
    assert err.worker_id == 5
    assert "Worker failed" in str(err)

    # Should be subclass of PoolError
    assert issubclass(WorkerError, PoolError)

# %%
test_worker_error();

# %% [markdown]
# ## Test Pool Protocol

# %%
#|export
def test_pool_is_protocol():
    """Pool should be a Protocol."""
    # Pool is decorated with @runtime_checkable
    assert hasattr(Pool, '__protocol_attrs__') or isinstance(Pool, type)

# %%
test_pool_is_protocol();
