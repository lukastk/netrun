# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Pickling Module

# %%
#|default_exp _iutils.test_pickling

# %%
#|export
import pytest
from netrun._iutils.pickling import PicklingMethod, pickle_data, unpickle_data

# %% [markdown]
# ## pickle_data / unpickle_data with stdlib pickle

# %%
#|export
def test_pickle_roundtrip_dict():
    """Test pickle roundtrip with a dict."""
    data = {"key": "value", "nested": [1, 2, 3]}
    serialized = pickle_data(data, PicklingMethod.pickle)
    assert isinstance(serialized, bytes)
    assert unpickle_data(serialized, PicklingMethod.pickle) == data

# %%
#|export
def test_pickle_roundtrip_various_types():
    """Test pickle roundtrip with various Python types."""
    for data in [42, 3.14, "hello", True, None, [1, 2], (1, 2), {1, 2}]:
        serialized = pickle_data(data, PicklingMethod.pickle)
        result = unpickle_data(serialized, PicklingMethod.pickle)
        assert result == data

# %%
#|export
def test_pickle_default_protocol_4():
    """Test that default protocol is 4."""
    data = "test"
    serialized = pickle_data(data, PicklingMethod.pickle)
    # Protocol 4 starts with \\x80\\x04
    assert serialized[0:2] == b'\x80\x04'

# %%
#|export
def test_pickle_custom_protocol():
    """Test custom protocol override."""
    data = "test"
    serialized = pickle_data(data, PicklingMethod.pickle, protocol=5)
    assert serialized[0:2] == b'\x80\x05'

# %%
#|export
def test_pickle_optimize_default():
    """Test that optimization is applied by default."""
    data = {"key": "value"}
    optimized = pickle_data(data, PicklingMethod.pickle, optimize=True)
    unoptimized = pickle_data(data, PicklingMethod.pickle, optimize=False)
    # Optimized should be <= unoptimized in size
    assert len(optimized) <= len(unoptimized)
    # Both should deserialize to same value
    assert unpickle_data(optimized, PicklingMethod.pickle) == data
    assert unpickle_data(unoptimized, PicklingMethod.pickle) == data

# %% [markdown]
# ## PicklingMethod enum

# %%
#|export
def test_pickling_method_values():
    """Test PicklingMethod enum values."""
    assert PicklingMethod.pickle.value == "pickle"
    assert PicklingMethod.dill.value == "dill"
    assert PicklingMethod.cloudpickle.value == "cloudpickle"

# %%
#|export
def test_pickling_method_from_string():
    """Test creating PicklingMethod from string."""
    assert PicklingMethod("pickle") == PicklingMethod.pickle
    assert PicklingMethod("dill") == PicklingMethod.dill
    assert PicklingMethod("cloudpickle") == PicklingMethod.cloudpickle

# %% [markdown]
# ## Unknown method

# %%
#|export
def test_pickle_data_unknown_method():
    """Test that unknown method raises ValueError."""
    with pytest.raises(ValueError, match="Unknown pickling method"):
        pickle_data("data", "not_a_method")

def test_unpickle_data_unknown_method():
    """Test that unknown method raises ValueError."""
    with pytest.raises(ValueError, match="Unknown pickling method"):
        unpickle_data(b"data", "not_a_method")

# %% [markdown]
# ## cloudpickle unpickle uses stdlib pickle

# %%
#|export
def test_cloudpickle_unpickle_uses_stdlib():
    """Test that cloudpickle method uses stdlib pickle.loads for deserialization."""
    import pickle
    data = {"test": 123}
    # Serialize with stdlib pickle
    serialized = pickle.dumps(data, protocol=4)
    # Deserialize with cloudpickle method (should use pickle.loads)
    result = unpickle_data(serialized, PicklingMethod.cloudpickle)
    assert result == data
