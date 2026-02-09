# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Serialization Module

# %%
#|default_exp storage.test_serialization

# %%
#|export
import pytest

from netrun.storage._serialization import (
    SerializationMethod,
    serialize,
    deserialize,
    get_file_extension,
    validate_imports,
)

# %% [markdown]
# ## JSON round-trip

# %%
#|export
def test_json_round_trip():
    value = {"key": [1, 2, 3], "nested": {"a": True}}
    data = serialize(value, SerializationMethod.json)
    assert isinstance(data, bytes)
    result = deserialize(data, SerializationMethod.json)
    assert result == value


def test_json_extension():
    assert get_file_extension(SerializationMethod.json) == ".json"

# %% [markdown]
# ## Pickle round-trip

# %%
#|export
def test_pickle_round_trip():
    value = {"data": [1, 2, 3], "set": {4, 5}}
    data = serialize(value, SerializationMethod.pickle)
    assert isinstance(data, bytes)
    result = deserialize(data, SerializationMethod.pickle)
    assert result == value


def test_pickle_extension():
    assert get_file_extension(SerializationMethod.pickle) == ".pkl"

# %% [markdown]
# ## String round-trip

# %%
#|export
def test_str_round_trip():
    value = "hello world"
    data = serialize(value, SerializationMethod.str)
    assert isinstance(data, bytes)
    assert data == b"hello world"
    result = deserialize(data, SerializationMethod.str)
    assert result == value


def test_str_extension():
    assert get_file_extension(SerializationMethod.str) == ".txt"

# %% [markdown]
# ## Binary passthrough

# %%
#|export
def test_binary_round_trip():
    value = b"\x00\x01\x02\xff"
    data = serialize(value, SerializationMethod.binary)
    assert data is value  # passthrough, no copy
    result = deserialize(data, SerializationMethod.binary)
    assert result == value


def test_binary_extension():
    assert get_file_extension(SerializationMethod.binary) == ".bin"

# %% [markdown]
# ## Validate imports (stdlib)

# %%
#|export
def test_validate_imports_json():
    """JSON is stdlib, should never fail."""
    validate_imports(SerializationMethod.json)


def test_validate_imports_pickle():
    """Pickle is stdlib, should never fail."""
    validate_imports(SerializationMethod.pickle)

# %% [markdown]
# ## Extension mapping completeness

# %%
#|export
def test_all_methods_have_extensions():
    """Every SerializationMethod should have a file extension."""
    for method in SerializationMethod:
        ext = get_file_extension(method)
        assert ext.startswith("."), f"{method} extension should start with '.', got {ext!r}"

# %% [markdown]
# ## JSON with special values

# %%
#|export
def test_json_unicode():
    value = {"emoji": "hello 🌍", "cjk": "你好"}
    data = serialize(value, SerializationMethod.json)
    result = deserialize(data, SerializationMethod.json)
    assert result == value


def test_json_list():
    value = [1, "two", 3.0, None, True]
    data = serialize(value, SerializationMethod.json)
    result = deserialize(data, SerializationMethod.json)
    assert result == value

# %% [markdown]
# ## Pickle with complex types

# %%
#|export
def test_pickle_complex_types():
    """Pickle should handle Python-specific types that JSON can't."""
    import datetime
    value = {
        "tuple": (1, 2, 3),
        "set": {4, 5, 6},
        "bytes": b"hello",
        "datetime": datetime.datetime(2024, 1, 1, 12, 0, 0),
    }
    data = serialize(value, SerializationMethod.pickle)
    result = deserialize(data, SerializationMethod.pickle)
    assert result == value
