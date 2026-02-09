# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Lazy Packet Value Evaluation

# %%
#|default_exp packets.test_lazy_eval

# %%
#|export
import pytest
from ulid import ULID

from netrun.packets import (
    PacketStore,
    LazyPacketValueSpec,
    LazyPacketValueEvaluationError,
)

# %% [markdown]
# ## LazyPacketValueSpec

# %%
#|export
def test_lazy_spec_fields():
    """Test LazyPacketValueSpec stores func_import_path, args, kwargs."""
    spec = LazyPacketValueSpec(
        func_import_path="os.getpid",
        args=(),
        kwargs={},
    )
    assert spec.func_import_path == "os.getpid"
    assert spec.args == ()
    assert spec.kwargs == {}

# %%
#|export
def test_lazy_spec_with_args():
    """Test LazyPacketValueSpec stores args and kwargs."""
    spec = LazyPacketValueSpec(
        func_import_path="os.path.join",
        args=("/tmp", "test"),
        kwargs={},
    )
    assert spec.args == ("/tmp", "test")

# %% [markdown]
# ## consume() with lazy values

# %%
#|export
def test_consume_lazy_value_no_args():
    """Test consuming a lazy value that takes no arguments."""
    store = PacketStore()
    pkt = ULID()
    store.register(pkt, LazyPacketValueSpec(
        func_import_path="os.getpid",
        args=(),
        kwargs={},
    ))

    result = store.consume(pkt)

    assert isinstance(result, int)
    assert not store.exists(pkt)

# %%
#|export
def test_consume_lazy_value_with_args():
    """Test consuming a lazy value with positional arguments."""
    store = PacketStore()
    pkt = ULID()
    store.register(pkt, LazyPacketValueSpec(
        func_import_path="os.path.join",
        args=("/tmp", "subdir", "file.txt"),
        kwargs={},
    ))

    result = store.consume(pkt)

    assert result == "/tmp/subdir/file.txt"

# %%
#|export
def test_consume_lazy_value_with_kwargs():
    """Test consuming a lazy value with keyword arguments."""
    store = PacketStore()
    pkt = ULID()
    store.register(pkt, LazyPacketValueSpec(
        func_import_path="json.dumps",
        args=({"a": 1},),
        kwargs={"indent": 2},
    ))

    import json
    result = store.consume(pkt)

    assert result == json.dumps({"a": 1}, indent=2)

# %%
#|export
def test_consume_lazy_value_nested_module():
    """Test consuming a lazy value from a nested module path."""
    store = PacketStore()
    pkt = ULID()
    store.register(pkt, LazyPacketValueSpec(
        func_import_path="os.path.basename",
        args=("/usr/local/bin/python",),
        kwargs={},
    ))

    result = store.consume(pkt)

    assert result == "python"

# %%
#|export
def test_consume_regular_value_unaffected():
    """Test that consuming a regular (non-lazy) value still works."""
    store = PacketStore()
    pkt = ULID()
    store.register(pkt, {"key": "value"})

    result = store.consume(pkt)

    assert result == {"key": "value"}

# %% [markdown]
# ## Error handling

# %%
#|export
def test_consume_lazy_value_func_raises():
    """Test that func exceptions are wrapped in LazyPacketValueEvaluationError."""
    store = PacketStore()
    pkt = ULID()
    store.register(pkt, LazyPacketValueSpec(
        func_import_path="builtins.int",
        args=("not_a_number",),
        kwargs={},
    ))

    with pytest.raises(LazyPacketValueEvaluationError) as exc_info:
        store.consume(pkt)

    assert exc_info.value.packet_id == pkt
    assert isinstance(exc_info.value.original_exception, ValueError)

# %%
#|export
def test_lazy_value_nonexistent_function():
    """Test that referencing a nonexistent function raises ValueError."""
    store = PacketStore()
    pkt = ULID()
    store.register(pkt, LazyPacketValueSpec(
        func_import_path="os.nonexistent_function_xyz",
        args=(),
        kwargs={},
    ))

    with pytest.raises(ValueError, match="not found in module"):
        store.consume(pkt)

# %%
#|export
def test_lazy_value_nonexistent_module():
    """Test that referencing a nonexistent module raises ImportError."""
    store = PacketStore()
    pkt = ULID()
    store.register(pkt, LazyPacketValueSpec(
        func_import_path="nonexistent_module_xyz.some_func",
        args=(),
        kwargs={},
    ))

    with pytest.raises(ModuleNotFoundError):
        store.consume(pkt)

# %% [markdown]
# ## peek() with lazy values

# %%
#|export
def test_peek_returns_lazy_spec():
    """Test that peek returns the LazyPacketValueSpec without evaluating."""
    store = PacketStore()
    pkt = ULID()
    spec = LazyPacketValueSpec(
        func_import_path="os.getpid",
        args=(),
        kwargs={},
    )
    store.register(pkt, spec)

    result = store.peek(pkt)

    assert result is spec
    assert store.exists(pkt)

# %%
#|export
def test_peek_returns_regular_value():
    """Test that peek returns regular values."""
    store = PacketStore()
    pkt = ULID()
    store.register(pkt, 42)

    assert store.peek(pkt) == 42
    assert store.exists(pkt)

# %% [markdown]
# ## destroy() with lazy values

# %%
#|export
def test_destroy_lazy_value():
    """Test that destroy removes a lazy value without evaluating it."""
    store = PacketStore()
    pkt = ULID()
    store.register(pkt, LazyPacketValueSpec(
        func_import_path="nonexistent_module_xyz.some_func",
        args=(),
        kwargs={},
    ))

    store.destroy(pkt)

    assert not store.exists(pkt)
