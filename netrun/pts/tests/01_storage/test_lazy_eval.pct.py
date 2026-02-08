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
#|default_exp storage.test_lazy_eval

# %%
#|export
import pytest
from ulid import ULID

from netrun.storage import (
    PacketStore,
    PacketStoreConfig,
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
    store = PacketStore(PacketStoreConfig())
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
    store = PacketStore(PacketStoreConfig())
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
    store = PacketStore(PacketStoreConfig())
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
    store = PacketStore(PacketStoreConfig())
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
    store = PacketStore(PacketStoreConfig())
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
    store = PacketStore(PacketStoreConfig())
    pkt = ULID()
    # int() with an invalid string will raise ValueError
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
    store = PacketStore(PacketStoreConfig())
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
    store = PacketStore(PacketStoreConfig())
    pkt = ULID()
    store.register(pkt, LazyPacketValueSpec(
        func_import_path="nonexistent_module_xyz.some_func",
        args=(),
        kwargs={},
    ))

    with pytest.raises(ModuleNotFoundError):
        store.consume(pkt)

# %% [markdown]
# ## get_hash() with lazy values

# %%
#|export
def test_hash_lazy_value_without_evaluation():
    """Test that get_hash hashes the LazyPacketValueSpec itself when evaluation is off."""
    store = PacketStore(PacketStoreConfig(evaluate_lazy_value_for_hash=False))
    pkt = ULID()
    store.register(pkt, LazyPacketValueSpec(
        func_import_path="os.getpid",
        args=(),
        kwargs={},
    ))

    h = store.get_hash(pkt)

    assert isinstance(h, int)

# %%
#|export
def test_hash_lazy_value_with_evaluation():
    """Test that get_hash evaluates the lazy value when evaluation is on."""
    store = PacketStore(PacketStoreConfig(evaluate_lazy_value_for_hash=True))
    pkt = ULID()
    store.register(pkt, LazyPacketValueSpec(
        func_import_path="os.path.join",
        args=("/a", "b"),
        kwargs={},
    ))

    h = store.get_hash(pkt)

    # Hash of the evaluated result "/a/b"
    store2 = PacketStore(PacketStoreConfig())
    pkt2 = ULID()
    store2.register(pkt2, "/a/b")
    h2 = store2.get_hash(pkt2)

    assert h == h2

# %%
#|export
def test_hash_lazy_value_cached():
    """Test that get_hash caches the hash and doesn't re-evaluate."""
    store = PacketStore(PacketStoreConfig(evaluate_lazy_value_for_hash=False))
    pkt = ULID()
    store.register(pkt, LazyPacketValueSpec(
        func_import_path="os.getpid",
        args=(),
        kwargs={},
    ))

    h1 = store.get_hash(pkt)
    h2 = store.get_hash(pkt)

    assert h1 == h2

# %% [markdown]
# ## destroy() with lazy values

# %%
#|export
def test_destroy_lazy_value():
    """Test that destroy removes a lazy value without evaluating it."""
    store = PacketStore(PacketStoreConfig())
    pkt = ULID()
    # Use a func that would fail if called — proves destroy doesn't evaluate
    store.register(pkt, LazyPacketValueSpec(
        func_import_path="nonexistent_module_xyz.some_func",
        args=(),
        kwargs={},
    ))

    store.destroy(pkt)

    assert not store.exists(pkt)
