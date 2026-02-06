# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Function Node Factory
#
# Tests for the `from_function` node factory.

# %%
#|default_exp node_factories.test_from_function

# %%
#|export
import pytest
from netrun.node_factories.from_function import (
    _from_function,
    _parse_function_signature,
    get_node_config,
    get_node_funcs,
)
from netrun.net.config import PortConfig

# %% [markdown]
# ## Tests for include_port_types

# %%
#|export
def test_parse_function_signature_with_types():
    """Test that signature parsing includes types by default."""
    def typed_func(a: int, b: str) -> float:
        return float(a) + len(b)

    parsed = _parse_function_signature(typed_func)
    assert parsed.in_ports["a"].port_type == int
    assert parsed.in_ports["b"].port_type == str
    assert parsed.out_ports["out"].port_type == float

# %%
#|export
def test_parse_function_signature_without_types():
    """Test that signature parsing can exclude types."""
    def typed_func(a: int, b: str) -> float:
        return float(a) + len(b)

    parsed = _parse_function_signature(typed_func, include_port_types=False)
    assert parsed.in_ports["a"].port_type is None
    assert parsed.in_ports["b"].port_type is None
    assert parsed.out_ports["out"].port_type is None

# %%
#|export
def test_from_function_with_types():
    """Test _from_function includes types by default."""
    def typed_func(a: int, b: str) -> float:
        return float(a) + len(b)

    config = _from_function(typed_func)
    assert config.in_ports["a"].port_type == int
    assert config.in_ports["b"].port_type == str
    assert config.out_ports["out"].port_type == float

# %%
#|export
def test_from_function_without_types():
    """Test _from_function can exclude types."""
    def typed_func(a: int, b: str) -> float:
        return float(a) + len(b)

    config = _from_function(typed_func, include_port_types=False)
    assert config.in_ports["a"].port_type is None
    assert config.in_ports["b"].port_type is None
    assert config.out_ports["out"].port_type is None

# %%
#|export
def test_get_node_config_with_types():
    """Test get_node_config includes types by default."""
    def typed_func(a: int, b: str) -> float:
        return float(a) + len(b)

    config = get_node_config(typed_func)
    assert config.in_ports["a"].port_type == int
    assert config.in_ports["b"].port_type == str
    assert config.out_ports["out"].port_type == float
    # Factory protocol: execution_config should be None
    assert config.execution_config is None

# %%
#|export
def test_get_node_config_without_types():
    """Test get_node_config can exclude types via factory_args."""
    def typed_func(a: int, b: str) -> float:
        return float(a) + len(b)

    config = get_node_config(typed_func, include_port_types=False)
    assert config.in_ports["a"].port_type is None
    assert config.in_ports["b"].port_type is None
    assert config.out_ports["out"].port_type is None
    # Factory protocol: execution_config should be None
    assert config.execution_config is None

# %%
#|export
def test_get_node_funcs_accepts_include_port_types():
    """Test get_node_funcs accepts include_port_types for consistency."""
    def typed_func(a: int) -> int:
        return a * 2

    # Should not raise - parameter accepted for consistency
    funcs = get_node_funcs(typed_func, include_port_types=False)
    assert len(funcs) == 4
    assert funcs[0] is not None  # exec_func
    assert funcs[1] is None  # start_func
    assert funcs[2] is None  # stop_func
    assert funcs[3] is None  # on_failure_func

# %%
#|export
def test_include_port_types_with_generic_types():
    """Test include_port_types works with generic types like list[int]."""
    def func_with_generics(items: list[int]) -> dict[str, int]:
        return {"count": len(items)}

    # With types - should store actual generic type objects (not strings)
    config_with = _from_function(func_with_generics, include_port_types=True)
    assert config_with.in_ports["items"].port_type is not None
    assert config_with.in_ports["items"].port_type == list[int]
    assert config_with.out_ports["out"].port_type == dict[str, int]

    # Without types - should be None
    config_without = _from_function(func_with_generics, include_port_types=False)
    assert config_without.in_ports["items"].port_type is None
    assert config_without.out_ports["out"].port_type is None

# %%
#|export
def test_generic_type_stored_as_type_object_not_string():
    """Test that generic types like list[int] are stored as actual type objects,
    not stringified. This is important for beartype-based type checking."""
    from typing import get_origin

    def func(data: list[int]) -> list[str]:
        return [str(x) for x in data]

    config = get_node_config(func)

    # Should be the actual generic alias, not a string
    in_type = config.in_ports["data"].port_type
    assert not isinstance(in_type, str), f"Expected type object, got string: {in_type!r}"
    assert get_origin(in_type) is list

    out_type = config.out_ports["out"].port_type
    assert not isinstance(out_type, str), f"Expected type object, got string: {out_type!r}"
    assert get_origin(out_type) is list

# %%
#|export
def test_port_config_accepts_generic_types():
    """Test that PortConfig can store generic type annotations."""
    from typing import get_origin

    # These should all be accepted without error
    pc_list = PortConfig(port_type=list[int])
    assert pc_list.port_type == list[int]
    assert get_origin(pc_list.port_type) is list

    pc_dict = PortConfig(port_type=dict[str, int])
    assert pc_dict.port_type == dict[str, int]

    pc_tuple = PortConfig(port_type=tuple[int, str])
    assert pc_tuple.port_type == tuple[int, str]

# %%
#|export
def test_port_config_rejects_invalid_types():
    """Test that PortConfig rejects truly invalid port_type values."""
    with pytest.raises(Exception):  # Pydantic ValidationError
        PortConfig(port_type=42)

    with pytest.raises(Exception):
        PortConfig(port_type=[1, 2, 3])

# %%
#|export
def test_generic_type_survives_from_function_roundtrip():
    """Test that generic types survive the model_dump/model_validate
    roundtrip inside _from_function."""
    from typing import get_origin

    def func(start: int, stop: int) -> list[int]:
        return list(range(start, stop))

    config = _from_function(func)

    # The out port should have the actual list[int] type, not a string
    out_type = config.out_ports["out"].port_type
    assert get_origin(out_type) is list, f"Expected generic alias, got {type(out_type)}: {out_type!r}"
    assert out_type == list[int]
