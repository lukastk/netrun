"""Pydantic model introspection for config schema generation."""
from __future__ import annotations

import enum
import types
import typing
from collections.abc import Callable
from typing import Any, get_args, get_origin

from pydantic import BaseModel
from pydantic.fields import FieldInfo


class FieldCategory(str, enum.Enum):
    """Category of a model field for UI rendering."""
    BOOL = "bool"
    STR = "str"
    INT = "int"
    FLOAT = "float"
    ENUM = "enum"
    BOOL_OR_NULL = "bool_or_null"
    STR_OR_NULL = "str_or_null"
    INT_OR_NULL = "int_or_null"
    FLOAT_OR_NULL = "float_or_null"
    ENUM_OR_NULL = "enum_or_null"
    COMPLEX = "complex"


class FieldSchema(BaseModel):
    """Schema information for a single model field."""
    name: str
    category: FieldCategory
    default: Any = None
    description: str | None = None
    enum_values: list[str] | None = None
    required: bool = False
    env_var_supported: bool = False  # True when field supports VarRef ($env or $var)


class ModelSchema(BaseModel):
    """Schema for an entire pydantic model."""
    model_name: str
    fields: list[FieldSchema]


class ConfigSchemaResponse(BaseModel):
    """Response containing schemas for all config models."""
    models: dict[str, ModelSchema]


def _unwrap_optional(annotation: Any) -> tuple[Any, bool]:
    """Unwrap Optional[X] / X | None to (X, True). Returns (annotation, False) if not optional."""
    origin = get_origin(annotation)

    # Handle Union types (typing.Union or X | Y)
    if origin is types.UnionType or origin is typing.Union:
        args = get_args(annotation)
        non_none = [a for a in args if a is not type(None)]
        has_none = len(non_none) < len(args)
        if has_none and len(non_none) == 1:
            return non_none[0], True
        if has_none and len(non_none) > 1:
            # Multiple non-None types in union → complex
            return annotation, False
        # No None in union but multiple types → complex
        if len(non_none) > 1:
            return annotation, False

    return annotation, False


def _has_callable_or_module(annotation: Any) -> bool:
    """Check if annotation contains Callable or ModuleType."""
    origin = get_origin(annotation)
    if origin is types.UnionType or origin is typing.Union:
        args = get_args(annotation)
        for arg in args:
            if arg is Callable or arg is types.ModuleType:
                return True
            # Check if it's collections.abc.Callable
            if hasattr(arg, '__module__') and hasattr(arg, '__qualname__'):
                if arg.__qualname__ == 'Callable' or getattr(arg, '__name__', '') == 'Callable':
                    return True
    if annotation is Callable or annotation is types.ModuleType:
        return True
    return False


def _strip_envvar(annotation: Any) -> tuple[Any, bool]:
    """Remove VarRef (EnvVar) from a union type annotation.

    Returns (stripped_annotation, had_varref).
    For ``int | VarRef`` → ``(int, True)``.
    For ``bool | VarRef | None`` → ``(bool | None, True)``.
    For non-union types or unions without VarRef → ``(annotation, False)``.
    """
    from netrun._iutils.var_ref import VarRef

    origin = get_origin(annotation)
    if origin is not types.UnionType and origin is not typing.Union:
        return annotation, False

    args = get_args(annotation)
    has_varref = any(a is VarRef for a in args)
    if not has_varref:
        return annotation, False

    remaining = [a for a in args if a is not VarRef]
    if len(remaining) == 0:
        # Degenerate: union was only VarRef
        return annotation, True
    if len(remaining) == 1:
        return remaining[0], True
    # Rebuild union from remaining types
    rebuilt = remaining[0]
    for t in remaining[1:]:
        rebuilt = rebuilt | t
    return rebuilt, True


def _classify_type(annotation: Any) -> tuple[FieldCategory, list[str] | None]:
    """Classify a type annotation into a FieldCategory.

    Returns (category, enum_values) where enum_values is set for ENUM types.
    """
    # Check for Callable/ModuleType first (even in unions)
    if _has_callable_or_module(annotation):
        return FieldCategory.COMPLEX, None

    inner, is_optional = _unwrap_optional(annotation)

    # Check for Callable/ModuleType in inner
    if _has_callable_or_module(inner):
        return FieldCategory.COMPLEX, None

    # Map basic types
    if inner is bool:
        return (FieldCategory.BOOL_OR_NULL if is_optional else FieldCategory.BOOL), None
    if inner is str:
        return (FieldCategory.STR_OR_NULL if is_optional else FieldCategory.STR), None
    if inner is int:
        return (FieldCategory.INT_OR_NULL if is_optional else FieldCategory.INT), None
    if inner is float:
        return (FieldCategory.FLOAT_OR_NULL if is_optional else FieldCategory.FLOAT), None

    # Check for Enum subclass
    if isinstance(inner, type) and issubclass(inner, enum.Enum):
        values = [e.value for e in inner]
        return (FieldCategory.ENUM_OR_NULL if is_optional else FieldCategory.ENUM), values

    # Check for Literal types
    origin = get_origin(inner)
    if origin is typing.Literal:
        values = [str(v) for v in get_args(inner)]
        return (FieldCategory.ENUM_OR_NULL if is_optional else FieldCategory.ENUM), values

    # Everything else is complex
    return FieldCategory.COMPLEX, None


def _get_default_value(field_info: FieldInfo) -> Any:
    """Extract a JSON-serializable default value from a field."""
    from pydantic_core import PydanticUndefined

    if field_info.default is PydanticUndefined:
        if field_info.default_factory is not None:
            try:
                val = field_info.default_factory()
                return _serialize_default(val)
            except Exception:
                return None
        return None

    return _serialize_default(field_info.default)


def _serialize_default(value: Any) -> Any:
    """Make a default value JSON-serializable."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, str)):
        return value
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, (list, tuple)):
        return [_serialize_default(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _serialize_default(v) for k, v in value.items()}
    # Callable, BaseModel, etc. → None
    return None


def get_model_schema(model_cls: type[BaseModel], model_name: str) -> ModelSchema:
    """Introspect a pydantic model and return its schema.

    Args:
        model_cls: The pydantic BaseModel class to introspect.
        model_name: Display name for the model.

    Returns:
        ModelSchema with classified fields.
    """
    from pydantic_core import PydanticUndefined

    fields: list[FieldSchema] = []

    for name, field_info in model_cls.model_fields.items():
        annotation = field_info.annotation
        stripped_annotation, env_var_supported = _strip_envvar(annotation)
        category, enum_values = _classify_type(stripped_annotation)

        required = (
            field_info.default is PydanticUndefined
            and field_info.default_factory is None
        )

        default = _get_default_value(field_info)
        description = field_info.description

        fields.append(FieldSchema(
            name=name,
            category=category,
            default=default,
            description=description,
            enum_values=enum_values,
            required=required,
            env_var_supported=env_var_supported,
        ))

    return ModelSchema(model_name=model_name, fields=fields)
