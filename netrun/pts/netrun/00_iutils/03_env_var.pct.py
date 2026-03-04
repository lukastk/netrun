# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp _iutils.env_var

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
from pydantic import BaseModel, Field, model_validator, model_serializer
from typing import Annotated, Any, Union, get_origin, get_args
from types import UnionType
from enum import Enum
import json as _json_module
import os
from pathlib import Path

# %%
#|export
class VarRef(BaseModel):
    """Reference to an environment variable or node variable.

    Serialised as ``{"$env": "VAR_NAME"}`` or ``{"$var": "var_name"}``,
    with optional ``"default"``.
    """
    model_config = {"populate_by_name": True}
    env: str | None = Field(default=None, alias="$env")
    var: str | None = Field(default=None, alias="$var")
    default: Any = None

    @model_validator(mode='before')
    @classmethod
    def check_source(cls, data):
        if isinstance(data, dict):
            has_env = '$env' in data or 'env' in data
            has_var = '$var' in data or 'var' in data
            if not has_env and not has_var:
                raise ValueError("VarRef requires either '$env' or '$var'")
            if has_env and has_var:
                raise ValueError("VarRef cannot have both '$env' and '$var'")
        return data

    @model_serializer(mode='wrap')
    def _serialize(self, handler):
        """Exclude None fields so dump produces clean ``{"$env": "X"}`` or ``{"$var": "x"}``."""
        d = handler(self)
        return {k: v for k, v in d.items() if v is not None}

# Backwards compat alias
EnvVar = VarRef


class ProjectRootPath:
    """Metadata marker for path fields that should be resolved relative to the project root.

    Usage: ``Annotated[str | VarRef | None, ProjectRootPath()]``
    """
    pass


def _has_project_root_path_marker(field_info) -> bool:
    """Check if a pydantic FieldInfo has a ProjectRootPath marker in its metadata."""
    for meta in getattr(field_info, 'metadata', []):
        if isinstance(meta, ProjectRootPath):
            return True
    return False


def _cast_env_var_value(raw: str, target_type: type, env_name: str = "") -> Any:
    """Cast a raw string from os.environ to the target Python type.

    Args:
        raw: The raw string value from the environment.
        target_type: The Python type to cast to.
        env_name: Name of the env var (for error messages).

    Returns:
        The cast value.

    Raises:
        ValueError: If casting fails.
    """
    try:
        if target_type is str or target_type is Any:
            return raw
        if target_type is int:
            return int(raw)
        if target_type is float:
            return float(raw)
        if target_type is bool:
            lowered = raw.lower().strip()
            if lowered in ("true", "1", "yes"):
                return True
            if lowered in ("false", "0", "no"):
                return False
            raise ValueError(
                f"Cannot parse '{raw}' as bool. "
                f"Expected one of: true/false, 1/0, yes/no"
            )
        if isinstance(target_type, type) and issubclass(target_type, Enum):
            return target_type(raw)
        # Fallback: try JSON parsing for complex types (list, dict, tuple, etc.)
        parsed = _json_module.loads(raw)
        return parsed
    except (ValueError, TypeError, _json_module.JSONDecodeError) as exc:
        raise ValueError(
            f"Cannot cast env var '{env_name}' value {raw!r} to {target_type.__name__ if hasattr(target_type, '__name__') else target_type}: {exc}"
        ) from exc


def _extract_target_type(annotation: Any) -> type:
    """Extract the actual target type from a union annotation, stripping VarRef and None.

    For ``Callable | str | VarRef | None`` returns ``str`` (env vars provide import
    path strings; callable resolution happens later).

    Args:
        annotation: The field type annotation.

    Returns:
        The target type for var ref casting.
    """
    from collections.abc import Callable

    origin = get_origin(annotation)

    # Handle Annotated[X, ...] — unwrap to X
    if origin is Annotated:
        inner_args = get_args(annotation)
        if inner_args:
            return _extract_target_type(inner_args[0])

    # Handle Union / X | Y
    if origin is Union or isinstance(annotation, UnionType):
        args = get_args(annotation)
        # Filter out VarRef and NoneType
        candidates = [
            a for a in args
            if a is not type(None) and a is not VarRef
        ]
        if not candidates:
            return str
        # If Callable is among candidates alongside str, prefer str
        # (env vars provide import path strings)
        non_callable = [a for a in candidates if a is not Callable and not (isinstance(a, type) and issubclass(a, Callable))]
        if non_callable:
            # Prefer the simplest scalar type
            for preferred in (str, int, float, bool):
                if preferred in non_callable:
                    return preferred
            return non_callable[0]
        return str  # All callable → return str

    # Not a union — return as-is
    if isinstance(annotation, type):
        return annotation
    return str


def _resolve_env_vars_in_list(lst: list) -> tuple[list, bool]:
    """Recursively resolve EnvVar instances in a list of EnvVarResolvableModel items."""
    changed = False
    new_list = []
    for item in lst:
        if isinstance(item, EnvVarResolvableModel):
            resolved = item.resolve_env_vars()
            if resolved is not item:
                changed = True
            new_list.append(resolved)
        else:
            new_list.append(item)
    return new_list, changed


def _resolve_env_vars_in_dict(d: dict) -> tuple[dict, bool]:
    """Recursively resolve EnvVar instances in a dict of EnvVarResolvableModel values."""
    changed = False
    new_dict = {}
    for key, value in d.items():
        if isinstance(value, EnvVarResolvableModel):
            resolved = value.resolve_env_vars()
            if resolved is not value:
                changed = True
            new_dict[key] = resolved
        else:
            new_dict[key] = value
    return new_dict, changed


def _resolve_var_ref_value(ref: "VarRef", resolved_vars: dict[str, Any], target_type: type, field_context: str) -> Any:
    """Resolve a single $var VarRef against a dict of resolved variables.

    Args:
        ref: The VarRef with var set.
        resolved_vars: Dict of variable name -> resolved value.
        target_type: The target Python type for this field.
        field_context: String describing the field (for error messages).

    Returns:
        The resolved value.

    Raises:
        ValueError: If the variable is not found and no default is provided.
    """
    value = resolved_vars.get(ref.var)
    if value is not None:
        # If already the right type, use directly
        if isinstance(value, str) and target_type is not str and target_type is not Any:
            return _cast_env_var_value(value, target_type, env_name=f"$var:{ref.var}")
        return value
    # Variable not found — check default
    if ref.default is not None:
        return ref.default
    raise ValueError(
        f"Node variable '{ref.var}' is not defined "
        f"and no default provided (field: {field_context})"
    )


def _resolve_var_refs_in_list(lst: list, resolved_vars: dict[str, Any]) -> tuple[list, bool]:
    """Recursively resolve $var VarRef instances in a list of EnvVarResolvableModel items."""
    changed = False
    new_list = []
    for item in lst:
        if isinstance(item, EnvVarResolvableModel):
            resolved = item.resolve_var_refs(resolved_vars)
            if resolved is not item:
                changed = True
            new_list.append(resolved)
        else:
            new_list.append(item)
    return new_list, changed


def _resolve_var_refs_in_dict(d: dict, resolved_vars: dict[str, Any]) -> tuple[dict, bool]:
    """Recursively resolve $var VarRef instances in a dict of EnvVarResolvableModel values."""
    changed = False
    new_dict = {}
    for key, value in d.items():
        if isinstance(value, EnvVarResolvableModel):
            resolved = value.resolve_var_refs(resolved_vars)
            if resolved is not value:
                changed = True
            new_dict[key] = resolved
        else:
            new_dict[key] = value
    return new_dict, changed


def _resolve_project_root_paths_in_list(lst: list, project_root: Path) -> tuple[list, bool]:
    """Recursively resolve project root paths in a list of EnvVarResolvableModel items."""
    changed = False
    new_list = []
    for item in lst:
        if isinstance(item, EnvVarResolvableModel):
            resolved = item.resolve_project_root_paths(project_root)
            if resolved is not item:
                changed = True
            new_list.append(resolved)
        else:
            new_list.append(item)
    return new_list, changed


def _resolve_project_root_paths_in_dict(d: dict, project_root: Path) -> tuple[dict, bool]:
    """Recursively resolve project root paths in a dict of EnvVarResolvableModel values."""
    changed = False
    new_dict = {}
    for key, value in d.items():
        if isinstance(value, EnvVarResolvableModel):
            resolved = value.resolve_project_root_paths(project_root)
            if resolved is not value:
                changed = True
            new_dict[key] = resolved
        else:
            new_dict[key] = value
    return new_dict, changed


class EnvVarResolvableModel(BaseModel):
    """Base class for config models that may contain VarRef fields."""

    def resolve_env_vars(self) -> "EnvVarResolvableModel":
        """Return a copy with all $env VarRef fields resolved from os.environ.

        Skips $var VarRefs (those are resolved in a second phase via resolve_var_refs).
        """
        updates = {}
        for field_name, field_info in type(self).model_fields.items():
            value = getattr(self, field_name)
            if isinstance(value, VarRef) and value.env is not None:
                target_type = _extract_target_type(field_info.annotation)
                raw = os.environ.get(value.env)
                if raw is None:
                    if value.default is not None:
                        updates[field_name] = value.default
                    else:
                        raise ValueError(
                            f"Environment variable '{value.env}' is not set "
                            f"and no default provided (field: {type(self).__name__}.{field_name})"
                        )
                else:
                    updates[field_name] = _cast_env_var_value(raw, target_type, env_name=value.env)
            elif isinstance(value, EnvVarResolvableModel):
                resolved = value.resolve_env_vars()
                if resolved is not value:
                    updates[field_name] = resolved
            elif isinstance(value, list):
                new_list, changed = _resolve_env_vars_in_list(value)
                if changed:
                    updates[field_name] = new_list
            elif isinstance(value, dict):
                new_dict, changed = _resolve_env_vars_in_dict(value)
                if changed:
                    updates[field_name] = new_dict
        if updates:
            return self.model_copy(update=updates)
        return self

    def resolve_var_refs(self, resolved_vars: dict[str, Any]) -> "EnvVarResolvableModel":
        """Return a copy with all $var VarRef fields resolved from resolved_vars.

        Skips $env VarRefs (those should already be resolved via resolve_env_vars).

        Args:
            resolved_vars: Dict of variable name -> resolved Python value.

        Returns:
            A copy with $var fields replaced by their resolved values.
        """
        if not resolved_vars:
            return self
        updates = {}
        for field_name, field_info in type(self).model_fields.items():
            value = getattr(self, field_name)
            if isinstance(value, VarRef) and value.var is not None:
                target_type = _extract_target_type(field_info.annotation)
                updates[field_name] = _resolve_var_ref_value(
                    value, resolved_vars, target_type,
                    f"{type(self).__name__}.{field_name}",
                )
            elif isinstance(value, EnvVarResolvableModel):
                resolved = value.resolve_var_refs(resolved_vars)
                if resolved is not value:
                    updates[field_name] = resolved
            elif isinstance(value, list):
                new_list, changed = _resolve_var_refs_in_list(value, resolved_vars)
                if changed:
                    updates[field_name] = new_list
            elif isinstance(value, dict):
                new_dict, changed = _resolve_var_refs_in_dict(value, resolved_vars)
                if changed:
                    updates[field_name] = new_dict
        if updates:
            return self.model_copy(update=updates)
        return self

    def resolve_project_root_paths(self, project_root: Path) -> "EnvVarResolvableModel":
        """Return a copy with all ProjectRootPath-annotated str fields resolved against project_root.

        For each field annotated with ``ProjectRootPath()``: if the value is a ``str``
        that is not an absolute path, resolve it relative to *project_root*.

        Recurses into nested ``EnvVarResolvableModel``, list, and dict values.

        Args:
            project_root: The project root directory to resolve relative paths against.

        Returns:
            A copy with relative path strings resolved to absolute paths.
        """
        updates = {}
        for field_name, field_info in type(self).model_fields.items():
            value = getattr(self, field_name)
            if _has_project_root_path_marker(field_info) and isinstance(value, str):
                p = Path(value)
                if not p.is_absolute():
                    updates[field_name] = str((project_root / p).resolve())
            elif isinstance(value, EnvVarResolvableModel):
                resolved = value.resolve_project_root_paths(project_root)
                if resolved is not value:
                    updates[field_name] = resolved
            elif isinstance(value, list):
                new_list, changed = _resolve_project_root_paths_in_list(value, project_root)
                if changed:
                    updates[field_name] = new_list
            elif isinstance(value, dict):
                new_dict, changed = _resolve_project_root_paths_in_dict(value, project_root)
                if changed:
                    updates[field_name] = new_dict
        if updates:
            return self.model_copy(update=updates)
        return self
