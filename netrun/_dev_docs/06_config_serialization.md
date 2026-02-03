# GraphConfig Serialization Assessment

## Overview

This document assesses the current state of `GraphConfig` (and related config classes) serialization, specifically focusing on:
1. **Serialization**: Converting configs to JSON (particularly handling function fields)
2. **Deserialization**: Converting JSON back to configs (keeping import paths as strings)
3. **Runtime handling**: Net dynamically importing functions from string import paths

## Current State

### Function Field Serialization

#### `NodeExecutionConfig` Function Fields

```python
@field_serializer("exec_node_func", "start_node_func", "stop_node_func", "on_node_failure")
def serialize_func(self, func: Callable | str | None) -> str | None:
    if func is None:
        return None
    if isinstance(func, str):
        return func
    # Can't serialize function objects - return None
    return None
```

**Status: PROBLEMATIC**

Current behavior:
- If func is `None`: returns `None` ✓
- If func is a string (import path): returns the string ✓
- If func is a callable: **returns `None` (silently loses the function)**

Problems:
1. **Silent data loss**: Functions are silently converted to `None` during serialization
2. **No attempt to extract import path**: Could try `func.__module__` + `func.__qualname__`
3. **No error for unserializable functions**: Lambdas, closures, and `__main__` functions should error

#### `NodeConfig.factory` Field

```python
@field_serializer("factory")
def serialize_factory(self, factory: str | ModuleType | None) -> str | None:
    if factory is None:
        return None
    if isinstance(factory, str):
        return factory
    # Convert module to import path
    return factory.__name__
```

**Status: PARTIAL**

- Works for module objects (uses `__name__`)
- If module is `__main__`, serializes to `"__main__"` which can't be imported back

#### `PortConfig.port_type` Field

```python
@field_serializer("port_type")
def serialize_port_type(self, port_type: str | type | PortTypeConfig | None) -> str | dict | None:
    if isinstance(port_type, type):
        # Convert type to name string
        # Note: loses isinstance capability on reload, becomes name match
        return port_type.__name__
```

**Status: PROBLEMATIC**

Current behavior:
- Type objects serialize to just their `__name__` (e.g., `DataFrame` instead of `pandas.DataFrame`)
- This loses the ability to import the type back and do `isinstance` checks
- Types from `__main__` silently serialize to just the name with no error

Problems:
1. **Should serialize to full import path**: `type.__module__` + `type.__qualname__`
2. **Should error for `__main__` types**: Can't be imported back
3. **Net should import type from string path** at runtime to preserve `isinstance` capability

#### `NetConfig.dead_letter_callback` Field

```python
dead_letter_callback: Callable | str = None
```

**Status: MISSING SERIALIZER**

- No `@field_serializer` defined
- Pydantic will fail to serialize if a Callable is provided

### Deserialization

**Status: CORRECT**

When deserializing from JSON:
- Import path strings remain as strings (no auto-import)
- This is the desired behavior per requirements

### Net Runtime Handling of String Import Paths

#### `on_node_failure` callback

```python
async def _call_failure_callback(
    self,
    callback: Callable | str,
    failure_ctx: NodeFailureContext,
) -> None:
    if isinstance(callback, str):
        # Import from path
        import importlib
        module_path, func_name = callback.rsplit(".", 1)
        module = importlib.import_module(module_path)
        callback = getattr(module, func_name)
```

**Status: WORKING**

Correctly handles string import paths by dynamically importing at runtime.

#### `exec_node_func` in `_register_node_functions`

```python
async def _register_node_functions(self) -> None:
    for node_config in self.config.graph.nodes:
        if node_config.execution_config.exec_node_func is None:
            continue
        config = node_config.execution_config
        for pool_id in config.pools:
            await self._execution_manager.send_function_to_pool(
                pool_id=pool_id,
                func_key=func_key,
                func=config.exec_node_func,  # <-- Direct passthrough
            )
```

**Status: NOT HANDLED**

- String import paths are passed directly without importing
- `send_function_to_pool` expects a callable, will likely fail or behave incorrectly

#### `start_node_func` / `stop_node_func`

**Status: NOT IMPLEMENTED**

These callbacks don't appear to be invoked anywhere in the current Net implementation.

#### Port Type Checking (`_check_type` in `NodeExecutionContext`)

```python
def _check_type(self, port_type: "str | type | PortTypeConfig", value: Any) -> tuple[str, bool]:
    if isinstance(port_type, str):
        # String: check type name
        return (port_type, type(value).__name__ == port_type)

    if isinstance(port_type, type):
        # Type object: use isinstance
        return (port_type.__name__, isinstance(value, port_type))
```

**Status: PARTIAL**

- When `port_type` is a `type` object: uses `isinstance` correctly
- When `port_type` is a string: only does name matching, no `isinstance`

After serialization roundtrip, type objects become strings, so `isinstance` capability is lost.
Need to import the type from the string import path to preserve `isinstance` checks.

---

## Gaps Summary

| Area | Issue | Severity |
|------|-------|----------|
| Function serialization | Silent data loss (returns `None` for callables) | HIGH |
| Function serialization | No attempt to compute import path | HIGH |
| Function serialization | No error for `__main__`/lambda/closure | MEDIUM |
| Port type serialization | Only serializes `__name__`, not full import path | HIGH |
| Port type serialization | No error for `__main__` types | MEDIUM |
| Port type runtime | String port types don't support `isinstance` (only name match) | HIGH |
| `dead_letter_callback` | Missing serializer | MEDIUM |
| Net: `exec_node_func` | Doesn't handle string import paths | HIGH |
| Net: `start_node_func` | Not implemented (and no string handling) | LOW |
| Net: `stop_node_func` | Not implemented (and no string handling) | LOW |

---

## Plan

### Phase 1: Fix Function Serialization

Update `NodeExecutionConfig.serialize_func` to:

```python
@field_serializer("exec_node_func", "start_node_func", "stop_node_func", "on_node_failure")
def serialize_func(self, func: Callable | str | None) -> str | None:
    """Serialize functions to their import path.

    Raises:
        ValueError: If function cannot be serialized (lambda, closure, __main__).
    """
    if func is None:
        return None
    if isinstance(func, str):
        return func

    # Attempt to extract import path
    module = getattr(func, "__module__", None)
    qualname = getattr(func, "__qualname__", None)

    if module is None or qualname is None:
        raise ValueError(
            f"Cannot serialize function {func}: missing __module__ or __qualname__"
        )

    # Check for unserializable cases
    if module == "__main__":
        raise ValueError(
            f"Cannot serialize function '{qualname}' defined in __main__. "
            "Move it to an importable module or use a string import path."
        )

    if "<lambda>" in qualname:
        raise ValueError(
            f"Cannot serialize lambda functions. "
            "Define a named function or use a string import path."
        )

    if "<locals>" in qualname:
        raise ValueError(
            f"Cannot serialize closure/local function '{qualname}'. "
            "Define it at module level or use a string import path."
        )

    # Return import path
    return f"{module}.{qualname}"
```

### Phase 2: Fix Port Type Serialization

Update `PortConfig.serialize_port_type` to use full import paths:

```python
@field_serializer("port_type")
def serialize_port_type(self, port_type: str | type | PortTypeConfig | None) -> str | dict | None:
    """Serialize port_type to import path or config dict.

    Raises:
        ValueError: If type cannot be serialized (__main__, etc.).
    """
    if port_type is None:
        return None
    if isinstance(port_type, str):
        return port_type
    if isinstance(port_type, PortTypeConfig):
        return port_type.model_dump()
    if isinstance(port_type, type):
        # Extract full import path
        module = getattr(port_type, "__module__", None)
        qualname = getattr(port_type, "__qualname__", None)

        if module is None or qualname is None:
            raise ValueError(
                f"Cannot serialize type {port_type}: missing __module__ or __qualname__"
            )

        if module == "__main__":
            raise ValueError(
                f"Cannot serialize type '{qualname}' defined in __main__. "
                "Move it to an importable module or use a string import path."
            )

        if module == "builtins":
            # Built-in types like int, str, list, dict - just use name
            return qualname

        # Return full import path
        return f"{module}.{qualname}"

    return None
```

### Phase 3: Add Serializer for `dead_letter_callback`

Add to `NetConfig`:

```python
@field_serializer("dead_letter_callback")
def serialize_dead_letter_callback(self, callback: Callable | str | None) -> str | None:
    # Same logic as NodeExecutionConfig.serialize_func
    ...
```

### Phase 4: Update Net to Handle String Import Paths

Update `_register_node_functions` to import from string paths:

```python
async def _register_node_functions(self) -> None:
    for node_config in self.config.graph.nodes:
        if node_config.execution_config is None:
            continue
        exec_func = node_config.execution_config.exec_node_func
        if exec_func is None:
            continue

        # Resolve string import path if needed
        if isinstance(exec_func, str):
            exec_func = self._import_from_path(exec_func)

        config = node_config.execution_config
        func_key = self._get_func_key(node_config.name)

        for pool_id in config.pools:
            await self._execution_manager.send_function_to_pool(
                pool_id=pool_id,
                func_key=func_key,
                func=exec_func,
            )

def _import_from_path(self, import_path: str) -> Any:
    """Import a function or type from a dotted import path."""
    module_path, name = import_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, name)
```

### Phase 5: Update Port Type Checking to Support String Import Paths

Update `NodeExecutionContext._check_type` to import types from string import paths:

```python
def _check_type(self, port_type: "str | type | PortTypeConfig", value: Any) -> tuple[str, bool]:
    """Check if value matches port type. Returns (expected_name, matches)."""
    if isinstance(port_type, str):
        # String could be:
        # 1. Simple name like "DataFrame" - do name match only
        # 2. Full import path like "pandas.DataFrame" - import and isinstance

        if "." in port_type:
            # Full import path - import the type and use isinstance
            try:
                type_obj = self._import_type(port_type)
                return (port_type.rsplit(".", 1)[-1], isinstance(value, type_obj))
            except (ImportError, AttributeError):
                # Fall back to name match if import fails
                type_name = port_type.rsplit(".", 1)[-1]
                return (type_name, type(value).__name__ == type_name)
        else:
            # Simple name - do name match
            return (port_type, type(value).__name__ == port_type)

    if isinstance(port_type, type):
        # Type object: use isinstance
        return (port_type.__name__, isinstance(value, port_type))

    if isinstance(port_type, PortTypeConfig):
        # Config object - check isinstance_check flag
        if port_type.isinstance_check and "." in port_type.name:
            try:
                type_obj = self._import_type(port_type.name)
                return (port_type.name.rsplit(".", 1)[-1], isinstance(value, type_obj))
            except (ImportError, AttributeError):
                pass
        # Fall back to name match
        type_name = port_type.name.rsplit(".", 1)[-1] if "." in port_type.name else port_type.name
        return (type_name, type(value).__name__ == type_name)

    # Unknown type spec - skip validation
    return ("any", True)

def _import_type(self, import_path: str) -> type:
    """Import a type from a dotted import path."""
    import importlib
    module_path, type_name = import_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, type_name)
```

**Note**: The `_import_type` helper could benefit from caching to avoid repeated imports. Consider a module-level cache dict.

### Phase 6: Add Tests

1. **Function serialization tests**:
   - Test that module-level functions serialize to import paths
   - Test that lambdas raise `ValueError`
   - Test that `__main__` functions raise `ValueError`
   - Test that closures raise `ValueError`
   - Test roundtrip: serialize -> deserialize -> import -> call

2. **Port type serialization tests**:
   - Test that type objects serialize to full import paths
   - Test that `__main__` types raise `ValueError`
   - Test that builtin types serialize to just the name
   - Test roundtrip: serialize -> deserialize -> import -> isinstance

3. **Net runtime tests**:
   - Test that string import paths work for `exec_node_func`
   - Test that string import paths work for `on_node_failure`
   - Test that port type import paths enable isinstance checks
   - Test error handling for invalid import paths

---

## Implementation Checklist

### Phase 1: Fix Function Serialization
- [ ] Update `NodeExecutionConfig.serialize_func` to compute import path
- [ ] Add validation for `__main__`, lambda, and closure functions
- [ ] Run `nbl export -r`

### Phase 2: Fix Port Type Serialization
- [ ] Update `PortConfig.serialize_port_type` to use full import paths
- [ ] Add validation for `__main__` types
- [ ] Handle builtin types specially (just use name)
- [ ] Run `nbl export -r`

### Phase 3: Add dead_letter_callback Serializer
- [ ] Add `@field_serializer("dead_letter_callback")` to `NetConfig`
- [ ] Run `nbl export -r`

### Phase 4: Update Net String Import Handling for Functions
- [ ] Add `_import_from_path` helper method to `Net`
- [ ] Update `_register_node_functions` to handle string import paths
- [ ] (Future) Handle `start_node_func` and `stop_node_func` when implemented
- [ ] Run `nbl export -r`

### Phase 5: Update Port Type Checking for String Import Paths
- [ ] Update `NodeExecutionContext._check_type` to import types from paths
- [ ] Add `_import_type` helper method
- [ ] Consider adding import caching for performance
- [ ] Run `nbl export -r`

### Phase 6: Tests
- [ ] Add serialization tests for function fields
- [ ] Add serialization tests for port types
- [ ] Add Net runtime tests for string import paths
- [ ] Add port type isinstance tests with string import paths
- [ ] Test error cases (invalid paths, __main__, lambdas)

### Phase 7: Documentation
- [ ] Update PROJECT_SPEC.md with serialization requirements
- [ ] Add examples showing string import path usage

---

## Open Questions

1. **Should we support partial serialization?**

   Option A: Raise error if any function can't be serialized (strict)
   Option B: Return `None` for unserializable functions, log warning (lenient)

   **Recommendation**: Option A (strict). Silent data loss is worse than an error.

2. **Should deserialization auto-import functions?**

   **Decision**: No. Keep import paths as strings during deserialization. Net handles import at runtime. This:
   - Allows configs to be loaded without importing heavy dependencies
   - Makes configs truly serializable (JSON doesn't have function references)
   - Follows the pattern already used for `on_node_failure`

3. **What about factory functions returning functions?**

   Factory modules (via `factory` field) return functions from `get_node_funcs()`. These are already handled correctly since the factory is imported and called at config construction time, returning actual function objects. The serialization issue only applies to direct function references.

4. **Should we add a validation method to check import paths?**

   Could add `GraphConfig.validate_import_paths()` to verify all string import paths can be resolved. Useful for catching errors early.

   **Recommendation**: Add as a separate utility, not automatic on load.

5. **How should port type string import paths behave?**

   When `port_type` is a string like `"pandas.DataFrame"`:
   - Should we always try to import and use `isinstance`?
   - Or only when `PortTypeConfig.isinstance_check=True`?

   **Decision**: If the string contains a `.`, assume it's an import path and try to import for isinstance. If import fails, fall back to name matching. This preserves the "it just works" experience while enabling proper isinstance checks after serialization roundtrip.

6. **What about nested types (e.g., `pandas.core.frame.DataFrame`)?**

   The `type.__module__` + `type.__qualname__` approach handles these correctly. For example:
   - `pd.DataFrame.__module__` = `"pandas.core.frame"`
   - `pd.DataFrame.__qualname__` = `"DataFrame"`
   - Result: `"pandas.core.frame.DataFrame"`

   This is the correct import path.
