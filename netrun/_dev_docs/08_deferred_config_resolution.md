# Deferred Config Resolution

## Problem

Currently, `NodeConfig` eagerly expands factories at creation time via `@model_validator(mode="before")`. This causes issues:

1. **Serialization fails**: Factory expansion creates closures in `execution_config` that can't be serialized to JSON
2. **Loss of original form**: The raw config with `factory`/`factory_args` is transformed, losing the original representation
3. **Import side effects**: Functions are imported at config creation time, even if the config is never used

## Solution

### Principle: Configs Stay Raw Until Needed

Configs remain in their "raw" serializable form until explicitly resolved. Resolution happens at runtime (when creating a `Net`), not at config creation time.

### Changes

#### 1. Remove Eager Factory Expansion from `NodeConfig`

Remove the `expand_factory` model validator. The `factory` and `factory_args` fields stay as-is until `resolve()` is called.

#### 2. Add `resolve()` Methods

Same types, just populated differently:

```python
class NodeConfig(BaseModel):
    def resolve(self) -> "NodeConfig":
        """Return a resolved copy with factory expanded and imports resolved."""
        ...

class GraphConfig(BaseModel):
    def resolve(self) -> "GraphConfig":
        """Return a resolved copy with all nodes resolved."""
        ...

class NetConfig(BaseModel):
    def resolve(self) -> "NetConfig":
        """Return a resolved copy ready for execution."""
        ...
```

#### 3. Update `Net`

```python
class Net:
    def __init__(self, config: NetConfig):
        self._config = config  # Original unresolved
        self._config_resolved = config.resolve()  # Resolved for execution

    @property
    def config(self) -> NetConfig:
        """The original (unresolved) config."""
        return self._config
```

## Implementation Checklist

- [x] Add `NodeConfig.resolve()` - expands factory, resolves execution_config imports
- [x] Add `NodeExecutionConfig.resolve()` - resolves function import paths to callables
- [x] Add `GraphConfig.resolve()` - resolves all nodes
- [x] Add `NetConfig.resolve()` - resolves graph and any other import paths
- [x] Remove `expand_factory` validator from `NodeConfig`
- [x] Update `Net.__init__` to store `_config` and `_config_resolved`
- [x] Update `Net` internals to use `_config_resolved`
- [x] Update tests (all 48 config tests and 82 net tests pass)
- [x] Remove workarounds from example notebook
