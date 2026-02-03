# Node Factories Implementation Plan

## Overview

Node factories provide a way to create `NodeGraphConfig` instances from reusable factory modules. A factory module contains two functions that define the node's graph structure and execution functions separately.

## Design

### Factory Module Structure

A node factory is a Python module containing two functions with the same signature:

```python
# myapp/nodes/worker.py

from netrun.net.config import NodeGraphConfig, PortConfig, SalvoConditionConfig, ...

def get_node_config(name: str, threshold: float = 0.5) -> NodeGraphConfig:
    """Returns graph structure: name, ports, salvo conditions.

    Must NOT set execution_config - that comes from get_node_funcs().
    """
    return NodeGraphConfig(
        name=name,
        in_ports={"task": PortConfig()},
        out_ports={"result": PortConfig()},
        in_salvo_conditions={
            "trigger": SalvoConditionConfig(
                max_salvos=MaxSalvosFiniteConfig(max=1),
                ports={"task": PacketCountAllConfig()},
                term=SalvoConditionTermPortConfig(
                    port_name="task",
                    state=PortStateNonEmptyConfig(),
                ),
            ),
        },
        out_salvo_conditions={
            "send": SalvoConditionConfig(
                max_salvos=MaxSalvosFiniteConfig(max=1),
                ports={"result": PacketCountAllConfig()},
                term=SalvoConditionTermTrueConfig(),
            ),
        },
        # execution_config is NOT set here
    )


def get_node_funcs(name: str, threshold: float = 0.5) -> tuple[
    Callable | None,    # exec_node_func
    Callable | None,    # start_node_func
    Callable | None,    # stop_node_func
    Callable | None,    # on_node_failure
]:
    """Returns execution functions.

    Arguments can be captured in closures for use in the functions.
    """

    def exec_func(ctx, packets):
        ctx.print(f"Processing with threshold={threshold}")
        for packet_id in packets.get("task", []):
            value = ctx.consume_packet(packet_id)
            if value.get("score", 0) > threshold:
                out_id = ctx.create_packet({"passed": True, **value})
                ctx.load_output_port("result", out_id)
        ctx.send_output_salvo("send")

    return (exec_func, None, None, None)
```

**Key points:**
- Both functions take the **same arguments**
- `get_node_config()` returns `NodeGraphConfig` WITHOUT `execution_config`
- `get_node_funcs()` returns a tuple of 4 functions (any can be None)
- Factory args can be captured in closures within the functions

### API

#### `NodeGraphConfig.from_factory()` Class Method

```python
@classmethod
def from_factory(
    cls,
    factory: str | ModuleType,
    args: dict[str, Any] | None = None,
) -> "NodeGraphConfig":
    """Create a NodeGraphConfig from a factory module.

    Args:
        factory: Factory module or import path to module containing
                 get_node_config() and get_node_funcs().
        args: Arguments passed to both factory functions.

    Returns:
        Complete NodeGraphConfig with execution_config populated.

    Raises:
        ImportError: If factory module cannot be imported.
        AttributeError: If module missing get_node_config or get_node_funcs.
    """
```

**Implementation:**
```python
@classmethod
def from_factory(
    cls,
    factory: str | ModuleType,
    args: dict[str, Any] | None = None,
) -> "NodeGraphConfig":
    args = args or {}

    # Import module if string
    if isinstance(factory, str):
        module = importlib.import_module(factory)
    else:
        module = factory

    # Get factory functions
    get_node_config = getattr(module, "get_node_config")
    get_node_funcs = getattr(module, "get_node_funcs")

    # Call factories
    base_config = get_node_config(**args)
    exec_func, start_func, stop_func, on_failure_func = get_node_funcs(**args)

    # Build execution config from functions
    execution_config = NodeExecutionConfig(
        exec_node_func=exec_func,
        start_node_func=start_func,
        stop_node_func=stop_func,
        on_node_failure=on_failure_func,
    )

    # Return complete config
    return cls(
        name=base_config.name,
        in_ports=base_config.in_ports,
        out_ports=base_config.out_ports,
        in_salvo_conditions=base_config.in_salvo_conditions,
        out_salvo_conditions=base_config.out_salvo_conditions,
        execution_config=execution_config,
    )
```

#### `NodeGraphConfig.factory` Field

New field on `NodeGraphConfig`:

```python
class NodeGraphConfig(BaseModel):
    name: str = ""
    in_ports: dict[str, PortConfig] = {}
    out_ports: dict[str, PortConfig] = {}
    in_salvo_conditions: dict[str, SalvoConditionConfig] = {}
    out_salvo_conditions: dict[str, SalvoConditionConfig] = {}
    execution_config: NodeExecutionConfig | None = None

    # New fields for factory support
    factory: str | ModuleType | None = None
    """Factory module or import path. If set, generates base config from factory."""

    factory_args: dict[str, Any] = {}
    """Arguments passed to factory functions."""
```

**Behavior:**

When `factory` is set in the constructor, the config is built by:
1. Generating base config from factory
2. Overlaying any explicitly provided fields

```python
# Pseudo-code for __init__ behavior
def __init__(self, **data):
    if data.get('factory') is not None:
        # Generate base from factory
        base = NodeGraphConfig.from_factory(
            factory=data['factory'],
            args=data.get('factory_args', {}),
        )
        # Merge: factory output first, then explicit overrides
        data = {**base.model_dump(), **data}

    super().__init__(**data)
```

**Implementation via Pydantic validator:**

```python
class NodeGraphConfig(BaseModel):
    # ... fields ...

    @model_validator(mode="before")
    @classmethod
    def expand_factory(cls, data: dict[str, Any]) -> dict[str, Any]:
        """If factory is set, expand it and merge with provided data."""
        if not isinstance(data, dict):
            return data

        factory = data.get("factory")
        if factory is None:
            return data

        # Generate base config from factory
        factory_args = data.get("factory_args", {})
        base_config = cls.from_factory(factory=factory, args=factory_args)

        # Merge: base config, then overrides from data
        merged = base_config.model_dump()
        for key, value in data.items():
            if value is not None and key not in ("factory", "factory_args"):
                merged[key] = value

        # Keep factory/factory_args for serialization
        merged["factory"] = factory
        merged["factory_args"] = factory_args

        return merged
```

**Serialization:**

When serializing, `factory` is always converted to a string import path:

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

### Usage Examples

#### Example 1: Pure factory usage

```python
# Using from_factory directly
config = NodeGraphConfig.from_factory(
    factory="myapp.nodes.worker",
    args={"name": "Worker1", "threshold": 0.7},
)

# Using factory field
config = NodeGraphConfig(
    factory="myapp.nodes.worker",
    factory_args={"name": "Worker1", "threshold": 0.7},
)
```

#### Example 2: Factory with overrides

```python
# Override the name
config = NodeGraphConfig(
    factory="myapp.nodes.worker",
    factory_args={"name": "Worker1", "threshold": 0.7},
    name="CustomWorker",  # Overrides factory's name
)

# Override execution settings
config = NodeGraphConfig(
    factory="myapp.nodes.worker",
    factory_args={"name": "Worker1", "threshold": 0.7},
    execution_config=NodeExecutionConfig(
        # Note: This REPLACES the factory's execution_config entirely
        # You'd need to call get_node_funcs manually if you want to keep the functions
        pools=["gpu_pool"],
        retries=3,
    ),
)
```

#### Example 3: Factory with additional ports

```python
# Add extra ports (merged with factory output)
config = NodeGraphConfig(
    factory="myapp.nodes.worker",
    factory_args={"name": "Worker1"},
    out_ports={"errors": PortConfig()},  # Added to factory's out_ports
)
```

#### Example 4: Using module object

```python
import myapp.nodes.worker as worker_factory

config = NodeGraphConfig(
    factory=worker_factory,  # Module object
    factory_args={"name": "Worker1"},
)

# Serializes to JSON as:
# {"factory": "myapp.nodes.worker", "factory_args": {"name": "Worker1"}, ...}
```

#### Example 5: In GraphConfig

```python
GraphConfig(
    nodes=[
        # Regular node
        NodeGraphConfig(name="Source", ...),

        # Factory-created nodes
        NodeGraphConfig(
            factory="myapp.nodes.worker",
            factory_args={"name": "Worker1", "threshold": 0.5},
        ),
        NodeGraphConfig(
            factory="myapp.nodes.worker",
            factory_args={"name": "Worker2", "threshold": 0.7},
        ),

        # Regular node
        NodeGraphConfig(name="Sink", ...),
    ],
    edges=[...],
)
```

---

## Additional Change: Remove `NodeExecutionConfig.node_name`

### Problem

`NodeExecutionConfig.node_name` is redundant because the node name is already stored in `NodeGraphConfig.name`.

Current structure:
```python
class NodeGraphConfig:
    name: str  # Node name here
    execution_config: NodeExecutionConfig | None

class NodeExecutionConfig:
    node_name: str  # Redundant! Same as parent's name
```

### Solution

Remove `node_name` from `NodeExecutionConfig`:

```python
class NodeExecutionConfig(BaseModel):
    # node_name: str  # REMOVED

    exec_node_func: Callable | str | None = None
    start_node_func: Callable | str | None = None
    stop_node_func: Callable | str | None = None
    on_node_failure: Callable | str | None = None

    pools: list[str] = ["main"]
    # ... rest of fields ...
```

### Migration

Update all code that references `execution_config.node_name`:

1. **`Net._get_func_key()`** - Get node_name from the loop variable, not config
2. **`Net._register_node_functions()`** - Already has access to `node_config.name`
3. **`create_net_func_preprocessor()`** - Node name passed as argument, not from config
4. **Tests** - Remove `node_name` from test fixtures

---

## Implementation Checklist

### Phase 1: Remove `node_name` from `NodeExecutionConfig` ✅

- [x] Update `NodeExecutionConfig` in `pts/netrun/05_net/00_config.pct.py`
- [x] Update `Net` methods that reference `node_name` (`_get_func_key()`)
- [x] Update tests (removed `node_name` from test fixtures)
- [x] Run `nbl export -r`
- [x] Run tests to verify

### Phase 2: Add `from_factory()` class method ✅

- [x] Add `from_factory()` to `NodeGraphConfig` in `pts/netrun/05_net/00_config.pct.py`
- [x] Add tests for `from_factory()` in `src/tests/net/test_node_factory.py`
- [x] Run `nbl export -r`

### Phase 3: Add `factory` and `factory_args` fields ✅

- [x] Add fields to `NodeGraphConfig`
- [x] Add `@model_validator` for factory expansion
- [x] Add `@field_serializer` for factory serialization
- [x] Add `@field_serializer` for function fields in `NodeExecutionConfig` (handles non-serializable callables)
- [x] Update function field types to allow `None` (`Callable | str | None`)
- [x] Add tests for factory field usage
- [x] Run `nbl export -r`

### Phase 4: Documentation and examples

- [x] Created test factory module `src/tests/net/sample_factory.py`
- [ ] Create example factory module in `pts/examples/`
- [ ] Update `00_basic_net.pct.py` with factory example (optional section)
- [ ] Update `03_remaining_features.md` to mark as complete

---

## Open Questions

1. **Deep merge for execution_config?** Currently, if you override `execution_config`, it completely replaces the factory's. Should we deep-merge so you can override just `pools` while keeping the factory's `exec_node_func`?

   **Decision:** Start with shallow merge (replace). Deep merge adds complexity and users can call `from_factory()` directly for more control.

2. **Error handling:** What if `get_node_config()` returns a config with `execution_config` already set?

   **Decision:** Ignore it / overwrite with functions from `get_node_funcs()`. Log a warning.

3. **Optional `get_node_funcs()`:** Should factories be allowed to omit `get_node_funcs()` for nodes that don't execute (pure routing)?

   **Decision:** No, both functions are required. If a node doesn't need execution functions, `get_node_funcs()` should return `(None, None, None, None)`.
