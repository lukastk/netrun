# Function Node Factory

## Overview

A node factory that takes a regular Python function and automatically generates a `NodeConfig` based on the function's signature. This allows users to write simple functions and have them automatically wrapped into proper node configurations.

## Requirements

### Input Port Generation

Input ports are derived from the function's parameters:

1. **Simple type annotations** → Creates a `PortConfig` with that type as `port_type`
   ```python
   def my_func(x: int, y: str): ...
   # → in_ports = {"x": PortConfig(port_type="int"), "y": PortConfig(port_type="str")}
   ```

2. **PortConfig as annotation** → Uses that PortConfig directly
   ```python
   def my_func(x: PortConfig(port_type="int", slots=PortSlotSpecFiniteConfig(count=3))): ...
   # → in_ports = {"x": PortConfig(port_type="int", slots=...)}
   ```

3. **No annotation** → Creates a default `PortConfig()` with no type constraint

### Output Port Generation

Output ports are derived from the return type annotation:

1. **Single type annotation** → Single output port named "out" with that type
   ```python
   def my_func(x: int) -> str: ...
   # → out_ports = {"out": PortConfig(port_type="str")}
   ```

2. **Dict annotation with PortConfig values** → Multiple output ports
   ```python
   def my_func(x: int) -> {"result": PortConfig(port_type="str"), "status": PortConfig(port_type="int")}: ...
   # → out_ports = {"result": PortConfig(...), "status": PortConfig(...)}
   ```

3. **No return annotation** → No output ports (can be overridden via `_node_config`)

### Special Arguments

Certain argument names have special meaning and are not treated as input ports:

1. **`ctx: NodeExecutionContext`** → The actual `NodeExecutionContext` is passed
2. **`print`** → `ctx.print` function is passed (for captured printing)

### Configuration Override via `func._node_config`

Users can attach a `_node_config` attribute to the function to override/extend the auto-generated config:

```python
def my_func(x: int) -> str:
    return str(x)

my_func._node_config = {
    "name": "MyCustomName",
    "in_salvo_conditions": {...},
}
# Or:
my_func._node_config = NodeConfig(name="MyCustomName", ...)
# Or:
my_func._node_config = '''
[in_salvo_conditions.trigger]
max_salvos = {type = "finite", max = 2}
'''
```

The `_node_config` is **merged** with the auto-generated config (overriding where specified).

### Default Salvo Conditions

1. **Input Salvo Condition** ("trigger"):
   - Fires when all input ports have at least one packet
   - `max_salvos = 1` (fires once, creating one epoch)
   - All input ports included in the salvo

2. **Output Salvo Condition** ("send"):
   - Fires once per epoch when all output ports have at least one packet
   - `max_salvos = 1`
   - All output ports included in the salvo

### Execution Wrapper

The node factory generates a wrapper function that:

1. Extracts packet values from input ports
2. Handles special arguments (`ctx`, `print`)
3. Calls the user function with the extracted values
4. Routes the return value to output ports:
   - Single output port: return value sent directly
   - Multiple output ports: return value must be a dict mapping port names to values

## Implementation Plan

### Phase 1: Signature Parser

Create a `FunctionSignatureParser` class that:
- Extracts parameter info (name, annotation, default)
- Identifies special parameters (`ctx`, `print`)
- Extracts return annotation
- Generates `in_ports` and `out_ports` dicts

```python
@dataclass
class ParsedSignature:
    in_ports: dict[str, PortConfig]
    out_ports: dict[str, PortConfig]
    special_params: dict[str, str]  # param_name -> special_type ("ctx", "print")
    regular_params: list[str]  # ordered list of regular param names
```

### Phase 2: Config Generator

Create functions to generate the default `NodeConfig`:
- `_generate_default_salvo_conditions(in_ports, out_ports)`
- `_generate_node_config(func, parsed_sig)`

### Phase 3: Config Merger

Create a function to merge `_node_config` with auto-generated config:
- Handle `NodeConfig`, `dict`, and TOML string inputs
- Deep merge dicts (override specific fields)

### Phase 4: Execution Wrapper Generator

Create a function that generates the `exec_node_func`:
```python
def _create_exec_func(func, parsed_sig) -> Callable:
    async def exec_node_func(ctx: NodeExecutionContext):
        # Extract packet values
        kwargs = {}
        for port_name in parsed_sig.regular_params:
            packets = ctx.get_input_packets(port_name)
            # Consume and get value
            value = ctx.consume_packet(packets[0])
            kwargs[port_name] = value

        # Handle special params
        if "ctx" in parsed_sig.special_params:
            kwargs["ctx"] = ctx
        if "print" in parsed_sig.special_params:
            kwargs["print"] = ctx.print

        # Call function
        result = func(**kwargs)
        if asyncio.iscoroutine(result):
            result = await result

        # Route to output ports
        if len(parsed_sig.out_ports) == 1:
            port_name = list(parsed_sig.out_ports.keys())[0]
            packet_id = ctx.create_packet(result)
            ctx.load_output_port(port_name, packet_id)
        elif len(parsed_sig.out_ports) > 1:
            for port_name, value in result.items():
                packet_id = ctx.create_packet(value)
                ctx.load_output_port(port_name, packet_id)

        ctx.send_output_salvo("send")

    return exec_node_func
```

### Phase 5: Factory Interface

Create the main factory interface:

```python
def from_function(
    func: Callable,
    name: str | None = None,
) -> NodeConfig:
    """Create a NodeConfig from a function."""
    parsed = parse_function_signature(func)
    config = generate_node_config(func, parsed, name)

    if hasattr(func, "_node_config"):
        config = merge_config(config, func._node_config)

    return config
```

### Phase 6: Module Factory Protocol

Implement the module factory protocol so it can be used with `NodeConfig.from_factory`:

```python
# In 06_node_factories/00_function.pct.py

def get_node_config(func: Callable, name: str | None = None, **kwargs) -> NodeConfig:
    return from_function(func, name)

def get_node_funcs(func: Callable, **kwargs) -> NodeExecutionFuncs:
    parsed = parse_function_signature(func)
    return NodeExecutionFuncs(
        exec_node_func=_create_exec_func(func, parsed),
    )
```

## File Structure

```
netrun/pts/netrun/06_node_factories/
├── 00_function.pct.py      # Main function factory implementation

netrun/pts/examples/net/
├── function_factory_example/
│   ├── __init__.py         # Make it a package
│   ├── nodes.py            # Define node functions here (importable)
│   └── config.toml         # Graph config in TOML format
```

## Example Usage

### Defining Node Functions (nodes.py)

```python
from netrun.net.config import PortConfig, PortTypeConfig
from netrun.net._net import NodeExecutionContext

def add_numbers(a: int, b: int) -> int:
    """Simple function with two inputs and one output."""
    return a + b

def process_with_context(data: str, ctx: NodeExecutionContext) -> str:
    """Function that uses the execution context."""
    ctx.print(f"Processing: {data}")
    return data.upper()

def multi_output(value: int) -> {"doubled": PortConfig(port_type="int"), "tripled": PortConfig(port_type="int")}:
    """Function with multiple outputs."""
    return {"doubled": value * 2, "tripled": value * 3}

# Custom config override
def custom_node(x: int) -> int:
    return x * 10

custom_node._node_config = {
    "name": "TenXMultiplier",
}
```

### TOML Configuration (config.toml)

```toml
[net]
# Net-level config

[graph]
# Graph-level config

[[graph.nodes]]
factory = "netrun.node_factories.from_function"
factory_args = {func = "examples.net.function_factory_example.nodes.add_numbers"}

[[graph.nodes]]
factory = "netrun.node_factories.from_function"
factory_args = {func = "examples.net.function_factory_example.nodes.process_with_context"}

[[graph.edges]]
from_node = "add_numbers"
from_port = "out"
to_node = "process_with_context"
to_port = "data"
```

## Open Questions

1. **Async functions**: Should the factory detect async functions and handle them appropriately?
   - **Decision**: Yes, detect with `asyncio.iscoroutinefunction()` and await if needed

2. **Default values in function signatures**: Should they translate to optional ports?
   - **Decision**: For now, ignore defaults. All parameters become required ports.

3. **`*args` and `**kwargs`**: How to handle?
   - **Decision**: Error on `*args`/`**kwargs` - not supported

4. **Variadic ports (receiving multiple packets)**: How to specify?
   - **Decision**: Use `PortConfig(slots=PortSlotSpecFiniteConfig(count=N))` annotation

5. **Node naming**: Should default to function name or require explicit name?
   - **Decision**: Default to function's `__name__`, can override via `_node_config` or factory args

## Implementation Checklist

### Phase 1: Signature Parser
- [x] Create `ParsedSignature` dataclass
- [x] Implement `parse_function_signature()`
- [x] Handle special parameters detection
- [x] Handle PortConfig annotations
- [x] Handle return type parsing

### Phase 2: Config Generator
- [x] Implement `_generate_input_salvo_conditions()`
- [x] Implement `_generate_output_salvo_conditions()`
- [x] Implement `generate_node_config()` (via `from_function()`)

### Phase 3: Config Merger
- [x] Implement TOML parsing for `_node_config`
- [x] Implement dict merging (`_deep_merge_dicts()`)
- [x] Implement `_parse_node_config_override()`

### Phase 4: Execution Wrapper
- [x] Implement `_create_exec_func()`
- [x] Handle async functions
- [x] Handle single vs multiple outputs

### Phase 5: Factory Interface
- [x] Implement `from_function()`
- [x] Implement `get_node_config()`
- [x] Implement `get_node_funcs()`

### Phase 6: Example
- [x] Create example package structure (`src/examples/net/`)
- [x] Define example node functions (`function_factory_nodes.py`)
- [x] Create TOML config example in notebook
- [x] Create example notebook (`pts/examples/net/01_function_factory.pct.py`)

### Phase 7: Tests
- [x] Test signature parsing (in pct.py file)
- [x] Test config generation (in pct.py file)
- [x] Test config merging (in pct.py file)
- [x] Test execution wrapper (integration)
- [ ] Test end-to-end with TOML config (documented in example, not automated test)

### Phase 8: Export
- [x] Run `nbl export --pipeline "pts->nbs"`
- [x] Run `nbl export`

## Implementation Status: COMPLETE

All 573 tests pass. The function factory is implemented and working.

### Files Created/Modified

1. **`netrun/pts/netrun/06_node_factories/00_function.pct.py`** - Main implementation
2. **`netrun/src/netrun/node_factories/function.py`** - Generated module
3. **`netrun/src/netrun/node_factories/__init__.py`** - Package init
4. **`netrun/src/examples/net/function_factory_nodes.py`** - Example node functions
5. **`netrun/pts/examples/net/01_function_factory.pct.py`** - Example notebook
