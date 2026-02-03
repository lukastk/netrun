# Port Types Implementation Plan

## Overview

Port types add optional type validation for packets flowing through ports. This catches type mismatches early (at `load_output_port` time) instead of getting confusing errors deep in downstream node logic.

## Design

### Goals

1. **Catch type errors early**: Validate when loading packets into output ports
2. **Clear error messages**: Include port name, expected type, actual type, and packet ID
3. **Flexible type specification**: Support class names (strings), class objects, and isinstance checks
4. **Non-breaking**: Type checking is opt-in; ports without `port_type` skip validation
5. **Serializable**: Type specs should roundtrip through JSON

### Type Specification Options

```python
from netrun.net.config import PortConfig

# Option 1: String - checks type(value).__name__ == "DataFrame"
PortConfig(port_type="DataFrame")

# Option 2: Type object - checks isinstance(value, pd.DataFrame)
import pandas as pd
PortConfig(port_type=pd.DataFrame)

# Option 3: Dict with options
PortConfig(port_type={
    "name": "DataFrame",      # Type name to match
    "isinstance": False,      # Use name match instead of isinstance (default: True for type objects)
})
```

### When Validation Happens

**Primary validation point: `ctx.load_output_port()`**

When a node loads a packet into an output port:
1. Look up the port's type spec from `NodeGraphConfig.out_ports[port_name].port_type`
2. If type spec is set, validate the packet's value
3. Raise `PacketTypeMismatch` on failure

**Why at `load_output_port` instead of `create_packet`?**
- A packet might be created but never loaded to a port (e.g., discarded based on condition)
- The output port is where the type contract is defined
- Validation message can include the port name for better debugging

**Optional: `ctx.consume_packet()` validation**
- Could validate input port types as a double-check
- Probably redundant if output ports are validated
- Skipping for initial implementation

### API

#### PortConfig Changes

```python
class PortConfig(BaseModel):
    """Configuration for a port on a node."""
    slots_spec: PortSlotSpecConfig = Field(default_factory=PortSlotSpecInfiniteConfig)

    # NEW: Type validation
    port_type: "PortTypeSpec | None" = None
    """Expected type for packets on this port. None = no validation."""

# Type specification - supports multiple formats
PortTypeSpec = str | type | PortTypeConfig

class PortTypeConfig(BaseModel):
    """Detailed port type configuration."""
    name: str
    """Type name to match (e.g., "DataFrame", "dict", "MyClass")."""

    isinstance_check: bool = True
    """If True, use isinstance(). If False, use type().__name__ match."""
```

#### New Exception

```python
class PacketTypeMismatch(Exception):
    """Raised when a packet value doesn't match the expected port type."""

    def __init__(
        self,
        port_name: str,
        expected: str,
        actual: str,
        packet_id: str,
        node_name: str,
    ):
        self.port_name = port_name
        self.expected = expected
        self.actual = actual
        self.packet_id = packet_id
        self.node_name = node_name
        super().__init__(
            f"Port '{node_name}.{port_name}' expects {expected}, got {actual} "
            f"(packet {packet_id})"
        )
```

#### NodeExecutionContext Changes

The context needs access to port type information:

```python
@dataclass
class NodeExecutionContext:
    # ... existing fields ...

    # NEW: Port configuration for type validation
    _out_ports: dict[str, PortConfig] = field(default_factory=dict, repr=False)

    def load_output_port(self, port_name: str, packet_id: str) -> None:
        """Load a packet into an output port.

        Validates packet type if the port has a port_type configured.
        """
        # Validate type if configured
        port_config = self._out_ports.get(port_name)
        if port_config and port_config.port_type:
            # Get the packet value (need to look it up)
            value = self._get_packet_value(packet_id)
            self._validate_port_type(port_name, port_config.port_type, value, packet_id)

        self._deferred_actions.add_load_output_port(port_name, packet_id)

    def _get_packet_value(self, packet_id: str) -> Any:
        """Get the value of a packet by ID.

        Handles both input packets and deferred (created) packets.
        """
        # Check if it's an input packet
        if packet_id in self._input_packet_values:
            return self._input_packet_values[packet_id]

        # Check if it's a deferred packet we created
        return self._deferred_actions.get_deferred_value(packet_id)

    def _validate_port_type(
        self,
        port_name: str,
        port_type: PortTypeSpec,
        value: Any,
        packet_id: str,
    ) -> None:
        """Validate a value against a port type specification."""
        expected, matches = self._check_type(port_type, value)

        if not matches:
            actual = type(value).__name__
            raise PacketTypeMismatch(
                port_name=port_name,
                expected=expected,
                actual=actual,
                packet_id=packet_id,
                node_name=self.node_name,
            )

    def _check_type(self, port_type: PortTypeSpec, value: Any) -> tuple[str, bool]:
        """Check if value matches port type. Returns (expected_name, matches)."""
        if isinstance(port_type, str):
            # String: check type name
            return (port_type, type(value).__name__ == port_type)

        if isinstance(port_type, type):
            # Type object: use isinstance
            return (port_type.__name__, isinstance(value, port_type))

        if isinstance(port_type, PortTypeConfig):
            # Config object
            if port_type.isinstance_check:
                # Need to import the type - not supported for now
                # Fall back to name check
                return (port_type.name, type(value).__name__ == port_type.name)
            else:
                return (port_type.name, type(value).__name__ == port_type.name)

        # Unknown type spec - skip validation
        return ("any", True)
```

#### DeferredActionQueue Changes

Need to track packet values for validation:

```python
class DeferredActionQueue:
    # ... existing ...

    _packet_values: dict[str, Any] = field(default_factory=dict)
    """Map of deferred packet ID -> value for type validation."""

    def add_create_packet(self, value: Any) -> str:
        deferred_id = f"deferred_{uuid.uuid4().hex}"
        self._creates.append((deferred_id, value))
        self._packet_values[deferred_id] = value  # Store for validation
        return deferred_id

    def get_deferred_value(self, deferred_id: str) -> Any:
        """Get the value of a deferred packet."""
        if deferred_id not in self._packet_values:
            raise KeyError(f"Unknown packet ID: {deferred_id}")
        return self._packet_values[deferred_id]
```

### Serialization

For JSON roundtripping, type objects need to be serialized:

```python
class PortConfig(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    slots_spec: PortSlotSpecConfig = Field(default_factory=PortSlotSpecInfiniteConfig)
    port_type: str | type | PortTypeConfig | None = None

    @field_serializer("port_type")
    def serialize_port_type(self, port_type) -> str | dict | None:
        if port_type is None:
            return None
        if isinstance(port_type, str):
            return port_type
        if isinstance(port_type, type):
            # Convert type to name string (loses isinstance capability on reload)
            return port_type.__name__
        if isinstance(port_type, PortTypeConfig):
            return port_type.model_dump()
        return None
```

### Usage Examples

#### Example 1: Basic Type Checking

```python
# Define node with typed ports
NodeGraphConfig(
    name="Process",
    in_ports={
        "data": PortConfig(port_type="DataFrame"),
    },
    out_ports={
        "result": PortConfig(port_type="DataFrame"),
        "errors": PortConfig(port_type="list"),
    },
)

# In node execution
def exec_func(ctx, packets):
    df = ctx.consume_packet(packets["data"][0])  # Assumes DataFrame

    result_df = df.transform(...)
    errors = []

    # This validates that result_df is a DataFrame
    out_id = ctx.create_packet(result_df)
    ctx.load_output_port("result", out_id)  # Type check happens here

    # This validates that errors is a list
    err_id = ctx.create_packet(errors)
    ctx.load_output_port("errors", err_id)  # Type check happens here
```

#### Example 2: Type Mismatch Error

```python
def buggy_node(ctx, packets):
    data = ctx.consume_packet(packets["in"][0])

    # Bug: returning string instead of DataFrame
    result = "oops"

    out_id = ctx.create_packet(result)
    ctx.load_output_port("result", out_id)
    # Raises: PacketTypeMismatch: Port 'Process.result' expects DataFrame, got str (packet deferred_abc123)
```

#### Example 3: Using Type Objects

```python
import pandas as pd

NodeGraphConfig(
    name="DataNode",
    out_ports={
        "data": PortConfig(port_type=pd.DataFrame),  # isinstance check
    },
)

# Subclasses also pass
class MyDataFrame(pd.DataFrame):
    pass

df = MyDataFrame(...)
out_id = ctx.create_packet(df)
ctx.load_output_port("data", out_id)  # Passes: isinstance(df, pd.DataFrame) is True
```

---

## Implementation Checklist

### Phase 1: Add PortTypeConfig and PortConfig.port_type ✅

- [x] Create `PortTypeConfig` model in `pts/netrun/05_net/00_config.pct.py`
- [x] Add `port_type` field to `PortConfig`
- [x] Add `@field_serializer` for type objects
- [x] Add `PacketTypeMismatch` exception
- [x] Run `nbl export -r`

### Phase 2: Update DeferredActionQueue ✅

- [x] `_packet_values` dict already exists (stores values)
- [x] `add_create_packet` already stores values
- [x] Values accessible via `packet_values` dict
- [x] Run `nbl export -r`

### Phase 3: Update NodeExecutionContext ✅

- [x] Add `_out_ports` field
- [x] Add `_get_packet_value` method
- [x] Add `_validate_port_type` and `_check_type` methods
- [x] Update `load_output_port` to call validation
- [x] Run `nbl export -r`

### Phase 4: Update create_net_func_preprocessor ✅

- [x] Pass `out_ports` from `NodeGraphConfig` to context
- [x] Build `_node_out_ports` lookup in Net.__init__
- [x] Update context creation to include `_out_ports`
- [x] Run `nbl export -r`

### Phase 5: Tests ✅

- [x] Test `PortTypeConfig` creation and serialization
- [x] Test `PortConfig` with various `port_type` values
- [x] Test type validation success cases
- [x] Test `PacketTypeMismatch` raised correctly
- [x] Test with string types, type objects, and PortTypeConfig
- [x] Test serialization roundtrip
- [x] Tests: `src/tests/net/test_port_types.py` (29 tests)

### Phase 6: Documentation

- [ ] Update `03_remaining_features.md` to mark complete
- [ ] Add example to `00_basic_net.pct.py` (optional)

---

## Open Questions

1. **Should input ports also be validated?**

   **Decision:** Not for initial implementation. Output validation is sufficient since the type was already checked when the upstream node loaded the packet.

2. **What about lazy packet values (LazyPacketValueSpec)?**

   **Decision:** Skip validation for lazy values. The value isn't materialized until consumption, so we can't check it at `load_output_port` time. Document this limitation.

3. **Should we support union types (e.g., "DataFrame | None")?**

   **Decision:** Not for initial implementation. Keep it simple. Users can use `None` port_type to skip validation for optional outputs.

4. **What if port_name doesn't exist in out_ports?**

   **Decision:** Let it fail normally when the deferred action is committed. The type validation only happens if the port exists and has a type configured.
