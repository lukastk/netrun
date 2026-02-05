# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp net.config

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
from pydantic import BaseModel, Field, PrivateAttr, model_validator, field_serializer
from typing import Annotated, Literal, NewType, Any
from types import ModuleType
from collections.abc import Callable
import importlib
import importlib.util
import json as _json_module
import os
import sys
import tomllib
from ulid import ULID

import netrun_sim
from netrun.execution_manager import RunAllocationMethod

# %%
#|export
def _get_callable_import_path(func: Callable) -> str:
    """Get the import path for a callable (function or method).

    Args:
        func: The callable to get the import path for.

    Returns:
        The import path as "module.qualname" (e.g., "myapp.utils.process_data").

    Raises:
        ValueError: If the callable cannot be serialized (lambda, closure, __main__).
    """
    module = getattr(func, "__module__", None)
    qualname = getattr(func, "__qualname__", None)

    if module is None or qualname is None:
        raise ValueError(
            f"Cannot serialize callable {func}: missing __module__ or __qualname__"
        )

    if module == "__main__":
        raise ValueError(
            f"Cannot serialize callable '{qualname}' defined in __main__. "
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

    return f"{module}.{qualname}"


def _get_type_import_path(type_obj: type) -> str:
    """Get the import path for a type.

    Args:
        type_obj: The type to get the import path for.

    Returns:
        The import path as "module.qualname" (e.g., "pandas.DataFrame").
        For builtin types, returns just the name (e.g., "int", "str").

    Raises:
        ValueError: If the type cannot be serialized (__main__).
    """
    module = getattr(type_obj, "__module__", None)
    qualname = getattr(type_obj, "__qualname__", None)

    if module is None or qualname is None:
        raise ValueError(
            f"Cannot serialize type {type_obj}: missing __module__ or __qualname__"
        )

    if module == "__main__":
        raise ValueError(
            f"Cannot serialize type '{qualname}' defined in __main__. "
            "Move it to an importable module or use a string import path."
        )

    if module == "builtins":
        # Built-in types like int, str, list, dict - just use name
        return qualname

    return f"{module}.{qualname}"


def _is_file_path_ref(s: str) -> bool:
    """Check if string is a file-path reference (vs dotted import path).

    A string is a file-path reference if any of:
    - Contains '::' (file path + attribute separator)
    - Contains '/' or '\\' (path separator)
    - Starts with '.' (relative path like './' or '../')
    """
    return '::' in s or '/' in s or '\\' in s or s.startswith('.')


def _import_from_file_path(file_ref: str, project_root: 'Path | None' = None) -> Any:
    """Import from a file-path reference.

    - "path/to/file.py::attr" -> load file, getattr(module, attr)
    - "path/to/file.py" -> load file, return module

    Relative paths resolve from project_root (or cwd).
    """
    if '::' in file_ref:
        file_path_str, attr_name = file_ref.split('::', 1)
    else:
        file_path_str = file_ref
        attr_name = None

    file_path = Path(file_path_str)
    if not file_path.is_absolute():
        base = project_root if project_root is not None else Path(os.getcwd())
        file_path = (base / file_path).resolve()
    else:
        file_path = file_path.resolve()

    if not file_path.exists():
        raise FileNotFoundError(f"Module file not found: {file_path}")

    module_name = f"_netrun_filemod_{file_path.stem}_{hash(str(file_path)) & 0xFFFFFFFF:08x}"

    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot create module spec from: {file_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    if attr_name is not None:
        return getattr(module, attr_name)
    return module


def _import_from_path(import_path: str, project_root: 'Path | None' = None) -> Any:
    """Import an object from a dotted import path or file-path reference.

    Args:
        import_path: Dotted import path (e.g., "myapp.utils.my_function")
                     or file-path reference (e.g., "./nodes.py::my_func").
        project_root: Base path for resolving relative file paths.

    Returns:
        The imported object.

    Raises:
        ImportError: If the module cannot be imported.
        AttributeError: If the object doesn't exist in the module.
    """
    if _is_file_path_ref(import_path):
        return _import_from_file_path(import_path, project_root=project_root)
    module_path, name = import_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, name)

# %% [markdown]
# # Graph configs
#
# These Pydantic models mirror the `netrun_sim` graph types, providing a serializable
# DSL for defining flow-based networks.

# %% [markdown]
# ## Port slot specification
#
# Defines the capacity of a port (how many packets it can hold).

# %%
#|export
class PortSlotSpecInfiniteConfig(BaseModel):
    """Port can hold unlimited packets."""
    type: Literal["infinite"] = "infinite"

    def to_netrun_sim(self) -> netrun_sim.PortSlotSpec:
        return netrun_sim.PortSlotSpec.infinite()


class PortSlotSpecFiniteConfig(BaseModel):
    """Port can hold at most `capacity` packets."""
    type: Literal["finite"] = "finite"
    capacity: int

    def to_netrun_sim(self) -> netrun_sim.PortSlotSpecFinite:
        return netrun_sim.PortSlotSpec.finite(self.capacity)


PortSlotSpecConfig = Annotated[
    PortSlotSpecInfiniteConfig | PortSlotSpecFiniteConfig,
    Field(discriminator="type")
]

# %% [markdown]
# ## Port type configuration
#
# Optional type validation for packets flowing through ports.

# %%
#|export
class PortTypeConfig(BaseModel):
    """Detailed port type configuration.

    Used when you need more control than a simple type name string.
    """
    name: str
    """Type name to match (e.g., "DataFrame", "dict", "MyClass")."""

    isinstance_check: bool = False
    """If True and a type object is available, use isinstance().
    If False, use type().__name__ match. Default is False (name match)."""


# Type specification union - supports multiple formats
PortTypeSpec = str | type | PortTypeConfig
"""Port type specification.

Can be:
- str: Type name to match against type(value).__name__
- type: Type object for isinstance() check
- PortTypeConfig: Detailed configuration
"""

# %% [markdown]
# ## Port configuration

# %%
#|export
class PortConfig(BaseModel):
    """Configuration for a port on a node."""
    model_config = {"arbitrary_types_allowed": True}

    slots_spec: PortSlotSpecConfig = Field(default_factory=PortSlotSpecInfiniteConfig)

    port_type: str | type | PortTypeConfig | None = None
    """Expected type for packets on this port.

    - None: No validation (default)
    - str: Match type(value).__name__ exactly
    - type: Use isinstance(value, port_type)
    - PortTypeConfig: Detailed configuration

    Example:
        PortConfig(port_type="DataFrame")  # Match by name
        PortConfig(port_type=pd.DataFrame)  # Match with isinstance
    """

    @field_serializer("port_type", when_used='json')
    def serialize_port_type(self, port_type: str | type | PortTypeConfig | None) -> str | dict | None:
        """Serialize port_type to import path for JSON roundtrip.

        Type objects are serialized to their full import path (e.g., "pandas.DataFrame")
        to preserve isinstance capability after deserialization.

        Note: Only called during JSON serialization (model_dump_json).

        Raises:
            ValueError: If type is defined in __main__ and cannot be imported.
        """
        if port_type is None:
            return None
        if isinstance(port_type, str):
            return port_type
        if isinstance(port_type, type):
            return _get_type_import_path(port_type)
        if isinstance(port_type, PortTypeConfig):
            return port_type.model_dump()
        return None

    def to_netrun_sim(self) -> netrun_sim.Port:
        return netrun_sim.Port(self.slots_spec.to_netrun_sim())

# %% [markdown]
# ## Port state predicates
#
# Used in salvo condition terms to check the state of a port.

# %%
#|export
class PortStateEmptyConfig(BaseModel):
    """Port has zero packets."""
    type: Literal["empty"] = "empty"

    def to_netrun_sim(self) -> netrun_sim.PortState:
        return netrun_sim.PortState.empty()


class PortStateFullConfig(BaseModel):
    """Port is at capacity (always false for infinite ports)."""
    type: Literal["full"] = "full"

    def to_netrun_sim(self) -> netrun_sim.PortState:
        return netrun_sim.PortState.full()


class PortStateNonEmptyConfig(BaseModel):
    """Port has at least one packet."""
    type: Literal["non_empty"] = "non_empty"

    def to_netrun_sim(self) -> netrun_sim.PortState:
        return netrun_sim.PortState.non_empty()


class PortStateNonFullConfig(BaseModel):
    """Port is below capacity (always true for infinite ports)."""
    type: Literal["non_full"] = "non_full"

    def to_netrun_sim(self) -> netrun_sim.PortState:
        return netrun_sim.PortState.non_full()


class PortStateEqualsConfig(BaseModel):
    """Port has exactly `value` packets."""
    type: Literal["equals"] = "equals"
    value: int

    def to_netrun_sim(self) -> netrun_sim.PortStateNumeric:
        return netrun_sim.PortState.equals(self.value)


class PortStateLessThanConfig(BaseModel):
    """Port has fewer than `value` packets."""
    type: Literal["less_than"] = "less_than"
    value: int

    def to_netrun_sim(self) -> netrun_sim.PortStateNumeric:
        return netrun_sim.PortState.less_than(self.value)


class PortStateGreaterThanConfig(BaseModel):
    """Port has more than `value` packets."""
    type: Literal["greater_than"] = "greater_than"
    value: int

    def to_netrun_sim(self) -> netrun_sim.PortStateNumeric:
        return netrun_sim.PortState.greater_than(self.value)


class PortStateEqualsOrLessThanConfig(BaseModel):
    """Port has at most `value` packets."""
    type: Literal["equals_or_less_than"] = "equals_or_less_than"
    value: int

    def to_netrun_sim(self) -> netrun_sim.PortStateNumeric:
        return netrun_sim.PortState.equals_or_less_than(self.value)


class PortStateEqualsOrGreaterThanConfig(BaseModel):
    """Port has at least `value` packets."""
    type: Literal["equals_or_greater_than"] = "equals_or_greater_than"
    value: int

    def to_netrun_sim(self) -> netrun_sim.PortStateNumeric:
        return netrun_sim.PortState.equals_or_greater_than(self.value)


PortStateConfig = Annotated[
    PortStateEmptyConfig | PortStateFullConfig | PortStateNonEmptyConfig | PortStateNonFullConfig | PortStateEqualsConfig | PortStateLessThanConfig | PortStateGreaterThanConfig | PortStateEqualsOrLessThanConfig | PortStateEqualsOrGreaterThanConfig,
    Field(discriminator="type")
]

# %% [markdown]
# ## Packet count specification
#
# Specifies how many packets to take from a port in a salvo.

# %%
#|export
class PacketCountAllConfig(BaseModel):
    """Take all packets from the port."""
    type: Literal["all"] = "all"

    def to_netrun_sim(self) -> netrun_sim.PacketCount:
        return netrun_sim.PacketCount.all()


class PacketCountNConfig(BaseModel):
    """Take at most `count` packets (takes fewer if port has fewer)."""
    type: Literal["count"] = "count"
    count: int

    def to_netrun_sim(self) -> netrun_sim.PacketCountN:
        return netrun_sim.PacketCount.count(self.count)


PacketCountConfig = Annotated[
    PacketCountAllConfig | PacketCountNConfig,
    Field(discriminator="type")
]

# %% [markdown]
# ## Max salvos specification
#
# Specifies the maximum number of times a salvo condition can trigger.

# %%
#|export
class MaxSalvosInfiniteConfig(BaseModel):
    """No limit on how many times the condition can trigger."""
    type: Literal["infinite"] = "infinite"

    def to_netrun_sim(self) -> netrun_sim.MaxSalvos:
        return netrun_sim.MaxSalvos.infinite()


class MaxSalvosFiniteConfig(BaseModel):
    """Can trigger at most `max` times."""
    type: Literal["finite"] = "finite"
    max: int

    def to_netrun_sim(self) -> netrun_sim.MaxSalvosFinite:
        return netrun_sim.MaxSalvos.finite(self.max)


MaxSalvosConfig = Annotated[
    MaxSalvosInfiniteConfig | MaxSalvosFiniteConfig,
    Field(discriminator="type")
]

# %% [markdown]
# ## Salvo condition term
#
# Boolean expressions over port states, used to define when salvos can trigger.

# %%
#|export
class SalvoConditionTermTrueConfig(BaseModel):
    """Always true. Useful for source nodes with no input ports."""
    type: Literal["true"] = "true"

    def to_netrun_sim(self) -> netrun_sim.SalvoConditionTerm:
        return netrun_sim.SalvoConditionTerm.true_()


class SalvoConditionTermFalseConfig(BaseModel):
    """Always false. Useful as a placeholder or with Not."""
    type: Literal["false"] = "false"

    def to_netrun_sim(self) -> netrun_sim.SalvoConditionTerm:
        return netrun_sim.SalvoConditionTerm.false_()


class SalvoConditionTermPortConfig(BaseModel):
    """Check if a specific port matches a state predicate."""
    type: Literal["port"] = "port"
    port_name: str
    state: PortStateConfig

    def to_netrun_sim(self) -> netrun_sim.SalvoConditionTerm:
        return netrun_sim.SalvoConditionTerm.port(self.port_name, self.state.to_netrun_sim())


class SalvoConditionTermAndConfig(BaseModel):
    """All sub-terms must be true."""
    type: Literal["and"] = "and"
    terms: list["SalvoConditionTermConfig"]

    def to_netrun_sim(self) -> netrun_sim.SalvoConditionTerm:
        return netrun_sim.SalvoConditionTerm.and_([t.to_netrun_sim() for t in self.terms])


class SalvoConditionTermOrConfig(BaseModel):
    """At least one sub-term must be true."""
    type: Literal["or"] = "or"
    terms: list["SalvoConditionTermConfig"]

    def to_netrun_sim(self) -> netrun_sim.SalvoConditionTerm:
        return netrun_sim.SalvoConditionTerm.or_([t.to_netrun_sim() for t in self.terms])


class SalvoConditionTermNotConfig(BaseModel):
    """The sub-term must be false."""
    type: Literal["not"] = "not"
    term: "SalvoConditionTermConfig"

    def to_netrun_sim(self) -> netrun_sim.SalvoConditionTerm:
        return netrun_sim.SalvoConditionTerm.not_(self.term.to_netrun_sim())


SalvoConditionTermConfig = Annotated[
    SalvoConditionTermTrueConfig | SalvoConditionTermFalseConfig | SalvoConditionTermPortConfig | SalvoConditionTermAndConfig | SalvoConditionTermOrConfig | SalvoConditionTermNotConfig,
    Field(discriminator="type")
]

# Rebuild models to resolve forward references
SalvoConditionTermAndConfig.model_rebuild()
SalvoConditionTermOrConfig.model_rebuild()
SalvoConditionTermNotConfig.model_rebuild()

# %% [markdown]
# ## Salvo condition
#
# Defines when packets can trigger an epoch or be sent.

# %%
#|export
class SalvoConditionConfig(BaseModel):
    """A condition that defines when packets can trigger an epoch or be sent.

    Input salvo conditions must have max_salvos set to finite(1).
    """
    max_salvos: MaxSalvosConfig
    ports: dict[str, PacketCountConfig]
    term: SalvoConditionTermConfig

    def to_netrun_sim(self) -> netrun_sim.SalvoCondition:
        ports_dict = {name: pc.to_netrun_sim() for name, pc in self.ports.items()}
        return netrun_sim.SalvoCondition(
            max_salvos=self.max_salvos.to_netrun_sim(),
            ports=ports_dict,
            term=self.term.to_netrun_sim(),
        )

# %% [markdown]
# ## Default salvo condition generation
#
# Generate default salvo conditions when None is specified.

# %%
#|export
def _generate_default_in_salvo_conditions(
    in_ports: dict[str, PortConfig]
) -> dict[str, SalvoConditionConfig]:
    """Generate default input salvo condition.

    Default: Fires when all input ports have at least one packet.
    Takes all packets from all ports.

    Args:
        in_ports: The input port configurations.

    Returns:
        Dict with a single "default" salvo condition.
    """
    if not in_ports:
        # No input ports - use always-true condition
        return {
            "default": SalvoConditionConfig(
                max_salvos=MaxSalvosFiniteConfig(max=1),
                ports={},
                term=SalvoConditionTermTrueConfig(),
            )
        }

    # Build AND condition: all ports must be non-empty
    port_terms = [
        SalvoConditionTermPortConfig(port_name=name, state=PortStateNonEmptyConfig())
        for name in in_ports.keys()
    ]

    if len(port_terms) == 1:
        term = port_terms[0]
    else:
        term = SalvoConditionTermAndConfig(terms=port_terms)

    # Include all packets from all ports
    ports = {name: PacketCountAllConfig() for name in in_ports.keys()}

    return {
        "default": SalvoConditionConfig(
            max_salvos=MaxSalvosFiniteConfig(max=1),
            ports=ports,
            term=term,
        )
    }


def _generate_default_out_salvo_conditions(
    out_ports: dict[str, PortConfig]
) -> dict[str, SalvoConditionConfig]:
    """Generate default output salvo condition.

    Default: Fires once per epoch, sends all packets from all output ports.

    Args:
        out_ports: The output port configurations.

    Returns:
        Dict with a single "default" salvo condition, or empty dict if no output ports.
    """
    if not out_ports:
        return {}

    # Include all packets from all output ports
    ports = {name: PacketCountAllConfig() for name in out_ports.keys()}

    return {
        "default": SalvoConditionConfig(
            max_salvos=MaxSalvosFiniteConfig(max=1),
            ports=ports,
            term=SalvoConditionTermTrueConfig(),
        )
    }

# %% [markdown]
# ## Edge configuration

# %%
#|export
class PortRefConfig(BaseModel):
    """Reference to a specific port on a node."""
    node_name: str
    port_type: Literal["input", "output"]
    port_name: str

    def to_netrun_sim(self) -> netrun_sim.PortRef:
        port_type = netrun_sim.PortType.Input if self.port_type == "input" else netrun_sim.PortType.Output
        return netrun_sim.PortRef(self.node_name, port_type, self.port_name)


class EdgeConfig(BaseModel):
    """A connection between an output port and an input port.

    Can be specified as:
    - Full form: source and target PortRefConfig objects
    - Shorthand: source_str and target_str as "node.port" strings
    """
    source: PortRefConfig | None = None
    target: PortRefConfig | None = None
    # Shorthand notation: "NodeA.out" -> "NodeB.in"
    source_str: str | None = None
    target_str: str | None = None

    def model_post_init(self, __context):
        # Validate that either full form or shorthand is provided
        has_full = self.source is not None and self.target is not None
        has_short = self.source_str is not None and self.target_str is not None
        if not (has_full or has_short):
            raise ValueError("Must provide either (source, target) or (source_str, target_str)")
        if has_full and has_short:
            raise ValueError("Cannot provide both (source, target) and (source_str, target_str)")

    def _parse_port_str(self, s: str, port_type: Literal["input", "output"]) -> PortRefConfig:
        parts = s.split(".")
        if len(parts) != 2:
            raise ValueError(f"Invalid port string '{s}', expected 'NodeName.port_name'")
        return PortRefConfig(node_name=parts[0], port_type=port_type, port_name=parts[1])

    def get_source(self) -> PortRefConfig:
        if self.source is not None:
            return self.source
        return self._parse_port_str(self.source_str, "output")

    def get_target(self) -> PortRefConfig:
        if self.target is not None:
            return self.target
        return self._parse_port_str(self.target_str, "input")

    def to_netrun_sim(self) -> netrun_sim.Edge:
        return netrun_sim.Edge(
            self.get_source().to_netrun_sim(),
            self.get_target().to_netrun_sim(),
        )

# %% [markdown]
# ## Node configuration
#
# This is the node configuration, defining ports and salvo conditions, and how it is executed.
#
# The execution config can be left out when defining a node, in which case it will still be in the Net but will not executed or crash when packets are incoming.

# %% [markdown]
# ## Node execution config
#
# Runtime configuration for node execution (separate from graph structure).

# %%
#|export
PacketID = NewType("PacketID", ULID)

NodeExecutionFunc = Callable
"""
Function that executes a node.

Args:
    ctx: NodeExecutionContext
    packets: dict[str, PacketID]
"""

NodeStartFunc = Callable
"""
Function that starts a node.

Args:
    net: Net
"""

NodeStopFunc = Callable
"""
Function that stops a node.

Args:
    net: Net
"""

OnNodeFailureFunc = Callable
"""
Function that is called when a node execution fails.

Args:
    ctx: NodeFailureContext
""";

# %%
#|export
class NodeVariable(BaseModel):
    """A typed variable accessible to nodes via ctx.vars."""
    value: str
    type: str = "str"  # "str", "int", "float", "bool", "json"

    def resolve_value(self) -> Any:
        """Resolve the string value to the appropriate Python type."""
        match self.type:
            case "str" | "":
                return self.value
            case "int":
                return int(self.value)
            case "float":
                return float(self.value)
            case "bool":
                l = self.value.lower().strip()
                if l in ("true", "1", "yes"):
                    return True
                if l in ("false", "0", "no"):
                    return False
                raise ValueError(f"Cannot parse '{self.value}' as bool")
            case "json":
                return _json_module.loads(self.value)
            case _:
                raise ValueError(f"Unsupported NodeVariable type: '{self.type}'")

# %%
#|export
class NodeExecutionConfig(BaseModel):
    """Runtime configuration for a node's execution behavior."""
    model_config = {"arbitrary_types_allowed": True}

    pools: list[str] = Field(default_factory=lambda: ["main"])
    exec_node_func: NodeExecutionFunc | str | None = None
    """
    The function to execute the node with.
    If a string, it is interpreted as the import path of the function.
    """

    start_node_func: NodeStartFunc | str | None = None
    stop_node_func: NodeStopFunc | str | None = None
    on_node_failure: OnNodeFailureFunc | str | None = None

    # Additional execution options (from PROJECT_SPEC.md)
    defer_startup: bool = False
    """
    If True, the node's `start_node_func` will not be called until before the first time `start_node_func` is called.
    """

    max_parallel_epochs: int | None = None
    rate_limit_per_second: float | None = None

    defer_net_actions: bool|None = None
    """
    This must be True or None if retries are enabled. If None, then the node will defer if retires are enabled, or not if retries are not enabled.
    Deferring entails that the net will only be notified of the NetActions transpiring during a node's epochs
    (e.g. creating, consuming packets, etc) if the epoch successfully completes.
    """

    retries: int = 0
    retry_wait: float = 0.0
    timeout: float | None = None

    capture_prints: bool = True
    """
    If True, 'print' statements in the node will be captured.
    """

    print_flush_interval: float = 0.1
    """
    How often to flush the print buffer back to Net (in seconds). Default is 100ms.
    """

    print_buffer_max_size: int | None = None
    """
    Max buffer size before forced flush. None = unlimited (default).
    """

    print_echo_stdout: bool = False
    """
    If True, also print to actual stdout when ctx.print() is called.
    """

    pool_allocation_method: RunAllocationMethod | None = None
    """
    How to select a worker when node has multiple pools. None = use Net default.
    """

    node_vars: dict[str, NodeVariable] | None = None
    """Per-node variables. Override net-level vars with the same name."""

    @field_serializer("exec_node_func", "start_node_func", "stop_node_func", "on_node_failure", when_used='json')
    def serialize_func(self, func: Callable | str | None) -> str | None:
        """Serialize functions to their import path for JSON.

        Function objects are serialized to their full import path
        (e.g., "myapp.nodes.process_data") for JSON roundtripping.

        Note: Only called during JSON serialization (model_dump_json), not
        during Python serialization (model_dump). This allows factories to
        return closures that work at runtime but fail if serialized to JSON.

        Raises:
            ValueError: If function is defined in __main__, is a lambda, or is a closure.
        """
        if func is None:
            return None
        if isinstance(func, str):
            return func
        return _get_callable_import_path(func)

    def resolve(self, project_root: 'Path | None' = None) -> "NodeExecutionConfig":
        """Return a resolved copy with string import paths converted to callables.

        Args:
            project_root: Base path for resolving relative file-path references.

        Returns:
            A new NodeExecutionConfig with all string function references
            resolved to actual callable objects.
        """
        updates = {}

        for field_name in ("exec_node_func", "start_node_func", "stop_node_func", "on_node_failure"):
            value = getattr(self, field_name)
            if isinstance(value, str):
                updates[field_name] = _import_from_path(value, project_root=project_root)

        if updates:
            return self.model_copy(update=updates)
        return self

# %%
#|export
class NodeConfig(BaseModel):
    """Configuration for a node's graph structure (ports and salvo conditions).

    Can be created directly or from a factory module using the `factory` field
    or the `from_factory()` class method.

    Example with factory:
        # Using factory field
        config = NodeConfig(
            factory="myapp.nodes.worker",
            factory_args={"name": "Worker1", "threshold": 0.5},
        )

        # Using from_factory class method
        config = NodeConfig.from_factory(
            factory="myapp.nodes.worker",
            args={"name": "Worker1", "threshold": 0.5},
        )
    """
    model_config = {"arbitrary_types_allowed": True}

    type: Literal["node"] = "node"
    """Discriminator field to distinguish from SubgraphConfig."""

    name: str = ""
    in_ports: dict[str, PortConfig] = Field(default_factory=dict)
    out_ports: dict[str, PortConfig] = Field(default_factory=dict)
    in_salvo_conditions: dict[str, SalvoConditionConfig] | None = None
    """Input salvo conditions. None = generate defaults on resolve(), {} = no conditions."""
    out_salvo_conditions: dict[str, SalvoConditionConfig] | None = None
    """Output salvo conditions. None = generate defaults on resolve(), {} = no conditions."""

    execution_config: NodeExecutionConfig | None = None

    meta: dict[str, Any] = Field(default_factory=dict)
    """Arbitrary metadata for this node.

    Can be used to store UI position, custom tags, documentation, or any
    other tool-specific data that should be preserved across serialization.

    Example:
        NodeConfig(
            name="Processor",
            meta={
                "ui": {"id": "node-1", "position": {"x": 100, "y": 200}},
                "description": "Processes incoming data",
            }
        )
    """

    # Factory support
    factory: str | ModuleType | None = None
    """Factory module or import path. If set, generates base config from factory.

    The factory module must contain two functions:
    - get_node_config(**args) -> NodeConfig (without execution_config)
    - get_node_funcs(**args) -> tuple[exec_func, start_func, stop_func, on_failure_func]
    """

    factory_args: dict[str, Any] = Field(default_factory=dict)
    """Arguments passed to factory functions."""

    @field_serializer("factory", when_used='json')
    def serialize_factory(self, factory: str | ModuleType | None) -> str | None:
        """Serialize factory to import path string for JSON.

        Note: Only called during JSON serialization (model_dump_json).

        Raises:
            ValueError: If factory module is __main__.
        """
        if factory is None:
            return None
        if isinstance(factory, str):
            return factory
        # Convert module to import path
        module_name = factory.__name__
        if module_name == "__main__":
            raise ValueError(
                "Cannot serialize factory module '__main__'. "
                "Use a string import path or import the factory from a module."
            )
        return module_name

    @classmethod
    def from_factory(
        cls,
        factory: str | ModuleType,
        args: dict[str, Any] | None = None,
        name: str | None = None,
        project_root: 'Path | None' = None,
    ) -> "NodeConfig":
        """Create a NodeConfig from a factory module.

        Args:
            factory: Factory module or import path to module containing
                     get_node_config() and get_node_funcs().
            args: Arguments passed to both factory functions.
            name: Optional explicit node name. If provided, overrides the
                  factory-generated name. If None, uses the factory's default name.
            project_root: Base path for resolving relative file-path references.

        Returns:
            Complete NodeConfig with execution_config populated.

        Raises:
            ImportError: If factory module cannot be imported.
            AttributeError: If module missing get_node_config or get_node_funcs.
        """
        args = args or {}

        # Import module if string
        if isinstance(factory, str):
            if _is_file_path_ref(factory):
                module = _import_from_file_path(factory, project_root=project_root)
            else:
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

        # Use explicit name if provided, otherwise use factory's default name
        node_name = name if name is not None else base_config.name

        # Return complete config (don't set factory/factory_args here - that's for the field-based path)
        return cls.model_construct(
            name=node_name,
            in_ports=base_config.in_ports,
            out_ports=base_config.out_ports,
            in_salvo_conditions=base_config.in_salvo_conditions,
            out_salvo_conditions=base_config.out_salvo_conditions,
            execution_config=execution_config,
            meta=base_config.meta,
        )

    def resolve(self, project_root: 'Path | None' = None) -> "NodeConfig":
        """Return a resolved copy with factory expanded and imports resolved.

        If this node has a factory set, expands it to generate the full config.
        Also resolves any string import paths in execution_config to callables.

        Args:
            project_root: Base path for resolving relative file-path references.

        Returns:
            A new NodeConfig with factory expanded and functions resolved.
            If no resolution is needed, returns self.
        """
        result = self

        # If factory is set, expand it
        if self.factory is not None:
            # Import module if string
            if isinstance(self.factory, str):
                factory_path = self.factory
                if _is_file_path_ref(self.factory):
                    module = _import_from_file_path(self.factory, project_root=project_root)
                else:
                    module = importlib.import_module(self.factory)
            else:
                factory_path = self.factory.__name__
                module = self.factory

            # Get node config factory function
            get_node_config_fn = getattr(module, "get_node_config")

            # Call get_node_config only (NOT get_node_funcs - that's resolved lazily on workers)
            # This avoids creating closures that can't be pickled for multiprocess pools.
            base_config = get_node_config_fn(**self.factory_args)

            # Build execution config - exec_node_func is None because it will be
            # resolved lazily on workers using the factory info from NodeConfig
            factory_exec_config = NodeExecutionConfig(
                exec_node_func=None,
                start_node_func=None,
                stop_node_func=None,
                on_node_failure=None,
            )

            # Merge: base config first, then any explicit overrides from self
            merged_in_ports = {**base_config.in_ports, **self.in_ports}
            merged_out_ports = {**base_config.out_ports, **self.out_ports}

            # Merge salvo conditions (None means "use factory's", {} means "override with empty")
            if self.in_salvo_conditions is not None:
                merged_in_salvo = {**(base_config.in_salvo_conditions or {}), **self.in_salvo_conditions}
            else:
                merged_in_salvo = base_config.in_salvo_conditions  # Keep None or factory value

            if self.out_salvo_conditions is not None:
                merged_out_salvo = {**(base_config.out_salvo_conditions or {}), **self.out_salvo_conditions}
            else:
                merged_out_salvo = base_config.out_salvo_conditions  # Keep None or factory value

            # Use explicit name if provided, else factory name
            name = self.name if self.name else base_config.name

            # Merge execution configs if both exist
            if self.execution_config is not None:
                # Override factory exec_config with explicit fields
                exec_config_dict = factory_exec_config.model_dump()
                for field_name, value in self.execution_config.model_dump(exclude_defaults=True).items():
                    exec_config_dict[field_name] = value
                merged_exec_config = NodeExecutionConfig.model_validate(exec_config_dict)
            else:
                merged_exec_config = factory_exec_config

            # Merge meta: base config first, then explicit overrides from self
            merged_meta = {**base_config.meta, **self.meta}

            # Keep factory and factory_args in resolved config for lazy resolution on workers
            result = NodeConfig.model_construct(
                name=name,
                in_ports=merged_in_ports,
                out_ports=merged_out_ports,
                in_salvo_conditions=merged_in_salvo,
                out_salvo_conditions=merged_out_salvo,
                execution_config=merged_exec_config,
                meta=merged_meta,
                factory=factory_path,
                factory_args=self.factory_args,
            )

        # Resolve execution_config import paths
        if result.execution_config is not None:
            resolved_exec = result.execution_config.resolve(project_root=project_root)
            if resolved_exec is not result.execution_config:
                result = result.model_copy(update={"execution_config": resolved_exec})

        # Generate default salvo conditions if None
        updates = {}
        if result.in_salvo_conditions is None:
            updates["in_salvo_conditions"] = _generate_default_in_salvo_conditions(result.in_ports)
        if result.out_salvo_conditions is None:
            updates["out_salvo_conditions"] = _generate_default_out_salvo_conditions(result.out_ports)
        if updates:
            result = result.model_copy(update=updates)

        return result

    def to_netrun_sim(self) -> netrun_sim.Node:
        # Salvo conditions should be resolved before calling to_netrun_sim
        # but handle None gracefully by treating as empty dict
        in_salvos = self.in_salvo_conditions or {}
        out_salvos = self.out_salvo_conditions or {}

        return netrun_sim.Node(
            name=self.name,
            in_ports={name: port.to_netrun_sim() for name, port in self.in_ports.items()},
            out_ports={name: port.to_netrun_sim() for name, port in self.out_ports.items()},
            in_salvo_conditions={name: sc.to_netrun_sim() for name, sc in in_salvos.items()},
            out_salvo_conditions={name: sc.to_netrun_sim() for name, sc in out_salvos.items()},
        )

# %% [markdown]
# ## Exposed Port Configuration
#
# Maps an exposed port on a subgraph to an internal node's port.

# %%
#|export
class ExposedPortConfig(BaseModel):
    """Maps an exposed port to an internal node's port.

    When a subgraph exposes a port, this config defines which internal
    node and port it maps to.

    Example:
        # Expose internal node "Processor"'s "input" port as "in"
        ExposedPortConfig(
            internal_node="Processor",
            internal_port="input",
            rename="in",  # Optional: exposed name (defaults to internal_port)
        )
    """
    internal_node: str
    """Name of the internal node (within the subgraph)."""

    internal_port: str
    """Name of the port on the internal node."""

    rename: str | None = None
    """Optional exposed name. If None, uses internal_port as the exposed name."""

    def get_exposed_name(self) -> str:
        """Get the name used for the exposed port."""
        return self.rename if self.rename is not None else self.internal_port

# %% [markdown]
# ## Subgraph Configuration
#
# A subgraph is a group of nodes that acts as a single node in the parent graph.
# Subgraphs can be defined inline (with nodes and edges) or by referencing an
# external .netrun.json file.

# %%
#|export
from pathlib import Path
import json

# %%
#|export
class SubgraphConfig(BaseModel):
    """A group of nodes that acts as a single node.

    Subgraphs can be defined in two ways:
    1. Inline: nodes and edges defined directly
    2. File reference: path to external .netrun.json file

    When resolved, all internal node names are prefixed with the subgraph name
    (e.g., "subgraph.internal_node"), and edges are rewritten accordingly.

    Example (inline):
        SubgraphConfig(
            name="preprocess",
            nodes=[
                NodeConfig(name="A", ...),
                NodeConfig(name="B", ...),
            ],
            edges=[EdgeConfig(source_str="A.out", target_str="B.in")],
            exposed_in_ports={"input": ExposedPortConfig(internal_node="A", internal_port="in")},
            exposed_out_ports={"output": ExposedPortConfig(internal_node="B", internal_port="out")},
        )

    Example (file reference):
        SubgraphConfig(
            name="preprocess",
            path="./subgraphs/preprocess.netrun.json",
            exposed_in_ports={"input": ExposedPortConfig(internal_node="A", internal_port="in")},
            exposed_out_ports={"output": ExposedPortConfig(internal_node="B", internal_port="out")},
        )
    """
    model_config = {"arbitrary_types_allowed": True}

    type: Literal["subgraph"] = "subgraph"
    """Discriminator field to distinguish from NodeConfig."""

    name: str
    """Name of this subgraph in the parent graph."""

    # Either inline OR file reference (not both)
    nodes: list["NodeConfig | SubgraphConfig"] | None = None
    """Inline nodes (mutually exclusive with path)."""

    edges: list[EdgeConfig] = Field(default_factory=list)
    """Internal edges between nodes in this subgraph."""

    path: str | None = None
    """Path to external .netrun.json file (mutually exclusive with nodes)."""

    # Exposed ports
    exposed_in_ports: dict[str, ExposedPortConfig] = Field(default_factory=dict)
    """Input ports exposed to the parent graph."""

    exposed_out_ports: dict[str, ExposedPortConfig] = Field(default_factory=dict)
    """Output ports exposed to the parent graph."""

    meta: dict[str, Any] = Field(default_factory=dict)
    """Arbitrary metadata for this subgraph."""

    @model_validator(mode='after')
    def validate_inline_or_path(self) -> "SubgraphConfig":
        """Validate that either inline nodes or path is provided, not both."""
        has_inline = self.nodes is not None
        has_path = self.path is not None

        if has_inline and has_path:
            raise ValueError(
                f"SubgraphConfig '{self.name}': cannot specify both 'nodes' and 'path'. "
                "Use either inline definition or file reference."
            )
        if not has_inline and not has_path:
            raise ValueError(
                f"SubgraphConfig '{self.name}': must specify either 'nodes' (inline) or 'path' (file reference)."
            )
        return self

    def _load_from_file(self, base_path: Path | None = None) -> tuple[list["NodeConfig | SubgraphConfig"], list[EdgeConfig]]:
        """Load nodes and edges from external file.

        Args:
            base_path: Base path for resolving relative file paths.

        Returns:
            Tuple of (nodes, edges) from the external file.

        Raises:
            FileNotFoundError: If the file doesn't exist.
            ValueError: If the file doesn't contain valid graph config.
        """
        if self.path is None:
            raise ValueError("No path specified for file-based subgraph")

        file_path = Path(self.path)
        if not file_path.is_absolute() and base_path is not None:
            file_path = base_path / file_path

        if not file_path.exists():
            raise FileNotFoundError(f"Subgraph file not found: {file_path}")

        with open(file_path) as f:
            data = json.load(f)

        # The file should contain a GraphConfig or NetConfig structure
        # We extract just the graph portion
        if "graph" in data:
            # NetConfig format
            graph_data = data["graph"]
        else:
            # GraphConfig format
            graph_data = data

        if "nodes" not in graph_data:
            raise ValueError(f"Subgraph file {file_path} must contain 'nodes' field")

        # Parse nodes - need to handle both NodeConfig and SubgraphConfig
        nodes = []
        for node_data in graph_data.get("nodes", []):
            if node_data.get("type") == "subgraph":
                nodes.append(SubgraphConfig.model_validate(node_data))
            else:
                nodes.append(NodeConfig.model_validate(node_data))

        edges = [EdgeConfig.model_validate(e) for e in graph_data.get("edges", [])]

        return nodes, edges

    def resolve(
        self,
        base_path: Path | None = None,
        _seen_paths: set[str] | None = None,
    ) -> tuple[list["NodeConfig"], list[EdgeConfig], dict[str, str], dict[str, str]]:
        """Resolve this subgraph to flat nodes and edges with prefixed names.

        Args:
            base_path: Base path for resolving relative file paths.
            _seen_paths: Set of already-seen file paths (for circular reference detection).

        Returns:
            Tuple of:
            - nodes: List of resolved NodeConfig with prefixed names
            - edges: List of EdgeConfig with prefixed node references
            - in_port_mapping: Dict mapping exposed port name to "prefixed_node.port"
            - out_port_mapping: Dict mapping exposed port name to "prefixed_node.port"

        Raises:
            ValueError: If circular reference detected or validation fails.
        """
        _seen_paths = _seen_paths or set()

        # Detect circular references for file-based subgraphs
        if self.path is not None:
            abs_path = str(Path(self.path).resolve() if Path(self.path).is_absolute()
                          else (base_path / self.path).resolve() if base_path else Path(self.path).resolve())
            if abs_path in _seen_paths:
                raise ValueError(f"Circular subgraph reference detected: {abs_path}")
            _seen_paths = _seen_paths | {abs_path}

        # Get nodes and edges (from inline or file)
        if self.nodes is not None:
            nodes_to_resolve = self.nodes
            edges_to_resolve = self.edges
        else:
            nodes_to_resolve, edges_to_resolve = self._load_from_file(base_path)

        # Resolve nested subgraphs and flatten
        resolved_nodes: list[NodeConfig] = []
        # Track name mappings for nested subgraphs
        nested_in_mappings: dict[str, dict[str, str]] = {}
        nested_out_mappings: dict[str, dict[str, str]] = {}

        for node in nodes_to_resolve:
            if isinstance(node, SubgraphConfig):
                # Recursively resolve nested subgraph
                nested_nodes, nested_edges, nested_in_map, nested_out_map = node.resolve(
                    base_path=base_path,
                    _seen_paths=_seen_paths,
                )
                resolved_nodes.extend(nested_nodes)
                edges_to_resolve = list(edges_to_resolve) + nested_edges
                nested_in_mappings[node.name] = nested_in_map
                nested_out_mappings[node.name] = nested_out_map
            else:
                resolved_nodes.append(node)

        # Prefix all node names with subgraph name
        prefixed_nodes: list[NodeConfig] = []
        for node in resolved_nodes:
            prefixed_name = f"{self.name}.{node.name}"
            prefixed_node = node.model_copy(update={"name": prefixed_name})
            prefixed_nodes.append(prefixed_node)

        # Rewrite edges with prefixed names
        prefixed_edges: list[EdgeConfig] = []
        for edge in edges_to_resolve:
            source = edge.get_source()
            target = edge.get_target()

            # Check if source/target refers to a nested subgraph's exposed port
            source_node = source.node_name
            target_node = target.node_name

            # Handle nested subgraph port references
            if source_node in nested_out_mappings:
                # Source is a nested subgraph - map to actual internal port
                mapping = nested_out_mappings[source_node]
                if source.port_name in mapping:
                    mapped = mapping[source.port_name]
                    node_name, port_name = mapped.rsplit(".", 1)
                    source_node = node_name
                    source = PortRefConfig(node_name=source_node, port_type="output", port_name=port_name)

            if target_node in nested_in_mappings:
                # Target is a nested subgraph - map to actual internal port
                mapping = nested_in_mappings[target_node]
                if target.port_name in mapping:
                    mapped = mapping[target.port_name]
                    node_name, port_name = mapped.rsplit(".", 1)
                    target_node = node_name
                    target = PortRefConfig(node_name=target_node, port_type="input", port_name=port_name)

            prefixed_edge = EdgeConfig(
                source=PortRefConfig(
                    node_name=f"{self.name}.{source.node_name}",
                    port_type=source.port_type,
                    port_name=source.port_name,
                ),
                target=PortRefConfig(
                    node_name=f"{self.name}.{target.node_name}",
                    port_type=target.port_type,
                    port_name=target.port_name,
                ),
            )
            prefixed_edges.append(prefixed_edge)

        # Build port mappings for exposed ports
        in_port_mapping: dict[str, str] = {}
        for exposed_name, config in self.exposed_in_ports.items():
            internal_node = config.internal_node
            # Check if internal_node is a nested subgraph
            if internal_node in nested_in_mappings:
                mapping = nested_in_mappings[internal_node]
                if config.internal_port in mapping:
                    # Map through nested subgraph
                    mapped = mapping[config.internal_port]
                    in_port_mapping[exposed_name] = f"{self.name}.{mapped}"
                    continue
            in_port_mapping[exposed_name] = f"{self.name}.{internal_node}.{config.internal_port}"

        out_port_mapping: dict[str, str] = {}
        for exposed_name, config in self.exposed_out_ports.items():
            internal_node = config.internal_node
            # Check if internal_node is a nested subgraph
            if internal_node in nested_out_mappings:
                mapping = nested_out_mappings[internal_node]
                if config.internal_port in mapping:
                    # Map through nested subgraph
                    mapped = mapping[config.internal_port]
                    out_port_mapping[exposed_name] = f"{self.name}.{mapped}"
                    continue
            out_port_mapping[exposed_name] = f"{self.name}.{internal_node}.{config.internal_port}"

        return prefixed_nodes, prefixed_edges, in_port_mapping, out_port_mapping


# Need to rebuild models for forward references
SubgraphConfig.model_rebuild()

# %% [markdown]
# ## Graph configuration
#
# The complete graph topology configuration.

# %%
#|export
from pydantic import BeforeValidator

def _parse_graph_node(value: Any) -> NodeConfig | SubgraphConfig:
    """Parse a graph node, defaulting to NodeConfig when type is not specified."""
    if isinstance(value, (NodeConfig, SubgraphConfig)):
        return value
    if isinstance(value, dict):
        # Check if it's explicitly a subgraph
        if value.get("type") == "subgraph":
            return SubgraphConfig.model_validate(value)
        # Default to NodeConfig (handles both "node" and missing type)
        return NodeConfig.model_validate(value)
    raise ValueError(f"Invalid node config: {value}")

# Type alias for nodes that can be either regular nodes or subgraphs
# Uses BeforeValidator to default to NodeConfig when type is not specified
GraphNodeConfig = Annotated[
    NodeConfig | SubgraphConfig,
    BeforeValidator(_parse_graph_node)
]

# %%
#|export
class GraphConfig(BaseModel):
    """Configuration for a complete flow-based network graph.

    Example:
        config = GraphConfig(
            nodes=[
                NodeConfig(
                    name="A",
                    out_ports={"out": PortConfig()},
                ),
                NodeConfig(
                    name="B",
                    in_ports={"in": PortConfig()},
                    in_salvo_conditions={
                        "default": SalvoConditionConfig(
                            max_salvos=MaxSalvosFiniteConfig(max=1),
                            ports={"in": PacketCountAllConfig()},
                            term=SalvoConditionTermPortConfig(
                                port_name="in",
                                state=PortStateNonEmptyConfig(),
                            ),
                        ),
                    },
                ),
            ],
            edges=[
                EdgeConfig(source_str="A.out", target_str="B.in"),
            ],
        )
        graph = config.get_graph()
    """
    nodes: list[GraphNodeConfig]
    """List of nodes (NodeConfig) and/or subgraphs (SubgraphConfig)."""

    edges: list[EdgeConfig] = Field(default_factory=list)

    meta: dict[str, Any] = Field(default_factory=dict)
    """Arbitrary metadata for the graph.

    Can be used to store descriptions, UI viewport state, or any other
    tool-specific data that should be preserved across serialization.
    """

    def resolve(self, base_path: Path | None = None, project_root: 'Path | None' = None) -> "GraphConfig":
        """Return a resolved copy with all subgraphs flattened and nodes resolved.

        This method:
        1. Flattens all SubgraphConfig into NodeConfig with prefixed names
        2. Rewrites edges to use the prefixed names
        3. Adds edges connecting exposed ports to parent graph edges
        4. Resolves all node factories and import paths
        5. Validates no name collisions exist

        Args:
            base_path: Base path for resolving relative file paths in subgraphs.
            project_root: Base path for resolving relative file-path references
                          in factories and function imports.

        Returns:
            A new GraphConfig with only NodeConfig (no SubgraphConfig) and all
            edges properly connected.

        Raises:
            ValueError: If name collisions are detected after resolution.
        """
        resolved_nodes: list[NodeConfig] = []
        resolved_edges: list[EdgeConfig] = list(self.edges)

        # Track subgraph port mappings for edge rewriting
        subgraph_in_mappings: dict[str, dict[str, str]] = {}
        subgraph_out_mappings: dict[str, dict[str, str]] = {}

        # First pass: resolve subgraphs and collect regular nodes
        for node in self.nodes:
            if isinstance(node, SubgraphConfig):
                # Resolve subgraph to flat nodes and edges
                sg_nodes, sg_edges, in_mapping, out_mapping = node.resolve(base_path=base_path)
                resolved_nodes.extend(sg_nodes)
                resolved_edges.extend(sg_edges)
                subgraph_in_mappings[node.name] = in_mapping
                subgraph_out_mappings[node.name] = out_mapping
            else:
                # Regular node - resolve factories
                resolved_nodes.append(node.resolve(project_root=project_root))

        # Second pass: rewrite edges that connect to subgraph exposed ports
        final_edges: list[EdgeConfig] = []
        for edge in resolved_edges:
            source = edge.get_source()
            target = edge.get_target()

            new_source = source
            new_target = target

            # Check if source refers to a subgraph's exposed output port
            if source.node_name in subgraph_out_mappings:
                mapping = subgraph_out_mappings[source.node_name]
                if source.port_name in mapping:
                    # Map to internal node.port
                    mapped = mapping[source.port_name]
                    internal_node, internal_port = mapped.rsplit(".", 1)
                    new_source = PortRefConfig(
                        node_name=internal_node,
                        port_type="output",
                        port_name=internal_port,
                    )

            # Check if target refers to a subgraph's exposed input port
            if target.node_name in subgraph_in_mappings:
                mapping = subgraph_in_mappings[target.node_name]
                if target.port_name in mapping:
                    # Map to internal node.port
                    mapped = mapping[target.port_name]
                    internal_node, internal_port = mapped.rsplit(".", 1)
                    new_target = PortRefConfig(
                        node_name=internal_node,
                        port_type="input",
                        port_name=internal_port,
                    )

            final_edges.append(EdgeConfig(source=new_source, target=new_target))

        # Validate no name collisions
        node_names = [n.name for n in resolved_nodes]
        seen_names: set[str] = set()
        for name in node_names:
            if name in seen_names:
                raise ValueError(f"Node name collision after subgraph resolution: '{name}'")
            # Also check for prefix conflicts
            for seen in seen_names:
                if name.startswith(f"{seen}.") or seen.startswith(f"{name}."):
                    raise ValueError(
                        f"Node name prefix collision: '{name}' conflicts with '{seen}'. "
                        "Node names cannot be prefixes of other node names when using subgraphs."
                    )
            seen_names.add(name)

        return GraphConfig(nodes=resolved_nodes, edges=final_edges, meta=self.meta)

    def has_subgraphs(self) -> bool:
        """Check if this graph contains any subgraphs."""
        return any(isinstance(node, SubgraphConfig) for node in self.nodes)

    def get_graph(self) -> netrun_sim.Graph:
        """Convert this config to a netrun_sim.Graph object.

        Raises:
            ValueError: If graph contains SubgraphConfig nodes. Call resolve() first.
        """
        if self.has_subgraphs():
            raise ValueError(
                "Cannot convert GraphConfig with subgraphs to netrun_sim.Graph. "
                "Call resolve() first to flatten subgraphs."
            )
        # At this point, all nodes are NodeConfig
        nodes = [node.to_netrun_sim() for node in self.nodes]  # type: ignore
        edges = [edge.to_netrun_sim() for edge in self.edges]
        return netrun_sim.Graph(nodes, edges)

# %% [markdown]
# # Net configuation

# %% [markdown]
# ## Output Queue Configuration
#
# For DAG-style workflows, packets flow from source nodes through processing nodes to sink nodes.
# The sink nodes' output ports are typically unconnected (no downstream edges).
# Output queues provide a clean way to collect these outputs.

# %%
#|export
class OutputQueueConfig(BaseModel):
    """Configuration for an output queue.

    Output queues collect packets that are sent from unconnected output ports
    (ports with no downstream edges). This enables DAG-style workflows where
    final results are collected from sink nodes.
    """

    ports: list[tuple[str, str]]
    """List of (node_name, port_name) tuples that feed this queue."""

# %% [markdown]
# ## Pool configs

# %%
#|export
class MainPoolConfig(BaseModel):
    """Configuration for running in the main thread/event loop."""
    type: Literal["main"] = "main"


class ThreadPoolConfig(BaseModel):
    """Configuration for a thread pool."""
    type: Literal["thread"] = "thread"
    num_workers: int = 1


class MultiprocessPoolConfig(BaseModel):
    """Configuration for a multiprocess pool."""
    type: Literal["multiprocess"] = "multiprocess"
    num_processes: int = 1
    threads_per_process: int = 1


class RemotePoolConfig(BaseModel):
    """Configuration for a remote pool."""
    type: Literal["remote"] = "remote"
    url: str
    worker_name: str
    num_processes: int = 1
    threads_per_process: int = 1


PoolSpecConfig = Annotated[
    MainPoolConfig | ThreadPoolConfig | MultiprocessPoolConfig | RemotePoolConfig,
    Field(discriminator="type")
]


class PoolConfig(BaseModel):
    """Configuration for a pool of workers."""
    print_flush_interval: float = 0.1
    capture_prints: bool = True
    spec: PoolSpecConfig = Field(default_factory=MainPoolConfig)

# %% [markdown]
# ## The Net configuration class

# %%
#|export
def _default_pools() -> dict[str, "PoolConfig"]:
    """Create default pools with a main thread pool."""
    return {"main": PoolConfig(spec=MainPoolConfig())}


class NetConfig(BaseModel):
    """Configuration for a Net."""
    model_config = {"arbitrary_types_allowed": True}

    project_root: str | None = None
    """Project root path. Relative paths resolve from the config file's directory."""

    _file_path: Path | None = PrivateAttr(default=None)

    @property
    def project_root_path(self) -> Path:
        """Return the resolved project root as an absolute Path.

        Resolution order:
        - If project_root is set and absolute, return it directly.
        - If project_root is set and relative, resolve from _file_path.parent (or cwd).
        - If project_root is None and _file_path is set, return _file_path.parent.
        - If both are None, return cwd.
        """
        if self.project_root is not None:
            p = Path(self.project_root)
            if p.is_absolute():
                return p
            # Relative: resolve from config file dir or cwd
            base = self._file_path.parent if self._file_path is not None else Path(os.getcwd())
            return (base / p).resolve()
        if self._file_path is not None:
            return self._file_path.parent
        return Path(os.getcwd())

    @classmethod
    def from_file(cls, path: str | Path) -> "NetConfig":
        """Load a NetConfig from a JSON or TOML file.

        Args:
            path: Path to the config file (.json or .toml).

        Returns:
            A NetConfig with _file_path set to the resolved absolute path.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file extension is not .json or .toml.
        """
        path = Path(path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        content = path.read_text()
        suffix = path.suffix.lower()

        if suffix == ".json":
            data = json.loads(content)
        elif suffix == ".toml":
            data = tomllib.loads(content)
        else:
            raise ValueError(f"Unsupported config file format: {suffix}. Use .json or .toml.")

        config = cls.model_validate(data)
        config._file_path = path
        return config

    pools: dict[str, PoolConfig] | None = None
    """Pool configurations. None = generate default on resolve(), {} = no pools."""
    graph: GraphConfig

    meta: dict[str, Any] = Field(default_factory=dict)
    """Arbitrary metadata for the net configuration.

    Can be used to store descriptions, version info, or any other
    tool-specific data that should be preserved across serialization.
    """

    default_pool_allocation_method: RunAllocationMethod = RunAllocationMethod.ROUND_ROBIN
    """
    Default allocation method for nodes with multiple pools.
    """

    node_vars: dict[str, NodeVariable] | None = None
    """Global default node variables, accessible via ctx.vars."""

    dead_letter_queue: bool = True
    dead_letter_path: str | None = None
    dead_letter_callback: Callable | str | None = None

    # Output queue configuration
    output_queues: dict[str, OutputQueueConfig] = {}
    """
    Map of queue_name -> OutputQueueConfig.

    Example:
        output_queues={
            "results": OutputQueueConfig(ports=[("Sink", "out")]),
            "logs": OutputQueueConfig(ports=[("Logger", "out"), ("ErrorLogger", "out")]),
        }
    """

    catch_all_output_queue: str | None = None
    """
    If set, packets from unconnected output ports that aren't in any
    configured queue go to this queue. If None, they are silently discarded.

    Example: catch_all_output_queue="_uncategorized"
    """

    undeclared_output_behavior: Literal["discard", "error"] = "discard"
    """
    What to do with packets from unconnected ports not in any queue:
    - "discard": Silently discard (default)
    - "error": Raise an error (original behavior)
    """

    @field_serializer("dead_letter_callback", when_used='json')
    def serialize_dead_letter_callback(self, callback: Callable | str | None) -> str | None:
        """Serialize dead_letter_callback to import path for JSON.

        Note: Only called during JSON serialization (model_dump_json).

        Raises:
            ValueError: If callback is defined in __main__, is a lambda, or is a closure.
        """
        if callback is None:
            return None
        if isinstance(callback, str):
            return callback
        return _get_callable_import_path(callback)

    @model_validator(mode='after')
    def validate_single_main_pool(self) -> "NetConfig":
        """Validate that at most one pool has type 'main'."""
        if self.pools is None:
            return self
        main_pools = [name for name, cfg in self.pools.items() if cfg.spec.type == "main"]
        if len(main_pools) > 1:
            raise ValueError(
                f"Only one pool may have type 'main' (main thread), "
                f"but found {len(main_pools)}: {', '.join(repr(n) for n in main_pools)}"
            )
        return self

    def resolve(self, base_path: Path | None = None) -> "NetConfig":
        """Return a resolved copy with all factories and imports resolved.

        Resolves:
        - All subgraphs in the graph (flattening to regular nodes)
        - All node factories in the graph
        - All string import paths to callables
        - dead_letter_callback if it's a string
        - pools if None (generates default main pool)

        Args:
            base_path: Base path for resolving relative file paths in subgraphs.
                       If None, auto-derived from _file_path.parent when available.

        Returns:
            A new NetConfig ready for execution by Net.
        """
        # Auto-derive base_path from _file_path when not provided
        if base_path is None and self._file_path is not None:
            base_path = self._file_path.parent

        updates = {}

        # Generate default pools if None
        if self.pools is None:
            updates["pools"] = _default_pools()

        # Resolve graph (includes subgraph flattening)
        project_root = self.project_root_path
        resolved_graph = self.graph.resolve(base_path=base_path, project_root=project_root)
        if resolved_graph is not self.graph:
            updates["graph"] = resolved_graph

        # Resolve dead_letter_callback
        if isinstance(self.dead_letter_callback, str):
            updates["dead_letter_callback"] = _import_from_path(self.dead_letter_callback, project_root=project_root)

        if updates:
            result = self.model_copy(update=updates)
            # Pydantic v2 model_copy() does not copy PrivateAttr - preserve it
            result._file_path = self._file_path
            return result
        return self

# %% [markdown]
# # Examples

# %% [markdown]
# ## Simple A -> B graph

# %%
# Create a simple graph with two nodes: A (source) -> B (sink)
config = GraphConfig(
    nodes=[
        NodeConfig(
            name="A",
            out_ports={"out": PortConfig()},
        ),
        NodeConfig(
            name="B",
            in_ports={"in": PortConfig()},
            in_salvo_conditions={
                "default": SalvoConditionConfig(
                    max_salvos=MaxSalvosFiniteConfig(max=1),
                    ports={"in": PacketCountAllConfig()},
                    term=SalvoConditionTermPortConfig(
                        port_name="in",
                        state=PortStateNonEmptyConfig(),
                    ),
                ),
            },
        ),
    ],
    edges=[
        EdgeConfig(source_str="A.out", target_str="B.in"),
    ],
)

# Convert to netrun_sim.Graph
graph = config.get_graph()
print(f"Graph: {graph}")
print(f"Nodes: {list(graph.nodes().keys())}")
print(f"Edges: {graph.edges()}")

# Validate the graph
errors = graph.validate()
print(f"Validation errors: {len(errors)}")

# %% [markdown]
# ## JSON serialization

# %%
# GraphConfig is fully serializable
json_str = config.model_dump_json(indent=2)
print(json_str[:500] + "...")

# %%
# Deserialize from JSON
config_loaded = GraphConfig.model_validate_json(json_str)
graph_loaded = config_loaded.get_graph()
print(f"Loaded graph: {graph_loaded}")

# %% [markdown]
# ## Complex salvo conditions

# %%
# Example with AND/OR logic in salvo conditions
complex_config = GraphConfig(
    nodes=[
        NodeConfig(
            name="Source",
            out_ports={"out": PortConfig()},
        ),
        NodeConfig(
            name="Processor",
            in_ports={
                "in1": PortConfig(slots_spec=PortSlotSpecFiniteConfig(capacity=5)),
                "in2": PortConfig(),
            },
            out_ports={"out": PortConfig()},
            in_salvo_conditions={
                # Trigger when both input ports have packets
                "both_ready": SalvoConditionConfig(
                    max_salvos=MaxSalvosFiniteConfig(max=1),
                    ports={
                        "in1": PacketCountAllConfig(),
                        "in2": PacketCountAllConfig(),
                    },
                    term=SalvoConditionTermAndConfig(
                        terms=[
                            SalvoConditionTermPortConfig(
                                port_name="in1",
                                state=PortStateNonEmptyConfig(),
                            ),
                            SalvoConditionTermPortConfig(
                                port_name="in2",
                                state=PortStateNonEmptyConfig(),
                            ),
                        ]
                    ),
                ),
            },
            out_salvo_conditions={
                "send": SalvoConditionConfig(
                    max_salvos=MaxSalvosInfiniteConfig(),
                    ports={"out": PacketCountAllConfig()},
                    term=SalvoConditionTermPortConfig(
                        port_name="out",
                        state=PortStateNonEmptyConfig(),
                    ),
                ),
            },
        ),
        NodeConfig(
            name="Sink",
            in_ports={"in": PortConfig()},
            in_salvo_conditions={
                "default": SalvoConditionConfig(
                    max_salvos=MaxSalvosFiniteConfig(max=1),
                    ports={"in": PacketCountAllConfig()},
                    term=SalvoConditionTermPortConfig(
                        port_name="in",
                        state=PortStateNonEmptyConfig(),
                    ),
                ),
            },
        ),
    ],
    edges=[
        EdgeConfig(source_str="Source.out", target_str="Processor.in1"),
        EdgeConfig(source_str="Processor.out", target_str="Sink.in"),
    ],
)

complex_graph = complex_config.get_graph()
print(f"Complex graph nodes: {list(complex_graph.nodes().keys())}")
print(f"Validation errors: {len(complex_graph.validate())}")
