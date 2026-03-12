# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp node_factories.from_function

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();

# %% [markdown]
# # Function Node Factory
#
# A node factory that automatically generates a `NodeConfig` from a regular Python function.
# The function's signature is parsed to determine input and output ports.

# %%
#|export
from typing import Callable, Any, get_type_hints, get_origin
from dataclasses import dataclass, field
import inspect
import asyncio
import tomllib
import importlib
import os

from netrun.net.config import (
    NodeConfig,
    NodeExecutionConfig,
    PortConfig,
    PortTypeConfig,
    SalvoConditionConfig,
    SalvoConditionTermConfig,
    SalvoConditionTermPortConfig,
    SalvoConditionTermAndConfig,
    SalvoConditionTermTrueConfig,
    MaxSalvosFiniteConfig,
    PacketCountAllConfig,
    PacketCountNConfig,
    PortStateNonEmptyConfig,
)

# %% [markdown]
# ## Batch Annotation
#
# Annotation for input ports that collect multiple packets into a list.

# %%
#|export
@dataclass
class Batch:
    """Input-annotation wrapper marking a port as collecting multiple packets.

    When a parameter is annotated with ``Batch``, the factory generates a salvo
    condition that grabs multiple packets from that port and passes them to the
    function as a ``list``.  This cleanly separates **flow control** (how many
    packets to collect) from **type declarations** (what type each packet is).

    Without ``Batch``, each input port consumes exactly **one** packet per
    epoch and the function receives the packet's value directly.  With
    ``Batch``, the port's salvo entry uses ``PacketCountAllConfig`` (grab all
    available packets) or ``PacketCountNConfig(count=N)`` (grab at most N),
    and the function always receives a ``list`` of values — even if only one
    packet was available.

    Args:
        port_type: The type of **each individual packet** on the port.
            Can be a type, string, ``PortConfig``, or ``None``.
        count: How many packets to collect per epoch.

            - ``None`` (default): collect **all** available packets
              (maps to ``PacketCountAllConfig``).
            - ``int``: collect at most *count* packets
              (maps to ``PacketCountNConfig(count=N)``).

    Examples::

        from netrun.node_factories.from_function import Batch

        # Collect all available string packets into a list:
        def process_all(items: Batch(str)) -> str:
            return ", ".join(items)   # items is list[str]

        # Collect at most 5 int packets:
        def process_batch(nums: Batch(int, count=5)) -> float:
            return sum(nums) / len(nums)  # nums is list[int], len <= 5

        # list[int] is NOT batch — it's a single packet whose value is a list:
        def process_list(ids: list[int]) -> int:
            return len(ids)  # ids is list[int], a single packet value
    """

    port_type: Any = None
    count: int | None = None

# %% [markdown]
# ## PreCreatedPacket Annotation

# %%
#|export
@dataclass
class PreCreatedPacket:
    """Return-annotation wrapper marking an output port as receiving a pre-created packet ID.

    Normally, ``from_function`` takes each return value, wraps it in a new
    packet (via ``ctx.create_packet(value)``), and loads it into the output
    port.  When a port is annotated with ``PreCreatedPacket``, the factory
    skips packet creation and treats the returned value as an **existing
    packet ID** — one that the function already created via
    ``ctx.create_packet()`` or ``ctx.create_packet_from_value_func()``.

    Args:
        port_type: The type of the packet value (for runtime type checking).
            Can be a type, string, ``PortConfig``, or ``None``.

    Example::

        from netrun.node_factories.from_function import PreCreatedPacket

        def my_func(data: str, ctx) -> {"out": str, "lazy": PreCreatedPacket(str)}:
            pid = ctx.create_packet_from_value_func(
                "mymod.expensive_compute", args=(data,)
            )
            return {"out": f"processed {data}", "lazy": pid}

        # Single output port (all returns are packet IDs):
        def make_lazy(data: str, ctx) -> PreCreatedPacket(str):
            return ctx.create_packet_from_value_func(
                "mymod.compute", args=(data,)
            )
    """

    port_type: Any = None

# %% [markdown]
# ## Signature Parser
#
# Parse a function's signature to extract input/output port configurations.

# %%
#|exporti
# Sentinel to detect unannotated parameters
_MISSING = object()

# Special parameter names that are not treated as input ports
_SPECIAL_PARAMS = {"ctx", "print", "log"}


@dataclass
class _ParsedSignature:
    """Result of parsing a function signature."""

    in_ports: dict[str, PortConfig]
    """Input port configurations derived from function parameters."""

    out_ports: dict[str, PortConfig]
    """Output port configurations derived from return annotation."""

    special_params: set[str]
    """Special parameters (ctx, print) that need special handling."""

    regular_params: list[str]
    """Ordered list of regular parameter names (input ports)."""

    batch_ports: dict[str, int | None] = field(default_factory=dict)
    """Input ports annotated with Batch.

    Maps port name to count (int for at-most-N, None for all).
    Ports not in this dict consume exactly 1 packet (scalar).
    """

    packet_ports: set[str] = field(default_factory=set)
    """Output ports annotated with PreCreatedPacket (receive packet IDs, not values)."""


def _annotation_to_port_config(annotation: Any, include_type: bool = True) -> PortConfig:
    """Convert a type annotation to a PortConfig.

    ``Batch`` annotations are unwrapped: the inner ``port_type`` is used for
    the port's type constraint.  The ``Batch`` wrapper itself is handled
    separately by ``_generate_input_salvo_condition`` and ``_prepare_kwargs``
    (via the ``batch_ports`` set on ``_ParsedSignature``).

    Args:
        annotation: The type annotation from the function signature.
        include_type: If False, return PortConfig without type information.

    Returns:
        A PortConfig derived from the annotation.
    """
    if not include_type:
        # Skip type information entirely
        return PortConfig()

    # Unwrap Batch to get the inner port_type
    if isinstance(annotation, Batch):
        annotation = annotation.port_type

    if annotation is _MISSING or annotation is inspect.Parameter.empty or annotation is None:
        # No annotation - default port with no type constraint
        return PortConfig()

    if isinstance(annotation, PortConfig):
        # Already a PortConfig - use directly
        return annotation

    if isinstance(annotation, type):
        # Type object - use for isinstance checking
        return PortConfig(port_type=annotation)

    if get_origin(annotation) is not None:
        # Generic type (e.g., list[int], dict[str, int]) - store as-is for beartype checking
        return PortConfig(port_type=annotation)

    if isinstance(annotation, str):
        # String type name
        return PortConfig(port_type=annotation)

    if isinstance(annotation, PortTypeConfig):
        # PortTypeConfig - wrap in PortConfig
        return PortConfig(port_type=annotation)

    # For other annotations, use the string representation
    return PortConfig(port_type=str(annotation))


def _parse_return_annotation(
    annotation: Any, include_type: bool = True
) -> tuple[dict[str, PortConfig], set[str]]:
    """Parse the return annotation to determine output ports.

    Args:
        annotation: The return annotation from the function signature.
        include_type: If False, return PortConfigs without type information.

    Returns:
        Tuple of (out_ports dict, packet_ports set).
        packet_ports contains port names annotated with PreCreatedPacket.
    """
    packet_ports: set[str] = set()

    if annotation is inspect.Signature.empty or annotation is None:
        # No return annotation - no output ports
        return {}, packet_ports

    # Check if it's a dict with string keys and PortConfig/PreCreatedPacket values
    # This handles the case: -> {"out1": PortConfig(...), "out2": PreCreatedPacket(int)}
    if isinstance(annotation, dict):
        out_ports = {}
        for name, value in annotation.items():
            if isinstance(value, PreCreatedPacket):
                packet_ports.add(name)
                out_ports[name] = _annotation_to_port_config(value.port_type, include_type)
            elif isinstance(value, PortConfig):
                out_ports[name] = value if include_type else PortConfig()
            else:
                out_ports[name] = _annotation_to_port_config(value, include_type)
        return out_ports, packet_ports

    # Single return type
    if isinstance(annotation, PreCreatedPacket):
        packet_ports.add("out")
        return {"out": _annotation_to_port_config(annotation.port_type, include_type)}, packet_ports

    return {"out": _annotation_to_port_config(annotation, include_type)}, packet_ports


def _parse_function_signature(func: Callable|str, include_port_types: bool = True) -> _ParsedSignature:
    """Parse a function's signature to extract port configurations.

    Args:
        func: The function to parse.
        include_port_types: If False, ports will not have type information.

    Returns:
        ParsedSignature with port configs and parameter info.

    Raises:
        ValueError: If the function has *args or **kwargs.
    """
    if isinstance(func, str):
        # Import the function from path (supports both dotted and file-path refs)
        func = _get_func_from_import_path(func)

    sig = inspect.signature(func)

    in_ports: dict[str, PortConfig] = {}
    batch_ports: dict[str, int | None] = {}
    special_params: set[str] = set()
    regular_params: list[str] = []

    for param_name, param in sig.parameters.items():
        # Check for unsupported parameter types
        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            raise ValueError(f"*args not supported in function {func.__name__}")
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            raise ValueError(f"**kwargs not supported in function {func.__name__}")

        # Check for special parameters
        if param_name in _SPECIAL_PARAMS:
            special_params.add(param_name)
            continue

        # Regular parameter - becomes an input port
        annotation = param.annotation if param.annotation is not inspect.Parameter.empty else _MISSING
        in_ports[param_name] = _annotation_to_port_config(annotation, include_port_types)
        if isinstance(annotation, Batch):
            batch_ports[param_name] = annotation.count
        regular_params.append(param_name)

    # Parse return annotation for output ports
    return_annotation = sig.return_annotation
    out_ports, packet_ports = _parse_return_annotation(return_annotation, include_port_types)

    return _ParsedSignature(
        in_ports=in_ports,
        out_ports=out_ports,
        special_params=special_params,
        regular_params=regular_params,
        batch_ports=batch_ports,
        packet_ports=packet_ports,
    )

# %% [markdown]
# ## Default Salvo Conditions
#
# Generate default salvo conditions for input and output.

# %%
#|exporti
def _generate_input_salvo_condition(
    in_ports: dict[str, PortConfig],
    batch_ports: dict[str, int | None] | None = None,
) -> dict[str, SalvoConditionConfig] | None:
    """Generate the default ``"trigger"`` input salvo condition.

    Fires when all input ports have at least one packet.  The per-port packet
    count is determined by ``Batch`` annotations:

    - Ports **not** in ``batch_ports``: grab exactly 1 packet (scalar).
    - Ports in ``batch_ports`` with ``count=None``: grab **all** available
      packets (``PacketCountAllConfig``).
    - Ports in ``batch_ports`` with ``count=N``: grab at most *N* packets
      (``PacketCountNConfig(count=N)``).

    The exec_func reads back the ``"trigger"`` salvo's per-port packet counts
    to decide whether to pass a scalar or a list to the user function.

    Args:
        in_ports: The input port configurations.
        batch_ports: Mapping of port name → count from ``Batch`` annotations.
            ``None`` count means "all".

    Returns:
        Dict with a single ``"trigger"`` salvo condition, or ``None`` for
        source nodes (no input ports) — letting ``NodeConfig.resolve()``
        generate the appropriate default based on full context.
    """
    if not in_ports:
        return None

    batch_ports = batch_ports or {}

    # Build condition: all ports must be non-empty
    port_terms = [
        SalvoConditionTermPortConfig(port_name=port_name, state=PortStateNonEmptyConfig())
        for port_name in in_ports.keys()
    ]

    if len(port_terms) == 1:
        term = port_terms[0]
    else:
        term = SalvoConditionTermAndConfig(terms=port_terms)

    # Determine packet count per port from Batch annotations
    ports = {}
    for port_name in in_ports.keys():
        if port_name in batch_ports:
            count = batch_ports[port_name]
            if count is None:
                ports[port_name] = PacketCountAllConfig()
            else:
                ports[port_name] = PacketCountNConfig(count=count)
        else:
            ports[port_name] = PacketCountNConfig(count=1)

    return {
        "trigger": SalvoConditionConfig(
            max_salvos=MaxSalvosFiniteConfig(max=1),
            ports=ports,
            term=term,
        )
    }


def _generate_output_salvo_condition(out_ports: dict[str, PortConfig]) -> dict[str, SalvoConditionConfig]:
    """Generate default output salvo condition.

    Default: Fires once per epoch (no port conditions).

    Args:
        out_ports: The output port configurations.

    Returns:
        Dict with a single "send" salvo condition.
    """
    if not out_ports:
        # No output ports - no output salvo condition needed
        return {}

    # Include all packets from all output ports
    ports = {port_name: PacketCountAllConfig() for port_name in out_ports.keys()}

    return {
        "send": SalvoConditionConfig(
            max_salvos=MaxSalvosFiniteConfig(max=1),
            ports=ports,
            term=SalvoConditionTermTrueConfig(),
        )
    }

# %% [markdown]
# ## Execution Wrapper
#
# Create the wrapper function that bridges user functions to node execution.

# %%
#|exporti
def _create_exec_func(func: Callable, parsed_sig: _ParsedSignature, manual_output: bool = False) -> Callable:
    """Create the exec_node_func that wraps the user function.

    The wrapper:
    1. Extracts packet values from input ports
    2. Handles special parameters (ctx, print)
    3. Calls the user function
    4. Routes return value to output ports (unless manual_output=True)
    5. Sends the output salvo (unless manual_output=True)

    Args:
        func: The user function to wrap.
        parsed_sig: The parsed function signature.
        manual_output: If True, the wrapper does not create packets or send
            salvos from the return value.  The function must return None and
            manage its own output via ``ctx``.

    Returns:
        An exec_node_func suitable for NodeExecutionConfig.
    """
    is_async = asyncio.iscoroutinefunction(func)

    def _prepare_kwargs(ctx, packets):
        """Extract kwargs from packets and special params."""
        kwargs = {}

        # Extract packet values for regular parameters
        for param_name in parsed_sig.regular_params:
            if param_name in packets and packets[param_name]:
                packet_ids = packets[param_name]

                if param_name in parsed_sig.batch_ports:
                    # Batch port — consume all delivered packets, always pass as list
                    values = [ctx.consume_packet(pid) for pid in packet_ids]
                    kwargs[param_name] = values
                else:
                    # Scalar port — consume exactly 1 packet
                    packet_id = packet_ids[0]
                    kwargs[param_name] = ctx.consume_packet(packet_id)
            else:
                # Port has no packets - this shouldn't happen with proper salvo conditions
                raise ValueError(f"No packets in port '{param_name}' for function {func.__name__}")

        # Handle special parameters
        if "ctx" in parsed_sig.special_params:
            kwargs["ctx"] = ctx
        if "print" in parsed_sig.special_params:
            kwargs["print"] = ctx.print
        if "log" in parsed_sig.special_params:
            kwargs["log"] = ctx.log

        return kwargs

    def _route_result(ctx, result):
        """Route function result to output ports and send salvo."""
        if parsed_sig.out_ports:
            if len(parsed_sig.out_ports) == 1:
                # Single output port - unwrap dict if keyed by port name
                port_name = list(parsed_sig.out_ports.keys())[0]
                value = result[port_name] if isinstance(result, dict) and port_name in result else result
                if port_name in parsed_sig.packet_ports:
                    # PreCreatedPacket: value is already a packet ID
                    ctx.load_output_port(port_name, value)
                else:
                    packet_id = ctx.create_packet(value)
                    ctx.load_output_port(port_name, packet_id)
            else:
                # Multiple output ports - result must be a dict
                if not isinstance(result, dict):
                    raise TypeError(
                        f"Function {func.__name__} has multiple output ports "
                        f"but returned {type(result).__name__} instead of dict"
                    )
                for port_name in parsed_sig.out_ports.keys():
                    if port_name in result:
                        value = result[port_name]
                        if port_name in parsed_sig.packet_ports:
                            # PreCreatedPacket: value is already a packet ID
                            ctx.load_output_port(port_name, value)
                        else:
                            packet_id = ctx.create_packet(value)
                            ctx.load_output_port(port_name, packet_id)

            # Send the output salvo
            ctx.send_output_salvo("send")

    def _handle_result(ctx, result):
        """Route result or validate manual_output mode."""
        if manual_output:
            if result is not None:
                raise TypeError(
                    f"Function {func.__name__} returned {type(result).__name__} "
                    f"but manual_output=True requires it to return None "
                    f"(manage output via ctx instead)"
                )
        else:
            _route_result(ctx, result)

    if is_async:
        # Async user function - need to handle event loop
        def exec_node_func(ctx, packets):
            """Wrapper for async user function."""
            kwargs = _prepare_kwargs(ctx, packets)

            async def _run_async():
                result = await func(**kwargs)
                _handle_result(ctx, result)
                return result

            # Check if we're already in a running event loop
            try:
                loop = asyncio.get_running_loop()
                # We're in a running loop - can't use run_until_complete
                # This happens when running in the main pool (SingleWorkerPool)
                # We need to run the coroutine synchronously using asyncio.run()
                # But that also fails in a running loop, so we need nest_asyncio
                # or a different approach.
                # For now, create a new event loop in a way that works
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, _run_async())
                    return future.result()
            except RuntimeError:
                # No running loop - create a new one
                return asyncio.run(_run_async())
    else:
        # Sync user function - no event loop needed
        def exec_node_func(ctx, packets):
            """Wrapper for sync user function."""
            kwargs = _prepare_kwargs(ctx, packets)
            result = func(**kwargs)
            _handle_result(ctx, result)
            return result

    return exec_node_func

# %% [markdown]
# ## Config Merger
#
# Merge user-provided `_node_config` with auto-generated config.

# %%
#|exporti
def _deep_merge_dicts(base: dict, override: dict) -> dict:
    """Deep merge two dictionaries.

    Override values take precedence. Nested dicts are merged recursively.

    Args:
        base: The base dictionary.
        override: The override dictionary.

    Returns:
        Merged dictionary.
    """
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def _parse_node_config_override(override: Any) -> dict:
    """Parse a _node_config override value.

    Args:
        override: Can be NodeConfig, dict, or TOML string.

    Returns:
        A dictionary suitable for merging.
    """
    if override is None:
        return {}

    if isinstance(override, NodeConfig):
        return override.model_dump(exclude_none=True, exclude_unset=True)

    if isinstance(override, dict):
        return override

    if isinstance(override, str):
        # Parse as TOML
        return tomllib.loads(override)

    raise TypeError(f"_node_config must be NodeConfig, dict, or TOML string, got {type(override)}")

# %% [markdown]
# ## Factory Functions

# %% [markdown]
# Helper functions

# %%
#|exporti
def _from_function(func: Callable|str, include_port_types: bool = True, manual_output: bool = False, project_root=None) -> NodeConfig:
    """Create a NodeConfig from a function.

    Parses the function signature to determine input/output ports and
    generates default salvo conditions.

    Args:
        func: The function to create a node from.
        include_port_types: If True (default), port configs will include type
            information from function annotations for runtime type checking.
            If False, ports will have no type constraints.
        manual_output: If True, the node function manages its own output via
            ctx and must return None.

    Returns:
        A complete NodeConfig.

    Note:
        The node name is always set to func.__name__. To use a different name,
        call NodeConfig.from_factory() with the name parameter instead.

    Example:
        def process(data: str, ctx) -> int:
            ctx.print(f"Processing: {data}")
            return len(data)

        config = from_function(process)
        # Creates node with:
        # - name: "process" (from func.__name__)
        # - in_ports: {"data": PortConfig(port_type=str)}
        # - out_ports: {"out": PortConfig(port_type=int)}
        # - Default salvo conditions
        # - exec_node_func that wraps process()
    """
    # Parse the function signature
    parsed_sig = _parse_function_signature(func, include_port_types)

    # Generate base config - always use func.__name__
    node_name = func.__name__

    in_salvos = _generate_input_salvo_condition(parsed_sig.in_ports, parsed_sig.batch_ports)

    base_config_dict = {
        "name": node_name,
        "in_ports": {k: v.model_dump() for k, v in parsed_sig.in_ports.items()},
        "out_ports": {k: v.model_dump() for k, v in parsed_sig.out_ports.items()},
        "out_salvo_conditions": {
            k: v.model_dump()
            for k, v in _generate_output_salvo_condition(parsed_sig.out_ports).items()
        },
    }

    if in_salvos is not None:
        base_config_dict["in_salvo_conditions"] = {
            k: v.model_dump() for k, v in in_salvos.items()
        }

    # Set source_path in extra to the source file of the function's module
    try:
        source_path = inspect.getfile(func)
        if project_root is not None:
            try:
                source_path = os.path.relpath(source_path, str(project_root))
            except ValueError:
                pass  # relpath fails across drives on Windows; keep absolute
        base_config_dict.setdefault("extra", {})["source_path"] = source_path
    except (TypeError, OSError):
        pass  # Built-in functions or functions without source files

    # Auto-populate description from function docstring
    if func.__doc__:
        base_config_dict["description"] = inspect.cleandoc(func.__doc__)

    # Apply _node_config override if present
    if hasattr(func, "_node_config"):
        override = _parse_node_config_override(func._node_config)
        base_config_dict = _deep_merge_dicts(base_config_dict, override)

    # Create the NodeConfig
    config = NodeConfig.model_validate(base_config_dict)

    # Create execution config with the wrapper function
    exec_func = _create_exec_func(func, parsed_sig, manual_output=manual_output)
    config.execution_config = NodeExecutionConfig(exec_node_func=exec_func)

    return config

def _get_func_from_import_path(func_path: str, project_root=None) -> Callable:
    from netrun.net.config import _import_from_path
    try:
        return _import_from_path(func_path, project_root=project_root)
    except Exception as e:
        raise ValueError(f"Error importing function from path: {func_path}") from e

# %% [markdown]
# The main entry points for the node factory.

# %%
#|exporti
def get_node_config(_net_config=None, *, func: Callable | str, include_port_types: bool = True, manual_output: bool = False) -> NodeConfig:
    """Factory function to get NodeConfig from a function.

    This implements the factory module protocol.  See ``_factory_desc`` for
    full documentation on how function signatures map to node configuration
    (input/output ports, salvo conditions, special parameters, and
    ``PreCreatedPacket`` annotation).

    Args:
        _net_config: NetConfig instance injected by the system (not user-facing).
        func: The function or its import path (e.g. ``"mymodule.my_func"``).
        include_port_types: If True (default), port configs will include type
            information from function annotations for runtime type checking.
            If False, ports will have no type constraints.
        manual_output: If True, the node manages its own output via ctx.
            The function must return None.

    Returns:
        NodeConfig without execution_config (per factory protocol).

    Note:
        The node name is always derived from ``func.__name__``. To override
        the name, use ``NodeConfig.from_factory()`` with the name parameter.
    """
    project_root = _net_config.project_root_path if _net_config is not None else None
    if isinstance(func, str):
        func = _get_func_from_import_path(func, project_root=project_root)

    # Get full config and strip execution_config
    config = _from_function(func, include_port_types, manual_output=manual_output, project_root=project_root)
    config.execution_config = None
    return config


def get_node_funcs(_net_config=None, *, func: Callable | str, include_port_types: bool = True, manual_output: bool = False) -> tuple:
    """Factory function to get execution functions.

    This implements the factory module protocol.

    Args:
        _net_config: NetConfig instance injected by the system (not user-facing).
        func: The function or its import path.
        include_port_types: Accepted for consistency with get_node_config but
            not used (type checking is a config concern, not execution).
        manual_output: If True, the wrapper enforces that the function
            returns None.

    Returns:
        Tuple of (exec_func, start_func, stop_func, on_failure_func).
    """
    project_root = _net_config.project_root_path if _net_config is not None else None
    if isinstance(func, str):
        func = _get_func_from_import_path(func, project_root=project_root)

    parsed_sig = _parse_function_signature(func)
    exec_func = _create_exec_func(func, parsed_sig, manual_output=manual_output)

    return (exec_func, None, None, None)

# %%
#|export
_factory_desc = """\
Creates a node from a regular Python function.

**Factory args:**
- ``func`` (str | callable): The function or its import path.
- ``include_port_types`` (bool, default True): Include type annotations on ports.
- ``manual_output`` (bool, default False): When True, the factory does not
  create packets or send salvos from the return value.  The function must
  return ``None`` and manage its own output via ``ctx`` (e.g. by calling
  ``ctx.create_packet()``, ``ctx.load_output_port()``, and
  ``ctx.send_output_salvo()`` directly).  Raises ``TypeError`` if the
  function returns a non-None value.

**Input ports** are derived from regular function parameters. Each parameter
becomes an input port. If the parameter has a type annotation, the port gets
that type for runtime type checking.

- Type annotations are purely for type validation — ``list[int]`` means
  "this port expects a single packet whose value is a ``list[int]``".
- To collect **multiple packets** into a list, use the ``Batch`` annotation::

      from netrun.node_factories.from_function import Batch

      def process(items: Batch(str)):           # all available str packets
          ...
      def process(items: Batch(int, count=5)):  # at most 5 int packets
          ...

  ``Batch`` controls the ``"trigger"`` salvo condition's per-port packet
  count.  The function **always** receives a ``list`` for ``Batch`` ports,
  even if only one packet was available.
- Parameters named ``ctx`` or ``print`` are special: ``ctx`` receives the
  ``NodeExecutionContext``; ``print`` receives ``ctx.print`` for captured output.
  These do **not** become input ports.

**Output ports** are derived from the return annotation:

- Single type (``-> int``): creates one port named ``"out"``.
- Dict (``-> {"a": int, "b": str}``): creates one port per key.
- ``PortConfig`` values are accepted for custom port configuration
  (``-> {"a": PortConfig(port_type=int)}``).
- ``PreCreatedPacket`` marks a port as receiving a pre-created packet ID
  rather than a value. Use this when the function creates packets itself
  via ``ctx.create_packet()`` or ``ctx.create_packet_from_value_func()``
  (``-> {"out": str, "lazy": PreCreatedPacket(str)}``).
- No return annotation means no output ports.

**Salvo conditions** are generated automatically:

- Input: fires when all input ports are non-empty.  Packet count per port
  is derived from ``Batch`` annotations: ``Batch()`` → all packets,
  ``Batch(count=N)`` → at most N, no ``Batch`` → exactly 1.
- Output: sends all output port packets in a single salvo.
"""

# %% [markdown]
# ## Tests

# %%
# Test basic signature parsing
def simple_func(a: int, b: str) -> float:
    return float(a) + len(b)

parsed = _parse_function_signature(simple_func)
assert parsed.regular_params == ["a", "b"]
assert "a" in parsed.in_ports
assert "b" in parsed.in_ports
assert parsed.in_ports["a"].port_type == int
assert parsed.in_ports["b"].port_type == str
assert "out" in parsed.out_ports
assert parsed.out_ports["out"].port_type == float
print("Basic parsing test passed")

# %%
# Test special params
def with_special(data: str, ctx, print) -> int:
    return len(data)

parsed = _parse_function_signature(with_special)
assert parsed.regular_params == ["data"]
assert "ctx" in parsed.special_params
assert "print" in parsed.special_params
assert "ctx" not in parsed.in_ports
assert "print" not in parsed.in_ports
print("Special params test passed")

# %%
# Test no annotations
def no_annot(x, y):
    pass

parsed = _parse_function_signature(no_annot)
assert parsed.regular_params == ["x", "y"]
assert parsed.in_ports["x"].port_type is None
assert parsed.in_ports["y"].port_type is None
assert len(parsed.out_ports) == 0  # No return annotation
print("No annotations test passed")

# %%
# Test PortConfig annotation
def with_port_config(data: PortConfig(port_type="DataFrame")) -> int:
    return 42

parsed = _parse_function_signature(with_port_config)
assert parsed.in_ports["data"].port_type == "DataFrame"
print("PortConfig annotation test passed")

# %%
# Test from_function
config = _from_function(simple_func)
assert config.name == "simple_func"
assert "a" in config.in_ports
assert "b" in config.in_ports
assert "out" in config.out_ports
assert "trigger" in config.in_salvo_conditions
assert "send" in config.out_salvo_conditions
assert config.execution_config is not None
assert config.execution_config.exec_node_func is not None
print("from_function test passed")

# %%
# Test _node_config override
def custom_func(x: int) -> int:
    return x * 2

custom_func._node_config = {
    "name": "CustomName",
}

config = _from_function(custom_func)
assert config.name == "CustomName"
print("_node_config override test passed")

# %%
# Test TOML override
def toml_func(x: int) -> int:
    return x * 3

toml_func._node_config = '''
name = "TomlNode"
'''

config = _from_function(toml_func)
assert config.name == "TomlNode"
print("TOML override test passed")

# %%
# Test include_port_types=False
def typed_func(a: int, b: str) -> float:
    return float(a) + len(b)

# With types (default)
config_with_types = _from_function(typed_func, include_port_types=True)
assert config_with_types.in_ports["a"].port_type == int
assert config_with_types.in_ports["b"].port_type == str
assert config_with_types.out_ports["out"].port_type == float

# Without types
config_no_types = _from_function(typed_func, include_port_types=False)
assert config_no_types.in_ports["a"].port_type is None
assert config_no_types.in_ports["b"].port_type is None
assert config_no_types.out_ports["out"].port_type is None

print("include_port_types test passed")

# %%
# Test get_node_config with include_port_types
config_via_factory = get_node_config(func=typed_func, include_port_types=False)
assert config_via_factory.in_ports["a"].port_type is None
assert config_via_factory.out_ports["out"].port_type is None
assert config_via_factory.execution_config is None  # Factory protocol strips this
print("get_node_config include_port_types test passed")
