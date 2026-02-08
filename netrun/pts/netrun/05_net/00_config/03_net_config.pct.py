# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp net.config._net_config

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
from pydantic import BaseModel, Field, PrivateAttr, model_validator, field_serializer
from typing import Annotated, Literal, Any
from collections.abc import Callable
from pathlib import Path
import os
import json
import tomllib

from netrun.net.config._base import (
    _get_callable_import_path,
    _import_from_path,
)
from netrun.net.config._nodes import NodeVariable
from netrun.net.config._graph import GraphConfig
from netrun.execution_manager import RunAllocationMethod

# %% [markdown]
# # Net Configuration

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
    """Configuration for a remote pool.

    ``url`` and ``worker_name`` may be left as ``None`` when building a
    partial config (e.g. loaded from a file).  They **must** be set before
    the config is used to construct a ``Net`` — otherwise a ``ValueError``
    is raised at pool-construction time.
    """
    type: Literal["remote"] = "remote"
    url: str | None = None
    worker_name: str | None = None
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


def _generate_default_output_queues(graph: "GraphConfig") -> dict[str, "OutputQueueConfig"]:
    """Generate output queues for all unconnected output ports.

    Creates one queue per unconnected output port with the naming convention
    "NODE_NAME::PORT_NAME".

    Args:
        graph: The resolved graph configuration.

    Returns:
        Dict mapping queue names to OutputQueueConfig objects.
    """
    # Collect all connected output ports (source of edges)
    connected_ports: set[tuple[str, str]] = set()
    for edge in graph.edges:
        source = edge.get_source()
        connected_ports.add((source.node_name, source.port_name))

    # Generate queues for unconnected output ports
    queues: dict[str, OutputQueueConfig] = {}
    for node in graph.nodes:
        for port_name in node.out_ports:
            if (node.name, port_name) not in connected_ports:
                queue_name = f"{node.name}::{port_name}"
                queues[queue_name] = OutputQueueConfig(ports=[(node.name, port_name)])

    return queues


class NetConfig(BaseModel):
    """Configuration for a Net."""
    model_config = {"arbitrary_types_allowed": True}

    project_root: str | None = Field(default=None, description="Project root path. Relative paths resolve from the config file's directory.")

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

    pools: dict[str, PoolConfig] | None = Field(default=None, description="Pool configurations. None generates a default main pool on resolve().")
    graph: GraphConfig

    extra: dict[str, Any] = Field(default_factory=dict, description="Arbitrary extra data (descriptions, version info, tool-specific data).")

    default_pool_allocation_method: RunAllocationMethod = Field(default=RunAllocationMethod.ROUND_ROBIN, description="Default worker allocation method for nodes with multiple pools.")

    node_vars: dict[str, NodeVariable] | None = Field(default=None, description="Global default node variables, accessible via ctx.vars.")

    dead_letter_queue: bool = Field(default=True, description="Enable dead letter queue for undeliverable packets.")
    dead_letter_path: str | None = Field(default=None, description="File path for dead letter queue storage.")
    dead_letter_callback: Callable | str | None = Field(default=None, description="Callback function or import path for dead letter handling.")

    # Output queue configuration
    output_queues: dict[str, OutputQueueConfig] | None = Field(default=None, description="Output queue configurations. None auto-generates queues for unconnected output ports.")

    error_on_undeclared_output: bool = Field(default=False, description="Raise an error when a packet is sent from an unconnected output port with no queue.")

    type_checking_enabled: bool = Field(default=True, description="Enable runtime type checking for packet values. Can be overridden per-node.")

    propagate_exceptions: bool = Field(default=True, description="Propagate epoch exceptions immediately from run_step/run_until_blocked. Can be overridden per-node.")

    print_exceptions: bool = Field(default=False, description="Print epoch exceptions to stderr when they occur. Can be overridden per-node.")

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
        resolved_graph = self.graph.resolve(base_path=base_path, net_config=self)
        if resolved_graph is not self.graph:
            updates["graph"] = resolved_graph

        # Generate default output queues for unconnected output ports
        if self.output_queues is None:
            updates["output_queues"] = _generate_default_output_queues(resolved_graph)

        # Resolve dead_letter_callback
        if isinstance(self.dead_letter_callback, str):
            updates["dead_letter_callback"] = _import_from_path(self.dead_letter_callback, project_root=project_root)

        if updates:
            result = self.model_copy(update=updates)
            # Pydantic v2 model_copy() does not copy PrivateAttr - preserve it
            result._file_path = self._file_path
            return result
        return self
