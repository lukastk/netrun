# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp _commands

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
import importlib
import inspect
import json
import sys
import tomllib
from pathlib import Path
from typing import Annotated, Any, Optional

import typer
from pydantic import ValidationError

from netrun_cli._helpers import (
    ConfigOpt,
    PrettyOpt,
    find_config,
    load_config,
    load_resolved_config,
    load_raw_data,
    output_json,
    get_node_by_name,
    port_type_str,
)
from netrun.net.config._nodes import NodeConfig, SubgraphConfig

# %% [markdown]
# # Core CLI Commands

# %% [markdown]
# ## validate

# %%
#|export
def validate(
    config: ConfigOpt = None,
    pretty: PrettyOpt = True,
) -> None:
    """Validate a netrun config file."""
    config_path = find_config(config)
    errors: list[str] = []
    warnings: list[str] = []
    node_count = 0
    edge_count = 0

    # Step 1: Pydantic validation — must succeed to continue
    try:
        from netrun.net.config._net_config import NetConfig
        net_config = NetConfig.from_file(config_path)
        node_count = len(net_config.graph.nodes)
        edge_count = len(net_config.graph.edges)
    except ValidationError as e:
        for err in e.errors():
            loc = " → ".join(str(l) for l in err["loc"])
            errors.append({"loc": loc, "msg": err["msg"], "type": err["type"]})
        output_json({"valid": False, "file": str(config_path), "errors": errors}, pretty)
        raise typer.Exit(1)
    except Exception as e:
        errors.append(f"Config validation error: {e}")
        output_json({"valid": False, "file": str(config_path), "errors": errors}, pretty)
        raise typer.Exit(1)

    # Step 2: Pre-resolution structural validation (always runs)
    config_errors = net_config.graph.validate()
    for err in config_errors:
        errors.append(f"{err.type}: {err.msg}")

    # Step 3: Post-resolution validation via Rust (only if resolve succeeds)
    try:
        resolved = net_config.resolve()
    except Exception as e:
        resolved = None
        warnings.append(f"Resolution error: {e}")

    if resolved is not None:
        try:
            resolved.graph.get_graph()  # validates via netrun_sim
        except ValueError as e:
            errors.append(f"Graph validation error: {e}")

    # Step 4: Raw data checks (recipes, actions) — always runs
    try:
        raw, _ = load_raw_data(config)
        config_dir = config_path.parent

        # Check recipe file paths exist
        raw_recipes = raw.get("recipes", {})
        for name, recipe in raw_recipes.items():
            recipe_path = recipe.get("path")
            if recipe_path:
                rp = Path(recipe_path)
                if not rp.is_absolute():
                    rp = config_dir / rp
                if not rp.exists():
                    errors.append(f"Recipe '{name}': file not found: {rp}")

        # Check actions have required fields
        def _check_actions(actions: list[dict], prefix: str) -> None:
            for i, action in enumerate(actions):
                for field in ("id", "label", "command"):
                    if field not in action:
                        errors.append(f"{prefix} action [{i}]: missing '{field}'")

        graph_extra = raw.get("graph", {}).get("extra", raw.get("extra", {}))
        ui = graph_extra.get("ui", {})
        _check_actions(ui.get("actions", []), "Project")

        # Check per-node actions
        for node_data in raw.get("graph", {}).get("nodes", []):
            node_name = node_data.get("name", "?")
            node_ui = node_data.get("extra", {}).get("ui", {})
            _check_actions(node_ui.get("actions", []), f"Node '{node_name}'")

    except Exception as e:
        errors.append(f"Raw data check error: {e}")

    if errors:
        result = {"valid": False, "file": str(config_path), "errors": errors}
        if warnings:
            result["warnings"] = warnings
        output_json(result, pretty)
        raise typer.Exit(1)
    else:
        result = {"valid": True, "file": str(config_path), "nodes": node_count, "edges": edge_count}
        if warnings:
            result["warnings"] = warnings
        output_json(result, pretty)

# %% [markdown]
# ## structure

# %%
#|export
def _port_map(ports: dict) -> dict[str, str | None]:
    """Convert port dict to {name: type_str}."""
    return {name: port_type_str(pc.port_type) for name, pc in ports.items()}


def _mermaid_id(name: str) -> str:
    """Sanitise a node name for use as a Mermaid node ID."""
    return name.replace(" ", "_").replace("-", "_")


def structure(
    config: ConfigOpt = None,
    format: Annotated[str, typer.Option("--format", "-f", help="Output format: json or mermaid.")] = "json",
    pretty: PrettyOpt = True,
) -> None:
    """Output graph topology (default JSON; use --format mermaid for Mermaid diagram)."""
    net_config, config_path = load_resolved_config(config)

    if format == "mermaid":
        lines = ["graph LR"]
        for n in net_config.graph.nodes:
            mid = _mermaid_id(n.name)
            lines.append(f'    {mid}["{n.name}"]')
        for e in net_config.graph.edges:
            src = _mermaid_id(e.source_node)
            tgt = _mermaid_id(e.target_node)
            label = f"{e.source_port} → {e.target_port}"
            lines.append(f'    {src} -->|"{label}"| {tgt}')
        typer.echo("\n".join(lines))
        return

    if format != "json":
        typer.echo(f"Error: unsupported format '{format}'. Use 'json' or 'mermaid'.", err=True)
        raise typer.Exit(1)

    nodes_out = []
    for n in net_config.graph.nodes:
        if isinstance(n, SubgraphConfig):
            nodes_out.append({
                "name": n.name,
                "type": "subgraph",
            })
            continue
        entry: dict[str, Any] = {
            "name": n.name,
            "in_ports": _port_map(n.in_ports),
            "out_ports": _port_map(n.out_ports),
        }
        if n.in_salvo_conditions:
            entry["in_salvo_conditions"] = list(n.in_salvo_conditions.keys())
        if n.out_salvo_conditions:
            entry["out_salvo_conditions"] = list(n.out_salvo_conditions.keys())
        if n.factory:
            entry["factory"] = str(n.factory) if not isinstance(n.factory, str) else n.factory
        if n.factory_args:
            entry["factory_args"] = n.factory_args
        nodes_out.append(entry)

    edges_out = []
    for e in net_config.graph.edges:
        edges_out.append({
            "source": f"{e.source_node}.{e.source_port}",
            "target": f"{e.target_node}.{e.target_port}",
        })

    output_json({"nodes": nodes_out, "edges": edges_out}, pretty)

# %% [markdown]
# ## convert

# %%
#|export
def convert(
    config_file: Annotated[str, typer.Argument(help="Path to config file to convert.")],
    output: Annotated[Optional[str], typer.Option("--output", "-o", help="Output file path.")] = None,
    pretty: Annotated[bool, typer.Option("--pretty/--compact", help="Pretty or compact output.")] = True,
) -> None:
    """Convert between .netrun.json and .netrun.toml formats."""
    src = Path(config_file).resolve()
    if not src.exists():
        typer.echo(f"Error: file not found: {src}", err=True)
        raise typer.Exit(1)

    content = src.read_text()
    src_name = src.name.lower()

    if src_name.endswith(".netrun.json"):
        # JSON -> TOML
        data = json.loads(content)
        try:
            import tomli_w
        except ImportError:
            typer.echo("Error: tomli-w is required for TOML output. Install with: pip install tomli-w", err=True)
            raise typer.Exit(1)
        result = tomli_w.dumps(data)
        default_ext = ".netrun.toml"
    elif src_name.endswith(".netrun.toml"):
        # TOML -> JSON
        data = tomllib.loads(content)
        indent = 2 if pretty else None
        result = json.dumps(data, indent=indent, default=str)
        default_ext = ".netrun.json"
    else:
        typer.echo("Error: file must end with .netrun.json or .netrun.toml", err=True)
        raise typer.Exit(1)

    if output:
        Path(output).write_text(result)
        typer.echo(f"Written to {output}")
    else:
        typer.echo(result)

# %% [markdown]
# ## factory-info

# %%
#|export
def factory_info(
    factory_path: Annotated[str, typer.Argument(help="Dotted import path to factory module.")],
    pretty: PrettyOpt = True,
) -> None:
    """Inspect a factory module: parameters, types, defaults."""
    try:
        module = importlib.import_module(factory_path)
    except ImportError as e:
        typer.echo(f"Error: cannot import '{factory_path}': {e}", err=True)
        raise typer.Exit(1)

    get_node_config_fn = getattr(module, "get_node_config", None)
    has_get_node_funcs = hasattr(module, "get_node_funcs")

    if get_node_config_fn is None:
        typer.echo(f"Error: '{factory_path}' has no get_node_config function.", err=True)
        raise typer.Exit(1)

    sig = inspect.signature(get_node_config_fn)
    params = []
    for name, param in sig.parameters.items():
        # Skip internal parameters (e.g. _net_config) injected by the system
        if name.startswith("_"):
            continue
        p: dict[str, Any] = {"name": name}
        if param.annotation != inspect.Parameter.empty:
            p["type"] = str(param.annotation)
        if param.default != inspect.Parameter.empty:
            p["default"] = repr(param.default)
            p["required"] = False
        else:
            p["required"] = True
        params.append(p)

    result: dict[str, Any] = {
        "factory": factory_path,
        "type": "node" if has_get_node_funcs else "subgraph",
        "params": params,
    }

    desc = getattr(module, "_factory_desc", None)
    if desc:
        result["description"] = desc

    output_json(result, pretty)

# %% [markdown]
# ## info

# %%
#|export
def info(
    config: ConfigOpt = None,
    pretty: PrettyOpt = True,
) -> None:
    """Summary stats for a netrun config."""
    net_config, config_path = load_resolved_config(config)
    raw, _ = load_raw_data(config)

    node_count = 0
    subgraph_count = 0
    factories_used: list[str] = []
    for n in net_config.graph.nodes:
        if isinstance(n, SubgraphConfig):
            subgraph_count += 1
        else:
            node_count += 1
            if n.factory:
                f = str(n.factory) if not isinstance(n.factory, str) else n.factory
                if f not in factories_used:
                    factories_used.append(f)

    edge_count = len(net_config.graph.edges)

    # Pool info
    pool_info: dict[str, str] = {}
    if net_config.pools:
        for pname, pcfg in net_config.pools.items():
            pool_info[pname] = pcfg.spec.type

    # Action count
    graph_extra = net_config.extra
    ui = graph_extra.get("ui", {})
    action_count = len(ui.get("actions", []))

    # Recipe count
    recipe_count = len(raw.get("recipes", {}))

    result: dict[str, Any] = {
        "file": str(config_path),
        "project_root": str(net_config.project_root_path),
        "nodes": node_count,
        "edges": edge_count,
    }
    if subgraph_count:
        result["subgraphs"] = subgraph_count
    if pool_info:
        result["pools"] = pool_info
    if factories_used:
        result["factories"] = factories_used
    result["actions"] = action_count
    result["recipes"] = recipe_count

    output_json(result, pretty)

# %% [markdown]
# ## nodes

# %%
#|export
def nodes(
    config: ConfigOpt = None,
    pretty: PrettyOpt = True,
) -> None:
    """List all nodes with port names."""
    net_config, _ = load_resolved_config(config)

    result = []
    for n in net_config.graph.nodes:
        if isinstance(n, SubgraphConfig):
            result.append({"name": n.name, "type": "subgraph"})
            continue
        entry: dict[str, Any] = {
            "name": n.name,
            "in_ports": list(n.in_ports.keys()),
            "out_ports": list(n.out_ports.keys()),
        }
        if n.factory:
            entry["factory"] = str(n.factory) if not isinstance(n.factory, str) else n.factory
        result.append(entry)

    output_json(result, pretty)

# %% [markdown]
# ## node

# %%
#|export
def node(
    name: Annotated[str, typer.Argument(help="Node name.")],
    config: ConfigOpt = None,
    edges: Annotated[bool, typer.Option("--edges", help="Include connected edges.")] = False,
    pretty: PrettyOpt = True,
) -> None:
    """Detailed info about a specific node."""
    net_config, _ = load_resolved_config(config)
    n = get_node_by_name(net_config, name)

    result: dict[str, Any] = {
        "name": n.name,
        "in_ports": {pname: {"type": port_type_str(pc.port_type)} for pname, pc in n.in_ports.items()},
        "out_ports": {pname: {"type": port_type_str(pc.port_type)} for pname, pc in n.out_ports.items()},
    }

    if n.in_salvo_conditions:
        result["in_salvo_conditions"] = list(n.in_salvo_conditions.keys())
    if n.out_salvo_conditions:
        result["out_salvo_conditions"] = list(n.out_salvo_conditions.keys())
    if n.factory:
        result["factory"] = str(n.factory) if not isinstance(n.factory, str) else n.factory
    if n.factory_args:
        result["factory_args"] = n.factory_args
    if n.execution_config:
        ec = n.execution_config
        result["execution_config"] = {
            "pools": ec.pools,
            "type_checking_enabled": ec.type_checking_enabled,
            "retries": ec.retries,
            "timeout": ec.timeout,
        }
    if n.extra:
        result["extra"] = n.extra

    if edges:
        incoming = []
        outgoing = []
        for e in net_config.graph.edges:
            if e.target_node == name:
                incoming.append({"source": f"{e.source_node}.{e.source_port}", "port": e.target_port})
            if e.source_node == name:
                outgoing.append({"port": e.source_port, "target": f"{e.target_node}.{e.target_port}"})
        result["edges"] = {"incoming": incoming, "outgoing": outgoing}

    output_json(result, pretty)

# %% [markdown]
# ## dry-run

# %%
#|export
def _topological_sort(nodes: list[str], edges: list[tuple[str, str]]) -> list[str]:
    """Kahn's algorithm for topological sort. Returns nodes in execution order."""
    from collections import defaultdict, deque

    in_degree: dict[str, int] = {n: 0 for n in nodes}
    adjacency: dict[str, list[str]] = defaultdict(list)

    for src, tgt in edges:
        if src in in_degree and tgt in in_degree:
            adjacency[src].append(tgt)
            in_degree[tgt] += 1

    queue = deque(n for n in nodes if in_degree[n] == 0)
    result = []

    while queue:
        n = queue.popleft()
        result.append(n)
        for neighbor in adjacency[n]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    return result


def dry_run(
    config: ConfigOpt = None,
    pretty: PrettyOpt = True,
) -> None:
    """Show execution plan without running node code.

    Displays topological order, source/sink nodes, pools, and scheduling constraints.
    """
    net_config, _ = load_resolved_config(config)

    node_names = [n.name for n in net_config.graph.nodes]

    # Build data edges (non-dependency) for topological ordering
    data_edges = [
        (e.source_node, e.target_node)
        for e in net_config.graph.edges
        if not e.dependency
    ]

    # Include depends_on as edges for ordering
    for n in net_config.graph.nodes:
        if isinstance(n, SubgraphConfig):
            continue
        if n.execution_config and n.execution_config.depends_on:
            for dep in n.execution_config.depends_on:
                data_edges.append((dep, n.name))

    topo_order = _topological_sort(node_names, data_edges)

    # Source nodes: no incoming data edges
    targets = {tgt for _, tgt in data_edges}
    source_nodes = [n for n in topo_order if n not in targets]

    # Sink nodes: no outgoing data edges
    sources = {src for src, _ in data_edges}
    sink_nodes = [n for n in topo_order if n not in sources]

    # Build execution order with metadata
    execution_order = []
    for name in topo_order:
        entry: dict[str, Any] = {"node": name}

        for n in net_config.graph.nodes:
            if n.name == name and not isinstance(n, SubgraphConfig):
                if n.execution_config:
                    ec = n.execution_config
                    if ec.pools:
                        entry["pools"] = ec.pools
                    if ec.depends_on:
                        entry["depends_on"] = ec.depends_on
                    if ec.resources:
                        entry["resources"] = ec.resources
                break

        execution_order.append(entry)

    result = {
        "source_nodes": source_nodes,
        "sink_nodes": sink_nodes,
        "execution_order": execution_order,
        "total_nodes": len(node_names),
        "total_edges": len(net_config.graph.edges),
    }

    output_json(result, pretty)
