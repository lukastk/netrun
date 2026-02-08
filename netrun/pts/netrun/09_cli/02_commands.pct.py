# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp cli._commands

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

from netrun.cli._helpers import (
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
ConfigOpt = Annotated[Optional[str], typer.Option("--config", "-c", help="Path to netrun config file.")]
PrettyOpt = Annotated[bool, typer.Option("--pretty/--compact", help="Pretty-print or compact JSON output.")]


def validate(
    config: ConfigOpt = None,
    pretty: PrettyOpt = True,
) -> None:
    """Validate a netrun config file."""
    config_path = find_config(config)
    errors: list[str] = []
    node_count = 0
    edge_count = 0

    # Pydantic validation via NetConfig.from_file
    try:
        from netrun.net.config._net_config import NetConfig
        net_config = NetConfig.from_file(config_path)
        node_count = len(net_config.graph.nodes)
        edge_count = len(net_config.graph.edges)
    except Exception as e:
        errors.append(f"Config validation error: {e}")

    # Raw data checks (recipes, actions)
    if not errors:
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
        output_json({"valid": False, "file": str(config_path), "errors": errors}, pretty)
        raise typer.Exit(1)
    else:
        output_json({"valid": True, "file": str(config_path), "nodes": node_count, "edges": edge_count}, pretty)

# %% [markdown]
# ## structure

# %%
#|export
def _port_map(ports: dict) -> dict[str, str | None]:
    """Convert port dict to {name: type_str}."""
    return {name: port_type_str(pc.port_type) for name, pc in ports.items()}


def structure(
    config: ConfigOpt = None,
    pretty: PrettyOpt = True,
) -> None:
    """Output graph topology as JSON (strips extra, execution_config, pools)."""
    net_config, config_path = load_resolved_config(config)

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
        src = e.get_source()
        tgt = e.get_target()
        edges_out.append({
            "source": f"{src.node_name}.{src.port_name}",
            "target": f"{tgt.node_name}.{tgt.port_name}",
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

    output_json(result, pretty)
