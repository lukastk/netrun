# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp cli._actions

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
import asyncio
from typing import Annotated, Optional

import typer

from netrun.cli._helpers import ConfigOpt, PrettyOpt, load_config, output_json, get_node_by_name
from netrun.tools._helpers import (
    get_available_actions,
    build_action_context,
)
from netrun.tools._execute import execute_action
from netrun.tools._models import ActionConfig

# %% [markdown]
# # Actions Commands
#
# List and run actions defined in a netrun config.

# %%
#|export
actions_app = typer.Typer(help="List and run actions.", no_args_is_help=True)

NodeOpt = Annotated[Optional[str], typer.Option("--node", "-n", help="Node name for node-level actions.")]


@actions_app.command("list")
def actions_list(
    config: ConfigOpt = None,
    node_name: NodeOpt = None,
    pretty: PrettyOpt = True,
) -> None:
    """List available actions."""
    net_config, config_path = load_config(config)

    node_extra = None
    if node_name:
        n = get_node_by_name(net_config, node_name)
        node_extra = n.extra

    actions = get_available_actions(net_config.graph.extra, node_extra)
    result = [a.model_dump() for a in actions]
    output_json(result, pretty)


@actions_app.command("run")
def actions_run(
    action_id: Annotated[str, typer.Argument(help="Action ID to run.")],
    node_name: Annotated[Optional[str], typer.Argument(help="Node name to run the action on.")] = None,
    config: ConfigOpt = None,
    global_only: Annotated[bool, typer.Option("--global", "-g", help="Run with project-level context only (no node).")] = False,
    timeout: Annotated[float, typer.Option("--timeout", "-t", help="Timeout in seconds.")] = 30.0,
    pretty: PrettyOpt = True,
) -> None:
    """Run an action by ID on a specific node."""
    if not node_name and not global_only:
        typer.echo("Error: provide a node name or use --global for project-level context.", err=True)
        raise typer.Exit(2)
    if node_name and global_only:
        typer.echo("Error: cannot use --global with a node name.", err=True)
        raise typer.Exit(2)

    net_config, config_path = load_config(config)

    node_extra = None
    node_execution_config = None
    node_config = None
    if node_name:
        n = get_node_by_name(net_config, node_name)
        node_extra = n.extra
        if n.execution_config:
            node_execution_config = n.execution_config.model_dump(exclude_none=True)
        node_config = n.model_dump_json(exclude_none=True)

    actions = get_available_actions(net_config.graph.extra, node_extra)
    action: ActionConfig | None = None
    for a in actions:
        if a.id == action_id:
            action = a
            break

    if action is None:
        typer.echo(f"Error: action '{action_id}' not found.", err=True)
        raise typer.Exit(1)

    # Extract global node_vars as plain dict for build_action_context
    global_node_vars = None
    if net_config.node_vars:
        global_node_vars = {k: v.model_dump() for k, v in net_config.node_vars.items()}

    context = build_action_context(
        graph_extra=net_config.graph.extra,
        node_name=node_name,
        node_extra=node_extra,
        net_file_path=str(config_path),
        project_root=str(net_config.project_root_path),
        global_node_vars=global_node_vars,
        node_execution_config=node_execution_config,
        node_config=node_config,
    )

    result = asyncio.run(execute_action(action, context, timeout=timeout))
    output_json(result.model_dump(), pretty)

    if not result.success:
        raise typer.Exit(1)
