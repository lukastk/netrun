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

from netrun.cli._helpers import load_config, output_json, get_node_by_name
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

ConfigOpt = Annotated[Optional[str], typer.Option("--config", "-c", help="Path to netrun config file.")]
PrettyOpt = Annotated[bool, typer.Option("--pretty", "-p", help="Pretty-print JSON output.")]
NodeOpt = Annotated[Optional[str], typer.Option("--node", "-n", help="Node name for node-level actions.")]


@actions_app.command("list")
def actions_list(
    config: ConfigOpt = None,
    node_name: NodeOpt = None,
    pretty: PrettyOpt = False,
) -> None:
    """List available actions."""
    net_config, config_path = load_config(config)

    node_extra = None
    if node_name:
        n = get_node_by_name(net_config, node_name)
        node_extra = n.extra

    actions = get_available_actions(net_config.extra, node_extra)
    result = [a.model_dump() for a in actions]
    output_json(result, pretty)


@actions_app.command("run")
def actions_run(
    action_id: Annotated[str, typer.Argument(help="Action ID to run.")],
    config: ConfigOpt = None,
    node_name: NodeOpt = None,
    timeout: Annotated[float, typer.Option("--timeout", "-t", help="Timeout in seconds.")] = 30.0,
    pretty: PrettyOpt = False,
) -> None:
    """Run an action by ID."""
    net_config, config_path = load_config(config)

    node_extra = None
    node_id = None
    if node_name:
        n = get_node_by_name(net_config, node_name)
        node_extra = n.extra
        node_id = node_name

    actions = get_available_actions(net_config.extra, node_extra)
    action: ActionConfig | None = None
    for a in actions:
        if a.id == action_id:
            action = a
            break

    if action is None:
        typer.echo(f"Error: action '{action_id}' not found.", err=True)
        raise typer.Exit(1)

    context = build_action_context(
        graph_extra=net_config.extra,
        node_name=node_name,
        node_id=node_id,
        node_extra=node_extra,
        net_file_path=str(config_path),
        project_root=str(net_config.project_root_path),
    )

    result = asyncio.run(execute_action(action, context, timeout=timeout))
    output_json(result.model_dump(), pretty)

    if not result.success:
        raise typer.Exit(1)
