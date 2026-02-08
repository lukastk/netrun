# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp cli._app

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
import typer

# %% [markdown]
# # CLI Application
#
# Typer-based CLI for inspecting and managing netrun projects.

# %%
#|export
app = typer.Typer(
    name="netrun",
    help="CLI for inspecting and managing netrun projects.",
    no_args_is_help=True,
)

# %%
#|export
from netrun.cli._commands import (
    validate,
    structure,
    convert,
    factory_info,
    info,
    nodes,
    node,
)

app.command("validate")(validate)
app.command("structure")(structure)
app.command("convert")(convert)
app.command("factory-info")(factory_info)
app.command("info")(info)
app.command("nodes")(nodes)
app.command("node")(node)

# %%
#|export
from netrun.cli._download_agents import download_agents

app.command("download-agents")(download_agents)

# %%
#|export
from netrun.cli._actions import actions_app
from netrun.cli._recipes import recipes_app

app.add_typer(actions_app, name="actions")
app.add_typer(recipes_app, name="recipes")

# %%
#|export
def app_main() -> None:
    """Entry point for the netrun CLI."""
    app()
