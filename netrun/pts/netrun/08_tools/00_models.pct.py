# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp tools._models

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
from pydantic import BaseModel
from typing import Literal, Any

# %% [markdown]
# # Tools Models
#
# Pydantic models for actions, recipes, and their execution contexts.

# %%
#|export
class ActionConfig(BaseModel):
    """A single action (shell command with template variables)."""
    id: str
    label: str
    command: str


class ActionContext(BaseModel):
    """Variables available during action template resolution and execution."""
    node_name: str | None = None
    node_id: str | None = None
    net_file_path: str | None = None
    project_root: str | None = None
    default_cmd: str | None = None
    env: dict[str, str] | None = None
    """Project-level custom variables."""
    node_env: dict[str, str] | None = None
    """Node-level variable overrides (highest precedence)."""
    working_directory: str | None = None
    node_config: str | None = None
    """JSON-serialized node configuration, available as $NODE_CONFIG."""


class ActionResult(BaseModel):
    """Result of executing an action."""
    success: bool
    exit_code: int
    stdout: str
    stderr: str
    resolved_command: str
    timed_out: bool = False


class RecipeConfig(BaseModel):
    """A recipe definition (stored in extra data)."""
    path: str
    description: str | None = None


class RecipePrompt(BaseModel):
    """A prompt a recipe requests from the user."""
    name: str
    label: str
    type: Literal["text", "number", "select", "checkbox"] = "text"
    default: Any = None
    options: list[str] | None = None
