# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp tools._template

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
import re
from pathlib import Path

from netrun.tools._models import ActionContext

# %% [markdown]
# # Template Resolution
#
# Resolve template variables in action commands.

# %%
#|export
def resolve_project_root(
    project_root: str | None,
    net_file_path: str | None,
) -> str | None:
    """Resolve project root to an absolute path.

    If project_root is:
    - Absolute path: return as-is
    - Relative path: resolve relative to net file directory
    - None: return net file directory
    """
    if not project_root:
        if net_file_path:
            return str(Path(net_file_path).parent)
        return None

    project_path = Path(project_root)

    if project_path.is_absolute():
        return str(project_path)

    if net_file_path:
        base_dir = Path(net_file_path).parent
        return str((base_dir / project_path).resolve())

    return str(project_path)


def build_template_variables(context: ActionContext) -> dict[str, str]:
    """Build the variable mapping from an ActionContext.

    Precedence (lowest to highest):
    1. Built-in variables (NODE_NAME, etc.)
    2. Project-level custom variables (env)
    3. Node-level custom variables (node_env)
    """
    net_file_dir = None
    if context.net_file_path:
        net_file_dir = str(Path(context.net_file_path).parent)

    variables: dict[str, str] = {}

    # 1. Built-in variables
    variables["NODE_NAME"] = context.node_name or ""
    variables["NET_FILE_PATH"] = context.net_file_path or ""
    variables["NET_FILE_DIR"] = net_file_dir or ""
    variables["PROJECT_ROOT"] = context.project_root or net_file_dir or ""
    variables["DEFAULT_CMD"] = context.default_cmd or ""
    variables["NODE_CONFIG"] = context.node_config or "{}"

    # 2. Project-level custom variables
    if context.env:
        variables.update(context.env)

    # 3. Node-level custom variables (highest precedence)
    if context.node_env:
        variables.update(context.node_env)

    return variables


def resolve_template(template: str, context: ActionContext) -> str:
    """Resolve template variables in a command string.

    Supported syntax:
    - $VAR or ${VAR}

    Args:
        template: Command string with template variables.
        context: ActionContext with variable values.

    Returns:
        The template with variables resolved.
    """
    result = template
    variables = build_template_variables(context)

    # Replace ${VAR} syntax first (more specific)
    for var_name, var_value in variables.items():
        result = result.replace(f"${{{var_name}}}", var_value)

    # Replace $VAR syntax (word boundary aware)
    for var_name, var_value in variables.items():
        pattern = rf"\${var_name}(?=\W|$)"
        result = re.sub(pattern, lambda _: var_value, result)

    return result


def resolve_working_directory(context: ActionContext) -> str | None:
    """Determine the working directory for command execution.

    Priority:
    1. Explicit working_directory from context
    2. project_root
    3. Net file directory
    """
    if context.working_directory:
        return context.working_directory
    if context.project_root:
        return context.project_root
    if context.net_file_path:
        return str(Path(context.net_file_path).parent)
    return None
