"""Action execution endpoints for running commands from the UI."""
import asyncio
import os
import re
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()


class ExecuteActionRequest(BaseModel):
    """Request to execute an action command."""
    command: str
    working_directory: str | None = None
    env: dict[str, str] | None = None  # Project-level custom variables
    node_env: dict[str, str] | None = None  # Node-level custom variables (override project)
    # Variables for template resolution
    node_name: str | None = None
    node_id: str | None = None
    net_file_path: str | None = None
    project_root: str | None = None
    default_cmd: str | None = None


class ExecuteActionResponse(BaseModel):
    """Response from action execution."""
    success: bool
    exit_code: int
    stdout: str
    stderr: str
    resolved_command: str  # The command after variable resolution


def resolve_template(
    template: str,
    node_name: str | None,
    node_id: str | None,
    net_file_path: str | None,
    project_root: str | None,
    default_cmd: str | None,
    custom_env: dict[str, str] | None,
    node_env: dict[str, str] | None = None,
) -> str:
    """Resolve template variables for PREVIEW purposes only.

    This function is used to show users what their command will look like
    after variable expansion. The actual execution uses real environment
    variables and lets the shell handle expansion.

    Supported variables:
    - $NODE_NAME or ${NODE_NAME} - Node's label/name
    - $NODE_ID or ${NODE_ID} - Node's internal ID
    - $NET_FILE_PATH or ${NET_FILE_PATH} - Full path to the .netrun file
    - $NET_FILE_DIR or ${NET_FILE_DIR} - Directory containing the net file
    - $PROJECT_ROOT or ${PROJECT_ROOT} - Configured project root
    - $DEFAULT_CMD or ${DEFAULT_CMD} - Configured default command
    - Any custom variables from env (project-level)
    - Any custom variables from node_env (node-level, takes precedence)
    """
    result = template

    # Calculate NET_FILE_DIR from NET_FILE_PATH
    net_file_dir = None
    if net_file_path:
        net_file_dir = str(Path(net_file_path).parent)

    # Build variable mapping (order matters for precedence)
    variables: dict[str, str] = {}

    # 1. Built-in variables (lowest precedence for overrides)
    variables["NODE_NAME"] = node_name or ""
    variables["NODE_ID"] = node_id or ""
    variables["NET_FILE_PATH"] = net_file_path or ""
    variables["NET_FILE_DIR"] = net_file_dir or ""
    variables["PROJECT_ROOT"] = project_root or net_file_dir or ""
    variables["DEFAULT_CMD"] = default_cmd or ""

    # 2. Project-level custom variables
    if custom_env:
        variables.update(custom_env)

    # 3. Node-level custom variables (highest precedence - can override anything)
    if node_env:
        variables.update(node_env)

    # Replace ${VAR} syntax first (more specific)
    for var_name, var_value in variables.items():
        result = result.replace(f"${{{var_name}}}", var_value)

    # Replace $VAR syntax (word boundary aware)
    for var_name, var_value in variables.items():
        # Use regex to match $VAR followed by non-word character or end of string
        pattern = rf"\${var_name}(?=\W|$)"
        result = re.sub(pattern, var_value, result)

    return result


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

    # If absolute, return as-is
    if project_path.is_absolute():
        return str(project_path)

    # If relative, resolve against net file directory
    if net_file_path:
        base_dir = Path(net_file_path).parent
        return str((base_dir / project_path).resolve())

    return str(project_path)


@router.post("/execute", response_model=ExecuteActionResponse)
async def execute_action(request: ExecuteActionRequest) -> ExecuteActionResponse:
    """Execute a shell command with environment variables set.

    This endpoint:
    1. Sets up environment variables (built-in + project + node level)
    2. Sets up the working directory
    3. Executes the ORIGINAL command (shell expands $VAR references)
    4. Returns stdout, stderr, and exit code
    5. Also returns resolved_command for preview/display purposes
    """
    # Resolve project root
    resolved_project_root = resolve_project_root(
        request.project_root,
        request.net_file_path,
    )

    # Determine working directory
    working_dir = request.working_directory
    if not working_dir:
        working_dir = resolved_project_root
    if not working_dir and request.net_file_path:
        working_dir = str(Path(request.net_file_path).parent)

    # Validate working directory exists
    if working_dir and not Path(working_dir).is_dir():
        raise HTTPException(
            status_code=400,
            detail=f"Working directory does not exist: {working_dir}"
        )

    # Build environment with proper precedence:
    # 1. System environment (lowest)
    # 2. Built-in variables (NODE_NAME, etc.)
    # 3. Project-level custom variables
    # 4. Node-level custom variables (highest - can override anything)
    env = os.environ.copy()

    # Built-in variables
    env["NODE_NAME"] = request.node_name or ""
    env["NODE_ID"] = request.node_id or ""
    env["NET_FILE_PATH"] = request.net_file_path or ""
    env["NET_FILE_DIR"] = str(Path(request.net_file_path).parent) if request.net_file_path else ""
    env["PROJECT_ROOT"] = resolved_project_root or ""
    env["DEFAULT_CMD"] = request.default_cmd or ""

    # Project-level custom variables
    if request.env:
        env.update(request.env)

    # Node-level custom variables (highest precedence)
    if request.node_env:
        env.update(request.node_env)

    # Compute resolved command for preview/display only
    resolved_command = resolve_template(
        request.command,
        request.node_name,
        request.node_id,
        request.net_file_path,
        resolved_project_root,
        request.default_cmd,
        request.env,
        request.node_env,
    )

    try:
        # Execute the ORIGINAL command - shell will expand $VAR references
        process = await asyncio.create_subprocess_shell(
            request.command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=working_dir,
            env=env,
        )

        # Wait for completion with timeout
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=30.0  # 30 second timeout
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            return ExecuteActionResponse(
                success=False,
                exit_code=-1,
                stdout="",
                stderr="Command timed out after 30 seconds",
                resolved_command=resolved_command,
            )

        return ExecuteActionResponse(
            success=process.returncode == 0,
            exit_code=process.returncode or 0,
            stdout=stdout.decode("utf-8", errors="replace"),
            stderr=stderr.decode("utf-8", errors="replace"),
            resolved_command=resolved_command,
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error executing command: {e}"
        )


class ResolveTemplateRequest(BaseModel):
    """Request to resolve template variables without executing."""
    template: str
    node_name: str | None = None
    node_id: str | None = None
    net_file_path: str | None = None
    project_root: str | None = None
    default_cmd: str | None = None
    env: dict[str, str] | None = None  # Project-level custom variables
    node_env: dict[str, str] | None = None  # Node-level custom variables


class ResolveTemplateResponse(BaseModel):
    """Response with resolved template."""
    resolved: str


@router.post("/resolve", response_model=ResolveTemplateResponse)
async def resolve_action_template(request: ResolveTemplateRequest) -> ResolveTemplateResponse:
    """Resolve template variables without executing.

    Useful for previewing what command will be executed.
    """
    resolved_project_root = resolve_project_root(
        request.project_root,
        request.net_file_path,
    )

    resolved = resolve_template(
        request.template,
        request.node_name,
        request.node_id,
        request.net_file_path,
        resolved_project_root,
        request.default_cmd,
        request.env,
        request.node_env,
    )

    return ResolveTemplateResponse(resolved=resolved)
