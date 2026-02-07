"""Action execution endpoints for running commands from the UI."""
import asyncio
import os
import re
from pathlib import Path

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
    node_config: str | None = None  # JSON-serialized node config


class ExecuteActionResponse(BaseModel):
    """Response from action execution."""
    success: bool
    exit_code: int
    stdout: str
    stderr: str
    resolved_command: str  # The command after variable resolution


def _resolve_project_root(
    project_root: str | None,
    net_file_path: str | None,
) -> str | None:
    """Resolve project root to an absolute path."""
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


def _build_template_variables(
    node_name: str | None,
    node_id: str | None,
    net_file_path: str | None,
    project_root: str | None,
    default_cmd: str | None,
    env: dict[str, str] | None,
    node_env: dict[str, str] | None,
    node_config: str | None = None,
) -> dict[str, str]:
    """Build variable mapping with proper precedence."""
    net_file_dir = str(Path(net_file_path).parent) if net_file_path else None
    variables: dict[str, str] = {}

    variables["NODE_NAME"] = node_name or ""
    variables["NODE_ID"] = node_id or ""
    variables["NET_FILE_PATH"] = net_file_path or ""
    variables["NET_FILE_DIR"] = net_file_dir or ""
    variables["PROJECT_ROOT"] = project_root or net_file_dir or ""
    variables["DEFAULT_CMD"] = default_cmd or ""
    variables["NODE_CONFIG"] = node_config or "{}"

    if env:
        variables.update(env)
    if node_env:
        variables.update(node_env)

    return variables


def _resolve_template(template: str, variables: dict[str, str]) -> str:
    """Resolve $VAR and ${VAR} references in a template string."""
    result = template

    for var_name, var_value in variables.items():
        result = result.replace(f"${{{var_name}}}", var_value)

    for var_name, var_value in variables.items():
        pattern = rf"\${var_name}(?=\W|$)"
        result = re.sub(pattern, lambda _: var_value, result)

    return result


@router.post("/execute", response_model=ExecuteActionResponse)
async def execute_action(request: ExecuteActionRequest) -> ExecuteActionResponse:
    """Execute a shell command with environment variables set."""
    resolved_project_root = _resolve_project_root(
        request.project_root, request.net_file_path,
    )

    variables = _build_template_variables(
        request.node_name, request.node_id, request.net_file_path,
        resolved_project_root, request.default_cmd,
        request.env, request.node_env, request.node_config,
    )

    # Determine working directory
    working_dir = request.working_directory
    if not working_dir:
        working_dir = resolved_project_root
    if not working_dir and request.net_file_path:
        working_dir = str(Path(request.net_file_path).parent)

    if working_dir and not Path(working_dir).is_dir():
        raise HTTPException(
            status_code=400,
            detail=f"Working directory does not exist: {working_dir}",
        )

    # Build environment
    env = os.environ.copy()
    env.update(variables)

    resolved_command = _resolve_template(request.command, variables)

    try:
        process = await asyncio.create_subprocess_shell(
            request.command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=working_dir,
            env=env,
        )

        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=30.0,
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            return ExecuteActionResponse(
                success=False, exit_code=-1,
                stdout="", stderr="Command timed out after 30 seconds",
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
        raise HTTPException(status_code=500, detail=f"Error executing command: {e}")


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
    node_config: str | None = None  # JSON-serialized node config


class ResolveTemplateResponse(BaseModel):
    """Response with resolved template."""
    resolved: str


@router.post("/resolve", response_model=ResolveTemplateResponse)
async def resolve_action_template(request: ResolveTemplateRequest) -> ResolveTemplateResponse:
    """Resolve template variables without executing."""
    resolved_project_root = _resolve_project_root(
        request.project_root, request.net_file_path,
    )

    variables = _build_template_variables(
        request.node_name, request.node_id, request.net_file_path,
        resolved_project_root, request.default_cmd,
        request.env, request.node_env, request.node_config,
    )

    resolved = _resolve_template(request.template, variables)
    return ResolveTemplateResponse(resolved=resolved)
