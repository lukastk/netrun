"""Action execution endpoints for running commands from the UI."""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from netrun.tools._models import ActionContext
from netrun.tools._template import resolve_project_root, resolve_template
from netrun.tools._execute import execute_command

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


def _build_context(
    node_name: str | None = None,
    node_id: str | None = None,
    net_file_path: str | None = None,
    project_root: str | None = None,
    default_cmd: str | None = None,
    env: dict[str, str] | None = None,
    node_env: dict[str, str] | None = None,
    working_directory: str | None = None,
) -> ActionContext:
    """Build an ActionContext, resolving project_root first."""
    resolved_root = resolve_project_root(project_root, net_file_path)
    return ActionContext(
        node_name=node_name,
        node_id=node_id,
        net_file_path=net_file_path,
        project_root=resolved_root,
        default_cmd=default_cmd,
        env=env,
        node_env=node_env,
        working_directory=working_directory,
    )


@router.post("/execute", response_model=ExecuteActionResponse)
async def execute_action(request: ExecuteActionRequest) -> ExecuteActionResponse:
    """Execute a shell command with environment variables set."""
    ctx = _build_context(
        node_name=request.node_name,
        node_id=request.node_id,
        net_file_path=request.net_file_path,
        project_root=request.project_root,
        default_cmd=request.default_cmd,
        env=request.env,
        node_env=request.node_env,
        working_directory=request.working_directory,
    )

    try:
        result = await execute_command(request.command, ctx)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing command: {e}")

    return ExecuteActionResponse(
        success=result.success,
        exit_code=result.exit_code,
        stdout=result.stdout,
        stderr=result.stderr,
        resolved_command=result.resolved_command,
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
    ctx = _build_context(
        node_name=request.node_name,
        node_id=request.node_id,
        net_file_path=request.net_file_path,
        project_root=request.project_root,
        default_cmd=request.default_cmd,
        env=request.env,
        node_env=request.node_env,
    )

    resolved = resolve_template(request.template, ctx)
    return ResolveTemplateResponse(resolved=resolved)
