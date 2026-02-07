"""Action execution endpoints — delegates to netrun.tools."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from netrun.tools._models import ActionContext, ActionResult
from netrun.tools._execute import execute_command
from netrun.tools._template import resolve_project_root, resolve_template

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
    timed_out: bool = False


def _build_context(request: ExecuteActionRequest | "ResolveTemplateRequest") -> ActionContext:
    """Build an ActionContext from a request, resolving project_root."""
    resolved_root = resolve_project_root(request.project_root, request.net_file_path)
    return ActionContext(
        node_name=request.node_name,
        node_id=request.node_id,
        net_file_path=request.net_file_path,
        project_root=resolved_root,
        default_cmd=request.default_cmd,
        env=getattr(request, "env", None),
        node_env=getattr(request, "node_env", None),
        working_directory=getattr(request, "working_directory", None),
        node_config=request.node_config,
    )


@router.post("/execute", response_model=ExecuteActionResponse)
async def execute_action(request: ExecuteActionRequest) -> ExecuteActionResponse:
    """Execute a shell command with environment variables set."""
    context = _build_context(request)

    try:
        result: ActionResult = await execute_command(request.command, context)
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
        timed_out=result.timed_out,
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
    node_config: str | None = None  # JSON-serialized node config


class ResolveTemplateResponse(BaseModel):
    """Response with resolved template."""
    resolved: str


@router.post("/resolve", response_model=ResolveTemplateResponse)
async def resolve_action_template(request: ResolveTemplateRequest) -> ResolveTemplateResponse:
    """Resolve template variables without executing."""
    context = _build_context(request)
    resolved = resolve_template(request.template, context)
    return ResolveTemplateResponse(resolved=resolved)
