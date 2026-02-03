"""File I/O endpoints for netrun config files."""
import json
from pathlib import Path
from typing import Any

import tomli
import tomli_w
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..converter import (
    ui_to_graph_config,
    graph_config_to_ui,
    extract_graph_and_extras,
    merge_graph_with_extras,
)

router = APIRouter()


class FileReadRequest(BaseModel):
    """Request to read a file."""
    path: str


class FileReadResponse(BaseModel):
    """Response containing file data."""
    path: str
    format: str  # "json" or "toml"
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    meta: dict[str, Any] | None = None
    extra_data: dict[str, Any] | None = None  # Non-graph data (pools, etc.)


class FileSaveRequest(BaseModel):
    """Request to save a file."""
    path: str
    format: str  # "json" or "toml"
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    meta: dict[str, Any] | None = None
    extra_data: dict[str, Any] | None = None  # Non-graph data (pools, etc.)


class FileSaveResponse(BaseModel):
    """Response after saving."""
    success: bool
    path: str


@router.post("/read", response_model=FileReadResponse)
async def read_file(request: FileReadRequest) -> FileReadResponse:
    """Read a .netrun.json or .netrun.toml file and return UI-compatible data.

    Supports both GraphConfig (graph-only) and NetConfig (full config with pools).
    When loading NetConfig, non-graph data (pools, etc.) is returned in extra_data
    so it can be preserved when saving.
    """
    path = Path(request.path)

    if not path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")

    if not path.suffix in (".json", ".toml"):
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file format: {path.suffix}. Must be .json or .toml"
        )

    try:
        content = path.read_text()

        if path.suffix == ".json":
            data = json.loads(content)
            file_format = "json"
        else:  # .toml
            data = tomli.loads(content)
            file_format = "toml"

        # Extract graph config and any extra data (pools, net-level settings)
        graph_data, extra_data = extract_graph_and_extras(data)

        # Convert to UI format
        nodes, edges = graph_config_to_ui(graph_data)

        # Extract graph-level meta if present
        meta = graph_data.get("meta")

        return FileReadResponse(
            path=str(path),
            format=file_format,
            nodes=nodes,
            edges=edges,
            meta=meta,
            extra_data=extra_data if extra_data else None,
        )

    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")
    except tomli.TOMLDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid TOML: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading file: {e}")


@router.post("/save", response_model=FileSaveResponse)
async def save_file(request: FileSaveRequest) -> FileSaveResponse:
    """Save UI data to a .netrun.json or .netrun.toml file.

    If extra_data is provided (e.g., pools from a NetConfig file), it will be
    preserved in the output. Otherwise, produces a graph-only format.
    """
    path = Path(request.path)

    # Ensure directory exists
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Convert UI format to GraphConfig
        graph_config = ui_to_graph_config(request.nodes, request.edges)

        # Merge graph with any extra data (pools, net-level settings)
        output_data = merge_graph_with_extras(
            graph_config,
            request.extra_data or {},
            request.meta,
        )

        # Serialize based on format
        if request.format == "json":
            content = json.dumps(output_data, indent=2)
        elif request.format == "toml":
            content = tomli_w.dumps(output_data)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported format: {request.format}. Must be 'json' or 'toml'"
            )

        path.write_text(content)

        return FileSaveResponse(success=True, path=str(path))

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving file: {e}")


class ConvertRequest(BaseModel):
    """Request to convert between formats."""
    content: str
    from_format: str  # "json" or "toml"
    to_format: str    # "json" or "toml"


class ConvertResponse(BaseModel):
    """Response with converted content."""
    content: str


@router.post("/convert", response_model=ConvertResponse)
async def convert_format(request: ConvertRequest) -> ConvertResponse:
    """Convert file content between JSON and TOML formats."""
    try:
        # Parse input
        if request.from_format == "json":
            data = json.loads(request.content)
        elif request.from_format == "toml":
            data = tomli.loads(request.content)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported from_format: {request.from_format}"
            )

        # Convert output
        if request.to_format == "json":
            output = json.dumps(data, indent=2)
        elif request.to_format == "toml":
            output = tomli_w.dumps(data)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported to_format: {request.to_format}"
            )

        return ConvertResponse(content=output)

    except (json.JSONDecodeError, tomli.TOMLDecodeError) as e:
        raise HTTPException(status_code=400, detail=f"Parse error: {e}")


class DirectoryListRequest(BaseModel):
    """Request to list directory contents."""
    path: str
    include_hidden: bool = False


class FileEntry(BaseModel):
    """A file or directory entry."""
    name: str
    path: str
    is_dir: bool
    is_netrun_file: bool = False  # True for .netrun.json or .netrun.toml


class DirectoryListResponse(BaseModel):
    """Response with directory contents."""
    path: str
    parent: str | None
    entries: list[FileEntry]


@router.post("/list", response_model=DirectoryListResponse)
async def list_directory(request: DirectoryListRequest) -> DirectoryListResponse:
    """List contents of a directory, highlighting netrun config files."""
    path = Path(request.path).expanduser().resolve()

    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Directory not found: {path}")

    if not path.is_dir():
        raise HTTPException(status_code=400, detail=f"Path is not a directory: {path}")

    entries = []

    try:
        for item in sorted(path.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
            # Skip hidden files unless requested
            if not request.include_hidden and item.name.startswith('.'):
                continue

            is_netrun = (
                item.is_file() and
                (item.name.endswith('.netrun.json') or
                 item.name.endswith('.netrun.toml') or
                 item.suffix in ('.json', '.toml'))
            )

            entries.append(FileEntry(
                name=item.name,
                path=str(item),
                is_dir=item.is_dir(),
                is_netrun_file=is_netrun,
            ))

        # Get parent directory
        parent = str(path.parent) if path.parent != path else None

        return DirectoryListResponse(
            path=str(path),
            parent=parent,
            entries=entries,
        )

    except PermissionError:
        raise HTTPException(status_code=403, detail=f"Permission denied: {path}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing directory: {e}")
