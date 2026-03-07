"""File I/O endpoints for netrun config files."""
import json
from pathlib import Path
from typing import Any

import tomli
import tomli_w
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, ValidationError


def is_netrun_file(name: str) -> bool:
    """Check if a filename is a valid netrun config file.

    Accepts both 'foo.netrun.json' and plain 'netrun.json'/'netrun.toml'.
    """
    return (
        name.endswith('.netrun.json') or name.endswith('.netrun.toml')
        or name == 'netrun.json' or name == 'netrun.toml'
    )

from ..converter import (
    ui_to_graph_config,
    graph_config_to_ui,
    extract_graph_and_extras,
    merge_graph_with_extras,
    dump_graph_config,
    _PrefixedValidationError,
)

from netrun.net.config import NetConfig, GraphConfig, SubgraphConfig
from netrun.tools._models import ActionConfig, RecipeConfig

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
    extra: dict[str, Any] | None = None
    extra_data: dict[str, Any] | None = None  # Non-graph data (pools, etc.)


class FileSaveRequest(BaseModel):
    """Request to save a file."""
    path: str
    format: str  # "json" or "toml"
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    extra: dict[str, Any] | None = None
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
    path = Path(request.path).resolve()

    if not path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")

    if not is_netrun_file(path.name):
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file format: {path.name}. Must be .netrun.json or .netrun.toml"
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

        # Convert to UI format (pass the file's directory for factory resolution)
        working_dir = str(path.parent)
        nodes, edges = graph_config_to_ui(graph_data, working_dir=working_dir)

        # Extract graph-level extra if present
        extra = graph_data.get("extra")

        return FileReadResponse(
            path=str(path),
            format=file_format,
            nodes=nodes,
            edges=edges,
            extra=extra,
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
    path = Path(request.path).resolve()

    # Ensure directory exists
    path.parent.mkdir(parents=True, exist_ok=True)

    # Check for duplicate node names
    node_names = [n.get("data", {}).get("label", n.get("id", "")) for n in request.nodes]
    seen: set[str] = set()
    duplicates: list[str] = []
    for name in node_names:
        if name in seen:
            duplicates.append(name)
        seen.add(name)
    if duplicates:
        raise HTTPException(
            status_code=400,
            detail=f"Duplicate node names: {', '.join(sorted(set(duplicates)))}"
        )

    try:
        # Convert UI format to GraphConfig (model when netrun available, dict otherwise)
        graph = ui_to_graph_config(request.nodes, request.edges, request.extra)

        # Serialize to dict
        graph_dict = dump_graph_config(graph)

        # Merge graph with any extra data (pools, net-level settings)
        output_data = merge_graph_with_extras(
            graph_dict,
            request.extra_data or {},
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

    except HTTPException:
        raise
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

            is_netrun = item.is_file() and is_netrun_file(item.name)

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


# =============================================================================
# Subgraph Endpoints
# =============================================================================


class SubgraphLoadRequest(BaseModel):
    """Request to load subgraph content."""
    path: str | None = None  # Path to external file
    base_path: str | None = None  # Base path for resolving relative paths
    project_root: str | None = None  # Explicit project root for resolving relative paths
    inline_config: dict[str, Any] | None = None  # Inline subgraph config


class SubgraphLoadResponse(BaseModel):
    """Response with subgraph content for editing."""
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    exposed_in_ports: dict[str, Any]
    exposed_out_ports: dict[str, Any]
    source: str  # "Inline" or file path


@router.post("/subgraph/load", response_model=SubgraphLoadResponse)
async def load_subgraph(request: SubgraphLoadRequest) -> SubgraphLoadResponse:
    """Load subgraph content for editing.

    Can load from:
    1. File path - reads external .netrun.json file
    2. Inline config - uses provided subgraph configuration
    """
    if request.path and request.inline_config:
        raise HTTPException(
            status_code=400,
            detail="Provide either 'path' or 'inline_config', not both"
        )

    if not request.path and not request.inline_config:
        raise HTTPException(
            status_code=400,
            detail="Must provide either 'path' or 'inline_config'"
        )

    try:
        if request.path:
            # Load from file
            path = Path(request.path)

            # If path is relative, resolve against project_root or base_path
            if not path.is_absolute():
                if request.project_root:
                    base_dir = Path(request.project_root)
                    # Relative project_root must be resolved against the config file's directory
                    if not base_dir.is_absolute() and request.base_path:
                        base_dir = (Path(request.base_path).parent / base_dir).resolve()
                elif request.base_path:
                    base_dir = Path(request.base_path).parent
                else:
                    base_dir = None
                if base_dir:
                    path = (base_dir / path).resolve()
            else:
                path = path.resolve()

            if not path.exists():
                raise HTTPException(status_code=404, detail=f"File not found: {path}")

            if not path.is_file():
                raise HTTPException(status_code=400, detail=f"Path is not a file: {path}")

            if not is_netrun_file(path.name):
                raise HTTPException(
                    status_code=400,
                    detail=f"Not a valid netrun file (expected .netrun.json or .netrun.toml): {path.name}"
                )

            content = path.read_text()
            if path.suffix == '.toml':
                data = tomli.loads(content)
            else:
                data = json.loads(content)

            # Extract graph data (could be GraphConfig or NetConfig format)
            graph_data, _ = extract_graph_and_extras(data)

            # Pass working directory for factory resolution
            working_dir = str(path.parent)
            nodes, edges = graph_config_to_ui(graph_data, working_dir=working_dir)
            source = str(path)
            exposed_in_ports = {}
            exposed_out_ports = {}

        else:
            # Load from inline config
            config = request.inline_config

            # Get nodes and edges from inline subgraph
            nodes_data = config.get("nodes", [])
            edges_data = config.get("edges", [])

            graph_data = {"nodes": nodes_data, "edges": edges_data}
            # Use project_root or base_path's directory for factory resolution
            if request.project_root:
                working_dir = request.project_root
            elif request.base_path:
                working_dir = str(Path(request.base_path).parent)
            else:
                working_dir = None
            nodes, edges = graph_config_to_ui(graph_data, working_dir=working_dir)

            source = "Inline"
            exposed_in_ports = config.get("exposed_in_ports", {})
            exposed_out_ports = config.get("exposed_out_ports", {})

        return SubgraphLoadResponse(
            nodes=nodes,
            edges=edges,
            exposed_in_ports=exposed_in_ports,
            exposed_out_ports=exposed_out_ports,
            source=source,
        )

    except HTTPException:
        raise  # Re-raise HTTP exceptions as-is
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading subgraph: {e}")


class SubgraphCreateRequest(BaseModel):
    """Request to create a subgraph from selected nodes."""
    subgraph_name: str
    selected_node_ids: list[str]
    all_nodes: list[dict[str, Any]]
    all_edges: list[dict[str, Any]]


class SubgraphCreateResponse(BaseModel):
    """Response with the new subgraph and updated graph."""
    subgraph_node: dict[str, Any]  # The new subgraph UI node
    remaining_nodes: list[dict[str, Any]]  # Nodes not in subgraph
    remaining_edges: list[dict[str, Any]]  # Edges not inside subgraph
    internal_edges: list[dict[str, Any]]  # Edges inside the subgraph


@router.post("/subgraph/create", response_model=SubgraphCreateResponse)
async def create_subgraph(request: SubgraphCreateRequest) -> SubgraphCreateResponse:
    """Create a subgraph from selected nodes.

    This endpoint:
    1. Extracts selected nodes
    2. Identifies internal edges (edges between selected nodes)
    3. Identifies boundary edges (edges crossing the selection boundary)
    4. Auto-creates exposed ports for boundary edges
    5. Returns the new subgraph node and updated graph
    """
    selected_ids = set(request.selected_node_ids)

    if len(selected_ids) < 1:
        raise HTTPException(status_code=400, detail="Must select at least one node")

    # Separate nodes
    selected_nodes = []
    remaining_nodes = []

    for node in request.all_nodes:
        if node["id"] in selected_ids:
            selected_nodes.append(node)
        else:
            remaining_nodes.append(node)

    if len(selected_nodes) != len(selected_ids):
        raise HTTPException(status_code=400, detail="Some selected nodes not found")

    # Classify edges
    internal_edges = []  # Both ends inside subgraph
    incoming_edges = []  # Target inside, source outside
    outgoing_edges = []  # Source inside, target outside
    external_edges = []  # Both ends outside

    for edge in request.all_edges:
        source_inside = edge["source"] in selected_ids
        target_inside = edge["target"] in selected_ids

        if source_inside and target_inside:
            internal_edges.append(edge)
        elif not source_inside and target_inside:
            incoming_edges.append(edge)
        elif source_inside and not target_inside:
            outgoing_edges.append(edge)
        else:
            external_edges.append(edge)

    # Build id -> label mapping for selected nodes
    id_to_label = {}
    for node in selected_nodes:
        id_to_label[node["id"]] = node.get("data", {}).get("label", node["id"])

    # Auto-create exposed ports from boundary edges
    exposed_in_ports = {}
    exposed_out_ports = {}
    new_edges_to_subgraph = []

    for edge in incoming_edges:
        # Create exposed input port
        target_label = id_to_label.get(edge["target"], edge["target"])
        port_name = edge.get("targetHandle", "in")
        exposed_name = f"{target_label}_{port_name}"

        # Ensure unique names
        base_name = exposed_name
        counter = 1
        while exposed_name in exposed_in_ports:
            exposed_name = f"{base_name}_{counter}"
            counter += 1

        exposed_in_ports[exposed_name] = {
            "internal_node": target_label,
            "internal_port": port_name,
        }

        # Update edge to point to subgraph's exposed port
        new_edges_to_subgraph.append({
            **edge,
            "target": request.subgraph_name,
            "targetHandle": exposed_name,
        })

    for edge in outgoing_edges:
        # Create exposed output port
        source_label = id_to_label.get(edge["source"], edge["source"])
        port_name = edge.get("sourceHandle", "out")
        exposed_name = f"{source_label}_{port_name}"

        # Ensure unique names
        base_name = exposed_name
        counter = 1
        while exposed_name in exposed_out_ports:
            exposed_name = f"{base_name}_{counter}"
            counter += 1

        exposed_out_ports[exposed_name] = {
            "internal_node": source_label,
            "internal_port": port_name,
        }

        # Update edge to come from subgraph's exposed port
        new_edges_to_subgraph.append({
            **edge,
            "source": request.subgraph_name,
            "sourceHandle": exposed_name,
        })

    # Calculate subgraph position (average of selected nodes)
    if selected_nodes:
        avg_x = sum(n.get("position", {}).get("x", 0) for n in selected_nodes) / len(selected_nodes)
        avg_y = sum(n.get("position", {}).get("y", 0) for n in selected_nodes) / len(selected_nodes)
    else:
        avg_x, avg_y = 0, 0

    # Build internal nodes config (convert UI nodes to config format)
    internal_nodes_config = []
    for node in selected_nodes:
        data = node.get("data", {})
        # Relativize positions
        rel_x = node.get("position", {}).get("x", 0) - avg_x + 200
        rel_y = node.get("position", {}).get("y", 0) - avg_y + 100

        node_config = {
            "type": data.get("nodeType", "node") if data.get("nodeType") != "subgraph" else "subgraph",
            "name": node["id"],
            "in_ports": {p["name"]: {} for p in data.get("inPorts", [])},
            "out_ports": {p["name"]: {} for p in data.get("outPorts", [])},
            "extra": {
                "ui": {
                    "position": {"x": rel_x, "y": rel_y},
                }
            },
        }

        # Preserve _subgraphConfig if it was a subgraph
        if data.get("_subgraphConfig"):
            node_config = {
                **data["_subgraphConfig"],
                "name": data.get("label", node["id"]),
                "extra": node_config["extra"],
            }

        # Preserve _config if present
        if data.get("_config"):
            for k, v in data["_config"].items():
                if k not in node_config:
                    node_config[k] = v

        internal_nodes_config.append(node_config)

    # Build internal edges config
    internal_edges_config = []
    for edge in internal_edges:
        source_label = id_to_label.get(edge["source"], edge["source"])
        target_label = id_to_label.get(edge["target"], edge["target"])
        internal_edges_config.append({
            "source_str": f"{source_label}.{edge.get('sourceHandle', 'out')}",
            "target_str": f"{target_label}.{edge.get('targetHandle', 'in')}",
        })

    # Create the subgraph UI node
    subgraph_node = {
        "id": request.subgraph_name,
        "type": "subgraphNode",
        "position": {"x": avg_x, "y": avg_y},
        "data": {
            "label": request.subgraph_name,
            "nodeType": "subgraph",
            "inPorts": [{"name": name, "type": None} for name in exposed_in_ports.keys()],
            "outPorts": [{"name": name, "type": None} for name in exposed_out_ports.keys()],
            "isValid": True,
            "source": "Inline",
            "nodeCount": len(selected_nodes),
            "_subgraphConfig": {
                "type": "subgraph",
                "name": request.subgraph_name,
                "nodes": internal_nodes_config,
                "edges": internal_edges_config,
                "exposed_in_ports": exposed_in_ports,
                "exposed_out_ports": exposed_out_ports,
            },
        },
    }

    # Combine external edges with new edges to subgraph
    remaining_edges = external_edges + new_edges_to_subgraph

    return SubgraphCreateResponse(
        subgraph_node=subgraph_node,
        remaining_nodes=remaining_nodes,
        remaining_edges=remaining_edges,
        internal_edges=internal_edges,
    )


# =============================================================================
# Validation Endpoint
# =============================================================================


class ValidationError_(BaseModel):
    """A single validation error."""
    loc: list[str | int]  # Location path in the config
    msg: str  # Error message
    type: str  # Error type


class ValidateRequest(BaseModel):
    """Request to validate a config."""
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    extra: dict[str, Any] | None = None
    extra_data: dict[str, Any] | None = None
    file_path: str | None = None  # Config file path (for project_root resolution)


class ValidateResponse(BaseModel):
    """Response with validation results."""
    valid: bool
    errors: list[ValidationError_]


def validate_tool_configs(graph: Any, extra_data: dict[str, Any]) -> list[ValidationError_]:
    """Validate ActionConfig and RecipeConfig within graph and extra_data."""
    errors: list[ValidationError_] = []

    # Helper to extract extra dict from model or dict
    def _get_extra(obj: Any) -> dict:
        if hasattr(obj, "extra"):
            return obj.extra or {}
        if isinstance(obj, dict):
            return obj.get("extra", {})
        return {}

    def _get_nodes(obj: Any) -> list:
        if hasattr(obj, "nodes"):
            return obj.nodes or []
        if isinstance(obj, dict):
            return obj.get("nodes", [])
        return []

    # Project-level actions: graph.extra.ui.actions
    graph_extra = _get_extra(graph)
    for i, action in enumerate(graph_extra.get("ui", {}).get("actions", [])):
        try:
            ActionConfig.model_validate(action)
        except ValidationError as e:
            for err in e.errors():
                errors.append(ValidationError_(
                    loc=["graph", "extra", "ui", "actions", str(i)]
                        + [str(x) for x in err["loc"]],
                    msg=err["msg"],
                    type=err["type"],
                ))

    # Node-level actions: node.extra.ui.actions
    for idx, node in enumerate(_get_nodes(graph)):
        node_extra = _get_extra(node)
        for i, action in enumerate(node_extra.get("ui", {}).get("actions", [])):
            try:
                ActionConfig.model_validate(action)
            except ValidationError as e:
                for err in e.errors():
                    errors.append(ValidationError_(
                        loc=["graph", "nodes", str(idx), "extra", "ui", "actions", str(i)]
                            + [str(x) for x in err["loc"]],
                        msg=err["msg"],
                        type=err["type"],
                    ))

    # Recipes: extra_data.recipes
    for name, recipe in extra_data.get("recipes", {}).items():
        try:
            RecipeConfig.model_validate(recipe)
        except ValidationError as e:
            for err in e.errors():
                errors.append(ValidationError_(
                    loc=["recipes", name] + [str(x) for x in err["loc"]],
                    msg=err["msg"],
                    type=err["type"],
                ))

    return errors


@router.post("/validate", response_model=ValidateResponse)
async def validate_config(request: ValidateRequest) -> ValidateResponse:
    """Validate UI data against NetConfig/GraphConfig models, then resolve.

    Performs:
    1. Structural validation via pydantic model_validate
    2. Full resolution (factory expansion, import resolution, subgraph flattening)
    3. Tool config validation (actions, recipes)
    """
    errors: list[ValidationError_] = []

    # Filter out decoration nodes before validation
    nodes = [n for n in request.nodes if n.get("data", {}).get("nodeType") != "decoration"]

    # Step 1: Build graph config (structural validation via pydantic models)
    try:
        graph = ui_to_graph_config(nodes, request.edges, request.extra)
    except _PrefixedValidationError as e:
        # Per-node validation error with node index already in loc
        for err in e.original.errors():
            errors.append(ValidationError_(
                loc=["graph", "nodes", str(e.node_idx)] + [str(x) for x in err.get("loc", [])],
                msg=err.get("msg", "Unknown error"),
                type=err.get("type", "unknown"),
            ))
        return ValidateResponse(valid=False, errors=errors)
    except ValidationError as e:
        for err in e.errors():
            errors.append(ValidationError_(
                loc=[str(x) for x in err.get("loc", [])],
                msg=err.get("msg", "Unknown error"),
                type=err.get("type", "unknown"),
            ))
        return ValidateResponse(valid=False, errors=errors)
    except Exception as e:
        return ValidateResponse(
            valid=False,
            errors=[ValidationError_(loc=["_root_"], msg=str(e), type="validation_error")],
        )

    try:
        # Step 2: Pre-resolution config validation (fan-out, missing nodes/ports, etc.)
        config_errors = graph.validate()
        for err in config_errors:
            errors.append(ValidationError_(
                loc=["graph"] + [str(x) for x in err.loc],
                msg=err.msg,
                type=err.type,
            ))

        # Step 3: Validate as NetConfig if extra_data present
        net_config = None
        if request.extra_data:
            graph_dict = dump_graph_config(graph)
            full_config = merge_graph_with_extras(graph_dict, request.extra_data or {})
            try:
                net_config = NetConfig.model_validate(full_config)
                # Set _file_path so project_root_path resolves relative paths correctly
                if request.file_path:
                    net_config._file_path = Path(request.file_path).resolve()
            except ValidationError as e:
                for err in e.errors():
                    errors.append(ValidationError_(
                        loc=[str(x) for x in err.get("loc", [])],
                        msg=err.get("msg", "Unknown error"),
                        type=err.get("type", "unknown"),
                    ))

        # Step 4: Resolve each node individually for per-node error attribution
        # Derive base_path from project_root_override (or fall back to file_path dir)
        # Check both "project_root_override" (field name) and "project_root" (alias)
        ed = request.extra_data or {}
        pr_override = ed.get("project_root_override") or ed.get("project_root")
        if pr_override:
            p = Path(pr_override)
            if p.is_absolute():
                resolve_base_path = p
            elif request.file_path:
                resolve_base_path = (Path(request.file_path).resolve().parent / p).resolve()
            else:
                resolve_base_path = None
        elif request.file_path:
            resolve_base_path = Path(request.file_path).resolve().parent
        else:
            resolve_base_path = None
        for idx, node in enumerate(graph.nodes):
            if hasattr(node, 'resolve'):
                try:
                    if isinstance(node, SubgraphConfig):
                        # SubgraphConfig.resolve() only accepts base_path, not net_config
                        node.resolve(base_path=resolve_base_path)
                    else:
                        node.resolve(net_config=net_config)
                except Exception as e:
                    errors.append(ValidationError_(
                        loc=["graph", "nodes", str(idx)],
                        msg=str(e),
                        type="resolve_error",
                    ))

        # Step 5: Validate tool configs (actions, recipes)
        errors.extend(validate_tool_configs(graph, request.extra_data or {}))

        return ValidateResponse(
            valid=len(errors) == 0,
            errors=errors,
        )

    except Exception as e:
        return ValidateResponse(
            valid=False,
            errors=[ValidationError_(
                loc=["_root_"],
                msg=str(e),
                type="validation_error",
            )],
        )


# --- Image serving for decoration nodes ---

_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".bmp", ".ico"}


@router.get("/image")
async def serve_image(
    path: str = Query(..., description="Image path relative to project_root"),
    project_root: str = Query(..., description="Absolute project root path"),
) -> FileResponse:
    """Serve an image file from the project root for decoration nodes."""
    root = Path(project_root).resolve()
    image_path = (root / path).resolve()

    # Security: ensure the resolved path is within the project root
    if not image_path.is_relative_to(root):
        raise HTTPException(status_code=403, detail="Path traversal not allowed")

    if not image_path.exists():
        raise HTTPException(status_code=404, detail=f"Image not found: {path}")

    if not image_path.is_file():
        raise HTTPException(status_code=400, detail="Path is not a file")

    if image_path.suffix.lower() not in _IMAGE_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"Unsupported image type: {image_path.suffix}")

    return FileResponse(image_path)
