"""Convert between UI format and netrun config format.

UI Format (flowStore.ts):
- nodes: list of {id, type, position: {x, y}, data: {label, nodeType, inPorts, outPorts, factory, factoryArgs, ...}}
- edges: list of {id, source, target, sourceHandle, targetHandle, ...}

GraphConfig Format (netrun.net.config):
- nodes: list of NodeConfig {name, in_ports, out_ports, in_salvo_conditions, out_salvo_conditions, factory, factory_args, extra, ...}
- edges: list of EdgeConfig {source_str, target_str} or {source, target}
- extra: optional dict for graph-level extra data

NetConfig Format (netrun.net.config):
- pools: dict[str, PoolConfig] (required)
- graph: GraphConfig
- extra: optional dict for net-level extra data
- ... other net-level settings
"""
from typing import Any
import importlib
import logging

from .import_utils import is_file_path_ref, import_module_from_ref, reload_module

from netrun.net.config import (
    NodeConfig as _NodeConfig,
    SubgraphConfig as _SubgraphConfig,
    EdgeConfig as _EdgeConfig,
    GraphConfig as _GraphConfig,
    PortConfig as _PortConfig,
)

logger = logging.getLogger(__name__)


def is_net_config(data: dict[str, Any]) -> bool:
    """Check if the data is a full NetConfig (vs just GraphConfig).

    NetConfig has a required 'pools' field and 'graph' field.
    GraphConfig has 'nodes' at the top level.
    """
    return "pools" in data or ("graph" in data and "nodes" in data.get("graph", {}))


def extract_graph_and_extras(data: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Extract graph data and extra (non-graph) data from a config file.

    Args:
        data: The parsed file data.

    Returns:
        Tuple of (graph_data, extra_data).
        - graph_data: The GraphConfig portion
        - extra_data: Everything else (pools, net-level meta, etc.)
    """
    if "graph" in data:
        # NetConfig format - graph is nested
        graph_data = data["graph"]
        extra_data = {k: v for k, v in data.items() if k != "graph"}
    else:
        # GraphConfig format - graph is at top level
        graph_data = data
        extra_data = {}

    return graph_data, extra_data


def merge_graph_with_extras(
    graph_config: dict[str, Any],
    extra_data: dict[str, Any],
    graph_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge graph config with extra data to produce final output.

    Args:
        graph_config: The GraphConfig portion.
        extra_data: Non-graph data to preserve (pools, net-level settings).
        graph_extra: Optional graph-level extra data to include.

    Returns:
        Complete config ready for serialization.
    """
    # Add graph-level extra if provided
    if graph_extra:
        graph_config = {**graph_config, "extra": graph_extra}

    if extra_data:
        # Has extra data - produce NetConfig format
        return {
            **extra_data,
            "graph": graph_config,
        }
    else:
        # No extra data - produce GraphConfig format (graph at top level)
        return {
            "graph": graph_config,
        }


def dump_graph_config(graph: "_GraphConfig") -> dict[str, Any]:
    """Serialize a GraphConfig model to a dict for file output.

    Uses model_dump() so all fields (including extra) are correctly serialized.
    """
    nodes = []
    for node in graph.nodes:
        d = node.model_dump(exclude_defaults=True, exclude_none=True)
        # Always include the type discriminator (it's a default so excluded above)
        d["type"] = node.type
        nodes.append(d)
    edges = [e.model_dump(exclude_none=True) for e in graph.edges]
    result: dict[str, Any] = {"nodes": nodes, "edges": edges}
    if graph.extra:
        result["extra"] = graph.extra
    return result


def _count_subgraph_nodes(subgraph: dict[str, Any]) -> int:
    """Count the total number of nodes inside a subgraph (including nested)."""
    nodes = subgraph.get("nodes", [])
    count = 0
    for node in nodes:
        if node.get("type") == "subgraph":
            count += _count_subgraph_nodes(node)
        else:
            count += 1
    return count


def resolve_factory_ports(
    factory_path: str,
    factory_args: dict[str, Any],
    working_dir: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], str | None, dict[str, Any] | None] | None:
    """Resolve ports from a factory by calling get_node_config.

    Args:
        factory_path: Import path to the factory module (e.g. "netrun.node_factories.from_function").
        factory_args: Arguments to pass to get_node_config.
        working_dir: Optional working directory to add to sys.path for imports.

    Returns:
        Tuple of (in_ports, out_ports, description, extra) dicts, or None if resolution fails.
    """
    import sys

    # Temporarily add working directory to sys.path for local imports
    added_to_path = False
    if working_dir and working_dir not in sys.path:
        sys.path.insert(0, working_dir)
        added_to_path = True

    try:
        module, attr_name = import_module_from_ref(factory_path, base_dir=working_dir)

        if attr_name is not None:
            get_node_config = getattr(module, attr_name)
        elif hasattr(module, "get_node_config"):
            get_node_config = getattr(module, "get_node_config")
        else:
            logger.warning(f"Factory module '{factory_path}' has no get_node_config")
            return None

        # Filter out empty string values so factory defaults are used
        filtered_args = {
            k: v for k, v in factory_args.items()
            if v != "" and v is not None
        }
        node_config = get_node_config(**filtered_args)

        # Extract ports from the NodeConfig
        in_ports = {}
        for name, port in node_config.in_ports.items():
            port_dict = {}
            if hasattr(port, "port_type") and port.port_type is not None:
                if isinstance(port.port_type, str):
                    port_dict["port_type"] = port.port_type
                elif hasattr(port.port_type, "__name__"):
                    port_dict["port_type"] = port.port_type.__name__
            in_ports[name] = port_dict

        out_ports = {}
        for name, port in node_config.out_ports.items():
            port_dict = {}
            if hasattr(port, "port_type") and port.port_type is not None:
                if isinstance(port.port_type, str):
                    port_dict["port_type"] = port.port_type
                elif hasattr(port.port_type, "__name__"):
                    port_dict["port_type"] = port.port_type.__name__
            out_ports[name] = port_dict

        description = getattr(node_config, 'description', None)
        factory_extra = None
        if hasattr(node_config, 'extra') and node_config.extra:
            factory_extra = node_config.extra if isinstance(node_config.extra, dict) else node_config.extra.copy()
        return in_ports, out_ports, description, factory_extra

    except Exception as e:
        logger.warning(f"Failed to resolve factory '{factory_path}': {e}")
        return None

    finally:
        # Clean up sys.path
        if added_to_path and working_dir in sys.path:
            sys.path.remove(working_dir)


def graph_config_to_ui(
    graph_data: dict[str, Any],
    working_dir: str | None = None,
) -> tuple[list[dict], list[dict]]:
    """Convert GraphConfig-style data to UI format.

    Args:
        graph_data: Dictionary with "nodes" and "edges" keys in GraphConfig format.
        working_dir: Optional working directory for resolving factory imports.

    Returns:
        Tuple of (ui_nodes, ui_edges) ready for SvelteFlow.
    """
    ui_nodes = []
    ui_edges = []

    nodes_data = graph_data.get("nodes", [])
    edges_data = graph_data.get("edges", [])

    # Track node positions from meta.ui if available
    for i, node in enumerate(nodes_data):
        node_name = node.get("name", f"node_{i}")

        # Extract extra.ui for position, dimensions, and other UI data
        extra = node.get("extra", {})
        ui_extra = extra.get("ui", {})
        position = ui_extra.get("position", {"x": i * 200, "y": 100})
        dimensions = ui_extra.get("dimensions")  # {"width": ..., "height": ...} or None

        # Check if this is a subgraph
        is_subgraph = node.get("type") == "subgraph"

        if is_subgraph:
            # Handle subgraph node
            exposed_in_ports = node.get("exposed_in_ports", {})
            exposed_out_ports = node.get("exposed_out_ports", {})

            # Convert exposed ports to UI format
            in_ports = [
                {"name": exposed_name, "type": None}
                for exposed_name in exposed_in_ports.keys()
            ]
            out_ports = [
                {"name": exposed_name, "type": None}
                for exposed_name in exposed_out_ports.keys()
            ]

            # Determine source type
            is_file_ref = node.get("path") is not None
            source = node.get("path") if is_file_ref else "Inline"
            node_count = _count_subgraph_nodes(node) if not is_file_ref else None

            ui_node = {
                "id": node_name,
                "type": "subgraphNode",
                "position": position,
                "data": {
                    "label": node_name,
                    "nodeType": "subgraph",
                    "inPorts": in_ports,
                    "outPorts": out_ports,
                    "isValid": True,
                    "source": source,
                    "nodeCount": node_count,
                    # Store the full subgraph config for round-trip
                    "_subgraphConfig": {
                        k: v for k, v in node.items()
                        if k not in ("extra",)  # Exclude extra, we handle it separately
                    },
                },
            }
            if node.get("description"):
                ui_node["data"]["description"] = node["description"]
            if dimensions:
                ui_node["width"] = dimensions.get("width")
                ui_node["height"] = dimensions.get("height")
        else:
            # Handle regular node
            # Determine node type
            is_factory = node.get("factory") is not None
            node_type = "factory" if is_factory else "regular"

            # Get ports from config
            config_in_ports = node.get("in_ports", {})
            config_out_ports = node.get("out_ports", {})

            # For factory nodes without explicit ports, try to resolve from factory
            factory_description = None
            factory_extra = None
            if is_factory and not config_in_ports and not config_out_ports:
                factory_path = node.get("factory")
                factory_args = node.get("factory_args", {})
                resolved = resolve_factory_ports(factory_path, factory_args, working_dir)
                if resolved:
                    config_in_ports, config_out_ports, factory_description, factory_extra = resolved

            # Convert ports to UI format
            in_ports = [
                {"name": name, "type": port.get("port_type")}
                for name, port in config_in_ports.items()
            ]
            out_ports = [
                {"name": name, "type": port.get("port_type")}
                for name, port in config_out_ports.items()
            ]

            ui_node = {
                "id": node_name,
                "type": "netrunNode",
                "position": position,
                "data": {
                    "label": node_name,
                    "nodeType": node_type,
                    "inPorts": in_ports,
                    "outPorts": out_ports,
                    "isValid": True,
                },
            }

            if is_factory:
                ui_node["data"]["factory"] = node.get("factory")
                ui_node["data"]["factoryArgs"] = node.get("factory_args", {})

            # Store original config data for non-UI fields
            # Include 'extra' so node-level extra data (env, actions, etc.)
            # survives the round-trip through the UI.
            ui_node["data"]["_config"] = {
                k: v for k, v in node.items()
                if k not in ("name", "in_ports", "out_ports", "factory", "factory_args", "type", "description")
            }

            # Merge factory extra.ui defaults (node's own values take precedence)
            if factory_extra and "ui" in factory_extra:
                config = ui_node["data"]["_config"]
                config_extra = config.setdefault("extra", {})
                config_ui = config_extra.setdefault("ui", {})
                for key, value in factory_extra["ui"].items():
                    if key not in config_ui:
                        config_ui[key] = value

            # Promote description to a first-class UI field
            # Use explicit node description, falling back to factory-resolved description
            node_description = node.get("description")
            if node_description:
                ui_node["data"]["description"] = node_description
            elif factory_description:
                ui_node["data"]["description"] = factory_description

            if dimensions:
                ui_node["width"] = dimensions.get("width")
                ui_node["height"] = dimensions.get("height")

        ui_nodes.append(ui_node)

    # Build name -> id mapping for edges
    name_to_id = {}
    for node in ui_nodes:
        # Use the label (which is the node name) to find node IDs
        name_to_id[node["data"]["label"]] = node["id"]

    # Convert edges
    for i, edge in enumerate(edges_data):
        # Parse edge source/target
        if edge.get("source_str"):
            source_parts = edge["source_str"].split(".")
            target_parts = edge["target_str"].split(".")
            source_node = source_parts[0]
            source_port = source_parts[1]
            target_node = target_parts[0]
            target_port = target_parts[1]
        else:
            source_ref = edge.get("source", {})
            target_ref = edge.get("target", {})
            source_node = source_ref.get("node_name", "")
            source_port = source_ref.get("port_name", "")
            target_node = target_ref.get("node_name", "")
            target_port = target_ref.get("port_name", "")

        # Get node IDs
        source_id = name_to_id.get(source_node, source_node)
        target_id = name_to_id.get(target_node, target_node)

        ui_edge = {
            "id": f"edge-{i}",
            "source": source_id,
            "target": target_id,
            "sourceHandle": source_port,
            "targetHandle": target_port,
            "type": "smoothstep",
        }

        ui_edges.append(ui_edge)

    return ui_nodes, ui_edges


def _build_merged_extra(data: dict, position: dict, dimensions: dict | None = None) -> dict:
    """Build merged extra dict from _config.extra and current UI position/dimensions."""
    extra_config = data.get("_config", {})
    saved_extra = extra_config.get("extra", {})
    saved_ui = saved_extra.get("ui", {}) if isinstance(saved_extra, dict) else {}
    merged_ui = {**saved_ui, "position": position}
    if dimensions:
        merged_ui["dimensions"] = dimensions
    elif "dimensions" in merged_ui:
        # Remove stale dimensions if node was reset to auto-size
        del merged_ui["dimensions"]
    return {**saved_extra, "ui": merged_ui} if isinstance(saved_extra, dict) else {"ui": merged_ui}


def ui_to_graph_config(
    ui_nodes: list[dict],
    ui_edges: list[dict],
    graph_extra: dict[str, Any] | None = None,
) -> _GraphConfig:
    """Convert UI format to GraphConfig.

    Args:
        ui_nodes: List of UI nodes from SvelteFlow.
        ui_edges: List of UI edges from SvelteFlow.
        graph_extra: Optional graph-level extra data.

    Returns:
        GraphConfig model.
    """
    return _ui_to_graph_config_model(ui_nodes, ui_edges, graph_extra)


def _ui_to_graph_config_model(
    ui_nodes: list[dict],
    ui_edges: list[dict],
    graph_extra: dict[str, Any] | None = None,
) -> "_GraphConfig":
    """Build GraphConfig using pydantic models for correct serialization."""
    config_nodes: list[_NodeConfig | _SubgraphConfig] = []
    config_edges: list[_EdgeConfig] = []

    for node in ui_nodes:
        data = node.get("data", {})
        position = node.get("position", {"x": 0, "y": 0})
        node_type = data.get("nodeType")
        node_name = node["id"]
        extra_config = data.get("_config", {})
        width = node.get("width")
        height = node.get("height")
        dimensions = {"width": width, "height": height} if width is not None and height is not None else None
        merged_extra = _build_merged_extra(data, position, dimensions)

        if node_type == "subgraph" or node.get("type") == "subgraphNode":
            subgraph_config = data.get("_subgraphConfig", {})
            subgraph_kwargs = {
                **subgraph_config,
                "name": node_name,
                "extra": merged_extra,
            }
            if data.get("description"):
                subgraph_kwargs["description"] = data["description"]
            config_node = _SubgraphConfig.model_validate(subgraph_kwargs)

        elif node_type == "factory":
            # Build factory NodeConfig — no in_ports/out_ports (factory generates them)
            kwargs: dict[str, Any] = {
                "name": node_name,
                "extra": merged_extra,
            }
            if data.get("factory"):
                kwargs["factory"] = data["factory"]
            if data.get("factoryArgs"):
                filtered_args = {
                    k: v for k, v in data["factoryArgs"].items()
                    if v != "" and v is not None
                }
                if filtered_args:
                    kwargs["factory_args"] = filtered_args
            if "execution_config" in extra_config:
                kwargs["execution_config"] = extra_config["execution_config"]
            if data.get("description"):
                kwargs["description"] = data["description"]
            config_node = _NodeConfig(**kwargs)

        else:
            # Regular node — include port info and restore extra config fields
            in_ports = {}
            for port in data.get("inPorts", []):
                if port.get("type"):
                    in_ports[port["name"]] = _PortConfig(port_type=port["type"])
                else:
                    in_ports[port["name"]] = _PortConfig()

            out_ports = {}
            for port in data.get("outPorts", []):
                if port.get("type"):
                    out_ports[port["name"]] = _PortConfig(port_type=port["type"])
                else:
                    out_ports[port["name"]] = _PortConfig()

            kwargs = {
                "name": node_name,
                "in_ports": in_ports,
                "out_ports": out_ports,
                "extra": merged_extra,
            }
            # Restore known fields from _config (execution_config, salvo conditions, etc.)
            for key, value in extra_config.items():
                if key not in ("extra",) and key not in kwargs:
                    kwargs[key] = value
            if data.get("description"):
                kwargs["description"] = data["description"]
            config_node = _NodeConfig(**kwargs)

        config_nodes.append(config_node)

    # Convert edges
    for edge in ui_edges:
        source_name = edge["source"]
        target_name = edge["target"]
        source_handle = edge.get("sourceHandle", "out")
        target_handle = edge.get("targetHandle", "in")
        config_edges.append(_EdgeConfig(
            source_str=f"{source_name}.{source_handle}",
            target_str=f"{target_name}.{target_handle}",
        ))

    return _GraphConfig(
        nodes=config_nodes,
        edges=config_edges,
        extra=graph_extra or {},
    )


