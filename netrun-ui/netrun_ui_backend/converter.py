"""Convert between UI format and netrun config format.

UI Format (flowStore.ts):
- nodes: list of {id, type, position: {x, y}, data: {label, nodeType, inPorts, outPorts, factory, factoryArgs, ...}}
- edges: list of {id, source, target, sourceHandle, targetHandle, ...}

GraphConfig Format (netrun.net.config):
- nodes: list of NodeConfig {name, in_ports, out_ports, in_salvo_conditions, out_salvo_conditions, factory, factory_args, meta, ...}
- edges: list of EdgeConfig {source_str, target_str} or {source, target}
- meta: optional dict for graph-level metadata

NetConfig Format (netrun.net.config):
- pools: dict[str, PoolConfig] (required)
- graph: GraphConfig
- meta: optional dict for net-level metadata
- ... other net-level settings
"""
from typing import Any
import importlib
import logging

from .import_utils import is_file_path_ref, import_module_from_ref, reload_module

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
    graph_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge graph config with extra data to produce final output.

    Args:
        graph_config: The GraphConfig portion.
        extra_data: Non-graph data to preserve (pools, net-level settings).
        graph_meta: Optional graph-level meta to include.

    Returns:
        Complete config ready for serialization.
    """
    # Add graph-level meta if provided
    if graph_meta:
        graph_config = {**graph_config, "meta": graph_meta}

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
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Resolve ports from a factory by calling get_node_config.

    Args:
        factory_path: Import path to the factory module (e.g. "netrun.node_factories.from_function").
        factory_args: Arguments to pass to get_node_config.
        working_dir: Optional working directory to add to sys.path for imports.

    Returns:
        Tuple of (in_ports, out_ports) dicts, or None if resolution fails.
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
        node_config = get_node_config(**factory_args)

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

        return in_ports, out_ports

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

        # Extract meta.ui for position and other UI data
        meta = node.get("meta", {})
        ui_meta = meta.get("ui", {})
        position = ui_meta.get("position", {"x": i * 200, "y": 100})

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
                        if k not in ("meta",)  # Exclude meta, we handle it separately
                    },
                },
            }
        else:
            # Handle regular node
            # Determine node type
            is_factory = node.get("factory") is not None
            node_type = "factory" if is_factory else "regular"

            # Get ports from config
            config_in_ports = node.get("in_ports", {})
            config_out_ports = node.get("out_ports", {})

            # For factory nodes without explicit ports, try to resolve from factory
            if is_factory and not config_in_ports and not config_out_ports:
                factory_path = node.get("factory")
                factory_args = node.get("factory_args", {})
                resolved = resolve_factory_ports(factory_path, factory_args, working_dir)
                if resolved:
                    config_in_ports, config_out_ports = resolved

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
            ui_node["data"]["_config"] = {
                k: v for k, v in node.items()
                if k not in ("name", "in_ports", "out_ports", "factory", "factory_args", "meta", "type")
            }

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


def ui_to_graph_config(ui_nodes: list[dict], ui_edges: list[dict]) -> dict[str, Any]:
    """Convert UI format to GraphConfig-style data.

    Args:
        ui_nodes: List of UI nodes from SvelteFlow.
        ui_edges: List of UI edges from SvelteFlow.

    Returns:
        Dictionary in GraphConfig format for serialization.
    """
    config_nodes = []
    config_edges = []

    # Convert nodes
    for node in ui_nodes:
        data = node.get("data", {})
        position = node.get("position", {"x": 0, "y": 0})
        node_type = data.get("nodeType")
        # id === name in the new format
        node_name = node["id"]

        # Check if this is a subgraph node
        if node_type == "subgraph" or node.get("type") == "subgraphNode":
            # Restore subgraph config
            subgraph_config = data.get("_subgraphConfig", {})

            config_node = {
                "type": "subgraph",
                "name": node_name,
                **subgraph_config,
                "meta": {
                    "ui": {
                        "position": position,
                    }
                },
            }

            # Update exposed ports from UI if they were modified
            # (in case ports were added/removed in the UI)
            in_ports = data.get("inPorts", [])
            out_ports = data.get("outPorts", [])

            # If the subgraph config doesn't have exposed_in_ports but UI has inPorts,
            # we need to preserve the existing mapping or create placeholders
            if "exposed_in_ports" not in config_node and in_ports:
                config_node["exposed_in_ports"] = {}
            if "exposed_out_ports" not in config_node and out_ports:
                config_node["exposed_out_ports"] = {}

        else:
            # Handle regular node
            # Build in_ports dict
            in_ports = {}
            for port in data.get("inPorts", []):
                port_config = {}
                if port.get("type"):
                    port_config["port_type"] = port["type"]
                in_ports[port["name"]] = port_config

            # Build out_ports dict
            out_ports = {}
            for port in data.get("outPorts", []):
                port_config = {}
                if port.get("type"):
                    port_config["port_type"] = port["type"]
                out_ports[port["name"]] = port_config

            config_node = {
                "type": "node",
                "name": node_name,
                "in_ports": in_ports,
                "out_ports": out_ports,
                "meta": {
                    "ui": {
                        "position": position,
                    }
                },
            }

            # Add factory if present
            if node_type == "factory":
                if data.get("factory"):
                    config_node["factory"] = data["factory"]
                if data.get("factoryArgs"):
                    config_node["factory_args"] = data["factoryArgs"]

            # Restore any extra config data
            extra_config = data.get("_config", {})
            for key, value in extra_config.items():
                if key not in config_node:
                    config_node[key] = value

        config_nodes.append(config_node)

    # Convert edges (id === name, so use node id directly)
    for edge in ui_edges:
        source_name = edge["source"]
        target_name = edge["target"]
        source_handle = edge.get("sourceHandle", "out")
        target_handle = edge.get("targetHandle", "in")

        config_edge = {
            "source_str": f"{source_name}.{source_handle}",
            "target_str": f"{target_name}.{target_handle}",
        }

        config_edges.append(config_edge)

    return {
        "nodes": config_nodes,
        "edges": config_edges,
    }
