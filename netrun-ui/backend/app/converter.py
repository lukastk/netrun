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


def graph_config_to_ui(graph_data: dict[str, Any]) -> tuple[list[dict], list[dict]]:
    """Convert GraphConfig-style data to UI format.

    Args:
        graph_data: Dictionary with "nodes" and "edges" keys in GraphConfig format.

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

        # Determine node type
        is_factory = node.get("factory") is not None
        node_type = "factory" if is_factory else "regular"

        # Convert ports
        in_ports = [
            {"name": name, "type": port.get("port_type")}
            for name, port in node.get("in_ports", {}).items()
        ]
        out_ports = [
            {"name": name, "type": port.get("port_type")}
            for name, port in node.get("out_ports", {}).items()
        ]

        ui_node = {
            "id": ui_meta.get("id", node_name),
            "type": "netrunNode",
            "position": position,
            "data": {
                "label": ui_meta.get("label", node_name),
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
            if k not in ("name", "in_ports", "out_ports", "factory", "factory_args", "meta")
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

    # Build id -> name mapping
    id_to_name = {}
    for node in ui_nodes:
        node_id = node["id"]
        data = node.get("data", {})
        name = data.get("label", node_id)
        id_to_name[node_id] = name

    # Convert nodes
    for node in ui_nodes:
        data = node.get("data", {})
        position = node.get("position", {"x": 0, "y": 0})

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
            "name": data.get("label", node["id"]),
            "in_ports": in_ports,
            "out_ports": out_ports,
            "meta": {
                "ui": {
                    "id": node["id"],
                    "label": data.get("label"),
                    "position": position,
                }
            },
        }

        # Add factory if present
        if data.get("nodeType") == "factory":
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

    # Convert edges
    for edge in ui_edges:
        source_id = edge["source"]
        target_id = edge["target"]
        source_handle = edge.get("sourceHandle", "out")
        target_handle = edge.get("targetHandle", "in")

        source_name = id_to_name.get(source_id, source_id)
        target_name = id_to_name.get(target_id, target_id)

        config_edge = {
            "source_str": f"{source_name}.{source_handle}",
            "target_str": f"{target_name}.{target_handle}",
        }

        config_edges.append(config_edge)

    return {
        "nodes": config_nodes,
        "edges": config_edges,
    }
