"""Recipe: Add a function node.

Creates a factory node backed by netrun.node_factories.from_function
and generates a stub Python function in nodes.py.
"""
import copy
import uuid


def get_prompts(config):
    """Return prompts for user input before running the recipe."""
    return [
        {
            "name": "node_name",
            "label": "Node name",
            "type": "text",
            "default": "my_function",
        },
        {
            "name": "input_ports",
            "label": "Input port names (comma-separated)",
            "type": "text",
            "default": "x",
        },
        {
            "name": "output_ports",
            "label": "Output port names (comma-separated)",
            "type": "text",
            "default": "out",
        },
        {
            "name": "module_path",
            "label": "Python module path (relative to project root)",
            "type": "text",
            "default": "./nodes.py",
        },
    ]


def run(config, inputs):
    """Add a function factory node to the graph."""
    result = copy.deepcopy(config)

    node_name = inputs.get("node_name", "my_function")
    in_ports_str = inputs.get("input_ports", "x")
    out_ports_str = inputs.get("output_ports", "out")
    module_path = inputs.get("module_path", "./nodes.py")

    in_port_names = [p.strip() for p in in_ports_str.split(",") if p.strip()]
    out_port_names = [p.strip() for p in out_ports_str.split(",") if p.strip()]

    node_id = f"node-{uuid.uuid4().hex[:8]}"

    # Build the import path: ./nodes.py::my_function
    func_import = f"{module_path}::{node_name}"

    new_node = {
        "id": node_id,
        "type": "netrunNode",
        "position": {"x": 400, "y": 200},
        "data": {
            "label": node_name,
            "nodeType": "factory",
            "factory": "netrun.node_factories.from_function",
            "factoryArgs": {"func": func_import},
            "inPorts": [{"name": name} for name in in_port_names],
            "outPorts": [{"name": name} for name in out_port_names],
        },
    }

    result.setdefault("nodes", []).append(new_node)
    return result
