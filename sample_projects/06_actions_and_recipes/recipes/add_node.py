"""Recipe: Add a new node to the graph.

Demonstrates all prompt types: text, number, select, checkbox.
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
            "default": "new_processor",
        },
        {
            "name": "node_type",
            "label": "Processing mode",
            "type": "select",
            "options": ["passthrough", "uppercase", "reverse"],
            "default": "passthrough",
        },
        {
            "name": "position_x",
            "label": "X position",
            "type": "number",
            "default": 500,
        },
        {
            "name": "position_y",
            "label": "Y position",
            "type": "number",
            "default": 300,
        },
        {
            "name": "add_to_output",
            "label": "Add output queue?",
            "type": "checkbox",
            "default": True,
        },
    ]


def run(config, inputs):
    """Add a new node with the configured settings."""
    result = copy.deepcopy(config)

    node_name = inputs.get("node_name", "new_processor")
    node_type = inputs.get("node_type", "passthrough")
    pos_x = inputs.get("position_x", 500)
    pos_y = inputs.get("position_y", 300)
    add_output = inputs.get("add_to_output", True)

    node_id = f"node-{uuid.uuid4().hex[:8]}"

    new_node = {
        "id": node_id,
        "type": "netrunNode",
        "position": {"x": pos_x, "y": pos_y},
        "data": {
            "label": node_name,
            "nodeType": "regular",
            "inPorts": [{"name": "data", "type": "str"}],
            "outPorts": [{"name": "out", "type": "str"}],
            "_config": {
                "execution_config": {
                    "node_vars": {
                        "mode": {
                            "value": node_type,
                            "type": "str",
                        }
                    }
                },
                "extra": {
                    "ui": {
                        "env": {"PROCESSING_MODE": node_type},
                    }
                },
            },
        },
    }

    result.setdefault("nodes", []).append(new_node)
    return result
