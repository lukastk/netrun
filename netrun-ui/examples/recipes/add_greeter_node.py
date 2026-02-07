"""Example recipe: adds a greeter node to the graph.

Recipes receive and must return config in the UI format:
{
    "nodes": [{"id": "...", "type": "netrunNode", "position": {...},
               "data": {"label": "...", "nodeType": "regular",
                        "inPorts": [...], "outPorts": [...], "_config": {...}}}],
    "edges": [...],
    "extra": {...},
    "extraData": {...}
}
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
            "default": "Greeter",
        },
        {
            "name": "greeting",
            "label": "Greeting message",
            "type": "text",
            "default": "Hello, world!",
        },
        {
            "name": "position_x",
            "label": "X position",
            "type": "number",
            "default": 400,
        },
        {
            "name": "position_y",
            "label": "Y position",
            "type": "number",
            "default": 300,
        },
    ]


def run(config, inputs):
    """Add a greeter node to the graph."""
    result = copy.deepcopy(config)

    node_name = inputs.get("node_name", "Greeter")
    greeting = inputs.get("greeting", "Hello, world!")
    pos_x = inputs.get("position_x", 400)
    pos_y = inputs.get("position_y", 300)

    node_id = f"node-{uuid.uuid4().hex[:8]}"

    new_node = {
        "id": node_id,
        "type": "netrunNode",
        "position": {"x": pos_x, "y": pos_y},
        "data": {
            "label": node_name,
            "nodeType": "regular",
            "inPorts": [{"name": "name", "type": "str"}],
            "outPorts": [{"name": "message", "type": "str"}],
            "_config": {
                "extra": {
                    "ui": {
                        "env": {"GREETING_MSG": greeting},
                    }
                }
            },
        },
    }

    result.setdefault("nodes", []).append(new_node)
    return result
