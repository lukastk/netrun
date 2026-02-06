"""Sample recipe that adds a new node to the graph."""


def get_prompts(config: dict) -> list[dict]:
    """Return prompts to collect user input before running."""
    return [
        {
            "name": "node_name",
            "label": "Node Name",
            "type": "text",
            "default": "new_node",
        },
        {
            "name": "node_type",
            "label": "Node Type",
            "type": "select",
            "options": ["regular", "factory"],
            "default": "regular",
        },
    ]


def run(config: dict, inputs: dict) -> dict:
    """Add a new node to the config."""
    # Calculate position for new node (to the right of existing nodes)
    nodes = config.get("nodes", [])
    max_x = 0
    for node in nodes:
        pos = node.get("position", {})
        x = pos.get("x", 0)
        if x > max_x:
            max_x = x

    new_node = {
        "id": inputs["node_name"],
        "type": "netrunNode",
        "position": {"x": max_x + 250, "y": 100},
        "data": {
            "label": inputs["node_name"],
            "nodeType": inputs["node_type"],
            "inPorts": [{"name": "in", "type": "any"}],
            "outPorts": [{"name": "out", "type": "any"}],
        },
    }

    config["nodes"] = nodes + [new_node]
    return config
