"""Recipe: Set default node variables for all nodes.

Demonstrates a simple recipe with text and select prompts that
modifies existing config rather than adding new nodes.
"""
import copy


def get_prompts(config):
    """Return prompts for setting default values."""
    return [
        {
            "name": "greeting_template",
            "label": "Default greeting template",
            "type": "text",
            "default": "Hello, {name}!",
        },
        {
            "name": "mode",
            "label": "Default transform mode",
            "type": "select",
            "options": ["uppercase", "lowercase", "reverse"],
            "default": "uppercase",
        },
    ]


def run(config, inputs):
    """Update the global node_vars in extraData."""
    result = copy.deepcopy(config)

    greeting = inputs.get("greeting_template", "Hello, {name}!")
    mode = inputs.get("mode", "uppercase")

    # Update extraData which maps to NetConfig-level fields
    result.setdefault("extraData", {})
    result["extraData"].setdefault("node_vars", {})
    result["extraData"]["node_vars"]["greeting_template"] = {
        "value": greeting,
        "type": "str",
    }
    result["extraData"]["node_vars"]["mode"] = {
        "value": mode,
        "type": "str",
    }

    return result
