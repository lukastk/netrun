"""Node functions for the actions and recipes demo.

These nodes demonstrate file-path imports (./nodes.py::func syntax)
and interaction with node variables.
"""


def greet(name: str, print, ctx) -> str:
    """Greet someone using node variables for the greeting template.

    Uses ctx.vars to access node_vars (greeting_template).
    """
    template = ctx.vars.get("greeting_template", "Hello, {name}!")
    result = template.format(name=name)
    print(f"Greeting: {result}")
    return result


def transform(data: str, print, ctx) -> str:
    """Transform data based on a configurable mode.

    Uses ctx.vars for 'mode' (uppercase, lowercase, reverse).
    """
    mode = ctx.vars.get("mode", "uppercase")
    print(f"Transforming '{data}' with mode={mode}")

    if mode == "uppercase":
        return data.upper()
    elif mode == "lowercase":
        return data.lower()
    elif mode == "reverse":
        return data[::-1]
    else:
        return data


def format_output(value: str, print) -> str:
    """Format the final output."""
    result = f">>> {value} <<<"
    print(f"Formatted: {result}")
    return result
