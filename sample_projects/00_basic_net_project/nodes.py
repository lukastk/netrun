"""Node functions for the basic net example.

Each function becomes a node in the network. The function signature determines
the node's ports:
- Parameters become input ports (except 'ctx' and 'print' which are special)
- Return type becomes output port(s)

Special parameters:
- ctx: NodeExecutionContext - provides access to the execution context
- print: A captured print function that logs output with timestamps
"""

def double(x: int, print) -> int:
    """Double the input value."""
    print(f"Doubling {x}")
    result = x * 2
    print(f"Result: {result}")
    return result


def add(a: int, b: int, print) -> int:
    """Add two numbers together."""
    print(f"Adding {a} + {b}")
    result = a + b
    print(f"Result: {result}")
    return result


def format_result(value: int, print) -> str:
    """Format the result as a string and log it."""
    print(f"Formatting result: {value}")
    result = f"The answer is: {value}"
    print(f"Formatted: {result}")
    return result


# _node_config override (TOML string) — adds extra metadata to the node
# without modifying the JSON config file. Merged with the auto-generated config.
format_result._node_config = '''
[extra]
description = "Formats the final result as a human-readable string"
category = "output"
'''


def analyze(value: int, print) -> {"summary": str, "breakdown": str}:
    """Analyze a value and produce multiple outputs.

    Demonstrates multiple output ports via dict return annotation.
    Each key in the return dict maps to a separate output port.
    """
    print(f"Analyzing {value}")
    is_even = "even" if value % 2 == 0 else "odd"
    is_positive = "positive" if value > 0 else "non-positive"
    return {
        "summary": f"Result: {value}",
        "breakdown": f"{value} is {is_even} and {is_positive}",
    }
