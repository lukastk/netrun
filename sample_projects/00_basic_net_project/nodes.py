"""Node functions for the basic net example.

Each function becomes a node in the network. The function signature determines
the node's ports:
- Parameters become input ports (except 'ctx' and 'print' which are special)
- Return type becomes output port(s)

Special parameters:
- ctx: NodeExecutionContext - provides access to the execution context
- print: A captured print function that logs output with timestamps
"""


def double(x: int) -> int:
    """Double the input value."""
    return x * 2


def add(a: int, b: int) -> int:
    """Add two numbers together."""
    return a + b


def format_result(value: int, print) -> str:
    """Format the result as a string and log it."""
    print(f"Formatting result: {value}")
    result = f"The answer is: {value}"
    print(f"Formatted: {result}")
    return result
