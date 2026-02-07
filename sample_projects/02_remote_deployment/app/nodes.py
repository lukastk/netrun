"""Node functions for the basic net example.

Each function becomes a node in the network. The function signature determines
the node's ports:
- Parameters become input ports (except 'ctx' and 'print' which are special)
- Return type becomes output port(s)

Special parameters:
- ctx: NodeExecutionContext - provides access to the execution context
- print: A captured print function that logs output with timestamps
"""

import socket


def _get_ip() -> str:
    """Return the primary IP address of this machine."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except OSError:
        return "unknown"


def double(x: int, print) -> int:
    """Double the input value."""
    print("IP: ", _get_ip())
    print(f"Doubling {x}")
    result = x * 2
    print(f"Result: {result}")
    return result


def add(a: int, b: int, print) -> int:
    """Add two numbers together."""
    print("IP: ", _get_ip())
    print(f"Adding {a} + {b}")
    result = a + b
    print(f"Result: {result}")
    return result


def format_result(value: int, print) -> str:
    """Format the result as a string and log it."""
    print("IP: ", _get_ip())
    print(f"Formatting result: {value}")
    result = f"The answer is: {value}"
    print(f"Formatted: {result}")
    return result
