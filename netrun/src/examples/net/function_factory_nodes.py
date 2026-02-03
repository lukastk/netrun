"""Node functions for the function factory example.

These functions demonstrate various features of the function node factory:
- Simple type annotations for ports
- Special parameters (ctx, print)
- Multiple output ports
- _node_config override
"""

from netrun.net.config import PortConfig


def double_number(x: int) -> int:
    """A simple function that doubles a number.

    Input port: x (int)
    Output port: out (int)
    """
    return x * 2


def add_numbers(a: int, b: int) -> int:
    """Add two numbers together.

    Input ports: a (int), b (int)
    Output port: out (int)
    """
    return a + b


def process_with_logging(data: str, print) -> str:
    """Process data with logging via ctx.print.

    The 'print' parameter is a special parameter that receives ctx.print,
    allowing the function to log messages that are captured by Net.

    Input port: data (str)
    Output port: out (str)
    """
    print(f"Processing data: {data}")
    result = data.upper()
    print(f"Result: {result}")
    return result


def transform_with_context(value: int, ctx) -> int:
    """Transform a value and access the execution context.

    The 'ctx' parameter is a special parameter that receives the
    NodeExecutionContext, giving access to epoch_id, node_name, etc.

    Input port: value (int)
    Output port: out (int)
    """
    ctx.print(f"Epoch {ctx.epoch_id}: Transforming value {value}")
    return value * 10


def split_number(value: int) -> {"half": PortConfig(port_type=int), "doubled": PortConfig(port_type=int)}:
    """Split a number into two outputs.

    This demonstrates multiple output ports using a dict return annotation.

    Input port: value (int)
    Output ports: half (int), doubled (int)
    """
    return {
        "half": value // 2,
        "doubled": value * 2,
    }


def custom_named_node(x: int) -> int:
    """A function with a custom node name via _node_config."""
    return x + 100


custom_named_node._node_config = {
    "name": "MyCustomNode",
}


def toml_configured_node(x: int) -> int:
    """A function with TOML-based config override."""
    return x - 50


toml_configured_node._node_config = '''
name = "TomlConfiguredProcessor"
'''
