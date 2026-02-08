# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Worker Functions for ExecutionManager Tests
#
# These worker functions are defined in a separate module so they can be properly
# pickled and sent to subprocesses in MultiprocessPool tests.

# %%
#|default_exp execution_manager.workers

# %%
#|export
import asyncio
import sys
import time


def add_numbers(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b


def multiply_numbers(x: int, y: int) -> int:
    """Multiply two numbers."""
    return x * y


def function_with_print(name: str) -> str:
    """A function that prints."""
    print(f"Hello, {name}!")
    return f"greeted {name}"


def slow_function(delay: float) -> str:
    """A function that takes some time."""
    time.sleep(delay)
    return "done"


def function_with_error() -> None:
    """A function that raises an error."""
    raise ValueError("Intentional error")


def function_returns_non_serializable():
    """A function that returns something non-serializable."""
    return lambda x: x  # Lambdas can't be pickled


async def async_add(a: int, b: int) -> int:
    """Async function that adds two numbers."""
    await asyncio.sleep(0.01)
    return a + b


def function_with_kwargs(a: int, b: int = 10, c: int = 100) -> int:
    """Function with keyword arguments."""
    return a + b + c


def mp_stdout_function(message: str) -> str:
    """A function that writes directly to stdout to test subprocess output capture.

    This uses sys.stdout.write which bypasses the ExecutionManager's print capture
    and goes directly to the subprocess stdout, which is captured by the MultiprocessPool.
    """
    sys.stdout.write(f"MP Output: {message}\n")
    sys.stdout.flush()
    return f"printed {message}"


def function_with_multiple_prints(count: int) -> str:
    """A function that prints multiple times."""
    for i in range(count):
        print(f"Line {i}")
    return f"printed {count} lines"


def slow_printing_function(iterations: int, delay: float) -> str:
    """A function that prints with delays between prints."""
    for i in range(iterations):
        print(f"Step {i}")
        time.sleep(delay)
    return "done"
