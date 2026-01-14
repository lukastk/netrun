# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp utils

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();
import dev.utils as this_module

# %%
#|hide
show_doc(this_module.patch_to)

# %%
#|export
import inspect
import typing
from types import ModuleType
from pathlib import Path
from typing import get_type_hints

def generate_stub_for_class(cls, indent: int = 0) -> str:
    """Generate a .pyi stub string for a class by runtime inspection.

    This captures monkey-patched methods that static analysis tools miss.

    Args:
        cls: The class to generate a stub for.
        indent: Indentation level (for nested classes).

    Returns:
        A string containing the stub definition.
    """
    ind = "    " * indent
    lines = [f"{ind}class {cls.__name__}:"]

    # Get type hints for the class itself (for class variables)
    try:
        class_hints = get_type_hints(cls) if hasattr(cls, '__annotations__') else {}
    except Exception:
        class_hints = getattr(cls, '__annotations__', {})

    # Class variables
    for name, type_hint in class_hints.items():
        type_str = _format_type(type_hint)
        lines.append(f"{ind}    {name}: {type_str}")

    has_members = bool(class_hints)

    # Methods
    for name, method in inspect.getmembers(cls):
        # Skip private/dunder (except __init__)
        if name.startswith('_') and name not in ('__init__', '__len__', '__iter__', '__next__', '__getitem__', '__setitem__', '__delitem__', '__contains__', '__repr__', '__str__', '__eq__', '__hash__', '__call__'):
            continue

        if inspect.isfunction(method) or inspect.ismethod(method):
            stub = _generate_method_stub(name, method, indent + 1)
            if stub:
                lines.append(stub)
                has_members = True
        elif isinstance(method, property):
            stub = _generate_property_stub(name, method, indent + 1)
            if stub:
                lines.append(stub)
                has_members = True

    if not has_members:
        lines.append(f"{ind}    ...")

    return '\n'.join(lines)


def _format_type(type_hint) -> str:
    """Format a type hint as a string suitable for .pyi files."""
    if type_hint is None:
        return "None"
    if type_hint is type(None):
        return "None"
    if type_hint is ...:
        return "..."

    # Handle string annotations
    if isinstance(type_hint, str):
        return type_hint

    # Handle typing special forms
    origin = getattr(type_hint, '__origin__', None)
    args = getattr(type_hint, '__args__', None)

    if origin is not None:
        origin_name = getattr(origin, '__name__', str(origin))
        # Clean up origin name
        if origin_name == 'dict':
            origin_name = 'dict'
        elif origin_name == 'list':
            origin_name = 'list'
        elif origin_name == 'set':
            origin_name = 'set'
        elif origin_name == 'tuple':
            origin_name = 'tuple'
        elif origin_name == 'Union':
            if args and len(args) == 2 and type(None) in args:
                # Optional[X] -> X | None
                other = args[0] if args[1] is type(None) else args[1]
                return f"{_format_type(other)} | None"
            return ' | '.join(_format_type(a) for a in args) if args else 'Any'

        if args:
            args_str = ', '.join(_format_type(a) for a in args)
            return f"{origin_name}[{args_str}]"
        return origin_name

    # Handle regular types
    if hasattr(type_hint, '__name__'):
        return type_hint.__name__

    return str(type_hint).replace('typing.', '')


def _generate_method_stub(name: str, method, indent: int) -> str:
    """Generate a stub for a method."""
    ind = "    " * indent

    try:
        sig = inspect.signature(method)
    except (ValueError, TypeError):
        return f"{ind}def {name}(self, *args, **kwargs) -> Any: ..."

    # Get type hints
    try:
        hints = get_type_hints(method)
    except Exception:
        hints = getattr(method, '__annotations__', {})

    # Build parameters
    params = []
    for pname, param in sig.parameters.items():
        if param.annotation != inspect.Parameter.empty:
            type_str = _format_type(param.annotation)
            if param.default != inspect.Parameter.empty:
                if param.default is None:
                    params.append(f"{pname}: {type_str} = None")
                else:
                    params.append(f"{pname}: {type_str} = ...")
            else:
                params.append(f"{pname}: {type_str}")
        elif pname in hints:
            type_str = _format_type(hints[pname])
            if param.default != inspect.Parameter.empty:
                params.append(f"{pname}: {type_str} = ...")
            else:
                params.append(f"{pname}: {type_str}")
        elif param.kind == inspect.Parameter.VAR_POSITIONAL:
            params.append(f"*{pname}")
        elif param.kind == inspect.Parameter.VAR_KEYWORD:
            params.append(f"**{pname}")
        elif param.default != inspect.Parameter.empty:
            if param.default is None:
                params.append(f"{pname}=None")
            else:
                params.append(f"{pname}=...")
        else:
            params.append(pname)

    # Return type
    ret_hint = hints.get('return', sig.return_annotation)
    if ret_hint != inspect.Parameter.empty and ret_hint is not None:
        ret_str = _format_type(ret_hint)
    else:
        ret_str = "None" if name == "__init__" else "..."

    return f"{ind}def {name}({', '.join(params)}) -> {ret_str}: ..."


def _generate_property_stub(name: str, prop: property, indent: int) -> str:
    """Generate a stub for a property."""
    ind = "    " * indent

    # Try to get type from getter
    if prop.fget:
        try:
            hints = get_type_hints(prop.fget)
            ret_type = hints.get('return', '...')
            type_str = _format_type(ret_type)
        except Exception:
            type_str = "..."
    else:
        type_str = "..."

    lines = [f"{ind}@property", f"{ind}def {name}(self) -> {type_str}: ..."]

    if prop.fset:
        lines.extend([f"{ind}@{name}.setter", f"{ind}def {name}(self, value: {type_str}) -> None: ..."])

    return '\n'.join(lines)

# %%
#|hide
show_doc(this_module.generate_stub_for_module)

# %%
#|export
def generate_stub_for_module(module: ModuleType) -> str:
    """Generate a .pyi stub string for a module by runtime inspection.

    Args:
        module: The module to generate stubs for.

    Returns:
        A string containing the complete .pyi file content.
    """
    lines = []

    # Collect imports we'll need
    imports = set()
    imports.add("from typing import Any")

    # Get __all__ if defined, otherwise use dir()
    if hasattr(module, '__all__'):
        names = module.__all__
    else:
        names = [n for n in dir(module) if not n.startswith('_')]

    # Process each exported name
    for name in sorted(names):
        try:
            obj = getattr(module, name)
        except AttributeError:
            continue

        if inspect.isclass(obj):
            lines.append(generate_stub_for_class(obj))
            lines.append("")
        elif inspect.isfunction(obj):
            stub = _generate_method_stub(name, obj, 0)
            lines.append(stub)
            lines.append("")

    # Build final output
    header = '\n'.join(sorted(imports))
    body = '\n'.join(lines)

    return f"{header}\n\n{body}"

# %%
#|hide
show_doc(this_module.generate_stubs_for_package)

# %%
#|export
import importlib

def generate_stubs_for_package(package_name: str, output_dir: str) -> list[str]:
    """Generate .pyi stub files for an entire package by runtime inspection.

    This imports the package and all submodules, then generates stubs based
    on the actual runtime state (capturing monkey-patched methods, etc.).

    Args:
        package_name: The package to generate stubs for (e.g., "netrun").
        output_dir: Directory to write .pyi files to.

    Returns:
        List of generated .pyi file paths.

    Example:
        >>> generate_stubs_for_package("netrun", "src/netrun")
        ['src/netrun/storage.pyi', 'src/netrun/utils.pyi']
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    generated = []

    # Import the main package
    package = importlib.import_module(package_name)

    # Find all submodules
    package_dir = Path(package.__file__).parent

    for py_file in package_dir.glob("*.py"):
        if py_file.name.startswith('_'):
            continue

        module_name = f"{package_name}.{py_file.stem}"
        try:
            module = importlib.import_module(module_name)
            stub_content = generate_stub_for_module(module)

            if stub_content.strip():
                stub_file = output_path / f"{py_file.stem}.pyi"
                stub_file.write_text(stub_content)
                generated.append(str(stub_file))
        except Exception as e:
            print(f"Warning: Could not generate stub for {module_name}: {e}")

    return generated
