"""Shared utilities for importing modules from dotted paths or file-path references.

File-path reference detection rules:
- Contains '::' (file path + attribute separator)
- Contains '/' or '\\' (path separator)
- Starts with '.' (relative path like './' or '../')

Otherwise: dotted import path (existing behavior).
"""
import importlib
import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Any


def is_file_path_ref(s: str) -> bool:
    """Check if string is a file-path reference (vs dotted import path)."""
    return '::' in s or '/' in s or '\\' in s or s.startswith('.')


def import_module_from_ref(
    ref: str,
    base_dir: str | None = None,
) -> tuple[ModuleType, str | None]:
    """Import a module from a dotted path or file-path reference.

    Args:
        ref: Module reference. Either a dotted import path (e.g. "myapp.nodes")
             or a file-path reference (e.g. "./nodes.py", "./nodes.py::my_func").
        base_dir: Base directory for resolving relative file paths.
                  If None, uses cwd.

    Returns:
        Tuple of (module, attr_name).
        - For dotted paths: (imported_module, None)
        - For "path/to/file.py": (loaded_module, None)
        - For "path/to/file.py::attr": (loaded_module, "attr")

    Raises:
        ImportError: If the module cannot be imported/loaded.
        FileNotFoundError: If a file-path reference points to a missing file.
    """
    if not is_file_path_ref(ref):
        # Standard dotted import path
        module = importlib.import_module(ref)
        return module, None

    # File-path reference
    if '::' in ref:
        file_path_str, attr_name = ref.split('::', 1)
    else:
        file_path_str = ref
        attr_name = None

    file_path = Path(file_path_str)
    if not file_path.is_absolute():
        base = Path(base_dir) if base_dir else Path(os.getcwd())
        file_path = (base / file_path).resolve()
    else:
        file_path = file_path.resolve()

    if not file_path.exists():
        raise FileNotFoundError(f"Module file not found: {file_path}")

    module_name = f"_netrun_ui_filemod_{file_path.stem}_{hash(str(file_path)) & 0xFFFFFFFF:08x}"

    # Return cached module if available (use reload_module to clear cache)
    if module_name in sys.modules:
        return sys.modules[module_name], attr_name

    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot create module spec from: {file_path}")

    # Disable bytecode caching so reloads always pick up source changes
    spec.cached = None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    return module, attr_name


def reload_module(ref: str, base_dir: str | None = None) -> None:
    """Remove a module from sys.modules cache to force reimport.

    For dotted paths: clears the top-level package and submodules.
    For file-path references: clears the generated module name.
    """
    if is_file_path_ref(ref):
        # File-path reference: resolve the path and clear its module name
        if '::' in ref:
            file_path_str = ref.split('::', 1)[0]
        else:
            file_path_str = ref

        file_path = Path(file_path_str)
        if not file_path.is_absolute() and base_dir:
            file_path = (Path(base_dir) / file_path).resolve()
        else:
            file_path = file_path.resolve()

        module_name = f"_netrun_ui_filemod_{file_path.stem}_{hash(str(file_path)) & 0xFFFFFFFF:08x}"
        if module_name in sys.modules:
            del sys.modules[module_name]

        # Delete bytecode cache (.pyc) so exec_module re-reads source
        try:
            pyc_path = importlib.util.cache_from_source(str(file_path))
            if os.path.exists(pyc_path):
                os.remove(pyc_path)
        except (NotImplementedError, OSError):
            pass

        importlib.invalidate_caches()
    else:
        # Dotted import path: clear top-level package and submodules
        top = ref.split(".")[0]
        to_remove = [
            name for name in sys.modules
            if name == top or name.startswith(top + ".")
        ]
        for name in to_remove:
            del sys.modules[name]
        importlib.invalidate_caches()


def get_attr_from_ref(
    ref: str,
    base_dir: str | None = None,
    default_attr: str | None = None,
) -> Any:
    """Import and get an attribute from a module reference.

    Args:
        ref: Module reference (dotted path or file-path reference).
        base_dir: Base directory for resolving relative file paths.
        default_attr: Attribute name to look up when no '::' is specified
                      in a file-path reference. If None, returns the module itself.

    Returns:
        The resolved attribute or module.
    """
    module, attr_name = import_module_from_ref(ref, base_dir=base_dir)

    if attr_name is not None:
        return getattr(module, attr_name)

    if default_attr is not None and is_file_path_ref(ref):
        return getattr(module, default_attr)

    return module
