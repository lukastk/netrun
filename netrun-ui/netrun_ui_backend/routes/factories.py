"""Factory inspection and preview endpoints."""
import enum
import importlib
import inspect
import json
import os
import sys
from contextlib import contextmanager
from typing import Any, Union, get_args

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..import_utils import is_file_path_ref, import_module_from_ref, reload_module

router = APIRouter()


def _get_working_dir() -> str | None:
    """Return the project working directory (if set)."""
    return os.environ.get("NETRUN_UI_WORKING_DIR")


@contextmanager
def _with_working_dir_on_path():
    """Temporarily add the project working directory to sys.path.

    This allows importing local modules (e.g. ``nodes``) that live in the
    user's project directory.
    """
    wd = _get_working_dir()
    added = False
    if wd and wd not in sys.path:
        sys.path.insert(0, wd)
        added = True
    try:
        yield
    finally:
        if added and wd in sys.path:
            sys.path.remove(wd)


def _resolve_base_dir(project_root: str | None = None) -> str | None:
    """Resolve the base directory for file-path imports.

    If project_root is provided and relative, resolves it against NETRUN_UI_WORKING_DIR.
    """
    if project_root:
        from pathlib import Path
        p = Path(project_root)
        if not p.is_absolute():
            wd = _get_working_dir()
            if wd:
                return str((Path(wd) / p).resolve())
        return str(p.resolve())
    return _get_working_dir()


def _import_factory_module(
    factory_path: str,
    base_dir: str | None = None,
) -> Any:
    """Import a factory module from a dotted path or file-path reference.

    Returns the module. For file-path refs with '::', returns the module
    (not the attribute).
    """
    module, attr_name = import_module_from_ref(factory_path, base_dir=base_dir)
    if attr_name is not None:
        # For factory paths, we want the module, not the attribute.
        # The attribute (e.g., get_node_config) is looked up separately.
        # But if someone specifies path.py::get_node_config, that's fine too.
        pass
    return module, attr_name


class BuiltinFactory(BaseModel):
    """A built-in factory from netrun.node_factories."""
    import_path: str
    name: str
    docstring: str | None = None


class ListBuiltinFactoriesResponse(BaseModel):
    """Response with list of built-in factories."""
    factories: list[BuiltinFactory]
    errors: list[str] = []  # Any import errors encountered


@router.get("/builtin", response_model=ListBuiltinFactoriesResponse)
async def list_builtin_factories() -> ListBuiltinFactoriesResponse:
    """List all built-in factories from netrun.node_factories.

    Returns factories that have a get_node_config function.
    """
    import os
    import sys
    factories = []
    errors = []

    # Find the netrun package location without importing it
    netrun_path = None

    # Check common locations
    for path in sys.path:
        candidate = os.path.join(path, "netrun", "node_factories")
        if os.path.isdir(candidate):
            netrun_path = candidate
            break

    if netrun_path is None:
        return ListBuiltinFactoriesResponse(
            factories=[],
            errors=["netrun.node_factories package not found in Python path"]
        )

    # List Python files in the directory
    try:
        for entry in os.listdir(netrun_path):
            if entry.startswith("_"):
                continue
            if not entry.endswith(".py"):
                continue

            modname = entry[:-3]
            full_path = f"netrun.node_factories.{modname}"

            try:
                module = importlib.import_module(full_path)

                # Check if it's a factory module (has get_node_config)
                if hasattr(module, "get_node_config"):
                    get_node_config = getattr(module, "get_node_config")
                    docstring = getattr(module, "_factory_desc", None) or inspect.getdoc(get_node_config)

                    factories.append(BuiltinFactory(
                        import_path=full_path,
                        name=modname,
                        docstring=docstring,
                    ))
            except Exception as e:
                error_msg = f"{full_path}: {e}"
                errors.append(error_msg)
                continue
    except OSError as e:
        errors.append(f"Could not list {netrun_path}: {e}")

    return ListBuiltinFactoriesResponse(factories=factories, errors=errors)


class FactorySignatureRequest(BaseModel):
    """Request to get factory function signature."""
    factory_path: str
    project_root: str | None = None


class FactoryParameter(BaseModel):
    """A parameter from the factory signature."""
    name: str
    type: str | None = None
    default: Any | None = None
    has_default: bool = False
    enum_options: list[str] | None = None


class FactorySignatureResponse(BaseModel):
    """Response with factory signature info."""
    factory_path: str
    parameters: list[FactoryParameter]
    docstring: str | None = None


class FactoryPreviewRequest(BaseModel):
    """Request to preview factory-generated config."""
    factory_path: str
    factory_args: dict[str, Any] = {}
    project_root: str | None = None


class PortInfo(BaseModel):
    """Port information from factory preview."""
    name: str
    port_type: str | None = None


class FactoryPreviewResponse(BaseModel):
    """Response with factory-generated node preview."""
    factory_path: str
    name: str
    in_ports: list[PortInfo]
    out_ports: list[PortInfo]
    has_in_salvo_conditions: bool
    has_out_salvo_conditions: bool
    description: str | None = None
    extra: dict[str, Any] | None = None
    error: str | None = None


def _unwrap_optional(annotation: Any) -> Any:
    """Extract the non-None type from Optional[T] / T | None.

    Returns annotation as-is if not a union type.  Returns None for
    multi-type unions (e.g. str | int).
    """
    origin = getattr(annotation, "__origin__", None)
    # Handle typing.Union and types.UnionType (T | None syntax)
    if origin is Union or type(annotation).__name__ == "UnionType":
        args = get_args(annotation)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return non_none[0]
        return None  # multi-type union, skip
    return annotation


@router.post("/signature", response_model=FactorySignatureResponse)
async def get_factory_signature(request: FactorySignatureRequest) -> FactorySignatureResponse:
    """Get the signature of a factory's get_node_config function.

    This allows the UI to know what arguments the factory accepts.
    """
    try:
        base_dir = _resolve_base_dir(request.project_root)
        reload_module(request.factory_path, base_dir=base_dir)

        with _with_working_dir_on_path():
            module, attr_name = import_module_from_ref(request.factory_path, base_dir=base_dir)

        # If attr_name is specified (e.g. path.py::get_node_config), use it directly
        if attr_name is not None:
            get_node_config = getattr(module, attr_name)
        elif hasattr(module, "get_node_config"):
            get_node_config = getattr(module, "get_node_config")
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Factory module '{request.factory_path}' does not have a get_node_config function"
            )

        # Inspect the signature
        sig = inspect.signature(get_node_config)
        parameters = []

        for name, param in sig.parameters.items():
            # Skip internal parameters (e.g. _net_config) injected by the system
            if name.startswith("_"):
                continue
            # Get type annotation as string
            type_str = None
            if param.annotation != inspect.Parameter.empty:
                if hasattr(param.annotation, "__name__"):
                    type_str = param.annotation.__name__
                else:
                    type_str = str(param.annotation)

            # Detect Enum types and extract member values
            enum_options = None
            annotation = param.annotation
            if annotation != inspect.Parameter.empty:
                actual_type = _unwrap_optional(annotation)
                if actual_type is not None and isinstance(actual_type, type) and issubclass(actual_type, enum.Enum):
                    enum_options = [member.value for member in actual_type]

            # Get default value
            has_default = param.default != inspect.Parameter.empty
            default = param.default if has_default else None

            # Convert default to JSON-serializable format
            if default is not None:
                if isinstance(default, enum.Enum):
                    default = default.value
                else:
                    try:
                        json.dumps(default)  # Test if serializable
                    except (TypeError, ValueError):
                        default = str(default)

            parameters.append(FactoryParameter(
                name=name,
                type=type_str,
                default=default,
                has_default=has_default,
                enum_options=enum_options,
            ))

        # Get description: prefer _factory_desc attribute, fall back to docstring
        docstring = getattr(module, "_factory_desc", None) or inspect.getdoc(get_node_config)

        return FactorySignatureResponse(
            factory_path=request.factory_path,
            parameters=parameters,
            docstring=docstring,
        )

    except (ImportError, FileNotFoundError) as e:
        raise HTTPException(
            status_code=400,
            detail=f"Could not import factory module '{request.factory_path}': {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error inspecting factory: {e}"
        )


@router.post("/preview", response_model=FactoryPreviewResponse)
async def preview_factory(request: FactoryPreviewRequest) -> FactoryPreviewResponse:
    """Call a factory's get_node_config and return the generated config preview.

    This allows the UI to show what ports the factory node will have.
    """
    try:
        base_dir = _resolve_base_dir(request.project_root)

        # Reload the factory module and any modules referenced in args
        reload_module(request.factory_path, base_dir=base_dir)
        for val in request.factory_args.values():
            if isinstance(val, str) and ("." in val or is_file_path_ref(val)):
                reload_module(val, base_dir=base_dir)

        with _with_working_dir_on_path():
            module, attr_name = import_module_from_ref(request.factory_path, base_dir=base_dir)

            # Get the get_node_config function
            if attr_name is not None:
                get_node_config = getattr(module, attr_name)
            elif hasattr(module, "get_node_config"):
                get_node_config = getattr(module, "get_node_config")
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Factory module '{request.factory_path}' does not have a get_node_config function"
                )

            # Filter out empty string values so factory defaults are used
            filtered_args = {
                k: v for k, v in request.factory_args.items()
                if v != "" and v is not None
            }

            # Call the factory
            try:
                node_config = get_node_config(**filtered_args)
            except ModuleNotFoundError as e:
                return FactoryPreviewResponse(
                    factory_path=request.factory_path,
                    name="",
                    in_ports=[],
                    out_ports=[],
                    has_in_salvo_conditions=False,
                    has_out_salvo_conditions=False,
                    error=f"Module not found: {e.name}. Check the import path.",
                )
            except AttributeError as e:
                return FactoryPreviewResponse(
                    factory_path=request.factory_path,
                    name="",
                    in_ports=[],
                    out_ports=[],
                    has_in_salvo_conditions=False,
                    has_out_salvo_conditions=False,
                    error=f"Function not found: {e}",
                )
            except TypeError as e:
                # Missing required argument or wrong type
                error_msg = str(e)
                if "missing" in error_msg and "required" in error_msg:
                    import re
                    arg_matches = re.findall(r"'([^']+)'", error_msg)
                    if arg_matches:
                        if len(arg_matches) == 1:
                            friendly_msg = f"Required argument '{arg_matches[0]}' is missing"
                        else:
                            friendly_msg = f"Required arguments are missing: {', '.join(repr(a) for a in arg_matches)}"
                    else:
                        friendly_msg = "Required arguments are missing"
                    return FactoryPreviewResponse(
                        factory_path=request.factory_path,
                        name="",
                        in_ports=[],
                        out_ports=[],
                        has_in_salvo_conditions=False,
                        has_out_salvo_conditions=False,
                        error=friendly_msg,
                    )
                return FactoryPreviewResponse(
                    factory_path=request.factory_path,
                    name="",
                    in_ports=[],
                    out_ports=[],
                    has_in_salvo_conditions=False,
                    has_out_salvo_conditions=False,
                    error=f"Type error: {e}",
                )
            except ValueError as e:
                return FactoryPreviewResponse(
                    factory_path=request.factory_path,
                    name="",
                    in_ports=[],
                    out_ports=[],
                    has_in_salvo_conditions=False,
                    has_out_salvo_conditions=False,
                    error=f"Invalid value: {e}",
                )
            except Exception as e:
                return FactoryPreviewResponse(
                    factory_path=request.factory_path,
                    name="",
                    in_ports=[],
                    out_ports=[],
                    has_in_salvo_conditions=False,
                    has_out_salvo_conditions=False,
                    error=f"Factory error: {e}",
                )

        # Extract port information
        in_ports = []
        for name, port in node_config.in_ports.items():
            port_type = None
            if hasattr(port, "port_type") and port.port_type is not None:
                if isinstance(port.port_type, str):
                    port_type = port.port_type
                elif hasattr(port.port_type, "__name__"):
                    port_type = port.port_type.__name__
            in_ports.append(PortInfo(name=name, port_type=port_type))

        out_ports = []
        for name, port in node_config.out_ports.items():
            port_type = None
            if hasattr(port, "port_type") and port.port_type is not None:
                if isinstance(port.port_type, str):
                    port_type = port.port_type
                elif hasattr(port.port_type, "__name__"):
                    port_type = port.port_type.__name__
            out_ports.append(PortInfo(name=name, port_type=port_type))

        return FactoryPreviewResponse(
            factory_path=request.factory_path,
            name=node_config.name,
            in_ports=in_ports,
            out_ports=out_ports,
            has_in_salvo_conditions=bool(node_config.in_salvo_conditions),
            has_out_salvo_conditions=bool(node_config.out_salvo_conditions),
            description=getattr(node_config, 'description', None),
            extra=node_config.extra or None,
        )

    except (ImportError, FileNotFoundError) as e:
        raise HTTPException(
            status_code=400,
            detail=f"Could not import factory module '{request.factory_path}': {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error previewing factory: {e}"
        )


class ValidateImportRequest(BaseModel):
    """Request to validate an import path."""
    import_path: str
    project_root: str | None = None


class ValidateImportResponse(BaseModel):
    """Response from import validation."""
    valid: bool
    error: str | None = None
    is_factory: bool = False


@router.post("/validate-import", response_model=ValidateImportResponse)
async def validate_import(request: ValidateImportRequest) -> ValidateImportResponse:
    """Validate that an import path is valid and check if it's a factory module."""
    try:
        base_dir = _resolve_base_dir(request.project_root)
        reload_module(request.import_path, base_dir=base_dir)

        with _with_working_dir_on_path():
            module, attr_name = import_module_from_ref(request.import_path, base_dir=base_dir)

        # Check if it's a factory module (has get_node_config)
        if attr_name is not None:
            # If attr specified, check if the attr is callable
            is_factory = hasattr(module, attr_name) and callable(getattr(module, attr_name))
        else:
            is_factory = hasattr(module, "get_node_config")

        return ValidateImportResponse(
            valid=True,
            is_factory=is_factory,
        )

    except (ImportError, FileNotFoundError) as e:
        return ValidateImportResponse(
            valid=False,
            error=str(e),
        )
    except Exception as e:
        return ValidateImportResponse(
            valid=False,
            error=f"Unexpected error: {e}",
        )
