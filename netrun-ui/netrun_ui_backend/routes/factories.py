"""Factory inspection and preview endpoints."""
import importlib
import importlib.util
import inspect
import pkgutil
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()


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
                    docstring = inspect.getdoc(get_node_config)

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


class FactoryParameter(BaseModel):
    """A parameter from the factory signature."""
    name: str
    type: str | None = None
    default: Any | None = None
    has_default: bool = False


class FactorySignatureResponse(BaseModel):
    """Response with factory signature info."""
    factory_path: str
    parameters: list[FactoryParameter]
    docstring: str | None = None


class FactoryPreviewRequest(BaseModel):
    """Request to preview factory-generated config."""
    factory_path: str
    factory_args: dict[str, Any] = {}


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
    error: str | None = None


@router.post("/signature", response_model=FactorySignatureResponse)
async def get_factory_signature(request: FactorySignatureRequest) -> FactorySignatureResponse:
    """Get the signature of a factory's get_node_config function.

    This allows the UI to know what arguments the factory accepts.
    """
    try:
        # Import the factory module
        module = importlib.import_module(request.factory_path)

        # Get the get_node_config function
        if not hasattr(module, "get_node_config"):
            raise HTTPException(
                status_code=400,
                detail=f"Factory module '{request.factory_path}' does not have a get_node_config function"
            )

        get_node_config = getattr(module, "get_node_config")

        # Inspect the signature
        sig = inspect.signature(get_node_config)
        parameters = []

        for name, param in sig.parameters.items():
            # Get type annotation as string
            type_str = None
            if param.annotation != inspect.Parameter.empty:
                if hasattr(param.annotation, "__name__"):
                    type_str = param.annotation.__name__
                else:
                    type_str = str(param.annotation)

            # Get default value
            has_default = param.default != inspect.Parameter.empty
            default = param.default if has_default else None

            # Convert default to JSON-serializable format
            if default is not None:
                try:
                    import json
                    json.dumps(default)  # Test if serializable
                except (TypeError, ValueError):
                    default = str(default)

            parameters.append(FactoryParameter(
                name=name,
                type=type_str,
                default=default,
                has_default=has_default,
            ))

        # Get docstring
        docstring = inspect.getdoc(get_node_config)

        return FactorySignatureResponse(
            factory_path=request.factory_path,
            parameters=parameters,
            docstring=docstring,
        )

    except ImportError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Could not import factory module '{request.factory_path}': {e}"
        )
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
        # Import the factory module
        module = importlib.import_module(request.factory_path)

        # Get the get_node_config function
        if not hasattr(module, "get_node_config"):
            raise HTTPException(
                status_code=400,
                detail=f"Factory module '{request.factory_path}' does not have a get_node_config function"
            )

        get_node_config = getattr(module, "get_node_config")

        # Call the factory
        try:
            node_config = get_node_config(**request.factory_args)
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
                # Parse Python's error message to extract argument names
                # Format: "get_node_config() missing 1 required positional argument: 'func'"
                # Or: "get_node_config() missing 2 required positional arguments: 'arg1' and 'arg2'"
                import re
                # Extract quoted argument names
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
        )

    except ImportError as e:
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


class ValidateImportResponse(BaseModel):
    """Response from import validation."""
    valid: bool
    error: str | None = None
    is_factory: bool = False


@router.post("/validate-import", response_model=ValidateImportResponse)
async def validate_import(request: ValidateImportRequest) -> ValidateImportResponse:
    """Validate that an import path is valid and check if it's a factory module."""
    try:
        module = importlib.import_module(request.import_path)

        # Check if it's a factory module (has get_node_config)
        is_factory = hasattr(module, "get_node_config")

        return ValidateImportResponse(
            valid=True,
            is_factory=is_factory,
        )

    except ImportError as e:
        return ValidateImportResponse(
            valid=False,
            error=str(e),
        )
    except Exception as e:
        return ValidateImportResponse(
            valid=False,
            error=f"Unexpected error: {e}",
        )
