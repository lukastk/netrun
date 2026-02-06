"""Recipe execution endpoints for running Python scripts that transform configs."""
import importlib.util
import sys
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()


class GetPromptsRequest(BaseModel):
    """Request to get prompts from a recipe."""
    recipe_path: str  # Absolute path to .py file
    config: dict[str, Any]  # Current NetConfig


class GetPromptsResponse(BaseModel):
    """Response containing recipe prompts."""
    prompts: list[dict[str, Any]]


class ExecuteRequest(BaseModel):
    """Request to execute a recipe."""
    recipe_path: str  # Absolute path to .py file
    config: dict[str, Any]  # Current NetConfig
    inputs: dict[str, Any]  # User-provided inputs from prompts


class ExecuteResponse(BaseModel):
    """Response containing the modified config."""
    config: dict[str, Any]


def _load_recipe_module(path: str):
    """Hot-load a Python module from path.

    Each invocation creates a fresh module to ensure code changes are picked up.
    """
    path_obj = Path(path)
    if not path_obj.exists():
        raise HTTPException(status_code=404, detail=f"Recipe not found: {path}")

    if not path_obj.suffix == '.py':
        raise HTTPException(status_code=400, detail=f"Recipe must be a .py file: {path}")

    # Create unique module name to force reload
    # Use path hash + id to ensure uniqueness even for repeated calls
    module_name = f"_recipe_{path_obj.stem}_{id(path_obj)}_{hash(path)}"

    # Remove from cache if present (shouldn't be, but just in case)
    if module_name in sys.modules:
        del sys.modules[module_name]

    try:
        spec = importlib.util.spec_from_file_location(module_name, path_obj)
        if spec is None or spec.loader is None:
            raise HTTPException(status_code=500, detail=f"Failed to load recipe module: {path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        return module
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading recipe: {e}")
    finally:
        # Clean up module from cache to avoid memory leaks
        if module_name in sys.modules:
            del sys.modules[module_name]


@router.post("/get-prompts", response_model=GetPromptsResponse)
async def get_prompts(request: GetPromptsRequest) -> GetPromptsResponse:
    """Load a recipe and return its prompts.

    If the recipe has a `get_prompts(config)` function, call it to get the list
    of prompts. Otherwise, return an empty list (recipe runs immediately).
    """
    module = _load_recipe_module(request.recipe_path)

    if hasattr(module, 'get_prompts'):
        try:
            prompts = module.get_prompts(request.config)
            if not isinstance(prompts, list):
                raise HTTPException(
                    status_code=400,
                    detail="get_prompts() must return a list"
                )
            return GetPromptsResponse(prompts=prompts)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error calling get_prompts(): {e}"
            )

    return GetPromptsResponse(prompts=[])


@router.post("/execute", response_model=ExecuteResponse)
async def execute(request: ExecuteRequest) -> ExecuteResponse:
    """Execute a recipe with the given inputs and return the modified config.

    The recipe must have a `run(config, inputs)` function that takes the current
    config and user inputs, and returns the modified config.
    """
    module = _load_recipe_module(request.recipe_path)

    if not hasattr(module, 'run'):
        raise HTTPException(
            status_code=400,
            detail="Recipe missing required 'run' function"
        )

    try:
        result = module.run(request.config, request.inputs)
        if not isinstance(result, dict):
            raise HTTPException(
                status_code=400,
                detail="run() must return a dict"
            )
        return ExecuteResponse(config=result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error executing recipe: {e}"
        )
