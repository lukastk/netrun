"""Recipe execution endpoints for running Python scripts that transform configs."""
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from netrun.tools._recipes import get_recipe_prompts, execute_recipe

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


@router.post("/get-prompts", response_model=GetPromptsResponse)
async def get_prompts(request: GetPromptsRequest) -> GetPromptsResponse:
    """Load a recipe and return its prompts.

    If the recipe has a `get_prompts(config)` function, call it to get the list
    of prompts. Otherwise, return an empty list (recipe runs immediately).
    """
    try:
        prompts = get_recipe_prompts(request.recipe_path, request.config)
        return GetPromptsResponse(prompts=[p.model_dump() for p in prompts])
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except TypeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except (ImportError, Exception) as e:
        raise HTTPException(status_code=500, detail=f"Error loading recipe: {e}")


@router.post("/execute", response_model=ExecuteResponse)
async def execute(request: ExecuteRequest) -> ExecuteResponse:
    """Execute a recipe with the given inputs and return the modified config.

    The recipe must have a `run(config, inputs)` function that takes the current
    config and user inputs, and returns the modified config.
    """
    try:
        result = execute_recipe(request.recipe_path, request.config, request.inputs)
        return ExecuteResponse(config=result)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValueError, AttributeError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except TypeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except (ImportError, Exception) as e:
        raise HTTPException(status_code=500, detail=f"Error executing recipe: {e}")
