"""Recipe execution endpoints — delegates to netrun.tools."""
import traceback
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from netrun.tools._models import RecipePrompt
from netrun.tools._recipes import get_recipe_prompts, execute_recipe

router = APIRouter()


class GetPromptsRequest(BaseModel):
    """Request to get prompts from a recipe."""
    recipe_path: str  # Absolute path to .py file
    config: dict[str, Any]  # Current NetConfig


class GetPromptsResponse(BaseModel):
    """Response containing recipe prompts."""
    prompts: list[RecipePrompt]


class ExecuteRequest(BaseModel):
    """Request to execute a recipe."""
    recipe_path: str  # Absolute path to .py file
    config: dict[str, Any]  # Current NetConfig
    inputs: dict[str, Any]  # User-provided inputs from prompts


class ExecuteResponse(BaseModel):
    """Response containing the modified config."""
    config: dict[str, Any]
    stdout: str = ""


@router.post("/get-prompts", response_model=GetPromptsResponse)
async def get_prompts(request: GetPromptsRequest) -> GetPromptsResponse:
    """Load a recipe and return its prompts."""
    try:
        prompts = get_recipe_prompts(request.recipe_path, request.config)
        return GetPromptsResponse(prompts=prompts)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        tb = traceback.format_exc()
        raise HTTPException(status_code=500, detail=f"{e}\n\n{tb}")


@router.post("/execute", response_model=ExecuteResponse)
async def execute(request: ExecuteRequest) -> ExecuteResponse:
    """Execute a recipe with the given inputs and return the modified config."""
    try:
        result, stdout = execute_recipe(request.recipe_path, request.config, request.inputs)
        return ExecuteResponse(config=result, stdout=stdout)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        tb = traceback.format_exc()
        raise HTTPException(status_code=500, detail=f"{e}\n\n{tb}")
